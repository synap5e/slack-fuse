"""Composition tests for `slack_fuse_server.dispatch`.

The RFC requires `/health`, `/metrics`, and `/ws` on a single listen address.
These tests bind one TCP port, run the shared dispatch over it, and exercise all
three endpoints — proving the same-port Upgrade dispatch routes HTTP and
WebSocket correctly and that `/metrics` reflects the live WS subscriber.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import cast

import httpx
import psycopg
import pytest
import trio
from psycopg.conninfo import make_conninfo
from psycopg.rows import TupleRow
from psycopg.types.json import Jsonb
from trio_websocket import WebSocketConnection, open_websocket

import slack_fuse_server.migrations as server_migrations
from slack_fuse.migrations.runner import apply_migrations
from slack_fuse_server._json import JsonObject
from slack_fuse_server.dispatch import serve_dispatch_on_listeners
from slack_fuse_server.http.dto import MetricsResponse
from slack_fuse_server.http.metrics import MetricsAggregator, SubscriberSnapshot
from slack_fuse_server.wire.frames import CaughtUpFrame, EventFrame, Frame, FrameAdapter, SubscribeFrame
from slack_fuse_server.wire.server import WireServer, WireServerOptions

pytestmark = pytest.mark.trio

_SERVER_MIGRATIONS = Path(server_migrations.__file__).parent
_NO_HEARTBEAT_S = 3_600.0


def _prepare_database(pg_conn: psycopg.Connection[TupleRow]) -> str:
    apply_migrations(pg_conn, _SERVER_MIGRATIONS)
    with pg_conn.cursor() as cur:
        cur.execute("SELECT current_schema()")
        row = cur.fetchone()
    assert row is not None
    return make_conninfo(pg_conn.info.dsn, options=f"-c search_path={row[0]}")


def _seed_stream(pg_conn: psycopg.Connection[TupleRow], stream: str, payloads: list[JsonObject]) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO stream_heads (stream, next_offset) VALUES (%s, %s)",
            (stream, len(payloads) + 1),
        )
        for offset, payload in enumerate(payloads, start=1):
            cur.execute(
                "INSERT INTO events (stream, offset_in_stream, kind, ts, payload) VALUES (%s, %s, %s, %s, %s)",
                (stream, offset, "message", payload.get("ts"), Jsonb(payload)),
            )
    pg_conn.commit()


def _build_metrics(database_url: str, wire_server: WireServer) -> MetricsAggregator:
    def _subscribers() -> list[SubscriberSnapshot]:
        return [
            SubscriberSnapshot(
                client_id=info.client_id,
                connected_since=info.connected_since,
                subscriptions=info.subscriptions,
            )
            for info in wire_server.connection_infos()
        ]

    return MetricsAggregator(
        database_url=database_url,
        server_started_at=datetime(2026, 6, 8, tzinfo=UTC),
        socket_mode_state=lambda: "connected",
        subscribers=_subscribers,
    )


@asynccontextmanager
async def _running_dispatch(database_url: str) -> AsyncIterator[tuple[int, WireServer]]:
    wire_server = WireServer(
        database_url,
        options=WireServerOptions(heartbeat_interval_s=_NO_HEARTBEAT_S, client_timeout_s=_NO_HEARTBEAT_S),
    )
    metrics = _build_metrics(database_url, wire_server)
    listeners = await trio.open_tcp_listeners(0, host="127.0.0.1")
    port = cast(tuple[str, int], listeners[0].socket.getsockname())[1]
    handler = partial(serve_dispatch_on_listeners, listeners, wire_server=wire_server, metrics_source=metrics)
    async with trio.open_nursery() as nursery:
        nursery.start_soon(handler)
        await trio.sleep(0.05)
        try:
            yield port, wire_server
        finally:
            nursery.cancel_scope.cancel()


async def _recv_frame(ws: WebSocketConnection, *, timeout_s: float = 1.0) -> Frame:
    with trio.fail_after(timeout_s):
        return FrameAdapter.validate_json(await ws.get_message())


async def test_health_served_on_shared_port(pg_conn: psycopg.Connection[TupleRow]) -> None:
    database_url = _prepare_database(pg_conn)
    async with (
        _running_dispatch(database_url) as (port, _wire),
        httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client,
    ):
        response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"ok": True}


async def test_metrics_served_on_shared_port(pg_conn: psycopg.Connection[TupleRow]) -> None:
    database_url = _prepare_database(pg_conn)
    _seed_stream(pg_conn, "channel:C1", [{"ts": "1.000001", "text": "one"}])
    async with (
        _running_dispatch(database_url) as (port, _wire),
        httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client,
    ):
        response = await client.get("/metrics")
    assert response.status_code == 200
    parsed = MetricsResponse.model_validate(response.json())
    assert parsed.subscribers.active_ws_connections == 0
    assert any(stream.stream == "channel:C1" for stream in parsed.streams)


async def test_unknown_path_returns_404_on_shared_port(pg_conn: psycopg.Connection[TupleRow]) -> None:
    database_url = _prepare_database(pg_conn)
    async with (
        _running_dispatch(database_url) as (port, _wire),
        httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client,
    ):
        response = await client.get("/nope")
    assert response.status_code == 404


async def test_websocket_served_on_shared_port(pg_conn: psycopg.Connection[TupleRow]) -> None:
    database_url = _prepare_database(pg_conn)
    stream = "channel:C1"
    _seed_stream(pg_conn, stream, [{"ts": "1.000001", "text": "one"}, {"ts": "2.000001", "text": "two"}])
    async with (
        _running_dispatch(database_url) as (port, _wire),
        open_websocket("127.0.0.1", port, "/ws", use_ssl=False) as ws,
    ):
        await ws.send_message(SubscribeFrame(stream=stream, since=0).model_dump_json())
        first = await _recv_frame(ws)
        second = await _recv_frame(ws)
        caught_up = await _recv_frame(ws)
    assert isinstance(first, EventFrame)
    assert first.offset == 1
    assert isinstance(second, EventFrame)
    assert second.offset == 2
    assert isinstance(caught_up, CaughtUpFrame)
    assert caught_up.head_offset == 2


async def test_http_and_ws_coexist_on_one_port(pg_conn: psycopg.Connection[TupleRow]) -> None:
    """A live WS connection and HTTP requests share the same port; /metrics sees the subscriber."""
    database_url = _prepare_database(pg_conn)
    stream = "channel:C1"
    _seed_stream(pg_conn, stream, [{"ts": "1.000001", "text": "one"}])
    async with _running_dispatch(database_url) as (port, _wire):
        base_url = f"http://127.0.0.1:{port}"
        async with open_websocket("127.0.0.1", port, "/ws", use_ssl=False) as ws:
            await ws.send_message(SubscribeFrame(stream=stream, since=0).model_dump_json())
            assert isinstance(await _recv_frame(ws), EventFrame)
            assert isinstance(await _recv_frame(ws), CaughtUpFrame)

            # HTTP still works while the WS connection is open, and reports it.
            async with httpx.AsyncClient(base_url=base_url) as client:
                health = await client.get("/health")
                metrics = await client.get("/metrics")
    assert health.status_code == 200
    assert metrics.status_code == 200
    parsed = MetricsResponse.model_validate(metrics.json())
    assert parsed.subscribers.active_ws_connections == 1
    assert parsed.subscribers.by_client[0].subscriptions == 1
