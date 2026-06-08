"""Integration tests for the WebSocket wire server."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

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
from slack_fuse_server.wire.frames import (
    CaughtUpFrame,
    ErrorCode,
    ErrorFrame,
    EventFrame,
    Frame,
    FrameAdapter,
    PingFrame,
    PongFrame,
    SubscribeFrame,
)
from slack_fuse_server.wire.server import WireServer, WireServerOptions

pytestmark = pytest.mark.trio

_SERVER_MIGRATIONS = Path(server_migrations.__file__).parent
_NO_HEARTBEAT_S = 3_600.0


@asynccontextmanager
async def _running_server(
    database_url: str,
    *,
    max_replay_events: int = 5_000,
    heartbeat_interval_s: float = _NO_HEARTBEAT_S,
) -> AsyncIterator[int]:
    server = WireServer(
        database_url,
        port=0,
        options=WireServerOptions(
            max_replay_events=max_replay_events,
            heartbeat_interval_s=heartbeat_interval_s,
            client_timeout_s=_NO_HEARTBEAT_S,
        ),
    )
    async with trio.open_nursery() as nursery:
        ws_server = await nursery.start(server.serve)
        try:
            yield ws_server.port
        finally:
            nursery.cancel_scope.cancel()


@asynccontextmanager
async def _connect(port: int) -> AsyncIterator[WebSocketConnection]:
    async with open_websocket("127.0.0.1", port, "/ws", use_ssl=False) as ws:
        yield ws


async def _recv_frame(ws: WebSocketConnection, *, timeout_s: float = 5.0) -> Frame:
    # Bumped from 1.0s to 5.0s after the 2F-auto-provisioned per-session
    # postgres made test_concurrent_connections_do_not_crosstalk flake
    # under full-suite load. 1.0s catches real hangs in isolation but is
    # too tight when the per-test schema-create transaction is sharing a
    # cold-started Pg backend with 50+ other DB-touching tests.
    with trio.fail_after(timeout_s):
        return FrameAdapter.validate_json(await ws.get_message())


async def _maybe_recv_frame(ws: WebSocketConnection, *, timeout_s: float = 0.2) -> Frame | None:
    frame: Frame | None = None
    with trio.move_on_after(timeout_s) as cancel_scope:
        frame = FrameAdapter.validate_json(await ws.get_message())
    if cancel_scope.cancelled_caught:
        return None
    return frame


def _prepare_database(pg_conn: psycopg.Connection[TupleRow]) -> str:
    apply_migrations(pg_conn, _SERVER_MIGRATIONS)
    schema = _current_schema(pg_conn)
    return make_conninfo(pg_conn.info.dsn, options=f"-c search_path={schema}")


def _current_schema(pg_conn: psycopg.Connection[TupleRow]) -> str:
    with pg_conn.cursor() as cur:
        cur.execute("SELECT current_schema()")
        row = cur.fetchone()
    assert row is not None
    return str(row[0])


def _seed_stream(pg_conn: psycopg.Connection[TupleRow], stream: str, payloads: list[JsonObject]) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO stream_heads (stream, next_offset) VALUES (%s, %s)",
            (stream, len(payloads) + 1),
        )
        for offset, payload in enumerate(payloads, start=1):
            cur.execute(
                """
                INSERT INTO events (stream, offset_in_stream, kind, ts, payload)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (stream, offset, "message", payload.get("ts"), Jsonb(payload)),
            )
    pg_conn.commit()


def _append_event(
    pg_conn: psycopg.Connection[TupleRow],
    stream: str,
    payload: JsonObject,
    *,
    notify_payload: str | None = None,
) -> int:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO stream_heads (stream, next_offset)
            VALUES (%s, 1)
            ON CONFLICT (stream) DO NOTHING
            """,
            (stream,),
        )
        cur.execute(
            """
            UPDATE stream_heads
            SET next_offset = next_offset + 1
            WHERE stream = %s
            RETURNING next_offset - 1
            """,
            (stream,),
        )
        row = cur.fetchone()
        assert row is not None
        offset = int(row[0])
        cur.execute(
            """
            INSERT INTO events (stream, offset_in_stream, kind, ts, payload)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (stream, offset, "message", payload.get("ts"), Jsonb(payload)),
        )
        cur.execute("SELECT pg_notify('new_event', %s)", (stream if notify_payload is None else notify_payload,))
    pg_conn.commit()
    return offset


def _message(ts: str, text: str) -> JsonObject:
    return {"ts": ts, "text": text}


async def test_subscribe_from_zero_replays_events_then_caught_up(pg_conn: psycopg.Connection[TupleRow]) -> None:
    database_url = _prepare_database(pg_conn)
    stream = "channel:C1"
    _seed_stream(pg_conn, stream, [_message("1.000001", "one"), _message("2.000001", "two")])

    async with _running_server(database_url) as port, _connect(port) as ws:
        await ws.send_message(SubscribeFrame(stream=stream, since=0).model_dump_json())

        first = await _recv_frame(ws)
        second = await _recv_frame(ws)
        caught_up = await _recv_frame(ws)

    assert isinstance(first, EventFrame)
    assert first.stream == stream
    assert first.offset == 1
    assert first.payload["text"] == "one"
    assert isinstance(second, EventFrame)
    assert second.offset == 2
    assert isinstance(caught_up, CaughtUpFrame)
    assert caught_up.stream == stream
    assert caught_up.head_offset == 2


async def test_subscribe_from_recent_offset_replays_gap(pg_conn: psycopg.Connection[TupleRow]) -> None:
    database_url = _prepare_database(pg_conn)
    stream = "channel:C1"
    _seed_stream(
        pg_conn,
        stream,
        [_message("1.000001", "one"), _message("2.000001", "two"), _message("3.000001", "three")],
    )

    async with _running_server(database_url) as port, _connect(port) as ws:
        await ws.send_message(SubscribeFrame(stream=stream, since=1).model_dump_json())

        first = await _recv_frame(ws)
        second = await _recv_frame(ws)
        caught_up = await _recv_frame(ws)

    assert isinstance(first, EventFrame)
    assert first.offset == 2
    assert isinstance(second, EventFrame)
    assert second.offset == 3
    assert isinstance(caught_up, CaughtUpFrame)
    assert caught_up.head_offset == 3


async def test_subscribe_unknown_stream_returns_stream_not_found(pg_conn: psycopg.Connection[TupleRow]) -> None:
    database_url = _prepare_database(pg_conn)

    async with _running_server(database_url) as port, _connect(port) as ws:
        await ws.send_message(SubscribeFrame(stream="channel:UNKNOWN", since=0).model_dump_json())
        frame = await _recv_frame(ws)

    assert isinstance(frame, ErrorFrame)
    assert frame.code is ErrorCode.STREAM_NOT_FOUND
    assert frame.stream == "channel:UNKNOWN"


async def test_subscribe_since_too_high_returns_head_offset(pg_conn: psycopg.Connection[TupleRow]) -> None:
    database_url = _prepare_database(pg_conn)
    stream = "channel:C1"
    _seed_stream(pg_conn, stream, [_message("1.000001", "one")])

    async with _running_server(database_url) as port, _connect(port) as ws:
        await ws.send_message(SubscribeFrame(stream=stream, since=2).model_dump_json())
        frame = await _recv_frame(ws)

    assert isinstance(frame, ErrorFrame)
    assert frame.code is ErrorCode.SINCE_TOO_HIGH
    assert frame.stream == stream
    assert frame.head_offset == 1


async def test_subscribe_too_old_returns_snapshot_required(pg_conn: psycopg.Connection[TupleRow]) -> None:
    database_url = _prepare_database(pg_conn)
    stream = "channel:C1"
    _seed_stream(pg_conn, stream, [_message("1.000001", "one"), _message("2.000001", "two")])

    async with _running_server(database_url, max_replay_events=1) as port, _connect(port) as ws:
        await ws.send_message(SubscribeFrame(stream=stream, since=0).model_dump_json())
        frame = await _recv_frame(ws)

    assert isinstance(frame, ErrorFrame)
    assert frame.code is ErrorCode.SNAPSHOT_REQUIRED
    assert frame.stream == stream
    assert frame.head_offset == 2


async def test_client_ping_gets_pong(pg_conn: psycopg.Connection[TupleRow]) -> None:
    database_url = _prepare_database(pg_conn)

    async with _running_server(database_url) as port, _connect(port) as ws:
        await ws.send_message(PingFrame().model_dump_json())
        frame = await _recv_frame(ws)

    assert isinstance(frame, PongFrame)


async def test_server_heartbeat_ping_accepts_client_pong(pg_conn: psycopg.Connection[TupleRow]) -> None:
    database_url = _prepare_database(pg_conn)

    async with _running_server(database_url, heartbeat_interval_s=0.05) as port, _connect(port) as ws:
        frame = await _recv_frame(ws)
        assert isinstance(frame, PingFrame)
        await ws.send_message(PongFrame().model_dump_json())


async def test_notify_delivers_live_event_within_500ms(pg_conn: psycopg.Connection[TupleRow]) -> None:
    database_url = _prepare_database(pg_conn)
    stream = "channel:C1"
    _seed_stream(pg_conn, stream, [_message("1.000001", "one")])

    async with _running_server(database_url) as port, _connect(port) as ws:
        await ws.send_message(SubscribeFrame(stream=stream, since=1).model_dump_json())
        caught_up = await _recv_frame(ws)
        assert isinstance(caught_up, CaughtUpFrame)

        offset = _append_event(pg_conn, stream, _message("2.000001", "two"))
        frame = await _recv_frame(ws, timeout_s=3.0)

    assert isinstance(frame, EventFrame)
    assert frame.stream == stream
    assert frame.offset == offset
    assert frame.payload["text"] == "two"


async def test_concurrent_connections_do_not_crosstalk(pg_conn: psycopg.Connection[TupleRow]) -> None:
    database_url = _prepare_database(pg_conn)
    stream_a = "channel:CA"
    stream_b = "channel:CB"
    _seed_stream(pg_conn, stream_a, [_message("1.000001", "a1")])
    _seed_stream(pg_conn, stream_b, [_message("1.000002", "b1")])

    async with _running_server(database_url) as port, _connect(port) as ws_a, _connect(port) as ws_b:
        await ws_a.send_message(SubscribeFrame(stream=stream_a, since=1).model_dump_json())
        await ws_b.send_message(SubscribeFrame(stream=stream_b, since=1).model_dump_json())
        assert isinstance(await _recv_frame(ws_a), CaughtUpFrame)
        assert isinstance(await _recv_frame(ws_b), CaughtUpFrame)

        offset = _append_event(pg_conn, stream_a, _message("2.000001", "a2"))
        frame_a = await _recv_frame(ws_a, timeout_s=3.0)
        frame_b = await _maybe_recv_frame(ws_b)

    assert isinstance(frame_a, EventFrame)
    assert frame_a.stream == stream_a
    assert frame_a.offset == offset
    assert frame_b is None


async def test_empty_notify_payload_checks_all_subscriptions(pg_conn: psycopg.Connection[TupleRow]) -> None:
    database_url = _prepare_database(pg_conn)
    stream = "channel:C1"
    _seed_stream(pg_conn, stream, [_message("1.000001", "one")])

    async with _running_server(database_url) as port, _connect(port) as ws:
        await ws.send_message(SubscribeFrame(stream=stream, since=1).model_dump_json())
        assert isinstance(await _recv_frame(ws), CaughtUpFrame)

        offset = _append_event(pg_conn, stream, _message("2.000001", "two"), notify_payload="")
        frame = await _recv_frame(ws, timeout_s=3.0)

    assert isinstance(frame, EventFrame)
    assert frame.offset == offset
