"""HTTP snapshot endpoint tests (`GET /streams/<id>/snapshot?at=<offset>`)."""

from __future__ import annotations

import gzip
import json
import uuid
from collections.abc import Callable, Iterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from urllib.parse import quote

import httpx
import psycopg
import pytest
import trio
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import TupleRow

import slack_fuse.migrations as client_migrations
from slack_fuse.migrations.runner import apply_migrations
from slack_fuse.models import Message
from slack_fuse.projector.snapshot_fetch import SnapshotRedirect, fetch_and_apply_snapshot
from slack_fuse_server._json import JsonObject
from slack_fuse_server.http.dto import (
    SNAPSHOT_CONTENT_ENCODING,
    SNAPSHOT_CONTENT_TYPE,
    BackfillMetrics,
    MetricsResponse,
    RateLimitBudget,
    SlackMetrics,
    SubscribersMetrics,
)
from slack_fuse_server.http.handlers import SnapshotDeps
from slack_fuse_server.http.metrics import MetricsSource
from slack_fuse_server.http.server import HttpRequest, route_request, serve_http_on_listeners
from slack_fuse_server.slurper.offsets import EventRecord, write_event
from slack_fuse_server.snapshot.generator import SnapshotResult, generate_snapshot
from tests.conftest import ServerConnFactory

_CLIENT_MIGRATIONS_DIR = Path(client_migrations.__file__).parent
type ClientConnFactory = Callable[[], psycopg.Connection[TupleRow]]


@pytest.fixture
def client_conn_factory(database_url: str) -> Iterator[ClientConnFactory]:
    schema = f"sf_client_{uuid.uuid4().hex}"
    opened: list[psycopg.Connection[TupleRow]] = []
    admin: psycopg.Connection[TupleRow] = psycopg.connect(database_url)
    with admin.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    admin.commit()

    def make() -> psycopg.Connection[TupleRow]:
        conn: psycopg.Connection[TupleRow] = psycopg.connect(database_url)
        with conn.cursor() as cur:
            cur.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
        conn.commit()
        conn.autocommit = True
        opened.append(conn)
        return conn

    setup = make()
    apply_migrations(setup, _CLIENT_MIGRATIONS_DIR)
    setup.commit()

    try:
        yield make
    finally:
        for conn in opened:
            conn.close()
        with admin.cursor() as cur:
            cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
        admin.commit()
        admin.close()


@dataclass(frozen=True, slots=True)
class _StaticMetricsSource:
    payload: MetricsResponse

    def snapshot(self) -> MetricsResponse:
        return self.payload


def _sample_metrics() -> MetricsResponse:
    now = datetime(2026, 6, 8, 21, 0, 0, tzinfo=UTC)
    return MetricsResponse(
        server_started_at=now,
        slack=SlackMetrics(
            socket_mode_state="connected",
            last_event_at=now,
            rate_limit_budget=RateLimitBudget(remaining_pct=92),
            last_health_kind="slack_healthy",
        ),
        streams=[],
        backfill=BackfillMetrics(completed_count=0, aborted_count=0),
        subscribers=SubscribersMetrics(active_ws_connections=0),
    )


def _database_url_for_conn(conn: psycopg.Connection[TupleRow]) -> str:
    with conn.cursor() as cur:
        cur.execute("SELECT current_schema()")
        row = cur.fetchone()
    assert row is not None
    schema = str(row[0])
    return make_conninfo(conn.info.dsn, options=f"-c search_path={schema}")


def _write_message(conn: psycopg.Connection[TupleRow], stream: str, ts: str, text: str) -> None:
    payload: JsonObject = Message.model_validate({"ts": ts, "user": "U1", "text": text}).model_dump(mode="json")
    offset = write_event(conn, EventRecord(stream=stream, kind="message", ts=ts, payload=payload, dedup=False))
    assert offset is not None


def _must_generate_snapshot(conn: psycopg.Connection[TupleRow], stream: str) -> SnapshotResult:
    result = generate_snapshot(conn, stream, trigger="manual")
    assert result is not None
    return result


def _decode_snapshot_body(body: bytes) -> list[JsonObject]:
    raw = gzip.decompress(body).decode("utf-8")
    if not raw:
        return []
    rows: list[JsonObject] = []
    for line in raw.splitlines():
        parsed = json.loads(line)
        assert isinstance(parsed, dict)
        rows.append(cast("JsonObject", parsed))
    return rows


def _snapshot_use(conn: psycopg.Connection[TupleRow]) -> tuple[int, int, int]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT snapshot_at_offset, client_since_offset, events_skipped "
            "FROM snapshot_uses ORDER BY used_at DESC, snapshot_at_offset DESC LIMIT 1"
        )
        row = cur.fetchone()
    assert row is not None
    return (int(row[0]), int(row[1]), int(row[2]))


def _count_chunks(conn: psycopg.Connection[TupleRow], channel_id: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM chunks WHERE channel_id = %s", (channel_id,))
        row = cur.fetchone()
    return 0 if row is None else int(row[0])


def _cursor(conn: psycopg.Connection[TupleRow], stream: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT applied_offset FROM cursors WHERE stream = %s", (stream,))
        row = cur.fetchone()
    return 0 if row is None else int(row[0])


@asynccontextmanager
async def _running_http(
    metrics_source: MetricsSource,
    snapshot_deps: SnapshotDeps,
):
    listeners = await trio.open_tcp_listeners(0, host="127.0.0.1")
    sockname = cast(tuple[str, int], listeners[0].socket.getsockname())
    port = sockname[1]
    async with trio.open_nursery() as nursery:
        nursery.start_soon(serve_http_on_listeners, listeners, metrics_source, None, snapshot_deps)
        await trio.sleep(0.05)
        try:
            yield f"http://127.0.0.1:{port}"
        finally:
            nursery.cancel_scope.cancel()


def test_snapshot_route_returns_gzip_jsonl_and_records_snapshot_use(server_conn_factory: ServerConnFactory) -> None:
    server_conn = server_conn_factory()
    stream = "channel:C0"
    _write_message(server_conn, stream, "100.000001", "one")
    _write_message(server_conn, stream, "100.000002", "two")
    snapshot = _must_generate_snapshot(server_conn, stream)

    response = route_request(
        HttpRequest(
            method="GET",
            target=f"/streams/{quote(stream, safe='')}/snapshot?at={snapshot.at_offset}&since=1",
        ),
        metrics_source=cast(MetricsSource, _StaticMetricsSource(_sample_metrics())),
        snapshot_deps=SnapshotDeps(database_url=_database_url_for_conn(server_conn)),
    )

    assert response.status_code == 200
    assert response.content_type == SNAPSHOT_CONTENT_TYPE
    assert ("content-encoding", SNAPSHOT_CONTENT_ENCODING) in response.headers

    rows = _decode_snapshot_body(response.body)
    assert [row["text"] for row in rows] == ["one", "two"]

    snapshot_at, since, skipped = _snapshot_use(server_conn)
    assert snapshot_at == snapshot.at_offset
    assert since == 1
    assert skipped == snapshot.at_offset - 1


def test_snapshot_route_rounds_down_to_latest_snapshot(server_conn_factory: ServerConnFactory) -> None:
    server_conn = server_conn_factory()
    stream = "channel:C0"

    _write_message(server_conn, stream, "200.000001", "one")
    _write_message(server_conn, stream, "200.000002", "two")
    first = _must_generate_snapshot(server_conn, stream)

    _write_message(server_conn, stream, "200.000003", "three")
    _write_message(server_conn, stream, "200.000004", "four")
    second = _must_generate_snapshot(server_conn, stream)
    assert first.at_offset == 2 and second.at_offset == 4

    response = route_request(
        HttpRequest(
            method="GET",
            target=f"/streams/{quote(stream, safe='')}/snapshot?at=3&since=0",
        ),
        metrics_source=cast(MetricsSource, _StaticMetricsSource(_sample_metrics())),
        snapshot_deps=SnapshotDeps(database_url=_database_url_for_conn(server_conn)),
    )
    assert response.status_code == 200

    rows = _decode_snapshot_body(response.body)
    assert [row["text"] for row in rows] == ["one", "two"]

    snapshot_at, since, skipped = _snapshot_use(server_conn)
    assert snapshot_at == first.at_offset
    assert since == 0
    assert skipped == first.at_offset


def test_snapshot_route_returns_404_when_no_snapshot_at_or_before_at(
    server_conn_factory: ServerConnFactory,
) -> None:
    server_conn = server_conn_factory()
    stream = "channel:C0"
    _write_message(server_conn, stream, "300.000001", "one")
    _must_generate_snapshot(server_conn, stream)

    response = route_request(
        HttpRequest(
            method="GET",
            target=f"/streams/{quote(stream, safe='')}/snapshot?at=0",
        ),
        metrics_source=cast(MetricsSource, _StaticMetricsSource(_sample_metrics())),
        snapshot_deps=SnapshotDeps(database_url=_database_url_for_conn(server_conn)),
    )
    assert response.status_code == 404
    assert response.body == b'{"error":"not_found"}'


@pytest.mark.trio
async def test_snapshot_endpoint_integrates_with_projector_fetch(
    server_conn_factory: ServerConnFactory,
    client_conn_factory: ClientConnFactory,
) -> None:
    server_conn = server_conn_factory()
    stream = "channel:CSNAP"

    _write_message(server_conn, stream, "400.000001", "one")
    _write_message(server_conn, stream, "400.000002", "two")
    _write_message(server_conn, stream, "400.000003", "three")
    snapshot = _must_generate_snapshot(server_conn, stream)

    snapshot_deps = SnapshotDeps(database_url=_database_url_for_conn(server_conn))
    redirect = SnapshotRedirect(
        stream=stream,
        at_offset=snapshot.at_offset,
        url=f"/streams/{quote(stream, safe='')}/snapshot?at={snapshot.at_offset}&since=0",
    )

    projector_conn = client_conn_factory()
    async with (
        _running_http(cast(MetricsSource, _StaticMetricsSource(_sample_metrics())), snapshot_deps) as base_url,
        httpx.AsyncClient(base_url=base_url, timeout=30.0) as http,
    ):
        result = await fetch_and_apply_snapshot(http, projector_conn, redirect)

    assert result.stream == stream
    assert result.at_offset == snapshot.at_offset
    assert result.records_applied == 3
    assert _count_chunks(projector_conn, "CSNAP") == 3
    assert _cursor(projector_conn, stream) == snapshot.at_offset
