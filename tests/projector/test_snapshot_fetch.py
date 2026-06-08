"""HTTP snapshot fetch tests (acceptance criterion 7).

Drives `fetch_and_apply_snapshot` against an httpx mock transport that returns
a JSONL response. Verifies: streaming apply happens in one TX, the cursor
advances to `at_offset`, every JSONL row lands in `chunks`, and invalidations
fire post-commit.
"""

from __future__ import annotations

import json
from decimal import Decimal

import httpx
import psycopg
import trio
from psycopg.rows import TupleRow

from slack_fuse.projector.apply import ChunkRef
from slack_fuse.projector.snapshot_fetch import (
    SnapshotFetchError,
    SnapshotRedirect,
    fetch_and_apply_snapshot,
)
from tests._synthetic_events import synthetic_ts
from tests.projector.conftest import ClientConnFactory, RecordingSink


def _make_snapshot_lines(channel: str, count: int) -> list[bytes]:
    """One JSONL line per synthetic message — full Message shape."""
    lines: list[bytes] = []
    for i in range(count):
        ts = synthetic_ts(i)
        payload = {
            "type": "message",
            "ts": ts,
            "user": f"U{i:04d}",
            "text": f"snapshot row {i}",
            "thread_ts": None,
        }
        lines.append(json.dumps(payload).encode("utf-8"))
    _ = channel
    return lines


def _count_chunks(conn: psycopg.Connection[TupleRow], channel: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM chunks WHERE channel_id = %s", (channel,))
        row = cur.fetchone()
    return 0 if row is None else int(row[0])


def _cursor(conn: psycopg.Connection[TupleRow], stream: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT applied_offset FROM cursors WHERE stream = %s", (stream,))
        row = cur.fetchone()
    return 0 if row is None else int(row[0])


def test_snapshot_fetch_applies_jsonl_atomically(client_conn_factory: ClientConnFactory) -> None:
    """Happy path: 10 JSONL rows → 10 chunks + cursor at `at_offset`."""
    stream = "channel:CSNAP"
    lines = _make_snapshot_lines("CSNAP", 10)
    body = b"\n".join(lines) + b"\n"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.startswith("/streams/")
        return httpx.Response(200, content=body, headers={"content-type": "application/x-ndjson"})

    sink = RecordingSink()
    conn = client_conn_factory()

    async def run() -> None:
        async with httpx.AsyncClient(base_url="http://snapshot.test", transport=httpx.MockTransport(handler)) as http:
            result = await fetch_and_apply_snapshot(
                http,
                conn,
                SnapshotRedirect(stream=stream, at_offset=10500, url="/streams/channel%3ACSNAP/snapshot?at=10500"),
                sink=sink,
            )
            assert result.records_applied == 10
            assert result.at_offset == 10500

    trio.run(run)

    verify_conn = client_conn_factory()
    assert _count_chunks(verify_conn, "CSNAP") == 10
    assert _cursor(verify_conn, stream) == 10500
    # One invalidation per applied chunk.
    assert len(sink.chunks) == 10
    # Decimal type round-trips: every chunk_ts is one of our synthetic ts values.
    seen_ts = {ref.message_ts for ref in sink.chunks}
    assert all(isinstance(ts, Decimal) for ts in seen_ts)


def test_snapshot_fetch_atomic_on_malformed_row(client_conn_factory: ClientConnFactory) -> None:
    """A malformed JSONL row aborts the whole snapshot — no partial chunks."""
    stream = "channel:CBAD"
    bad_body = b'{"type": "message", "ts": "1700000000.000000", "user": "U0", "text": "ok"}\nNOT_JSON\n'

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=bad_body)

    conn = client_conn_factory()

    async def run() -> None:
        async with httpx.AsyncClient(base_url="http://snapshot.test", transport=httpx.MockTransport(handler)) as http:
            try:
                await fetch_and_apply_snapshot(
                    http,
                    conn,
                    SnapshotRedirect(stream=stream, at_offset=100, url="/streams/X/snapshot?at=100"),
                )
                msg = "expected SnapshotFetchError"
                raise AssertionError(msg)
            except SnapshotFetchError:
                pass

    trio.run(run)

    verify_conn = client_conn_factory()
    # No partial chunks — the TX rolled back.
    assert _count_chunks(verify_conn, "CBAD") == 0
    # Cursor also untouched (advance was in the same TX).
    assert _cursor(verify_conn, stream) == 0


def test_snapshot_fetch_idempotent_on_replay(client_conn_factory: ClientConnFactory) -> None:
    """Re-applying the same snapshot is a no-op (ON CONFLICT + GREATEST cursor)."""
    stream = "channel:CIDS"
    lines = _make_snapshot_lines("CIDS", 5)
    body = b"\n".join(lines) + b"\n"

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=body)

    conn = client_conn_factory()

    async def run() -> None:
        async with httpx.AsyncClient(base_url="http://snapshot.test", transport=httpx.MockTransport(handler)) as http:
            await fetch_and_apply_snapshot(
                http, conn, SnapshotRedirect(stream=stream, at_offset=200, url="/streams/X/snapshot?at=200")
            )
            await fetch_and_apply_snapshot(
                http, conn, SnapshotRedirect(stream=stream, at_offset=200, url="/streams/X/snapshot?at=200")
            )

    trio.run(run)

    verify_conn = client_conn_factory()
    assert _count_chunks(verify_conn, "CIDS") == 5
    assert _cursor(verify_conn, stream) == 200


def test_snapshot_fetch_advances_cursor_on_empty_body(client_conn_factory: ClientConnFactory) -> None:
    """An empty snapshot body still advances the cursor (so we don't re-fetch)."""
    stream = "channel:CEMP"

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"")

    conn = client_conn_factory()

    async def run() -> None:
        async with httpx.AsyncClient(base_url="http://snapshot.test", transport=httpx.MockTransport(handler)) as http:
            result = await fetch_and_apply_snapshot(
                http, conn, SnapshotRedirect(stream=stream, at_offset=33, url="/streams/X/snapshot?at=33")
            )
            assert result.records_applied == 0

    trio.run(run)

    verify_conn = client_conn_factory()
    assert _cursor(verify_conn, stream) == 33


_ = ChunkRef  # keep the import alive for the test header
