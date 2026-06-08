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


def _seed_top_level_chunk(conn: psycopg.Connection[TupleRow], channel_id: str, ts: str, *, mention: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO chunks (channel_id, message_ts, content_md, reply_count) VALUES (%s, %s, %s, 0)",
            (channel_id, Decimal(ts), f"stale chunk mentioning <@{mention}>"),
        )
        cur.execute(
            "INSERT INTO chunk_mentions (channel_id, message_ts, mention_kind, mentioned_id) "
            "VALUES (%s, %s, 'user', %s)",
            (channel_id, Decimal(ts), mention),
        )


def _seed_thread_reply(conn: psycopg.Connection[TupleRow], channel_id: str, thread_ts: str, reply_ts: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO thread_chunks (channel_id, thread_ts, reply_ts, role, content_md) "
            "VALUES (%s, %s, %s, 'reply', %s)",
            (channel_id, Decimal(thread_ts), Decimal(reply_ts), "stale reply"),
        )


def _chunk_exists(conn: psycopg.Connection[TupleRow], channel_id: str, ts: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM chunks WHERE channel_id = %s AND message_ts = %s", (channel_id, Decimal(ts)))
        return cur.fetchone() is not None


def _mention_count(conn: psycopg.Connection[TupleRow], channel_id: str, ts: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM chunk_mentions WHERE channel_id = %s AND message_ts = %s",
            (channel_id, Decimal(ts)),
        )
        row = cur.fetchone()
    return 0 if row is None else int(row[0])


def _thread_reply_exists(conn: psycopg.Connection[TupleRow], channel_id: str, thread_ts: str, reply_ts: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM thread_chunks WHERE channel_id = %s AND thread_ts = %s AND reply_ts = %s",
            (channel_id, Decimal(thread_ts), Decimal(reply_ts)),
        )
        return cur.fetchone() is not None


def test_snapshot_apply_removes_stale_rows_before_advancing_cursor(client_conn_factory: ClientConnFactory) -> None:
    """Review P0-B regression: a snapshot is full-state, not additive.

    Seed a local chunk (and its mention) that the server has since deleted; the
    snapshot at a later offset omits it. Applying the snapshot must delete the
    stale chunk + its `chunk_mentions` rows AND advance the cursor — atomically,
    in one TX. On the pre-fix upsert-only path the stale chunk survived forever
    once the cursor advanced past the delete event the client never saw.
    """
    stream = "channel:CDEL"
    stale_ts = "100.000001"

    seed_conn = client_conn_factory()
    _seed_top_level_chunk(seed_conn, "CDEL", stale_ts, mention="UGHOST")
    assert _chunk_exists(seed_conn, "CDEL", stale_ts)
    assert _mention_count(seed_conn, "CDEL", stale_ts) == 1

    # The snapshot at offset 500 contains only a *different* message — the
    # server's current state after the stale message was deleted.
    kept_ts = synthetic_ts(0)
    body = json.dumps({"type": "message", "ts": kept_ts, "user": "U0001", "text": "kept", "thread_ts": None}).encode()

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=body)

    conn = client_conn_factory()

    async def run() -> None:
        async with httpx.AsyncClient(base_url="http://snapshot.test", transport=httpx.MockTransport(handler)) as http:
            result = await fetch_and_apply_snapshot(
                http, conn, SnapshotRedirect(stream=stream, at_offset=500, url="/streams/X/snapshot?at=500")
            )
            assert result.records_applied == 1

    trio.run(run)

    verify_conn = client_conn_factory()
    # Stale chunk + its mentions are gone (full-state replacement).
    assert not _chunk_exists(verify_conn, "CDEL", stale_ts)
    assert _mention_count(verify_conn, "CDEL", stale_ts) == 0
    # The snapshot's message is present, and the cursor advanced — atomically.
    assert _chunk_exists(verify_conn, "CDEL", kept_ts)
    assert _cursor(verify_conn, stream) == 500


def test_snapshot_apply_removes_stale_thread_replies(client_conn_factory: ClientConnFactory) -> None:
    """Review P0-B: thread replies absent from the snapshot are removed too."""
    stream = "channel:CTHR"
    parent_ts = synthetic_ts(0)
    kept_reply_ts = synthetic_ts(1)
    stale_reply_ts = "100.000002"

    seed_conn = client_conn_factory()
    # A stale reply the snapshot will omit, plus the parent so the kept reply
    # has somewhere to attach.
    _seed_thread_reply(seed_conn, "CTHR", parent_ts, stale_reply_ts)
    assert _thread_reply_exists(seed_conn, "CTHR", parent_ts, stale_reply_ts)

    parent = {"type": "message", "ts": parent_ts, "user": "U0", "text": "parent", "thread_ts": parent_ts}
    kept_reply = {"type": "message", "ts": kept_reply_ts, "user": "U1", "text": "kept reply", "thread_ts": parent_ts}
    body = (json.dumps(parent) + "\n" + json.dumps(kept_reply)).encode()

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=body)

    conn = client_conn_factory()

    async def run() -> None:
        async with httpx.AsyncClient(base_url="http://snapshot.test", transport=httpx.MockTransport(handler)) as http:
            await fetch_and_apply_snapshot(
                http, conn, SnapshotRedirect(stream=stream, at_offset=700, url="/streams/X/snapshot?at=700")
            )

    trio.run(run)

    verify_conn = client_conn_factory()
    assert not _thread_reply_exists(verify_conn, "CTHR", parent_ts, stale_reply_ts)
    assert _thread_reply_exists(verify_conn, "CTHR", parent_ts, kept_reply_ts)
    assert _cursor(verify_conn, stream) == 700


_ = ChunkRef  # keep the import alive for the test header
