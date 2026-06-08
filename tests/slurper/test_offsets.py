"""Offset-assignment transaction: sequencing, dedup, and concurrency.

Acceptance criterion 6: under concurrent writers to one stream, offsets are
gap-free and unique. The concurrency test uses *separate connections in real
threads* so it exercises the `stream_heads` row lock, not just the
single-connection serialization the production slurper happens to use.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import psycopg
from psycopg.rows import TupleRow

from slack_fuse_server.slurper.offsets import EventRecord, write_event
from tests.conftest import ServerConnFactory


def _count(conn: psycopg.Connection[TupleRow], stream: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM events WHERE stream = %s", (stream,))
        row = cur.fetchone()
    assert row is not None
    return int(row[0])


def _next_offset(conn: psycopg.Connection[TupleRow], stream: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT next_offset FROM stream_heads WHERE stream = %s", (stream,))
        row = cur.fetchone()
    assert row is not None
    return int(row[0])


def test_sequential_offsets_start_at_one(server_conn: psycopg.Connection[TupleRow]) -> None:
    for i in range(3):
        record = EventRecord(stream="users", kind="user_added", ts=None, payload={"i": i})
        assert write_event(server_conn, record) == i + 1
    assert _count(server_conn, "users") == 3
    assert _next_offset(server_conn, "users") == 4


def test_message_dedup_is_noop_without_offset_gap(server_conn: psycopg.Connection[TupleRow]) -> None:
    stream = "channel:C1"
    first = EventRecord(stream=stream, kind="message", ts="100.0001", payload={"ts": "100.0001"}, dedup=True)
    assert write_event(server_conn, first) == 1

    # Same ts again — no row written, no offset consumed.
    dup = EventRecord(stream=stream, kind="message", ts="100.0001", payload={"ts": "100.0001", "x": 1}, dedup=True)
    assert write_event(server_conn, dup) is None

    # The next distinct message gets offset 2, not 3 — the bump was rolled back.
    second = EventRecord(stream=stream, kind="message", ts="101.0002", payload={"ts": "101.0002"}, dedup=True)
    assert write_event(server_conn, second) == 2

    assert _count(server_conn, stream) == 2
    assert _next_offset(server_conn, stream) == 3


def test_non_message_events_are_not_deduped(server_conn: psycopg.Connection[TupleRow]) -> None:
    # reaction-style events legitimately repeat; only `message` carries the index.
    stream = "channel:C2"
    one = EventRecord(stream=stream, kind="reaction_added", ts="5.0", payload={"ts": "5.0"})
    two = EventRecord(stream=stream, kind="reaction_added", ts="5.0", payload={"ts": "5.0"})
    assert write_event(server_conn, one) == 1
    assert write_event(server_conn, two) == 2
    assert _count(server_conn, stream) == 2


def test_write_persists_after_a_prior_bare_read(server_conn_factory: ServerConnFactory) -> None:
    # Regression: a bare read before the write (as the backfill-override lookup
    # does) must not turn the write's transaction into a savepoint that's lost
    # on close. Verify durability through a *separate* connection.
    writer_conn = server_conn_factory()
    with writer_conn.cursor() as cur:  # bare read, no explicit transaction
        cur.execute("SELECT max_messages FROM backfill_overrides WHERE channel_id = %s", ("CX",))
        assert cur.fetchone() is None

    record = EventRecord(stream="channel:CX", kind="message", ts="1.0", payload={"ts": "1.0"}, dedup=True)
    assert write_event(writer_conn, record) == 1

    reader_conn = server_conn_factory()
    assert _count(reader_conn, "channel:CX") == 1


def test_concurrent_writers_no_gaps_or_duplicates(server_conn_factory: ServerConnFactory) -> None:
    per_worker = 25
    workers = 4
    stream = "channel:HOT"

    def worker(worker_id: int) -> list[int]:
        conn = server_conn_factory()
        offsets: list[int] = []
        for j in range(per_worker):
            ts = f"{worker_id}.{j:06d}"
            record = EventRecord(stream=stream, kind="message", ts=ts, payload={"ts": ts}, dedup=True)
            off = write_event(conn, record)
            assert off is not None
            offsets.append(off)
        return offsets

    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(worker, range(workers)))

    all_offsets = sorted(off for sub in results for off in sub)
    assert all_offsets == list(range(1, workers * per_worker + 1))
