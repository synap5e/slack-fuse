"""Offset-assignment transaction: sequencing, dedup, and concurrency.

Acceptance criterion 6: under concurrent writers to one stream, offsets are
gap-free and unique. The concurrency test uses *separate connections in real
threads* so it exercises the `stream_heads` row lock, not just the
single-connection serialization the production slurper happens to use.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import psycopg
import pytest
import trio
from psycopg.rows import TupleRow

from slack_fuse_server.slurper.limiters import SlurperLimiters
from slack_fuse_server.slurper.offsets import EventRecord, OffsetWriter, WriterPoolExhausted, write_event
from slack_fuse_server.slurper.spans import span
from tests.conftest import ServerConnFactory, make_test_writer


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


def _set_session_timeouts(
    conn: psycopg.Connection[TupleRow],
    *,
    lock_timeout_s: float,
    statement_timeout_s: float,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT set_config('lock_timeout', %s, false), set_config('statement_timeout', %s, false)",
            (f"{int(lock_timeout_s * 1000)}ms", f"{int(statement_timeout_s * 1000)}ms"),
        )


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


def test_lock_timeout_fires_when_stream_head_row_is_locked(server_conn_factory: ServerConnFactory) -> None:
    stream = "channel:LOCKED"
    locker_conn = server_conn_factory()
    writer_conn = server_conn_factory()
    _set_session_timeouts(writer_conn, lock_timeout_s=0.1, statement_timeout_s=5.0)

    assert write_event(locker_conn, EventRecord(stream=stream, kind="message", ts="1.0", payload={"ts": "1.0"})) == 1

    with locker_conn.transaction(), locker_conn.cursor() as cur:
        cur.execute("SELECT next_offset FROM stream_heads WHERE stream = %s FOR UPDATE", (stream,))
        assert cur.fetchone() is not None

        start = time.monotonic()
        with pytest.raises(psycopg.errors.LockNotAvailable):
            write_event(
                writer_conn,
                EventRecord(stream=stream, kind="message", ts="2.0", payload={"ts": "2.0"}),
            )
        elapsed = time.monotonic() - start

    assert elapsed < 0.75


def test_statement_timeout_fires_for_slow_query(server_conn_factory: ServerConnFactory) -> None:
    writer_conn = server_conn_factory()
    _set_session_timeouts(writer_conn, lock_timeout_s=5.0, statement_timeout_s=0.1)

    start = time.monotonic()
    with pytest.raises(psycopg.errors.QueryCanceled), writer_conn.cursor() as cur:
        cur.execute("SELECT pg_sleep(0.5)")
    elapsed = time.monotonic() - start

    assert elapsed < 0.75


def test_offset_writer_rejects_limiter_pool_size_mismatch(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    with pytest.raises(ValueError, match="limiter size must match"):
        OffsetWriter([server_conn], limiter=trio.CapacityLimiter(2), acquire_timeout_s=1.0)


def test_offset_writer_conn_and_limiter_attributes_are_removed(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    writer = make_test_writer(server_conn)

    with pytest.raises(AttributeError):
        object.__getattribute__(writer, "conn")
    with pytest.raises(AttributeError):
        object.__getattribute__(writer, "limiter")


def test_pooled_writer_concurrent_same_stream_no_gaps_or_duplicates(
    server_conn_factory: ServerConnFactory,
) -> None:
    writer = make_test_writer(server_conn_factory(), server_conn_factory())
    stream = "channel:POOL-HOT"
    total = 30

    async def body() -> list[int]:
        offsets: list[int] = []
        send_offsets, receive_offsets = trio.open_memory_channel[int](total)

        async def worker(i: int) -> None:
            ts = f"2000.{i:06d}"
            record = EventRecord(stream=stream, kind="message", ts=ts, payload={"ts": ts}, dedup=True)
            offset = await writer.write_event(record)
            assert offset is not None
            await send_offsets.send(offset)

        async with trio.open_nursery() as nursery:
            for i in range(total):
                nursery.start_soon(worker, i)
            async with receive_offsets:
                async for offset in receive_offsets:
                    offsets.append(offset)
                    if len(offsets) == total:
                        break
            nursery.cancel_scope.cancel()
        return offsets

    all_offsets = sorted(trio.run(body))
    assert all_offsets == list(range(1, total + 1))


def test_one_held_pool_transaction_does_not_block_other_writer_connection(
    server_conn_factory: ServerConnFactory,
) -> None:
    writer = make_test_writer(server_conn_factory(), server_conn_factory(), acquire_timeout_s=0.05)
    entered = trio.Event()
    release = trio.Event()

    async def body() -> int | None:
        async def hold_transaction() -> None:
            async with writer.acquire_transaction():
                entered.set()
                await release.wait()

        async with trio.open_nursery() as nursery:
            nursery.start_soon(hold_transaction)
            await entered.wait()
            offset = await writer.write_event(
                EventRecord(stream="channel:POOL-FREE", kind="message", ts="1.0", payload={"ts": "1.0"})
            )
            release.set()
            nursery.cancel_scope.cancel()
            return offset

    assert trio.run(body) == 1


def test_writer_pool_exhaustion_times_out_when_all_connections_held(
    server_conn_factory: ServerConnFactory,
) -> None:
    pool_size = 2
    writer = make_test_writer(
        *(server_conn_factory() for _ in range(pool_size)),
        acquire_timeout_s=0.05,
    )

    async def body() -> float:
        started_send, started_receive = trio.open_memory_channel[None](pool_size)
        release = trio.Event()

        async def hold_connection() -> None:
            async with writer.acquire_read():
                await started_send.send(None)
                await release.wait()

        async with trio.open_nursery() as nursery:
            for _ in range(pool_size):
                nursery.start_soon(hold_connection)
            for _ in range(pool_size):
                await started_receive.receive()

            start = trio.current_time()
            with pytest.raises(WriterPoolExhausted):
                async with writer.acquire_read():
                    pass
            elapsed = trio.current_time() - start

            release.set()
            nursery.cancel_scope.cancel()
            return elapsed

    assert trio.run(body) < 0.5


def test_slack_api_limiter_wait_does_not_block_writer_pool(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    writer = make_test_writer(server_conn)
    limiters = SlurperLimiters(
        slack_api=trio.CapacityLimiter(1),
        writer=trio.CapacityLimiter(1),
        snapshot=trio.CapacityLimiter(1),
        admin_read=trio.CapacityLimiter(1),
    )
    started = threading.Event()
    release = threading.Event()

    def _slow_slack_api() -> None:
        started.set()
        release.wait()

    async def body() -> int | None:
        async def hold_slack_api_token() -> None:
            await trio.to_thread.run_sync(_slow_slack_api, limiter=limiters.slack_api)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(hold_slack_api_token)
            await trio.to_thread.run_sync(started.wait)
            offset = await writer.write_event(
                EventRecord(stream="channel:LIMITERS", kind="message", ts="1.0", payload={"ts": "1.0"})
            )
            release.set()
            nursery.cancel_scope.cancel()
            return offset

    assert trio.run(body) == 1


def test_run_read_and_transaction_populate_span_timings(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    writer = make_test_writer(server_conn)
    limiter = trio.CapacityLimiter(1)

    def _read(_conn: psycopg.Connection[TupleRow]) -> int:
        time.sleep(0.005)
        return 42

    def _write(conn: psycopg.Connection[TupleRow]) -> int:
        time.sleep(0.005)
        with conn.cursor() as cur:
            cur.execute("SELECT 7")
            row = cur.fetchone()
        assert row is not None
        return int(row[0])

    async def body() -> tuple[tuple[int, int], tuple[int, int]]:
        async with span(op="slurper.test.run_read", task="unit") as read_span:
            assert await writer.run_read(_read, limiter=limiter, span=read_span) == 42
            read_timings = (read_span.limiter_wait_ms, read_span.sync_ms)
        async with span(op="slurper.test.run_transaction", task="unit") as tx_span:
            assert await writer.run_transaction(_write, span=tx_span) == 7
            tx_timings = (tx_span.limiter_wait_ms, tx_span.sync_ms)
        return read_timings, tx_timings

    read_timings, tx_timings = trio.run(body)

    assert read_timings[0] >= 0
    assert read_timings[1] > 0
    assert tx_timings[0] >= 0
    assert tx_timings[1] > 0


def test_pool_acquire_methods_accept_optional_span(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    writer = make_test_writer(server_conn)

    async def body() -> tuple[tuple[int, int], tuple[int, int]]:
        async with span(op="slurper.test.acquire_read", task="unit") as read_span:
            async with writer.acquire_read(span=read_span) as conn:
                assert conn is server_conn
            read_timings = (read_span.limiter_wait_ms, read_span.sync_ms)
        async with span(op="slurper.test.acquire_transaction", task="unit") as tx_span:
            async with writer.acquire_transaction(span=tx_span) as conn:
                assert conn is server_conn
            tx_timings = (tx_span.limiter_wait_ms, tx_span.sync_ms)
        return read_timings, tx_timings

    read_timings, tx_timings = trio.run(body)

    assert read_timings[0] >= 0
    assert read_timings[1] == 0
    assert tx_timings[0] >= 0
    assert tx_timings[1] >= 0


def test_writer_methods_preserve_span_none_behavior(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    writer = make_test_writer(server_conn)

    async def body() -> tuple[int, int | None]:
        read_value = await writer.run_read(lambda _conn: 5, limiter=trio.CapacityLimiter(1), span=None)
        offset = await writer.write_event(
            EventRecord(stream="channel:SPAN-NONE", kind="message", ts="1.0", payload={"ts": "1.0"}),
            span=None,
        )
        return read_value, offset

    assert trio.run(body) == (5, 1)
