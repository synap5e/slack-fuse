"""Per-stream applier tests.

Covers:

- the happy path (drain queue → chunks + cursor advance);
- the `CaughtUpFrame` → `stream_caught_up` insert;
- the head-of-line invariant at the applier level (a slow stream A must not
  delay a fast stream B);
- the bounded-connection invariant (review P0-A): N appliers share a small
  pool, never opening one connection per stream;
- the failed-apply poison invariant (review P1-D): a failed offset must not be
  skipped by a later successful offset.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from typing import cast

import psycopg
import pytest
import trio
from psycopg.rows import TupleRow

from slack_fuse.projector import per_stream as per_stream_module
from slack_fuse.projector.per_stream import ProjectorMessage, StreamApplier, StreamApplyError
from slack_fuse.projector.pool import ConnectionPool
from slack_fuse_server.wire.frames import CaughtUpFrame, EventFrame
from tests._synthetic_events import channel_message_events
from tests.projector.conftest import ClientConnFactory, RecordingSink


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


def _flatten(exc: BaseException) -> list[BaseException]:
    if isinstance(exc, BaseExceptionGroup):
        nested = cast("tuple[BaseException, ...]", exc.exceptions)
        out: list[BaseException] = []
        for sub in nested:
            out.extend(_flatten(sub))
        return out
    return [exc]


def test_applier_applies_events_and_advances_cursor(client_conn_factory: ClientConnFactory) -> None:
    """The applier task drains its queue and writes chunks + advances the cursor."""
    sink = RecordingSink()
    pool = ConnectionPool(client_conn_factory)

    async def body() -> None:
        applier = StreamApplier("channel:CSA", pool, sink)
        async with trio.open_nursery() as nursery:
            await nursery.start(applier.serve)
            for event in channel_message_events("CSA", 5, start_offset=1):
                await applier.enqueue(event.to_frame())
            with trio.fail_after(5.0):
                while applier.queue_depth > 0:
                    await trio.sleep(0.01)
                while applier.health().applied_offset < 5:
                    await trio.sleep(0.01)
            await applier.close()
        await pool.aclose()

    trio.run(body)

    verify_conn = client_conn_factory()
    assert _count_chunks(verify_conn, "CSA") == 5
    assert _cursor(verify_conn, "channel:CSA") == 5
    assert len(sink.chunks) == 5


def test_health_seconds_since_last_apply(
    client_conn_factory: ClientConnFactory,
) -> None:
    """``StreamHealth.seconds_since_last_apply`` is ``None`` before the first apply
    and non-``None`` after — the primary signal the straggler watchdog reads.
    """
    pool = ConnectionPool(client_conn_factory)

    async def body() -> None:
        applier = StreamApplier("channel:CTIMER", pool)
        assert applier.health().seconds_since_last_apply is None
        async with trio.open_nursery() as nursery:
            await nursery.start(applier.serve)
            await applier.enqueue(next(iter(channel_message_events("CTIMER", 1, start_offset=1))).to_frame())
            with trio.fail_after(5.0):
                while applier.health().applied_offset < 1:
                    await trio.sleep(0.01)
            idle_immediately = applier.health().seconds_since_last_apply
            assert idle_immediately is not None
            assert idle_immediately >= 0.0
            await trio.sleep(0.1)
            idle_later = applier.health().seconds_since_last_apply
            assert idle_later is not None
            assert idle_later > idle_immediately, "seconds_since_last_apply must grow while idle"
            await applier.close()
        await pool.aclose()

    trio.run(body)


def test_slow_apply_logs_warning_with_split_timing(
    client_conn_factory: ClientConnFactory,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A single event apply that breaches SLOW_APPLY_S logs a WARN with the
    per-phase breakdown (acquire_ms + sync_ms) so post-hoc the operator can
    tell pool contention from SQL/render cost.

    Set SLOW_APPLY_S to ~0 so any real apply trips it. The test doesn't care
    about the ABSOLUTE timing, just that the log line carries the split.
    """
    monkeypatch.setattr(per_stream_module, "SLOW_APPLY_S", 0.0)
    pool = ConnectionPool(client_conn_factory)
    caplog.set_level("WARNING", logger="slack_fuse.projector.per_stream")

    async def body() -> None:
        applier = StreamApplier("channel:CSLOW", pool)
        async with trio.open_nursery() as nursery:
            await nursery.start(applier.serve)
            await applier.enqueue(next(iter(channel_message_events("CSLOW", 1, start_offset=1))).to_frame())
            with trio.fail_after(5.0):
                while applier.health().applied_offset < 1:
                    await trio.sleep(0.01)
            await applier.close()
        await pool.aclose()

    trio.run(body)
    messages = [rec.getMessage() for rec in caplog.records if "slow apply" in rec.getMessage()]
    assert messages, "expected one slow-apply warning"
    text = messages[0]
    assert "channel:CSLOW" in text
    assert "acquire_ms=" in text
    assert "sync_ms=" in text
    assert "kind=" in text


def test_caught_up_frame_inserts_stream_caught_up(client_conn_factory: ClientConnFactory) -> None:
    """A `CaughtUpFrame` enqueued on the applier results in `stream_caught_up` insert."""
    pool = ConnectionPool(client_conn_factory)

    async def body() -> None:
        applier = StreamApplier("channel:CSC", pool)
        async with trio.open_nursery() as nursery:
            await nursery.start(applier.serve)
            await applier.enqueue(CaughtUpFrame(stream="channel:CSC", head_offset=42))
            with trio.fail_after(5.0):
                while applier.health().caught_up_at_offset != 42:
                    await trio.sleep(0.01)
            await applier.close()
        await pool.aclose()

    trio.run(body)

    verify_conn = client_conn_factory()
    with verify_conn.cursor() as cur:
        cur.execute("SELECT at_offset FROM stream_caught_up WHERE stream = 'channel:CSC'")
        row = cur.fetchone()
    assert row is not None and int(row[0]) == 42


def _slow_hook(delay_s: float) -> Callable[[ProjectorMessage], Awaitable[None]]:
    async def hook(_: ProjectorMessage) -> None:
        await trio.sleep(delay_s)

    return hook


def test_per_stream_no_hol_blocking(client_conn_factory: ClientConnFactory) -> None:
    """Stream A is throttled per-event; stream B must drain BEFORE A even though
    A was enqueued first. Appliers share a pool but run concurrently."""
    sink = RecordingSink()
    pool = ConnectionPool(client_conn_factory)
    slow_s = 0.005
    a_count = 50
    b_count = 20

    async def body() -> None:
        a_applier = StreamApplier("channel:CA", pool, sink, before_apply=_slow_hook(slow_s))
        b_applier = StreamApplier("channel:CB", pool, sink)

        async with trio.open_nursery() as nursery:
            await nursery.start(a_applier.serve)
            await nursery.start(b_applier.serve)

            for event in channel_message_events("CA", a_count, start_offset=1):
                await a_applier.enqueue(event.to_frame())
            for event in channel_message_events("CB", b_count, start_offset=1):
                await b_applier.enqueue(event.to_frame())

            b_started = time.monotonic()
            with trio.fail_after(30.0):
                while b_applier.health().applied_offset < b_count:
                    await trio.sleep(0.005)
            b_done = time.monotonic() - b_started
            a_progress_when_b_done = a_applier.health().applied_offset
            assert a_progress_when_b_done < a_count, (
                f"A should still be applying when B finishes, but "
                f"A.applied={a_progress_when_b_done}/{a_count} — HoL suspected"
            )
            a_min_serial_s = a_count * slow_s
            assert b_done < a_min_serial_s, (
                f"B took {b_done:.3f}s, A's serial-sleep floor is {a_min_serial_s:.3f}s — "
                f"if B finished only after A's sleeps, HoL is the most likely cause."
            )

            with trio.fail_after(30.0):
                while a_applier.health().applied_offset < a_count:
                    await trio.sleep(0.005)
            await a_applier.close()
            await b_applier.close()
        await pool.aclose()

    trio.run(body)

    verify_conn = client_conn_factory()
    assert _count_chunks(verify_conn, "CA") == a_count
    assert _count_chunks(verify_conn, "CB") == b_count


def test_pool_bounds_connections_under_many_streams(client_conn_factory: ClientConnFactory) -> None:
    """Review P0-A regression: 300 subscribed streams must not open 300+ DB
    connections. With a factory that *fails* after the pool cap, startup +
    apply must still succeed because the appliers share the bounded pool.

    On the pre-fix code (each applier opened its own connection in `serve()`),
    the 9th applier's startup would hit the failing factory and tear the nursery
    down — this test guards that the connection count stays bounded by the pool.
    """
    num_streams = 300
    cap = 8
    calls = {"n": 0}

    def limited_factory() -> psycopg.Connection[TupleRow]:
        calls["n"] += 1
        if calls["n"] > cap:
            msg = "simulated max_connections exhaustion"
            raise psycopg.OperationalError(msg)
        return client_conn_factory()

    pool = ConnectionPool(limited_factory, max_size=cap)

    async def body() -> None:
        appliers = [StreamApplier(f"channel:C{i:04d}", pool) for i in range(num_streams)]
        async with trio.open_nursery() as nursery:
            for applier in appliers:
                await nursery.start(applier.serve)
            for i, applier in enumerate(appliers):
                event = next(iter(channel_message_events(f"C{i:04d}", 1, start_offset=1)))
                await applier.enqueue(event.to_frame())
            with trio.fail_after(120.0):
                while any(a.health().applied_offset < 1 for a in appliers):
                    await trio.sleep(0.02)
            for applier in appliers:
                await applier.close()
        await pool.aclose()

    trio.run(body)

    # The whole point: the factory was never called more than the pool cap,
    # even though 300 streams were subscribed and all applied an event.
    assert calls["n"] <= cap
    assert pool.connections_created <= cap

    verify_conn = client_conn_factory()
    assert _count_chunks(verify_conn, "C0000") == 1
    assert _count_chunks(verify_conn, "C0299") == 1


def test_applier_failure_poisons_stream_without_skipping_offset(
    client_conn_factory: ClientConnFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review P1-D regression: if offset 42 fails to apply, the cursor must NOT
    advance to 43 by way of a later successful event. The applier raises
    `StreamApplyError` so the WS client tears down and reconnect replays 42.

    On the pre-fix code (`except Exception: log; return`), offset 41 applied,
    42 was logged + skipped, 43 applied and advanced the cursor to 43 — losing
    42 forever. This test asserts the cursor stays at 41 and the stream poisons.
    """
    real_apply = per_stream_module.apply_event

    def failing_apply(conn: psycopg.Connection[TupleRow], frame: EventFrame):
        if frame.offset == 42:
            msg = "simulated transient apply failure"
            raise RuntimeError(msg)
        return real_apply(conn, frame)

    monkeypatch.setattr(per_stream_module, "apply_event", failing_apply)

    pool = ConnectionPool(client_conn_factory)
    captured: list[BaseException] = []

    async def body() -> None:
        applier = StreamApplier("channel:CFAIL", pool)
        try:
            async with trio.open_nursery() as nursery:
                await nursery.start(applier.serve)
                # Offsets 41, 42, 43 (42 will fail to apply).
                for event in channel_message_events("CFAIL", 3, start_offset=41, start_index=41):
                    await applier.enqueue(event.to_frame())
                with trio.fail_after(10.0):
                    while True:
                        await trio.sleep(0.02)
        except BaseException as exc:
            captured.append(exc)
        finally:
            await pool.aclose()

    trio.run(body)

    poison = [e for c in captured for e in _flatten(c) if isinstance(e, StreamApplyError)]
    assert poison, f"expected StreamApplyError, got {captured!r}"
    assert poison[0].offset == 42

    verify_conn = client_conn_factory()
    # 41 applied; 42 failed; 43 never processed (stream died at 42).
    assert _cursor(verify_conn, "channel:CFAIL") == 41
    assert _count_chunks(verify_conn, "CFAIL") == 1


def test_acquire_discards_stale_cached_connection_after_pg_restart(
    client_conn_factory: ClientConnFactory,
) -> None:
    """Postgres-restart regression: a cached idle connection that PG has killed
    must be discarded + replaced on acquire, not handed to a borrower who then
    sees ``OperationalError: the connection is lost`` on their first SQL.

    Simulates the failure mode the projector hit when local PG restarted while
    the FUSE mount was live — every cached pool connection was poisoned and the
    mount started returning EIO until the service was bounced.
    """
    pool = ConnectionPool(client_conn_factory)

    async def body() -> None:
        # 1. Acquire + release → one connection in the idle list.
        conn = await pool.acquire()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        await pool.release(conn)
        # 2. Kill the cached connection out-of-band (mirrors PG restart).
        assert len(pool._idle) == 1  # pyright: ignore[reportPrivateUsage]
        cached = pool._idle[0]  # pyright: ignore[reportPrivateUsage]
        cached.close()
        # 3. Acquire again: must NOT return the dead connection.
        fresh = await pool.acquire()
        assert fresh is not cached, "pool handed back a dead connection"
        # And the fresh connection actually works.
        with fresh.cursor() as cur:
            cur.execute("SELECT 1")
            row = cur.fetchone()
        assert row == (1,)
        await pool.release(fresh)
        await pool.aclose()

    trio.run(body)
