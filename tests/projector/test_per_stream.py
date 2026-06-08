"""Per-stream applier tests — including the HoL (head-of-line) acceptance test.

The acceptance criterion: 1000 events to stream A with a 1ms-per-apply hook +
100 events to stream B with no hook → B's chunks appear before A's complete.

We don't actually fire 1100 SQL-roundtrips per test (Postgres roundtrips
dominate; the test gets slow). Instead we use the applier's `before_apply`
hook to inject a configurable delay per event on stream A only, and assert the
*ordering* invariant directly: B finishes before A.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from decimal import Decimal

import psycopg
import trio
from psycopg.rows import TupleRow

from slack_fuse.projector.per_stream import ProjectorMessage, StreamApplier
from slack_fuse_server.wire.frames import CaughtUpFrame
from tests._synthetic_events import channel_message_events
from tests.projector.conftest import ClientConnFactory, RecordingSink


def _count_chunks(conn: psycopg.Connection[TupleRow], channel_id: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM chunks WHERE channel_id = %s", (channel_id,))
        row = cur.fetchone()
    return 0 if row is None else int(row[0])


def test_applier_applies_events_and_advances_cursor(client_conn_factory: ClientConnFactory) -> None:
    """The applier task drains its queue and writes chunks + advances the cursor."""
    sink = RecordingSink()

    async def body() -> None:
        applier = StreamApplier("channel:CSA", client_conn_factory, sink)
        async with trio.open_nursery() as nursery:
            await nursery.start(applier.serve)
            for event in channel_message_events("CSA", 5, start_offset=1):
                await applier.enqueue(event.to_frame())
            # Wait for the queue to drain.
            with trio.fail_after(5.0):
                while applier.queue_depth > 0:
                    await trio.sleep(0.01)
                # Allow the final SQL roundtrip to complete.
                while applier.health().applied_offset < 5:
                    await trio.sleep(0.01)
            await applier.close()

    trio.run(body)

    # Verify final state in a fresh connection.
    verify_conn = client_conn_factory()
    assert _count_chunks(verify_conn, "CSA") == 5
    with verify_conn.cursor() as cur:
        cur.execute("SELECT applied_offset FROM cursors WHERE stream = 'channel:CSA'")
        row = cur.fetchone()
    assert row is not None and int(row[0]) == 5
    # InvalidationSink fired once per chunk.
    assert len(sink.chunks) == 5


def test_caught_up_frame_inserts_stream_caught_up(client_conn_factory: ClientConnFactory) -> None:
    """A `CaughtUpFrame` enqueued on the applier results in `stream_caught_up` insert."""

    async def body() -> None:
        applier = StreamApplier("channel:CSC", client_conn_factory)
        async with trio.open_nursery() as nursery:
            await nursery.start(applier.serve)
            await applier.enqueue(CaughtUpFrame(stream="channel:CSC", head_offset=42))
            with trio.fail_after(5.0):
                while applier.health().caught_up_at_offset != 42:
                    await trio.sleep(0.01)
            await applier.close()

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
    """The acceptance test: stream A is throttled per-event; stream B must
    drain BEFORE stream A even though A was enqueued first.

    Implementation: a `before_apply` hook on A injects `trio.sleep(slow_s)`
    per event. B's applier runs concurrently in the same nursery, so its
    fast roundtrips outpace A's throttled ones.
    """
    sink = RecordingSink()
    # Tighter knobs than the spec (1000/100) so the test stays under a few
    # seconds while still showing the ordering property with margin: A sleeps
    # 5ms per event * 50 events = ~250ms minimum, B does 20 events with no
    # delay and finishes in well under that.
    slow_s = 0.005
    a_count = 50
    b_count = 20

    async def body() -> None:
        a_applier = StreamApplier(
            "channel:CA",
            client_conn_factory,
            sink,
            before_apply=_slow_hook(slow_s),
        )
        b_applier = StreamApplier("channel:CB", client_conn_factory, sink)

        async with trio.open_nursery() as nursery:
            await nursery.start(a_applier.serve)
            await nursery.start(b_applier.serve)

            # Enqueue A first — adversarial order: if HoL existed it would
            # punish B.
            for event in channel_message_events("CA", a_count, start_offset=1):
                await a_applier.enqueue(event.to_frame())
            for event in channel_message_events("CB", b_count, start_offset=1):
                await b_applier.enqueue(event.to_frame())

            b_started = time.monotonic()
            # Wait for B to finish.
            with trio.fail_after(30.0):
                while b_applier.health().applied_offset < b_count:
                    await trio.sleep(0.005)
            b_done = time.monotonic() - b_started
            # Core HoL-prevention invariant: when B completes, A is still
            # mid-flight. If HoL existed, the WS receiver would have been
            # gated on A's per-event sleeps, and B would only run after A
            # finished — A.applied_offset would be a_count.
            a_progress_when_b_done = a_applier.health().applied_offset
            assert a_progress_when_b_done < a_count, (
                f"A should still be applying when B finishes, but "
                f"A.applied={a_progress_when_b_done}/{a_count} — HoL suspected"
            )
            # And B should genuinely run concurrently with A — not get
            # delayed behind A's full serial-sleep budget.
            a_min_serial_s = a_count * slow_s
            assert b_done < a_min_serial_s, (
                f"B took {b_done:.3f}s, A's serial-sleep floor is {a_min_serial_s:.3f}s — "
                f"if B finished only after A's sleeps, HoL is the most likely cause."
            )

            # Now wait for A to drain.
            with trio.fail_after(30.0):
                while a_applier.health().applied_offset < a_count:
                    await trio.sleep(0.005)
            await a_applier.close()
            await b_applier.close()

    trio.run(body)

    verify_conn = client_conn_factory()
    assert _count_chunks(verify_conn, "CA") == a_count
    assert _count_chunks(verify_conn, "CB") == b_count


_ = Decimal  # keep the import alive for typed tests further down
