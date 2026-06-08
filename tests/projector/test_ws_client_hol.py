"""Review P1-E regression: the WebSocket *receiver routing path* must not
head-of-line block when one stream's applier is saturated.

The earlier HoL test (`test_per_stream_no_hol_blocking`) exercised two appliers
directly — it never drove the receive loop's `_dispatch_frame`, which is the
actual site where a full per-stream queue used to block the socket. This test
drives `_dispatch_frame` exactly as `_receive_loop` does: it pushes a large run
of frames for a slow stream A, then one frame for stream B, and asserts B is
applied while A is still draining.

On the pre-fix code (bounded per-stream channel + blocking `send`), dispatching
A's frames would block at the channel capacity, so B's frame could not be
routed until A drained below the cap — B would only apply after A had applied
~(N - capacity) events. The unbounded `send_nowait` queue removes that coupling.

The driving logic lives on a `WSClient` subclass so it can touch the receive
path's internals (`_nursery`, `_ensure_applier`, `_dispatch_frame`, `_pool`)
through the protected surface intended for subclasses.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import psycopg
import trio
from psycopg.rows import TupleRow

from slack_fuse.projector.per_stream import ProjectorMessage, StreamApplier
from slack_fuse.projector.ws_client import WSClient, WSClientOptions
from tests._synthetic_events import channel_message_events
from tests.projector.conftest import ClientConnFactory, RecordingSink

_SLOW_STREAM = "channel:HA"
_FAST_STREAM = "channel:HB"
_A_COUNT = 300  # well above the old 256 per-stream channel capacity


def _slow_hook(delay_s: float) -> Callable[[ProjectorMessage], Awaitable[None]]:
    async def hook(_: ProjectorMessage) -> None:
        await trio.sleep(delay_s)

    return hook


class _HoLProbeClient(WSClient):
    """WSClient whose stream-A applier sleeps per event, with a test driver that
    routes frames through the real `_dispatch_frame` path."""

    def _make_applier(self, stream: str) -> StreamApplier:
        hook = _slow_hook(0.02) if stream == _SLOW_STREAM else None
        return StreamApplier(stream, self._pool, self._sink, before_apply=hook)

    async def drive_until_b_applied(self) -> int:
        """Route A's flood then one B frame; return A's progress when B lands."""
        async with trio.open_nursery() as nursery:
            self._nursery = nursery
            a_applier = await self._ensure_applier(_SLOW_STREAM)
            b_applier = await self._ensure_applier(_FAST_STREAM)

            # Mimic the receive loop: route all of A's frames, then one B frame.
            # Post-fix this never blocks (send_nowait into an unbounded queue).
            for event in channel_message_events("HA", _A_COUNT, start_offset=1):
                await self._dispatch_frame(event.to_frame())
            b_event = next(iter(channel_message_events("HB", 1, start_offset=1)))
            await self._dispatch_frame(b_event.to_frame())

            with trio.fail_after(30.0):
                while b_applier.health().applied_offset < 1:
                    await trio.sleep(0.005)
            a_progress = a_applier.health().applied_offset
            nursery.cancel_scope.cancel()
        await self._pool.aclose()
        return a_progress


def test_ws_receiver_routes_fast_stream_past_saturated_slow_stream(
    client_conn_factory: ClientConnFactory,
) -> None:
    sink = RecordingSink()
    state_conn: psycopg.Connection[TupleRow] = client_conn_factory()
    options = WSClientOptions(server_url="ws://unused.test", pool_size=8)
    client = _HoLProbeClient(options, client_conn_factory, state_conn, sink=sink)

    a_progress = trio.run(client.drive_until_b_applied)

    # B applied while A has barely started. Pre-fix, B could only be routed after
    # A drained below the 256 cap, so A would have applied ~44+ events by then.
    assert a_progress < 30, (
        f"B applied only after A reached {a_progress}/{_A_COUNT} — the receiver "
        f"head-of-line blocked on stream A's queue (review P1-E)."
    )
