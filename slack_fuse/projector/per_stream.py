"""Per-stream applier task: queue + worker that applies events serially.

Per RFC §Wire protocol → Flow control. One applier task per subscribed stream,
each owning a bounded `trio.MemoryReceiveChannel`. Different streams' appliers
run concurrently — postgres handles disjoint-PK writes cheaply — so a slow
apply on stream A cannot stall live events for stream B.

Connection ownership (review P0-A). Appliers do **not** own a postgres
connection. They borrow one from a shared bounded `ConnectionPool` for the
duration of a single event apply and return it immediately, so the projector
opens ~`pool_size` connections regardless of how many channels are subscribed.
Each `apply_event` is its own TX (chunk INSERT + chunk_mentions INSERT + cursor
advance), and the applier processes its queue one message at a time, so
borrowing a fresh connection per event preserves in-stream ordering.

Queue / head-of-line (review P1-E). The per-stream queue is **unbounded** and
`enqueue` is non-blocking (`send_nowait`). The WS receive loop can therefore
route every frame without ever blocking on a slow stream's backpressure — a
saturated stream A can never stop stream B's events from being read off the
socket. A persistently-overflowing stream is surfaced via a logged soft-cap
warning and the `queue_depth` health metric rather than by blocking the socket.

Failure policy (review P1-D). If `apply_event` raises, the applier does **not**
advance its cursor and does **not** swallow the error: it raises
`StreamApplyError` out of `serve()`, tearing the WS client down so reconnect
resumes from the last durable cursor and replays the failed offset. Continuing
to drain later offsets after a failed one would let a later success advance the
cursor *past* the failure, dropping it forever — the event-sourced projection
must never silently diverge from the log.
"""

from __future__ import annotations

import functools
import logging
import math
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Final, Protocol

import trio
from psycopg import Connection
from psycopg.rows import TupleRow

from slack_fuse.projector.apply import (
    ApplyResult,
    InvalidationSink,
    NullInvalidationSink,
    apply_event,
    record_caught_up,
)
from slack_fuse_server.wire.frames import CaughtUpFrame, EventFrame

log = logging.getLogger(__name__)


#: Soft cap on the per-stream queue depth. The queue itself is unbounded (P1-E),
#: but crossing this depth logs a one-shot warning so a persistently-slow
#: applier shows up in the logs/metrics instead of silently growing memory.
DEFAULT_QUEUE_SOFT_CAP: Final = 256


#: A pre-event or pre-catchup message routed to a per-stream applier.
type ProjectorMessage = EventFrame | CaughtUpFrame


#: How the pool acquires a postgres connection (also the pool's own factory).
type ConnectionFactory = Callable[[], Connection[TupleRow]]


class ConnectionLease(Protocol):
    """Structural type for the bounded connection pool the appliers borrow from.

    `slack_fuse.projector.pool.ConnectionPool` satisfies this. Declared here
    (rather than imported) to avoid a cycle: `pool` imports `ConnectionFactory`
    from this module.
    """

    async def acquire(self) -> Connection[TupleRow]: ...
    async def release(self, conn: Connection[TupleRow], *, discard: bool = ...) -> None: ...


class StreamApplyError(Exception):
    """An event could not be applied; the stream must not advance past it.

    Carries the offending stream + offset for the supervisor's logs. Raising
    this out of `serve()` tears the WS client down so reconnect resumes from the
    durable cursor and replays the failed offset (review P1-D).
    """

    def __init__(self, stream: str, offset: int) -> None:
        super().__init__(f"failed to apply {stream} offset={offset}")
        self.stream = stream
        self.offset = offset


@dataclass(frozen=True, slots=True)
class StreamHealth:
    """A snapshot of the per-stream queue + cursor state for `/metrics` style reads."""

    stream: str
    queue_depth: int
    last_routed_offset: int
    applied_offset: int
    caught_up_at_offset: int | None


class StreamApplier:
    """Owns one stream's applier task and its inbound queue.

    Usage:

        applier = StreamApplier(stream, pool, sink)
        async with trio.open_nursery() as nursery:
            await nursery.start(applier.serve)
            await applier.enqueue(frame)
            ...
            await applier.close()
    """

    def __init__(  # noqa: PLR0913  (keyword-only config + test-injection knobs)
        self,
        stream: str,
        pool: ConnectionLease,
        sink: InvalidationSink | None = None,
        *,
        queue_soft_cap: int = DEFAULT_QUEUE_SOFT_CAP,
        before_apply: Callable[[ProjectorMessage], Awaitable[None]] | None = None,
        always_blocked: frozenset[str] = frozenset(),
    ) -> None:
        self.stream = stream
        self._pool = pool
        self._sink: InvalidationSink = sink if sink is not None else NullInvalidationSink()
        self._always_blocked = always_blocked
        # Unbounded queue (P1-E): send_nowait never blocks the WS receive loop.
        self._send, self._receive = trio.open_memory_channel[ProjectorMessage](math.inf)
        self._soft_cap = queue_soft_cap
        self._warned_overflow = False
        self._last_routed_offset = 0
        self._applied_offset = 0
        self._caught_up_at: int | None = None
        # Optional hook (tests): awaited before each event apply. Used to
        # simulate slow appliers without touching the SQL path.
        self._before_apply = before_apply

    @property
    def queue_depth(self) -> int:
        return self._send.statistics().current_buffer_used

    def health(self) -> StreamHealth:
        return StreamHealth(
            stream=self.stream,
            queue_depth=self.queue_depth,
            last_routed_offset=self._last_routed_offset,
            applied_offset=self._applied_offset,
            caught_up_at_offset=self._caught_up_at,
        )

    async def enqueue(self, message: ProjectorMessage) -> None:
        """Route a frame into the applier queue without blocking the caller.

        The queue is unbounded, so `send_nowait` can only fail if the applier
        has been closed — backpressure on stream A therefore never stalls the
        WS receive loop that stream B's events depend on (review P1-E)."""
        if isinstance(message, EventFrame):
            self._last_routed_offset = max(self._last_routed_offset, message.offset)
        self._send.send_nowait(message)
        depth = self.queue_depth
        if depth > self._soft_cap and not self._warned_overflow:
            log.warning(
                "applier %s: queue depth %d exceeds soft cap %d — slow applier or catch-up burst",
                self.stream,
                depth,
                self._soft_cap,
            )
            self._warned_overflow = True
        elif depth <= self._soft_cap:
            self._warned_overflow = False

    async def serve(self, *, task_status: trio.TaskStatus[None] = trio.TASK_STATUS_IGNORED) -> None:
        """Drain the queue until close. Borrows a pooled connection per event."""
        task_status.started()
        async for message in self._receive:
            await self._handle(message)

    async def close(self) -> None:
        await self._send.aclose()

    # === internals ===

    async def _handle(self, message: ProjectorMessage) -> None:
        if self._before_apply is not None:
            await self._before_apply(message)
        if isinstance(message, EventFrame):
            await self._apply_event_frame(message)
        else:
            await self._record_caught_up_frame(message)

    async def _apply_event_frame(self, message: EventFrame) -> None:
        conn = await self._pool.acquire()
        try:
            result = await trio.to_thread.run_sync(
                functools.partial(apply_event, conn, message, always_blocked=self._always_blocked)
            )
        except Exception as exc:
            await self._pool.release(conn, discard=True)
            # P1-D: do NOT advance the cursor past a failed offset. Poison the
            # stream so the WS client tears down and reconnect replays it.
            log.exception(
                "applier %s: failed to apply offset=%d kind=%s — poisoning stream",
                self.stream,
                message.offset,
                message.kind,
            )
            raise StreamApplyError(self.stream, message.offset) from exc
        except BaseException:
            await self._pool.release(conn, discard=True)
            raise
        await self._pool.release(conn)
        self._applied_offset = max(self._applied_offset, message.offset)
        # Invalidations call ``pyfuse3.invalidate_inode`` which can block on
        # writeback. Calling it from the trio event-loop thread can deadlock
        # against in-flight FUSE reads (the kernel is mid-read of an inode
        # we're trying to invalidate; invalidate waits for the read; the read
        # needs the event loop). Dispatch to a worker thread — matches v1's
        # InodeInvalidator threading contract (see fuse_ops.py:678-685).
        # 2026-06-24 wedge: this used to run inline → folio_wait_bit_common.
        await trio.to_thread.run_sync(self._fire_invalidations, result)

    async def _record_caught_up_frame(self, message: CaughtUpFrame) -> None:
        conn = await self._pool.acquire()
        try:
            await trio.to_thread.run_sync(record_caught_up, conn, message.stream, message.head_offset)
        except Exception as exc:
            await self._pool.release(conn, discard=True)
            log.exception(
                "applier %s: failed to record caught_up at %d — poisoning stream",
                self.stream,
                message.head_offset,
            )
            raise StreamApplyError(self.stream, message.head_offset) from exc
        except BaseException:
            await self._pool.release(conn, discard=True)
            raise
        await self._pool.release(conn)
        self._caught_up_at = max(self._caught_up_at or 0, message.head_offset)

    def _fire_invalidations(self, result: ApplyResult) -> None:
        for ref in result.chunks:
            self._sink.chunk_changed(ref)
        for thread_ref in result.thread_chunks:
            self._sink.thread_chunk_changed(thread_ref)
        if result.channel_list_changed:
            self._sink.channel_list_changed()
