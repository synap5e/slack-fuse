"""Per-stream applier task: queue + worker that applies events serially.

Per RFC §Wire protocol → Flow control. One applier task per subscribed stream,
each owning a bounded `trio.MemoryReceiveChannel` and a postgres connection in
autocommit mode. Different streams' appliers run concurrently — postgres
handles disjoint-PK writes cheaply — so a slow apply on stream A cannot stall
live events for stream B.

Each `EventFrame` becomes one TX (chunk INSERT + chunk_mentions INSERT + cursor
advance). After the TX commits, the post-commit invalidations land on the
shared `InvalidationSink` so the FUSE kernel page cache drops affected inodes.
Crash recovery is implicit: the next subscribe sends `since = applied_offset`,
the partial batch replays harmlessly (writes are idempotent), and the cursor
moves forward via `GREATEST`.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Final

import trio
from psycopg import Connection
from psycopg.rows import TupleRow

from slack_fuse.projector.apply import (
    ApplyResult,
    InvalidationSink,
    NullInvalidationSink,
    apply_event,
    record_caught_up,
    require_autocommit,
)
from slack_fuse_server.wire.frames import CaughtUpFrame, EventFrame

log = logging.getLogger(__name__)


#: Cap per per-stream queue. Generous enough that bursts (catch-up batches)
#: don't immediately backpressure, small enough that a misbehaving applier
#: shows up in metrics rather than silently growing memory.
DEFAULT_QUEUE_CAPACITY: Final = 256


#: A pre-event or pre-catchup message routed to a per-stream applier.
type ProjectorMessage = EventFrame | CaughtUpFrame


#: How an applier acquires its postgres connection.
type ConnectionFactory = Callable[[], Connection[TupleRow]]


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

        applier = StreamApplier(stream, connection_factory, sink)
        async with trio.open_nursery() as nursery:
            await nursery.start(applier.serve)
            await applier.enqueue(frame)
            ...
            await applier.close()
    """

    def __init__(
        self,
        stream: str,
        connection_factory: ConnectionFactory,
        sink: InvalidationSink | None = None,
        *,
        queue_capacity: int = DEFAULT_QUEUE_CAPACITY,
        before_apply: Callable[[ProjectorMessage], Awaitable[None]] | None = None,
    ) -> None:
        self.stream = stream
        self._factory = connection_factory
        self._sink: InvalidationSink = sink if sink is not None else NullInvalidationSink()
        self._send, self._receive = trio.open_memory_channel[ProjectorMessage](queue_capacity)
        self._conn: Connection[TupleRow] | None = None
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
        """Route a frame into the applier queue. Blocks the *calling task* if the
        queue is full — the WS receiver should call this from a dedicated
        per-frame router task so backpressure on stream A doesn't stop the
        socket from being drained for stream B."""
        if isinstance(message, EventFrame):
            self._last_routed_offset = max(self._last_routed_offset, message.offset)
        await self._send.send(message)

    async def serve(self, *, task_status: trio.TaskStatus[None] = trio.TASK_STATUS_IGNORED) -> None:
        """Drain the queue until close. Owns its postgres connection."""
        self._conn = self._factory()
        require_autocommit(self._conn)
        task_status.started()
        try:
            async for message in self._receive:
                await self._handle(message)
        finally:
            self._conn.close()
            self._conn = None

    async def close(self) -> None:
        await self._send.aclose()

    # === internals ===

    async def _handle(self, message: ProjectorMessage) -> None:
        if self._before_apply is not None:
            await self._before_apply(message)
        conn = self._conn
        if conn is None:  # pragma: no cover - serve() owns the connection
            return
        if isinstance(message, EventFrame):
            try:
                result = await trio.to_thread.run_sync(apply_event, conn, message)
            except Exception:
                log.exception(
                    "applier %s: failed to apply offset=%d kind=%s",
                    self.stream,
                    message.offset,
                    message.kind,
                )
                return
            self._applied_offset = max(self._applied_offset, message.offset)
            self._fire_invalidations(result)
        else:
            # CaughtUpFrame — drives the trailer logic; not an event apply.
            try:
                await trio.to_thread.run_sync(record_caught_up, conn, message.stream, message.head_offset)
            except Exception:
                log.exception("applier %s: failed to record caught_up at %d", self.stream, message.head_offset)
                return
            self._caught_up_at = max(self._caught_up_at or 0, message.head_offset)

    def _fire_invalidations(self, result: ApplyResult) -> None:
        for ref in result.chunks:
            self._sink.chunk_changed(ref)
        for thread_ref in result.thread_chunks:
            self._sink.thread_chunk_changed(thread_ref)
        if result.channel_list_changed:
            self._sink.channel_list_changed()
