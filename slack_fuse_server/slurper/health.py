"""The `slurper-health` stream emitter.

Per RFC §Wire protocol → Slurper health stream. The server self-publishes
health transitions on the singleton `slurper-health` stream so clients can
detect "the pipeline upstream of me is broken" without out-of-band signalling.
Every emit does two writes in one transaction:

1. An event on the `slurper-health` stream (offset-assigned like any other
   event), so subscribers receive it on the wire.
2. A row in the `health_log` table, so an operator can `SELECT` health history
   directly without parsing the event log.

Both land atomically — a client that sees the event and an operator reading
`health_log` never disagree.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from enum import StrEnum

import trio
from psycopg import Connection
from psycopg.rows import TupleRow

from slack_fuse_server._json import JsonObject
from slack_fuse_server.slurper.offsets import EventRecord, OffsetWriter, assign_offset, insert_event
from slack_fuse_server.slurper.spans import span

log = logging.getLogger(__name__)

_HEALTH_STREAM = "slurper-health"


class HealthKind(StrEnum):
    """The allowed `slurper-health` event kinds (RFC §Slurper health stream).

    Global-health kinds (drive ``connection_state.last_slurper_health`` on
    the client and the FUSE staleness trailer):
    ``SLACK_HEALTHY``, ``SLACK_DEGRADED``, ``SOCKET_MODE_DISCONNECTED``,
    ``SOCKET_MODE_RECONNECTED``, ``AUTH_TOKEN_INVALID``.

    Per-channel kinds (observability only; never affect the global trailer
    state, because one channel hitting a backfill size cap is not a workspace
    ingestion-health concern):
    ``BACKFILL_STARTED``, ``BACKFILL_PROGRESS``, ``BACKFILL_COMPLETED``,
    ``BACKFILL_ABORTED``, ``BACKFILL_SKIPPED``, ``BACKFILL_WARN_LARGE``.
    """

    SLACK_HEALTHY = "slack_healthy"
    SLACK_DEGRADED = "slack_degraded"
    SOCKET_MODE_DISCONNECTED = "socket_mode_disconnected"
    SOCKET_MODE_RECONNECTED = "socket_mode_reconnected"
    AUTH_TOKEN_INVALID = "auth_token_invalid"
    BACKFILL_STARTED = "backfill_started"
    BACKFILL_PROGRESS = "backfill_progress"
    BACKFILL_COMPLETED = "backfill_completed"
    BACKFILL_ABORTED = "backfill_aborted"
    BACKFILL_SKIPPED = "backfill_skipped"
    BACKFILL_WARN_LARGE = "backfill_warn_large"


class HealthEmitter:
    """Writes `slurper-health` events and mirrors them to `health_log`.

    Shares the `OffsetWriter`'s connection + limiter so health writes serialize
    with every other DB write in the single-process slurper.
    """

    def __init__(self, writer: OffsetWriter) -> None:
        self._writer = writer

    def _emit_sync(self, conn: Connection[TupleRow], kind: HealthKind, payload: JsonObject) -> int:
        # Single write to `events` (stream='slurper-health'). The old
        # `health_log` table was a dual-write of the same data for operator
        # convenience; migration 0005 replaces it with a VIEW over `events`,
        # so operators can still `SELECT … FROM health_log …` without the
        # dual-write anti-pattern. See BACKLOG / ES audit findings.
        record = EventRecord(stream=_HEALTH_STREAM, kind=str(kind), ts=None, payload=payload)
        with conn.cursor() as cur:
            offset = assign_offset(cur, _HEALTH_STREAM)
            insert_event(cur, offset, record)
        return offset

    async def emit(self, kind: HealthKind, payload: JsonObject | None = None) -> int:
        """Emit one health transition. Returns the assigned `slurper-health` offset."""
        body: JsonObject = payload if payload is not None else {}
        async with span(op="slurper.health.emit", task="health", extra={"kind": str(kind)}) as recorder:
            offset = await self._writer.run_transaction(lambda conn: self._emit_sync(conn, kind, body), span=recorder)
            recorder.set("offset", offset)
        log.info("slurper-health: %s %s (offset=%d)", kind, body, offset)
        return offset


class SlackDegradedTracker:
    """Debounces `slack_degraded` so a transient blip doesn't spam the stream.

    A degraded *episode* begins on the first failure and ends when the next
    success is recorded. Within an episode, `slack_degraded` is emitted at most
    once, and only after the episode has persisted for `min_duration_s` — so a
    one-off failure that recovers on the next attempt produces no event, while a
    genuine outage surfaces once it crosses the threshold (RFC §Slurper health
    stream; `slack_degraded_min_duration_s` in `ServerConfig`).

    `clock` is injectable so tests can advance time deterministically; it
    defaults to the trio clock the rest of the slurper measures against.
    """

    def __init__(
        self,
        health: HealthEmitter,
        min_duration_s: float,
        *,
        clock: Callable[[], float] = trio.current_time,
    ) -> None:
        self._health = health
        self._min_duration_s = min_duration_s
        self._clock = clock
        self._degraded_since: float | None = None
        self._emitted = False

    async def record_failure(self, reason: str) -> None:
        """Note a connection-attempt failure; emit once the episode crosses the threshold."""
        now = self._clock()
        if self._degraded_since is None:
            self._degraded_since = now
        if not self._emitted and (now - self._degraded_since) >= self._min_duration_s:
            await self._health.emit(HealthKind.SLACK_DEGRADED, {"reason": reason})
            self._emitted = True

    def record_healthy(self) -> None:
        """End any degraded episode. The caller emits its own recovery transition."""
        self._degraded_since = None
        self._emitted = False
