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
from enum import StrEnum

import trio
from psycopg.types.json import Jsonb

from slack_fuse_server._json import JsonObject
from slack_fuse_server.slurper.offsets import EventRecord, OffsetWriter, assign_offset, insert_event

log = logging.getLogger(__name__)

_HEALTH_STREAM = "slurper-health"


class HealthKind(StrEnum):
    """The allowed `slurper-health` event kinds (RFC §Slurper health stream)."""

    SLACK_HEALTHY = "slack_healthy"
    SLACK_DEGRADED = "slack_degraded"
    SOCKET_MODE_DISCONNECTED = "socket_mode_disconnected"
    SOCKET_MODE_RECONNECTED = "socket_mode_reconnected"
    AUTH_TOKEN_INVALID = "auth_token_invalid"
    BACKFILL_STARTED = "backfill_started"
    BACKFILL_COMPLETED = "backfill_completed"
    BACKFILL_ABORTED = "backfill_aborted"


class HealthEmitter:
    """Writes `slurper-health` events and mirrors them to `health_log`.

    Shares the `OffsetWriter`'s connection + limiter so health writes serialize
    with every other DB write in the single-process slurper.
    """

    def __init__(self, writer: OffsetWriter) -> None:
        self._writer = writer

    def _emit_sync(self, kind: HealthKind, payload: JsonObject) -> int:
        conn = self._writer.conn
        record = EventRecord(stream=_HEALTH_STREAM, kind=str(kind), ts=None, payload=payload)
        with conn.transaction(), conn.cursor() as cur:
            offset = assign_offset(cur, _HEALTH_STREAM)
            insert_event(cur, offset, record)
            cur.execute(
                "INSERT INTO health_log (kind, payload) VALUES (%s, %s)",
                (str(kind), Jsonb(payload)),
            )
        return offset

    async def emit(self, kind: HealthKind, payload: JsonObject | None = None) -> int:
        """Emit one health transition. Returns the assigned `slurper-health` offset."""
        body: JsonObject = payload if payload is not None else {}
        offset = await trio.to_thread.run_sync(
            lambda: self._emit_sync(kind, body),
            limiter=self._writer.limiter,
        )
        log.info("slurper-health: %s %s (offset=%d)", kind, body, offset)
        return offset
