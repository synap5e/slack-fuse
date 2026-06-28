"""Read-side helpers for auto-backfill state.

The slurper health stream is append-only event history. Auto-backfill uses
these helpers to decide whether a channel has already completed a full
backfill and can be skipped on restart.

To force a channel re-walk, run with
``auto_backfill_skip_if_completed=false``. Do not delete event-log rows except
as manual database repair; deleting ``slurper-health`` rows violates the
event-sourcing contract.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import psycopg
import trio
from psycopg.rows import TupleRow

from slack_fuse_server.slurper.offsets import OffsetWriter


@dataclass(frozen=True, slots=True)
class BackfillCompletion:
    at: datetime
    events_written: int


def find_last_backfill_completion(
    conn: psycopg.Connection[TupleRow],
    channel_id: str,
) -> BackfillCompletion | None:
    """Return the latest completed auto/manual backfill event for ``channel_id``."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT created_at, (payload->>'events_written')::int
            FROM events
            WHERE stream = 'slurper-health'
              AND kind = 'backfill_completed'
              AND payload->>'channel_id' = %s
            ORDER BY id DESC
            LIMIT 1
            """,
            (channel_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    created_at, events_written = row
    if not isinstance(created_at, datetime):  # pragma: no cover - schema guarantees TIMESTAMPTZ.
        msg = f"expected datetime created_at for backfill completion, got {type(created_at).__name__}"
        raise TypeError(msg)
    return BackfillCompletion(at=created_at, events_written=0 if events_written is None else int(events_written))


async def async_find_last_backfill_completion(
    writer: OffsetWriter,
    channel_id: str,
) -> BackfillCompletion | None:
    """Async wrapper for ``find_last_backfill_completion`` using the writer limiter."""
    return await trio.to_thread.run_sync(
        lambda: find_last_backfill_completion(writer.conn, channel_id),
        limiter=writer.limiter,
    )
