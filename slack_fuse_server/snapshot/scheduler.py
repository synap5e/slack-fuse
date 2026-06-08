"""Periodic snapshot scheduler — a trio task in the slurper nursery.

Cadence (RFC §Snapshot delivery via HTTP, defaults in `ServerConfig`): generate
a fresh snapshot for a stream once it has accrued `snapshot_every_n_events`
events since its last snapshot, OR once `snapshot_max_age_hours` have elapsed,
whichever comes first.

The scheduler owns its own psycopg connection (separate from the slurper's
`OffsetWriter` connection) and its own `CapacityLimiter`, so generating a large
snapshot never blocks live event writes. Each tick:

1. enumerates the projectable streams that have events,
2. asks `decide_trigger` whether each is due, and
3. generates the due ones via `generate_snapshot` (each in its own
   `REPEATABLE READ` transaction).

`decide_trigger` is pure so the cadence is unit-testable without a clock or a
database; `tick` is `await`-able directly so the integration tests can
fast-forward the trigger condition without real time passing.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import psycopg
import trio
from psycopg import Connection
from psycopg.rows import TupleRow

from slack_fuse_server.snapshot.generator import (
    GenerationTrigger,
    SnapshotResult,
    generate_snapshot,
    is_projectable_stream,
)

log = logging.getLogger(__name__)

# How often a tick polls for due streams. Small relative to the 24h time
# trigger and cheap (a handful of indexed counts per stream), so the time
# trigger fires within one poll of crossing the threshold.
_DEFAULT_POLL_INTERVAL_S = 60.0


def decide_trigger(
    new_events: int,
    age_seconds: float | None,
    *,
    every_n_events: int,
    max_age_seconds: float,
) -> GenerationTrigger | None:
    """Decide whether a stream is due for a snapshot, and why.

    `new_events` is the gap between the stream head and its last snapshot;
    `age_seconds` is the age of the oldest not-yet-snapshotted event (or the
    last snapshot), or `None` when the stream has no new events to age.

    Returns the winning trigger, or `None` when neither threshold is crossed.
    Event count wins ties — it is the cheaper, more responsive signal.
    """
    if new_events <= 0:
        return None
    if new_events >= every_n_events:
        return "event_count"
    if age_seconds is not None and age_seconds >= max_age_seconds:
        return "time"
    return None


@dataclass(frozen=True, slots=True)
class _StreamCadence:
    stream: str
    new_events: int
    age_seconds: float | None


_CHANNEL_STREAM_PREFIX = "channel:"


def _snapshot_candidate(stream: str) -> bool:
    """Whether the scheduler should auto-generate a snapshot for `stream`.

    Only `channel:<id>` streams. The `users` / `channel-list` singleton streams
    are small and the WS server always replays them rather than redirecting to a
    snapshot the split client cannot full-state-apply (review P0-C), so
    generating singleton snapshots would just be wasted work.
    """
    return is_projectable_stream(stream) and stream.startswith(_CHANNEL_STREAM_PREFIX)


def _candidate_streams(cur: psycopg.Cursor[TupleRow]) -> list[str]:
    cur.execute("SELECT DISTINCT stream FROM events")
    return sorted(s for (s,) in cur.fetchall() if isinstance(s, str) and _snapshot_candidate(s))


def _stream_cadence(cur: psycopg.Cursor[TupleRow], stream: str) -> _StreamCadence | None:
    """Gather the cadence inputs for one stream, or `None` if it has no events."""
    cur.execute("SELECT max(offset_in_stream) FROM events WHERE stream = %s", (stream,))
    head_row = cur.fetchone()
    head = head_row[0] if head_row is not None else None
    if head is None:
        return None

    cur.execute(
        "SELECT at_offset, created_at FROM snapshots WHERE stream = %s ORDER BY at_offset DESC LIMIT 1",
        (stream,),
    )
    snap_row = cur.fetchone()
    last_offset = int(snap_row[0]) if snap_row is not None else 0
    new_events = int(head) - last_offset
    if new_events <= 0:
        return _StreamCadence(stream=stream, new_events=0, age_seconds=None)

    # Age reference: the last snapshot's timestamp, else the oldest event not
    # yet covered (so a fresh stream's first snapshot fires 24h after its first
    # event, exactly as for a stream that has snapshotted before).
    if snap_row is not None:
        cur.execute("SELECT EXTRACT(EPOCH FROM (now() - %s))", (snap_row[1],))
    else:
        cur.execute(
            "SELECT EXTRACT(EPOCH FROM (now() - min(created_at))) "
            "FROM events WHERE stream = %s AND offset_in_stream > %s",
            (stream, last_offset),
        )
    age_row = cur.fetchone()
    age_seconds = float(age_row[0]) if age_row is not None and age_row[0] is not None else None
    return _StreamCadence(stream=stream, new_events=new_events, age_seconds=age_seconds)


def due_streams(
    conn: Connection[TupleRow],
    *,
    every_n_events: int,
    max_age_seconds: float,
) -> list[tuple[str, GenerationTrigger]]:
    """The streams currently due for a snapshot, paired with their trigger."""
    due: list[tuple[str, GenerationTrigger]] = []
    with conn.transaction(), conn.cursor() as cur:
        for stream in _candidate_streams(cur):
            cadence = _stream_cadence(cur, stream)
            if cadence is None:
                continue
            trigger = decide_trigger(
                cadence.new_events,
                cadence.age_seconds,
                every_n_events=every_n_events,
                max_age_seconds=max_age_seconds,
            )
            if trigger is not None:
                due.append((stream, trigger))
    return due


class SnapshotScheduler:
    """Periodic snapshot worker. Run as `nursery.start_soon(scheduler.run)`."""

    def __init__(
        self,
        conn: Connection[TupleRow],
        *,
        every_n_events: int,
        max_age_seconds: float,
        limiter: trio.CapacityLimiter,
        poll_interval_s: float = _DEFAULT_POLL_INTERVAL_S,
    ) -> None:
        self._conn = conn
        self._every_n_events = every_n_events
        self._max_age_seconds = max_age_seconds
        self._limiter = limiter
        self._poll_interval_s = poll_interval_s

    def _tick_sync(self) -> list[SnapshotResult]:
        due = due_streams(
            self._conn,
            every_n_events=self._every_n_events,
            max_age_seconds=self._max_age_seconds,
        )
        results: list[SnapshotResult] = []
        for stream, trigger in due:
            result = generate_snapshot(self._conn, stream, trigger=trigger)
            if result is not None:
                results.append(result)
                log.info(
                    "snapshot: stream=%s at_offset=%d events_covered=%d payload_bytes=%d trigger=%s (%dms)",
                    result.stream,
                    result.at_offset,
                    result.events_covered,
                    result.payload_bytes,
                    result.generation_trigger,
                    result.generation_duration_ms,
                )
        return results

    async def tick(self) -> list[SnapshotResult]:
        """Run one scheduling pass on a worker thread; return generated snapshots."""
        return await trio.to_thread.run_sync(self._tick_sync, limiter=self._limiter)

    async def run(self) -> None:
        """Loop forever: tick, then sleep one poll interval. Resilient to DB errors."""
        while True:
            try:
                await self.tick()
            except psycopg.Error:
                log.warning("snapshot: scheduling tick failed; retrying next interval", exc_info=True)
            await trio.sleep(self._poll_interval_s)
