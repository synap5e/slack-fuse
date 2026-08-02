"""Single-task scheduler for immutable, event-backed probe facts."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import trio

from slack_fuse_server.probes.registry import ProbeDeps, ProbeKind, validate_registry
from slack_fuse_server.slurper.offsets import EventRecord, OffsetWriter, assign_offset, insert_event
from slack_fuse_server.slurper.spans import span
from slack_fuse_server.slurper.supervisor import TaskSupervisor, phase

if TYPE_CHECKING:
    import psycopg
    from psycopg.rows import TupleRow

log = logging.getLogger(__name__)

DEFAULT_SWEEP_INTERVAL_S = 60.0
TASK_NAME = "probe-event-sweep"


@dataclass(frozen=True, slots=True)
class EventsTableProbeCursor:
    """Read per-kind probe cadence from the append-only events table."""

    writer: OffsetWriter
    limiter: trio.CapacityLimiter

    async def last_run_at(self, kind: str) -> datetime | None:
        return await self.writer.run_read(
            lambda conn: _last_run_at_sync(conn, kind),
            limiter=self.limiter,
        )


def _last_run_at_sync(conn: psycopg.Connection[TupleRow], kind: str) -> datetime | None:
    # Probe timestamps use a fixed-width UTC ISO representation, so MAX(text)
    # is chronological without relying on the database session timezone.
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(ts) FROM events WHERE kind = %s", (kind,))
        row = cur.fetchone()
    if row is None or row[0] is None:
        return None
    value = str(row[0])
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"probe event {kind!r} has a timezone-naive ts")
    return parsed.astimezone(UTC)


async def run_probe_sweep(
    supervisor: TaskSupervisor,
    deps: ProbeDeps,
    registry: Sequence[ProbeKind],
    *,
    sweep_interval_s: float = DEFAULT_SWEEP_INTERVAL_S,
) -> None:
    """Walk the registry forever and dispatch every probe whose interval elapsed."""
    if sweep_interval_s <= 0:
        raise ValueError("probe sweep interval must be positive")
    active_registry = validate_registry(registry)
    while True:
        await run_probe_sweep_once(supervisor, deps, active_registry)
        supervisor.declare(TASK_NAME, "sleeping_until", deadline_s=sweep_interval_s * 2)
        await deps.sleep(sweep_interval_s)


async def run_probe_sweep_once(
    supervisor: TaskSupervisor,
    deps: ProbeDeps,
    registry: Sequence[ProbeKind],
) -> None:
    """Run one deterministic registry tick; the long-lived task calls this."""
    for probe in validate_registry(registry):
        try:
            last_run_at = await deps.cursor.last_run_at(probe.kind)
            now = deps.clock()
            if now.tzinfo is None or now.utcoffset() is None:
                raise ValueError("probe clock must return a timezone-aware datetime")
            if last_run_at is not None and (now - last_run_at).total_seconds() < probe.interval_s:
                continue
            async with phase(
                supervisor,
                TASK_NAME,
                probe.kind,
                details={"scope": probe.scope.value, "tier": int(probe.tier)},
                deadline_s=None,
            ):
                await _run_probe(deps, probe)
        except Exception:
            # A failed kind leaves no new event-backed cadence marker, so the
            # next sweep tick retries it. Other registry entries still run.
            log.exception("probe-event-sweep: probe %s failed", probe.kind)


async def _run_probe(deps: ProbeDeps, probe: ProbeKind) -> None:
    async with span(
        op=f"slurper.probe.{probe.kind}",
        task=TASK_NAME,
        extra={"scope": probe.scope.value, "tier": int(probe.tier)},
    ) as recorder:
        recorder.set("events_written", 0)
        recorder.set("outcome", "error")
        records = tuple(await probe.run(replace(deps, recorder=recorder)))
        _validate_records(probe, records)
        events_written = await deps.writer.run_transaction(
            lambda conn: _write_records_sync(conn, records),
            span=recorder,
        )
        recorder.set("events_written", events_written)
        recorder.set("outcome", "ok")


def _validate_records(probe: ProbeKind, records: tuple[EventRecord, ...]) -> None:
    for record in records:
        if record.kind != probe.kind:
            msg = f"probe {probe.kind!r} returned event kind {record.kind!r}"
            raise ValueError(msg)
        if record.ts is None:
            msg = f"probe {probe.kind!r} returned an event without a cadence timestamp"
            raise ValueError(msg)
        if record.dedup:
            msg = f"probe {probe.kind!r} returned a deduplicated event; probe history must be immutable"
            raise ValueError(msg)


def _write_records_sync(
    conn: psycopg.Connection[TupleRow],
    records: tuple[EventRecord, ...],
) -> int:
    """Append one probe result atomically inside ``OffsetWriter``'s transaction."""
    written = 0
    with conn.cursor() as cur:
        for record in records:
            offset = assign_offset(cur, record.stream)
            if not insert_event(cur, offset, record):
                raise RuntimeError(f"unexpected duplicate probe event {record.kind!r}")
            written += 1
    return written


__all__ = [
    "DEFAULT_SWEEP_INTERVAL_S",
    "TASK_NAME",
    "EventsTableProbeCursor",
    "run_probe_sweep",
    "run_probe_sweep_once",
]
