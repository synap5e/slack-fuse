"""Composable probe registry contracts shared by the single slurper sweep."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import IntEnum, StrEnum
from typing import TYPE_CHECKING, Protocol

import trio

from slack_fuse_server._json import JsonObject
from slack_fuse_server.slurper.api import SlackClient
from slack_fuse_server.slurper.limiters import SlurperLimiters
from slack_fuse_server.slurper.offsets import EventRecord, OffsetWriter, assign_offset, insert_event
from slack_fuse_server.slurper.spans import SpanRecorder

if TYPE_CHECKING:
    import psycopg
    from psycopg import Connection
    from psycopg.rows import TupleRow


class SlackTier(IntEnum):
    """Slack Web API rate-limit tier used by a probe kind."""

    TIER_1 = 1
    TIER_2 = 2
    TIER_3 = 3
    TIER_4 = 4


class ProbeScope(StrEnum):
    """Target/cardinality shape exposed to the unified sweep and control API."""

    WORKSPACE = "workspace"
    PER_CHANNEL = "per_channel"


@dataclass(frozen=True, slots=True)
class ProbeTarget:
    """One restart-safe scheduling key for a probe kind."""

    value: str
    payload_field: str | None = None

    def span_extra(self) -> JsonObject:
        if self.payload_field is None:
            return {"target": self.value}
        return {self.payload_field: self.value}


type ProbeClock = Callable[[], datetime]
type ProbeSleep = Callable[[float], Awaitable[None]]


def utc_now() -> datetime:
    """Return an aware UTC timestamp; injectable for deterministic cycles."""
    return datetime.now(UTC)


def probe_timestamp(value: datetime) -> str:
    """Format a probe-fact timestamp so textual ``MAX(ts)`` is chronological."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("probe timestamps must be timezone-aware")
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class ProbeDeps:
    """Dependencies shared by one sweep and every registered probe kind."""

    client: SlackClient
    writer: OffsetWriter
    limiters: SlurperLimiters
    clock: ProbeClock = utc_now
    sleep: ProbeSleep = trio.sleep
    recorder: SpanRecorder | None = None


type ProbeRunFn = Callable[[ProbeDeps, ProbeTarget], Awaitable[Sequence[EventRecord]]]
type ProbeTargeter = Callable[[ProbeDeps], Awaitable[Sequence[ProbeTarget]]]
type ProbeDueSync = Callable[[Connection[TupleRow], ProbeTarget, float, datetime], bool]


class ProbeSink(Protocol):
    """Persistence policy for one purpose-specific family of probe results."""

    async def write(
        self,
        deps: ProbeDeps,
        probe: ProbeKind,
        records: Sequence[EventRecord],
    ) -> int: ...


@dataclass(frozen=True, slots=True)
class ProbeKind:
    """One independently scheduled unit in the unified probe registry."""

    job_id: str
    kind: str
    interval_s: float
    tier: SlackTier
    scope: ProbeScope
    run: ProbeRunFn
    targets: ProbeTargeter
    due: ProbeDueSync
    sink: ProbeSink
    op: str
    cadence_config_field: str | None = None
    manual_triggerable: bool = True

    def __post_init__(self) -> None:
        if not self.job_id:
            raise ValueError("probe job id must not be empty")
        if not self.kind:
            raise ValueError("probe event kind must not be empty")
        if self.interval_s <= 0:
            raise ValueError("probe interval must be positive")

    @property
    def event_kind(self) -> str:
        """Compatibility/readability alias used by existing telemetry."""
        return self.kind

    @property
    def cadence_s(self) -> float:
        """Compatibility/readability alias used by existing due checks."""
        return self.interval_s

    @property
    def is_per_target(self) -> bool:
        """Whether the manual control surface may specify a channel target."""
        return self.scope is ProbeScope.PER_CHANNEL


@dataclass(frozen=True, slots=True)
class SlurperHealthSink:
    """Append raw detection samples to the singleton health stream."""

    stream: str = "slurper-health"

    async def write(
        self,
        deps: ProbeDeps,
        probe: ProbeKind,
        records: Sequence[EventRecord],
    ) -> int:
        written = 0
        for record in records:
            if record.stream != self.stream:
                msg = f"health probe {probe.job_id!r} returned stream {record.stream!r}"
                raise ValueError(msg)
            if record.kind != probe.kind:
                msg = f"health probe {probe.job_id!r} returned event kind {record.kind!r}"
                raise ValueError(msg)
            offset = await deps.writer.write_event(record, span=deps.recorder)
            if offset is not None:
                written += 1
        if deps.recorder is not None:
            deps.recorder.set("events_written", written)
        return written


@dataclass(frozen=True, slots=True)
class EventFactsSink:
    """Atomically append interpreted probe facts to their normal event streams."""

    async def write(
        self,
        deps: ProbeDeps,
        probe: ProbeKind,
        records: Sequence[EventRecord],
    ) -> int:
        frozen = tuple(records)
        for record in frozen:
            if record.kind != probe.kind:
                msg = f"fact probe {probe.job_id!r} returned event kind {record.kind!r}"
                raise ValueError(msg)
            if record.ts is None:
                msg = f"fact probe {probe.job_id!r} returned an event without a cadence timestamp"
                raise ValueError(msg)
            if record.dedup:
                msg = f"fact probe {probe.job_id!r} returned a deduplicated event"
                raise ValueError(msg)
        written = await deps.writer.run_transaction(
            lambda conn: _write_records_sync(conn, frozen),
            span=deps.recorder,
        )
        if deps.recorder is not None:
            deps.recorder.set("events_written", written)
        return written


def _write_records_sync(
    conn: psycopg.Connection[TupleRow],
    records: Sequence[EventRecord],
) -> int:
    """Append a fact batch inside ``OffsetWriter``'s outer transaction."""
    written = 0
    with conn.cursor() as cur:
        for record in records:
            offset = assign_offset(cur, record.stream)
            if not insert_event(cur, offset, record):
                raise RuntimeError(f"unexpected duplicate probe event {record.kind!r}")
            written += 1
    return written


def validate_registry(registry: Sequence[ProbeKind]) -> tuple[ProbeKind, ...]:
    """Freeze a registry and reject ambiguous duplicate job ids."""
    frozen = tuple(registry)
    job_ids = [probe.job_id for probe in frozen]
    if len(job_ids) != len(set(job_ids)):
        raise ValueError("probe registry contains duplicate job ids")
    return frozen


__all__ = [
    "EventFactsSink",
    "ProbeDeps",
    "ProbeDueSync",
    "ProbeKind",
    "ProbeRunFn",
    "ProbeScope",
    "ProbeSink",
    "ProbeTarget",
    "ProbeTargeter",
    "SlackTier",
    "SlurperHealthSink",
    "probe_timestamp",
    "utc_now",
    "validate_registry",
]
