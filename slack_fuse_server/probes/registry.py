"""Typed registry contracts for immutable, periodic probe events."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import IntEnum, StrEnum
from typing import Protocol

import trio

from slack_fuse_server.slurper.api import SlackClient
from slack_fuse_server.slurper.limiters import SlurperLimiters
from slack_fuse_server.slurper.offsets import EventRecord, OffsetWriter
from slack_fuse_server.slurper.spans import SpanRecorder


class SlackTier(IntEnum):
    """Slack Web API rate-limit tier recorded for a probe kind."""

    TIER_1 = 1
    TIER_2 = 2
    TIER_3 = 3
    TIER_4 = 4


class ProbeScope(StrEnum):
    """Cardinality of the facts produced by one successful probe run."""

    WORKSPACE = "workspace"
    PER_CHANNEL = "per_channel"


class ProbeRunCursor(Protocol):
    """Restart-safe access to the latest persisted run of a probe kind."""

    async def last_run_at(self, kind: str) -> datetime | None: ...


type ProbeClock = Callable[[], datetime]
type ProbeSleep = Callable[[float], Awaitable[None]]


def utc_now() -> datetime:
    """Return an aware UTC timestamp; injectable through :class:`ProbeDeps`."""
    return datetime.now(UTC)


def probe_timestamp(value: datetime) -> str:
    """Format a probe fact timestamp so textual ``MAX(ts)`` is chronological."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("probe timestamps must be timezone-aware")
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class ProbeDeps:
    """Dependencies shared by the registry sweep and individual probes."""

    client: SlackClient
    writer: OffsetWriter
    limiters: SlurperLimiters
    cursor: ProbeRunCursor
    clock: ProbeClock = utc_now
    sleep: ProbeSleep = trio.sleep
    recorder: SpanRecorder | None = None


type ProbeRunFn = Callable[[ProbeDeps], Awaitable[Sequence[EventRecord]]]


@dataclass(frozen=True, slots=True)
class ProbeKind:
    """One independently scheduled immutable fact probe."""

    kind: str
    interval_s: float
    tier: SlackTier
    scope: ProbeScope
    run: ProbeRunFn

    def __post_init__(self) -> None:
        if not self.kind:
            raise ValueError("probe event kind must not be empty")
        if self.interval_s <= 0:
            raise ValueError("probe interval must be positive")


def validate_registry(registry: Sequence[ProbeKind]) -> tuple[ProbeKind, ...]:
    """Freeze a registry and reject ambiguous duplicate event kinds."""
    frozen = tuple(registry)
    kinds = [probe.kind for probe in frozen]
    if len(kinds) != len(set(kinds)):
        raise ValueError("probe registry contains duplicate event kinds")
    return frozen


__all__ = [
    "ProbeDeps",
    "ProbeKind",
    "ProbeRunCursor",
    "ProbeRunFn",
    "ProbeScope",
    "SlackTier",
    "probe_timestamp",
    "utc_now",
    "validate_registry",
]
