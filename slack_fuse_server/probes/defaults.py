"""Built-in probe registry and production dependency factory."""

from __future__ import annotations

from slack_fuse_server.probes.channel_message_count import (
    CHANNEL_MESSAGE_COUNT_INTERVAL_S,
    CHANNEL_MESSAGE_COUNT_PROBED,
    probe_channel_message_counts,
)
from slack_fuse_server.probes.registry import ProbeDeps, ProbeKind, ProbeScope, SlackTier
from slack_fuse_server.probes.sweep import EventsTableProbeCursor
from slack_fuse_server.slurper.api import SlackClient
from slack_fuse_server.slurper.limiters import SlurperLimiters
from slack_fuse_server.slurper.offsets import OffsetWriter


def register_default_probes() -> tuple[ProbeKind, ...]:
    """Return the built-in registry; adding a kind requires one entry here."""
    return (
        ProbeKind(
            kind=CHANNEL_MESSAGE_COUNT_PROBED,
            interval_s=CHANNEL_MESSAGE_COUNT_INTERVAL_S,
            tier=SlackTier.TIER_2,
            scope=ProbeScope.PER_CHANNEL,
            run=probe_channel_message_counts,
        ),
    )


def make_probe_deps(
    client: SlackClient,
    writer: OffsetWriter,
    limiters: SlurperLimiters,
) -> ProbeDeps:
    """Build production dependencies with event-table-backed cadence state."""
    cursor = EventsTableProbeCursor(writer=writer, limiter=limiters.admin_read)
    return ProbeDeps(client=client, writer=writer, limiters=limiters, cursor=cursor)


__all__ = ["make_probe_deps", "register_default_probes"]
