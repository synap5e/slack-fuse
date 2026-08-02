"""Built-in interpreted fact probes registered into the slurper sweep."""

from __future__ import annotations

from slack_fuse_server.probes.channel_message_count import (
    CHANNEL_MESSAGE_COUNT_INTERVAL_S,
    CHANNEL_MESSAGE_COUNT_PROBED,
    JOB_CHANNEL_MESSAGE_COUNT,
    channel_message_count_due,
    channel_message_count_targets,
    probe_channel_message_counts,
)
from slack_fuse_server.probes.registry import EventFactsSink, ProbeKind, ProbeScope, SlackTier


def register_fact_probes() -> tuple[ProbeKind, ...]:
    """Return interpreted fact probes for the unified slurper registry."""
    return (
        ProbeKind(
            job_id=JOB_CHANNEL_MESSAGE_COUNT,
            kind=CHANNEL_MESSAGE_COUNT_PROBED,
            interval_s=CHANNEL_MESSAGE_COUNT_INTERVAL_S,
            tier=SlackTier.TIER_2,
            scope=ProbeScope.PER_CHANNEL,
            run=probe_channel_message_counts,
            targets=channel_message_count_targets,
            due=channel_message_count_due,
            sink=EventFactsSink(),
            op=f"slurper.probe.{CHANNEL_MESSAGE_COUNT_PROBED}",
            manual_triggerable=False,
        ),
    )


__all__ = ["register_fact_probes"]
