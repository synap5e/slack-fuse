"""Interpreted ``search.messages`` facts for the unified slurper probe sweep."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import trio

from slack_fuse_server.probes.registry import ProbeDeps, ProbeTarget, probe_timestamp
from slack_fuse_server.search_messages import search_channel_message_total
from slack_fuse_server.slurper.offsets import EventRecord
from slack_fuse_server.slurper.spans import run_sync_with_span

if TYPE_CHECKING:
    import psycopg
    from psycopg import Connection
    from psycopg.rows import TupleRow

CHANNEL_MESSAGE_COUNT_PROBED = "channel_message_count_probed"
JOB_CHANNEL_MESSAGE_COUNT = "channel_message_count"
CHANNEL_MESSAGE_COUNT_INTERVAL_S = 6 * 60 * 60.0
WORKSPACE_TARGET = "workspace"


@dataclass(frozen=True, slots=True)
class ChannelMessageCountTarget:
    channel_id: str
    name: str


async def channel_message_count_targets(_deps: ProbeDeps) -> tuple[ProbeTarget, ...]:
    """Schedule one atomic all-channel fact batch per cadence period."""
    await trio.lowlevel.checkpoint()
    return (ProbeTarget(WORKSPACE_TARGET),)


async def probe_channel_message_counts(
    deps: ProbeDeps,
    target: ProbeTarget,
) -> tuple[EventRecord, ...]:
    """Capture one message-count fact per visible non-DM channel.

    Scheduled runs use one workspace target and gather every API result before
    the ``EventFactsSink`` writes the batch. A failure therefore writes no
    partial period. The expensive fact probe is deliberately excluded from the
    existing raw-probe manual control surface.
    """
    if deps.client.token.startswith("xoxb-"):
        raise ValueError("channel message-count probes require a Slack user token, not a bot token")

    if target.value != WORKSPACE_TARGET:
        raise ValueError(f"channel message-count probe requires target {WORKSPACE_TARGET!r}")
    targets = await deps.writer.run_read(
        _list_targets,
        limiter=deps.limiters.admin_read,
        span=deps.recorder,
    )
    records: list[EventRecord] = []
    for channel in targets:
        # Shared with channel_totals.py: unlike the worker CapacityLimiter,
        # this process-wide tier pacer spaces request starts across both jobs.
        await deps.limiters.slack_tier2.wait()
        result = await run_sync_with_span(
            lambda channel_name=channel.name: search_channel_message_total(deps.client.http, channel_name),
            limiter=deps.limiters.slack_api,
            span=deps.recorder,
        )
        records.append(
            EventRecord(
                stream="channel-list",
                kind=CHANNEL_MESSAGE_COUNT_PROBED,
                ts=probe_timestamp(deps.clock()),
                payload={
                    "channel_id": channel.channel_id,
                    "message_total": result.total,
                    "approximate": result.approximate,
                },
            )
        )
    return tuple(records)


def channel_message_count_due(
    conn: Connection[TupleRow],
    _target: ProbeTarget,
    interval_s: float,
    now: datetime,
) -> bool:
    """Read the latest fact timestamp through migration 0015's partial index."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(ts)
            FROM events
            WHERE kind = 'channel_message_count_probed'
              AND kind IN ('channel_message_count_probed')
            """
        )
        row = cur.fetchone()
    if row is None or row[0] is None:
        return True
    last_run_at = datetime.fromisoformat(str(row[0]).replace("Z", "+00:00"))
    if last_run_at.tzinfo is None or last_run_at.utcoffset() is None:
        raise ValueError("channel message-count probe has a timezone-naive ts")
    return (now.astimezone(UTC) - last_run_at.astimezone(UTC)).total_seconds() >= interval_s


def _list_targets(conn: psycopg.Connection[TupleRow]) -> tuple[ChannelMessageCountTarget, ...]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT channel_id, name
            FROM channels
            WHERE NOT COALESCE(is_archived, false)
              AND NOT COALESCE(is_im, false)
              AND name IS NOT NULL
            ORDER BY channel_id
            """
        )
        return tuple(ChannelMessageCountTarget(channel_id=str(row[0]), name=str(row[1])) for row in cur.fetchall())


__all__ = [
    "CHANNEL_MESSAGE_COUNT_INTERVAL_S",
    "CHANNEL_MESSAGE_COUNT_PROBED",
    "JOB_CHANNEL_MESSAGE_COUNT",
    "ChannelMessageCountTarget",
    "channel_message_count_due",
    "channel_message_count_targets",
    "probe_channel_message_counts",
]
