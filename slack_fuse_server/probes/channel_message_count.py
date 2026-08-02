"""Authoritative periodic ``search.messages`` totals for visible channels."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from slack_fuse_server.probes.registry import ProbeDeps, probe_timestamp
from slack_fuse_server.search_messages import search_channel_message_total
from slack_fuse_server.slurper.offsets import EventRecord
from slack_fuse_server.slurper.spans import run_sync_with_span

if TYPE_CHECKING:
    import psycopg
    from psycopg.rows import TupleRow

CHANNEL_MESSAGE_COUNT_PROBED = "channel_message_count_probed"
CHANNEL_MESSAGE_COUNT_INTERVAL_S = 6 * 60 * 60.0
TIER_2_PER_CHANNEL_SLEEP_S = 3.5


@dataclass(frozen=True, slots=True)
class ChannelMessageCountTarget:
    channel_id: str
    name: str


async def probe_channel_message_counts(deps: ProbeDeps) -> tuple[EventRecord, ...]:
    """Capture one message-count fact for every visible non-DM channel."""
    if deps.client.token.startswith("xoxb-"):
        raise ValueError("channel message-count probes require a Slack user token, not a bot token")

    targets = await deps.writer.run_read(
        _list_targets,
        limiter=deps.limiters.admin_read,
        span=deps.recorder,
    )
    records: list[EventRecord] = []
    made_api_call = False
    for target in targets:
        # search.messages is Tier 2. Match channel_totals.py's conservative
        # pacing while sharing the process-wide Slack API concurrency gate.
        if made_api_call:
            await deps.sleep(TIER_2_PER_CHANNEL_SLEEP_S)
        made_api_call = True
        result = await run_sync_with_span(
            lambda channel_name=target.name: search_channel_message_total(deps.client.http, channel_name),
            limiter=deps.limiters.slack_api,
            span=deps.recorder,
        )
        records.append(
            EventRecord(
                stream="channel-list",
                kind=CHANNEL_MESSAGE_COUNT_PROBED,
                ts=probe_timestamp(deps.clock()),
                payload={
                    "channel_id": target.channel_id,
                    "message_total": result.total,
                    "approximate": result.approximate,
                },
            )
        )
    return tuple(records)


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
    "ChannelMessageCountTarget",
    "probe_channel_message_counts",
]
