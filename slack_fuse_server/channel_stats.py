"""One-query workspace channel inventory projection."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, cast

from slack_fuse_server.http.dto import ChannelStat, ChannelStatsResponse, ChannelStatStatus

if TYPE_CHECKING:
    import psycopg
    from psycopg.rows import TupleRow


_CHANNEL_STATS_SQL = """
WITH latest_full_payload AS (
    SELECT DISTINCT ON (payload->>'id')
        payload->>'id' AS channel_id,
        payload
    FROM events
    WHERE stream = 'channel-list'
      AND kind IN ('channel_added', 'channel_info_refreshed')
      AND payload->>'id' IS NOT NULL
    ORDER BY payload->>'id', offset_in_stream DESC
),
-- Lifetime-ingested count of message events. Was briefly switched to
-- count(active_messages) in 185fde4 (post-fold, so deletions removed from
-- the total per WTF-audit DESIGN-3), but active_messages is an unmaterialized
-- view that folds edits+deletes across the entire event stream on every
-- query — 261s wall clock against 828k events, starving the process every
-- 5min when the client warmer polls this endpoint (production incident,
-- 2026-08-17). Reverted here to the raw-event count until a maintained
-- counter table lands. Metric name is now honest about being lifetime-ingested
-- rather than active; the "done" comparison at 99% of Slack total is still
-- approximate but false-completion via deletion is a rare mode we accept
-- until the counter fix.
message_counts AS (
    SELECT substr(stream, length('channel:') + 1) AS channel_id,
           count(*)::bigint AS ingested
    FROM events
    WHERE stream LIKE 'channel:%'
      AND kind = 'message'
    GROUP BY stream
)
SELECT
    channels.channel_id,
    channels.name,
    totals.total,
    COALESCE(message_counts.ingested, 0),
    COALESCE(channels.is_member, false),
    COALESCE(channels.is_archived, false),
    (blocked.channel_id IS NOT NULL),
    CASE
        WHEN latest_full_payload.payload->>'created' ~ '^[0-9]+$'
        THEN (to_timestamp((latest_full_payload.payload->>'created')::bigint) AT TIME ZONE 'UTC')::date
    END AS created,
    totals.refreshed_at,
    COALESCE(totals.refresh_status, 'unavailable')
FROM channels
LEFT JOIN latest_full_payload USING (channel_id)
LEFT JOIN channel_message_totals totals USING (channel_id)
LEFT JOIN message_counts USING (channel_id)
LEFT JOIN blocked_channels blocked USING (channel_id)
ORDER BY channels.channel_id
"""


def fetch_channel_stats(conn: psycopg.Connection[TupleRow]) -> ChannelStatsResponse:
    """Join channel metadata, search totals, blocks, and ingest counts once."""
    with conn.cursor() as cur:
        cur.execute(_CHANNEL_STATS_SQL)
        rows = cur.fetchall()

    channels: list[ChannelStat] = []
    workspace_message_total = 0
    oldest_refreshed_at: datetime | None = None
    newest_refreshed_at: datetime | None = None
    refreshed_ok_channels = 0
    refreshable_channels = 0
    for row in rows:
        channel_id = str(row[0])
        name = str(row[1]) if row[1] is not None else channel_id
        total = int(row[2]) if row[2] is not None else None
        ingested = int(row[3])
        is_member = bool(row[4])
        is_archived = bool(row[5])
        is_blocked = bool(row[6])
        created = cast("date | None", row[7])
        channel_refreshed_at = cast("datetime | None", row[8])
        if channel_refreshed_at is not None:
            channel_refreshed_at = channel_refreshed_at.astimezone(UTC)
        refresh_status = str(row[9])

        if total is not None:
            workspace_message_total += total
        # A channel is "refreshable" if the workspace_channels sweep will
        # actually try to refresh it (skips DMs and unavailable channels).
        # Coverage percentage = refreshed_ok_channels / refreshable_channels.
        if refresh_status != "unavailable":
            refreshable_channels += 1
        if refresh_status == "ok":
            refreshed_ok_channels += 1
        if channel_refreshed_at is not None:
            if oldest_refreshed_at is None or channel_refreshed_at < oldest_refreshed_at:
                oldest_refreshed_at = channel_refreshed_at
            if newest_refreshed_at is None or channel_refreshed_at > newest_refreshed_at:
                newest_refreshed_at = channel_refreshed_at

        channels.append(
            ChannelStat(
                channel_id=channel_id,
                name=name,
                total=total,
                ingested=ingested,
                status=_channel_status(
                    total=total,
                    ingested=ingested,
                    is_member=is_member,
                    is_blocked=is_blocked,
                    refresh_status=refresh_status,
                ),
                is_member=is_member,
                is_archived=is_archived,
                is_blocked=is_blocked,
                created=created,
                refresh_status=refresh_status,
            )
        )

    return ChannelStatsResponse(
        oldest_refreshed_at=oldest_refreshed_at,
        newest_refreshed_at=newest_refreshed_at,
        refreshed_ok_channels=refreshed_ok_channels,
        refreshable_channels=refreshable_channels,
        workspace_message_total=workspace_message_total,
        channels=channels,
    )


def _channel_status(
    *,
    total: int | None,
    ingested: int,
    is_member: bool,
    is_blocked: bool,
    refresh_status: str,
) -> ChannelStatStatus:
    if is_blocked:
        return "blocked"
    if total is None or refresh_status in {"error", "unavailable"}:
        return "unavailable"
    if total == 0 or ingested * 100 >= total * 99:
        return "done"
    if ingested == 0 and not is_member:
        return "not_joined"
    if ingested == 0:
        return "not_started"
    return "in_progress"


__all__ = ["fetch_channel_stats"]
