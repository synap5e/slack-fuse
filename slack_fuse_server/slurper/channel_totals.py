"""Periodic ``search.messages`` sweep for workspace channel sizes.

The result is query-derived Slack state, so each channel is upserted into
``channel_message_totals`` rather than emitted into the append-only events
log. Failed refreshes update the status/timestamp while preserving the last
known total.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

import httpx
import trio

from slack_fuse_server.search_messages import SearchMessagesError, SearchMessageTotal, search_channel_message_total
from slack_fuse_server.slurper.channels import populate_channels_once
from slack_fuse_server.slurper.spans import run_sync_with_span, span

if TYPE_CHECKING:
    import psycopg
    from psycopg.rows import TupleRow

    from slack_fuse_server.slurper.api import SlackClient
    from slack_fuse_server.slurper.limiters import SlurperLimiters
    from slack_fuse_server.slurper.offsets import OffsetWriter
    from slack_fuse_server.slurper.supervisor import TaskSupervisor

log = logging.getLogger(__name__)

DEFAULT_INTERVAL_S = 6 * 60 * 60.0

type SearchFn = Callable[[httpx.Client, str], SearchMessageTotal]


@dataclass(frozen=True, slots=True)
class ChannelTotalTarget:
    channel_id: str
    name: str | None
    is_im: bool


async def refresh_channel_totals_periodically(
    writer: OffsetWriter,
    client: SlackClient,
    limiters: SlurperLimiters,
    supervisor: TaskSupervisor | None = None,
    *,
    interval_s: float = DEFAULT_INTERVAL_S,
) -> None:
    """Refresh all visible channel totals forever on a six-hour cadence."""
    while True:
        try:
            await refresh_channel_totals_once(
                writer,
                client,
                limiters,
                supervisor,
            )
        except Exception:
            log.exception("channel-totals: cycle failed; retrying after interval")
        if supervisor is not None:
            supervisor.declare("channel-totals", "sleeping_until", deadline_s=None)
        await trio.sleep(interval_s)


async def refresh_channel_totals_once(
    writer: OffsetWriter,
    client: SlackClient,
    limiters: SlurperLimiters,
    supervisor: TaskSupervisor | None = None,
    *,
    search_fn: SearchFn = search_channel_message_total,
) -> None:
    """Run one complete, per-channel persisted search sweep."""
    if client.token.startswith("xoxb-"):
        raise ValueError("channel totals require a Slack user token, not a bot token")

    # Refresh discovery first so non-member public channels found by
    # conversations.list are eligible in the same cycle. The importer is
    # idempotent and persists only channels not already in channel-list.
    await populate_channels_once(writer, client, limiters, supervisor, exclude_archived=False)

    async with span(op="slurper.channel_totals.list_channels", task="channel-totals") as list_span:
        targets = await writer.run_read(_list_targets, limiter=limiters.admin_read, span=list_span)
        list_span.set("channels", len(targets))

    log.info("channel-totals: starting cycle for %d channel(s)", len(targets))
    ok = 0
    approximate = 0
    unavailable = 0
    errors = 0
    for target in targets:
        if target.is_im or not target.name:
            await writer.run_transaction(
                lambda conn, channel_id=target.channel_id: _mark_refresh_failed(conn, channel_id, "unavailable")
            )
            unavailable += 1
            continue

        try:
            async with span(
                op="slurper.channel_totals.search_channel",
                task="channel-totals",
                extra={"channel_id": target.channel_id},
            ) as search_span:
                await limiters.slack_tier2.wait()
                result = await run_sync_with_span(
                    lambda channel_name=target.name: search_fn(client.http, channel_name or ""),
                    limiter=limiters.slack_api,
                    span=search_span,
                )
                refresh_status = "approximate" if result.approximate else "ok"
                await writer.run_transaction(
                    lambda conn, channel_id=target.channel_id, total=result.total, status=refresh_status: _upsert_total(
                        conn, channel_id, total, status
                    ),
                    span=search_span,
                )
                search_span.set("total", result.total)
                search_span.set("refresh_status", refresh_status)
        except (httpx.HTTPError, SearchMessagesError, ValueError):
            log.warning("channel-totals: search failed for %s", target.channel_id, exc_info=True)
            await writer.run_transaction(
                lambda conn, channel_id=target.channel_id: _mark_refresh_failed(conn, channel_id, "error")
            )
            errors += 1
        else:
            if result.approximate:
                approximate += 1
            else:
                ok += 1

    log.info(
        "channel-totals: cycle complete ok=%d approximate=%d unavailable=%d errors=%d",
        ok,
        approximate,
        unavailable,
        errors,
    )
    if supervisor is not None:
        supervisor.declare("channel-totals", "complete", deadline_s=None)


def _list_targets(conn: psycopg.Connection[TupleRow]) -> list[ChannelTotalTarget]:
    """Return every channel represented by the folded channel-list view."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT channel_id, name, COALESCE(is_im, false)
            FROM channels
            ORDER BY channel_id
            """
        )
        return [
            ChannelTotalTarget(
                channel_id=str(channel_id),
                name=str(name) if name is not None else None,
                is_im=bool(is_im),
            )
            for channel_id, name, is_im in cur.fetchall()
        ]


def _upsert_total(
    conn: psycopg.Connection[TupleRow],
    channel_id: str,
    total: int,
    refresh_status: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO channel_message_totals (channel_id, total, refreshed_at, refresh_status)
            VALUES (%s, %s, now(), %s)
            ON CONFLICT (channel_id) DO UPDATE
            SET total = EXCLUDED.total,
                refreshed_at = EXCLUDED.refreshed_at,
                refresh_status = EXCLUDED.refresh_status
            """,
            (channel_id, total, refresh_status),
        )


def _mark_refresh_failed(
    conn: psycopg.Connection[TupleRow],
    channel_id: str,
    refresh_status: str,
) -> None:
    """Record a failed attempt without erasing the last known total."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO channel_message_totals (channel_id, total, refreshed_at, refresh_status)
            VALUES (%s, 0, now(), %s)
            ON CONFLICT (channel_id) DO UPDATE
            SET refreshed_at = EXCLUDED.refreshed_at,
                refresh_status = EXCLUDED.refresh_status
            """,
            (channel_id, refresh_status),
        )


__all__ = [
    "DEFAULT_INTERVAL_S",
    "ChannelTotalTarget",
    "refresh_channel_totals_once",
    "refresh_channel_totals_periodically",
]
