"""Resolve Slack permalink URLs to FUSE filesystem paths."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING
from urllib.parse import parse_qs, urlparse

from .__main__ import _resolve_local_zoneinfo  # pyright: ignore[reportPrivateUsage]
from .fuse_v2_helpers import (
    ChannelRow,
    assign_conv_root_slugs,
    conv_root_for,
    dedup_thread_slug_map,
    fetch_day_thread_parents,
    ts_to_local_date,
)

if TYPE_CHECKING:
    from zoneinfo import ZoneInfo

    from psycopg import Connection
    from psycopg.rows import TupleRow


class PermalinkResolutionError(LookupError):
    """Raised when a permalink can't be mapped to a specific FUSE path.

    Distinct from ``ValueError`` (unparseable URL): the URL parsed fine
    but the target (a thread, typically) isn't reachable from the local
    projection state. Callers that care about thread-vs-channel distinctions
    should treat this as a hard miss rather than silently accepting a
    channel-level fallback.
    """


def parse_permalink(url: str) -> tuple[str, str | None, str | None]:
    """Parse a Slack permalink URL.

    Returns (channel_id, message_ts or None, thread_ts or None).
    `message_ts` is None for channel-only URLs (`/archives/<C>` with no `/p<ts>`).
    Raises ValueError if the URL format is unrecognized.
    """
    parsed = urlparse(url)
    parts = parsed.path.strip("/").split("/")

    if len(parts) < 2 or parts[0] != "archives":
        msg = f"Not a Slack archives URL: {url}"
        raise ValueError(msg)

    channel_id = parts[1]
    query = parse_qs(parsed.query)
    thread_ts_list = query.get("thread_ts")
    thread_ts = thread_ts_list[0] if thread_ts_list else None

    # Channel-only URL: /archives/<C>
    if len(parts) == 2:
        return channel_id, None, thread_ts

    # Message URL: /archives/<C>/p<digits>
    if len(parts) != 3 or not parts[2].startswith("p"):
        msg = f"Not a Slack message permalink: {url}"
        raise ValueError(msg)

    raw_ts = parts[2][1:]  # strip "p" prefix
    if len(raw_ts) < 11 or not raw_ts.isdigit():
        msg = f"Invalid timestamp in permalink: {parts[2]}"
        raise ValueError(msg)
    message_ts = f"{raw_ts[:10]}.{raw_ts[10:]}"

    return channel_id, message_ts, thread_ts


def resolve_permalink(
    url: str,
    mountpoint: str,
    conn: Connection[TupleRow],
) -> str:
    """Resolve a Slack permalink against the local v2 projections store."""
    channel_id, message_ts, thread_ts = parse_permalink(url)
    root, channel_slug = _resolve_channel_location(conn, channel_id)

    # Channel-only URL (no message ts) → channel directory
    if message_ts is None and thread_ts is None:
        return f"{mountpoint}/{root}/{channel_slug}"

    target_ts_text = thread_ts if thread_ts is not None else message_ts
    assert target_ts_text is not None  # narrowed by the channel-only return above
    target_ts = Decimal(target_ts_text)
    local_tz = _resolve_local_zoneinfo()
    target_day = ts_to_local_date(target_ts, local_tz)
    thread_slug = _find_thread_slug(conn, channel_id, target_ts, target_day, local_tz)
    if thread_slug is not None:
        return f"{mountpoint}/{root}/{channel_slug}/{target_day:%Y-%m}/{target_day:%d}/{thread_slug}/thread.md"

    if thread_ts is not None:
        # The URL explicitly named a thread (?thread_ts=...) but our
        # projected view of the parent's day doesn't show it as a thread
        # parent. Surface the miss instead of silently
        # returning the day's channel.md — the caller asked for a
        # specific thread and a channel-level fallback hides the bug.
        msg = f"thread {thread_ts} not found in {channel_slug} on {target_day:%Y-%m-%d}; local projection may be stale"
        raise PermalinkResolutionError(msg)

    return f"{mountpoint}/{root}/{channel_slug}/{target_day:%Y-%m}/{target_day:%d}/channel.md"


def _resolve_channel_location(conn: Connection[TupleRow], channel_id: str) -> tuple[str, str]:
    """Return the mount's conv-root and stable slug for a projected channel."""
    channel = _fetch_channel(conn, channel_id)
    if channel is None:
        msg = f"channel {channel_id} not found in local channels projection"
        raise PermalinkResolutionError(msg)

    root = conv_root_for(channel)
    for candidate, slug in assign_conv_root_slugs(conn, root):
        if candidate.channel_id == channel_id:
            return root, slug

    msg = f"channel {channel_id} is not reachable in the mounted projection"
    raise PermalinkResolutionError(msg)


def _fetch_channel(conn: Connection[TupleRow], channel_id: str) -> ChannelRow | None:
    """Load the channel columns needed for mount-compatible slug assignment."""
    with conn.cursor() as cur:
        _ = cur.execute(
            "SELECT channel_id, name, is_im, is_mpim, is_member, is_archived, im_user_id, tier "
            "FROM channels WHERE channel_id = %s",
            (channel_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return ChannelRow(
        channel_id=str(row[0]),
        name="" if row[1] is None else str(row[1]),
        is_im=bool(row[2]),
        is_mpim=bool(row[3]),
        is_member=bool(row[4]),
        is_archived=bool(row[5]),
        im_user_id=None if row[6] is None else str(row[6]),
        tier=str(row[7]),
    )


def _find_thread_slug(
    conn: Connection[TupleRow],
    channel_id: str,
    thread_ts: Decimal,
    thread_day: date,
    local_tz: ZoneInfo,
) -> str | None:
    """Return the mount's stable, mention-resolved slug for a thread parent."""
    parents = fetch_day_thread_parents(conn, channel_id, thread_day, local_tz)
    for slug, parent_ts in dedup_thread_slug_map(parents, conn).items():
        if parent_ts == thread_ts:
            return slug
    return None
