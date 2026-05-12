"""Shared slug and cache helpers for permalink/path resolution."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import cast

from .api import SlackClient
from .disk_cache import get_channel_list, get_day_messages, get_known_dates, put_day_messages, put_known_dates
from .models import Channel, JsonObject, Message
from .mrkdwn import convert as mrkdwn_convert
from .slug import slugify
from .user_cache import UserCache


def ts_to_local_date(ts: str) -> tuple[str, str]:
    """Convert a Slack timestamp to local-timezone (YYYY-MM, DD)."""
    dt = datetime.fromtimestamp(float(ts), tz=UTC).astimezone()
    return dt.strftime("%Y-%m"), dt.strftime("%d")


def conv_root(ch: Channel) -> str:
    """Determine the FUSE top-level directory for a channel."""
    if ch.is_im:
        return "dms"
    if ch.is_mpim:
        return "group-dms"
    if ch.is_member:
        return "channels"
    return "other-channels"


def build_channel_slug(channel: Channel, users: UserCache, slug_counts: dict[str, int]) -> str:
    """Compute channel directory slug. Mirrors store._build_slug."""
    if channel.is_im and channel.im_user_id:
        display = users.get_display_name(channel.im_user_id)
        base_slug = slugify(display) or channel.id[:12]
    else:
        base_slug = slugify(channel.name) or channel.id[:12]
    count = slug_counts.get(base_slug, 0)
    slug_counts[base_slug] = count + 1
    return base_slug if count == 0 else f"{base_slug}-{count + 1}"


def _load_channels(client: SlackClient) -> list[Channel]:
    raw_channels = get_channel_list()
    if raw_channels is not None:
        return [Channel.model_validate(c) for c in raw_channels]
    return client.list_conversations()


def find_channel(
    channel_id: str,
    client: SlackClient,
    users: UserCache,
) -> tuple[Channel, str]:
    """Find channel by ID and compute its slug.

    Loads the full channel list so slug dedup matches the FUSE mount.
    Falls back to conversations.info if the channel isn't in the list.
    """
    channels = _load_channels(client)

    slug_counts: dict[str, int] = {}
    for ch in channels:
        slug = build_channel_slug(ch, users, slug_counts)
        if ch.id == channel_id:
            return ch, slug

    # Not in list: fetch directly (slug won't account for dedup, but rare).
    ch = client.get_channel_info(channel_id)
    return ch, build_channel_slug(ch, users, {})


def find_channel_by_slug(
    slug: str,
    client: SlackClient,
    users: UserCache,
) -> tuple[Channel, str] | None:
    """Find a channel by replaying the FUSE slug generation."""
    channels = _load_channels(client)

    slug_counts: dict[str, int] = {}
    for ch in channels:
        channel_slug = build_channel_slug(ch, users, slug_counts)
        if channel_slug == slug:
            return ch, channel_slug
    return None


def load_day_messages(
    channel_id: str,
    date_str: str,
    client: SlackClient,
) -> list[Message]:
    """Load day messages from disk cache, falling back to API."""
    raw = get_day_messages(channel_id, date_str)
    if raw is not None:
        return [Message.model_validate(m) for m in raw]

    # Fetch from API using the local-day boundaries.
    date = datetime.strptime(date_str, "%Y-%m-%d").date()
    tz = datetime.now().astimezone().tzinfo
    start = datetime(date.year, date.month, date.day, tzinfo=tz)
    end = start + timedelta(days=1)
    messages = client.get_history(
        channel_id,
        oldest=str(start.timestamp()),
        latest=str(end.timestamp()),
    )
    _cache_day_messages(channel_id, date_str, messages)
    return messages


def _cache_day_messages(channel_id: str, date_str: str, messages: list[Message]) -> None:
    """Persist resolver-fetched day data so the FUSE mount can materialize the path."""
    put_day_messages(
        channel_id,
        date_str,
        [cast("JsonObject", m.model_dump(mode="json")) for m in messages],
    )
    dates = get_known_dates(channel_id) or set()
    if date_str not in dates:
        dates.add(date_str)
        put_known_dates(channel_id, dates)


def build_thread_slug_map(
    channel_id: str,
    date_str: str,
    client: SlackClient,
    users: UserCache,
) -> dict[str, str]:
    """Return thread slug -> thread_ts for threads starting on this date."""
    messages = load_day_messages(channel_id, date_str, client)

    threads: dict[str, str] = {}
    for msg in messages:
        if msg.reply_count > 0 and msg.thread_ts == msg.ts:
            text = mrkdwn_convert(msg.text[:80], users) if msg.text else msg.ts
            slug = slugify(text) or msg.ts.replace(".", "-")
            base = slug
            counter = 2
            while slug in threads:
                slug = f"{base}-{counter}"
                counter += 1
            threads[slug] = msg.ts
    return threads


def find_thread_slug(
    channel_id: str,
    thread_ts: str,
    date_str: str,
    client: SlackClient,
    users: UserCache,
) -> str | None:
    """Compute the thread slug for a given thread_ts."""
    for slug, ts in build_thread_slug_map(channel_id, date_str, client, users).items():
        if ts == thread_ts:
            return slug
    return None


def find_thread_ts_by_slug(
    channel_id: str,
    thread_slug: str,
    date_str: str,
    client: SlackClient,
    users: UserCache,
) -> str | None:
    """Resolve a thread directory slug back to its parent thread_ts."""
    return build_thread_slug_map(channel_id, date_str, client, users).get(thread_slug)
