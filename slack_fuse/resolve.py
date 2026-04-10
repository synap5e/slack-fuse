"""Resolve Slack permalink URLs to FUSE filesystem paths."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, urlparse

from .api import SlackClient
from .disk_cache import get_channel_list, get_day_messages
from .models import Channel, Message
from .mrkdwn import convert as mrkdwn_convert
from .slug import slugify
from .user_cache import UserCache


def parse_permalink(url: str) -> tuple[str, str, str | None]:
    """Parse a Slack permalink URL.

    Returns (channel_id, message_ts, thread_ts or None).
    Raises ValueError if the URL format is unrecognized.
    """
    parsed = urlparse(url)
    parts = parsed.path.strip("/").split("/")
    # Expected: ["archives", "<channel_id>", "p<digits>"]
    if len(parts) != 3 or parts[0] != "archives" or not parts[2].startswith("p"):
        msg = f"Not a Slack message permalink: {url}"
        raise ValueError(msg)

    channel_id = parts[1]
    raw_ts = parts[2][1:]  # strip "p" prefix
    if len(raw_ts) < 11 or not raw_ts.isdigit():
        msg = f"Invalid timestamp in permalink: {parts[2]}"
        raise ValueError(msg)
    message_ts = f"{raw_ts[:10]}.{raw_ts[10:]}"

    query = parse_qs(parsed.query)
    thread_ts_list = query.get("thread_ts")
    thread_ts = thread_ts_list[0] if thread_ts_list else None

    return channel_id, message_ts, thread_ts


def _ts_to_local_date(ts: str) -> tuple[str, str]:
    """Convert a Slack timestamp to local-timezone (YYYY-MM, DD)."""
    dt = datetime.fromtimestamp(float(ts), tz=UTC).astimezone()
    return dt.strftime("%Y-%m"), dt.strftime("%d")


def _conv_root(ch: Channel) -> str:
    """Determine the FUSE top-level directory for a channel."""
    if ch.is_im:
        return "dms"
    if ch.is_mpim:
        return "group-dms"
    if ch.is_member:
        return "channels"
    return "other-channels"


def _build_channel_slug(channel: Channel, users: UserCache, slug_counts: dict[str, int]) -> str:
    """Compute channel directory slug. Mirrors store._build_slug."""
    if channel.is_im and channel.im_user_id:
        display = users.get_display_name(channel.im_user_id)
        base_slug = slugify(display) or channel.id[:12]
    else:
        base_slug = slugify(channel.name) or channel.id[:12]
    count = slug_counts.get(base_slug, 0)
    slug_counts[base_slug] = count + 1
    return base_slug if count == 0 else f"{base_slug}-{count + 1}"


def _find_channel(
    channel_id: str,
    client: SlackClient,
    users: UserCache,
) -> tuple[Channel, str]:
    """Find channel by ID and compute its slug.

    Loads the full channel list so slug dedup matches the FUSE mount.
    Falls back to conversations.info if the channel isn't in the list.
    """
    raw_channels = get_channel_list()
    if raw_channels is not None:
        channels = [Channel.model_validate(c) for c in raw_channels]
    else:
        channels = client.list_conversations()

    slug_counts: dict[str, int] = {}
    for ch in channels:
        slug = _build_channel_slug(ch, users, slug_counts)
        if ch.id == channel_id:
            return ch, slug

    # Not in list: fetch directly (slug won't account for dedup, but rare)
    ch = client.get_channel_info(channel_id)
    return ch, _build_channel_slug(ch, users, {})


def _load_day_messages(
    channel_id: str,
    date_str: str,
    client: SlackClient,
) -> list[Message]:
    """Load day messages from disk cache, falling back to API."""
    raw = get_day_messages(channel_id, date_str)
    if raw is not None:
        return [Message.model_validate(m) for m in raw]

    # Fetch from API using the local-day boundaries
    date = datetime.strptime(date_str, "%Y-%m-%d").date()
    tz = datetime.now().astimezone().tzinfo
    start = datetime(date.year, date.month, date.day, tzinfo=tz)
    end = start + timedelta(days=1)
    return client.get_history(
        channel_id,
        oldest=str(start.timestamp()),
        latest=str(end.timestamp()),
    )


def _find_thread_slug(
    channel_id: str,
    thread_ts: str,
    date_str: str,
    client: SlackClient,
    users: UserCache,
) -> str | None:
    """Compute the thread slug for a given thread_ts.

    Replays the same slug logic as store.get_thread_slugs so the slug
    matches the FUSE mount. Returns None if the thread parent isn't
    found in the day's messages.
    """
    messages = _load_day_messages(channel_id, date_str, client)

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

    for slug, ts in threads.items():
        if ts == thread_ts:
            return slug
    return None


def resolve_permalink(
    url: str,
    mountpoint: str,
    client: SlackClient,
    users: UserCache,
) -> str:
    """Resolve a Slack permalink to the corresponding FUSE path."""
    channel_id, message_ts, thread_ts = parse_permalink(url)
    channel, channel_slug = _find_channel(channel_id, client, users)
    root = _conv_root(channel)

    if thread_ts:
        # Thread reply: directory is under the parent's date
        month, day = _ts_to_local_date(thread_ts)
        date_str = f"{month}-{day}"
        slug = _find_thread_slug(channel_id, thread_ts, date_str, client, users)
        if slug:
            return f"{mountpoint}/{root}/{channel_slug}/{month}/{day}/{slug}/thread.md"
        # Thread slug not resolvable, fall through to channel.md
        month, day = _ts_to_local_date(message_ts)
        return f"{mountpoint}/{root}/{channel_slug}/{month}/{day}/channel.md"

    # No thread_ts: check if message is itself a thread parent
    month, day = _ts_to_local_date(message_ts)
    date_str = f"{month}-{day}"
    slug = _find_thread_slug(channel_id, message_ts, date_str, client, users)
    if slug:
        return f"{mountpoint}/{root}/{channel_slug}/{month}/{day}/{slug}/thread.md"

    return f"{mountpoint}/{root}/{channel_slug}/{month}/{day}/channel.md"
