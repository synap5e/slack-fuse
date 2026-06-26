"""Server-side Slack permalink -> FUSE-path resolution helpers."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Protocol, cast
from urllib.parse import parse_qs, urlparse

from slack_fuse.models import Channel, Message
from slack_fuse.mrkdwn import convert as mrkdwn_convert
from slack_fuse.slug import slugify
from slack_fuse_server.slurper.api import SlackClient

if TYPE_CHECKING:
    from slack_fuse.user_cache import UserCache


class DisplayNameResolver(Protocol):
    """Minimal user-lookup surface needed for slug replay."""

    def get_display_name(self, user_id: str) -> str:
        """Return display name for a Slack user id."""
        ...


class PermalinkResolutionError(LookupError):
    """Raised when a thread permalink cannot map to a concrete thread path."""


def parse_permalink(url: str) -> tuple[str, str | None, str | None]:
    """Parse Slack permalink URL to `(channel_id, message_ts, thread_ts)`."""
    parsed = urlparse(url)
    parts = parsed.path.strip("/").split("/")

    if len(parts) < 2 or parts[0] != "archives":
        msg = f"Not a Slack archives URL: {url}"
        raise ValueError(msg)

    channel_id = parts[1]
    query = parse_qs(parsed.query)
    thread_ts_list = query.get("thread_ts")
    thread_ts = thread_ts_list[0] if thread_ts_list else None

    if len(parts) == 2:
        return channel_id, None, thread_ts

    if len(parts) != 3 or not parts[2].startswith("p"):
        msg = f"Not a Slack message permalink: {url}"
        raise ValueError(msg)

    raw_ts = parts[2][1:]
    if len(raw_ts) < 11 or not raw_ts.isdigit():
        msg = f"Invalid timestamp in permalink: {parts[2]}"
        raise ValueError(msg)

    return channel_id, f"{raw_ts[:10]}.{raw_ts[10:]}", thread_ts


def ts_to_local_date(ts: str) -> tuple[str, str]:
    """Convert a Slack timestamp to local timezone `(YYYY-MM, DD)`."""
    dt = datetime.fromtimestamp(float(ts), tz=UTC).astimezone()
    return dt.strftime("%Y-%m"), dt.strftime("%d")


def conv_root(channel: Channel) -> str:
    """Map a channel shape to its FUSE top-level directory."""
    if channel.is_im:
        return "dms"
    if channel.is_mpim:
        return "group-dms"
    if channel.is_member:
        return "channels"
    return "other-channels"


def build_channel_slug(
    channel: Channel,
    users: DisplayNameResolver,
    slug_counts: dict[str, int],
) -> str:
    """Replay legacy slug generation so server/client paths stay identical."""
    if channel.is_im and channel.im_user_id:
        display = users.get_display_name(channel.im_user_id)
        base_slug = slugify(display) or channel.id[:12]
    else:
        base_slug = slugify(channel.name) or channel.id[:12]
    count = slug_counts.get(base_slug, 0)
    slug_counts[base_slug] = count + 1
    return base_slug if count == 0 else f"{base_slug}-{count + 1}"


def _load_channels(client: SlackClient) -> list[Channel]:
    # In-process consumer (permalink resolution doesn't persist) — unwrap to
    # the validated model.
    return [v.model for v in client.list_conversations()]


def find_channel(
    channel_id: str,
    client: SlackClient,
    users: DisplayNameResolver,
) -> tuple[Channel, str]:
    """Find channel by id and return `(channel, replayed_slug)`."""
    channels = _load_channels(client)

    slug_counts: dict[str, int] = {}
    for channel in channels:
        slug = build_channel_slug(channel, users, slug_counts)
        if channel.id == channel_id:
            return channel, slug

    # Channel missing from list: fallback to direct lookup.
    channel = client.get_channel_info(channel_id).model
    return channel, build_channel_slug(channel, users, {})


def find_channel_by_slug(
    slug: str,
    client: SlackClient,
    users: DisplayNameResolver,
) -> tuple[Channel, str] | None:
    """Resolve a channel slug by replaying the same list-order dedup logic."""
    channels = _load_channels(client)
    slug_counts: dict[str, int] = {}
    for channel in channels:
        channel_slug = build_channel_slug(channel, users, slug_counts)
        if channel_slug == slug:
            return channel, channel_slug
    return None


def _local_day_bounds(date_str: str) -> tuple[datetime, datetime]:
    parsed_day = datetime.strptime(date_str, "%Y-%m-%d").date()
    local_tz = datetime.now().astimezone().tzinfo
    tzinfo = local_tz if local_tz is not None else UTC
    start = datetime(parsed_day.year, parsed_day.month, parsed_day.day, tzinfo=tzinfo)
    end = start + timedelta(days=1)
    return start, end


def load_day_messages(channel_id: str, date_str: str, client: SlackClient) -> list[Message]:
    """Load day messages from Slack API (server has no local FUSE day cache)."""
    start, end = _local_day_bounds(date_str)
    return client.get_history(
        channel_id,
        oldest=str(start.timestamp()),
        latest=str(end.timestamp()),
    )


def _thread_slug_from_parent_text(text: str, users: DisplayNameResolver) -> str:
    # Keep legacy mrkdwn -> markdown normalization so slugs match existing CLI.
    rendered = mrkdwn_convert(text[:80], users=cast("UserCache", users))
    return slugify(rendered)


def build_thread_slug_map(
    channel_id: str,
    date_str: str,
    client: SlackClient,
    users: DisplayNameResolver,
) -> dict[str, str]:
    """Return `thread_slug -> thread_ts` for threads whose parents are on date."""
    messages = load_day_messages(channel_id, date_str, client)
    threads: dict[str, str] = {}
    for message in messages:
        if message.reply_count > 0 and message.thread_ts == message.ts:
            slug = _thread_slug_from_parent_text(message.text or message.ts, users) or message.ts.replace(".", "-")
            base = slug
            counter = 2
            while slug in threads:
                slug = f"{base}-{counter}"
                counter += 1
            threads[slug] = message.ts
    return threads


def find_thread_slug(
    channel_id: str,
    thread_ts: str,
    date_str: str,
    client: SlackClient,
    users: DisplayNameResolver,
) -> str | None:
    """Resolve `thread_ts -> thread_slug` on the given parent date."""
    for slug, ts in build_thread_slug_map(channel_id, date_str, client, users).items():
        if ts == thread_ts:
            return slug
    return None


def find_thread_ts_by_slug(
    channel_id: str,
    thread_slug: str,
    date_str: str,
    client: SlackClient,
    users: DisplayNameResolver,
) -> str | None:
    """Resolve `thread_slug -> thread_ts` on the given parent date."""
    return build_thread_slug_map(channel_id, date_str, client, users).get(thread_slug)


def _date_str(month: str, day: str) -> str:
    return f"{month}-{day}"


def resolve_permalink_url(url: str, client: SlackClient, users: DisplayNameResolver) -> str:
    """Resolve Slack permalink URL -> relative FUSE path (no mountpoint prefix)."""
    channel_id, message_ts, thread_ts = parse_permalink(url)
    channel, channel_slug = find_channel(channel_id, client, users)
    root = conv_root(channel)

    if message_ts is None and thread_ts is None:
        return "/".join((root, channel_slug))

    if thread_ts is not None:
        month, day = ts_to_local_date(thread_ts)
        slug = find_thread_slug(channel_id, thread_ts, _date_str(month, day), client, users)
        if slug is None:
            msg = f"thread {thread_ts} not found in {channel_slug} on {_date_str(month, day)}"
            raise PermalinkResolutionError(msg)
        return "/".join((root, channel_slug, month, day, slug, "thread.md"))

    assert message_ts is not None
    month, day = ts_to_local_date(message_ts)
    slug = find_thread_slug(channel_id, message_ts, _date_str(month, day), client, users)
    if slug is not None:
        return "/".join((root, channel_slug, month, day, slug, "thread.md"))
    return "/".join((root, channel_slug, month, day, "channel.md"))
