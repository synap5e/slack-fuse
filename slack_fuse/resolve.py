"""Resolve Slack permalink URLs to FUSE filesystem paths."""

from __future__ import annotations

from urllib.parse import parse_qs, urlparse

from ._slug_helpers import (
    conv_root as _conv_root,
    find_channel as _find_channel,
    find_thread_slug as _find_thread_slug,
    load_day_messages as _load_day_messages,
    ts_to_local_date as _ts_to_local_date,
)
from .api import SlackClient
from .user_cache import UserCache


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
    client: SlackClient,
    users: UserCache,
) -> str:
    """Resolve a Slack permalink to the corresponding FUSE path."""
    channel_id, message_ts, thread_ts = parse_permalink(url)
    channel, channel_slug = _find_channel(channel_id, client, users)
    root = _conv_root(channel)

    # Channel-only URL (no message ts) → channel directory
    if message_ts is None and thread_ts is None:
        return f"{mountpoint}/{root}/{channel_slug}"

    if thread_ts:
        # Thread reply: directory is under the parent's date
        month, day = _ts_to_local_date(thread_ts)
        date_str = f"{month}-{day}"
        slug = _find_thread_slug(channel_id, thread_ts, date_str, client, users)
        if slug:
            return f"{mountpoint}/{root}/{channel_slug}/{month}/{day}/{slug}/thread.md"
        # Thread slug not resolvable, fall through to channel.md for the right date.
        # If we only have thread_ts (no message_ts), use the thread's date.
        if message_ts is not None:
            month, day = _ts_to_local_date(message_ts)
        _load_day_messages(channel_id, f"{month}-{day}", client)
        return f"{mountpoint}/{root}/{channel_slug}/{month}/{day}/channel.md"

    # message_ts is set, thread_ts is None: check if message is itself a thread parent
    assert message_ts is not None  # narrowed by the channel-only check above
    month, day = _ts_to_local_date(message_ts)
    date_str = f"{month}-{day}"
    slug = _find_thread_slug(channel_id, message_ts, date_str, client, users)
    if slug:
        return f"{mountpoint}/{root}/{channel_slug}/{month}/{day}/{slug}/thread.md"

    return f"{mountpoint}/{root}/{channel_slug}/{month}/{day}/channel.md"
