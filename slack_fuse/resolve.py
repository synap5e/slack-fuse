"""Resolve Slack permalink URLs to FUSE filesystem paths."""

from __future__ import annotations

from urllib.parse import parse_qs, urlparse

from ._slug_helpers import (
    conv_root as _conv_root,
    find_channel as _find_channel,
    find_thread_slug as _find_thread_slug,
    ts_to_local_date as _ts_to_local_date,
)
from .api import SlackClient
from .user_cache import UserCache


class PermalinkResolutionError(LookupError):
    """Raised when a permalink can't be mapped to a specific FUSE path.

    Distinct from ``ValueError`` (unparseable URL): the URL parsed fine
    but the target (a thread, typically) isn't reachable from current
    cache state. Callers that care about thread-vs-channel distinctions
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
        # Thread reply: directory is under the parent's date. Use the
        # thread_ts's date regardless of the reply's own ts — a reply
        # sent on a later date still lives under the parent's day dir.
        month, day = _ts_to_local_date(thread_ts)
        date_str = f"{month}-{day}"
        slug = _find_thread_slug(channel_id, thread_ts, date_str, client, users)
        if slug:
            return f"{mountpoint}/{root}/{channel_slug}/{month}/{day}/{slug}/thread.md"
        # The URL explicitly named a thread (?thread_ts=...) but our
        # cached view of the parent's day doesn't show it as a thread
        # parent. Most likely the day cache is stale (replies arrived
        # after first fetch). Surface the miss instead of silently
        # returning the day's channel.md — the caller asked for a
        # specific thread and a channel-level fallback hides the bug.
        msg = (
            f"thread {thread_ts} not found in {channel_slug} on {date_str}; "
            f"cached parent may be stale"
        )
        raise PermalinkResolutionError(msg)

    # message_ts is set, thread_ts is None: check if message is itself a thread parent
    assert message_ts is not None  # narrowed by the channel-only check above
    month, day = _ts_to_local_date(message_ts)
    date_str = f"{month}-{day}"
    slug = _find_thread_slug(channel_id, message_ts, date_str, client, users)
    if slug:
        return f"{mountpoint}/{root}/{channel_slug}/{month}/{day}/{slug}/thread.md"

    return f"{mountpoint}/{root}/{channel_slug}/{month}/{day}/channel.md"
