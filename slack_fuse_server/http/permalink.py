"""Server-side FUSE-path -> Slack permalink resolution helpers."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from slack_fuse_server.slurper.api import SlackClient

from .resolve import DisplayNameResolver, conv_root, find_channel_by_slug, find_thread_ts_by_slug

type _PathKind = Literal["channel_dir", "channel_file", "day_file", "thread_file"]

_CONV_ROOTS = frozenset({"channels", "dms", "group-dms", "other-channels"})
_DAY_FILES = frozenset({"channel.md", "feed.md"})
_THREAD_FILES = frozenset({"thread.md", "feed.md"})


@dataclass(frozen=True, slots=True)
class _ParsedPath:
    kind: _PathKind
    root: str
    channel_slug: str
    date_str: str | None = None
    thread_slug: str | None = None


def resolve_path_to_permalink_url(
    path: str,
    client: SlackClient,
    users: DisplayNameResolver,
    workspace_url: str | None,
    *,
    ts: str | None = None,
) -> str:
    """Resolve a client path (`channels/...`) to Slack permalink URL."""
    parsed = _parse_path(path)

    found = find_channel_by_slug(parsed.channel_slug, client, users)
    if found is None:
        msg = f"could not resolve channel slug {parsed.channel_slug!r}"
        raise ValueError(msg)

    channel, _resolved_slug = found
    expected_root = conv_root(channel)
    if parsed.root != expected_root:
        msg = f"channel slug {parsed.channel_slug!r} belongs under {expected_root!r}, not {parsed.root!r}"
        raise ValueError(msg)

    if parsed.kind in ("channel_dir", "channel_file"):
        if not workspace_url:
            msg = "set `SLACK_WORKSPACE_URL` to enable channel-root permalinks"
            raise ValueError(msg)
        return f"{workspace_url.rstrip('/')}/archives/{channel.id}"

    if parsed.kind == "day_file":
        if ts is None:
            msg = "day file is ambiguous; pass --ts <message_ts>"
            raise ValueError(msg)
        return client.get_permalink(channel.id, ts)

    if parsed.date_str is None or parsed.thread_slug is None:
        msg = "thread path is missing date and/or thread slug"
        raise ValueError(msg)

    message_ts = ts or find_thread_ts_by_slug(
        channel.id,
        parsed.thread_slug,
        parsed.date_str,
        client,
        users,
    )
    if message_ts is None:
        msg = (
            f"could not resolve thread slug {parsed.thread_slug!r} for channel {channel.id!r} "
            f"on {parsed.date_str}; pass --ts <message_ts>"
        )
        raise ValueError(msg)
    return client.get_permalink(channel.id, message_ts)


def _parse_path(path: str) -> _ParsedPath:  # noqa: C901 - path-shape dispatch hub.
    parts = _extract_conversation_path_parts(path)
    if not parts:
        msg = "path is empty after normalization"
        raise ValueError(msg)

    root = parts[0]
    if root not in _CONV_ROOTS:
        msg = f"parsed root {root!r}; expected one of {sorted(_CONV_ROOTS)}"
        raise ValueError(msg)
    if len(parts) < 2:
        msg = f"parsed root {root!r}; missing <channel_slug>"
        raise ValueError(msg)

    channel_slug = parts[1]
    tail = parts[2:]

    if not tail:
        return _ParsedPath(kind="channel_dir", root=root, channel_slug=channel_slug)

    if tail == ["channel.md"]:
        return _ParsedPath(kind="channel_file", root=root, channel_slug=channel_slug)

    if len(tail) == 2:
        date_str = _parse_date_token(tail[0])
        if date_str is not None and tail[1] in _DAY_FILES:
            return _ParsedPath(kind="day_file", root=root, channel_slug=channel_slug, date_str=date_str)
        msg = f"parsed {parts!r}; expected channel.md or <date>/<day-file>"
        raise ValueError(msg)

    if len(tail) == 3:
        day_date = _parse_date_pair(tail[0], tail[1])
        if day_date is not None and tail[2] in _DAY_FILES:
            return _ParsedPath(kind="day_file", root=root, channel_slug=channel_slug, date_str=day_date)

        thread_date = _parse_date_token(tail[0])
        if thread_date is not None and tail[2] in _THREAD_FILES:
            return _ParsedPath(
                kind="thread_file",
                root=root,
                channel_slug=channel_slug,
                date_str=thread_date,
                thread_slug=tail[1],
            )
        msg = f"parsed {parts!r}; expected <YYYY-MM>/<DD>/<day-file> or <YYYY-MM-DD>/<thread>/<thread-file>"
        raise ValueError(msg)

    if len(tail) == 4:
        thread_date = _parse_date_pair(tail[0], tail[1])
        if thread_date is not None and tail[3] in _THREAD_FILES:
            return _ParsedPath(
                kind="thread_file",
                root=root,
                channel_slug=channel_slug,
                date_str=thread_date,
                thread_slug=tail[2],
            )
        msg = f"parsed {parts!r}; expected <YYYY-MM>/<DD>/<thread>/<thread-file>"
        raise ValueError(msg)

    msg = f"parsed {parts!r}; expected channel root, day file, or thread file"
    raise ValueError(msg)


def _extract_conversation_path_parts(path: str) -> list[str]:
    normalized = os.path.normpath(os.path.expanduser(path))
    raw_parts = [part for part in normalized.split(os.sep) if part not in ("", ".")]
    if not raw_parts:
        return []

    if raw_parts[0] == ".cached-only":
        return raw_parts[1:]

    for idx, part in enumerate(raw_parts):
        if part in _CONV_ROOTS:
            if idx > 0 and raw_parts[idx - 1] == ".cached-only":
                return raw_parts[idx:]
            return raw_parts[idx:]

    msg = f"parsed {raw_parts!r}; expected one of {sorted(_CONV_ROOTS)}"
    raise ValueError(msg)


def _parse_date_token(token: str) -> str | None:
    try:
        datetime.strptime(token, "%Y-%m-%d")
    except ValueError:
        return None
    return token


def _parse_date_pair(month: str, day: str) -> str | None:
    combined = f"{month}-{day}"
    try:
        datetime.strptime(combined, "%Y-%m-%d")
    except ValueError:
        return None
    return combined
