"""Resolve FUSE paths back to Slack permalink URLs."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal

from ._slug_helpers import conv_root, find_channel_by_slug, find_thread_ts_by_slug
from .api import SlackClient
from .user_cache import UserCache

type _PathKind = Literal["channel_dir", "channel_file", "day_file", "thread_file"]

_CONV_ROOTS = frozenset({"channels", "dms", "group-dms", "other-channels"})
_DAY_FILES = frozenset({"channel.md", "feed.md"})
_THREAD_FILES = frozenset({"thread.md", "feed.md"})


@dataclass(frozen=True)
class _ParsedPath:
    kind: _PathKind
    root: str
    channel_slug: str
    absolute_path: Path
    frontmatter_path: Path
    date_str: str | None = None
    thread_slug: str | None = None


def resolve_path_to_permalink(  # noqa: PLR0913 - public API mirrors the forward resolver plus workspace URL.
    path: str,
    mountpoint: str,
    client: SlackClient,
    users: UserCache,
    workspace_url: str | None,
    *,
    ts: str | None = None,
) -> str:
    """Resolve a FUSE path to the corresponding Slack permalink URL."""
    parsed = _parse_path(path, mountpoint)
    frontmatter = _read_frontmatter(parsed.frontmatter_path)
    channel_id = _resolve_channel_id(parsed, frontmatter, client, users)

    if parsed.kind in ("channel_dir", "channel_file"):
        if not workspace_url:
            msg = "set `SLACK_WORKSPACE_URL` to enable channel-root permalinks"
            raise ValueError(msg)
        return f"{workspace_url.rstrip('/')}/archives/{channel_id}"

    if parsed.kind == "day_file":
        if ts is None:
            msg = "day file is ambiguous; pass --ts <message_ts>"
            raise ValueError(msg)
        return client.get_permalink(channel_id, ts)

    message_ts = ts or _resolve_thread_ts(parsed, frontmatter, channel_id, client, users)
    return client.get_permalink(channel_id, message_ts)


def _resolve_channel_id(
    parsed: _ParsedPath,
    frontmatter: dict[str, str],
    client: SlackClient,
    users: UserCache,
) -> str:
    channel_id = frontmatter.get("channel_id")
    if channel_id:
        return channel_id

    found = find_channel_by_slug(parsed.channel_slug, client, users)
    if found is None:
        msg = (
            f"could not resolve channel slug {parsed.channel_slug!r}; "
            "rendered frontmatter is missing channel_id and the channel list has no matching slug"
        )
        raise ValueError(msg)

    channel, _slug = found
    expected_root = conv_root(channel)
    if parsed.root != expected_root:
        msg = f"channel slug {parsed.channel_slug!r} belongs under {expected_root!r}, not parsed root {parsed.root!r}"
        raise ValueError(msg)
    return channel.id


def _resolve_thread_ts(
    parsed: _ParsedPath,
    frontmatter: dict[str, str],
    channel_id: str,
    client: SlackClient,
    users: UserCache,
) -> str:
    thread_ts = frontmatter.get("thread_ts")
    if thread_ts:
        return thread_ts

    if parsed.date_str is None or parsed.thread_slug is None:
        msg = "thread path is missing a date directory or thread slug"
        raise ValueError(msg)

    resolved = find_thread_ts_by_slug(channel_id, parsed.thread_slug, parsed.date_str, client, users)
    if resolved is None:
        msg = (
            f"could not resolve thread slug {parsed.thread_slug!r} for channel {channel_id!r} "
            f"on {parsed.date_str}; pass --ts <message_ts>"
        )
        raise ValueError(msg)
    return resolved


def _parse_path(path: str, mountpoint: str) -> _ParsedPath:  # noqa: C901  (path-depth dispatch hub)
    absolute_path = _normalize_path(path)
    absolute_mountpoint = _normalize_path(mountpoint)
    try:
        relative = absolute_path.relative_to(absolute_mountpoint)
    except ValueError as e:
        msg = f"path is not under mountpoint {absolute_mountpoint}: {absolute_path}"
        raise ValueError(msg) from e

    parts = list(relative.parts)
    if parts and parts[0] == ".cached-only":
        parts = parts[1:]
    if not parts:
        msg = f"path points at the mount root; expected <root>/<channel_slug> under {absolute_mountpoint}"
        raise ValueError(msg)

    root = parts[0]
    if root not in _CONV_ROOTS:
        msg = f"parsed root {root!r}; expected one of {sorted(_CONV_ROOTS)}"
        raise ValueError(msg)
    if len(parts) < 2:
        msg = f"parsed root {root!r}; missing <channel_slug>"
        raise ValueError(msg)

    channel_slug = parts[1]
    if len(parts) == 2:
        return _ParsedPath("channel_dir", root, channel_slug, absolute_path, absolute_path / "channel.md")

    if len(parts) == 3:
        if parts[2] == "channel.md":
            return _ParsedPath("channel_file", root, channel_slug, absolute_path, absolute_path)
        msg = f"parsed {parts!r}; expected channel.md or <YYYY-MM>/<DD>/<file>"
        raise ValueError(msg)

    if len(parts) == 4:
        msg = f"parsed date directory {parts!r}; choose channel.md, feed.md, or a thread file"
        raise ValueError(msg)

    date_str = _parse_date_parts(parts[2], parts[3], parts)
    if len(parts) == 5:
        if parts[4] in _DAY_FILES:
            return _ParsedPath("day_file", root, channel_slug, absolute_path, absolute_path, date_str=date_str)
        msg = f"parsed day path {parts!r}; expected channel.md or feed.md"
        raise ValueError(msg)

    if len(parts) == 6:
        if parts[5] in _THREAD_FILES:
            return _ParsedPath(
                "thread_file",
                root,
                channel_slug,
                absolute_path,
                absolute_path,
                date_str=date_str,
                thread_slug=parts[4],
            )
        msg = f"parsed thread path {parts!r}; expected thread.md or feed.md"
        raise ValueError(msg)

    msg = f"parsed {parts!r}; expected a channel root, day file, or thread file"
    raise ValueError(msg)


def _parse_date_parts(month: str, day: str, parts: list[str]) -> str:
    date_str = f"{month}-{day}"
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as e:
        msg = f"parsed {parts!r}; expected valid <YYYY-MM>/<DD> date components"
        raise ValueError(msg) from e
    return date_str


def _normalize_path(path: str) -> Path:
    expanded = os.path.expanduser(path)
    return Path(os.path.abspath(expanded))


def _read_frontmatter(path: Path) -> dict[str, str]:
    """Read a simple YAML frontmatter block from the top of a markdown file."""
    if not path.is_file():
        return {}
    try:
        lines = path.read_text().splitlines()
    except OSError:
        return {}

    if not lines or lines[0] != "---":
        return {}

    frontmatter: dict[str, str] = {}
    for line in lines[1:]:
        if line == "---":
            return frontmatter
        key, sep, value = line.partition(":")
        if sep:
            frontmatter[key.strip()] = _strip_yaml_scalar(value.strip())
    return {}


def _strip_yaml_scalar(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1]
    return value
