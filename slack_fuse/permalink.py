"""Resolve FUSE paths back to Slack permalink URLs (v2 projections store)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from .__main__ import _resolve_local_zoneinfo  # pyright: ignore[reportPrivateUsage]
from .fuse_v2_helpers import (
    assign_conv_root_slugs,
    conv_root_for,
    dedup_thread_slug_map,
    fetch_channel_by_slug,
    fetch_day_thread_parents,
)

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow


type _PathKind = Literal["channel_dir", "channel_file", "day_file", "thread_file"]

_CONV_ROOTS = frozenset({"channels", "dms", "group-dms", "other-channels"})
_DAY_FILES = frozenset({"channel.md", "feed.md"})
_THREAD_FILES = frozenset({"thread.md", "feed.md"})


class PermalinkGenerationError(LookupError):
    """FUSE path parsed fine but couldn't be mapped to a Slack permalink.

    Distinct from ``ValueError`` (unparseable path): the target channel or
    thread isn't reachable from the local projections store (stale, blocked,
    or the path names a target that never materialised). Callers that care
    about thread-vs-channel distinctions should treat this as a hard miss
    rather than silently accepting a channel-level fallback.
    """


@dataclass(frozen=True)
class _ParsedPath:
    kind: _PathKind
    root: str
    channel_slug: str
    absolute_path: Path
    frontmatter_path: Path
    date_str: str | None = None
    thread_slug: str | None = None


def resolve_path_to_permalink(
    path: str,
    mountpoint: str,
    conn: Connection[TupleRow],
    workspace_url: str,
    *,
    ts: str | None = None,
) -> str:
    """Resolve a FUSE path to its Slack permalink URL.

    Uses the local v2 projections store to reverse channel-slug → ``channel_id``
    and thread-slug → ``thread_ts``. The URL is synthesized from
    ``workspace_url`` + ``channel_id`` + ``ts`` — no Slack API call, matching
    ``chat.getPermalink``'s output shape for the workspace-hosted archives.
    """
    parsed = _parse_path(path, mountpoint)
    frontmatter = _read_frontmatter(parsed.frontmatter_path)
    channel_id = _resolve_channel_id(parsed, frontmatter, conn)
    workspace_url = workspace_url.rstrip("/")

    if parsed.kind in ("channel_dir", "channel_file"):
        return f"{workspace_url}/archives/{channel_id}"

    if parsed.kind == "day_file":
        if ts is None:
            msg = "day file is ambiguous; pass --ts <message_ts>"
            raise ValueError(msg)
        return _build_message_url(workspace_url, channel_id, ts)

    # thread_file: prefer rendered frontmatter's ``thread_ts``; fall back
    # to the ledger-derived slug map for older renders.
    thread_ts = frontmatter.get("thread_ts") or _resolve_thread_ts(parsed, channel_id, conn)
    if ts is not None:
        # Specific reply within the thread: /p<reply>?thread_ts=<parent>.
        return _build_message_url(workspace_url, channel_id, ts, thread_ts=thread_ts)
    return _build_message_url(workspace_url, channel_id, thread_ts)


def _build_message_url(
    workspace_url: str,
    channel_id: str,
    message_ts: str,
    *,
    thread_ts: str | None = None,
) -> str:
    p_ts = message_ts.replace(".", "")
    url = f"{workspace_url}/archives/{channel_id}/p{p_ts}"
    if thread_ts is not None:
        url += f"?thread_ts={thread_ts}&cid={channel_id}"
    return url


def _resolve_channel_id(
    parsed: _ParsedPath,
    frontmatter: dict[str, str],
    conn: Connection[TupleRow],
) -> str:
    channel_id = frontmatter.get("channel_id")
    if channel_id:
        return channel_id
    row = fetch_channel_by_slug(conn, parsed.root, parsed.channel_slug, allow_hidden=True)
    if row is None:
        msg = (
            f"could not resolve channel slug {parsed.channel_slug!r} under {parsed.root!r}; "
            "rendered frontmatter has no channel_id and the local projection has no matching slug"
        )
        raise PermalinkGenerationError(msg)
    expected_root = conv_root_for(row)
    if parsed.root != expected_root:
        msg = (
            f"channel slug {parsed.channel_slug!r} belongs under {expected_root!r}, "
            f"not parsed root {parsed.root!r}"
        )
        raise PermalinkGenerationError(msg)
    # Confirm reachability: same channel appears in the assigned slug set for
    # its conv-root. Avoids returning a permalink for a slug that maps to a
    # channel the mount would hide (e.g. blocked / tier-changed since render).
    for candidate, _slug in assign_conv_root_slugs(conn, expected_root):
        if candidate.channel_id == row.channel_id:
            return row.channel_id
    msg = f"channel {row.channel_id} is not reachable in the mounted projection"
    raise PermalinkGenerationError(msg)


def _resolve_thread_ts(
    parsed: _ParsedPath,
    channel_id: str,
    conn: Connection[TupleRow],
) -> str:
    if parsed.date_str is None or parsed.thread_slug is None:
        msg = "thread path is missing a date directory or thread slug"
        raise ValueError(msg)
    tz = _resolve_local_zoneinfo()
    try:
        day = datetime.strptime(parsed.date_str, "%Y-%m-%d").date()
    except ValueError as e:
        msg = f"invalid date directory {parsed.date_str!r}"
        raise ValueError(msg) from e
    parents = fetch_day_thread_parents(conn, channel_id, day, tz)
    for slug, parent_ts in dedup_thread_slug_map(parents, conn).items():
        if slug == parsed.thread_slug:
            return _format_ts(parent_ts)
    msg = (
        f"could not resolve thread slug {parsed.thread_slug!r} for channel {channel_id!r} "
        f"on {parsed.date_str}; local projection may be stale — pass --ts <message_ts>"
    )
    raise PermalinkGenerationError(msg)


def _format_ts(value: Decimal) -> str:
    """Format a projected numeric ts back to Slack's canonical string form."""
    try:
        return f"{value:.6f}"
    except (ValueError, InvalidOperation) as e:  # pragma: no cover - Decimal is well-formed
        msg = f"could not format thread_ts {value!r}"
        raise PermalinkGenerationError(msg) from e


def _parse_path(path: str, mountpoint: str) -> _ParsedPath:  # noqa: C901  (path-depth dispatch hub)
    absolute_path = _normalize_path(path)
    absolute_mountpoint = _normalize_path(mountpoint)
    try:
        relative = absolute_path.relative_to(absolute_mountpoint)
    except ValueError as e:
        msg = f"path is not under mountpoint {absolute_mountpoint}: {absolute_path}"
        raise ValueError(msg) from e

    parts = list(relative.parts)
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
    """Read the simple key:value YAML frontmatter block at the top of a rendered file."""
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
