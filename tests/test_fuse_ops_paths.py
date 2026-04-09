# pyright: reportPrivateUsage=false
"""Tests for the pure path-parsing helpers in slack_fuse.fuse_ops.

These don't touch the kernel — they're plain method calls on a SlackFuseOps
instance. We give it a real (stub-backed) SlackStore so type checking is happy.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from slack_fuse import disk_cache
from slack_fuse.api import SlackClient
from slack_fuse.fuse_ops import SlackFuseOps
from slack_fuse.store import SlackStore
from slack_fuse.user_cache import UserCache

from .stubs import (
    stub_get_channel_list,
    stub_get_huddle_index,
    stub_get_known_dates,
    stub_load_from_disk,
)


@pytest.fixture
def ops(monkeypatch: pytest.MonkeyPatch) -> Iterator[SlackFuseOps]:
    monkeypatch.setattr(disk_cache, "get_channel_list", stub_get_channel_list)
    monkeypatch.setattr(disk_cache, "get_huddle_index", stub_get_huddle_index)
    monkeypatch.setattr(disk_cache, "get_known_dates", stub_get_known_dates)
    monkeypatch.setattr(UserCache, "_load_from_disk", stub_load_from_disk)
    client = SlackClient(token="xoxp-fake")
    users = UserCache(token="xoxp-fake")
    yield SlackFuseOps(SlackStore(client=client, users=users))


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("/", []),
        ("/channels", ["channels"]),
        ("/channels/foo/bar", ["channels", "foo", "bar"]),
        ("/channels/foo/", ["channels", "foo"]),
    ],
)
def test_parse_path(ops: SlackFuseOps, path: str, expected: list[str]) -> None:
    assert ops._parse_path(path) == expected


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("/channels/foo", ("/channels/foo", False)),
        ("/.cached-only/channels/foo", ("/channels/foo", True)),
        ("/.cached-only", ("/", True)),
        ("/", ("/", False)),
        # Substring of the prefix must NOT match
        ("/.cached-onlyish/foo", ("/.cached-onlyish/foo", False)),
    ],
)
def test_strip_cached_prefix(ops: SlackFuseOps, path: str, expected: tuple[str, bool]) -> None:
    assert ops._strip_cached_prefix(path) == expected


@pytest.mark.parametrize(
    "parts",
    [
        ["channels", "general", "2026-04", "09", "thread", "huddles", "huddle-1430", "index"],
        ["dms", "alice", "2026-04", "09", "topic", "huddles", "huddle-0900", "index"],
        ["group-dms", "x", "2026-04", "09", "thread", "huddles", "huddle-1100", "index"],
    ],
)
def test_is_index_backlink_positive(ops: SlackFuseOps, parts: list[str]) -> None:
    assert ops._is_index_backlink(parts) is True


@pytest.mark.parametrize(
    ("parts", "reason"),
    [
        (["channels", "x", "y"], "too short"),
        (
            ["huddles", "x", "2026-04", "09", "thread", "huddles", "h", "index"],
            "wrong root",
        ),
        (
            ["channels", "x", "2026-04", "09", "thread", "NOT_HUDDLES", "h", "index"],
            "wrong segment 5",
        ),
        (
            ["channels", "x", "2026-04", "09", "thread", "huddles", "h", "notes.md"],
            "wrong segment 7",
        ),
    ],
)
def test_is_index_backlink_negative(
    ops: SlackFuseOps,
    parts: list[str],
    reason: str,
) -> None:
    assert ops._is_index_backlink(parts) is False


def test_list_dir_root_returns_top_level_dirs(ops: SlackFuseOps) -> None:
    entries = ops._list_dir_impl("/")
    names = [name for name, _ in entries]
    assert all(is_dir for _, is_dir in entries)
    for expected in ("channels", "dms", "group-dms", "other-channels", "huddles", ".cached-only"):
        assert expected in names


def test_list_dir_cached_only_root_strips_self(ops: SlackFuseOps) -> None:
    """Listing /.cached-only must not nest a .cached-only entry inside itself."""
    entries = ops._list_dir("/.cached-only")
    names = [name for name, _ in entries]
    assert ".cached-only" not in names
    assert "channels" in names
