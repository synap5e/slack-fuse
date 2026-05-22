# pyright: reportPrivateUsage=false
"""Tests for the resolve module — URL parsing and path resolution helpers."""

from __future__ import annotations

from typing import cast

import pytest

from slack_fuse import _slug_helpers, resolve
from slack_fuse.api import SlackClient
from slack_fuse.models import Channel, JsonObject, Message
from slack_fuse.resolve import _conv_root, parse_permalink
from slack_fuse.user_cache import UserCache

from .stubs import stub_load_from_disk


class TestParsePermalink:
    def test_channel_message(self) -> None:
        cid, ts, thread_ts = parse_permalink(
            "https://comfy-organization.slack.com/archives/C09LDUKDQ1K/p1775493247936389"
        )
        assert cid == "C09LDUKDQ1K"
        assert ts == "1775493247.936389"
        assert thread_ts is None

    def test_thread_reply(self) -> None:
        cid, ts, thread_ts = parse_permalink(
            "https://comfy-organization.slack.com/archives/C09LDUKDQ1K/p1775493247936389"
            "?thread_ts=1775490000.000000&cid=C09LDUKDQ1K"
        )
        assert cid == "C09LDUKDQ1K"
        assert ts == "1775493247.936389"
        assert thread_ts == "1775490000.000000"

    def test_short_microsecond_part(self) -> None:
        """Timestamps with fewer microsecond digits should still parse."""
        cid, ts, _ = parse_permalink("https://workspace.slack.com/archives/C123ABC/p1700000000000100")
        assert cid == "C123ABC"
        assert ts == "1700000000.000100"

    def test_channel_only_url(self) -> None:
        """`/archives/<C>` with no `/p<ts>` parses as a channel-only URL."""
        cid, ts, thread_ts = parse_permalink("https://comfy-organization.slack.com/archives/C0AMT1A1YBV")
        assert cid == "C0AMT1A1YBV"
        assert ts is None
        assert thread_ts is None

    def test_channel_only_url_trailing_slash(self) -> None:
        cid, ts, thread_ts = parse_permalink("https://workspace.slack.com/archives/C123/")
        assert cid == "C123"
        assert ts is None
        assert thread_ts is None

    def test_rejects_non_archives_path(self) -> None:
        with pytest.raises(ValueError, match="Not a Slack archives URL"):
            parse_permalink("https://workspace.slack.com/messages/C123")

    def test_rejects_missing_p_prefix(self) -> None:
        with pytest.raises(ValueError, match="Not a Slack message permalink"):
            parse_permalink("https://workspace.slack.com/archives/C123/1234567890123456")

    def test_rejects_non_numeric_timestamp(self) -> None:
        with pytest.raises(ValueError, match="Invalid timestamp"):
            parse_permalink("https://workspace.slack.com/archives/C123/pabcdefghijk")

    def test_rejects_short_timestamp(self) -> None:
        with pytest.raises(ValueError, match="Invalid timestamp"):
            parse_permalink("https://workspace.slack.com/archives/C123/p12345")


# === _conv_root ===


class TestConvRoot:
    def test_im(self) -> None:
        ch = Channel.model_validate({"id": "D1", "is_im": True, "user": "U1"})
        assert _conv_root(ch) == "dms"

    def test_mpim(self) -> None:
        ch = Channel.model_validate({"id": "G1", "name": "group", "is_mpim": True})
        assert _conv_root(ch) == "group-dms"

    def test_member_channel(self) -> None:
        ch = Channel.model_validate({"id": "C1", "name": "general", "is_member": True})
        assert _conv_root(ch) == "channels"

    def test_non_member_channel(self) -> None:
        ch = Channel.model_validate({"id": "C1", "name": "general", "is_member": False})
        assert _conv_root(ch) == "other-channels"


def test_resolve_caches_api_fetched_day(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resolving an uncached thread permalink should warm disk state used by FUSE."""
    channel = Channel.model_validate({"id": "C1", "name": "general", "is_member": True})
    parent = Message(
        ts="1700000000.000000",
        user="U1",
        text="Thread Topic",
        thread_ts="1700000000.000000",
        reply_count=1,
    )
    day_writes: dict[tuple[str, str], list[JsonObject]] = {}
    known_date_writes: dict[str, set[str]] = {}

    def cached_channel_list() -> list[JsonObject]:
        return [cast("JsonObject", channel.model_dump(mode="json"))]

    def no_day_messages(_channel_id: str, _date_str: str) -> list[JsonObject] | None:
        return None

    def no_known_dates(_channel_id: str) -> set[str] | None:
        return None

    def capture_day_messages(channel_id: str, date_str: str, messages: list[JsonObject]) -> None:
        day_writes[channel_id, date_str] = messages

    def capture_known_dates(channel_id: str, dates: set[str]) -> None:
        known_date_writes[channel_id] = dates

    def get_history(
        _channel_id: str,
        oldest: str | None = None,
        latest: str | None = None,
        limit: int = 200,
    ) -> list[Message]:
        return [parent]

    monkeypatch.setattr(UserCache, "_load_from_disk", stub_load_from_disk)
    monkeypatch.setattr(_slug_helpers, "get_channel_list", cached_channel_list)
    monkeypatch.setattr(_slug_helpers, "get_day_messages", no_day_messages)
    monkeypatch.setattr(_slug_helpers, "get_known_dates", no_known_dates)
    monkeypatch.setattr(_slug_helpers, "put_day_messages", capture_day_messages)
    monkeypatch.setattr(_slug_helpers, "put_known_dates", capture_known_dates)

    client = SlackClient(token="xoxp-fake")
    monkeypatch.setattr(client, "get_history", get_history)
    users = UserCache(client.http)

    try:
        path = resolve.resolve_permalink(
            "https://workspace.slack.com/archives/C1/p1700000000000000",
            "/mnt/slack",
            client,
            users,
        )
    finally:
        client.close()

    month, day = resolve._ts_to_local_date(parent.ts)
    date_str = f"{month}-{day}"
    assert path == f"/mnt/slack/channels/general/{month}/{day}/thread-topic/thread.md"
    assert day_writes["C1", date_str][0]["ts"] == parent.ts
    assert known_date_writes["C1"] == {date_str}


def test_resolve_thread_reply_uses_parent_date_not_reply_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bug 1: a thread reply URL with a thread_ts on a different day must
    land under the parent's date, not the reply's date.

    Reply ts 1777968798.279369 = May 5 UTC
    Parent ts 1777491885.581959 = Apr 29 UTC
    Resolver was overriding month/day with the reply's date in the
    fallback path; with the parent on disk it should now land in the
    parent's day's thread dir.
    """
    channel = Channel.model_validate({"id": "C1", "name": "general", "is_member": True})
    parent_ts = "1777491885.581959"
    reply_ts = "1777968798.279369"
    parent = Message(
        ts=parent_ts,
        user="U1",
        text="Bug under discussion",
        thread_ts=parent_ts,
        reply_count=1,
    )

    parent_month, parent_day = resolve._ts_to_local_date(parent_ts)
    parent_date_str = f"{parent_month}-{parent_day}"

    def cached_channel_list() -> list[JsonObject]:
        return [cast("JsonObject", channel.model_dump(mode="json"))]

    def cached_day_messages(channel_id: str, date_str: str) -> list[JsonObject] | None:
        if channel_id == "C1" and date_str == parent_date_str:
            return [cast("JsonObject", parent.model_dump(mode="json"))]
        return None

    monkeypatch.setattr(UserCache, "_load_from_disk", stub_load_from_disk)
    monkeypatch.setattr(_slug_helpers, "get_channel_list", cached_channel_list)
    monkeypatch.setattr(_slug_helpers, "get_day_messages", cached_day_messages)

    client = SlackClient(token="xoxp-fake")
    users = UserCache(client.http)

    try:
        path = resolve.resolve_permalink(
            f"https://workspace.slack.com/archives/C1/p{reply_ts.replace('.', '')}"
            f"?thread_ts={parent_ts}&cid=C1",
            "/mnt/slack",
            client,
            users,
        )
    finally:
        client.close()

    assert path == f"/mnt/slack/channels/general/{parent_month}/{parent_day}/bug-under-discussion/thread.md"


def test_resolve_thread_url_raises_when_slug_unresolvable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bug 2: a URL that explicitly names a thread must not silently
    fall back to channel.md. The caller asked for a thread, so a miss
    is a hard error so they can react (refresh, retry, surface to user)."""
    channel = Channel.model_validate({"id": "C1", "name": "general", "is_member": True})

    # No replies in the cached parent message → slug map won't see it as a thread.
    parent_ts = "1777491885.581959"
    plain_parent = Message(ts=parent_ts, user="U1", text="No replies yet")

    def cached_channel_list() -> list[JsonObject]:
        return [cast("JsonObject", channel.model_dump(mode="json"))]

    def cached_day_messages(_channel_id: str, _date_str: str) -> list[JsonObject] | None:
        return [cast("JsonObject", plain_parent.model_dump(mode="json"))]

    monkeypatch.setattr(UserCache, "_load_from_disk", stub_load_from_disk)
    monkeypatch.setattr(_slug_helpers, "get_channel_list", cached_channel_list)
    monkeypatch.setattr(_slug_helpers, "get_day_messages", cached_day_messages)

    client = SlackClient(token="xoxp-fake")
    users = UserCache(client.http)

    try:
        with pytest.raises(resolve.PermalinkResolutionError, match=parent_ts):
            _ = resolve.resolve_permalink(
                f"https://workspace.slack.com/archives/C1/p1777968798279369?thread_ts={parent_ts}&cid=C1",
                "/mnt/slack",
                client,
                users,
            )
    finally:
        client.close()


def test_resolve_channel_only_url_returns_channel_dir(monkeypatch: pytest.MonkeyPatch) -> None:
    """Channel-only URLs (no /p<ts>) resolve to the channel directory."""
    channel = Channel.model_validate({"id": "C1", "name": "general", "is_member": True})

    def cached_channel_list() -> list[JsonObject]:
        return [cast("JsonObject", channel.model_dump(mode="json"))]

    monkeypatch.setattr(UserCache, "_load_from_disk", stub_load_from_disk)
    monkeypatch.setattr(_slug_helpers, "get_channel_list", cached_channel_list)

    client = SlackClient(token="xoxp-fake")
    users = UserCache(client.http)

    try:
        path = resolve.resolve_permalink(
            "https://comfy-organization.slack.com/archives/C1",
            "/mnt/slack",
            client,
            users,
        )
    finally:
        client.close()

    assert path == "/mnt/slack/channels/general"
