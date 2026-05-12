# pyright: reportPrivateUsage=false
"""Tests for reverse FUSE path -> Slack permalink resolution."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pytest

from slack_fuse import _slug_helpers
from slack_fuse.api import SlackClient
from slack_fuse.models import Channel, JsonObject, Message
from slack_fuse.permalink import _read_frontmatter, resolve_path_to_permalink
from slack_fuse.user_cache import UserCache


class _StubClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.channels = [Channel.model_validate({"id": "C1", "name": "general", "is_member": True})]

    def get_permalink(self, channel_id: str, message_ts: str) -> str:
        self.calls.append((channel_id, message_ts))
        return f"https://workspace.slack.com/archives/{channel_id}/p{message_ts.replace('.', '')}"

    def list_conversations(self) -> list[Channel]:
        return self.channels

    def get_channel_info(self, channel_id: str) -> Channel:
        for channel in self.channels:
            if channel.id == channel_id:
                return channel
        return Channel.model_validate({"id": channel_id, "name": channel_id, "is_member": True})

    def get_history(
        self,
        _channel_id: str,
        oldest: str | None = None,
        latest: str | None = None,
        limit: int = 200,
    ) -> list[Message]:
        return []


class _StubUsers:
    def get_display_name(self, user_id: str) -> str:
        return user_id


@dataclass(frozen=True)
class _PathCase:
    relative_path: str
    frontmatter_relative: str
    ts: str | None
    workspace_url: str | None
    expected: str


def _client(stub: _StubClient) -> SlackClient:
    return cast("SlackClient", stub)


def _users() -> UserCache:
    return cast("UserCache", _StubUsers())


def _write(path: Path, frontmatter: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(frontmatter)


def _frontmatter(*, channel_id: str = "C1", thread_ts: str | None = None) -> str:
    lines = ["---", "channel: general", f"channel_id: {channel_id}"]
    if thread_ts is not None:
        lines.append(f'thread_ts: "{thread_ts}"')
    lines.extend(["---", "body"])
    return "\n".join(lines)


@pytest.mark.parametrize(
    "case",
    [
        _PathCase(
            "channels/general",
            "channels/general/channel.md",
            None,
            "https://workspace.slack.com",
            "archives/C1",
        ),
        _PathCase(
            "channels/general/channel.md",
            "channels/general/channel.md",
            None,
            "https://workspace.slack.com",
            "archives/C1",
        ),
        _PathCase(
            "channels/general/2026-04/09/channel.md",
            "channels/general/2026-04/09/channel.md",
            "1712649600.123456",
            None,
            "p1712649600123456",
        ),
        _PathCase(
            "channels/general/2026-04/09/feed.md",
            "channels/general/2026-04/09/feed.md",
            "1712649600.123456",
            None,
            "p1712649600123456",
        ),
        _PathCase(
            "channels/general/2026-04/09/thread-title/thread.md",
            "channels/general/2026-04/09/thread-title/thread.md",
            None,
            None,
            "p1712649600123456",
        ),
        _PathCase(
            "channels/general/2026-04/09/thread-title/feed.md",
            "channels/general/2026-04/09/thread-title/feed.md",
            None,
            None,
            "p1712649600123456",
        ),
    ],
)
def test_path_types_resolve(tmp_path: Path, case: _PathCase) -> None:
    mountpoint = tmp_path / "slack"
    _write(mountpoint / case.frontmatter_relative, _frontmatter(thread_ts="1712649600.123456"))
    stub = _StubClient()

    url = resolve_path_to_permalink(
        str(mountpoint / case.relative_path),
        str(mountpoint),
        _client(stub),
        _users(),
        case.workspace_url,
        ts=case.ts,
    )

    assert case.expected in url


@pytest.mark.parametrize(
    ("relative_path", "frontmatter_relative", "ts", "workspace_url"),
    [
        (
            ".cached-only/channels/general",
            ".cached-only/channels/general/channel.md",
            None,
            "https://workspace.slack.com",
        ),
        (
            ".cached-only/channels/general/channel.md",
            ".cached-only/channels/general/channel.md",
            None,
            "https://workspace.slack.com",
        ),
        (
            ".cached-only/channels/general/2026-04/09/channel.md",
            ".cached-only/channels/general/2026-04/09/channel.md",
            "1712649600.123456",
            None,
        ),
        (
            ".cached-only/channels/general/2026-04/09/thread-title/thread.md",
            ".cached-only/channels/general/2026-04/09/thread-title/thread.md",
            None,
            None,
        ),
    ],
)
def test_cached_only_path_prefix_resolves(
    tmp_path: Path,
    relative_path: str,
    frontmatter_relative: str,
    ts: str | None,
    workspace_url: str | None,
) -> None:
    mountpoint = tmp_path / "slack"
    _write(mountpoint / frontmatter_relative, _frontmatter(thread_ts="1712649600.123456"))
    stub = _StubClient()

    url = resolve_path_to_permalink(
        str(mountpoint / relative_path),
        str(mountpoint),
        _client(stub),
        _users(),
        workspace_url,
        ts=ts,
    )

    assert "C1" in url


def test_frontmatter_parser_reads_known_keys_and_ignores_extra(tmp_path: Path) -> None:
    path = tmp_path / "thread.md"
    path.write_text(
        "\n".join([
            "---",
            "channel: general",
            "channel_id: C1",
            'thread_ts: "1712649600.123456"',
            "unused: ok",
            "---",
            "body",
        ])
    )

    parsed = _read_frontmatter(path)

    assert parsed["channel_id"] == "C1"
    assert parsed["thread_ts"] == "1712649600.123456"
    assert parsed["unused"] == "ok"


def test_frontmatter_parser_tolerates_missing_keys(tmp_path: Path) -> None:
    path = tmp_path / "channel.md"
    path.write_text("---\nchannel: general\n---\nbody")

    parsed = _read_frontmatter(path)

    assert parsed == {"channel": "general"}


def test_fallback_slug_reversal_when_frontmatter_lacks_channel_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mountpoint = tmp_path / "slack"
    _write(mountpoint / "channels/general/channel.md", "---\nchannel: general\n---\nbody")
    channel = Channel.model_validate({"id": "C1", "name": "general", "is_member": True})

    def cached_channel_list() -> list[JsonObject]:
        return [cast("JsonObject", channel.model_dump(mode="json"))]

    monkeypatch.setattr(_slug_helpers, "get_channel_list", cached_channel_list)

    url = resolve_path_to_permalink(
        str(mountpoint / "channels/general/channel.md"),
        str(mountpoint),
        _client(_StubClient()),
        _users(),
        "https://workspace.slack.com/",
    )

    assert url == "https://workspace.slack.com/archives/C1"


def test_thread_slug_fallback_when_frontmatter_lacks_thread_ts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mountpoint = tmp_path / "slack"
    _write(
        mountpoint / "channels/general/2026-04/09/thread-title/thread.md",
        _frontmatter().replace("body", "body"),
    )
    parent = Message(
        ts="1712649600.123456",
        user="U1",
        text="Thread Title",
        thread_ts="1712649600.123456",
        reply_count=1,
    )

    def cached_day_messages(_channel_id: str, _date_str: str) -> list[JsonObject]:
        return [cast("JsonObject", parent.model_dump(mode="json"))]

    monkeypatch.setattr(_slug_helpers, "get_day_messages", cached_day_messages)
    stub = _StubClient()

    url = resolve_path_to_permalink(
        str(mountpoint / "channels/general/2026-04/09/thread-title/thread.md"),
        str(mountpoint),
        _client(stub),
        _users(),
        None,
    )

    assert url.endswith("/p1712649600123456")
    assert stub.calls == [("C1", "1712649600.123456")]


def test_day_file_without_ts_is_ambiguous(tmp_path: Path) -> None:
    mountpoint = tmp_path / "slack"
    _write(mountpoint / "channels/general/2026-04/09/channel.md", _frontmatter())

    with pytest.raises(ValueError, match="day file is ambiguous"):
        resolve_path_to_permalink(
            str(mountpoint / "channels/general/2026-04/09/channel.md"),
            str(mountpoint),
            _client(_StubClient()),
            _users(),
            None,
        )


def test_channel_root_without_workspace_url_errors(tmp_path: Path) -> None:
    mountpoint = tmp_path / "slack"
    _write(mountpoint / "channels/general/channel.md", _frontmatter())

    with pytest.raises(ValueError, match="SLACK_WORKSPACE_URL"):
        resolve_path_to_permalink(
            str(mountpoint / "channels/general/channel.md"),
            str(mountpoint),
            _client(_StubClient()),
            _users(),
            None,
        )


def test_get_permalink_called_with_expected_values(tmp_path: Path) -> None:
    mountpoint = tmp_path / "slack"
    _write(mountpoint / "channels/general/2026-04/09/channel.md", _frontmatter())
    _write(
        mountpoint / "channels/general/2026-04/09/thread-title/thread.md",
        _frontmatter(thread_ts="1712649600.111111"),
    )
    stub = _StubClient()

    resolve_path_to_permalink(
        str(mountpoint / "channels/general/2026-04/09/channel.md"),
        str(mountpoint),
        _client(stub),
        _users(),
        None,
        ts="1712649600.222222",
    )
    resolve_path_to_permalink(
        str(mountpoint / "channels/general/2026-04/09/thread-title/thread.md"),
        str(mountpoint),
        _client(stub),
        _users(),
        None,
    )
    resolve_path_to_permalink(
        str(mountpoint / "channels/general/2026-04/09/thread-title/thread.md"),
        str(mountpoint),
        _client(stub),
        _users(),
        None,
        ts="1712649600.333333",
    )

    assert stub.calls == [
        ("C1", "1712649600.222222"),
        ("C1", "1712649600.111111"),
        ("C1", "1712649600.333333"),
    ]


def test_rejects_path_outside_mountpoint(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="not under mountpoint"):
        resolve_path_to_permalink(
            str(tmp_path / "elsewhere" / "channels" / "general"),
            str(tmp_path / "slack"),
            _client(_StubClient()),
            _users(),
            "https://workspace.slack.com",
        )
