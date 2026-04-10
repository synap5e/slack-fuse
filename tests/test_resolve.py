# pyright: reportPrivateUsage=false
"""Tests for the resolve module — URL parsing and path resolution helpers."""

from __future__ import annotations

import pytest

from slack_fuse.models import Channel
from slack_fuse.resolve import _conv_root, parse_permalink


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

    def test_rejects_non_archives_path(self) -> None:
        with pytest.raises(ValueError, match="Not a Slack message permalink"):
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
