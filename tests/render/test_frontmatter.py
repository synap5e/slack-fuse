# pyright: reportPrivateUsage=false
"""Tests for the frontmatter helpers."""

from __future__ import annotations

from slack_fuse.models import Message
from slack_fuse_render import (
    ChannelId,
    ChannelView,
    channel_md_frontmatter,
    thread_md_frontmatter,
)
from slack_fuse_render.render import _ts_to_date

CHANNEL = ChannelView(channel_id=ChannelId("C1"), name="general", is_im=False, is_mpim=False)


def test_channel_frontmatter() -> None:
    assert channel_md_frontmatter(CHANNEL, "2026-06-08") == (
        "---\nchannel: general\nchannel_id: C1\ndate: 2026-06-08\n---\n"
    )


def test_thread_frontmatter() -> None:
    parent = Message(ts="1700000000.000100", user="U1", text="root", reply_count=3)
    out = thread_md_frontmatter(CHANNEL, parent)
    assert out == (
        "---\n"
        "channel: general\n"
        "channel_id: C1\n"
        'thread_ts: "1700000000.000100"\n'
        "reply_count: 3\n"
        f"date: {_ts_to_date(parent.ts)}\n"
        "---\n"
    )


def test_frontmatter_ends_with_delimiter_newline() -> None:
    # Composition prepends this block to the body, so it must end with `---\n`.
    assert channel_md_frontmatter(CHANNEL, "2026-06-08").endswith("---\n")
    parent = Message(ts="1700000000.000100", user="U1")
    assert thread_md_frontmatter(CHANNEL, parent).endswith("---\n")
