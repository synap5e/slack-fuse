# pyright: reportPrivateUsage=false
"""Light coverage of the pure utility modules.

These (slug, mrkdwn, canvas html, transcript renderer, channel renderer, inode_map)
weren't directly touched by the I/O-boundary refactor, so coverage here is
deliberately thin: spot-check that the most-load-bearing pieces still work.
"""

from __future__ import annotations

import pyfuse3
import pytest

from slack_fuse.canvas import _html_to_markdown
from slack_fuse.inode_map import InodeMap
from slack_fuse.models import Channel, HuddleTranscription, Message, Thread
from slack_fuse.mrkdwn import convert
from slack_fuse.renderer import render_channel_metadata, render_day_snapshot, render_thread_snapshot
from slack_fuse.slug import slugify
from slack_fuse.transcript import _render_blocks

# === slug ===


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("Hello, World!", "hello-world"),
        ("Café résumé", "cafe-resume"),
        ("---weird---", "weird"),
        ("", ""),
    ],
)
def test_slugify(text: str, expected: str) -> None:
    assert slugify(text) == expected


def test_slugify_truncates_on_word_boundary() -> None:
    text = "this is a fairly long sentence that exceeds the maximum slug length by quite a bit"
    result = slugify(text)
    assert len(result) <= 60
    assert not result.endswith("-")
    assert result.startswith("this-is-a-fairly")


# === mrkdwn ===


class _StubResolver:
    def __init__(self, names: dict[str, str]) -> None:
        self._names = names

    def get_display_name(self, user_id: str) -> str:
        return self._names.get(user_id, "")


def test_mrkdwn_user_mentions_links_and_formatting() -> None:
    resolver = _StubResolver({"U1": "Alice"})
    text = "hi <@U1> in <#C2|random>, see <https://example.com|here>. *bold* _italic_ ~strike~ <!here>"
    out = convert(text, resolver)  # type: ignore[arg-type]
    assert "@Alice" in out
    assert "#random" in out
    assert "[here](https://example.com)" in out
    assert "**bold**" in out
    assert "*italic*" in out
    assert "~~strike~~" in out
    assert "@here" in out


def test_mrkdwn_unresolved_user_falls_back_to_id_and_empty() -> None:
    assert convert("hi <@U1>") == "hi @U1"
    assert convert("") == ""


# === canvas html_to_markdown ===


def test_canvas_html_returns_none_when_no_h1() -> None:
    assert _html_to_markdown("<p>nothing</p>") is None


def test_canvas_html_full_pipeline() -> None:
    page = (
        "<h1>Title</h1>"
        "<h2>Sub</h2>"
        "<p>tom &amp; jerry</p>"
        "<ul><li>one</li></ul>"
        '<p><a href="https://x.test">link</a></p>'
        "<p><b>BOLD</b> <i>ital</i></p>"
        '<p><img alt="wave"/>hi</p>'
        '<ul><li><input type="checkbox" checked /> done</li></ul>'
        "<hr/>"
        "<script>alert('x')</script>"
        "</body>"
    )
    md = _html_to_markdown(page)
    assert md is not None
    assert "# Title" in md
    assert "## Sub" in md
    assert "tom & jerry" in md
    assert "- one" in md
    assert "[link](https://x.test)" in md
    assert "**BOLD**" in md
    assert "*ital*" in md
    assert ":wave:" in md
    assert "[x]" in md
    assert "---" in md
    assert "alert" not in md


# === transcript renderer ===


def test_transcript_render_blocks() -> None:
    transcription = HuddleTranscription.model_validate({
        "blocks": {
            "elements": [
                {
                    "type": "rich_text_section",
                    "elements": [
                        {"type": "user", "user_id": "U1"},
                        {"type": "text", "text": " said "},
                        {"type": "text", "text": "stuff", "style": {"bold": True}},
                    ],
                },
                {
                    "type": "rich_text_section",
                    "elements": [{"type": "text", "text": "next"}],
                },
                # Empty + non-rich-text sections must be skipped
                {
                    "type": "rich_text_section",
                    "elements": [{"type": "text", "text": "   "}],
                },
                {"type": "divider", "elements": []},
            ]
        }
    })
    resolver = _StubResolver({"U1": "Alice"})
    out = _render_blocks(transcription, resolver)  # type: ignore[arg-type]
    assert out == "**@Alice** said **stuff**\n\nnext"


# === renderer ===


def test_renderer_channel_metadata_variants() -> None:
    public = Channel.model_validate({
        "id": "C1",
        "name": "general",
        "is_member": True,
        "num_members": 42,
        "topic": {"value": "T"},
    })
    private = Channel.model_validate({"id": "C2", "name": "secret", "is_private": True})
    im = Channel.model_validate({"id": "D1", "is_im": True, "user": "U1"})
    mpim = Channel.model_validate({"id": "G1", "name": "g", "is_mpim": True})

    assert "Channel" in render_channel_metadata(public)
    assert "**Members**: 42" in render_channel_metadata(public)
    assert "**Topic**: T" in render_channel_metadata(public)
    assert "Private Channel" in render_channel_metadata(private)
    assert "Direct Message" in render_channel_metadata(im)
    assert "Group DM" in render_channel_metadata(mpim)


def test_renderer_day_snapshot_renders_message_features() -> None:
    """Single test that exercises edits, reactions, files, threads."""
    ch = Channel.model_validate({"id": "C1", "name": "general", "is_member": True})
    msgs = [
        Message.model_validate({
            "ts": "1700000000.0",
            "user": "U1",
            "text": "starts a thread",
            "thread_ts": "1700000000.0",
            "reply_count": 4,
            "edited": {"user": "U1", "ts": "1700000010.0"},
            "reactions": [{"name": "thumbsup", "count": 3, "users": ["U1", "U2", "U3"]}],
            "files": [
                {"id": "F1", "name": "diagram.png"},
                {"id": "F2", "name": "Huddle Notes.canvas", "is_huddle_canvas": True},
            ],
        }),
    ]
    out = render_day_snapshot(ch, "2026-04-09", msgs)
    assert "---\nchannel: general\ndate: 2026-04-09" in out
    assert "(edited" in out
    assert ":thumbsup: 3" in out
    assert "diagram.png" in out
    assert "[Huddle Notes]" in out
    assert "Thread: 4 replies" in out


def test_renderer_thread_snapshot_includes_parent_label_and_metadata() -> None:
    ch = Channel.model_validate({"id": "C1", "name": "general", "is_member": True})
    parent = Message.model_validate({
        "ts": "1700000000.000100",
        "user": "U1",
        "text": "kickoff",
        "thread_ts": "1700000000.000100",
        "reply_count": 1,
    })
    reply = Message.model_validate({"ts": "1700000050.000200", "user": "U2", "text": "reply"})
    out = render_thread_snapshot(Thread(parent=parent, replies=(reply,)), ch)
    assert "(parent)" in out
    assert "reply_count: 1" in out
    assert 'thread_ts: "1700000000.000100"' in out


# === inode_map ===


def test_inode_map_stability_count_and_clear() -> None:
    m = InodeMap()
    assert m.get_path(pyfuse3.ROOT_INODE) == "/"
    start = m.count

    a = m.get_or_create("/channels/foo")
    again = m.get_or_create("/channels/foo")
    b = m.get_or_create("/channels/bar")

    assert a == again
    assert a != b
    assert m.get_path(a) == "/channels/foo"
    assert m.count == start + 2  # repeat must not increment

    m.clear()
    assert m.count == 1
    assert m.get_inode("/channels/foo") is None
    assert m.get_path(pyfuse3.ROOT_INODE) == "/"
