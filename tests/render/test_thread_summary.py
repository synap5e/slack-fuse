# pyright: reportPrivateUsage=false
"""Tests for the thread-summary marker and its two late resolutions.

One stored chunk serves two files. `render_message_structural` writes a thread
parent's summary as an unresolved marker; the day assembler turns it into a
link into the thread directory, and the thread assembler drops it. Chunks
written before the marker existed carry the literal `> Thread: N replies`
instead, and both resolutions must treat them identically — there is no
re-render behind this change.
"""

from __future__ import annotations

from slack_fuse.models import Message
from slack_fuse_render import (
    has_thread_summary,
    render_message_structural,
    resolve_thread_summary_link,
    strip_thread_summary,
    thread_summary_label,
    thread_summary_marker,
)
from slack_fuse_render.render import _ts_to_time

_TS = "1700000000.000100"
_LEGACY_CHUNK = "## 10:00 <@U1>\n\nFlaky asset test\n\n> Thread: 3 replies\n"


def _parent_chunk(reply_count: int) -> str:
    return render_message_structural(
        Message(ts=_TS, user="U1", text="Flaky asset test", thread_ts=_TS, reply_count=reply_count)
    )


# === structural pass: the marker, not the rendered line ===


def test_parent_emits_marker() -> None:
    assert '<thread-summary reply_count="3"/>' in _parent_chunk(3)


def test_parent_emits_no_rendered_summary_text() -> None:
    # The literal must not survive the structural pass — presentation is late.
    assert "Thread:" not in _parent_chunk(3)


def test_reply_emits_no_marker() -> None:
    # A reply (thread_ts != ts) is not a parent and summarises nothing.
    msg = Message(ts="1700000500.000300", user="U1", text="reply", thread_ts=_TS, reply_count=3)
    assert not has_thread_summary(render_message_structural(msg))


def test_parent_with_no_replies_emits_no_marker() -> None:
    assert not has_thread_summary(_parent_chunk(0))


def test_has_thread_summary_sees_both_forms() -> None:
    assert has_thread_summary(_parent_chunk(3))
    assert has_thread_summary(_LEGACY_CHUNK)
    assert not has_thread_summary("## 10:00 <@U1>\n\nno thread here\n")


# === pluralisation ===


def test_label_is_singular_for_one_reply() -> None:
    assert thread_summary_label(1) == "Thread: 1 reply"


def test_label_is_plural_for_many_replies() -> None:
    assert thread_summary_label(2) == "Thread: 2 replies"


def test_day_view_pluralises_a_single_reply() -> None:
    """The bug Simon's example exposed: the structural pass hard-coded
    "replies" for every count, so a one-reply thread read "1 replies" unless
    `_patch_thread_indicator` happened to have rewritten the chunk. Counting
    now happens once, at resolution.
    """
    resolved = resolve_thread_summary_link(_parent_chunk(1), "flaky-asset-test")
    assert "[Thread: 1 reply](flaky-asset-test/thread.md)" in resolved


# === day view: marker becomes a link ===


def test_day_view_links_marker_to_thread_file() -> None:
    resolved = resolve_thread_summary_link(_parent_chunk(3), "flaky-asset-test")
    assert "[Thread: 3 replies](flaky-asset-test/thread.md)" in resolved
    assert "<thread-summary" not in resolved


def test_day_view_link_is_relative_to_the_day_folder() -> None:
    # thread.md lives at <day>/<slug>/thread.md and the link is emitted from
    # <day>/channel.md, so the slug-relative form resolves without a prefix.
    resolved = resolve_thread_summary_link(_parent_chunk(3), "flaky-asset-test")
    assert "](flaky-asset-test/thread.md)" in resolved
    assert "](/" not in resolved


def test_day_view_without_a_slug_degrades_to_plain_text() -> None:
    """A parent whose slug has not been derived — or a read racing a rename —
    must never produce a link that goes nowhere.
    """
    resolved = resolve_thread_summary_link(_parent_chunk(3), None)
    assert "> Thread: 3 replies" in resolved
    assert "](" not in resolved
    assert "<thread-summary" not in resolved


def test_day_view_leaves_a_chunk_with_no_summary_alone() -> None:
    plain = "## 10:00 <@U1>\n\njust a message\n"
    assert resolve_thread_summary_link(plain, "some-slug") == plain


# === day view: legacy chunks ===


def test_day_view_links_legacy_literal() -> None:
    resolved = resolve_thread_summary_link(_LEGACY_CHUNK, "flaky-asset-test")
    assert "[Thread: 3 replies](flaky-asset-test/thread.md)" in resolved
    assert "> Thread:" not in resolved


def test_day_view_leaves_legacy_literal_alone_without_a_slug() -> None:
    assert resolve_thread_summary_link(_LEGACY_CHUNK, None) == _LEGACY_CHUNK


def test_day_view_pluralises_legacy_one_replies() -> None:
    """Legacy chunks written by the structural pass say "1 replies". The label
    is regenerated from the count, so the link reads correctly either way.
    """
    legacy = "## 10:00 <@U1>\n\nFlaky asset test\n\n> Thread: 1 replies\n"
    assert "[Thread: 1 reply](slug/thread.md)" in resolve_thread_summary_link(legacy, "slug")


# === thread view: marker disappears ===


def test_thread_view_strips_marker() -> None:
    assert strip_thread_summary(_parent_chunk(3)) == f"## {_ts_to_time(_TS)} <@U1>\n\nFlaky asset test\n"


def test_thread_view_leaves_no_trailing_gap() -> None:
    """A stripped chunk must concatenate exactly like one that never had a
    summary — otherwise the thread file grows a blank line per parent.
    """
    with_summary = strip_thread_summary(_parent_chunk(3))
    without = _parent_chunk(0)
    assert with_summary == without


def test_thread_view_strips_legacy_literal() -> None:
    assert strip_thread_summary(_LEGACY_CHUNK) == "## 10:00 <@U1>\n\nFlaky asset test\n"


def test_thread_view_leaves_a_chunk_with_no_summary_alone() -> None:
    plain = "## 10:00 <@U1>\n\njust a message\n"
    assert strip_thread_summary(plain) == plain


def test_thread_view_preserves_trailing_content_of_untouched_chunks() -> None:
    # The normalisation only runs when something was removed, so a chunk with
    # its own trailing whitespace is returned byte-identical.
    odd = "## 10:00 <@U1>\n\nbody\n\n\n"
    assert strip_thread_summary(odd) == odd


# === marker round-trip ===


def test_marker_round_trips_through_both_resolutions() -> None:
    chunk = _parent_chunk(7)
    assert thread_summary_marker(7) in chunk
    assert "[Thread: 7 replies](s/thread.md)" in resolve_thread_summary_link(chunk, "s")
    assert not has_thread_summary(strip_thread_summary(chunk))
