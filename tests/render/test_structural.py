# pyright: reportPrivateUsage=false
"""Tests for `render_message_structural` — the chunk-write structural pass.

The output must carry unresolved `<@U…>` / `<#C…>` placeholders (resolved later
by `resolve_mentions`), apply the structural mrkdwn transforms to the body, and
render the author as a late-resolvable placeholder when it is a real user id.

Times are asserted via the module's own `_ts_to_time` helper so the suite stays
timezone-independent.
"""

from __future__ import annotations

from slack_fuse.models import Edited, FileAttachment, Message, Reaction
from slack_fuse_render import convert_structural, render_message_structural
from slack_fuse_render.render import _ts_to_time

_TS = "1700000000.000100"


def test_author_real_user_is_placeholder() -> None:
    md = render_message_structural(Message(ts=_TS, user="U123", text="hi"))
    assert md.startswith(f"## {_ts_to_time(_TS)} <@U123>")


def test_author_bot_or_unknown_is_literal() -> None:
    # Bot ids and the "unknown" sentinel are not <@U…> placeholders, so they
    # are emitted as literal @<id> and survive resolution untouched.
    bot = render_message_structural(Message(ts=_TS, user="B999", text="x"))
    unknown = render_message_structural(Message(ts=_TS, user="unknown", text="x"))
    assert bot.startswith(f"## {_ts_to_time(_TS)} @B999")
    assert unknown.startswith(f"## {_ts_to_time(_TS)} @unknown")


def test_body_structural_transforms_applied() -> None:
    md = render_message_structural(Message(ts=_TS, user="U1", text="*bold* and _it_"))
    assert "**bold** and *it*" in md


def test_body_mention_label_normalised() -> None:
    # Cached labels are stripped to bare placeholders; channel refs too.
    md = render_message_structural(
        Message(ts=_TS, user="U1", text="hi <@U2|bob> see <#C1|general>"),
    )
    assert "hi <@U2> see <#C1>" in md
    assert "|bob>" not in md
    assert "|general>" not in md


def test_body_mention_placeholder_preserved_in_code() -> None:
    # The structural pass does not protect code spans (matches the legacy pass).
    md = render_message_structural(Message(ts=_TS, user="U1", text="`<@U2>`"))
    assert "`<@U2>`" in md


def test_blockquote_passes_through() -> None:
    md = render_message_structural(Message(ts=_TS, user="U1", text="> quoted <@U2>"))
    assert "> quoted <@U2>" in md


def test_reactions_rendered() -> None:
    msg = Message(
        ts=_TS,
        user="U1",
        text="x",
        reactions=(Reaction(name="thumbsup", count=3), Reaction(name="eyes", count=1)),
    )
    md = render_message_structural(msg)
    assert ":thumbsup: 3  :eyes: 1" in md


def test_files_rendered() -> None:
    msg = Message(
        ts=_TS,
        user="U1",
        files=(
            FileAttachment(id="F1", name="diagram.png"),
            FileAttachment(id="F2", name="notes.md", is_huddle_canvas=True),
        ),
    )
    md = render_message_structural(msg)
    assert "\U0001f4ce [diagram.png](attachments/diagram.png)" in md
    assert "[Huddle Notes](notes.md)" in md


def test_edited_suffix_in_header() -> None:
    edit_ts = "1700000900.000200"
    msg = Message(ts=_TS, user="U1", text="x", edited=Edited(user="U1", ts=edit_ts))
    md = render_message_structural(msg)
    assert f"*(edited {_ts_to_time(edit_ts)})*" in md.splitlines()[0]


def test_thread_indicator_for_parent() -> None:
    msg = Message(ts=_TS, user="U1", text="root", thread_ts=_TS, reply_count=4)
    md = render_message_structural(msg)
    assert "> Thread: 4 replies" in md


def test_no_thread_indicator_for_reply() -> None:
    # A reply (thread_ts != ts) carries no thread indicator.
    msg = Message(ts="1700000500.000300", user="U1", text="reply", thread_ts=_TS, reply_count=4)
    md = render_message_structural(msg)
    assert "> Thread:" not in md


def test_empty_text_omits_body() -> None:
    md = render_message_structural(Message(ts=_TS, user="U1"))
    # Header, then a single trailing blank line — no body paragraph.
    assert md == f"## {_ts_to_time(_TS)} <@U1>\n"


def test_structural_pass_is_idempotent() -> None:
    # Running the structural body transform twice is a no-op on its own output.
    once = convert_structural("*b* <@U1|x> <#C1|y> <https://e.com|e>")
    assert convert_structural(once) == once
