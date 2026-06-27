# pyright: reportPrivateUsage=false
"""Tests for `render_message_structural` — the chunk-write structural pass.

The output must carry unresolved `<@U…>` / `<#C…>` placeholders (resolved later
by `resolve_mentions`), apply the structural mrkdwn transforms to the body, and
render the author as a late-resolvable placeholder when it is a real user id.

Times are asserted via the module's own `_ts_to_time` helper so the suite stays
timezone-independent.
"""

from __future__ import annotations

from slack_fuse.models import Attachment, Edited, FileAttachment, Message, Reaction
from slack_fuse_render import convert_structural, render_message_structural
from slack_fuse_render.render import _ts_to_time

_TS = "1700000000.000100"


# === attachments — app unfurls + bot/webhook posts ===


def test_attachment_with_title_and_text_renders_both() -> None:
    """The common shape for Linear / GitHub / Datadog: rich attachment
    with title, title_link, and a body. Render as bold-linked heading +
    structural body."""
    msg = Message(
        ts=_TS,
        user="B999",
        attachments=(
            Attachment(
                title="FE-740 Bug: Deleted generated assets...",
                title_link="https://linear.app/comfyorg/issue/FE-740",
                text="In Review (High priority)",
                fallback="FE-740 Bug: Deleted generated assets...",
            ),
        ),
    )
    md = render_message_structural(msg)
    assert "**[FE-740 Bug: Deleted generated assets...](https://linear.app/comfyorg/issue/FE-740)**" in md
    assert "In Review (High priority)" in md
    # Fallback isn't duplicated when the richer fields render.
    assert md.count("FE-740 Bug: Deleted generated assets...") == 1


def test_attachment_falls_back_to_fallback_when_other_fields_empty() -> None:
    """The minimal-shape attachment where only ``fallback`` is set — the
    Slack-side spec guarantees it for any attachment, so it's the safe
    last-resort body."""
    msg = Message(
        ts=_TS,
        user="B999",
        attachments=(Attachment(fallback="raw fallback body only"),),
    )
    md = render_message_structural(msg)
    assert "raw fallback body only" in md


def test_attachment_with_only_from_url_renders_link() -> None:
    """App unfurls that arrive with no title/text/fallback but a
    ``from_url`` — render as a bare link."""
    msg = Message(
        ts=_TS,
        user="B999",
        attachments=(Attachment(from_url="https://linear.app/comfyorg/issue/FE-814"),),
    )
    md = render_message_structural(msg)
    assert "<https://linear.app/comfyorg/issue/FE-814>" in md


def test_attachment_pretext_renders_above_title() -> None:
    """``pretext`` is the prelude that some integrations emit (e.g. GitHub
    PR alerts: 'a new pull request was opened'). Should render before the
    title block."""
    msg = Message(
        ts=_TS,
        user="B999",
        attachments=(
            Attachment(
                pretext="a new issue was opened",
                title="FE-740",
                title_link="https://linear.app/comfyorg/issue/FE-740",
            ),
        ),
    )
    md = render_message_structural(msg)
    pretext_idx = md.index("a new issue was opened")
    title_idx = md.index("**[FE-740]")
    assert pretext_idx < title_idx


def test_multiple_attachments_render_in_order() -> None:
    """Linear unfurls can attach multiple issues at once."""
    msg = Message(
        ts=_TS,
        user="B999",
        attachments=(
            Attachment(title="FE-740", fallback="FE-740"),
            Attachment(title="FE-814", fallback="FE-814"),
        ),
    )
    md = render_message_structural(msg)
    fe740_idx = md.index("FE-740")
    fe814_idx = md.index("FE-814")
    assert fe740_idx < fe814_idx


def test_empty_attachments_tuple_does_not_alter_output() -> None:
    """A message with no attachments should render identically to the
    pre-attachment code — pin the no-op case."""
    plain = render_message_structural(Message(ts=_TS, user="U1", text="hello"))
    with_empty = render_message_structural(
        Message(ts=_TS, user="U1", text="hello", attachments=()),
    )
    assert plain == with_empty


def test_attachment_text_passes_through_structural_transforms() -> None:
    """``text`` in attachments should be processed through ``convert_structural``
    so ``<@U…>`` placeholders survive and structural mrkdwn is normalized."""
    msg = Message(
        ts=_TS,
        user="B999",
        attachments=(Attachment(text="see <@U1234> for details"),),
    )
    md = render_message_structural(msg)
    # The structural converter preserves <@U…> placeholders so they can
    # resolve later — the test asserts the placeholder is intact.
    assert "<@U1234>" in md


def test_attachment_with_only_text_renders_text() -> None:
    """A degenerate but real shape: only ``text`` is set. The renderer
    should still output it."""
    msg = Message(
        ts=_TS,
        user="B999",
        attachments=(Attachment(text="alert: 5xx rate over 1%"),),
    )
    md = render_message_structural(msg)
    assert "alert: 5xx rate over 1%" in md


def test_linear_unfurl_shape_round_trip() -> None:
    """Pin the exact shape we saw in the production Linear unfurl that
    motivated this fix — see the investigation notes for the original
    bug. Asserts the attachment body now renders (was empty before)."""
    msg = Message(
        ts="1782508786.558189",
        user="U0AD9RYAKAL",  # the Linear bot user
        attachments=(
            Attachment(
                fallback="FE-740 Bug: Deleted generated assets still appear in cloud UI asset panel",
                from_url="https://linear.app/comfyorg/issue/FE-740/bug-deleted-generated-assets-still-appear-in-cloud-ui-asset-panel",
                is_app_unfurl=True,
            ),
            Attachment(
                from_url="https://linear.app/comfyorg/issue/FE-814/bug-cannot-delete-individual-images-within-a-job-stack",
            ),
        ),
    )
    md = render_message_structural(msg)
    assert "FE-740" in md
    assert "FE-814" in md


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
