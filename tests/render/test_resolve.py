"""Tests for `resolve_mentions` and the mention-extraction helpers.

`resolve_mentions` is the FUSE-read pass: it substitutes bare `<@U…>` / `<#C…>`
placeholders against the live resolver tables, falling back to the raw literal
when an id is absent (the startup / cross-stream-race window).
"""

from __future__ import annotations

from slack_fuse.models import Message
from slack_fuse_render import (
    ChannelId,
    UserId,
    extract_mention_channel_ids,
    extract_mention_user_ids,
    render_message_structural,
    resolve_mentions,
)
from tests.render._stubs import StubChannels, StubUsers

USERS = StubUsers({"U1": "Alice", "U2": "Bob"})
CHANNELS = StubChannels({"C1": "general", "C2": "random"})


def test_resolve_user_mention() -> None:
    assert resolve_mentions("hi <@U1>", USERS, CHANNELS) == "hi @Alice"


def test_resolve_channel_mention() -> None:
    assert resolve_mentions("see <#C1>", USERS, CHANNELS) == "see #general"


def test_resolve_mixed() -> None:
    out = resolve_mentions("<@U1> in <#C2> and <@U2>", USERS, CHANNELS)
    assert out == "@Alice in #random and @Bob"


def test_unknown_user_falls_back_to_id() -> None:
    assert resolve_mentions("<@U999>", USERS, CHANNELS) == "@U999"


def test_unknown_channel_falls_back_to_id() -> None:
    assert resolve_mentions("<#C999>", USERS, CHANNELS) == "#C999"


def test_empty_string() -> None:
    assert resolve_mentions("", USERS, CHANNELS) == ""


def test_no_placeholders_untouched() -> None:
    assert resolve_mentions("plain **bold** text", USERS, CHANNELS) == "plain **bold** text"


def test_resolved_name_inserted_verbatim() -> None:
    # Resolution runs last, so a name with markdown chars is NOT re-formatted.
    users = StubUsers({"U1": "a_b_c"})
    assert resolve_mentions("<@U1>", users, CHANNELS) == "@a_b_c"


def test_raw_label_honoured_for_robustness() -> None:
    # Structural output never carries labels, but a raw labelled tag reaching
    # the resolver directly should still degrade gracefully to the label.
    assert resolve_mentions("<@U1|legacy>", StubUsers({}), CHANNELS) == "@legacy"


def test_round_trip_author_resolves() -> None:
    # The author placeholder written by the structural pass resolves cleanly.
    md = render_message_structural_text("U1", "hello <@U2>")
    out = resolve_mentions(md, USERS, CHANNELS)
    assert out.splitlines()[0].endswith("@Alice")
    assert "hello @Bob" in out


def render_message_structural_text(user: str, text: str) -> str:
    return render_message_structural(Message(ts="1700000000.000100", user=user, text=text))


def test_extract_user_ids() -> None:
    md = "hi <@U1> and <@U2> and <@U1> again"
    assert extract_mention_user_ids(md) == {UserId("U1"), UserId("U2")}


def test_extract_channel_ids() -> None:
    md = "see <#C1> and <#C2>"
    assert extract_mention_channel_ids(md) == {ChannelId("C1"), ChannelId("C2")}


def test_extract_empty() -> None:
    assert extract_mention_user_ids("no mentions here") == set()
    assert extract_mention_channel_ids("no mentions here") == set()


def test_extract_author_placeholder_included() -> None:
    # The author header placeholder is a real mention and must be extracted so
    # a rename of the author invalidates the chunk's inode.
    md = render_message_structural_text("U1", "body with <@U2>")
    assert extract_mention_user_ids(md) == {UserId("U1"), UserId("U2")}
