# pyright: reportPrivateUsage=false
"""Byte-equivalence harness for POC B (renderer split).

Proves that for realistic inputs the single-pass production renderer
``slack_fuse.mrkdwn.convert(text, users)`` and the candidate two-pass pipeline
``resolve_mentions(convert_structural(text), users, channels)`` produce
byte-identical output.

The intentional, RFC-aligned divergences (late resolution beats stale labels;
channels resolved against a live table the single pass never had) are isolated
into their own tests at the bottom, which assert the *new* behaviour and
explain why it differs.
"""

from __future__ import annotations

import pytest

from slack_fuse import mrkdwn
from slack_fuse_poc_b import mrkdwn_split


class _StubUsers:
    def __init__(self, names: dict[str, str]) -> None:
        self._names = names

    def get_display_name(self, user_id: str) -> str:
        return self._names.get(user_id, "")


class _StubChannels:
    def __init__(self, names: dict[str, str]) -> None:
        self._names = names

    def get_channel_name(self, channel_id: str) -> str:
        return self._names.get(channel_id, "")


# A consistent workspace: the live tables agree with every cached label that
# appears in the equivalence corpus. This is the realistic case — Slack's API
# emits `<@U…|name>` / `<#C…|name>` with the current name, and the FUSE server
# holds the same names in its users/channels tables.
USERS = _StubUsers({
    "U1": "Alice",
    "U2": "Bob",
    "U3": "Zoë",  # unicode display name
    "U4": "carol-eng",
})
CHANNELS = _StubChannels({
    "C1": "general",
    "C2": "random",
})


def assert_equivalent(
    input_text: str,
    users: mrkdwn_split.UserResolver | None = USERS,
    channels: mrkdwn_split.ChannelResolver | None = CHANNELS,
) -> None:
    expected = mrkdwn.convert(input_text, users)  # type: ignore[arg-type]
    structural = mrkdwn_split.convert_structural(input_text)
    actual = mrkdwn_split.resolve_mentions(structural, users, channels)
    assert actual == expected, (
        f"diverged for input: {input_text!r}\n"
        f"structural: {structural!r}\n"
        f"expected:   {expected!r}\n"
        f"actual:     {actual!r}"
    )


# --- Equivalence corpus -------------------------------------------------------
# Inputs where the two-pass pipeline MUST match the single pass byte-for-byte.

EQUIVALENT_INPUTS = [
    # empties / plain
    "",
    "plain text with no markup at all",
    # formatting primitives
    "*bold*",
    "_italic_",
    "~strike~",
    "*bold* then _italic_ then ~strike~ together",
    "a*b*c",  # mid-word bold pair
    "snake_case_word",  # _case_ becomes italic in BOTH passes
    "2 * 3 is math not bold",  # single asterisk, no pair
    # special mentions
    "<!here>",
    "<!channel>",
    "<!everyone>",
    "ping <!here> and <!channel> and <!everyone>",
    # subteams
    "<!subteam^S1|@team-eng>",
    "<!subteam^S1>",
    "cc <!subteam^SABC|@ops> please",
    # links
    "<https://example.com>",
    "<https://example.com|click here>",
    "see <https://a.com|A> and <https://b.com>",
    # user mentions (bare: both consult the table; labelled: label == table)
    "<@U1>",
    "<@U1|Alice>",
    "<@U999>",  # unknown user, falls back to id in both
    "hi <@U1> and <@U2>",
    "<@U1>:",  # trailing punctuation
    "<@U4>",  # display name with a dash
    "<@U3>",  # unicode display name
    "<@U3|Zoë>",  # unicode label matching the table
    # channel mentions (labelled, label == table; bare id NOT in table)
    "<#C1|general>",
    "<#C2|random>",
    "<#CZZZ>",  # bare id absent from the channels table -> id in both
    # mentions next to formatting (placeholder is formatting-transparent)
    "*<@U1>*",
    "_<@U1>_",
    "~<@U2>~",
    # mentions inside code / blockquote (NEITHER pass protects these — matched)
    "`<@U1>`",
    "> quoted <@U1> in a blockquote",
    # newlines
    "line one\nline two has *bold*\nline three <@U2>",
    # kitchen sink
    "*Hi* <@U1>, welcome to <#C1|general> — see <https://x.com|docs> <!here> _ping_ ~not this~ <!subteam^S2|@ops>",
    "multi <@U1> <@U2> <#C1|general> <#C2|random> line",
]


@pytest.mark.parametrize("input_text", EQUIVALENT_INPUTS)
def test_byte_equivalence(input_text: str) -> None:
    assert_equivalent(input_text)


def test_equivalence_with_no_resolvers() -> None:
    # No users/channels at all: both passes fall back to raw ids.
    for text in ["<@U1>", "<#C1>", "hi <@U1> in <#C1>"]:
        expected = mrkdwn.convert(text, None)
        actual = mrkdwn_split.resolve_mentions(mrkdwn_split.convert_structural(text), None, None)
        assert actual == expected, (text, expected, actual)


def test_corpus_size() -> None:
    # Acceptance criterion: 30+ inputs in the equivalence corpus.
    assert len(EQUIVALENT_INPUTS) >= 30


# --- Intentional divergences --------------------------------------------------
# These are NOT bugs in the structural pass. They are the design consequences
# of deferring resolution. Each test pins the *new* behaviour and documents the
# single-pass behaviour it departs from.


def test_divergence_stale_label_loses_to_live_table() -> None:
    """RFC's whole point: a stale cached label must not beat the live name.

    Single pass trusts the inline label; two-pass strips it so the live users
    table wins. With a renamed user the outputs differ — by design.
    """
    text = "<@U1|alice-OLD>"
    users = _StubUsers({"U1": "alice-new"})
    single = mrkdwn.convert(text, users)  # type: ignore[arg-type]
    two = mrkdwn_split.convert_two_pass(text, users, CHANNELS)
    assert single == "@alice-OLD"  # trusts the stale cached label
    assert two == "@alice-new"  # resolves against the live table
    assert single != two


def test_divergence_labelled_mention_when_user_absent() -> None:
    """Stripping the label loses the inline fallback name on a table miss.

    Single pass shows the cached label; two-pass, having stripped it, shows the
    raw id when the user isn't in the live table. In production the table is
    populated at startup so misses are rare, but it is a real tradeoff.
    """
    text = "<@U1|alice>"
    empty = _StubUsers({})
    single = mrkdwn.convert(text, empty)  # type: ignore[arg-type]
    two = mrkdwn_split.convert_two_pass(text, empty, None)
    assert single == "@alice"
    assert two == "@U1"
    assert single != two


def test_divergence_bare_channel_with_table_entry() -> None:
    """Two-pass resolves bare channel ids the single pass left as raw ids.

    The single-pass `convert` never had a channels table: a bare `<#C…>` (no
    label) always renders as `#<id>`. The two-pass design resolves it against
    the live channels table. Strictly an improvement, but a byte divergence.
    Bare channel ids without a label do not occur in Slack API output.
    """
    text = "see <#C1>"
    single = mrkdwn.convert(text, USERS)  # type: ignore[arg-type]
    two = mrkdwn_split.convert_two_pass(text, USERS, CHANNELS)
    assert single == "see #C1"  # raw id — no table in the single pass
    assert two == "see #general"  # resolved against the live table
    assert single != two


def test_divergence_resolved_name_with_markdown_chars() -> None:
    """Resolution-before-formatting vs after: names with `*_~` differ.

    The single pass resolves first, so a display name containing markdown
    characters is then mangled by the bold/italic/strike transforms. The
    two-pass design resolves last, so the name is inserted verbatim. The
    two-pass output is arguably more correct (display names shouldn't be
    markdown-transformed) but it diverges byte-wise.
    """
    text = "<@U6>"
    users = _StubUsers({"U6": "a_b_c"})
    single = mrkdwn.convert(text, users)  # type: ignore[arg-type]
    two = mrkdwn_split.convert_two_pass(text, users, None)
    assert single == "@a*b*c"  # `_b_` italicised after resolution
    assert two == "@a_b_c"  # name inserted verbatim, never formatted
    assert single != two


def test_divergence_mention_nested_in_link_label() -> None:
    """Malformed mrkdwn: a mention inside a link label.

    The single pass resolves the mention before the link regex runs, so the
    link's `[^>]+` label sees clean text. The two-pass design defers
    resolution, so the link regex consumes the mention's `>` as its own
    terminator, mangling both. This requires nested `<…>` entities, which
    Slack's API never emits, so it does not affect real data.
    """
    text = "<https://x|see <@U1>>"
    single = mrkdwn.convert(text, USERS)  # type: ignore[arg-type]
    two = mrkdwn_split.convert_two_pass(text, USERS, CHANNELS)
    assert single == "[see @Alice](https://x)"
    assert two == "[see <@U1](https://x)>"
    assert single != two
