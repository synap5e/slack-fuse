"""Candidate two-pass split of ``slack_fuse.mrkdwn.convert``.

The production renderer (`slack_fuse/mrkdwn.py:convert`) does structural
mrkdwn->markdown conversion *and* `<@U…>` / `<#C…>` resolution in one pass,
taking a `UserCache`. The server-split RFC defers mention resolution to
read-time so chunks can be stored without baking in (possibly stale) display
names. This module is the candidate split:

* :func:`convert_structural` — pure, resolver-free. Runs at chunk-write time.
  Performs every structural transform the single-pass `convert` does, but
  instead of *resolving* `<@U…>` / `<#C…>` it *normalises* them: any cached
  inline label (``<@U123|alice>`` / ``<#C1|general>``) is stripped, leaving a
  bare ``<@U123>`` / ``<#C1>`` placeholder. Bold/italic/strike/link/special
  transforms run here.
* :func:`resolve_mentions` — substitutes the bare placeholders against the
  live users/channels tables. Runs at read time, last, so resolved names are
  never subject to the markdown formatting transforms.

See ``docs/plans/poc-reports/poc-b.md`` for the equivalence analysis and the
(intentional, RFC-aligned) divergences this split introduces vs the single
pass.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import Callable

# Regexes lifted verbatim from slack_fuse/mrkdwn.py so the structural pass
# matches the single pass byte-for-byte on the transforms it keeps.
_USER_MENTION = re.compile(r"<@(U[A-Z0-9]+)(?:\|([^>]+))?>")
_CHANNEL_MENTION = re.compile(r"<#(C[A-Z0-9]+)(?:\|([^>]+))?>")
_LINK = re.compile(r"<(https?://[^|>]+)(?:\|([^>]+))?>")
_SUBTEAM_LABELED = re.compile(r"<!subteam\^[A-Z0-9]+\|(@[^>]+)>")
_SUBTEAM = re.compile(r"<!subteam\^([A-Z0-9]+)>")
_BOLD = re.compile(r"(?<!\*)\*([^*\n]+)\*(?!\*)")
_ITALIC = re.compile(r"(?<!_)_([^_\n]+)_(?!_)")
_STRIKE = re.compile(r"(?<!~)~([^~\n]+)~(?!~)")


class UserResolver(Protocol):
    """Subset of ``UserCache`` the resolver needs."""

    def get_display_name(self, user_id: str) -> str: ...


class ChannelResolver(Protocol):
    """Local channel-id -> name lookup for read-time channel resolution."""

    def get_channel_name(self, channel_id: str) -> str: ...


def convert_structural(text: str) -> str:
    """Structural mrkdwn -> markdown conversion, resolver-free.

    Mention tags are normalised (cached labels stripped) but left as
    ``<@U…>`` / ``<#C…>`` placeholders for :func:`resolve_mentions`.
    """
    if not text:
        return ""

    result = text

    # User mentions: strip any cached label, keep a bare placeholder. This runs
    # at the same position the single pass resolved users, so the downstream
    # structural transforms see the same surrounding text.
    result = _USER_MENTION.sub(lambda m: f"<@{m.group(1)}>", result)

    # Channel references: strip any cached label, keep a bare placeholder.
    result = _CHANNEL_MENTION.sub(lambda m: f"<#{m.group(1)}>", result)

    # Links: <http://url|label> or <http://url> (purely structural).
    result = _LINK.sub(_link_repl, result)

    # Special mentions.
    result = result.replace("<!here>", "@here")
    result = result.replace("<!channel>", "@channel")
    result = result.replace("<!everyone>", "@everyone")

    # Subteam mentions: <!subteam^S123|@team-name> or <!subteam^S123>.
    result = _SUBTEAM_LABELED.sub(r"\1", result)
    result = _SUBTEAM.sub(r"@\1", result)

    # Bold / italic / strike.
    result = _BOLD.sub(r"**\1**", result)
    result = _ITALIC.sub(r"*\1*", result)
    result = _STRIKE.sub(r"~~\1~~", result)

    return result


def resolve_mentions(
    text: str,
    users: UserResolver | None = None,
    channels: ChannelResolver | None = None,
) -> str:
    """Substitute bare ``<@U…>`` / ``<#C…>`` placeholders against live tables.

    Runs at read time, last. Resolved names are inserted verbatim and are not
    subject to the markdown formatting transforms (which already ran in
    :func:`convert_structural`).
    """
    if not text:
        return ""

    def _resolve_user(m: re.Match[str]) -> str:
        user_id = m.group(1)
        # The structural pass strips inline labels, so group(2) is always None
        # here for structural output. We still honour it for robustness if a
        # raw tag reaches the resolver directly.
        label = m.group(2)
        if label:
            return f"@{label}"
        if users is not None:
            name = users.get_display_name(user_id)
            if name:
                return f"@{name}"
        return f"@{user_id}"

    result = _USER_MENTION.sub(_resolve_user, text)

    def _resolve_channel(m: re.Match[str]) -> str:
        channel_id = m.group(1)
        label = m.group(2)
        if label:
            return f"#{label}"
        if channels is not None:
            name = channels.get_channel_name(channel_id)
            if name:
                return f"#{name}"
        return f"#{channel_id}"

    return _CHANNEL_MENTION.sub(_resolve_channel, result)


def _link_repl(m: re.Match[str]) -> str:
    url = m.group(1)
    label = m.group(2)
    if label:
        return f"[{label}]({url})"
    return url


# Convenience: the full pipeline, mirroring the single-pass `convert` signature
# but with the read-time channels table threaded through.
def convert_two_pass(
    text: str,
    users: UserResolver | None = None,
    channels: ChannelResolver | None = None,
) -> str:
    """Structural pass then mention pass — the production read path in one call."""
    return resolve_mentions(convert_structural(text), users, channels)


if TYPE_CHECKING:
    _: Callable[[str], str] = convert_structural
