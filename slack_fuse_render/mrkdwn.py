"""Structural mrkdwn -> markdown conversion (resolver-free, pure).

The structural pass of the two-pass renderer (RFC §Renderer-as-library). It
performs every structural transform the legacy single-pass
``slack_fuse.mrkdwn.convert`` does — bold/italic/strike/links/special/subteam
mentions — but instead of *resolving* ``<@U…>`` / ``<#C…>`` it *normalises*
them: any cached inline label (``<@U123|alice>`` / ``<#C1|general>``) is
stripped, leaving a bare ``<@U123>`` / ``<#C1>`` placeholder for the read-time
:func:`slack_fuse_render.resolve_mentions` pass.

Lifted from ``slack_fuse_poc_b/mrkdwn_split.py`` (the proven blueprint, whose
45 byte-equivalence tests pin this against the legacy single pass). The regexes
are kept byte-identical to ``slack_fuse/mrkdwn.py`` so the structural pass
matches the single pass on every transform it keeps.
"""

from __future__ import annotations

import re

# Regexes lifted verbatim from slack_fuse/mrkdwn.py so the structural pass
# matches the single pass byte-for-byte on the transforms it keeps. The mention
# regexes are also reused by render.py for resolution and extraction.
USER_MENTION = re.compile(r"<@(U[A-Z0-9]+)(?:\|([^>]+))?>")
CHANNEL_MENTION = re.compile(r"<#(C[A-Z0-9]+)(?:\|([^>]+))?>")
_LINK = re.compile(r"<(https?://[^|>]+)(?:\|([^>]+))?>")
_SUBTEAM_LABELED = re.compile(r"<!subteam\^[A-Z0-9]+\|(@[^>]+)>")
_SUBTEAM = re.compile(r"<!subteam\^([A-Z0-9]+)>")
_BOLD = re.compile(r"(?<!\*)\*([^*\n]+)\*(?!\*)")
_ITALIC = re.compile(r"(?<!_)_([^_\n]+)_(?!_)")
_STRIKE = re.compile(r"(?<!~)~([^~\n]+)~(?!~)")


def convert_structural(text: str) -> str:
    """Structural mrkdwn -> markdown conversion, resolver-free.

    Mention tags are normalised (cached labels stripped) but left as
    ``<@U…>`` / ``<#C…>`` placeholders for the read-time resolution pass.
    """
    if not text:
        return ""

    result = text

    # User mentions: strip any cached label, keep a bare placeholder. This runs
    # at the same position the single pass resolved users, so the downstream
    # structural transforms see the same surrounding text.
    result = USER_MENTION.sub(lambda m: f"<@{m.group(1)}>", result)

    # Channel references: strip any cached label, keep a bare placeholder.
    result = CHANNEL_MENTION.sub(lambda m: f"<#{m.group(1)}>", result)

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


def _link_repl(m: re.Match[str]) -> str:
    url = m.group(1)
    label = m.group(2)
    if label:
        return f"[{label}]({url})"
    return url
