"""Convert Slack mrkdwn to standard markdown."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .user_cache import UserCache


def convert(text: str, users: UserCache | None = None) -> str:
    """Convert Slack mrkdwn to standard markdown.

    Resolves user mentions and channel references when a UserCache is provided.
    """
    if not text:
        return ""

    result = text

    # User mentions: <@U123ABC> or <@U123ABC|display_name>
    def _resolve_user(m: re.Match[str]) -> str:
        user_id = m.group(1)
        label = m.group(2)
        if label:
            return f"@{label}"
        if users is not None:
            name = users.get_display_name(user_id)
            if name:
                return f"@{name}"
        return f"@{user_id}"

    result = re.sub(r"<@(U[A-Z0-9]+)(?:\|([^>]+))?>", _resolve_user, result)

    # Channel references: <#C123ABC|channel-name> or <#C123ABC>
    def _resolve_channel(m: re.Match[str]) -> str:
        label = m.group(2)
        if label:
            return f"#{label}"
        return f"#{m.group(1)}"

    result = re.sub(r"<#(C[A-Z0-9]+)(?:\|([^>]+))?>", _resolve_channel, result)

    # Links: <http://url|label> or <http://url>
    def _resolve_link(m: re.Match[str]) -> str:
        url = m.group(1)
        label = m.group(2)
        if label:
            return f"[{label}]({url})"
        return url

    result = re.sub(r"<(https?://[^|>]+)(?:\|([^>]+))?>", _resolve_link, result)

    # Special mentions
    result = result.replace("<!here>", "@here")
    result = result.replace("<!channel>", "@channel")
    result = result.replace("<!everyone>", "@everyone")

    # Subteam mentions: <!subteam^S123|@team-name> or <!subteam^S123>
    result = re.sub(r"<!subteam\^[A-Z0-9]+\|(@[^>]+)>", r"\1", result)
    result = re.sub(r"<!subteam\^([A-Z0-9]+)>", r"@\1", result)

    # Bold: Slack uses *text*, markdown uses **text**
    result = re.sub(r"(?<!\*)\*([^*\n]+)\*(?!\*)", r"**\1**", result)

    # Italic: Slack uses _text_, markdown uses *text*
    result = re.sub(r"(?<!_)_([^_\n]+)_(?!_)", r"*\1*", result)

    # Strikethrough: Slack uses ~text~, markdown uses ~~text~~
    result = re.sub(r"(?<!~)~([^~\n]+)~(?!~)", r"~~\1~~", result)

    return result
