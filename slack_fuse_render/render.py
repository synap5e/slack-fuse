"""Public renderer API — production implementation (Sprint 2B).

Per RFC §Renderer-as-library → Public API. Two-pass rendering: the structural
pass (`render_message_structural`) runs at chunk-write time and stores markdown
with unresolved `<@U…>`/`<#C…>` placeholders; `resolve_mentions` runs at
FUSE-read time to substitute them against the consumer's local tables.

Pure and stateless: no file I/O, no `UserCache` dependency. Display-name
resolution is a late-bound step performed via the `UserResolver` /
`ChannelResolver` protocols (`resolvers.py`).

The structural mrkdwn transforms live in `mrkdwn.py` (lifted from POC B); this
module composes them into message rendering, mention resolution/extraction, and
frontmatter helpers.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime

from slack_fuse.models import Message
from slack_fuse_render.mrkdwn import CHANNEL_MENTION, USER_MENTION, convert_structural
from slack_fuse_render.resolvers import (
    ChannelId,
    ChannelResolver,
    ChannelView,
    UserId,
    UserResolver,
)

# A bare user id, used to decide whether a message author can be rendered as a
# resolvable `<@U…>` placeholder. Bot ids (`B…`) and the `"unknown"` sentinel
# don't match, so they're emitted as literal `@<id>` and survive resolution.
_BARE_USER_ID = re.compile(r"^U[A-Z0-9]+$")


def render_message_structural(msg: Message) -> str:
    """Render a single message to markdown with unresolved `<@U…>`/`<#C…>`
    placeholders. Output is stored in `chunks.content_md`. Pure; no resolvers
    needed. Slack's `<@U…|cached-name>` form is normalised to `<@U…>`.

    The author header is itself emitted as a `<@U…>` placeholder (when the
    author is a real user id) so it resolves late against the live users table
    — a rename updates the header without re-rendering the chunk.
    """
    ts_str = _ts_to_time(msg.ts)
    author = f"<@{msg.user}>" if _BARE_USER_ID.match(msg.user) else f"@{msg.user}"

    header = f"## {ts_str} {author}"
    if msg.edited:
        header += f" *(edited {_ts_to_time(msg.edited.ts)})*"

    lines = [header, ""]

    if msg.text:
        lines.append(convert_structural(msg.text))
        lines.append("")

    if msg.reactions:
        reaction_parts = [f":{r.name}: {r.count}" for r in msg.reactions]
        lines.append("  ".join(reaction_parts))
        lines.append("")

    for f in msg.files:
        if f.is_huddle_canvas:
            lines.append(f"[Huddle Notes]({f.name})")
        else:
            lines.append(f"\U0001f4ce [{f.name}](attachments/{f.name})")
        lines.append("")

    if msg.reply_count > 0 and msg.thread_ts == msg.ts:
        lines.append(f"> Thread: {msg.reply_count} replies")
        lines.append("")

    return "\n".join(lines)


def resolve_mentions(
    md: str,
    users: UserResolver,
    channels: ChannelResolver,
) -> str:
    """Substitute `<@U…>` and `<#C…>` placeholders against the supplied
    resolvers. Called by the FUSE read layer during chunk concat. Unknown IDs
    fall back to the raw UID/CID literal (graceful degradation during the
    startup / cross-stream-race window).

    Resolved names are inserted verbatim, last, so they are never subject to the
    markdown formatting transforms (which already ran in `convert_structural`).
    """
    if not md:
        return ""

    def _resolve_user(m: re.Match[str]) -> str:
        user_id = m.group(1)
        # Structural output strips inline labels, so group(2) is normally None.
        # Honour it for robustness if a raw tag reaches the resolver directly.
        label = m.group(2)
        if label:
            return f"@{label}"
        view = users.resolve(UserId(user_id))
        if view is not None and view.display_name:
            return f"@{view.display_name}"
        return f"@{user_id}"

    result = USER_MENTION.sub(_resolve_user, md)

    def _resolve_channel(m: re.Match[str]) -> str:
        channel_id = m.group(1)
        label = m.group(2)
        if label:
            return f"#{label}"
        view = channels.resolve(ChannelId(channel_id))
        if view is not None and view.name:
            return f"#{view.name}"
        return f"#{channel_id}"

    return CHANNEL_MENTION.sub(_resolve_channel, result)


def channel_md_frontmatter(channel: ChannelView, date: str) -> str:
    """YAML frontmatter block prepended to a `channel.md` day file."""
    return f"---\nchannel: {channel.name}\nchannel_id: {channel.channel_id.value}\ndate: {date}\n---\n"


def thread_md_frontmatter(channel: ChannelView, parent: Message) -> str:
    """YAML frontmatter block prepended to a `thread.md` file."""
    return (
        "---\n"
        f"channel: {channel.name}\n"
        f"channel_id: {channel.channel_id.value}\n"
        f'thread_ts: "{parent.ts}"\n'
        f"reply_count: {parent.reply_count}\n"
        f"date: {_ts_to_date(parent.ts)}\n"
        "---\n"
    )


def extract_mention_user_ids(structural_md: str) -> set[UserId]:
    """Return the set of `UserId`s referenced by `<@U…>` placeholders in a
    structural chunk. Used to populate `chunk_mentions` (mention_kind='user')
    when the projector writes a chunk.
    """
    return {UserId(m.group(1)) for m in USER_MENTION.finditer(structural_md)}


def extract_mention_channel_ids(structural_md: str) -> set[ChannelId]:
    """Return the set of `ChannelId`s referenced by `<#C…>` placeholders in a
    structural chunk. Used to populate `chunk_mentions`
    (mention_kind='channel') when the projector writes a chunk.
    """
    return {ChannelId(m.group(1)) for m in CHANNEL_MENTION.finditer(structural_md)}


def _ts_to_time(ts: str) -> str:
    """Convert a Slack timestamp to local-tz HH:MM."""
    try:
        dt = datetime.fromtimestamp(float(ts), tz=UTC).astimezone()
        return dt.strftime("%H:%M")
    except (ValueError, OSError):
        return ts


def _ts_to_date(ts: str) -> str:
    """Convert a Slack timestamp to local-tz YYYY-MM-DD."""
    try:
        dt = datetime.fromtimestamp(float(ts), tz=UTC).astimezone()
        return dt.strftime("%Y-%m-%d")
    except (ValueError, OSError):
        return "unknown"
