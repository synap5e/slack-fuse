"""Render Slack models to markdown."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from . import mrkdwn
from .models import Channel, Message, Thread

if TYPE_CHECKING:
    from .user_cache import UserCache


def render_channel_metadata(channel: Channel, users: UserCache | None = None) -> str:
    """Render channel.md at the channel root level (topic, purpose, etc)."""
    lines = [f"# #{channel.name}\n"]
    if channel.topic:
        lines.append(f"**Topic**: {mrkdwn.convert(channel.topic, users)}\n")
    if channel.purpose:
        lines.append(f"**Purpose**: {mrkdwn.convert(channel.purpose, users)}\n")
    if channel.num_members:
        lines.append(f"**Members**: {channel.num_members}\n")
    channel_type = "Private Channel" if channel.is_private else "Channel"
    if channel.is_im:
        channel_type = "Direct Message"
    elif channel.is_mpim:
        channel_type = "Group DM"
    lines.append(f"**Type**: {channel_type}\n")
    return "\n".join(lines)


def render_day_snapshot(
    channel: Channel,
    date: str,
    messages: list[Message],
    users: UserCache | None = None,
) -> str:
    """Render channel.md snapshot — current state, edits applied in place."""
    lines = [
        "---",
        f"channel: {channel.name}",
        f"date: {date}",
        "---\n",
    ]

    for msg in messages:
        lines.append(_render_message(msg, users))

    return "\n".join(lines)


def render_day_feed(
    channel: Channel,
    date: str,
    messages: list[Message],
    users: UserCache | None = None,
) -> str:
    """Render feed.md — append-only timeline."""
    lines = [
        "---",
        f"channel: {channel.name}",
        f"date: {date}",
        "type: feed",
        "---\n",
    ]

    for msg in messages:
        lines.append(_render_message(msg, users))

    return "\n".join(lines)


def render_thread_snapshot(
    thread: Thread,
    channel: Channel,
    users: UserCache | None = None,
) -> str:
    """Render thread.md — current state of a thread."""
    date = _ts_to_date(thread.parent.ts)
    lines = [
        "---",
        f"channel: {channel.name}",
        f"thread_ts: \"{thread.parent.ts}\"",
        f"reply_count: {len(thread.replies)}",
        f"date: {date}",
        "---\n",
    ]

    lines.append(_render_message(thread.parent, users, label="(parent)"))
    for reply in thread.replies:
        lines.append(_render_message(reply, users))

    return "\n".join(lines)


def render_thread_feed(
    thread: Thread,
    channel: Channel,
    users: UserCache | None = None,
) -> str:
    """Render thread feed.md — append-only timeline."""
    date = _ts_to_date(thread.parent.ts)
    lines = [
        "---",
        f"channel: {channel.name}",
        f"thread_ts: \"{thread.parent.ts}\"",
        f"date: {date}",
        "type: feed",
        "---\n",
    ]

    lines.append(_render_message(thread.parent, users, label="(parent)"))
    for reply in thread.replies:
        lines.append(_render_message(reply, users))

    return "\n".join(lines)


def _render_message(
    msg: Message,
    users: UserCache | None = None,
    label: str = "",
) -> str:
    """Render a single message as a markdown section."""
    ts_str = _ts_to_time(msg.ts)
    username = users.get_display_name(msg.user) if users else msg.user

    header = f"## {ts_str} @{username}"
    if label:
        header += f" {label}"
    if msg.edited:
        edit_time = _ts_to_time(msg.edited.ts)
        header += f" *(edited {edit_time})*"

    lines = [header, ""]

    if msg.text:
        lines.append(mrkdwn.convert(msg.text, users))
        lines.append("")

    # Reactions
    if msg.reactions:
        reaction_parts = [f":{r.name}: {r.count}" for r in msg.reactions]
        lines.append("  ".join(reaction_parts))
        lines.append("")

    # File attachments
    for f in msg.files:
        if f.is_huddle_canvas:
            lines.append(f"[Huddle Notes]({f.name})")
        else:
            lines.append(f"\U0001f4ce [{f.name}](attachments/{f.name})")
        lines.append("")

    # Thread indicator
    if msg.reply_count > 0 and msg.thread_ts == msg.ts:
        lines.append(f"> Thread: {msg.reply_count} replies")
        lines.append("")

    return "\n".join(lines)


def _ts_to_time(ts: str) -> str:
    """Convert Slack timestamp to HH:MM."""
    try:
        dt = datetime.fromtimestamp(float(ts), tz=UTC).astimezone()
        return dt.strftime("%H:%M")
    except (ValueError, OSError):
        return ts


def _ts_to_date(ts: str) -> str:
    """Convert Slack timestamp to YYYY-MM-DD."""
    try:
        dt = datetime.fromtimestamp(float(ts), tz=UTC).astimezone()
        return dt.strftime("%Y-%m-%d")
    except (ValueError, OSError):
        return "unknown"
