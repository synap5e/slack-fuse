"""Pure markdown renderer for ``/_workspace/channels.md``."""

from __future__ import annotations

from datetime import UTC, datetime

from slack_fuse.projector.channel_stats_fetch import ChannelStat, ChannelStats


def render_channel_stats(stats: ChannelStats) -> bytes:
    """Render the workspace inventory sorted by Slack total descending."""
    refresh_line = _format_refresh_line(stats)
    lines = [
        "# Workspace channels",
        "",
        f"{len(stats.channels):,} channels — {stats.workspace_message_total:,} total messages across the workspace.",
        refresh_line,
        "",
        "| Name | Messages | Ingested | Status | Member | Created |",
        "|---|---:|---:|---|---|---|",
    ]
    for channel in sorted(stats.channels, key=_sort_key):
        lines.append(_render_row(channel))
    lines.append("")
    return "\n".join(lines).encode()


def _sort_key(channel: ChannelStat) -> tuple[int, int, str, str]:
    return (
        channel.total is None,
        -(channel.total or 0),
        channel.name.casefold(),
        channel.channel_id,
    )


def _render_row(channel: ChannelStat) -> str:
    name = _escape_cell(channel.name)
    if channel.is_archived:
        name += " (archived)"
    if channel.total is None:
        total = "—"
    else:
        prefix = "~" if channel.refresh_status == "approximate" else ""
        total = f"{prefix}{channel.total:,}"
    ingested = "—" if channel.status == "not_joined" and channel.ingested == 0 else f"{channel.ingested:,}"
    status = channel.status.replace("_", "-")
    member = "✓" if channel.is_member else "—"
    created = channel.created.isoformat() if channel.created is not None else "—"
    return f"| {name} | {total} | {ingested} | {status} | {member} | {created} |"


def _escape_cell(value: str) -> str:
    return value.replace("\\", "\\\\").replace("|", "\\|").replace("\r", " ").replace("\n", " ")


def _format_refresh_line(stats: ChannelStats) -> str:
    if stats.refreshable_channels == 0:
        return "Refresh coverage: no channels are eligible for refresh."
    if stats.oldest_refreshed_at is None or stats.newest_refreshed_at is None:
        return f"Refresh coverage: 0/{stats.refreshable_channels} refreshable channels have a total yet."
    oldest = _format_ts(stats.oldest_refreshed_at)
    newest = _format_ts(stats.newest_refreshed_at)
    span = "at" if oldest == newest else f"between {oldest} and {newest}"
    return (
        f"Refresh coverage: {stats.refreshed_ok_channels}/{stats.refreshable_channels} "
        f"refreshable channels refreshed {span}."
    )


def _format_ts(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


__all__ = ["render_channel_stats"]
