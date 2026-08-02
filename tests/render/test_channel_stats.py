"""Workspace channel inventory markdown rendering."""

from __future__ import annotations

from datetime import UTC, date, datetime

from slack_fuse.channel_stats import render_channel_stats
from slack_fuse.projector.channel_stats_fetch import ChannelStat, ChannelStats


def _channel(  # noqa: PLR0913 - compact renderer fixture builder.
    channel_id: str,
    name: str,
    total: int | None,
    *,
    ingested: int = 0,
    status: str = "not_started",
    is_member: bool = True,
    refresh_status: str = "ok",
) -> ChannelStat:
    return ChannelStat.model_validate({
        "channel_id": channel_id,
        "name": name,
        "total": total,
        "ingested": ingested,
        "status": status,
        "is_member": is_member,
        "is_archived": False,
        "is_blocked": status == "blocked",
        "created": date(2024, 8, 15),
        "refresh_status": refresh_status,
    })


def test_render_channel_stats_sorts_formats_and_maps_statuses() -> None:
    stats = ChannelStats(
        refreshed_at=datetime(2026, 6, 28, 4, tzinfo=UTC),
        workspace_message_total=44_853,
        channels=[
            _channel("C1", "small", 10, ingested=10, status="done").model_copy(update={"is_archived": True}),
            _channel("C2", "large|bot", 32_361, ingested=2, status="blocked"),
            _channel(
                "C3",
                "other",
                12_482,
                status="not_joined",
                is_member=False,
                refresh_status="approximate",
            ),
            _channel("C4", "unknown", None, status="unavailable"),
        ],
    )

    rendered = render_channel_stats(stats).decode()

    assert "4 channels — 44,853 total messages across the workspace (refreshed 2026-06-28T04:00:00Z)." in rendered
    assert rendered.index("large\\|bot") < rendered.index("other") < rendered.index("small") < rendered.index("unknown")
    assert "| large\\|bot | 32,361 | 2 | blocked | ✓ | 2024-08-15 |" in rendered
    assert "| other | ~12,482 | — | not-joined | — | 2024-08-15 |" in rendered
    assert "| small (archived) | 10 | 10 | done | ✓ | 2024-08-15 |" in rendered
    assert "| unknown | — | 0 | unavailable | ✓ | 2024-08-15 |" in rendered
