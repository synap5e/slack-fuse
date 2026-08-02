"""Client-side typed fetch of the channel inventory."""

from __future__ import annotations

import httpx

from slack_fuse.projector.channel_stats_fetch import fetch_channel_stats


def test_fetch_channel_stats_authenticates_and_validates() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/channel-stats"
        assert request.headers["x-slack-fuse-secret"] == "sek"
        return httpx.Response(
            200,
            json={
                "refreshed_at": "2026-06-28T04:00:00Z",
                "workspace_message_total": 12,
                "channels": [
                    {
                        "channel_id": "C1",
                        "name": "general",
                        "total": 12,
                        "ingested": 5,
                        "status": "in_progress",
                        "is_member": True,
                        "is_archived": False,
                        "is_blocked": False,
                        "created": "2024-08-15",
                        "refresh_status": "ok",
                    }
                ],
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        stats = fetch_channel_stats(client, "http://server:8765/", shared_secret="sek")

    assert stats.workspace_message_total == 12
    assert stats.channels[0].name == "general"
    assert stats.channels[0].created is not None
    assert stats.channels[0].created.isoformat() == "2024-08-15"
