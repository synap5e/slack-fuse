"""Typed sync fetcher for the workspace channel inventory endpoint."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field

DEFAULT_FETCH_TIMEOUT_S = 30.0  # /channel-stats returns 100+KB across 600+ channels;
# empirically 5-10s from LAN, 8-12s over Tailscale. 5s was too tight and caused
# warmer ReadTimeout on flow (2026-08-02). Server-side query optimisation is a
# follow-up — noted in BACKLOG.

type ChannelStatus = Literal[
    "done",
    "in_progress",
    "blocked",
    "not_started",
    "not_joined",
    "unavailable",
]


class _Payload(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ChannelStat(_Payload):
    channel_id: str
    name: str
    total: int | None
    ingested: int
    status: ChannelStatus
    is_member: bool
    is_archived: bool
    is_blocked: bool
    created: date | None
    refresh_status: str


class ChannelStats(_Payload):
    refreshed_at: datetime | None
    workspace_message_total: int
    channels: list[ChannelStat] = Field(default_factory=list)


def fetch_channel_stats(
    http_client: httpx.Client,
    base_http_url: str,
    *,
    shared_secret: str | None = None,
    timeout_s: float = DEFAULT_FETCH_TIMEOUT_S,
) -> ChannelStats:
    """``GET {base}/channel-stats`` and validate the complete response."""
    url = f"{base_http_url.rstrip('/')}/channel-stats"
    headers = {"x-slack-fuse-secret": shared_secret} if shared_secret else {}
    response = http_client.get(url, headers=headers, timeout=timeout_s)
    response.raise_for_status()
    return ChannelStats.model_validate_json(response.content)


__all__ = ["ChannelStat", "ChannelStats", "ChannelStatus", "fetch_channel_stats"]
