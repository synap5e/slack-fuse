# pyright: reportPrivateUsage=false
"""Tests for HTTP `/permalink` path -> URL behavior."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

import httpx

from slack_fuse_server._json import JsonObject
from slack_fuse_server.http.dto import (
    BackfillMetrics,
    MetricsResponse,
    PermalinkRequest,
    PermalinkResponse,
    RateLimitBudget,
    SlackMetrics,
    SubscribersMetrics,
)
from slack_fuse_server.http.handlers import ResolvePermalinkDeps
from slack_fuse_server.http.metrics import MetricsSource
from slack_fuse_server.http.permalink import resolve_path_to_permalink_url
from slack_fuse_server.http.resolve import ts_to_local_date
from slack_fuse_server.http.server import HttpRequest, route_request
from slack_fuse_server.slurper.api import SlackClient
from tests._fake_slack import make_fake_slack_transport


@dataclass(frozen=True, slots=True)
class _StaticUsers:
    names: dict[str, str]

    def get_display_name(self, user_id: str) -> str:
        return self.names.get(user_id, user_id)


@dataclass(frozen=True, slots=True)
class _StaticMetricsSource:
    payload: MetricsResponse

    def snapshot(self) -> MetricsResponse:
        return self.payload


def _sample_metrics() -> MetricsResponse:
    now = datetime(2026, 6, 8, 7, 0, 0, tzinfo=UTC)
    return MetricsResponse(
        server_started_at=now,
        slack=SlackMetrics(
            socket_mode_state="connected",
            last_event_at=now,
            rate_limit_budget=RateLimitBudget(remaining_pct=95),
            last_health_kind="slack_healthy",
        ),
        streams=[],
        backfill=BackfillMetrics(completed_count=0, aborted_count=0),
        subscribers=SubscribersMetrics(active_ws_connections=0),
    )


def _make_client(overrides: dict[str, JsonObject] | None = None) -> SlackClient:
    client = SlackClient(token="xoxp-test")
    client.close()
    client._http = httpx.Client(transport=make_fake_slack_transport(overrides=overrides), timeout=30.0)
    return client


def _deps(client: SlackClient, workspace_url: str | None = "https://workspace.slack.com") -> ResolvePermalinkDeps:
    return ResolvePermalinkDeps(
        client=client,
        users=_StaticUsers({"U0002": "Bob Brown"}),
        workspace_url=workspace_url,
    )


def test_permalink_channel_root() -> None:
    client = _make_client()
    try:
        url = resolve_path_to_permalink_url(
            "channels/general",
            client,
            _StaticUsers({"U0002": "Bob Brown"}),
            "https://workspace.slack.com",
        )
    finally:
        client.close()
    assert url == "https://workspace.slack.com/archives/C0001"


def test_permalink_channel_file() -> None:
    client = _make_client()
    try:
        url = resolve_path_to_permalink_url(
            "channels/general/channel.md",
            client,
            _StaticUsers({"U0002": "Bob Brown"}),
            "https://workspace.slack.com",
        )
    finally:
        client.close()
    assert url == "https://workspace.slack.com/archives/C0001"


def test_permalink_day_file_with_ts() -> None:
    client = _make_client()
    month, day = ts_to_local_date("1700000000.000100")
    try:
        url = resolve_path_to_permalink_url(
            f"channels/general/{month}/{day}/channel.md",
            client,
            _StaticUsers({"U0002": "Bob Brown"}),
            None,
            ts="1700000000.000100",
        )
    finally:
        client.close()
    assert url == "https://example.slack.com/archives/C0001/p1700000000000100"


def test_permalink_day_file_compact_date_with_ts() -> None:
    client = _make_client()
    month, day = ts_to_local_date("1700000000.000100")
    try:
        url = resolve_path_to_permalink_url(
            f"channels/general/{month}-{day}/channel.md",
            client,
            _StaticUsers({"U0002": "Bob Brown"}),
            None,
            ts="1700000000.000100",
        )
    finally:
        client.close()
    assert url == "https://example.slack.com/archives/C0001/p1700000000000100"


def test_permalink_thread_file_without_ts() -> None:
    client = _make_client()
    month, day = ts_to_local_date("1700000100.000200")
    try:
        url = resolve_path_to_permalink_url(
            f"channels/general/{month}/{day}/thanks-glad-to-be-here/thread.md",
            client,
            _StaticUsers({"U0002": "Bob Brown"}),
            None,
        )
    finally:
        client.close()
    assert url == "https://example.slack.com/archives/C0001/p1700000000000100"


def test_permalink_im_channel_root() -> None:
    client = _make_client()
    try:
        url = resolve_path_to_permalink_url(
            "dms/bob-brown",
            client,
            _StaticUsers({"U0002": "Bob Brown"}),
            "https://workspace.slack.com",
        )
    finally:
        client.close()
    assert url == "https://workspace.slack.com/archives/D0001"


def test_permalink_mpim_channel_root() -> None:
    conversations_list_override: JsonObject = {
        "ok": True,
        "channels": [
            {
                "id": "C0001",
                "name": "general",
                "is_member": True,
            },
            {
                "id": "D0001",
                "is_im": True,
                "is_member": True,
                "user": "U0002",
            },
            {
                "id": "G0001",
                "name": "Project Squad",
                "is_mpim": True,
                "is_member": True,
            },
        ],
        "response_metadata": {"next_cursor": ""},
    }
    client = _make_client(overrides={"conversations.list": conversations_list_override})
    try:
        url = resolve_path_to_permalink_url(
            "group-dms/project-squad",
            client,
            _StaticUsers({"U0002": "Bob Brown"}),
            "https://workspace.slack.com",
        )
    finally:
        client.close()
    assert url == "https://workspace.slack.com/archives/G0001"


def test_http_route_permalink_endpoint() -> None:
    client = _make_client()
    deps = _deps(client)
    request = PermalinkRequest(path="channels/general")
    try:
        response = route_request(
            HttpRequest(method="POST", target="/permalink", body=request.model_dump_json().encode("utf-8")),
            metrics_source=cast(MetricsSource, _StaticMetricsSource(_sample_metrics())),
            resolve_permalink_deps=deps,
        )
    finally:
        client.close()
    assert response.status_code == 200
    parsed = PermalinkResponse.model_validate_json(response.body)
    assert parsed.url == "https://workspace.slack.com/archives/C0001"
