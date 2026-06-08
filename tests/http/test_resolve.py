# pyright: reportPrivateUsage=false
"""Tests for HTTP `/resolve` URL -> path behavior."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

import httpx

from slack_fuse_server._json import JsonObject
from slack_fuse_server.http.dto import (
    BackfillMetrics,
    MetricsResponse,
    RateLimitBudget,
    ResolveRequest,
    ResolveResponse,
    SlackMetrics,
    SubscribersMetrics,
)
from slack_fuse_server.http.handlers import ResolvePermalinkDeps, handle_resolve
from slack_fuse_server.http.metrics import MetricsSource
from slack_fuse_server.http.resolve import resolve_permalink_url, ts_to_local_date
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


def _deps(client: SlackClient) -> ResolvePermalinkDeps:
    return ResolvePermalinkDeps(
        client=client,
        users=_StaticUsers({"U0002": "Bob Brown"}),
        workspace_url="https://workspace.slack.com",
    )


def test_resolve_message_url() -> None:
    client = _make_client()
    try:
        month, day = ts_to_local_date("1700000000.000100")
        path = resolve_permalink_url(
            "https://workspace.slack.com/archives/C0001/p1700000000000100",
            client,
            _StaticUsers({"U0002": "Bob Brown"}),
        )
    finally:
        client.close()
    assert path == f"channels/general/{month}/{day}/channel.md"


def test_resolve_thread_url() -> None:
    client = _make_client()
    try:
        month, day = ts_to_local_date("1700000100.000200")
        path = resolve_permalink_url(
            "https://workspace.slack.com/archives/C0001/p1700000200000300"
            "?thread_ts=1700000100.000200&cid=C0001",
            client,
            _StaticUsers({"U0002": "Bob Brown"}),
        )
    finally:
        client.close()
    assert path == f"channels/general/{month}/{day}/thanks-glad-to-be-here/thread.md"


def test_resolve_channel_only_url() -> None:
    client = _make_client()
    try:
        path = resolve_permalink_url(
            "https://workspace.slack.com/archives/C0001",
            client,
            _StaticUsers({"U0002": "Bob Brown"}),
        )
    finally:
        client.close()
    assert path == "channels/general"


def test_resolve_im_url() -> None:
    client = _make_client()
    try:
        path = resolve_permalink_url(
            "https://workspace.slack.com/archives/D0001",
            client,
            _StaticUsers({"U0002": "Bob Brown"}),
        )
    finally:
        client.close()
    assert path == "dms/bob-brown"


def test_resolve_mpim_url() -> None:
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
        path = resolve_permalink_url(
            "https://workspace.slack.com/archives/G0001",
            client,
            _StaticUsers({"U0002": "Bob Brown"}),
        )
    finally:
        client.close()
    assert path == "group-dms/project-squad"


def test_http_route_resolve_endpoint() -> None:
    client = _make_client()
    deps = _deps(client)
    request = ResolveRequest(url="https://workspace.slack.com/archives/C0001/p1700000000000100")
    try:
        response = route_request(
            HttpRequest(method="POST", target="/resolve", body=request.model_dump_json().encode("utf-8")),
            metrics_source=cast(MetricsSource, _StaticMetricsSource(_sample_metrics())),
            resolve_permalink_deps=deps,
        )
    finally:
        client.close()
    assert response.status_code == 200
    parsed = ResolveResponse.model_validate_json(response.body)
    assert parsed.path.startswith("channels/general/")


def test_handle_resolve_round_trip() -> None:
    client = _make_client()
    deps = _deps(client)
    try:
        response = handle_resolve(
            ResolveRequest(url="https://workspace.slack.com/archives/C0001"),
            deps,
        )
    finally:
        client.close()
    assert response == ResolveResponse(path="channels/general")
