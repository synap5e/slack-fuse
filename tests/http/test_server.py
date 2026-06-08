"""Tests for the Sprint 1C HTTP server (`/health`, `/metrics`)."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

import httpx
import pytest
import trio

from slack_fuse_server.http.dto import (
    BackfillMetrics,
    MetricsResponse,
    RateLimitBudget,
    SlackMetrics,
    StreamMetrics,
    SubscribersMetrics,
)
from slack_fuse_server.http.metrics import MetricsSource
from slack_fuse_server.http.server import (
    HttpRequest,
    parse_listen_addr,
    route_request,
    serve_http_on_listeners,
)


@dataclass(frozen=True, slots=True)
class StaticMetricsSource:
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
            rate_limit_budget=RateLimitBudget(remaining_pct=93),
            last_health_kind="slack_healthy",
        ),
        streams=[StreamMetrics(stream="users", head_offset=12, events_per_min=1)],
        backfill=BackfillMetrics(completed_count=3, aborted_count=1),
        subscribers=SubscribersMetrics(active_ws_connections=0),
    )


@asynccontextmanager
async def _running_server(metrics_source: MetricsSource):
    listeners = await trio.open_tcp_listeners(0, host="127.0.0.1")
    sockname = cast(tuple[str, int], listeners[0].socket.getsockname())
    port = sockname[1]
    async with trio.open_nursery() as nursery:
        nursery.start_soon(serve_http_on_listeners, listeners, metrics_source)
        await trio.sleep(0.05)
        try:
            yield f"http://127.0.0.1:{port}"
        finally:
            nursery.cancel_scope.cancel()


def test_parse_listen_addr() -> None:
    assert parse_listen_addr("127.0.0.1:8765") == ("127.0.0.1", 8765)
    assert parse_listen_addr("[::1]:8765") == ("::1", 8765)


def test_route_request_health() -> None:
    response = route_request(
        HttpRequest(method="GET", target="/health"),
        metrics_source=StaticMetricsSource(_sample_metrics()),
    )
    assert response.status_code == 200
    assert response.body == b'{"ok":true}'


def test_route_request_metrics() -> None:
    payload = _sample_metrics()
    response = route_request(HttpRequest(method="GET", target="/metrics"), metrics_source=StaticMetricsSource(payload))
    assert response.status_code == 200
    assert MetricsResponse.model_validate_json(response.body) == payload


def test_route_request_not_found() -> None:
    response = route_request(
        HttpRequest(method="GET", target="/nope"),
        metrics_source=StaticMetricsSource(_sample_metrics()),
    )
    assert response.status_code == 404
    assert response.body == b'{"error":"not_found"}'


def test_route_request_method_not_allowed() -> None:
    response = route_request(
        HttpRequest(method="POST", target="/health"),
        metrics_source=StaticMetricsSource(_sample_metrics()),
    )
    assert response.status_code == 405
    assert response.body == b'{"error":"method_not_allowed"}'


@pytest.mark.trio
async def test_http_server_health_endpoint() -> None:
    async with _running_server(StaticMetricsSource(_sample_metrics())) as base_url, httpx.AsyncClient(
        base_url=base_url
    ) as client:
        response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"ok": True}


@pytest.mark.trio
async def test_http_server_metrics_endpoint_round_trip() -> None:
    expected = _sample_metrics()
    async with _running_server(StaticMetricsSource(expected)) as base_url, httpx.AsyncClient(
        base_url=base_url
    ) as client:
        response = await client.get("/metrics")
    assert response.status_code == 200
    parsed = MetricsResponse.model_validate(response.json())
    assert parsed == expected
