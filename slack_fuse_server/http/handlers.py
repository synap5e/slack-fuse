"""HTTP handlers for the Sprint 1C server surface."""

from __future__ import annotations

from slack_fuse_server.http.dto import HealthResponse, MetricsResponse
from slack_fuse_server.http.metrics import MetricsSource


def handle_health() -> HealthResponse:
    """`GET /health` liveness probe."""
    return HealthResponse(ok=True)


def handle_metrics(metrics_source: MetricsSource) -> MetricsResponse:
    """`GET /metrics` server-state snapshot."""
    return metrics_source.snapshot()
