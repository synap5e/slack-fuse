"""HTTP handlers for the server HTTP surface."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from slack_fuse_server.http.dto import (
    HealthResponse,
    MetricsResponse,
    PermalinkRequest,
    PermalinkResponse,
    ResolveRequest,
    ResolveResponse,
)
from slack_fuse_server.http.metrics import MetricsSource
from slack_fuse_server.http.permalink import resolve_path_to_permalink_url
from slack_fuse_server.http.resolve import resolve_permalink_url
from slack_fuse_server.http.snapshot import SnapshotPayload, fetch_snapshot_payload
from slack_fuse_server.slurper.api import SlackClient


class DisplayNameResolver(Protocol):
    """Resolver protocol shared by resolve/permalink handlers."""

    def get_display_name(self, user_id: str) -> str:
        """Return display name for a Slack user id."""
        ...


@dataclass(frozen=True, slots=True)
class ResolvePermalinkDeps:
    """Dependencies required by `/resolve` and `/permalink` handlers."""

    client: SlackClient
    users: DisplayNameResolver
    workspace_url: str | None = None


@dataclass(frozen=True, slots=True)
class SnapshotDeps:
    """Dependencies required by `GET /streams/<id>/snapshot`."""

    database_url: str


def handle_health() -> HealthResponse:
    """`GET /health` liveness probe."""
    return HealthResponse(ok=True)


def handle_metrics(metrics_source: MetricsSource) -> MetricsResponse:
    """`GET /metrics` server-state snapshot."""
    return metrics_source.snapshot()


def handle_resolve(request: ResolveRequest, deps: ResolvePermalinkDeps) -> ResolveResponse:
    """`POST /resolve` permalink URL -> relative FUSE path."""
    return ResolveResponse(path=resolve_permalink_url(request.url, deps.client, deps.users))


def handle_permalink(request: PermalinkRequest, deps: ResolvePermalinkDeps) -> PermalinkResponse:
    """`POST /permalink` relative FUSE path -> Slack permalink URL."""
    return PermalinkResponse(
        url=resolve_path_to_permalink_url(
            request.path,
            deps.client,
            deps.users,
            deps.workspace_url,
            ts=request.ts,
        )
    )


def handle_snapshot(
    stream: str,
    *,
    at: int,
    since: int | None,
    deps: SnapshotDeps,
) -> SnapshotPayload:
    """`GET /streams/<id>/snapshot?at=<offset>[&since=<offset>]`."""
    return fetch_snapshot_payload(
        deps.database_url,
        stream=stream,
        requested_at=at,
        client_since_offset=since,
    )
