"""HTTP handlers for the server HTTP surface."""

from __future__ import annotations

from collections.abc import Sequence
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


@dataclass(frozen=True, slots=True)
class OriginalsDeps:
    """Dependencies required by ``GET /originals/{channel_id}``.

    Holds the database URL only — the events replay opens its own connection
    per request so it doesn't contend with the long-lived snapshot / dispatch
    connections (the originals view is a low-rate forensic read).
    """

    database_url: str


@dataclass(frozen=True, slots=True)
class GapsDeps:
    """Dependencies required by ``GET /gaps`` and ``GET /gaps/{channel_id}``.

    Same shape as :class:`OriginalsDeps`: a forensic read-only surface that
    opens its own conn per request. The workspace-wide query is one scan
    over the events table, so the per-request overhead beats holding a
    long-lived conn for an endpoint that's hit rarely.
    """

    database_url: str


class RefreshTrigger(Protocol):
    """``POST /refresh-channels`` hands the request off to a long-lived
    background consumer via this trigger. The HTTP request returns 202
    immediately; the actual ``conversations.info`` sweep runs in the
    main nursery."""

    def request(self) -> bool:
        """Queue a refresh. Returns True if accepted (consumer was idle),
        False if one is already in progress (caller should treat as 409)."""
        ...


@dataclass(frozen=True, slots=True)
class RefreshDeps:
    """Dependencies required by ``POST /refresh-channels``.

    Mutating endpoint, so it carries a shared-secret check independent
    from the read-only endpoints. ``trigger.request()`` hands off to a
    long-lived task in the main nursery; the request returns 202 without
    waiting for the sweep to finish.
    """

    shared_secret: str | None
    trigger: RefreshTrigger


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


def handle_channel_gaps(channel_id: str, *, deps: GapsDeps) -> bytes:
    """``GET /gaps/{channel_id}`` — per-channel rendered gaps view."""
    import psycopg  # noqa: PLC0415

    from slack_fuse_server.gaps import render_channel_gaps  # noqa: PLC0415

    with psycopg.connect(deps.database_url, autocommit=True) as conn:
        return render_channel_gaps(conn, channel_id)


def handle_workspace_gaps(*, deps: GapsDeps) -> bytes:
    """``GET /gaps`` — workspace-wide rendered gaps summary."""
    import psycopg  # noqa: PLC0415

    from slack_fuse_server.gaps import render_workspace_gaps  # noqa: PLC0415

    with psycopg.connect(deps.database_url, autocommit=True) as conn:
        return render_workspace_gaps(conn)


def handle_refresh_channels(
    headers: Sequence[tuple[bytes, bytes]],
    *,
    deps: RefreshDeps,
) -> tuple[int, str]:
    """``POST /refresh-channels`` — queue a one-shot refresh cycle.

    Returns ``(status_code, message)``:
      * 202 — accepted, refresh queued
      * 401 — missing or wrong shared secret
      * 409 — refresh already in progress; try again later

    The actual sweep runs in a background task (the long-lived consumer
    spawned by the main nursery) and logs its summary on completion.
    """
    if not is_http_authorized(headers, deps.shared_secret):
        return 401, "unauthorized"
    if deps.trigger.request():
        return 202, "refresh queued"
    return 409, "refresh already in progress"


# === auth helper (shared with WS) ===


_SHARED_SECRET_HEADER = b"x-slack-fuse-secret"
_AUTHORIZATION_HEADER = b"authorization"


def is_http_authorized(
    headers: Sequence[tuple[bytes, bytes]],
    shared_secret: str | None,
) -> bool:
    """True if the request carries the shared secret (or no secret is
    configured). Accepts both ``X-Slack-Fuse-Secret: <secret>`` and
    ``Authorization: Bearer <secret>`` — same shape the WS endpoint uses."""
    if not shared_secret:
        return True
    expected_direct = shared_secret.encode()
    expected_bearer = f"Bearer {shared_secret}".encode()
    for name, value in headers:
        lowered = name.lower()
        if lowered == _SHARED_SECRET_HEADER and value == expected_direct:
            return True
        if lowered == _AUTHORIZATION_HEADER and value == expected_bearer:
            return True
    return False


def handle_originals(
    channel_id: str,
    *,
    from_epoch: float,
    to_epoch: float,
    deps: OriginalsDeps,
) -> bytes:
    """``GET /originals/{channel_id}?from=<epoch>&to=<epoch>``.

    Replays the events table for ``channel:{channel_id}`` over the UTC epoch
    range and returns markdown with unresolved ``<@U…>`` / ``<#C…>``
    placeholders. The FUSE client's existing resolver pipeline substitutes
    display names against its local users/channels tables (same shape as a
    chunks-backed read).

    Empty body when no messages exist in the range.
    """
    # Lazy import so the http package doesn't pull in psycopg unless this
    # endpoint is wired.
    import psycopg  # noqa: PLC0415

    from slack_fuse_server.originals import render_originals_for_range  # noqa: PLC0415

    with psycopg.connect(deps.database_url, autocommit=True) as conn:
        return render_originals_for_range(
            conn,
            channel_id,
            from_epoch=from_epoch,
            to_epoch=to_epoch,
        )
