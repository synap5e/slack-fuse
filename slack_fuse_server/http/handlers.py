"""HTTP handlers for the server HTTP surface."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

import psycopg

from slack_fuse_server.blocked_channels import (
    block_channel,
    get_blocked_channel,
    is_channel_blocked,
    list_blocked_channels,
    unblock_channel,
)
from slack_fuse_server.http.dto import (
    GapDetectionRow,
    HealthResponse,
    MetricsResponse,
    PermalinkRequest,
    PermalinkResponse,
    ProbeStatusResponse,
    ResolveRequest,
    ResolveResponse,
)
from slack_fuse_server.http.metrics import MetricsSource
from slack_fuse_server.http.permalink import resolve_path_to_permalink_url
from slack_fuse_server.http.resolve import resolve_permalink_url
from slack_fuse_server.http.snapshot import SnapshotPayload, fetch_snapshot_payload
from slack_fuse_server.slurper.api import SlackClient
from slack_fuse_server.slurper.probes import PROBE_REGISTRY
from slack_fuse_server.slurper.supervisor import TaskPhase, TaskSupervisor

log = logging.getLogger(__name__)


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
    """``POST /refresh-channels[/<channel_id>]`` hands the request off
    to a long-lived background consumer via this trigger. The HTTP
    request returns 202 immediately; the actual ``conversations.info``
    call(s) run in the main nursery."""

    def request(self) -> bool:
        """Queue a workspace-wide refresh. Returns True if accepted
        (consumer was idle), False if one is already in progress
        (caller should treat as 409)."""
        ...

    def request_channel(self, channel_id: str) -> bool:
        """Queue a single-channel refresh. Same return contract as
        ``request()``."""
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
    database_url: str | None = None


class BackfillTrigger(Protocol):
    """``POST /backfill-channel/{channel_id}`` hands a manual backfill to
    the long-lived slurper process and returns immediately."""

    def request_channel(self, channel_id: str) -> bool:
        """Queue a manual backfill. False means one is already in progress."""
        ...


class ProbeTrigger(Protocol):
    """``POST /probe-sweep[/<job_id>[/<target>]]`` manual probe trigger."""

    def request(self, *, job_id: str | None = None, target: str | None = None) -> bool:
        """Queue a manual probe sweep. False means the bounded queue is full."""
        ...


class RefillWindowTrigger(Protocol):
    """``POST /refill-window/{channel_id}`` queues a bounded refill run."""

    def request_window(self, channel_id: str, oldest: float, latest: float) -> str | None:
        """Queue a refill-window run.

        Returns the accepted ``run_id``. ``None`` means the consumer is already
        busy and the endpoint should respond 409.
        """
        ...


@dataclass(frozen=True, slots=True)
class ProbeDeps:
    """Dependencies for ``POST /probe-sweep``."""

    shared_secret: str | None
    trigger: ProbeTrigger


@dataclass(frozen=True, slots=True)
class ProbeStatusDeps:
    """Dependencies for ``GET /probe-status``."""

    database_url: str
    alert_threshold_seconds: int


@dataclass(frozen=True, slots=True)
class BlockedChannelsDeps:
    """Dependencies for the mutable channel-block policy endpoints."""

    shared_secret: str | None
    database_url: str


@dataclass(frozen=True, slots=True)
class BackfillDeps:
    """Dependencies for ``POST /backfill-channel/{channel_id}``."""

    shared_secret: str | None
    database_url: str
    trigger: BackfillTrigger


@dataclass(frozen=True, slots=True)
class RefillWindowDeps:
    """Dependencies for ``POST /refill-window/{channel_id}``."""

    shared_secret: str | None
    database_url: str
    trigger: RefillWindowTrigger


@dataclass(frozen=True, slots=True)
class LivezDeps:
    """Dependencies required by ``GET /livez``."""

    supervisor: TaskSupervisor


def handle_health() -> HealthResponse:
    """`GET /health` liveness probe."""
    return HealthResponse(ok=True)


def handle_livez(deps: LivezDeps) -> tuple[int, dict[str, object]]:
    """``GET /livez`` task-liveness snapshot."""
    phases = deps.supervisor.all_phases()
    overdue = deps.supervisor.overdue()
    serialized_phases = {task_name: _serialize_phase_for_map(phase) for task_name, phase in phases.items()}
    if not overdue:
        return 200, {"phases": serialized_phases}

    log.warning(
        "livez: overdue task phase(s): %s",
        ", ".join(
            f"{phase.task_name}:{phase.phase} deadline={phase.deadline.isoformat() if phase.deadline else None}"
            for phase in overdue
        ),
    )
    return 503, {
        "overdue": [_serialize_phase_for_overdue(phase) for phase in overdue],
        "phases": serialized_phases,
    }


def _serialize_phase_for_map(phase: TaskPhase) -> dict[str, object]:
    return {
        "phase": phase.phase,
        "details": phase.details,
        "entered_at": phase.entered_at.isoformat(),
        "deadline": None if phase.deadline is None else phase.deadline.isoformat(),
    }


def _serialize_phase_for_overdue(phase: TaskPhase) -> dict[str, object]:
    return {
        "task_name": phase.task_name,
        **_serialize_phase_for_map(phase),
    }


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


def handle_gap_detection(*, deps: GapsDeps) -> list[GapDetectionRow]:
    """``GET /gap-candidates`` — JSON day-presence refill candidates."""
    from slack_fuse_server.gap_detection import detect_day_presence_gaps  # noqa: PLC0415

    with psycopg.connect(deps.database_url, autocommit=True) as conn:
        return detect_day_presence_gaps(conn)


def handle_probe_status(*, deps: ProbeStatusDeps) -> ProbeStatusResponse:
    """``GET /probe-status`` — latest probe-sweep liveness summary."""
    from slack_fuse_server.gap_detection import fetch_probe_status  # noqa: PLC0415

    with psycopg.connect(deps.database_url, autocommit=True) as conn:
        return fetch_probe_status(conn, alert_threshold_seconds=deps.alert_threshold_seconds)


def handle_refresh_channels(
    headers: Sequence[tuple[bytes, bytes]],
    *,
    deps: RefreshDeps,
) -> tuple[int, str]:
    """``POST /refresh-channels`` — queue a workspace-wide refresh.

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


def handle_refresh_channel(
    channel_id: str,
    headers: Sequence[tuple[bytes, bytes]],
    *,
    deps: RefreshDeps,
) -> tuple[int, str]:
    """``POST /refresh-channels/{channel_id}`` — refresh a single channel.

    Same response shape as :func:`handle_refresh_channels`. Cheap (one
    ``conversations.info`` call), but still serialised through the
    consumer so it can't overlap a workspace sweep.
    """
    if not is_http_authorized(headers, deps.shared_secret):
        return 401, "unauthorized"
    if deps.database_url is not None:
        with psycopg.connect(deps.database_url, autocommit=True) as conn:
            if is_channel_blocked(conn, channel_id):
                return 409, "channel blocked"
    if deps.trigger.request_channel(channel_id):
        return 202, f"refresh queued for {channel_id}"
    return 409, "refresh already in progress"


def handle_list_blocked_channels(
    headers: Sequence[tuple[bytes, bytes]],
    *,
    deps: BlockedChannelsDeps,
) -> tuple[int, dict[str, object]]:
    if not is_http_authorized(headers, deps.shared_secret):
        return 401, {"error": "unauthorized"}
    with psycopg.connect(deps.database_url, autocommit=True) as conn:
        return 200, {"blocked": list_blocked_channels(conn)}


def handle_block_channel(
    channel_id: str,
    reason: str | None,
    headers: Sequence[tuple[bytes, bytes]],
    *,
    deps: BlockedChannelsDeps,
) -> tuple[int, dict[str, object]]:
    if not is_http_authorized(headers, deps.shared_secret):
        return 401, {"error": "unauthorized"}
    with psycopg.connect(deps.database_url, autocommit=True) as conn:
        return 200, dict(block_channel(conn, channel_id, reason=reason))


def handle_unblock_channel(
    channel_id: str,
    headers: Sequence[tuple[bytes, bytes]],
    *,
    deps: BlockedChannelsDeps,
) -> tuple[int, dict[str, object]]:
    if not is_http_authorized(headers, deps.shared_secret):
        return 401, {"error": "unauthorized"}
    with psycopg.connect(deps.database_url, autocommit=True) as conn:
        unblock_channel(conn, channel_id)
    return 200, {"status": "unblocked", "channel_id": channel_id}


def handle_backfill_channel(
    channel_id: str,
    headers: Sequence[tuple[bytes, bytes]],
    *,
    deps: BackfillDeps,
) -> tuple[int, str]:
    if not is_http_authorized(headers, deps.shared_secret):
        return 401, "unauthorized"
    with psycopg.connect(deps.database_url, autocommit=True) as conn:
        if get_blocked_channel(conn, channel_id) is not None:
            return 409, "blocked"
    if deps.trigger.request_channel(channel_id):
        return 202, f"backfill queued for {channel_id}"
    return 409, "backfill already in progress"


def handle_refill_window(
    channel_id: str,
    oldest: float,
    latest: float,
    headers: Sequence[tuple[bytes, bytes]],
    *,
    deps: RefillWindowDeps,
) -> tuple[int, str, str | None]:
    """``POST /refill-window/{channel_id}`` — queue one bounded refill."""
    if not is_http_authorized(headers, deps.shared_secret):
        return 401, "unauthorized", None
    if latest <= oldest or oldest < 0.0 or latest < 0.0:
        return 400, "bad_request", None
    from slack_fuse_server.gap_detection import refill_window_in_flight  # noqa: PLC0415

    with psycopg.connect(deps.database_url, autocommit=True) as conn:
        if refill_window_in_flight(conn, channel_id=channel_id, oldest=oldest, latest=latest):
            return 409, "refill already in progress", None
    run_id = deps.trigger.request_window(channel_id, oldest, latest)
    if run_id is None:
        return 409, "refill already in progress", None
    return 202, "refill queued", run_id


def handle_probe_sweep(
    headers: Sequence[tuple[bytes, bytes]],
    *,
    deps: ProbeDeps,
    job_id: str | None = None,
    target: str | None = None,
) -> tuple[int, str]:
    """``POST /probe-sweep[/<job_id>[/<target>]]`` — queue a manual probe sweep."""
    if not is_http_authorized(headers, deps.shared_secret):
        return 401, "unauthorized"

    error = _validate_probe_request(job_id=job_id, target=target)
    if error is not None:
        return 400, error

    if deps.trigger.request(job_id=job_id, target=target):
        if job_id is None:
            return 202, "probe sweep queued"
        if target is None:
            return 202, f"probe sweep queued for {job_id}"
        return 202, f"probe sweep queued for {job_id} {target}"
    return 409, "probe sweep already busy"


def _validate_probe_request(*, job_id: str | None, target: str | None) -> str | None:
    if job_id is None:
        if target is None:
            return None
        return "bad_target"
    descriptor = next((probe for probe in PROBE_REGISTRY if probe.job_id == job_id), None)
    if descriptor is None:
        return "unknown_job"
    if target is not None and (not target.strip() or any(char.isspace() for char in target)):
        return "bad_target"
    if target is not None and not descriptor.is_per_target:
        return "bad_target"
    return None


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
