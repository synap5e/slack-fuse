"""Pydantic request/response models for the server HTTP endpoints.

Per RFC §Server-side HTTP surface:

| Path         | Method        | Request          | Response          |
|--------------|---------------|------------------|-------------------|
| `/resolve`   | POST          | `ResolveRequest` | `ResolveResponse` |
| `/permalink` | POST          | `PermalinkRequest` | `PermalinkResponse` |
| `/metrics`   | GET           | —                | `MetricsResponse` |
| `/health`    | GET           | —                | `HealthResponse`  |
| `/streams/<id>/snapshot` | GET | `SnapshotQuery` (query string) | JSONL + gzip stream |

`/snapshot` has no JSON body — it takes the stream id as a path param and
`at=<offset>` as a query param, and streams a JSONL (gzip-encoded) body. The
content-type/-encoding constants below pin that contract.
"""

from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, ConfigDict, Field

from slack_fuse_server._json import JsonObject

# `/snapshot` response framing (see RFC §Snapshot delivery via HTTP).
SNAPSHOT_CONTENT_TYPE = "application/jsonl"
SNAPSHOT_CONTENT_ENCODING = "gzip"


class _DTO(BaseModel):
    """Base for HTTP DTOs: immutable, reject unknown fields."""

    model_config = ConfigDict(frozen=True, extra="forbid")


# === /snapshot JSONL line payload ===
#
# Per RFC §Snapshot delivery via HTTP, the GET /streams/<id>/snapshot endpoint
# streams one JSONL record per current-state item. "Format matches what the
# projector would have received as a sequence of `message` events — same
# shape." Lines have ts + payload only; stream is implicit in the URL and
# offsets aren't meaningful for snapshot lines (a snapshot is a current-state
# dump, not historical events). The projector's apply path treats each line as
# if it were the payload of a `kind="message"` EventFrame.


class SnapshotLine(_DTO):
    """One JSONL record from the /snapshot endpoint.

    Wire shape: `{"ts": "<slack-ts>", "payload": {<message-fields>}}`.
    `payload` matches the EventFrame `payload` for `kind="message"` events;
    consumers apply each line via the same projection code path used for live
    `message` event frames.
    """

    ts: str = Field(
        ...,
        description="Slack ts of the message (UTC epoch with microsecond fraction).",
    )
    payload: JsonObject = Field(
        ...,
        description="Message-shaped payload, same as EventFrame.payload for kind='message'.",
    )


# === /resolve ===


class ResolveRequest(_DTO):
    """POST /resolve — resolve a Slack permalink to a FUSE path."""

    url: str


class ResolveResponse(_DTO):
    path: str


# === /permalink ===


class PermalinkRequest(_DTO):
    """POST /permalink — resolve a FUSE path to a Slack permalink.

    `ts` selects a specific message within the path's day/thread when set.
    """

    path: str
    ts: str | None = None


class PermalinkResponse(_DTO):
    url: str


# === /health ===


class HealthResponse(_DTO):
    ok: bool


# === /gap-candidates + /probe-status + /refill-window ===


class GapDetectionRow(_DTO):
    channel_id: str
    day: date
    oldest_ts: float
    latest_ts: float
    slack_sample_ts: str
    sampled_at: datetime
    gap_type: str


class ProbeStatusResponse(_DTO):
    last_sweep_completed_at: datetime | None
    age_seconds: int | None
    channels_covered_last_sweep: int
    days_covered_last_sweep: int
    alert_threshold_seconds: int


class RefillWindowRequest(_DTO):
    oldest: float
    latest: float


class RefillWindowResponse(_DTO):
    status: str
    run_id: str | None = None


# === /snapshot (query string only) ===


class SnapshotQuery(_DTO):
    """Query params for GET /streams/<id>/snapshot."""

    at: int


# === /metrics ===


class RateLimitBudget(_DTO):
    remaining_pct: int


class SlackMetrics(_DTO):
    socket_mode_state: str
    last_event_at: datetime | None = None
    rate_limit_budget: RateLimitBudget
    last_health_kind: str


class StreamMetrics(_DTO):
    stream: str
    head_offset: int
    events_per_min: int


class BackfillInProgress(_DTO):
    channel_id: str
    messages_so_far: int


class BackfillMetrics(_DTO):
    in_progress: list[BackfillInProgress] = Field(default_factory=list)
    completed_count: int
    aborted_count: int


class ClientSubscription(_DTO):
    client_id: str
    connected_since: datetime
    subscriptions: int


class SubscribersMetrics(_DTO):
    active_ws_connections: int
    by_client: list[ClientSubscription] = Field(default_factory=list)


class MetricsResponse(_DTO):
    """GET /metrics — single JSON document of slurper state (RFC §/metrics)."""

    server_started_at: datetime
    slack: SlackMetrics
    streams: list[StreamMetrics] = Field(default_factory=list)
    backfill: BackfillMetrics
    subscribers: SubscribersMetrics
