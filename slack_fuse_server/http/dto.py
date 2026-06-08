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

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

# `/snapshot` response framing (see RFC §Snapshot delivery via HTTP).
SNAPSHOT_CONTENT_TYPE = "application/jsonl"
SNAPSHOT_CONTENT_ENCODING = "gzip"


class _DTO(BaseModel):
    """Base for HTTP DTOs: immutable, reject unknown fields."""

    model_config = ConfigDict(frozen=True, extra="forbid")


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
