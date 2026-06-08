"""Every HTTP DTO round-trips through JSON byte-equivalent serialisation.

The byte-level (`model_dump_json` / `model_validate_json`) round-trip is
the contract that matters on the wire — dict round-trip via `model_dump`
would hide serialisation-level divergences (datetime ISO formatting,
None vs missing keys, JSONL line shape) that bite when DTOs cross the
HTTP boundary. Dict round-trip kept as a structural sanity check
alongside the byte-level one.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import BaseModel, ValidationError

from slack_fuse_server.http.dto import (
    SNAPSHOT_CONTENT_ENCODING,
    SNAPSHOT_CONTENT_TYPE,
    BackfillInProgress,
    BackfillMetrics,
    ClientSubscription,
    HealthResponse,
    MetricsResponse,
    PermalinkRequest,
    PermalinkResponse,
    RateLimitBudget,
    ResolveRequest,
    ResolveResponse,
    SlackMetrics,
    SnapshotLine,
    SnapshotQuery,
    StreamMetrics,
    SubscribersMetrics,
)

_T = datetime(2026, 6, 1, 8, 0, 0, tzinfo=UTC)

_DTOS: list[BaseModel] = [
    ResolveRequest(url="https://example.slack.com/archives/C1/p1700000000000100"),
    ResolveResponse(path="channels/general/2026-06/08/channel.md"),
    PermalinkRequest(path="channels/general/2026-06/08/channel.md"),
    PermalinkRequest(path="channels/general/2026-06/08/channel.md", ts="1700000000.000100"),
    PermalinkResponse(url="https://example.slack.com/archives/C1/p1700000000000100"),
    HealthResponse(ok=True),
    SnapshotQuery(at=184500),
    SnapshotLine(
        ts="1779000000.000100",
        payload={
            "type": "message",
            "user": "U1",
            "text": "hello",
            "edited": None,
            "reactions": [{"name": "wave", "count": 2}],
        },
    ),
    MetricsResponse(
        server_started_at=_T,
        slack=SlackMetrics(
            socket_mode_state="connected",
            last_event_at=_T,
            rate_limit_budget=RateLimitBudget(remaining_pct=87),
            last_health_kind="slack_healthy",
        ),
        streams=[
            StreamMetrics(stream="users", head_offset=1240, events_per_min=0),
            StreamMetrics(stream="channel:C0", head_offset=184600, events_per_min=12),
        ],
        backfill=BackfillMetrics(
            in_progress=[BackfillInProgress(channel_id="C09", messages_so_far=4200)],
            completed_count=287,
            aborted_count=3,
        ),
        subscribers=SubscribersMetrics(
            active_ws_connections=2,
            by_client=[ClientSubscription(client_id="laptop", connected_since=_T, subscriptions=320)],
        ),
    ),
]


@pytest.mark.parametrize("dto", _DTOS, ids=lambda d: type(d).__name__)
def test_dto_json_byte_roundtrip(dto: BaseModel) -> None:
    """Byte-equivalent round-trip: serialise → bytes → parse → equal model
    AND a second serialise produces byte-identical output."""
    raw = dto.model_dump_json()
    restored = type(dto).model_validate_json(raw)
    assert restored == dto
    assert restored.model_dump_json() == raw


@pytest.mark.parametrize("dto", _DTOS, ids=lambda d: type(d).__name__)
def test_dto_dict_roundtrip(dto: BaseModel) -> None:
    """Structural round-trip retained alongside the byte-level one."""
    dumped = dto.model_dump()
    restored = type(dto).model_validate(dumped)
    assert restored == dto
    assert restored.model_dump() == dumped


def test_metrics_json_shape_matches_rfc() -> None:
    dto = next(d for d in _DTOS if isinstance(d, MetricsResponse))
    payload = dto.model_dump(mode="json")
    assert set(payload) == {"server_started_at", "slack", "streams", "backfill", "subscribers"}
    assert set(payload["slack"]) == {
        "socket_mode_state",
        "last_event_at",
        "rate_limit_budget",
        "last_health_kind",
    }
    assert payload["slack"]["rate_limit_budget"] == {"remaining_pct": 87}


def test_permalink_request_ts_optional() -> None:
    assert PermalinkRequest(path="x").ts is None


def test_dto_rejects_extra_field() -> None:
    with pytest.raises(ValidationError):
        ResolveRequest.model_validate({"url": "x", "surprise": 1})


def test_snapshot_content_constants() -> None:
    assert SNAPSHOT_CONTENT_TYPE == "application/jsonl"
    assert SNAPSHOT_CONTENT_ENCODING == "gzip"
