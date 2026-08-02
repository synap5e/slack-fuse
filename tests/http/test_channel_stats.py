"""Authenticated joined workspace channel inventory endpoint."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime

import psycopg
from psycopg.conninfo import make_conninfo
from psycopg.rows import TupleRow
from psycopg.types.json import Jsonb

from slack_fuse_server.http.dto import (
    BackfillMetrics,
    MetricsResponse,
    RateLimitBudget,
    SlackMetrics,
    SubscribersMetrics,
)
from slack_fuse_server.http.handlers import BlockedChannelsDeps
from slack_fuse_server.http.metrics import MetricsSource
from slack_fuse_server.http.server import HttpRequest, route_request


@dataclass(frozen=True, slots=True)
class _Metrics:
    def snapshot(self) -> MetricsResponse:
        return MetricsResponse(
            server_started_at=datetime(2026, 6, 28, tzinfo=UTC),
            slack=SlackMetrics(
                socket_mode_state="connected",
                rate_limit_budget=RateLimitBudget(remaining_pct=100),
                last_health_kind="slack_healthy",
            ),
            backfill=BackfillMetrics(completed_count=0, aborted_count=0),
            subscribers=SubscribersMetrics(active_ws_connections=0),
        )


def _metrics() -> MetricsSource:
    return _Metrics()


def _database_url(conn: psycopg.Connection[TupleRow]) -> str:
    with conn.cursor() as cur:
        cur.execute("SELECT current_schema()")
        row = cur.fetchone()
    assert row is not None and isinstance(row[0], str)
    return make_conninfo(conn.info.dsn, options=f"-c search_path={row[0]}")


def _insert_event(
    conn: psycopg.Connection[TupleRow],
    *,
    stream: str,
    offset: int,
    kind: str,
    payload: Mapping[str, object],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO events (stream, offset_in_stream, kind, payload) VALUES (%s, %s, %s, %s)",
            (stream, offset, kind, Jsonb(dict(payload))),
        )


def _seed_channel(  # noqa: PLR0913 - compact fixture builder mirrors endpoint inputs.
    conn: psycopg.Connection[TupleRow],
    offset: int,
    channel_id: str,
    name: str,
    *,
    is_member: bool,
    total: int | None,
    refresh_status: str = "ok",
    ingested: int = 0,
    blocked: bool = False,
) -> None:
    _insert_event(
        conn,
        stream="channel-list",
        offset=offset,
        kind="channel_added",
        payload={
            "id": channel_id,
            "name": name,
            "is_member": is_member,
            "is_im": False,
            "is_archived": False,
            "created": 1,
        },
    )
    for message_offset in range(1, ingested + 1):
        _insert_event(
            conn,
            stream=f"channel:{channel_id}",
            offset=message_offset,
            kind="message",
            payload={"ts": f"1700000000.{message_offset:06d}"},
        )
    with conn.cursor() as cur:
        if total is not None:
            cur.execute(
                """
                INSERT INTO channel_message_totals (channel_id, total, refreshed_at, refresh_status)
                VALUES (%s, %s, '2026-06-28T04:00:00Z', %s)
                """,
                (channel_id, total, refresh_status),
            )
        if blocked:
            cur.execute("INSERT INTO blocked_channels (channel_id, reason) VALUES (%s, 'noisy')", (channel_id,))


def test_get_channel_stats_joins_all_status_inputs(server_conn: psycopg.Connection[TupleRow]) -> None:
    _seed_channel(server_conn, 1, "C_BLOCK", "blocked", is_member=True, total=20, blocked=True)
    _seed_channel(server_conn, 2, "C_DONE", "done", is_member=True, total=2, ingested=2)
    _seed_channel(server_conn, 3, "C_PROGRESS", "progress", is_member=True, total=5, ingested=1)
    _seed_channel(server_conn, 4, "C_START", "start", is_member=True, total=5)
    _seed_channel(server_conn, 5, "C_OTHER", "other", is_member=False, total=10)
    _seed_channel(server_conn, 6, "C_ERROR", "error", is_member=True, total=7, refresh_status="error")
    _seed_channel(server_conn, 7, "C_NEVER", "never", is_member=True, total=None)
    deps = BlockedChannelsDeps(shared_secret="sek", database_url=_database_url(server_conn))

    response = route_request(
        HttpRequest(
            method="GET",
            target="/channel-stats",
            headers=((b"x-slack-fuse-secret", b"sek"),),
        ),
        metrics_source=_metrics(),
        blocked_channels_deps=deps,
    )

    assert response.status_code == 200
    payload = json.loads(response.body)
    assert payload["oldest_refreshed_at"] == "2026-06-28T04:00:00Z"
    assert payload["newest_refreshed_at"] == "2026-06-28T04:00:00Z"
    # Six channels have a channel_message_totals row (all except C_NEVER which
    # is unavailable and therefore not refreshable).
    assert payload["refreshable_channels"] == 6
    # Five of those are refresh_status='ok'; C_ERROR is refresh_status='error'.
    assert payload["refreshed_ok_channels"] == 5
    assert payload["workspace_message_total"] == 49
    by_id = {row["channel_id"]: row for row in payload["channels"]}
    assert {channel_id: row["status"] for channel_id, row in by_id.items()} == {
        "C_BLOCK": "blocked",
        "C_DONE": "done",
        "C_ERROR": "unavailable",
        "C_NEVER": "unavailable",
        "C_OTHER": "not_joined",
        "C_PROGRESS": "in_progress",
        "C_START": "not_started",
    }
    assert by_id["C_DONE"]["ingested"] == 2
    assert by_id["C_DONE"]["is_archived"] is False
    assert by_id["C_OTHER"]["is_member"] is False
    assert by_id["C_BLOCK"]["is_blocked"] is True
    assert by_id["C_ERROR"]["total"] == 7
    assert by_id["C_NEVER"]["total"] is None
    assert by_id["C_DONE"]["created"] == "1970-01-01"


def test_get_channel_stats_requires_shared_secret(server_conn: psycopg.Connection[TupleRow]) -> None:
    response = route_request(
        HttpRequest(method="GET", target="/channel-stats"),
        metrics_source=_metrics(),
        blocked_channels_deps=BlockedChannelsDeps(shared_secret="sek", database_url=_database_url(server_conn)),
    )
    assert response.status_code == 401
