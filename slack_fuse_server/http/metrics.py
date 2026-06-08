"""Metrics aggregation for `GET /metrics`.

Builds `MetricsResponse` from the server-side tables defined in
`slack_fuse_server/schema.sql`:

- `events`: per-stream heads, per-stream events/minute, latest event timestamp
- `health_log`: latest slurper health kind + backfill completed/aborted counts
- `backfill_overrides`: consulted while deriving in-progress backfills
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

import psycopg
from psycopg.rows import TupleRow

from slack_fuse_server.http.dto import (
    BackfillInProgress,
    BackfillMetrics,
    ClientSubscription,
    MetricsResponse,
    RateLimitBudget,
    SlackMetrics,
    StreamMetrics,
    SubscribersMetrics,
)

type PgConnection = psycopg.Connection[TupleRow]
type SocketModeStateProvider = Callable[[], str]
type RateLimitBudgetProvider = Callable[[], int]
type SubscribersProvider = Callable[[], Sequence["SubscriberSnapshot"]]


@dataclass(frozen=True, slots=True)
class SubscriberSnapshot:
    """Live WebSocket subscriber state exposed in `/metrics`."""

    client_id: str
    connected_since: datetime
    subscriptions: int


class MetricsSource(Protocol):
    """Minimal protocol used by the HTTP handler layer."""

    def snapshot(self) -> MetricsResponse: ...


class MetricsAggregator:
    """Collect metrics on demand from Postgres + runtime providers."""

    def __init__(
        self,
        *,
        database_url: str,
        server_started_at: datetime,
        socket_mode_state: SocketModeStateProvider | None = None,
        rate_limit_remaining_pct: RateLimitBudgetProvider | None = None,
        subscribers: SubscribersProvider | None = None,
    ) -> None:
        self._database_url = database_url
        self._server_started_at = _ensure_aware(server_started_at)
        self._socket_mode_state = socket_mode_state or _default_socket_mode_state
        self._rate_limit_remaining_pct = rate_limit_remaining_pct or _default_rate_limit_remaining_pct
        self._subscribers = subscribers or _default_subscribers

    def snapshot(self) -> MetricsResponse:
        with psycopg.connect(self._database_url) as conn:
            return collect_metrics(
                conn=conn,
                server_started_at=self._server_started_at,
                socket_mode_state=self._socket_mode_state(),
                rate_limit_remaining_pct=self._rate_limit_remaining_pct(),
                subscribers=self._subscribers(),
            )


def collect_metrics(
    *,
    conn: PgConnection,
    server_started_at: datetime,
    socket_mode_state: str,
    rate_limit_remaining_pct: int,
    subscribers: Sequence[SubscriberSnapshot],
) -> MetricsResponse:
    """Build a DTO-ready metrics snapshot from DB state + runtime state."""
    streams = _load_stream_metrics(conn)
    last_event_at = _load_last_event_at(conn)
    last_health_kind = _load_last_health_kind(conn)
    backfill = _load_backfill_metrics(conn)
    subscribers_metrics = _to_subscribers_metrics(subscribers)
    return MetricsResponse(
        server_started_at=_ensure_aware(server_started_at),
        slack=SlackMetrics(
            socket_mode_state=socket_mode_state,
            last_event_at=last_event_at,
            rate_limit_budget=RateLimitBudget(remaining_pct=rate_limit_remaining_pct),
            last_health_kind=last_health_kind,
        ),
        streams=streams,
        backfill=backfill,
        subscribers=subscribers_metrics,
    )


def _load_stream_metrics(conn: PgConnection) -> list[StreamMetrics]:
    query = """
        SELECT
            stream,
            MAX(offset_in_stream) AS head_offset,
            COUNT(*) FILTER (WHERE created_at >= (now() - interval '1 minute')) AS events_per_min
        FROM events
        GROUP BY stream
        ORDER BY stream
    """
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
    return [
        StreamMetrics(
            stream=str(row[0]),
            head_offset=int(row[1]),
            events_per_min=int(row[2]),
        )
        for row in rows
    ]


def _load_last_event_at(conn: PgConnection) -> datetime | None:
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(created_at) FROM events")
        row = cur.fetchone()
    if row is None or row[0] is None:
        return None
    return _ensure_aware(row[0])


def _load_last_health_kind(conn: PgConnection) -> str:
    query = "SELECT kind FROM health_log ORDER BY created_at DESC, id DESC LIMIT 1"
    with conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()
    if row is None:
        return "unknown"
    return str(row[0])


def _load_backfill_metrics(conn: PgConnection) -> BackfillMetrics:
    counts_query = """
        SELECT
            COUNT(*) FILTER (WHERE kind = 'backfill_completed') AS completed_count,
            COUNT(*) FILTER (WHERE kind = 'backfill_aborted') AS aborted_count
        FROM health_log
    """
    with conn.cursor() as cur:
        cur.execute(counts_query)
        counts_row = cur.fetchone()

    completed_count = int(counts_row[0]) if counts_row is not None else 0
    aborted_count = int(counts_row[1]) if counts_row is not None else 0
    in_progress = _load_in_progress_backfills(conn)
    return BackfillMetrics(
        in_progress=in_progress,
        completed_count=completed_count,
        aborted_count=aborted_count,
    )


def _load_in_progress_backfills(conn: PgConnection) -> list[BackfillInProgress]:
    # `backfill_overrides` is joined so operator-marked channels sort first.
    query = """
        WITH latest AS (
            SELECT DISTINCT ON (payload->>'channel_id')
                payload->>'channel_id' AS channel_id,
                kind,
                payload
            FROM health_log
            WHERE kind IN (
                'backfill_started',
                'backfill_progress',
                'backfill_completed',
                'backfill_aborted'
            )
                AND payload ? 'channel_id'
            ORDER BY (payload->>'channel_id'), created_at DESC, id DESC
        )
        SELECT
            latest.channel_id,
            CASE
                WHEN (latest.payload->>'messages_so_far') ~ '^[0-9]+$'
                    THEN (latest.payload->>'messages_so_far')::BIGINT
                ELSE 0
            END AS messages_so_far
        FROM latest
        LEFT JOIN backfill_overrides AS overrides
            ON overrides.channel_id = latest.channel_id
        WHERE latest.kind IN ('backfill_started', 'backfill_progress')
        ORDER BY (overrides.channel_id IS NULL), latest.channel_id
    """
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
    return [
        BackfillInProgress(
            channel_id=str(row[0]),
            messages_so_far=int(row[1]),
        )
        for row in rows
    ]


def _to_subscribers_metrics(subscribers: Sequence[SubscriberSnapshot]) -> SubscribersMetrics:
    ordered = sorted(subscribers, key=lambda item: item.client_id)
    return SubscribersMetrics(
        active_ws_connections=len(ordered),
        by_client=[
            ClientSubscription(
                client_id=item.client_id,
                connected_since=_ensure_aware(item.connected_since),
                subscriptions=item.subscriptions,
            )
            for item in ordered
        ],
    )


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


def _default_socket_mode_state() -> str:
    return "unknown"


def _default_rate_limit_remaining_pct() -> int:
    return 100


def _default_subscribers() -> Sequence[SubscriberSnapshot]:
    return ()
