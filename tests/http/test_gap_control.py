"""HTTP tests for gap detection, probe liveness, and refill-window triggers."""

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
from slack_fuse_server.http.handlers import GapsDeps, ProbeStatusDeps, RefillWindowDeps
from slack_fuse_server.http.metrics import MetricsSource
from slack_fuse_server.http.server import HttpRequest, route_request


@dataclass(frozen=True, slots=True)
class StaticMetricsSource:
    payload: MetricsResponse

    def snapshot(self) -> MetricsResponse:
        return self.payload


@dataclass(slots=True)
class RecordingRefillTrigger:
    run_id: str | None = "01RUN"
    calls: list[tuple[str, float, float]] | None = None

    def request_window(self, channel_id: str, oldest: float, latest: float) -> str | None:
        if self.calls is None:
            self.calls = []
        self.calls.append((channel_id, oldest, latest))
        return self.run_id


def _metrics_source() -> MetricsSource:
    now = datetime(2026, 7, 6, 1, 0, 0, tzinfo=UTC)
    return StaticMetricsSource(
        MetricsResponse(
            server_started_at=now,
            slack=SlackMetrics(
                socket_mode_state="connected",
                rate_limit_budget=RateLimitBudget(remaining_pct=100),
                last_health_kind="slack_healthy",
            ),
            backfill=BackfillMetrics(completed_count=0, aborted_count=0),
            subscribers=SubscribersMetrics(active_ws_connections=0),
        )
    )


def _database_url_for_conn(conn: psycopg.Connection[TupleRow]) -> str:
    with conn.cursor() as cur:
        cur.execute("SELECT current_schema()")
        row = cur.fetchone()
    assert row is not None and isinstance(row[0], str)
    return make_conninfo(conn.info.dsn, options=f"-c search_path={row[0]}")


def _insert_event(  # noqa: PLR0913 - test seed helper mirrors event columns.
    conn: psycopg.Connection[TupleRow],
    *,
    stream: str,
    offset: int,
    kind: str,
    payload: Mapping[str, object],
    ts: str | None = None,
    source: Mapping[str, object] | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO events (stream, offset_in_stream, kind, ts, payload, source)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (stream, offset, kind, ts, Jsonb(dict(payload)), None if source is None else Jsonb(dict(source))),
        )


def test_get_gap_candidates_returns_day_presence_rows(server_conn: psycopg.Connection[TupleRow]) -> None:
    _insert_event(
        server_conn,
        stream="slurper-health",
        offset=1,
        kind="conversations_history_sampled",
        payload={
            "call_params": {
                "channel": "C123",
                "oldest": "1783036800.000000",
                "latest": "1783123199.999999",
                "limit": 1,
            },
            "response": {"messages": [{"ts": "1783036810.000000"}]},
        },
    )

    response = route_request(
        HttpRequest(method="GET", target="/gap-candidates"),
        metrics_source=_metrics_source(),
        gaps_deps=GapsDeps(database_url=_database_url_for_conn(server_conn)),
    )

    assert response.status_code == 200
    assert json.loads(response.body) == [
        {
            "channel_id": "C123",
            "day": "2026-07-03",
            "oldest_ts": 1783036800.0,
            "latest_ts": 1783123199.999999,
            "slack_sample_ts": "1783036810.000000",
            "sampled_at": json.loads(response.body)[0]["sampled_at"],
            "gap_type": "day_presence",
        }
    ]


def test_get_probes_summarizes_latest_sweep(server_conn: psycopg.Connection[TupleRow]) -> None:
    source = {"run_id": "01PROBE"}
    _insert_event(
        server_conn,
        stream="slurper-health",
        offset=1,
        kind="conversations_history_sampled",
        source=source,
        payload={
            "call_params": {
                "channel": "C1",
                "oldest": "1783036800.000000",
                "latest": "1783123199.999999",
            },
            "response": {"messages": []},
        },
    )
    _insert_event(
        server_conn,
        stream="slurper-health",
        offset=2,
        kind="conversations_history_sampled",
        source=source,
        payload={
            "call_params": {
                "channel": "C2",
                "oldest": "1783036800.000000",
                "latest": "1783123199.999999",
            },
            "response": {"messages": []},
        },
    )
    _insert_event(
        server_conn,
        stream="slurper-health",
        offset=3,
        kind="probe_sweep_completed",
        source=source,
        payload={"started_at": "2026-07-06T01:00:00Z", "ended_at": "2026-07-06T01:00:01Z"},
    )

    response = route_request(
        HttpRequest(method="GET", target="/probe-status"),
        metrics_source=_metrics_source(),
        probe_status_deps=ProbeStatusDeps(
            database_url=_database_url_for_conn(server_conn),
            alert_threshold_seconds=7200,
        ),
    )

    payload = json.loads(response.body)
    assert response.status_code == 200
    assert payload["last_sweep_completed_at"] is not None
    assert payload["age_seconds"] >= 0
    assert payload["channels_covered_last_sweep"] == 2
    assert payload["days_covered_last_sweep"] == 1
    assert payload["alert_threshold_seconds"] == 7200


def test_post_refill_window_queues_and_returns_run_id(server_conn: psycopg.Connection[TupleRow]) -> None:
    trigger = RecordingRefillTrigger()
    response = route_request(
        HttpRequest(
            method="POST",
            target="/refill-window/C123",
            body=b'{"oldest":1783036800.0,"latest":1783123199.999999}',
        ),
        metrics_source=_metrics_source(),
        refill_window_deps=RefillWindowDeps(
            shared_secret=None,
            database_url=_database_url_for_conn(server_conn),
            trigger=trigger,
        ),
    )

    assert response.status_code == 202
    assert json.loads(response.body) == {"status": "refill queued", "run_id": "01RUN"}
    assert trigger.calls == [("C123", 1783036800.0, 1783123199.999999)]


def test_post_refill_window_rejects_matching_inflight(server_conn: psycopg.Connection[TupleRow]) -> None:
    _insert_event(
        server_conn,
        stream="backfill-run:C123",
        offset=1,
        kind="backfill_run_started",
        payload={
            "run_id": "01ACTIVE",
            "triggered_by": "refill-window",
            "params": {"oldest": 1783036800.0, "latest": 1783123199.999999},
        },
    )
    trigger = RecordingRefillTrigger()

    response = route_request(
        HttpRequest(
            method="POST",
            target="/refill-window/C123",
            body=b'{"oldest":1783036800.0,"latest":1783123199.999999}',
        ),
        metrics_source=_metrics_source(),
        refill_window_deps=RefillWindowDeps(
            shared_secret=None,
            database_url=_database_url_for_conn(server_conn),
            trigger=trigger,
        ),
    )

    assert response.status_code == 409
    assert json.loads(response.body) == {"status": "refill already in progress", "run_id": None}
    assert trigger.calls is None
