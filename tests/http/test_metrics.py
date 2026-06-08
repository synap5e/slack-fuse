"""Tests for the `/metrics` DB aggregator."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import psycopg
import pytest
from psycopg.rows import TupleRow

import slack_fuse_server.migrations as server_migrations
from slack_fuse.migrations.runner import apply_migrations
from slack_fuse_server.http.dto import BackfillInProgress, MetricsResponse
from slack_fuse_server.http.metrics import SubscriberSnapshot, collect_metrics

_SERVER_MIGRATIONS_DIR = Path(server_migrations.__file__).parent


@pytest.fixture
def server_db(pg_conn: psycopg.Connection[TupleRow]) -> psycopg.Connection[TupleRow]:
    apply_migrations(pg_conn, _SERVER_MIGRATIONS_DIR)
    return pg_conn


def test_collect_metrics_empty_database(server_db: psycopg.Connection[TupleRow]) -> None:
    started = datetime(2026, 6, 8, 7, 0, 0, tzinfo=UTC)
    metrics = collect_metrics(
        conn=server_db,
        server_started_at=started,
        socket_mode_state="disconnected",
        rate_limit_remaining_pct=100,
        subscribers=[],
    )

    assert metrics.server_started_at == started
    assert metrics.slack.socket_mode_state == "disconnected"
    assert metrics.slack.last_event_at is None
    assert metrics.slack.last_health_kind == "unknown"
    assert metrics.streams == []
    assert metrics.backfill.in_progress == []
    assert metrics.backfill.completed_count == 0
    assert metrics.backfill.aborted_count == 0
    assert metrics.subscribers.active_ws_connections == 0
    assert metrics.subscribers.by_client == []


def test_collect_metrics_aggregates_events_health_and_subscribers(
    server_db: psycopg.Connection[TupleRow],
) -> None:
    with server_db.cursor() as cur:
        cur.execute(
            """
            INSERT INTO events (stream, offset_in_stream, kind, ts, payload, created_at)
            VALUES
                ('users', 1, 'user_added', NULL, '{}'::jsonb, now() - interval '2 minutes'),
                ('users', 2, 'user_renamed', NULL, '{}'::jsonb, now() - interval '10 seconds'),
                (
                    'channel:C111',
                    1,
                    'message',
                    '1700000000.000100',
                    '{"ts":"1700000000.000100"}'::jsonb,
                    now() - interval '5 seconds'
                )
            """
        )
        cur.execute(
            """
            INSERT INTO health_log (kind, payload, created_at)
            VALUES
                (
                    'backfill_started',
                    '{"channel_id":"C111","messages_so_far":42}'::jsonb,
                    now() - interval '30 seconds'
                ),
                (
                    'backfill_completed',
                    '{"channel_id":"C222","events_written":99}'::jsonb,
                    now() - interval '20 seconds'
                ),
                (
                    'backfill_aborted',
                    '{"channel_id":"C333","reason":"exceeded_default_limit"}'::jsonb,
                    now() - interval '15 seconds'
                ),
                ('slack_healthy', '{}'::jsonb, now() - interval '1 seconds')
            """
        )
        cur.execute("INSERT INTO backfill_overrides (channel_id, max_messages) VALUES ('C111', 50000)")
    server_db.commit()

    started = datetime(2026, 6, 8, 6, 30, 0, tzinfo=UTC)
    metrics = collect_metrics(
        conn=server_db,
        server_started_at=started,
        socket_mode_state="connected",
        rate_limit_remaining_pct=87,
        subscribers=[
            SubscriberSnapshot(
                client_id="laptop",
                connected_since=datetime(2026, 6, 8, 6, 45, 0, tzinfo=UTC),
                subscriptions=320,
            )
        ],
    )

    assert metrics.server_started_at == started
    assert metrics.slack.socket_mode_state == "connected"
    assert metrics.slack.rate_limit_budget.remaining_pct == 87
    assert metrics.slack.last_event_at is not None
    assert metrics.slack.last_health_kind == "slack_healthy"

    streams_by_name = {item.stream: item for item in metrics.streams}
    assert set(streams_by_name) == {"users", "channel:C111"}
    assert streams_by_name["users"].head_offset == 2
    assert streams_by_name["users"].events_per_min == 1
    assert streams_by_name["channel:C111"].head_offset == 1
    assert streams_by_name["channel:C111"].events_per_min == 1

    assert metrics.backfill.completed_count == 1
    assert metrics.backfill.aborted_count == 1
    assert metrics.backfill.in_progress == [BackfillInProgress(channel_id="C111", messages_so_far=42)]

    assert metrics.subscribers.active_ws_connections == 1
    assert len(metrics.subscribers.by_client) == 1
    assert metrics.subscribers.by_client[0].client_id == "laptop"
    assert metrics.subscribers.by_client[0].subscriptions == 320

    round_tripped = MetricsResponse.model_validate(metrics.model_dump(mode="json"))
    assert round_tripped == metrics
