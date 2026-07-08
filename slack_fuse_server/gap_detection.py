"""SQL-backed control-surface gap/probe status reads."""

from __future__ import annotations

from datetime import UTC, date, datetime
from importlib import resources
from typing import LiteralString, cast

from psycopg import Connection, sql
from psycopg.rows import TupleRow

from slack_fuse_server.http.dto import GapDetectionRow, ProbeStatusResponse

_GAP_SQL = resources.files("slack_fuse_server.queries").joinpath("gap_detection.sql").read_text()


def detect_day_presence_gaps(conn: Connection[TupleRow]) -> list[GapDetectionRow]:
    """Run the packaged day-presence gap query and return typed rows."""
    with conn.cursor() as cur:
        cur.execute(sql.SQL(cast("LiteralString", _GAP_SQL)))
        rows = cur.fetchall()
    return [
        GapDetectionRow(
            channel_id=str(channel_id),
            day=_as_date(day_raw),
            oldest_ts=float(oldest_ts),
            latest_ts=float(latest_ts),
            slack_sample_ts=str(slack_sample_ts),
            sampled_at=_as_datetime(sampled_at),
            gap_type=str(gap_type),
        )
        for channel_id, day_raw, oldest_ts, latest_ts, slack_sample_ts, sampled_at, gap_type in rows
    ]


def fetch_probe_status(conn: Connection[TupleRow], *, alert_threshold_seconds: int) -> ProbeStatusResponse:
    """Summarize the latest completed probe sweep and its day-presence coverage.

    Join semantics: the two event kinds carry different ``source.run_id``
    (``probe_sweep_completed`` is per-sweep, ``conversations_history_sampled``
    is task-lifetime), so an equality check on run_id never matches. We use
    the sweep's payload ``started_at``/``ended_at`` window instead — that's
    the design-intent identity of "samples belonging to this sweep".
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH latest AS (
              SELECT
                  created_at,
                  COALESCE(NULLIF(payload->>'started_at', '')::timestamptz, created_at) AS started_at,
                  COALESCE(NULLIF(payload->>'ended_at', '')::timestamptz, created_at) AS ended_at
              FROM events
              WHERE stream = 'slurper-health'
                AND kind = 'probe_sweep_completed'
              ORDER BY created_at DESC, id DESC
              LIMIT 1
            ),
            samples AS (
              SELECT e.payload
              FROM events e
              JOIN latest l ON true
              WHERE e.stream = 'slurper-health'
                AND e.kind = 'conversations_history_sampled'
                AND e.payload->'call_params' ? 'oldest'
                AND e.payload->'call_params' ? 'latest'
                AND e.created_at >= l.started_at
                AND e.created_at <= l.ended_at
            )
            SELECT
                l.created_at,
                EXTRACT(EPOCH FROM (now() - l.created_at))::bigint AS age_seconds,
                COUNT(DISTINCT samples.payload->'call_params'->>'channel') AS channels_covered,
                COUNT(DISTINCT samples.payload->'call_params'->>'oldest') AS days_covered
            FROM latest l
            LEFT JOIN samples ON true
            GROUP BY l.created_at
            """,
        )
        row = cur.fetchone()
    if row is None:
        return ProbeStatusResponse(
            last_sweep_completed_at=None,
            age_seconds=None,
            channels_covered_last_sweep=0,
            days_covered_last_sweep=0,
            alert_threshold_seconds=alert_threshold_seconds,
        )
    completed_at_raw, age_raw, channels_raw, days_raw = row
    return ProbeStatusResponse(
        last_sweep_completed_at=_as_datetime(completed_at_raw),
        age_seconds=None if age_raw is None else int(age_raw),
        channels_covered_last_sweep=int(channels_raw or 0),
        days_covered_last_sweep=int(days_raw or 0),
        alert_threshold_seconds=alert_threshold_seconds,
    )


def refill_window_in_flight(
    conn: Connection[TupleRow],
    *,
    channel_id: str,
    oldest: float,
    latest: float,
) -> bool:
    """True when an identical refill-window run has started and not finished."""
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH started AS (
              SELECT payload->>'run_id' AS run_id
              FROM events
              WHERE stream = %s
                AND kind = 'backfill_run_started'
                AND payload->'params'->>'oldest' ~ '^[0-9]+(\\.[0-9]+)?$'
                AND payload->'params'->>'latest' ~ '^[0-9]+(\\.[0-9]+)?$'
                AND abs((payload->'params'->>'oldest')::double precision - %s) < 0.000001
                AND abs((payload->'params'->>'latest')::double precision - %s) < 0.000001
                AND payload ? 'run_id'
            )
            SELECT 1
            FROM started s
            WHERE NOT EXISTS (
              SELECT 1
              FROM events finished
              WHERE finished.stream = %s
                AND finished.kind = 'backfill_run_finished'
                AND finished.payload->>'run_id' = s.run_id
            )
            LIMIT 1
            """,
            (f"backfill-run:{channel_id}", oldest, latest, f"backfill-run:{channel_id}"),
        )
        return cur.fetchone() is not None


def _as_date(value: object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value)
    return cast(date, value)


def _as_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value)
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
    return cast(datetime, value)
