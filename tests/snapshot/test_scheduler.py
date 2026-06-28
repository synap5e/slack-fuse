"""Snapshot scheduler: cadence decisions + the periodic tick.

Covers acceptance criterion 2 (the periodic worker fires per the cadence,
verified by fast-forwarding the trigger condition: a low event-count threshold
and back-dated event timestamps).
"""

from __future__ import annotations

import psycopg
import trio
from psycopg.rows import TupleRow

from slack_fuse.models import Message
from slack_fuse_server._json import JsonObject
from slack_fuse_server.slurper.offsets import EventRecord, write_event
from slack_fuse_server.snapshot.scheduler import SnapshotScheduler, decide_trigger, due_streams
from tests.conftest import RecordingSupervisor


def _write_message(conn: psycopg.Connection[TupleRow], stream: str, ts: str) -> None:
    payload: JsonObject = Message.model_validate({"ts": ts, "user": "U1", "text": ts}).model_dump(mode="json")
    write_event(conn, EventRecord(stream=stream, kind="message", ts=ts, payload=payload, dedup=True))


def _backdate(conn: psycopg.Connection[TupleRow], stream: str, seconds: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE events SET created_at = now() - make_interval(secs => %s) WHERE stream = %s",
            (seconds, stream),
        )


# === decide_trigger (pure) ===


def test_decide_trigger_event_count_wins() -> None:
    # Both thresholds crossed → event_count is the reported (cheaper) trigger.
    assert decide_trigger(10, 99999.0, every_n_events=5, max_age_seconds=10.0) == "event_count"


def test_decide_trigger_time_when_only_age_crossed() -> None:
    assert decide_trigger(2, 100.0, every_n_events=5, max_age_seconds=10.0) == "time"


def test_decide_trigger_none_below_both() -> None:
    assert decide_trigger(2, 1.0, every_n_events=5, max_age_seconds=10.0) is None


def test_decide_trigger_none_without_new_events() -> None:
    assert decide_trigger(0, 99999.0, every_n_events=5, max_age_seconds=10.0) is None


# === due_streams (DB) ===


def test_due_streams_event_count(server_conn: psycopg.Connection[TupleRow]) -> None:
    stream = "channel:C1"
    for i in range(5):
        _write_message(server_conn, stream, f"10{i}.000000")
    due = due_streams(server_conn, every_n_events=5, max_age_seconds=99999.0)
    assert due == [(stream, "event_count")]


def test_due_streams_time_trigger(server_conn: psycopg.Connection[TupleRow]) -> None:
    stream = "channel:C1"
    for i in range(2):
        _write_message(server_conn, stream, f"20{i}.000000")
    _backdate(server_conn, stream, seconds=100)
    # Count threshold not met, but the oldest uncovered event is older than max age.
    due = due_streams(server_conn, every_n_events=1000, max_age_seconds=10.0)
    assert due == [(stream, "time")]


def test_due_streams_excludes_health_stream(server_conn: psycopg.Connection[TupleRow]) -> None:
    for _ in range(5):
        write_event(server_conn, EventRecord(stream="slurper-health", kind="slack_healthy", ts=None, payload={}))
    assert due_streams(server_conn, every_n_events=1, max_age_seconds=0.0) == []


def test_due_streams_empty_below_thresholds(server_conn: psycopg.Connection[TupleRow]) -> None:
    _write_message(server_conn, "channel:C1", "300.000000")
    assert due_streams(server_conn, every_n_events=1000, max_age_seconds=99999.0) == []


def test_due_streams_excludes_singleton_streams(server_conn: psycopg.Connection[TupleRow]) -> None:
    """Review P0-C: the scheduler does not auto-snapshot singleton streams.

    The WS server never redirects ``users`` / ``channel-list`` to a snapshot
    (the split client can't full-state-apply them), so generating their
    snapshots would be wasted work. Only the channel stream is due here.
    """
    for i in range(5):
        _write_message(server_conn, "users", f"60{i}.000000")
        _write_message(server_conn, "channel-list", f"61{i}.000000")
        _write_message(server_conn, "channel:C1", f"62{i}.000000")
    due = due_streams(server_conn, every_n_events=5, max_age_seconds=99999.0)
    assert due == [("channel:C1", "event_count")]


# === tick (async) ===


def _new_scheduler(conn: psycopg.Connection[TupleRow], *, every_n_events: int) -> SnapshotScheduler:
    return SnapshotScheduler(
        conn,
        every_n_events=every_n_events,
        max_age_seconds=99999.0,
        limiter=trio.CapacityLimiter(1),
    )


def test_tick_generates_due_snapshot_then_is_idempotent(server_conn: psycopg.Connection[TupleRow]) -> None:
    stream = "channel:C1"
    for i in range(3):
        _write_message(server_conn, stream, f"40{i}.000000")

    scheduler = _new_scheduler(server_conn, every_n_events=3)
    results = trio.run(scheduler.tick)

    assert len(results) == 1
    assert results[0].stream == stream
    assert results[0].at_offset == 3
    assert results[0].generation_trigger == "event_count"

    # No new events → the next tick generates nothing.
    assert trio.run(scheduler.tick) == []

    # One row persisted.
    with server_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM snapshots WHERE stream = %s", (stream,))
        row = cur.fetchone()
    assert row is not None and row[0] == 1


def test_tick_covers_multiple_streams(server_conn: psycopg.Connection[TupleRow]) -> None:
    for i in range(3):
        _write_message(server_conn, "channel:C1", f"50{i}.000000")
        _write_message(server_conn, "channel:C2", f"51{i}.000000")

    scheduler = _new_scheduler(server_conn, every_n_events=3)
    results = trio.run(scheduler.tick)

    assert {r.stream for r in results} == {"channel:C1", "channel:C2"}


def test_tick_declares_supervisor_phases(server_conn: psycopg.Connection[TupleRow]) -> None:
    stream = "channel:C_PHASE"
    for i in range(3):
        _write_message(server_conn, stream, f"70{i}.000000")
    scheduler = _new_scheduler(server_conn, every_n_events=3)
    supervisor = RecordingSupervisor()

    results = trio.run(scheduler.tick, supervisor)

    assert [result.stream for result in results] == [stream]
    phases = [(item.task_name, item.phase, item.details) for item in supervisor.declarations]
    assert ("snapshot", "tick", None) in phases
    assert ("snapshot", "generating", {"stream": stream, "trigger": "event_count"}) in phases
