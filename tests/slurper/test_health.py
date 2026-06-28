"""Health emitter: writes a `slurper-health` event AND mirrors to `health_log`.

Acceptance criteria 3 & 4. Async bodies run via `trio.run` so the suite needs
no pytest-trio mode configured.
"""

from __future__ import annotations

import json

import psycopg
import pytest
import trio
from psycopg import Cursor
from psycopg.rows import TupleRow

import slack_fuse_server.slurper.health as health_module
from slack_fuse_server.slurper.health import HealthEmitter, HealthKind, SlackDegradedTracker
from slack_fuse_server.slurper.offsets import EventRecord, insert_event
from tests.conftest import make_test_writer


def _events(conn: psycopg.Connection[TupleRow], stream: str) -> list[tuple[int, str, object]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT offset_in_stream, kind, payload FROM events WHERE stream = %s ORDER BY offset_in_stream",
            (stream,),
        )
        return [(int(r[0]), str(r[1]), r[2]) for r in cur.fetchall()]


def _health_log(conn: psycopg.Connection[TupleRow]) -> list[tuple[str, object]]:
    with conn.cursor() as cur:
        cur.execute("SELECT kind, payload FROM health_log ORDER BY id")
        return [(str(r[0]), r[1]) for r in cur.fetchall()]


def test_emit_writes_event_and_mirrors_health_log(server_conn: psycopg.Connection[TupleRow]) -> None:
    async def body() -> None:
        health = HealthEmitter(make_test_writer(server_conn))
        off1 = await health.emit(HealthKind.SLACK_HEALTHY)
        off2 = await health.emit(HealthKind.SOCKET_MODE_RECONNECTED, {"gap_seconds": 12.5})
        assert (off1, off2) == (1, 2)

    trio.run(body)

    events = _events(server_conn, "slurper-health")
    assert [(o, k) for o, k, _ in events] == [(1, "slack_healthy"), (2, "socket_mode_reconnected")]
    # Second event carries its payload.
    assert events[1][2] == {"gap_seconds": 12.5}

    log = _health_log(server_conn)
    assert log == [("slack_healthy", {}), ("socket_mode_reconnected", {"gap_seconds": 12.5})]
    # health_log mirror and event payload agree.
    assert json.dumps(log[1][1]) == json.dumps(events[1][2])


def test_emit_rolls_back_event_and_health_log_view_on_failure(
    server_conn: psycopg.Connection[TupleRow],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _insert_then_fail(cur: Cursor[TupleRow], offset: int, record: EventRecord) -> bool:
        inserted = insert_event(cur, offset, record)
        raise RuntimeError(f"synthetic failure after insert={inserted}")

    monkeypatch.setattr(health_module, "insert_event", _insert_then_fail)

    async def body() -> None:
        health = HealthEmitter(make_test_writer(server_conn))
        with pytest.raises(RuntimeError, match="synthetic failure"):
            await health.emit(HealthKind.SLACK_HEALTHY)

    trio.run(body)

    assert _events(server_conn, "slurper-health") == []
    assert _health_log(server_conn) == []


def test_slack_degraded_tracker_debounces_under_threshold(server_conn: psycopg.Connection[TupleRow]) -> None:
    """A blip shorter than min_duration emits nothing; a sustained episode emits once."""
    clock = {"t": 0.0}

    async def body() -> None:
        health = HealthEmitter(make_test_writer(server_conn))
        tracker = SlackDegradedTracker(health, min_duration_s=30.0, clock=lambda: clock["t"])

        # Episode 1: first failure starts the clock — no emit yet.
        await tracker.record_failure("timeout")
        clock["t"] = 10.0
        await tracker.record_failure("timeout")  # 10s < 30s — still silent
        # Recovered before crossing the threshold: nothing was ever emitted.
        tracker.record_healthy()

        # Episode 2: a sustained outage crosses the threshold and emits exactly once.
        clock["t"] = 100.0
        await tracker.record_failure("api_5xx")  # episode start
        clock["t"] = 131.0
        await tracker.record_failure("api_5xx")  # 31s >= 30s — emit
        clock["t"] = 140.0
        await tracker.record_failure("api_5xx")  # already emitted — no duplicate

    trio.run(body)

    log = _health_log(server_conn)
    degraded = [payload for kind, payload in log if kind == "slack_degraded"]
    assert degraded == [{"reason": "api_5xx"}]


def test_slack_degraded_tracker_emits_immediately_with_zero_duration(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    """min_duration_s=0 means the first failure is already "past" the threshold."""

    async def body() -> None:
        health = HealthEmitter(make_test_writer(server_conn))
        tracker = SlackDegradedTracker(health, min_duration_s=0.0, clock=lambda: 0.0)
        await tracker.record_failure("rate_limited")
        await tracker.record_failure("rate_limited")  # still one episode — no duplicate

    trio.run(body)

    log = _health_log(server_conn)
    degraded = [payload for kind, payload in log if kind == "slack_degraded"]
    assert degraded == [{"reason": "rate_limited"}]
