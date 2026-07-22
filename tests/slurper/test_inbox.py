# pyright: reportPrivateUsage=false
"""Durable webhook inbox retry, dedup, retention, and liveness semantics."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

import psycopg
import pytest
import trio

from slack_fuse.models import EventsApiPayload
from slack_fuse_server._json import JsonObject
from slack_fuse_server.slack_events.inbox import (
    InboxWriter,
    _claim_pending,
    _dispatch_claimed,
    consume,
    delete_processed_older_than,
    dispatch_backoff_seconds,
    emit_telemetry,
    enqueue,
    ensure_consumer_never_returns,
    read_inbox_metrics,
)
from slack_fuse_server.slack_events.types import DispatchErrorCode, DispatchTransientError, SlackEventSource
from slack_fuse_server.slurper.health import HealthEmitter, HealthKind
from slack_fuse_server.slurper.spans import SpanRecorder
from tests.conftest import make_test_writer

if TYPE_CHECKING:
    from psycopg.rows import TupleRow

    from tests.conftest import ServerConnFactory


def _envelope(event_id: str, *, channel: str = "C1") -> JsonObject:
    return {
        "type": "event_callback",
        "event_id": event_id,
        "event_time": 1_800_000_000,
        "event": {"type": "message", "channel": channel, "ts": "1.000001", "text": "hello"},
    }


@dataclass(slots=True)
class _RecordingHealth:
    events: list[tuple[HealthKind, JsonObject | None]] = field(default_factory=list)

    async def emit(self, kind: HealthKind, payload: JsonObject | None = None) -> int:
        self.events.append((kind, payload))
        return len(self.events)


@dataclass(slots=True)
class _StubDispatcher:
    failures: int = 0
    calls: list[str] = field(default_factory=list)

    async def dispatch(
        self,
        payload: EventsApiPayload,
        raw_event: JsonObject,
        source_ctx: SlackEventSource,
        span: SpanRecorder | None = None,
    ) -> None:
        del payload, raw_event, span
        self.calls.append(source_ctx.event_id)
        if self.failures > 0:
            self.failures -= 1
            raise DispatchTransientError(DispatchErrorCode.PG_TIMEOUT)


def _row_state(conn: psycopg.Connection[TupleRow], event_id: str) -> tuple[object, ...]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT processed_at, attempt_count, next_attempt_at, dispatch_error, dead_lettered_at "
            "FROM slack_event_inbox WHERE event_id = %s",
            (event_id,),
        )
        row = cur.fetchone()
    assert row is not None
    return row


async def _wait_until(predicate: Callable[[], bool], *, timeout: float = 2.0) -> None:
    with trio.fail_after(timeout):
        while not predicate():
            await trio.sleep(0.01)


def test_duplicate_enqueue_inserts_one_row(server_conn_factory: ServerConnFactory) -> None:
    conn = server_conn_factory()
    assert enqueue(conn, "Ev1", _envelope("Ev1")) is True
    assert enqueue(conn, "Ev1", _envelope("Ev1")) is False
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM slack_event_inbox WHERE event_id = 'Ev1'")
        row = cur.fetchone()
    assert row is not None and row[0] == 1


@pytest.mark.trio
async def test_concurrent_duplicate_enqueue_is_conflict_safe(server_conn_factory: ServerConnFactory) -> None:
    conn = server_conn_factory()
    writer = InboxWriter(conn)
    results: list[bool] = []

    async def post() -> None:
        results.append(await writer.enqueue("EvConcurrent", _envelope("EvConcurrent")))

    async with trio.open_nursery() as nursery:
        nursery.start_soon(post)
        nursery.start_soon(post)

    assert sorted(results) == [False, True]
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM slack_event_inbox WHERE event_id = 'EvConcurrent'")
        row = cur.fetchone()
    assert row is not None and row[0] == 1


def test_notify_is_delivered_only_after_insert_commit(server_conn_factory: ServerConnFactory) -> None:
    writer = server_conn_factory()
    listener = server_conn_factory()
    with listener.cursor() as cur:
        cur.execute("LISTEN slack_event_inbox_new")
    with writer.cursor() as cur:
        cur.execute("BEGIN")
        cur.execute(
            "INSERT INTO slack_event_inbox (event_id, envelope) VALUES ('EvNotify', '{}'::jsonb)"
        )
        cur.execute("SELECT pg_notify('slack_event_inbox_new', 'EvNotify')")
    assert next(listener.notifies(timeout=0.05, stop_after=1), None) is None
    with writer.cursor() as cur:
        cur.execute("COMMIT")
    notification = next(listener.notifies(timeout=2.0, stop_after=1), None)
    assert notification is not None and notification.payload == "EvNotify"


@pytest.mark.trio
async def test_consumer_processes_committed_row(server_conn_factory: ServerConnFactory) -> None:
    writer = server_conn_factory()
    consumer_conn = server_conn_factory()
    query = server_conn_factory()
    enqueue(writer, "EvHappy", _envelope("EvHappy"))
    dispatcher = _StubDispatcher()
    health = _RecordingHealth()

    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            consume,
            consumer_conn,
            dispatcher,
            cast(HealthEmitter, health),
        )
        await _wait_until(lambda: _row_state(query, "EvHappy")[0] is not None)
        nursery.cancel_scope.cancel()

    assert dispatcher.calls == ["EvHappy"]
    assert _row_state(query, "EvHappy")[3] is None


@pytest.mark.trio
async def test_transient_failure_survives_restart_then_succeeds(server_conn_factory: ServerConnFactory) -> None:
    writer = server_conn_factory()
    first_conn = server_conn_factory()
    second_conn = server_conn_factory()
    query = server_conn_factory()
    enqueue(writer, "EvRetry", _envelope("EvRetry"))
    failing = _StubDispatcher(failures=1)
    health = _RecordingHealth()

    async with trio.open_nursery() as nursery:
        nursery.start_soon(consume, first_conn, failing, cast(HealthEmitter, health))
        await _wait_until(lambda: _row_state(query, "EvRetry")[1] == 1)
        nursery.cancel_scope.cancel()

    with query.transaction(), query.cursor() as cur:
        cur.execute("UPDATE slack_event_inbox SET next_attempt_at = NOW() WHERE event_id = 'EvRetry'")
    succeeding = _StubDispatcher()
    async with trio.open_nursery() as nursery:
        nursery.start_soon(consume, second_conn, succeeding, cast(HealthEmitter, health))
        await _wait_until(lambda: _row_state(query, "EvRetry")[0] is not None)
        nursery.cancel_scope.cancel()

    assert failing.calls == ["EvRetry"]
    assert succeeding.calls == ["EvRetry"]
    assert any(kind is HealthKind.WEBHOOK_DISPATCH_FAILED for kind, _ in health.events)


@pytest.mark.trio
async def test_poison_row_backoff_does_not_starve_newer_rows(server_conn_factory: ServerConnFactory) -> None:
    writer = server_conn_factory()
    conn = server_conn_factory()
    query = server_conn_factory()
    for index in range(1, 6):
        enqueue(writer, f"Ev{index}", _envelope(f"Ev{index}"))

    @dataclass(slots=True)
    class _FirstFails(_StubDispatcher):
        async def dispatch(
            self,
            payload: EventsApiPayload,
            raw_event: JsonObject,
            source_ctx: SlackEventSource,
            span: SpanRecorder | None = None,
        ) -> None:
            self.calls.append(source_ctx.event_id)
            if source_ctx.event_id == "Ev1":
                raise DispatchTransientError(DispatchErrorCode.PG_TIMEOUT)

    dispatcher = _FirstFails()
    async with trio.open_nursery() as nursery:
        nursery.start_soon(consume, conn, dispatcher, cast(HealthEmitter, _RecordingHealth()))

        def later_processed() -> bool:
            with query.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM slack_event_inbox "
                    "WHERE event_id <> 'Ev1' AND processed_at IS NOT NULL"
                )
                row = cur.fetchone()
            return row is not None and row[0] == 4

        await _wait_until(later_processed)
        nursery.cancel_scope.cancel()

    state = _row_state(query, "Ev1")
    assert state[0] is None and state[1] == 1 and state[3] == "pg_timeout"
    assert dispatcher.calls == ["Ev1", "Ev2", "Ev3", "Ev4", "Ev5"]


@pytest.mark.trio
async def test_post_increment_attempt_twelve_dead_letters(server_conn_factory: ServerConnFactory) -> None:
    writer = server_conn_factory()
    conn = server_conn_factory()
    enqueue(writer, "EvDead", _envelope("EvDead"))
    with writer.transaction(), writer.cursor() as cur:
        cur.execute("UPDATE slack_event_inbox SET attempt_count = 11 WHERE event_id = 'EvDead'")
    row = _claim_pending(conn)
    assert row is not None
    dispatcher = _StubDispatcher(failures=1)
    health = _RecordingHealth()
    await _dispatch_claimed(conn, row, dispatcher, cast(HealthEmitter, health), None, 1.0)
    state = _row_state(writer, "EvDead")
    assert state[1] == 12
    assert state[4] is not None
    assert [kind for kind, _ in health.events] == [
        HealthKind.WEBHOOK_DISPATCH_FAILED,
        HealthKind.WEBHOOK_DEAD_LETTER,
    ]


def test_backoff_caps_after_attempt_eight() -> None:
    assert int(dispatch_backoff_seconds(8)) == 512
    assert int(dispatch_backoff_seconds(9)) == 512
    assert int(dispatch_backoff_seconds(12)) == 512


def test_retention_deletes_old_processed_but_not_dead_letter(server_conn_factory: ServerConnFactory) -> None:
    conn = server_conn_factory()
    enqueue(conn, "EvOld", _envelope("EvOld"))
    enqueue(conn, "EvDeadOld", _envelope("EvDeadOld"))
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "UPDATE slack_event_inbox SET processed_at = NOW() - INTERVAL '49 hours' WHERE event_id = 'EvOld'"
        )
        cur.execute(
            "UPDATE slack_event_inbox SET processed_at = NOW() - INTERVAL '49 hours', "
            "dead_lettered_at = NOW() - INTERVAL '49 hours' WHERE event_id = 'EvDeadOld'"
        )
    assert delete_processed_older_than(conn) == 1
    with conn.cursor() as cur:
        cur.execute("SELECT event_id FROM slack_event_inbox ORDER BY event_id")
        rows = cur.fetchall()
    assert [row[0] for row in rows] == ["EvDeadOld"]


def test_inbox_metrics_count_pending_and_oldest_age(server_conn_factory: ServerConnFactory) -> None:
    conn = server_conn_factory()
    for index in range(3):
        enqueue(conn, f"EvMetric{index}", _envelope(f"EvMetric{index}"))
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "UPDATE slack_event_inbox SET received_at = NOW() - INTERVAL '70 seconds' "
            "WHERE event_id = 'EvMetric0'"
        )
    metrics = read_inbox_metrics(conn)
    assert metrics.depth == 3
    assert 69 <= metrics.oldest_pending_age_s <= 75


@pytest.mark.trio
async def test_periodic_telemetry_emits_depth_age_and_liveness(server_conn_factory: ServerConnFactory) -> None:
    conn = server_conn_factory()
    for index in range(3):
        enqueue(conn, f"EvTelemetry{index}", _envelope(f"EvTelemetry{index}"))
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "UPDATE slack_event_inbox SET received_at = NOW() - INTERVAL '65 seconds' "
            "WHERE event_id = 'EvTelemetry0'"
        )
    health = _RecordingHealth()
    writer = make_test_writer(conn)
    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            emit_telemetry,
            writer,
            cast(HealthEmitter, health),
            trio.CapacityLimiter(1),
        )
        await _wait_until(lambda: len(health.events) >= 3)
        nursery.cancel_scope.cancel()

    assert health.events[0] == (HealthKind.WEBHOOK_INBOX_DEPTH, {"value": 3})
    oldest = health.events[1]
    assert oldest[0] is HealthKind.WEBHOOK_INBOX_OLDEST_PENDING_AGE_S
    assert oldest[1] is not None
    value = oldest[1].get("value")
    assert isinstance(value, float) and 64 <= value <= 70
    assert health.events[2] == (HealthKind.WEBHOOK_CONSUMER_ALIVE, {"counter": 1})


@pytest.mark.trio
async def test_consumer_wrapper_rejects_clean_return() -> None:
    async def returns() -> None:
        await trio.lowlevel.checkpoint()

    with pytest.raises(RuntimeError, match="inbox consumer must not exit"):
        await ensure_consumer_never_returns(returns)


@pytest.mark.trio
async def test_consumer_infrastructure_failure_propagates(server_conn_factory: ServerConnFactory) -> None:
    conn = server_conn_factory()
    conn.close()

    with pytest.raises(psycopg.Error):
        await consume(conn, _StubDispatcher(), cast(HealthEmitter, _RecordingHealth()))
