"""Unit coverage for the projector's durable single-connection wrapper."""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

import pytest
from psycopg import Connection, OperationalError
from psycopg.rows import TupleRow

from slack_fuse.control import ControlState
from slack_fuse.projector.reconnecting_conn import (
    ClosedConnectionError,
    HealthEventPayload,
    ReconnectingConnection,
    ReconnectRecord,
)

if TYPE_CHECKING:
    from psycopg.abc import Params, QueryNoTemplate


@dataclass(slots=True)
class _Behavior:
    execute_errors: list[OperationalError] = field(default_factory=list)
    rows: list[TupleRow] = field(default_factory=lambda: [(1,)])
    execute_delay_s: float = 0.0
    active_executes: int = 0
    max_active_executes: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)


class _FakeCursor:
    def __init__(self, behavior: _Behavior) -> None:
        self._behavior = behavior
        self._closed = False

    def execute(
        self,
        _query: QueryNoTemplate,
        _params: Params | None = None,
        *,
        prepare: bool | None = None,
        binary: bool | None = None,
    ) -> _FakeCursor:
        del prepare, binary
        with self._behavior.lock:
            self._behavior.active_executes += 1
            self._behavior.max_active_executes = max(
                self._behavior.max_active_executes,
                self._behavior.active_executes,
            )
        try:
            if self._behavior.execute_delay_s:
                time.sleep(self._behavior.execute_delay_s)
            if self._behavior.execute_errors:
                raise self._behavior.execute_errors.pop(0)
            return self
        finally:
            with self._behavior.lock:
                self._behavior.active_executes -= 1

    def fetchone(self) -> TupleRow | None:
        return self._behavior.rows[0] if self._behavior.rows else None

    def fetchmany(self, size: int = 0) -> list[TupleRow]:
        return self._behavior.rows[:size] if size else list(self._behavior.rows)

    def fetchall(self) -> list[TupleRow]:
        return list(self._behavior.rows)

    def close(self) -> None:
        self._closed = True


class _FakeConnection:
    def __init__(self, behavior: _Behavior) -> None:
        self._behavior = behavior
        self.closed = False
        self.autocommit = True

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._behavior)

    def close(self) -> None:
        self.closed = True


def _as_connection(fake: _FakeConnection) -> Connection[TupleRow]:
    return cast("Connection[TupleRow]", fake)


def test_basic_reconnect_on_operational_error() -> None:
    first_error = OperationalError("server socket disappeared")
    connections = [
        _FakeConnection(_Behavior(execute_errors=[first_error])),
        _FakeConnection(_Behavior(rows=[(42,)])),
    ]
    reconnect_reasons: list[str] = []

    def factory(_dsn: str) -> Connection[TupleRow]:
        return _as_connection(connections.pop(0))

    conn = ReconnectingConnection(
        "postgresql://test",
        connection_factory=factory,
        on_reconnect=reconnect_reasons.append,
    )

    with conn.cursor() as cur:
        _ = cur.execute("SELECT 42")
        assert cur.fetchone() == (42,)

    assert reconnect_reasons == ["server socket disappeared"]
    assert connections == []


def test_reconnect_records_exactly_one_result_per_whole_operation_when_retry_fails() -> None:
    first_error = OperationalError("first socket lost")
    second_error = OperationalError("replacement socket lost")
    connections = [
        _FakeConnection(_Behavior(execute_errors=[first_error])),
        _FakeConnection(_Behavior(execute_errors=[second_error])),
    ]
    events: list[tuple[str, HealthEventPayload]] = []

    def factory(_dsn: str) -> Connection[TupleRow]:
        return _as_connection(connections.pop(0))

    conn = ReconnectingConnection(
        "postgresql://test",
        connection_factory=factory,
        on_health_event=lambda kind, payload: events.append((kind, payload)),
    )

    with pytest.raises(OperationalError) as raised, conn.cursor() as cur:
        _ = cur.execute("SELECT 1")

    assert raised.value is second_error
    assert connections == []
    records = [cast("ReconnectRecord", payload) for kind, payload in events if kind == "reconnect_recorded"]
    assert len(records) == 1
    assert records[0]["attempt_result"] == "failed"
    assert records[0]["failure_phase"] == "execute"


def test_reconnect_log_is_not_duplicated(caplog: pytest.LogCaptureFixture) -> None:
    connections = [
        _FakeConnection(_Behavior(execute_errors=[OperationalError("socket lost")])),
        _FakeConnection(_Behavior(rows=[(42,)])),
    ]
    events: list[tuple[str, HealthEventPayload]] = []

    def factory(_dsn: str) -> Connection[TupleRow]:
        return _as_connection(connections.pop(0))

    caplog.set_level("INFO", logger="slack_fuse.projector.reconnecting_conn")
    conn = ReconnectingConnection(
        "postgresql://test",
        connection_factory=factory,
        name="block_sync",
        on_health_event=lambda kind, payload: events.append((kind, payload)),
    )

    assert conn.execute("SELECT 42").fetchone() == (42,)

    records = [cast("ReconnectRecord", payload) for kind, payload in events if kind == "reconnect_recorded"]
    log_lines = [record.getMessage() for record in caplog.records if "op=postgres-reconnect" in record.getMessage()]
    assert len(records) == 1
    assert records[0]["attempt_result"] == "succeeded"
    assert records[0]["connection"] == "block_sync"
    assert len(log_lines) == 1
    assert "connection=block_sync" in log_lines[0]


def test_explicit_close_never_reopens() -> None:
    factory_calls = 0

    def factory(_dsn: str) -> Connection[TupleRow]:
        nonlocal factory_calls
        factory_calls += 1
        return _as_connection(_FakeConnection(_Behavior()))

    conn = ReconnectingConnection("postgresql://test", connection_factory=factory)
    conn.close()

    with pytest.raises(ClosedConnectionError, match="explicitly closed"):
        _ = conn.execute("SELECT 1")
    assert factory_calls == 0


def test_shared_wrapper_serializes_two_threads_across_reopen() -> None:
    first = _FakeConnection(_Behavior(execute_errors=[OperationalError("dead")]))
    healthy_behavior = _Behavior(execute_delay_s=0.05)
    second = _FakeConnection(healthy_behavior)
    connections = [first, second]

    def factory(_dsn: str) -> Connection[TupleRow]:
        return _as_connection(connections.pop(0))

    conn = ReconnectingConnection("postgresql://test", connection_factory=factory)
    barrier = threading.Barrier(3)
    results: list[TupleRow | None] = []
    errors: list[BaseException] = []

    def select_one() -> None:
        barrier.wait()
        try:
            with conn.cursor() as cur:
                _ = cur.execute("SELECT 1")
                results.append(cur.fetchone())
        except BaseException as exc:  # pragma: no cover - assertion surfaces it
            errors.append(exc)

    threads = [threading.Thread(target=select_one), threading.Thread(target=select_one)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(timeout=2.0)

    assert errors == []
    assert results == [(1,), (1,)]
    assert healthy_behavior.max_active_executes == 1
    assert all(not thread.is_alive() for thread in threads)


def test_client_wedged_and_recovered_emit_once_per_episode() -> None:
    broken = _FakeConnection(_Behavior(execute_errors=[OperationalError("original socket lost")]))
    healthy = _FakeConnection(_Behavior())
    factory_results: list[Connection[TupleRow] | OperationalError] = [
        _as_connection(broken),
        OperationalError("reopen 1 failed"),
        OperationalError("reopen 2 failed"),
        OperationalError("reopen 3 failed"),
        OperationalError("reopen 4 failed"),
        OperationalError("reopen 5 failed"),
        _as_connection(healthy),
    ]
    now = 0.0
    events: list[tuple[str, HealthEventPayload]] = []
    control_state = ControlState(now_fn=lambda: datetime(2026, 8, 2, tzinfo=UTC))

    def factory(_dsn: str) -> Connection[TupleRow]:
        result = factory_results.pop(0)
        if isinstance(result, OperationalError):
            raise result
        return result

    def now_fn() -> float:
        return now

    def record_health(kind: str, payload: HealthEventPayload) -> None:
        events.append((kind, payload))
        if kind != "reconnect_recorded":
            control_state.record_client_health("test_connection", kind, payload["reason"])

    conn = ReconnectingConnection(
        "postgresql://test",
        connection_factory=factory,
        on_health_event=record_health,
        now_fn=now_fn,
    )

    for attempt in range(5):
        now = float(attempt * 30)
        with pytest.raises(OperationalError):
            _ = conn.execute("SELECT 1")

    assert [(kind, payload["reason"]) for kind, payload in events if kind != "reconnect_recorded"] == [
        ("client_wedged", "reopen 5 failed")
    ]
    wedged_status = json.loads(control_state.render())
    assert wedged_status["last_client_wedged"]["kind"] == "client_wedged"
    assert wedged_status["last_client_recovered"] is None

    now = 150.0
    with conn.cursor() as cur:
        _ = cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)

    assert [(kind, payload["reason"]) for kind, payload in events if kind != "reconnect_recorded"] == [
        ("client_wedged", "reopen 5 failed"),
        ("client_recovered", "reopen 5 failed"),
    ]
    recovered_status = json.loads(control_state.render())
    assert recovered_status["last_client_recovered"]["kind"] == "client_recovered"
    assert factory_results == []


def test_failures_outside_window_do_not_wedge() -> None:
    broken = _FakeConnection(_Behavior(execute_errors=[OperationalError("socket lost")]))
    failures = [OperationalError(f"reopen {index} failed") for index in range(5)]
    factory_results: list[Connection[TupleRow] | OperationalError] = [_as_connection(broken), *failures]
    now = 0.0
    events: list[tuple[str, HealthEventPayload]] = []

    def factory(_dsn: str) -> Connection[TupleRow]:
        result = factory_results.pop(0)
        if isinstance(result, OperationalError):
            raise result
        return result

    conn = ReconnectingConnection(
        "postgresql://test",
        connection_factory=factory,
        on_health_event=lambda kind, payload: events.append((kind, payload)),
        now_fn=lambda: now,
    )

    for attempt in range(5):
        now = float(attempt * 181)
        with pytest.raises(OperationalError):
            _ = conn.execute("SELECT 1")

    assert [kind for kind, _payload in events if kind != "reconnect_recorded"] == []


def test_reconnect_callback_failure_does_not_break_query() -> None:
    first = _FakeConnection(_Behavior(execute_errors=[OperationalError("dead")]))
    second = _FakeConnection(_Behavior())
    connections = [first, second]

    def factory(_dsn: str) -> Connection[TupleRow]:
        return _as_connection(connections.pop(0))

    def broken_callback(_reason: str) -> None:
        msg = "observer bug"
        raise RuntimeError(msg)

    conn = ReconnectingConnection("postgresql://test", connection_factory=factory, on_reconnect=broken_callback)
    assert conn.execute("SELECT 1").fetchone() == (1,)


def test_factory_type_accepts_callable_shape() -> None:
    """Keep the public injection seam obvious to strict type checking."""

    def factory(_dsn: str) -> Connection[TupleRow]:
        return _as_connection(_FakeConnection(_Behavior()))

    typed_factory: Callable[[str], Connection[TupleRow]] = factory
    conn = ReconnectingConnection("postgresql://test", connection_factory=typed_factory)
    assert conn.execute("SELECT 1").fetchone() == (1,)
