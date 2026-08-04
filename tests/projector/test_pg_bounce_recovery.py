# pyright: reportPrivateUsage=false
"""Fault injection: projection resumes after its fixed PG socket dies."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterable
from contextlib import AbstractContextManager
from types import TracebackType
from typing import cast

import pytest
from psycopg import Connection, Cursor, OperationalError
from psycopg.abc import Params, QueryNoTemplate
from psycopg.rows import TupleRow

from slack_fuse.control import ControlState
from slack_fuse.projector.apply import apply_event
from slack_fuse.projector.reconnecting_conn import ReconnectingConnection
from slack_fuse_server.wire.frames import EventFrame
from tests.projector.conftest import ClientConnFactory


class _BreakOnNthExecuteCursor:
    """Close a real psycopg transport immediately before execute N."""

    def __init__(
        self,
        conn: Connection[TupleRow],
        cursor: Cursor[TupleRow],
        state: list[int],
        fail_on: int,
    ) -> None:
        self._conn = conn
        self._cursor = cursor
        self._state = state
        self._fail_on = fail_on

    def execute(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        prepare: bool | None = None,
        binary: bool | None = None,
    ) -> _BreakOnNthExecuteCursor:
        self._state[0] += 1
        if self._state[0] == self._fail_on:
            self._conn.close()
        _ = self._cursor.execute(query, params, prepare=prepare, binary=binary)
        return self

    def executemany(
        self,
        query: QueryNoTemplate,
        params_seq: Iterable[Params],
        *,
        returning: bool = False,
    ) -> None:
        self._cursor.executemany(query, params_seq, returning=returning)

    def fetchone(self) -> TupleRow | None:
        return self._cursor.fetchone()

    def fetchmany(self, size: int = 0) -> list[TupleRow]:
        return self._cursor.fetchmany(size)

    def fetchall(self) -> list[TupleRow]:
        return self._cursor.fetchall()

    def close(self) -> None:
        self._cursor.close()


class _ConnectionProxy:
    """Small typed proxy around a real psycopg connection."""

    def __init__(self, conn: Connection[TupleRow]) -> None:
        self._conn = conn

    @property
    def closed(self) -> bool:
        return self._conn.closed

    @property
    def autocommit(self) -> bool:
        return self._conn.autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        self._conn.autocommit = value

    def cursor(self) -> Cursor[TupleRow]:
        return self._conn.cursor()

    def transaction(self) -> AbstractContextManager[object]:
        return cast("AbstractContextManager[object]", self._conn.transaction())

    def close(self) -> None:
        self._conn.close()


class _BreakOnNthExecuteConnection(_ConnectionProxy):
    def __init__(self, conn: Connection[TupleRow], *, fail_on: int) -> None:
        super().__init__(conn)
        self._state = [0]
        self._fail_on = fail_on

    def cursor(self) -> Cursor[TupleRow]:
        return cast(
            "Cursor[TupleRow]",
            _BreakOnNthExecuteCursor(self._conn, self._conn.cursor(), self._state, self._fail_on),
        )


class _BreakOnFetchCursor(_BreakOnNthExecuteCursor):
    def __init__(self, conn: Connection[TupleRow], cursor: Cursor[TupleRow]) -> None:
        super().__init__(conn, cursor, [0], 2**31)
        self._break_fetch_pending = True

    def fetchone(self) -> TupleRow | None:
        if self._break_fetch_pending:
            self._break_fetch_pending = False
            self._conn.close()
            raise OperationalError("fault injection: socket lost during fetchone")
        return self._cursor.fetchone()


class _BreakOnFetchConnection(_ConnectionProxy):
    def cursor(self) -> Cursor[TupleRow]:
        return cast("Cursor[TupleRow]", _BreakOnFetchCursor(self._conn, self._conn.cursor()))


class _CommitThenFailTransaction(AbstractContextManager[object]):
    def __init__(self, delegate: AbstractContextManager[object], error: OperationalError) -> None:
        self._delegate = delegate
        self._error = error

    def __enter__(self) -> object:
        return self._delegate.__enter__()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        result = self._delegate.__exit__(exc_type, exc_value, traceback)
        if exc_type is None:
            raise self._error
        return result


class _CommitThenFailConnection(_ConnectionProxy):
    def __init__(self, conn: Connection[TupleRow], error: OperationalError) -> None:
        super().__init__(conn)
        self._error = error

    def transaction(self) -> AbstractContextManager[object]:
        return _CommitThenFailTransaction(super().transaction(), self._error)


def _insert_channel(cur: Cursor[TupleRow], channel_id: str) -> None:
    cur.execute(
        "INSERT INTO channels "
        "(channel_id, name, is_im, is_mpim, is_member, is_archived, tier, tier_source, subscribed) "
        "VALUES (%s, %s, FALSE, FALSE, TRUE, FALSE, 'hot', 'auto', TRUE)",
        (channel_id, channel_id.lower()),
    )


def _channel_count(conn: Connection[TupleRow], channel_id: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM channels WHERE channel_id = %s", (channel_id,))
        row = cur.fetchone()
    assert row is not None
    return int(row[0])


def test_operational_error_mid_transaction_surfaces_original_error(
    client_conn_factory: ClientConnFactory,
) -> None:
    first_raw = client_conn_factory()
    recovered_raw = client_conn_factory()
    factory_results = [
        cast("Connection[TupleRow]", _BreakOnNthExecuteConnection(first_raw, fail_on=2)),
        recovered_raw,
    ]
    conn = ReconnectingConnection(
        "postgresql://fault-injection",
        connection_factory=lambda _dsn: factory_results.pop(0),
        autocommit=True,
    )

    with pytest.raises(OperationalError), conn.transaction(), conn.cursor() as cur:
        _insert_channel(cur, "CMIDTX")
        cur.execute("SELECT 1")

    assert conn._transactions == []
    assert _channel_count(recovered_raw, "CMIDTX") == 0

    with conn.transaction(), conn.cursor() as cur:
        _insert_channel(cur, "CMIDTX")

    assert _channel_count(recovered_raw, "CMIDTX") == 1
    assert factory_results == []


def test_fetch_error_mid_transaction_surfaces_original_error(
    client_conn_factory: ClientConnFactory,
) -> None:
    first_raw = client_conn_factory()
    recovered_raw = client_conn_factory()
    factory_results = [
        cast("Connection[TupleRow]", _BreakOnFetchConnection(first_raw)),
        recovered_raw,
    ]
    conn = ReconnectingConnection(
        "postgresql://fault-injection",
        connection_factory=lambda _dsn: factory_results.pop(0),
        autocommit=True,
    )

    with pytest.raises(OperationalError), conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "INSERT INTO channels "
            "(channel_id, name, is_im, is_mpim, is_member, is_archived, tier, tier_source, subscribed) "
            "VALUES ('CFETCHTX', 'fetchtx', FALSE, FALSE, TRUE, FALSE, 'hot', 'auto', TRUE) "
            "RETURNING channel_id"
        )
        _ = cur.fetchone()

    assert conn._transactions == []
    assert _channel_count(recovered_raw, "CFETCHTX") == 0

    with conn.transaction(), conn.cursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)
    assert factory_results == []


def test_commit_transport_error_propagates_as_operational_error(
    client_conn_factory: ClientConnFactory,
) -> None:
    first_raw = client_conn_factory()
    recovered_raw = client_conn_factory()
    commit_error = OperationalError("fault injection: COMMIT acknowledgement lost")
    factory_calls = 0

    def factory(_dsn: str) -> Connection[TupleRow]:
        nonlocal factory_calls
        factory_calls += 1
        if factory_calls == 1:
            return cast("Connection[TupleRow]", _CommitThenFailConnection(first_raw, commit_error))
        return recovered_raw

    conn = ReconnectingConnection(
        "postgresql://fault-injection",
        connection_factory=factory,
        autocommit=True,
    )

    with pytest.raises(OperationalError) as raised, conn.transaction(), conn.cursor() as cur:
        _insert_channel(cur, "CCOMMITUNKNOWN")

    assert raised.value is commit_error
    assert conn._transactions == []
    assert factory_calls == 1
    assert _channel_count(recovered_raw, "CCOMMITUNKNOWN") == 1

    with conn.transaction(), conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM channels WHERE channel_id = 'CCOMMITUNKNOWN'")
        assert cur.fetchone() == (1,)
    assert factory_calls == 2
    assert _channel_count(recovered_raw, "CCOMMITUNKNOWN") == 1


def test_reconnect_only_between_transactions(client_conn_factory: ClientConnFactory) -> None:
    first_raw = client_conn_factory()
    recovered_raw = client_conn_factory()
    factory_results = [first_raw, recovered_raw]
    conn = ReconnectingConnection(
        "postgresql://fault-injection",
        connection_factory=lambda _dsn: factory_results.pop(0),
        autocommit=True,
    )

    with conn.transaction(), conn.cursor() as cur:
        _insert_channel(cur, "CTXONE")
    first_raw.close()

    with conn.transaction(), conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM channels WHERE channel_id = 'CTXONE'")
        assert cur.fetchone() == (1,)
        _insert_channel(cur, "CTXTWO")

    assert _channel_count(recovered_raw, "CTXONE") == 1
    assert _channel_count(recovered_raw, "CTXTWO") == 1
    assert factory_results == []


def test_projector_recovers_after_connection_transport_break(
    client_conn_factory: ClientConnFactory,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The same durable client applies after a broken socket; no restart."""
    first_raw = client_conn_factory()
    recovered_raw = client_conn_factory()
    factory_calls = 0
    health_events: list[tuple[str, str]] = []
    control_state = ControlState()

    def factory(_dsn: str) -> Connection[TupleRow]:
        nonlocal factory_calls
        factory_calls += 1
        if factory_calls == 1:
            return cast("Connection[TupleRow]", _BreakOnNthExecuteConnection(first_raw, fail_on=2))
        if factory_calls == 2:
            # Model the short interval in which PG accepts no new sockets.
            raise OperationalError("fault injection: postgres still restarting")
        return recovered_raw

    def record_health(kind: str, reason: str) -> None:
        health_events.append((kind, reason))
        control_state.record_client_health("projector_state", kind, reason)

    conn = ReconnectingConnection(
        "postgresql://fault-injection",
        connection_factory=factory,
        autocommit=True,
        on_health_event=record_health,
        wedge_failure_count=1,
    )
    frame = EventFrame(
        stream="channel-list",
        offset=1,
        kind="channel_added",
        payload={"id": "CPGBOUNCE", "name": "pg-bounce-recovered", "is_member": True},
    )

    caplog.set_level(logging.INFO, logger="slack_fuse.projector.reconnecting_conn")
    deadline = time.monotonic() + 10.0
    attempts = 0
    while True:
        attempts += 1
        try:
            _ = apply_event(conn, frame)
        except OperationalError:
            if time.monotonic() >= deadline:  # pragma: no cover - fail_after guard
                raise
            time.sleep(0.01)
        else:
            break

    with recovered_raw.cursor() as cur:
        _ = cur.execute("SELECT name FROM channels WHERE channel_id = 'CPGBOUNCE'")
        assert cur.fetchone() == ("pg-bounce-recovered",)
        _ = cur.execute("SELECT applied_offset FROM cursors WHERE stream = 'channel-list'")
        assert cur.fetchone() == (1,)

    status = json.loads(control_state.render())
    assert attempts == 3
    assert factory_calls == 3
    assert [kind for kind, _reason in health_events] == ["client_wedged", "client_recovered"]
    assert status["last_client_recovered"]["kind"] == "client_recovered"
    assert "postgres connection reopened" in caplog.text
