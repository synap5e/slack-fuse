"""Fault injection: projection resumes after its fixed PG socket dies."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterable
from contextlib import AbstractContextManager
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


class _BreakOnExecuteCursor:
    """Close a real psycopg transport immediately before its first query."""

    def __init__(self, conn: Connection[TupleRow], cursor: Cursor[TupleRow]) -> None:
        self._conn = conn
        self._cursor = cursor
        self._break_pending = True

    def execute(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        prepare: bool | None = None,
        binary: bool | None = None,
    ) -> _BreakOnExecuteCursor:
        if self._break_pending:
            self._break_pending = False
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


class _BreakOnExecuteConnection:
    """Connection proxy used only for the first projector callback."""

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

    def cursor(self) -> _BreakOnExecuteCursor:
        return _BreakOnExecuteCursor(self._conn, self._conn.cursor())

    def transaction(self) -> AbstractContextManager[object]:
        return cast("AbstractContextManager[object]", self._conn.transaction())

    def close(self) -> None:
        self._conn.close()


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
            return cast("Connection[TupleRow]", _BreakOnExecuteConnection(first_raw))
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
    assert attempts == 2
    assert factory_calls == 3
    assert [kind for kind, _reason in health_events] == ["client_wedged", "client_recovered"]
    assert status["last_client_recovered"]["kind"] == "client_recovered"
    assert "postgres connection reopened" in caplog.text
