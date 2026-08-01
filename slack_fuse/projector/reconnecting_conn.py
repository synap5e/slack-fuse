# pyright: reportPrivateUsage=false
"""Single-slot psycopg connection that survives a local Postgres restart.

``psycopg.Connection`` objects do not become usable again after their server
socket dies.  The split mount deliberately keeps a handful of connections for
its whole lifetime, so retaining one raw object turns a short Postgres bounce
into a permanently stale mount.  ``ReconnectingConnection`` keeps the same
single-connection shape while replacing that object after an
``OperationalError`` and retrying the interrupted cursor operation once.

The wrapper is synchronous because its callers already run in worker threads.
An ``RLock`` serializes a cursor/transaction context, matching psycopg's shared
session semantics and preventing two worker threads from racing a reopen.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from collections.abc import Callable, Iterable
from contextlib import AbstractContextManager, suppress
from types import TracebackType
from typing import Final, Self, cast

import psycopg
from psycopg import Connection, Cursor, OperationalError
from psycopg.abc import Params, QueryNoTemplate
from psycopg.rows import TupleRow

log = logging.getLogger(__name__)

DEFAULT_WEDGE_FAILURE_COUNT: Final = 5
DEFAULT_WEDGE_WINDOW_S: Final = 180.0

ConnectionFactory = Callable[[str], Connection[TupleRow]]
ReconnectCallback = Callable[[str], None]
HealthEventCallback = Callable[[str, str], None]
NowFn = Callable[[], float]


class ClosedConnectionError(RuntimeError):
    """An operation was attempted after an explicit :meth:`close`."""


def _connect(dsn: str) -> Connection[TupleRow]:
    conn: Connection[TupleRow] = psycopg.connect(dsn)
    return conn


class ReconnectingConnection:
    """A durable, single-slot ``psycopg.Connection[TupleRow]`` wrapper.

    The raw connection is opened lazily.  On the first ``OperationalError``
    from ``execute``/``executemany``/``fetch*``, the broken socket is closed,
    a fresh connection is opened from the same DSN, and that operation is
    retried once.  A second ``OperationalError`` is propagated unchanged.

    ``on_reconnect`` fires after every successful replacement (never for the
    initial connection).  ``on_health_event`` receives ``client_wedged`` once
    five consecutive reconnect attempts fail within three minutes, then
    ``client_recovered`` once the next connection succeeds.
    """

    def __init__(  # noqa: PLR0913 - constructor exposes testable recovery policy knobs.
        self,
        dsn: str,
        *,
        connection_factory: ConnectionFactory = _connect,
        autocommit: bool | None = None,
        on_reconnect: ReconnectCallback | None = None,
        on_health_event: HealthEventCallback | None = None,
        wedge_failure_count: int = DEFAULT_WEDGE_FAILURE_COUNT,
        wedge_window_s: float = DEFAULT_WEDGE_WINDOW_S,
        now_fn: NowFn = time.monotonic,
    ) -> None:
        if wedge_failure_count < 1:
            msg = "wedge_failure_count must be positive"
            raise ValueError(msg)
        if wedge_window_s <= 0:
            msg = "wedge_window_s must be positive"
            raise ValueError(msg)
        self._dsn = dsn
        self._connection_factory = connection_factory
        self._configured_autocommit = autocommit
        self._on_reconnect = on_reconnect
        self._on_health_event = on_health_event
        self._wedge_failure_count = wedge_failure_count
        self._wedge_window_s = wedge_window_s
        self._now_fn = now_fn

        self._lock = threading.RLock()
        self._conn: Connection[TupleRow] | None = None
        self._generation = 0
        self._connect_attempted = False
        self._explicitly_closed = False
        self._last_reconnect_reason = "connection unavailable"
        self._failure_times: deque[float] = deque()
        self._wedged = False
        self._transactions: list[_ReconnectingTransaction] = []

    @property
    def closed(self) -> bool:
        """Whether the durable wrapper was explicitly closed.

        A dead/missing raw socket does not make the wrapper closed: the next
        operation is allowed to reopen it.
        """
        with self._lock:
            return self._explicitly_closed

    @property
    def autocommit(self) -> bool:
        with self._lock:
            return self._ensure_connection().autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        with self._lock:
            self._configured_autocommit = value
            self._ensure_connection().autocommit = value

    def cursor(self) -> _ReconnectingCursor:
        """Return a cursor proxy bound to the wrapper's current generation."""
        return _ReconnectingCursor(self)

    def execute(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        prepare: bool | None = None,
        binary: bool | None = None,
    ) -> _ReconnectingCursor:
        """Mirror psycopg's connection-level ``execute`` convenience API."""
        return self.cursor().execute(query, params, prepare=prepare, binary=binary)

    def transaction(self) -> _ReconnectingTransaction:
        """Return a transaction context that follows a reopened connection."""
        return _ReconnectingTransaction(self)

    def commit(self) -> None:
        with self._lock:
            self._ensure_connection().commit()

    def rollback(self) -> None:
        with self._lock:
            self._ensure_connection().rollback()

    def close(self) -> None:
        """Explicitly close the wrapper; later operations never reopen it."""
        with self._lock:
            if self._explicitly_closed:
                return
            self._explicitly_closed = True
            self._close_raw()

    def __enter__(self) -> Self:
        with self._lock:
            _ = self._ensure_connection()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback
        self.close()

    def _ensure_open(self) -> None:
        if self._explicitly_closed:
            msg = "reconnecting connection is explicitly closed"
            raise ClosedConnectionError(msg)

    def _ensure_connection(self) -> Connection[TupleRow]:
        self._ensure_open()
        if self._conn is not None and not self._conn.closed:
            return self._conn
        if self._conn is not None:
            self._close_raw()

        is_reconnect = self._connect_attempted
        self._connect_attempted = True
        try:
            conn = self._connection_factory(self._dsn)
            if self._configured_autocommit is not None:
                conn.autocommit = self._configured_autocommit
        except OperationalError as exc:
            self._conn = None
            self._last_reconnect_reason = str(exc)
            self._record_reconnect_failure(str(exc))
            raise

        self._conn = conn
        self._generation += 1
        try:
            for transaction in self._transactions:
                transaction._restart(conn)
        except OperationalError as exc:
            self._close_raw()
            self._last_reconnect_reason = str(exc)
            self._record_reconnect_failure(str(exc))
            raise

        if is_reconnect:
            reason = self._last_reconnect_reason
            self._record_reconnect_success(reason)
            self._fire_callback(self._on_reconnect, reason)
            log.info("postgres connection reopened: %s", reason)
        return conn

    def _reopen(self, reason: str) -> Connection[TupleRow]:
        self._last_reconnect_reason = reason
        self._close_raw()
        return self._ensure_connection()

    def _abandon_failed_retry(self, exc: OperationalError) -> None:
        """Leave no bad socket behind after the one permitted retry fails."""
        self._last_reconnect_reason = str(exc)
        self._close_raw()
        self._record_reconnect_failure(str(exc))

    def _close_raw(self) -> None:
        conn = self._conn
        self._conn = None
        if conn is not None:
            with suppress(Exception):
                conn.close()

    def _record_reconnect_failure(self, reason: str) -> None:
        now = self._now_fn()
        oldest = now - self._wedge_window_s
        self._failure_times.append(now)
        while self._failure_times and self._failure_times[0] < oldest:
            self._failure_times.popleft()
        if len(self._failure_times) >= self._wedge_failure_count and not self._wedged:
            self._wedged = True
            self._fire_health_event("client_wedged", reason)

    def _record_reconnect_success(self, reason: str) -> None:
        was_wedged = self._wedged
        self._failure_times.clear()
        self._wedged = False
        if was_wedged:
            self._fire_health_event("client_recovered", reason)

    def _fire_health_event(self, kind: str, reason: str) -> None:
        callback = self._on_health_event
        if callback is None:
            return
        try:
            callback(kind, reason)
        except Exception:
            log.exception("postgres reconnect health callback failed: kind=%s", kind)

    @staticmethod
    def _fire_callback(callback: ReconnectCallback | None, reason: str) -> None:
        if callback is None:
            return
        try:
            callback(reason)
        except Exception:
            log.exception("postgres reconnect callback failed")


class _ReconnectingCursor:
    """Cursor proxy that recreates and replays against a new connection."""

    def __init__(self, owner: ReconnectingConnection) -> None:
        self._owner = owner
        self._cursor: Cursor[TupleRow] | None = None
        self._generation = -1
        self._entered = False
        self._last_execute: tuple[QueryNoTemplate, Params | None, bool | None, bool | None] | None = None

    def __enter__(self) -> Self:
        self._owner._lock.acquire()
        try:
            self._owner._ensure_open()
            self._replace_cursor(replay=False)
        except BaseException:
            self._owner._lock.release()
            raise
        self._entered = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback
        try:
            self.close()
        finally:
            if self._entered:
                self._entered = False
                self._owner._lock.release()

    def execute(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        prepare: bool | None = None,
        binary: bool | None = None,
    ) -> Self:
        self._last_execute = (query, params, prepare, binary)

        def operation(cursor: Cursor[TupleRow]) -> object:
            return cursor.execute(query, params, prepare=prepare, binary=binary)

        _ = self._call_with_reconnect(operation, replay_before_retry=False)
        return self

    def executemany(
        self,
        query: QueryNoTemplate,
        params_seq: Iterable[Params],
        *,
        returning: bool = False,
    ) -> None:
        materialized_params = tuple(params_seq)

        def operation(cursor: Cursor[TupleRow]) -> object:
            return cursor.executemany(query, materialized_params, returning=returning)

        _ = self._call_with_reconnect(operation, replay_before_retry=False)

    def fetchone(self) -> TupleRow | None:
        return cast("TupleRow | None", self._call_with_reconnect(lambda cursor: cursor.fetchone()))

    def fetchmany(self, size: int = 0) -> list[TupleRow]:
        return cast("list[TupleRow]", self._call_with_reconnect(lambda cursor: cursor.fetchmany(size)))

    def fetchall(self) -> list[TupleRow]:
        return cast("list[TupleRow]", self._call_with_reconnect(lambda cursor: cursor.fetchall()))

    def close(self) -> None:
        cursor = self._cursor
        self._cursor = None
        if cursor is not None:
            with suppress(Exception):
                cursor.close()

    def _call_with_reconnect(
        self,
        operation: Callable[[Cursor[TupleRow]], object],
        *,
        replay_before_retry: bool = True,
    ) -> object:
        with self._owner._lock:
            cursor = self._current_cursor(replay=replay_before_retry)
            try:
                return operation(cursor)
            except OperationalError as first_exc:
                _ = self._owner._reopen(str(first_exc))
                cursor = self._replace_cursor(replay=replay_before_retry)
                try:
                    return operation(cursor)
                except OperationalError as retry_exc:
                    self._owner._abandon_failed_retry(retry_exc)
                    raise

    def _current_cursor(self, *, replay: bool) -> Cursor[TupleRow]:
        conn = self._owner._ensure_connection()
        if self._cursor is None or self._generation != self._owner._generation:
            return self._replace_cursor(conn=conn, replay=replay)
        return self._cursor

    def _replace_cursor(
        self,
        *,
        conn: Connection[TupleRow] | None = None,
        replay: bool,
    ) -> Cursor[TupleRow]:
        self.close()
        current = conn if conn is not None else self._owner._ensure_connection()
        cursor = current.cursor()
        self._cursor = cursor
        self._generation = self._owner._generation
        if replay and self._last_execute is not None:
            query, params, prepare, binary = self._last_execute
            _ = cursor.execute(query, params, prepare=prepare, binary=binary)
        return cursor


class _ReconnectingTransaction(AbstractContextManager[None]):
    """Transaction context whose commit target follows a connection reopen."""

    def __init__(self, owner: ReconnectingConnection) -> None:
        self._owner = owner
        self._transaction: AbstractContextManager[None] | None = None
        self._entered = False

    def __enter__(self) -> None:
        self._owner._lock.acquire()
        try:
            conn = self._owner._ensure_connection()
            transaction = cast("AbstractContextManager[None]", conn.transaction())
            _ = transaction.__enter__()
            self._transaction = transaction
            self._owner._transactions.append(self)
        except BaseException:
            self._owner._lock.release()
            raise
        self._entered = True
        return None

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        try:
            transaction = self._transaction
            if transaction is None:
                return None
            try:
                return transaction.__exit__(exc_type, exc_value, traceback)
            except Exception:
                if exc_value is None:
                    raise
                return False
        finally:
            if self in self._owner._transactions:
                self._owner._transactions.remove(self)
            if self._entered:
                self._entered = False
                self._owner._lock.release()

    def _restart(self, conn: Connection[TupleRow]) -> None:
        transaction = cast("AbstractContextManager[None]", conn.transaction())
        _ = transaction.__enter__()
        self._transaction = transaction


__all__ = [
    "DEFAULT_WEDGE_FAILURE_COUNT",
    "DEFAULT_WEDGE_WINDOW_S",
    "ClosedConnectionError",
    "ReconnectingConnection",
]
