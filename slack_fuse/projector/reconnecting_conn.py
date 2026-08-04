# pyright: reportPrivateUsage=false
"""Single-slot psycopg connection that survives a local Postgres restart.

``psycopg.Connection`` objects do not become usable again after their server
socket dies.  The split mount deliberately keeps a handful of connections for
its whole lifetime, so retaining one raw object turns a short Postgres bounce
into a permanently stale mount.  ``ReconnectingConnection`` keeps the same
single-connection shape while replacing that object after an
``OperationalError``. Cursor operations outside an explicit transaction are
retried once; failures inside a transaction are propagated so the wrapper can
never silently split one logical transaction across two sockets.

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
from typing import Final, Protocol, Self, cast

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


class TupleConnection(Protocol):
    """Minimal connection surface shared by raw and reconnecting connections."""

    @property
    def closed(self) -> bool: ...

    @property
    def autocommit(self) -> bool: ...

    @autocommit.setter
    def autocommit(self, value: bool) -> None: ...

    def cursor(self) -> Cursor[TupleRow]: ...

    def transaction(self) -> AbstractContextManager[object]: ...

    def close(self) -> None: ...


class ClosedConnectionError(RuntimeError):
    """An operation was attempted after an explicit :meth:`close`."""


def _connect(dsn: str) -> Connection[TupleRow]:
    conn: Connection[TupleRow] = psycopg.connect(dsn)
    return conn


class ReconnectingConnection:
    """A durable, single-slot ``psycopg.Connection[TupleRow]`` wrapper.

    The raw connection is opened lazily. Outside an explicit transaction, the
    first ``OperationalError`` from ``execute``/``executemany``/``fetch*``
    replaces the broken socket and retries that cursor operation once. Inside
    a transaction, the original error is propagated unchanged and the socket
    is abandoned after transaction unwind. In particular, a COMMIT transport
    error has an ambiguous server-side outcome and is never retried here.

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
        self._broken = False
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

    def cursor(self) -> Cursor[TupleRow]:
        """Return a cursor proxy bound to the wrapper's current generation."""
        return cast("Cursor[TupleRow]", _ReconnectingCursor(self))

    def execute(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        prepare: bool | None = None,
        binary: bool | None = None,
    ) -> Cursor[TupleRow]:
        """Mirror psycopg's connection-level ``execute`` convenience API."""
        return self.cursor().execute(query, params, prepare=prepare, binary=binary)

    def transaction(self) -> _ReconnectingTransaction:
        """Return a transaction context that abandons, rather than reopens, on failure."""
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
        if self._conn is not None and not self._conn.closed and not self._broken:
            return self._conn
        if self._transactions:
            self._broken = True
            msg = "postgres connection became unavailable during an active transaction"
            raise OperationalError(msg)
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
        self._broken = False
        self._generation += 1

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

    def _mark_broken(self, exc: OperationalError) -> None:
        """Prevent the failed raw socket from being reused or reopened mid-TX."""
        self._broken = True
        self._last_reconnect_reason = str(exc)

    def _abandon_broken_if_idle(self) -> None:
        """Close a failed socket only after its outermost transaction unwinds."""
        if self._broken and not self._transactions:
            self._close_raw()

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
        """Run one cursor operation, with one narrowly-scoped reconnect retry.

        Replay is permitted only when this operation discovers a broken socket
        while no explicit transaction is active. It is not a general safe-write
        retry contract: callers must account for ambiguous outcomes of writes
        outside transactions. An operation that fails inside a transaction is
        never reopened or retried, because doing so would lose prior statements.
        """
        with self._owner._lock:
            try:
                cursor = self._current_cursor(replay=replay_before_retry)
            except OperationalError as setup_exc:
                if self._owner._transactions:
                    self._owner._mark_broken(setup_exc)
                raise
            try:
                return operation(cursor)
            except OperationalError as first_exc:
                if self._owner._transactions:
                    self._owner._mark_broken(first_exc)
                    raise
                _ = self._owner._reopen(str(first_exc))
                try:
                    cursor = self._replace_cursor(replay=replay_before_retry)
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
    """Transaction context that never follows a connection reopen."""

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
            except OperationalError as exit_exc:
                self._owner._mark_broken(exit_exc)
                if exc_value is None:
                    raise
                # Preserve the original body failure. The rollback error is a
                # follow-on from the same dead socket, which is abandoned below.
                return False
        finally:
            if self in self._owner._transactions:
                self._owner._transactions.remove(self)
            self._owner._abandon_broken_if_idle()
            if self._entered:
                self._entered = False
                self._owner._lock.release()


__all__ = [
    "DEFAULT_WEDGE_FAILURE_COUNT",
    "DEFAULT_WEDGE_WINDOW_S",
    "ClosedConnectionError",
    "ReconnectingConnection",
    "TupleConnection",
]
