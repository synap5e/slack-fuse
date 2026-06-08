"""Bounded connection pool shared by the per-stream appliers (review P0-A).

One psycopg connection per subscribed stream does not scale. A 320-channel
workspace would open 320+ connections (plus the FUSE/health/state/sink ones)
and blow past a stock local Postgres ``max_connections = 100`` *before* the
projector finishes subscribing to every stream — and the reconnect loop just
repeats the connection storm. The RFC asks for one applier *task* per stream,
not one DB *backend* per stream.

The fix: appliers share a small bounded pool of autocommit connections.

Why this preserves correctness:

- Each ``apply_event`` is its own transaction (``with conn.transaction()`` on an
  autocommit connection). A connection therefore only needs to be held for the
  duration of *one* event apply, not for the applier's whole lifetime.
- The per-stream applier task still processes its queue strictly one event at a
  time (it ``await``\\s each apply before pulling the next message), so
  in-stream ordering is unaffected by *which* physical connection a given event
  borrows.
- Cross-stream concurrency is bounded by the pool size rather than the stream
  count: at most ``max_size`` applies run at once, and at most ``max_size``
  physical connections ever exist.

The pool is trio-native: ``acquire``/``release`` happen on the event-loop
thread (around the ``trio.to_thread.run_sync`` that runs the blocking SQL), and
the blocking ``connect()`` itself runs on a worker thread so it never stalls the
loop.
"""

from __future__ import annotations

import logging
from typing import Final

import trio
from psycopg import Connection
from psycopg.rows import TupleRow

from slack_fuse.projector.apply import require_autocommit
from slack_fuse.projector.per_stream import ConnectionFactory

log = logging.getLogger(__name__)


#: Default pool size. Comfortably below a stock local Postgres
#: ``max_connections = 100`` even after the split mount's other fixed
#: connections (FUSE read, health poll, projector state, invalidation sink) and
#: a transient snapshot connection are accounted for.
DEFAULT_POOL_SIZE: Final = 8


class ConnectionPoolClosed(RuntimeError):
    """Raised when acquiring from a pool that has already been closed."""


class ConnectionPool:
    """A bounded pool of autocommit psycopg connections.

    Connections are created lazily (up to ``max_size``) and reused. Every
    connection handed out is validated with ``require_autocommit`` so the
    projector's transaction contract can't silently break.
    """

    def __init__(self, factory: ConnectionFactory, *, max_size: int = DEFAULT_POOL_SIZE) -> None:
        if max_size < 1:
            msg = f"pool max_size must be >= 1, got {max_size}"
            raise ValueError(msg)
        self._factory = factory
        self._max_size = max_size
        self._idle: list[Connection[TupleRow]] = []
        self._slots = trio.Semaphore(max_size)
        self._lock = trio.Lock()
        self._closed = False
        self._created = 0

    @property
    def max_size(self) -> int:
        return self._max_size

    @property
    def connections_created(self) -> int:
        """Total physical connections opened so far (test/metrics introspection)."""
        return self._created

    async def acquire(self) -> Connection[TupleRow]:
        """Borrow a connection. Blocks until a slot is free; creates lazily."""
        if self._closed:
            raise ConnectionPoolClosed
        await self._slots.acquire()
        try:
            async with self._lock:
                if self._idle:
                    return self._idle.pop()
            conn = await trio.to_thread.run_sync(self._make)
            async with self._lock:
                self._created += 1
            return conn
        except BaseException:
            # Hand the slot back on any failure (cancellation, connect error) so
            # a transient failure can't permanently shrink the pool.
            self._slots.release()
            raise

    def _make(self) -> Connection[TupleRow]:
        conn = self._factory()
        require_autocommit(conn)
        return conn

    async def release(self, conn: Connection[TupleRow], *, discard: bool = False) -> None:
        """Return a connection to the pool, or close it if ``discard``/closed.

        After an apply *fails* the connection's transaction has rolled back and
        it is usually reusable, but the caller passes ``discard=True`` to be
        safe — a poisoned stream is about to tear the whole client down anyway.
        """
        if discard or self._closed:
            await trio.to_thread.run_sync(conn.close)
        else:
            async with self._lock:
                self._idle.append(conn)
        self._slots.release()

    async def aclose(self) -> None:
        """Close every idle connection. Borrowed connections close on release."""
        self._closed = True
        async with self._lock:
            idle = list(self._idle)
            self._idle.clear()
        for conn in idle:
            await trio.to_thread.run_sync(conn.close)
