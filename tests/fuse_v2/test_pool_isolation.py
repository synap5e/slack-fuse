"""Regression tests for the FUSE-mount-wedge bug.

Before this fix, every FUSE callback ran serially under
``CapacityLimiter(1)`` against a single shared ``psycopg`` connection. Any
one slow callback (e.g. a SELECT stalled behind WAL fsync contention from
the projector) held the slot indefinitely and queued every subsequent
callback behind it — the kernel pages allocated for waiting reads stayed
locked, every caller D-stated on ``folio_wait_bit_common``, the mount
looked dead from the outside.

The fix: per-callback connection pool + trio-level timeout on the sync
body. These tests pin both behaviours.
"""

from __future__ import annotations

import errno
import time as _time
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import pyfuse3
import pytest
import trio

from slack_fuse.fuse_ops_v2 import SlackFuseOpsV2
from slack_fuse.fuse_v2_helpers import borrowed_fuse_conn
from slack_fuse.projector.pool import ConnectionPool

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow

    from tests.projector.conftest import ClientConnFactory


@pytest.fixture
def fuse_pool(client_conn_factory: ClientConnFactory) -> ConnectionPool:
    """A small FUSE-side pool fed by the test conn factory.

    Production sets ``statement_timeout`` on each conn; tests skip that
    because the trio-level fail_after is what we want to exercise here.
    """
    return ConnectionPool(client_conn_factory, max_size=4)


@pytest.fixture
def local_tz() -> ZoneInfo:
    return ZoneInfo("UTC")


def _make_ops(
    conn: Connection[TupleRow],
    tz: ZoneInfo,
    *,
    pool: ConnectionPool | None,
    timeout_s: float,
) -> SlackFuseOpsV2:
    return SlackFuseOpsV2(
        conn,
        tz,
        trio.CapacityLimiter(1),
        pool=pool,
        callback_timeout_s=timeout_s,
    )


@pytest.mark.trio
async def test_pool_mode_passes_borrowed_conn_via_contextvar(
    client_conn: Connection[TupleRow],
    fuse_pool: ConnectionPool,
    local_tz: ZoneInfo,
) -> None:
    """A sync body run via ``_run_sync`` sees the pool-borrowed conn through
    the ``borrowed_fuse_conn`` ContextVar — not the inode fallback conn."""
    ops = _make_ops(client_conn, local_tz, pool=fuse_pool, timeout_s=30.0)
    seen: list[Connection[TupleRow] | None] = []

    def _sync() -> int:
        seen.append(borrowed_fuse_conn.get())
        return 42

    result: int = await ops._run_sync(_sync)  # pyright: ignore[reportPrivateUsage]
    assert result == 42
    assert len(seen) == 1
    assert seen[0] is not None
    # Borrowed conn is a *different* object from the inode fallback conn
    # (the pool created its own); also the contextvar resets on the way out.
    assert seen[0] is not client_conn
    assert borrowed_fuse_conn.get() is None


@pytest.mark.trio
async def test_pool_mode_concurrent_callbacks_dont_serialize(
    client_conn: Connection[TupleRow],
    fuse_pool: ConnectionPool,
    local_tz: ZoneInfo,
) -> None:
    """Two callbacks running concurrently borrow *different* conns from the
    pool — they don't queue behind one limiter slot. This is the
    regression test for the wedge: one slow callback used to block the
    other on ``CapacityLimiter(1)``.
    """
    ops = _make_ops(client_conn, local_tz, pool=fuse_pool, timeout_s=30.0)
    barrier = trio.Event()
    seen: list[int] = []

    def _slow() -> int:
        seen.append(id(borrowed_fuse_conn.get()))
        return 1

    def _fast() -> int:
        seen.append(id(borrowed_fuse_conn.get()))
        return 2

    async def _run_slow() -> None:
        await ops._run_sync(_slow)  # pyright: ignore[reportPrivateUsage]
        barrier.set()

    async def _run_fast() -> None:
        await ops._run_sync(_fast)  # pyright: ignore[reportPrivateUsage]

    async with trio.open_nursery() as nursery:
        nursery.start_soon(_run_slow)
        nursery.start_soon(_run_fast)

    # Both ran (no deadlock); each got a non-zero conn id; conns differ
    # (proves the pool issued separate connections).
    assert len(seen) == 2
    assert all(c != 0 for c in seen)
    assert seen[0] != seen[1]
    assert barrier.is_set()


@pytest.mark.trio
async def test_pool_mode_timeout_raises_eio_and_discards_conn(
    client_conn: Connection[TupleRow],
    fuse_pool: ConnectionPool,
    local_tz: ZoneInfo,
) -> None:
    """A sync body that exceeds the per-callback timeout surfaces as
    ``FUSEError(EIO)`` to the kernel — the kernel page never stays locked
    waiting for a never-returning upcall — and the borrowed conn is
    discarded (not returned to the pool), since the abandoned worker thread
    might still be using it.
    """
    ops = _make_ops(client_conn, local_tz, pool=fuse_pool, timeout_s=0.05)

    def _hangs() -> int:
        _time.sleep(5.0)  # well past the 0.05s timeout
        return 0

    created_before = fuse_pool.connections_created
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        _ = await ops._run_sync(_hangs)  # pyright: ignore[reportPrivateUsage]

    assert exc_info.value.errno == errno.EIO
    # The borrowed conn was discarded; the next acquire creates a fresh one
    # rather than reusing the abandoned one.
    next_conn = await fuse_pool.acquire()
    try:
        assert fuse_pool.connections_created == created_before + 2
    finally:
        await fuse_pool.release(next_conn)


@pytest.mark.trio
async def test_conn_only_mode_unchanged_behaviour(
    client_conn: Connection[TupleRow],
    local_tz: ZoneInfo,
) -> None:
    """With ``pool=None`` (the test default for direct ``SlackFuseOpsV2``
    instantiation, also the v1 legacy shape), the limiter still serializes
    and the contextvar stays unset — sync code falls back to the inode
    connection. No behavioural change for the existing test corpus."""
    ops = _make_ops(client_conn, local_tz, pool=None, timeout_s=30.0)

    def _sync() -> Connection[TupleRow] | None:
        return borrowed_fuse_conn.get()

    result = await ops._run_sync(_sync)  # pyright: ignore[reportPrivateUsage]
    # Conn-only mode never sets the contextvar.
    assert result is None
