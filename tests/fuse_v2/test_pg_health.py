"""Tests for the PG-down tolerance + ``/NO_POSTGRES`` virtual file.

The contract every FUSE callback honours: return valid data or raise
``FUSEError`` (intentional FS-level error code). Anything else —
``psycopg.OperationalError`` from a vanished PG socket, a
``KeyError`` from a render edge case, a ``trio.TooSlowError`` from a
slow operation — gets converted to ``FUSEError(EIO)`` and the process
keeps running. This file pins that contract for the new code paths
introduced alongside :mod:`slack_fuse.pg_health`.
"""

from __future__ import annotations

import errno
import time as _time
from typing import TYPE_CHECKING
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import psycopg
import pyfuse3
import pytest
import trio

from slack_fuse.fuse_ops_v2 import SlackFuseOpsV2
from slack_fuse.pg_health import NO_POSTGRES_INODE, NO_POSTGRES_NAME, PgHealth
from slack_fuse.projector.pool import ConnectionPool

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow

    from tests.projector.conftest import ClientConnFactory


@pytest.fixture
def local_tz() -> ZoneInfo:
    return ZoneInfo("UTC")


def _ops(
    conn: Connection[TupleRow],
    tz: ZoneInfo,
    *,
    pg_health: PgHealth | None = None,
    pool: ConnectionPool | None = None,
    timeout_s: float = 30.0,
) -> SlackFuseOpsV2:
    return SlackFuseOpsV2(
        conn,
        tz,
        trio.CapacityLimiter(1),
        pool=pool,
        pg_health=pg_health,
        callback_timeout_s=timeout_s,
    )


# ---------------------------------------------------------------------------
# PgHealth class — pure logic
# ---------------------------------------------------------------------------


def test_pg_health_starts_up() -> None:
    pg = PgHealth(MagicMock())
    assert pg.is_down() is False


def test_pg_health_mark_down_then_up() -> None:
    pg = PgHealth(MagicMock())
    pg.mark_down(reason="test")
    assert pg.is_down() is True
    pg.mark_up()
    assert pg.is_down() is False


def test_pg_health_explanation_bytes_describe_game_mode() -> None:
    pg = PgHealth(MagicMock())
    body = pg.explanation_bytes
    # The text is the user-facing recovery guide; it must name the
    # service + socket so an operator can act on it directly.
    assert b"claude-hooks-postgres" in body
    assert b"/run/user/1000/local-postgres" in body
    assert b"game-mode" in body


# ---------------------------------------------------------------------------
# _run_sync error handling — every error path → FUSEError(EIO)
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_run_sync_catches_unexpected_exception(
    client_conn: Connection[TupleRow],
    local_tz: ZoneInfo,
) -> None:
    """A plain Python exception in the sync body becomes ``FUSEError(EIO)``
    rather than crashing the process. Was the gap before this fix — a single
    bad sync_fn would kill the FUSE daemon."""
    ops = _ops(client_conn, local_tz)

    def _bad_sync() -> int:
        msg = "render regression"
        raise KeyError(msg)

    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        _ = await ops._run_sync(_bad_sync)  # pyright: ignore[reportPrivateUsage]
    assert exc_info.value.errno == errno.EIO


@pytest.mark.trio
async def test_run_sync_lets_fuseerror_through(
    client_conn: Connection[TupleRow],
    local_tz: ZoneInfo,
) -> None:
    """A FUSEError raised intentionally by sync_fn (ENOENT etc) propagates
    untouched — that's the FS-level signal pyfuse3 expects."""
    ops = _ops(client_conn, local_tz)

    def _enoent() -> int:
        raise pyfuse3.FUSEError(errno.ENOENT)

    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        _ = await ops._run_sync(_enoent)  # pyright: ignore[reportPrivateUsage]
    assert exc_info.value.errno == errno.ENOENT


@pytest.mark.trio
async def test_run_sync_marks_pg_down_on_operational_error(
    client_conn: Connection[TupleRow],
    local_tz: ZoneInfo,
) -> None:
    """When sync_fn raises ``psycopg.OperationalError``, the wrapper marks
    PG down (so subsequent callbacks fast-fail) and surfaces EIO to the
    current caller."""
    pg = PgHealth(MagicMock())
    ops = _ops(client_conn, local_tz, pg_health=pg)

    def _pg_dead() -> int:
        msg = "connection is bad: ..."
        raise psycopg.OperationalError(msg)

    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        _ = await ops._run_sync(_pg_dead)  # pyright: ignore[reportPrivateUsage]
    assert exc_info.value.errno == errno.EIO
    assert pg.is_down() is True


@pytest.mark.trio
async def test_run_sync_fast_fails_when_pg_known_down(
    client_conn: Connection[TupleRow],
    local_tz: ZoneInfo,
) -> None:
    """Once PG is known down, subsequent callbacks must not even try to
    acquire a conn or run sync_fn — they raise EIO immediately."""
    pg = PgHealth(MagicMock())
    pg.mark_down(reason="prior callback")
    ops = _ops(client_conn, local_tz, pg_health=pg)

    ran = [False]

    def _should_not_run() -> int:
        ran[0] = True
        return 0

    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        _ = await ops._run_sync(_should_not_run)  # pyright: ignore[reportPrivateUsage]
    assert exc_info.value.errno == errno.EIO
    assert ran[0] is False, "sync_fn must not run when pg_health says down"


@pytest.mark.trio
async def test_run_sync_pool_mode_times_out_under_one_second(
    client_conn: Connection[TupleRow],
    client_conn_factory: ClientConnFactory,
    local_tz: ZoneInfo,
) -> None:
    """The 1s default callback timeout means a slow sync_fn surfaces EIO
    quickly rather than queueing every subsequent FUSE op behind it."""
    pool = ConnectionPool(client_conn_factory, max_size=2)
    ops = _ops(client_conn, local_tz, pool=pool, timeout_s=0.05)

    def _hangs() -> int:
        _time.sleep(2.0)
        return 0

    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        _ = await ops._run_sync(_hangs)  # pyright: ignore[reportPrivateUsage]
    assert exc_info.value.errno == errno.EIO


# ---------------------------------------------------------------------------
# /NO_POSTGRES virtual file — surfaces when down, hides when up
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_no_postgres_lookup_returns_attrs_when_down(
    client_conn: Connection[TupleRow],
    local_tz: ZoneInfo,
) -> None:
    pg = PgHealth(MagicMock())
    pg.mark_down(reason="test")
    ops = _ops(client_conn, local_tz, pg_health=pg)

    # lookup of /NO_POSTGRES at the mount root
    ctx = MagicMock(spec=pyfuse3.RequestContext)
    attr = await ops.lookup(1, NO_POSTGRES_NAME.encode(), ctx)
    assert attr.st_ino == NO_POSTGRES_INODE
    assert attr.st_size == len(pg.explanation_bytes)


@pytest.mark.trio
async def test_no_postgres_lookup_returns_enoent_when_up(
    client_conn: Connection[TupleRow],
    local_tz: ZoneInfo,
) -> None:
    pg = PgHealth(MagicMock())  # default is up
    ops = _ops(client_conn, local_tz, pg_health=pg)

    ctx = MagicMock(spec=pyfuse3.RequestContext)
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        _ = await ops.lookup(1, NO_POSTGRES_NAME.encode(), ctx)
    assert exc_info.value.errno == errno.ENOENT


@pytest.mark.trio
async def test_no_postgres_read_returns_explanation_bytes(
    client_conn: Connection[TupleRow],
    local_tz: ZoneInfo,
) -> None:
    pg = PgHealth(MagicMock())
    pg.mark_down(reason="test")
    ops = _ops(client_conn, local_tz, pg_health=pg)

    # Full read.
    data = await ops.read(NO_POSTGRES_INODE, 0, len(pg.explanation_bytes))
    assert data == pg.explanation_bytes

    # Partial / offset read still works (pyfuse3 may issue these).
    head = await ops.read(NO_POSTGRES_INODE, 0, 16)
    assert head == pg.explanation_bytes[:16]
    middle = await ops.read(NO_POSTGRES_INODE, 16, 32)
    assert middle == pg.explanation_bytes[16:48]


@pytest.mark.trio
async def test_no_postgres_getattr_when_down(
    client_conn: Connection[TupleRow],
    local_tz: ZoneInfo,
) -> None:
    pg = PgHealth(MagicMock())
    pg.mark_down(reason="test")
    ops = _ops(client_conn, local_tz, pg_health=pg)

    ctx = MagicMock(spec=pyfuse3.RequestContext)
    attr = await ops.getattr(NO_POSTGRES_INODE, ctx)
    assert attr.st_ino == NO_POSTGRES_INODE
    assert attr.st_size == len(pg.explanation_bytes)


@pytest.mark.trio
async def test_no_postgres_getattr_enoent_when_up(
    client_conn: Connection[TupleRow],
    local_tz: ZoneInfo,
) -> None:
    pg = PgHealth(MagicMock())
    ops = _ops(client_conn, local_tz, pg_health=pg)

    ctx = MagicMock(spec=pyfuse3.RequestContext)
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        _ = await ops.getattr(NO_POSTGRES_INODE, ctx)
    assert exc_info.value.errno == errno.ENOENT


# ---------------------------------------------------------------------------
# Callback guard catches non-FUSEError exceptions before they escape
# ---------------------------------------------------------------------------


def test_callback_guard_converts_unexpected_to_eio(
    client_conn: Connection[TupleRow],
    local_tz: ZoneInfo,
) -> None:
    """Any non-FUSEError exception in a callback body is logged and turned
    into EIO at the boundary — never reaches pyfuse3 / never crashes the
    process. Regression test for the 2026-06-21 production crash where a
    raw OperationalError out of a callback killed the daemon."""
    ops = _ops(client_conn, local_tz)

    # Pry the guard open with a synthetic op name.
    with (
        pytest.raises(pyfuse3.FUSEError) as exc_info,
        ops._callback_guard("synthetic-op"),  # pyright: ignore[reportPrivateUsage]
    ):
        msg = "anything could go wrong here"
        raise RuntimeError(msg)
    assert exc_info.value.errno == errno.EIO


def test_callback_guard_marks_pg_down_on_operational_error(
    client_conn: Connection[TupleRow],
    local_tz: ZoneInfo,
) -> None:
    pg = PgHealth(MagicMock())
    ops = _ops(client_conn, local_tz, pg_health=pg)

    with (
        pytest.raises(pyfuse3.FUSEError) as exc_info,
        ops._callback_guard("synthetic-op"),  # pyright: ignore[reportPrivateUsage]
    ):
        msg = "connection is bad: ..."
        raise psycopg.OperationalError(msg)
    assert exc_info.value.errno == errno.EIO
    assert pg.is_down() is True
