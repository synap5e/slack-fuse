"""The 1-second-or-EIO contract: regression guard for the 2026-06-24 wedge.

The wedge was: ``pyfuse3.notify_store`` blocked forever on a kernel page lock
the in-flight read was holding (folio_wait_bit_common). The daemon never
returned from the read — it just hung until the watchdog killed it.

The fix (see ``_callback_guard``): every FUSE callback body runs under a
single outer ``trio.fail_after(callback_timeout_s)``. Any stage that stalls,
no matter where it lives in the read path, is surfaced as EIO within budget.
``trio.to_thread.run_sync(..., abandon_on_cancel=True)`` lets the trio side
return EIO even while a worker thread stays stuck — which is exactly what
the kernel deadlock looks like in production.

These tests would have caught the 2026-06-24 wedge before it shipped: if
anyone re-introduces a synchronous ``notify_store`` / ``invalidate_inode``
call from inside a callback, the read budget kicks in and EIO surfaces in
time — the test asserts the surfacing, not the wedge.

See also ``test_kernel_cache_invariants.py`` for the priming-decision
contract; this file is purely about the timing guarantee.
"""

from __future__ import annotations

import errno
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import pyfuse3
import pytest
import trio

from slack_fuse.fuse_ops_v2 import SlackFuseOpsV2
from tests.fuse_v2.conftest import (
    NOOP_INVALIDATE_INODE,
    NOOP_NOTIFY_STORE,
    mark_stream_caught_up,
    seed_channel,
    seed_chunk,
    set_connection_state,
)

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow


# Tight budget keeps the tests fast. Production runs with 1.0s; the
# contract is "EIO within budget", which holds at any budget.
_BUDGET_S = 0.5

# Cap so a regression doesn't hang the suite. Whichever fires first
# (``trio.fail_after`` inside the test body or pytest-trio's test timeout)
# the assertion below catches a hung callback as a clear failure mode
# rather than a silent CI lockup.
_TEST_BUDGET_S = 3.0


# ============================================================================
# Stubs: pyfuse3 substitutes whose cache calls block forever (simulates the
# kernel-side deadlock that caused the 2026-06-24 wedge).
# ============================================================================


@dataclass(slots=True)
class _BlockingPyfuse3:
    """A pyfuse3 stub whose ``notify_store`` / ``invalidate_inode`` block
    forever.

    Each call is recorded BEFORE the block so a regression can be diagnosed:
    if a test fails with TooSlowError and ``notify_calls`` is non-empty,
    someone re-added a synchronous notify call inside a callback (the
    2026-06-24 mistake).
    """

    notify_calls: list[tuple[int, int, int]] = field(default_factory=list[tuple[int, int, int]])
    invalidate_calls: list[int] = field(default_factory=list[int])
    _lock: threading.Lock = field(default_factory=threading.Lock)
    # A never-set Event — wait() blocks the worker thread until the test
    # tears down. Mirrors what folio_wait_bit_common did to the daemon.
    _never: threading.Event = field(default_factory=threading.Event)

    def notify_store(self, inode: int, offset: int, data: bytes) -> None:
        with self._lock:
            self.notify_calls.append((inode, offset, len(data)))
        self._never.wait()

    def invalidate_inode(self, inode: int) -> None:
        with self._lock:
            self.invalidate_calls.append(inode)
        self._never.wait()


def _seed_clean_world(conn: Connection[TupleRow]) -> None:
    """Minimal data: one hot channel, caught up, one chunk."""
    seed_channel(conn, "C1", "general", tier="hot")
    seed_chunk(
        conn,
        "C1",
        Decimal(str(datetime(2026, 6, 8, 14, 30, tzinfo=UTC).timestamp())),
        "## 14:30 alice\n\nHello\n",
    )
    set_connection_state(conn, last_slurper_health="healthy", last_frame_at_offset_s=1.0)
    mark_stream_caught_up(conn, "channel:C1", at_offset=10)


def _make_ops(
    conn: Connection[TupleRow],
    *,
    notify_store: object,
    invalidate_inode: object,
) -> SlackFuseOpsV2:
    return SlackFuseOpsV2(
        conn=conn,
        local_tz=ZoneInfo("UTC"),
        limiter=trio.CapacityLimiter(1),
        notify_store=notify_store,  # pyright: ignore[reportArgumentType]
        invalidate_inode=invalidate_inode,  # pyright: ignore[reportArgumentType]
        callback_timeout_s=_BUDGET_S,
    )


# ============================================================================
# Budget contract: a worker thread stuck forever → EIO within budget.
# ============================================================================


@pytest.mark.trio
async def test_callback_budget_returns_eio_when_worker_thread_blocks_forever(
    client_conn: Connection[TupleRow],
) -> None:
    """The shape of the 2026-06-24 wedge.

    A worker thread parks in a system call we have no way to interrupt
    (folio_wait_bit_common). ``abandon_on_cancel=True`` lets the trio side
    return — the contract is that the trio side returns EIO within budget,
    not that the worker stops (it can't until the kernel releases the lock).
    """
    blocker = _BlockingPyfuse3()
    ops = _make_ops(client_conn, notify_store=blocker.notify_store, invalidate_inode=blocker.invalidate_inode)

    # Drive a callback whose worker calls ``notify_store`` (which blocks).
    # The current implementation does NOT call notify_store from any callback,
    # so this synthesizes the scenario through ``_callback_guard`` directly.
    started = trio.current_time()
    with pytest.raises(pyfuse3.FUSEError) as exc:
        with trio.fail_after(_TEST_BUDGET_S):
            with ops._callback_guard("synthetic", inode=1):  # pyright: ignore[reportPrivateUsage]
                # Worker thread is stuck in a syscall we can't interrupt.
                # abandon_on_cancel=True is the production setting; trio
                # raises Cancelled, fail_after rewrites it as TooSlowError,
                # _callback_guard converts to EIO.
                def _stuck() -> None:
                    blocker.notify_store(1, 0, b"x")  # blocks forever

                await trio.to_thread.run_sync(_stuck, abandon_on_cancel=True)

    elapsed = trio.current_time() - started
    assert exc.value.errno == errno.EIO
    # Budget + epsilon for scheduling. We've slept for `_BUDGET_S` already;
    # anything close to that and not a hang is success.
    assert elapsed < _BUDGET_S * 2.0, (
        f"callback budget didn't fire — elapsed {elapsed:.2f}s, budget {_BUDGET_S}s "
        f"(notify recorded: {len(blocker.notify_calls)})"
    )


@pytest.mark.trio
async def test_callback_budget_returns_eio_when_async_stage_stalls(
    client_conn: Connection[TupleRow],
) -> None:
    """A purely async stall (e.g. an ``await`` that doesn't complete) is
    cancellable. ``_callback_guard``'s outer ``fail_after`` raises TooSlowError
    and the guard surfaces EIO. Production analog: a future async stage in the
    read path (post-render, post-prime) that waits on something it shouldn't.
    """
    ops = _make_ops(client_conn, notify_store=NOOP_NOTIFY_STORE, invalidate_inode=NOOP_INVALIDATE_INODE)

    started = trio.current_time()
    with (
        pytest.raises(pyfuse3.FUSEError) as exc,
        ops._callback_guard("synthetic", inode=1),  # pyright: ignore[reportPrivateUsage]
    ):
        await trio.sleep(_BUDGET_S * 10.0)  # 5s on the default budget

    elapsed = trio.current_time() - started
    assert exc.value.errno == errno.EIO
    assert elapsed < _BUDGET_S * 2.0


# ============================================================================
# End-to-end: the actual read path with blocking-cache stubs.
# ============================================================================


@pytest.mark.trio
async def test_read_path_does_not_block_on_blocking_pyfuse3_stub(
    client_conn: Connection[TupleRow],
) -> None:
    """The regression test for 2026-06-24: a real ``ops.read`` against the
    blocking-cache stub must complete (success or EIO) within budget.

    If a future commit re-introduces a synchronous ``notify_store`` or
    ``invalidate_inode`` call inside the read callback, the worker parks
    forever in the stub. The outer budget catches it and surfaces EIO —
    so the test still passes WITH a timing assertion. ``blocker.notify_calls``
    is the regression diagnostic: a non-empty list after this test means
    someone is calling the cache hooks inside a callback again.
    """
    _seed_clean_world(client_conn)
    blocker = _BlockingPyfuse3()
    ops = _make_ops(client_conn, notify_store=blocker.notify_store, invalidate_inode=blocker.invalidate_inode)
    inode = ops.inodes.get_or_create("/channels/general/2026-06/08/channel.md")

    started = trio.current_time()
    # Either succeeds with content, or surfaces EIO within budget. Both
    # outcomes are acceptable — what we forbid is hanging.
    with trio.fail_after(_TEST_BUDGET_S):
        try:
            content = await ops.read(inode, 0, 131072)
        except pyfuse3.FUSEError as exc:
            assert exc.errno == errno.EIO
            content = b""
    elapsed = trio.current_time() - started

    assert elapsed < _BUDGET_S * 2.0, (
        f"read hung beyond budget — elapsed {elapsed:.2f}s. "
        f"Cache calls during read: notify={blocker.notify_calls}, "
        f"invalidate={blocker.invalidate_calls}. "
        f"Someone re-added a synchronous cache call inside the read callback."
    )

    # Diagnostic: notify_store / invalidate_inode must NOT have been called
    # synchronously from inside the read callback. Calls from background
    # tasks (StreamApplier, health_subscriber) are fine — they're not in
    # this code path. If this assertion fires, the budget saved us, but
    # the fix belongs in fuse_ops_v2.read, not here.
    assert blocker.notify_calls == [], (
        f"read called notify_store from inside the callback: {blocker.notify_calls}. "
        "This is the 2026-06-24 wedge pattern — dispatch via the post-commit "
        "invalidator path instead."
    )
    assert blocker.invalidate_calls == [], (
        f"read called invalidate_inode from inside the callback: {blocker.invalidate_calls}. "
        "This is the 2026-06-24 wedge pattern — dispatch via the post-commit "
        "invalidator path instead."
    )

    # Success case sanity check.
    if content:
        assert b"Hello" in content
