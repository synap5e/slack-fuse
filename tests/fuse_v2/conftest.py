"""Shared fixtures + seed helpers for Sprint 3B FUSE adapter tests.

The ``client_conn`` / ``client_conn_factory`` fixtures are re-imported from
``tests/projector/conftest.py`` so we get a freshly-migrated client-schema
connection per test without duplicating that boilerplate.
"""

from __future__ import annotations

import threading
from collections.abc import Iterable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import pytest
import trio

from slack_fuse.fuse_ops_v2 import InvalidateInodeFn, NotifyStoreFn, SlackFuseOpsV2

# Re-export so pytest picks up the projector conftest's DB fixtures.
from tests.projector.conftest import client_conn as _client_conn, client_conn_factory as _client_conn_factory

client_conn = _client_conn
client_conn_factory = _client_conn_factory

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow


def _noop_notify_store(_inode: int, _offset: int, _data: bytes) -> None:
    return None


def _noop_invalidate_inode(_inode: int) -> None:
    return None


#: Convenience callables for tests that don't care about kernel-cache hooks.
NOOP_NOTIFY_STORE: NotifyStoreFn = _noop_notify_store
NOOP_INVALIDATE_INODE: InvalidateInodeFn = _noop_invalidate_inode


# ============================================================================
# Notify/invalidate fakes
# ============================================================================


@dataclass(slots=True)
class _NotifyStoreCall:
    inode: int
    offset: int
    data: bytes


@dataclass(slots=True)
class FakePyfuse3:
    """Thread-safe recorder for ``notify_store`` / ``invalidate_inode``."""

    notify_calls: list[_NotifyStoreCall] = field(default_factory=list[_NotifyStoreCall])
    invalidate_calls: list[int] = field(default_factory=list[int])
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def notify_store(self, inode: int, offset: int, data: bytes) -> None:
        with self._lock:
            self.notify_calls.append(_NotifyStoreCall(inode=inode, offset=offset, data=data))

    def invalidate_inode(self, inode: int) -> None:
        with self._lock:
            self.invalidate_calls.append(inode)

    def primed_inodes(self) -> set[int]:
        with self._lock:
            return {c.inode for c in self.notify_calls}


@pytest.fixture
def fake_pyfuse3() -> FakePyfuse3:
    return FakePyfuse3()


# ============================================================================
# Construction helpers
# ============================================================================


@pytest.fixture
def utc_tz() -> ZoneInfo:
    return ZoneInfo("UTC")


@pytest.fixture
def ops(
    client_conn: Connection[TupleRow],
    utc_tz: ZoneInfo,
    fake_pyfuse3: FakePyfuse3,
) -> SlackFuseOpsV2:
    limiter = trio.CapacityLimiter(1)
    return SlackFuseOpsV2(
        conn=client_conn,
        local_tz=utc_tz,
        limiter=limiter,
        notify_store=fake_pyfuse3.notify_store,
        invalidate_inode=fake_pyfuse3.invalidate_inode,
    )


# ============================================================================
# Schema seeding
# ============================================================================


def seed_channel(  # noqa: PLR0913  (test-only seed helper; mirrors channels-table shape)
    conn: Connection[TupleRow],
    channel_id: str,
    name: str,
    *,
    tier: str = "hot",
    is_im: bool = False,
    is_mpim: bool = False,
    is_member: bool = True,
    is_archived: bool = False,
    im_user_id: str | None = None,
) -> None:
    """Insert a channels row. Connection must be autocommit (or caller commits)."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO channels (channel_id, name, is_im, is_mpim, is_member, is_archived, im_user_id, "
            "  tier, tier_source, subscribed) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'manual', %s)",
            (channel_id, name, is_im, is_mpim, is_member, is_archived, im_user_id, tier, tier != "blocked"),
        )


def seed_user(conn: Connection[TupleRow], user_id: str, display_name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO users (user_id, display_name) VALUES (%s, %s)",
            (user_id, display_name),
        )


def seed_chunk(  # noqa: PLR0913  (test-only seed helper)
    conn: Connection[TupleRow],
    channel_id: str,
    message_ts: str | Decimal,
    content_md: str,
    *,
    reply_count: int = 0,
    mentioned_user_ids: Iterable[str] = (),
    mentioned_channel_ids: Iterable[str] = (),
) -> None:
    """Insert a chunks row + chunk_mentions rows."""
    ts = Decimal(str(message_ts))
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO chunks (channel_id, message_ts, content_md, reply_count) VALUES (%s, %s, %s, %s)",
            (channel_id, ts, content_md, reply_count),
        )
        for uid in mentioned_user_ids:
            cur.execute(
                "INSERT INTO chunk_mentions (channel_id, message_ts, mention_kind, mentioned_id) "
                "VALUES (%s, %s, 'user', %s)",
                (channel_id, ts, uid),
            )
        for cid in mentioned_channel_ids:
            cur.execute(
                "INSERT INTO chunk_mentions (channel_id, message_ts, mention_kind, mentioned_id) "
                "VALUES (%s, %s, 'channel', %s)",
                (channel_id, ts, cid),
            )


def seed_thread_chunk(  # noqa: PLR0913, PLR0917  (test-only seed helper; matches table arity)
    conn: Connection[TupleRow],
    channel_id: str,
    thread_ts: str | Decimal,
    reply_ts: str | Decimal,
    role: str,
    content_md: str,
) -> None:
    t_ts = Decimal(str(thread_ts))
    r_ts = Decimal(str(reply_ts))
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO thread_chunks (channel_id, thread_ts, reply_ts, role, content_md) VALUES (%s, %s, %s, %s, %s)",
            (channel_id, t_ts, r_ts, role, content_md),
        )


def set_connection_state(
    conn: Connection[TupleRow],
    *,
    last_slurper_health: str | None = None,
    last_frame_at_offset_s: float | None = None,
) -> None:
    """Convenience: mutate connection_state to drive the trailer.

    ``last_frame_at_offset_s = None`` → leave as-is. A positive value sets
    ``last_frame_at = now() - INTERVAL N second``; negative → in the future.
    """
    if last_slurper_health is not None and last_frame_at_offset_s is not None:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE connection_state SET last_slurper_health = %s, "
                "last_frame_at = now() - make_interval(secs => %s) WHERE id = 1",
                (last_slurper_health, last_frame_at_offset_s),
            )
        return
    if last_slurper_health is not None:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE connection_state SET last_slurper_health = %s WHERE id = 1",
                (last_slurper_health,),
            )
    if last_frame_at_offset_s is not None:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE connection_state SET last_frame_at = now() - make_interval(secs => %s) WHERE id = 1",
                (last_frame_at_offset_s,),
            )


def mark_stream_caught_up(
    conn: Connection[TupleRow],
    stream: str,
    at_offset: int = 1,
    *,
    seconds_ago: float = 0.0,
) -> None:
    """Stamp ``stream_caught_up`` for ``stream``.

    ``seconds_ago`` backdates ``caught_up_at`` (default 0 → ``now()``) so the
    catch-up freshness window (``catchup_window_s``) can be crossed without
    touching the wall clock.
    """
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO stream_caught_up (stream, caught_up_at, at_offset) "
            "VALUES (%s, now() - make_interval(secs => %s), %s) "
            "ON CONFLICT (stream) DO UPDATE SET caught_up_at = EXCLUDED.caught_up_at, "
            "at_offset = GREATEST(stream_caught_up.at_offset, EXCLUDED.at_offset)",
            (stream, seconds_ago, at_offset),
        )
