"""Regression tests for the pre-3B-merge review findings (P0/P1).

Each test is named ``test_<finding>_…`` so it greps back to the consolidated
fix list. These prove the *hard* half of the kernel-cache invariants the
original 3B tests left open: that already-primed clean bytes stop being served
once state goes bad — by wall-clock alone (P0-1), without spurious invalidation
while healthy (P0-3), through the real production wiring (P0-2) — plus the
filesystem-integrity and staleness gaps (P0-4, P1-5, P1-6, P1-7).
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import pyfuse3
import pytest
import trio

from slack_fuse.fuse_ops_v2 import (
    _IMMUTABLE_FILE_TIMEOUT_S,  # pyright: ignore[reportPrivateUsage]
    _MUTABLE_FILE_TIMEOUT_S,  # pyright: ignore[reportPrivateUsage]
    SlackFuseOpsV2,
    _file_attr_timeout,  # pyright: ignore[reportPrivateUsage]
)
from slack_fuse.fuse_v2_helpers import fetch_channel_by_slug
from slack_fuse.projector.health_subscriber import read_signature, watch_health, watch_health_once
from tests.fuse_v2.conftest import (
    NOOP_INVALIDATE_INODE,
    NOOP_NOTIFY_STORE,
    FakePyfuse3,
    mark_stream_caught_up,
    seed_channel,
    seed_chunk,
    seed_user,
    set_connection_state,
)

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow


def _ts(dt: datetime) -> Decimal:
    return Decimal(str(dt.timestamp()))


def _make_ops(conn: Connection[TupleRow], fake: FakePyfuse3) -> SlackFuseOpsV2:
    return SlackFuseOpsV2(
        conn=conn,
        local_tz=ZoneInfo("UTC"),
        limiter=trio.CapacityLimiter(1),
        notify_store=fake.notify_store,
        invalidate_inode=fake.invalidate_inode,
    )


def _seed_clean_world(conn: Connection[TupleRow]) -> None:
    """Healthy, caught-up world with one hot channel + one chunk."""
    seed_channel(conn, "C1", "general", tier="hot")
    seed_user(conn, "U1", "alice")
    seed_chunk(
        conn,
        "C1",
        _ts(datetime(2026, 6, 8, 14, 30, tzinfo=UTC)),
        "## 14:30 <@U1>\n\nHello world\n",
        mentioned_user_ids=["U1"],
    )
    set_connection_state(conn, last_slurper_health="healthy", last_frame_at_offset_s=1.0)
    mark_stream_caught_up(conn, "channel:C1", at_offset=10)
    mark_stream_caught_up(conn, "channel-list", at_offset=10)


DAY_PATH = "/channels/general/2026-06/08/channel.md"


# ============================================================================
# P0-1: time-driven staleness crossing invalidates already-primed bytes,
# with NO DB mutation. This is the merge gate both reviewers flagged.
# ============================================================================


@pytest.mark.trio
async def test_p0_1_time_crossing_invalidates_without_db_mutation(
    client_conn: Connection[TupleRow],
    fake_pyfuse3: FakePyfuse3,
) -> None:
    _seed_clean_world(client_conn)
    ops = _make_ops(client_conn, fake_pyfuse3)
    inode = ops.inodes.get_or_create(DAY_PATH)

    # Prime while healthy + caught up.
    _ = await ops.read(inode, 0, 131072)
    assert len(fake_pyfuse3.notify_calls) == 1
    assert ops.primed_inodes_snapshot == frozenset({inode})

    # Baseline now (frame fresh) → not stale.
    t0 = datetime.now(UTC)
    baseline = read_signature(client_conn, now=t0)
    assert baseline.frame_stale is False

    # Advance wall-clock past the 60s threshold. CRUCIALLY: no DB row changes.
    later = t0 + timedelta(seconds=61)
    updated = watch_health_once(client_conn, baseline, ops.invalidate_all_primed, now=later)

    # The derived signature flipped on time alone and fired invalidation.
    assert updated != baseline
    assert updated.frame_stale is True
    assert fake_pyfuse3.invalidate_calls == [inode]
    assert ops.primed_inodes_snapshot == frozenset()


@pytest.mark.trio
async def test_p0_1_read_after_threshold_trailers_and_skips_notify(
    client_conn: Connection[TupleRow],
    fake_pyfuse3: FakePyfuse3,
) -> None:
    """The other half of P0-1: once the frame is old, the next userspace read
    appends the trailer and does NOT re-prime."""
    _seed_clean_world(client_conn)
    ops = _make_ops(client_conn, fake_pyfuse3)
    inode = ops.inodes.get_or_create(DAY_PATH)

    _ = await ops.read(inode, 0, 131072)
    assert len(fake_pyfuse3.notify_calls) == 1

    # Frame frozen 61s in the past (server died, no new frames): the read path
    # classifies this as stale by wall-clock.
    set_connection_state(client_conn, last_frame_at_offset_s=61.0)
    content = await ops.read(inode, 0, 131072)

    assert b"Content may be stale" in content
    assert b"server unreachable" in content
    # No additional notify_store beyond the first (clean) prime.
    assert len(fake_pyfuse3.notify_calls) == 1


# ============================================================================
# P0-2: watch_health is wired to ops.invalidate_all_primed in production. This
# test exercises the real async loop against the real ops, mirroring
# cmd_mount_split's nursery wiring.
# ============================================================================


@pytest.mark.trio
async def test_p0_2_watch_health_integrated_invalidates_primed(
    client_conn: Connection[TupleRow],
    fake_pyfuse3: FakePyfuse3,
) -> None:
    _seed_clean_world(client_conn)
    ops = _make_ops(client_conn, fake_pyfuse3)
    inode = ops.inodes.get_or_create(DAY_PATH)
    _ = await ops.read(inode, 0, 131072)
    assert ops.primed_inodes_snapshot == frozenset({inode})

    async def mutator() -> None:
        await trio.sleep(0.001)
        set_connection_state(client_conn, last_slurper_health="degraded")

    # Exactly the wiring cmd_mount_split installs: watch_health → ops callback.
    async with trio.open_nursery() as nursery:
        nursery.start_soon(mutator)
        await watch_health(client_conn, ops.invalidate_all_primed, poll_interval_s=0.001, iterations=10)
        nursery.cancel_scope.cancel()

    assert inode in fake_pyfuse3.invalidate_calls
    assert ops.primed_inodes_snapshot == frozenset()


# ============================================================================
# P0-3: healthy operation (last_frame_at advancing on every frame) must NOT
# thrash the cache. The signature must be stable while frames keep arriving.
# ============================================================================


@pytest.mark.trio
async def test_p0_3_healthy_heartbeat_no_spurious_invalidation(
    client_conn: Connection[TupleRow],
    fake_pyfuse3: FakePyfuse3,
) -> None:
    _seed_clean_world(client_conn)
    ops = _make_ops(client_conn, fake_pyfuse3)
    inode = ops.inodes.get_or_create(DAY_PATH)
    _ = await ops.read(inode, 0, 131072)
    assert ops.primed_inodes_snapshot == frozenset({inode})

    async def heartbeat() -> None:
        # Simulate frames landing every "100ms": bump last_frame_at repeatedly,
        # always staying fresh. Pre-fix this changed the signature every tick.
        for _ in range(20):
            await trio.sleep(0.001)
            set_connection_state(client_conn, last_frame_at_offset_s=0.5)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(heartbeat)
        await watch_health(client_conn, ops.invalidate_all_primed, poll_interval_s=0.001, iterations=30)
        nursery.cancel_scope.cancel()

    # No invalidation: frame stayed fresh, health + catch-up unchanged.
    assert fake_pyfuse3.invalidate_calls == []
    assert ops.primed_inodes_snapshot == frozenset({inode})


# ============================================================================
# P0-4: a hidden channel must not steal a hot channel's slug between the
# readdir and lookup paths. Slug assignment is deterministic over the same set.
# ============================================================================


def test_p0_4_hidden_hot_same_name_no_slug_collision(
    client_conn: Connection[TupleRow],
) -> None:
    # The hidden channel sorts FIRST by channel_id; pre-fix it would have stolen
    # the base slug on the lookup path while readdir showed it on the hot one.
    seed_channel(client_conn, "C-AAA", "general", tier="hidden")
    seed_channel(client_conn, "C-ZZZ", "general", tier="hot")
    ops = SlackFuseOpsV2(
        conn=client_conn,
        local_tz=ZoneInfo("UTC"),
        limiter=trio.CapacityLimiter(1),
        notify_store=NOOP_NOTIFY_STORE,
        invalidate_inode=NOOP_INVALIDATE_INODE,
    )

    # readdir lists only the hot channel, under the unsuffixed slug.
    listing = {name for name, _ in ops.list_dir_for_test("/channels")}
    assert listing == {"general"}

    # lookup of the unsuffixed slug returns the HOT channel, not the hidden one.
    hot = fetch_channel_by_slug(client_conn, "channels", "general", allow_hidden=True)
    assert hot is not None
    assert hot.channel_id == "C-ZZZ"

    # The hidden channel is reachable but only under the suffixed slug.
    hidden = fetch_channel_by_slug(client_conn, "channels", "general-2", allow_hidden=True)
    assert hidden is not None
    assert hidden.channel_id == "C-AAA"

    # And the readdir path refuses the hidden channel under any slug.
    assert fetch_channel_by_slug(client_conn, "channels", "general-2", allow_hidden=False) is None


# ============================================================================
# P1-5: channel.md is staleness-aware (channel-list stream). Disconnected →
# trailer + no notify_store.
# ============================================================================


@pytest.mark.trio
async def test_p1_5_channel_md_disconnected_trailers_and_skips_notify(
    client_conn: Connection[TupleRow],
    fake_pyfuse3: FakePyfuse3,
) -> None:
    seed_channel(client_conn, "C1", "general", tier="hot")
    set_connection_state(client_conn, last_slurper_health="disconnected", last_frame_at_offset_s=1.0)
    ops = _make_ops(client_conn, fake_pyfuse3)
    inode = ops.inodes.get_or_create("/channels/general/channel.md")

    content = await ops.read(inode, 0, 131072)

    assert b"Content may be stale" in content
    assert b"socket-mode disconnected" in content
    assert fake_pyfuse3.notify_calls == []
    assert ops.primed_inodes_snapshot == frozenset()


def test_p1_5_channel_md_healthy_has_no_trailer(
    client_conn: Connection[TupleRow],
    fake_pyfuse3: FakePyfuse3,
) -> None:
    """Mirror image: a healthy + caught-up world leaves channel.md clean."""
    seed_channel(client_conn, "C1", "general", tier="hot")
    set_connection_state(client_conn, last_slurper_health="healthy", last_frame_at_offset_s=1.0)
    mark_stream_caught_up(client_conn, "channel-list", at_offset=5)
    ops = _make_ops(client_conn, fake_pyfuse3)
    resolved = ops.resolve_content_for_test("/channels/general/channel.md")
    assert resolved is not None
    _content, had_trailer, had_fallback = resolved
    assert had_trailer is False
    assert had_fallback is False


# ============================================================================
# P1-6: a tier flip to 'blocked' makes the WHOLE subtree ENOENT, even via
# inodes allocated while the channel was still hot.
# ============================================================================


def test_p1_6_blocked_tier_hides_already_allocated_deep_inodes(
    client_conn: Connection[TupleRow],
) -> None:
    seed_channel(client_conn, "C1", "charlie", tier="hot")
    seed_chunk(
        client_conn,
        "C1",
        _ts(datetime(2026, 6, 8, 14, 30, tzinfo=UTC)),
        "## 14:30 <@U1>\n\nHi\n",
    )
    ops = SlackFuseOpsV2(
        conn=client_conn,
        local_tz=ZoneInfo("UTC"),
        limiter=trio.CapacityLimiter(1),
        notify_store=NOOP_NOTIFY_STORE,
        invalidate_inode=NOOP_INVALIDATE_INODE,
    )
    deep_paths = [
        "/channels/charlie",
        "/channels/charlie/2026-06",
        "/channels/charlie/2026-06/08",
    ]
    inodes = {p: ops.inodes.get_or_create(p) for p in deep_paths}
    for p in deep_paths:
        assert ops.is_dir_for_test(p) is True

    # Flip the channel to blocked (e.g. archived).
    with client_conn.cursor() as cur:
        cur.execute(
            "UPDATE channels SET tier = 'blocked', is_archived = TRUE WHERE channel_id = 'C1'",
        )

    # Every depth is now non-dir, even through the pre-allocated inodes.
    for p in deep_paths:
        assert ops.is_dir_for_test(p) is False

    # getattr on the (still-known) month inode raises ENOENT.
    month_inode = inodes["/channels/charlie/2026-06"]

    async def _getattr() -> None:
        with pytest.raises(pyfuse3.FUSEError) as exc:
            _ = await ops.getattr(month_inode, _ctx())
        assert exc.value.errno == 2  # ENOENT

    trio.run(_getattr)

    # And the day channel.md content no longer resolves.
    assert ops.resolve_content_for_test("/channels/charlie/2026-06/08/channel.md") is None


def _ctx() -> pyfuse3.RequestContext:
    ctx = pyfuse3.RequestContext()
    return ctx


# ============================================================================
# P1-7: an upsert of an existing caught-up stream (no count change, no
# max-offset change) still moves the signature.
# ============================================================================


def test_p1_7_caught_up_upsert_changes_signature(
    client_conn: Connection[TupleRow],
) -> None:
    mark_stream_caught_up(client_conn, "channel:C1", at_offset=10)
    baseline = read_signature(client_conn)

    # Re-mark the SAME stream with the SAME offset: GREATEST keeps 10, so both
    # COUNT(*) and MAX(at_offset) are unchanged — the pre-fix signature missed
    # this. caught_up_at is restamped to now(), so the fixed signature differs.
    time.sleep(0.002)  # ensure now() advances a microsecond tick
    mark_stream_caught_up(client_conn, "channel:C1", at_offset=10)
    after = read_signature(client_conn)

    assert after.caught_up_count == baseline.caught_up_count
    assert after.caught_up_max_offset == baseline.caught_up_max_offset
    assert after != baseline


def test_p1_7_caught_up_lower_offset_upsert_changes_signature(
    client_conn: Connection[TupleRow],
) -> None:
    """A lower offset (GREATEST keeps the old) is also a transition worth
    detecting — covered by the restamped caught_up_at."""
    mark_stream_caught_up(client_conn, "channel:C1", at_offset=10)
    baseline = read_signature(client_conn)
    time.sleep(0.002)
    mark_stream_caught_up(client_conn, "channel:C1", at_offset=5)
    after = read_signature(client_conn)
    assert after.caught_up_max_offset == 10  # unchanged
    assert after != baseline


# ============================================================================
# P2-8: attr/entry timeouts. Only strictly-past-day channel.md is immutable.
# ============================================================================


def test_p2_8_attr_timeout_only_past_day_channel_md_is_cached() -> None:
    tz = ZoneInfo("UTC")
    # A day comfortably in the past is immutable → cacheable.
    assert _file_attr_timeout("/channels/general/2020-01/02/channel.md", tz) == _IMMUTABLE_FILE_TIMEOUT_S
    # A far-future day is "not past" → must not be cached.
    assert _file_attr_timeout("/channels/general/2999-01/02/channel.md", tz) == _MUTABLE_FILE_TIMEOUT_S
    # Thread files (replies can land any day) and metadata stay uncached.
    assert _file_attr_timeout("/channels/general/2020-01/02/some-thread/thread.md", tz) == _MUTABLE_FILE_TIMEOUT_S
    assert _file_attr_timeout("/channels/general/channel.md", tz) == _MUTABLE_FILE_TIMEOUT_S


# ============================================================================
# P2-9: forget() drops the in-memory inode cache without losing persistence;
# readdir snapshots are stable against concurrent inserts.
# ============================================================================


def test_p2_9_forget_drops_cache_but_inode_persists(
    client_conn: Connection[TupleRow],
) -> None:
    seed_channel(client_conn, "C1", "general", tier="hot")
    ops = SlackFuseOpsV2(
        conn=client_conn,
        local_tz=ZoneInfo("UTC"),
        limiter=trio.CapacityLimiter(1),
        notify_store=NOOP_NOTIFY_STORE,
        invalidate_inode=NOOP_INVALIDATE_INODE,
    )
    inode = ops.inodes.get_or_create("/channels/general")
    assert ops.inodes.get_path(inode) == "/channels/general"

    async def _forget() -> None:
        await ops.forget([(inode, 1)])

    trio.run(_forget)

    # The in-memory reverse map dropped the entry...
    assert inode not in ops.inodes._inode_to_path  # pyright: ignore[reportPrivateUsage]
    # ...but the persistent row survives: re-reading returns the SAME inode.
    assert ops.inodes.get_inode("/channels/general") == inode
    assert ops.inodes.get_path(inode) == "/channels/general"


def test_p2_9_forget_never_drops_root(
    client_conn: Connection[TupleRow],
) -> None:
    ops = SlackFuseOpsV2(
        conn=client_conn,
        local_tz=ZoneInfo("UTC"),
        limiter=trio.CapacityLimiter(1),
        notify_store=NOOP_NOTIFY_STORE,
        invalidate_inode=NOOP_INVALIDATE_INODE,
    )

    async def _forget_root() -> None:
        await ops.forget([(1, 1)])

    trio.run(_forget_root)
    assert ops.inodes.get_path(1) == "/"


def test_p2_9_readdir_snapshot_is_stable_against_concurrent_inserts(
    client_conn: Connection[TupleRow],
) -> None:
    """The opendir snapshot freezes the listing + pagination tokens, so a
    channel inserted mid-iteration can't shift the array (array-index tokens
    would otherwise skip/duplicate entries)."""
    seed_channel(client_conn, "C-AAA", "alpha", tier="hot")
    seed_channel(client_conn, "C-BBB", "bravo", tier="hot")
    ops = SlackFuseOpsV2(
        conn=client_conn,
        local_tz=ZoneInfo("UTC"),
        limiter=trio.CapacityLimiter(1),
        notify_store=NOOP_NOTIFY_STORE,
        invalidate_inode=NOOP_INVALIDATE_INODE,
    )
    snapshot = ops._snapshot_dir("/channels")  # pyright: ignore[reportPrivateUsage]
    names_before = [name for name, _attr, _tok in snapshot]
    tokens = [tok for _name, _attr, tok in snapshot]
    assert names_before == ["alpha", "bravo"]
    assert tokens == [1, 2]  # stable, contiguous

    # A new channel arrives; the already-captured snapshot is unaffected.
    seed_channel(client_conn, "C-000", "aaa-first", tier="hot")
    names_after = [name for name, _attr, _tok in snapshot]
    assert names_after == names_before
