"""The five hard invariants from the Sprint 3B handoff.

Each test corresponds to one numbered invariant in
``slack_fuse/fuse_ops_v2.py``'s module docstring and the spec. The test
names start with ``test_invariant_<n>_…`` so reviewers can grep them
back to the spec line.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import pytest
import trio

from slack_fuse.fuse_ops_v2 import SlackFuseOpsV2
from slack_fuse.fuse_v2_helpers import StalenessState, format_trailer, staleness_reason
from slack_fuse.projector.health_subscriber import HealthSignature, read_signature, watch_health_once
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


# ============================================================================
# Helpers
# ============================================================================


def _ts(dt: datetime) -> Decimal:
    return Decimal(str(dt.timestamp()))


def _seed_clean_world(
    conn: Connection[TupleRow],
    *,
    health: str = "healthy",
    catch_up: bool = True,
) -> None:
    """One healthy channel, one user, one chunk, optionally caught up."""
    seed_channel(conn, "C1", "general", tier="hot")
    seed_user(conn, "U1", "alice")
    seed_chunk(
        conn,
        "C1",
        _ts(datetime(2026, 6, 8, 14, 30, tzinfo=UTC)),
        "## 14:30 <@U1>\n\nHello world\n",
        mentioned_user_ids=["U1"],
    )
    set_connection_state(conn, last_slurper_health=health, last_frame_at_offset_s=1.0)
    if catch_up:
        mark_stream_caught_up(conn, "channel:C1", at_offset=10)


@pytest.fixture
def channel_path() -> str:
    return "/channels/general/2026-06/08/channel.md"


# ============================================================================
# Invariant 1: Trailer / kernel-cache invariant
# ============================================================================


@pytest.mark.trio
async def test_invariant_1_trailer_suppresses_notify_store(
    client_conn: Connection[TupleRow],
    fake_pyfuse3: FakePyfuse3,
    channel_path: str,
) -> None:
    """When the rendered bytes include a staleness trailer, the read handler
    MUST NOT call notify_store. Spec: RFC §FUSE read path → Trailer /
    kernel-cache invariant.
    """
    _seed_clean_world(client_conn, health="disconnected")
    ops = SlackFuseOpsV2(
        conn=client_conn,
        local_tz=ZoneInfo("UTC"),
        limiter=trio.CapacityLimiter(1),
        notify_store=fake_pyfuse3.notify_store,
        invalidate_inode=fake_pyfuse3.invalidate_inode,
    )
    inode = ops.inodes.get_or_create(channel_path)

    content = await ops.read(inode, 0, 131072)

    # 1. Trailer is present in the output.
    assert b"\xe2\x9a\xa0 Content may be stale" in content
    assert b"socket-mode disconnected" in content
    # 2. notify_store was NOT called.
    assert fake_pyfuse3.notify_calls == []
    assert ops.primed_inodes_snapshot == frozenset()


@pytest.mark.trio
async def test_invariant_1_no_trailer_marks_inode_primed(
    client_conn: Connection[TupleRow],
    fake_pyfuse3: FakePyfuse3,
    channel_path: str,
) -> None:
    """Mirror image of invariant 1: when the bytes are clean, the inode is
    tracked as primed.

    2026-06-24: notify_store was removed from the read path (it deadlocked
    against in-flight kernel reads — see fuse_ops_v2.read). The kernel
    caches the bytes via ``fi.keep_cache=True`` instead. The priming-decision
    bookkeeping (``primed_inodes``) is preserved so the invalidator still
    knows which inodes' caches to drop on health changes.
    """
    _seed_clean_world(client_conn)
    ops = SlackFuseOpsV2(
        conn=client_conn,
        local_tz=ZoneInfo("UTC"),
        limiter=trio.CapacityLimiter(1),
        notify_store=fake_pyfuse3.notify_store,
        invalidate_inode=fake_pyfuse3.invalidate_inode,
    )
    inode = ops.inodes.get_or_create(channel_path)
    content = await ops.read(inode, 0, 131072)

    assert b"\xe2\x9a\xa0 Content may be stale" not in content
    assert fake_pyfuse3.notify_calls == []
    assert ops.primed_inodes_snapshot == frozenset({inode})


# ============================================================================
# Invariant 2: Unresolved-fallback / kernel-cache invariant
# ============================================================================


@pytest.mark.trio
async def test_invariant_2_unresolved_user_fallback_suppresses_notify_store(
    client_conn: Connection[TupleRow],
    fake_pyfuse3: FakePyfuse3,
    channel_path: str,
) -> None:
    """If resolve_mentions falls back to a UID literal, notify_store MUST NOT
    fire. Spec: RFC §FUSE read path → Unresolved-fallback / kernel-cache
    invariant + 2026-06-08 adversarial-review entry in the same section.
    """
    seed_channel(client_conn, "C1", "general", tier="hot")
    # Chunk mentions <@U999>; the users table has NO U999 entry yet.
    seed_chunk(
        client_conn,
        "C1",
        _ts(datetime(2026, 6, 8, 14, 30, tzinfo=UTC)),
        "## 14:30 <@U999>\n\nUnknown poster\n",
        mentioned_user_ids=["U999"],
    )
    set_connection_state(client_conn, last_slurper_health="healthy", last_frame_at_offset_s=1.0)
    mark_stream_caught_up(client_conn, "channel:C1")
    ops = SlackFuseOpsV2(
        conn=client_conn,
        local_tz=ZoneInfo("UTC"),
        limiter=trio.CapacityLimiter(1),
        notify_store=fake_pyfuse3.notify_store,
        invalidate_inode=fake_pyfuse3.invalidate_inode,
    )
    inode = ops.inodes.get_or_create(channel_path)
    content = await ops.read(inode, 0, 131072)

    # Fallback literal is present.
    assert b"@U999" in content
    # No trailer (channel is otherwise healthy + caught up).
    assert b"\xe2\x9a\xa0 Content may be stale" not in content
    # And critically: notify_store was NOT called.
    assert fake_pyfuse3.notify_calls == []
    assert ops.primed_inodes_snapshot == frozenset()


@pytest.mark.trio
async def test_invariant_2_subsequent_read_after_user_added_marks_inode_primed(
    client_conn: Connection[TupleRow],
    fake_pyfuse3: FakePyfuse3,
    channel_path: str,
) -> None:
    """After the missing user_added arrives, the next read substitutes the
    display name AND marks the inode primed (the RFC's "next read re-renders
    against the now-populated users/channels tables and notify_stores the
    correct bytes" — see 2026-06-24 note in
    test_invariant_1_no_trailer_marks_inode_primed).
    """
    seed_channel(client_conn, "C1", "general", tier="hot")
    seed_chunk(
        client_conn,
        "C1",
        _ts(datetime(2026, 6, 8, 14, 30, tzinfo=UTC)),
        "## 14:30 <@U999>\n\nUnknown poster\n",
        mentioned_user_ids=["U999"],
    )
    set_connection_state(client_conn, last_slurper_health="healthy", last_frame_at_offset_s=1.0)
    mark_stream_caught_up(client_conn, "channel:C1")
    ops = SlackFuseOpsV2(
        conn=client_conn,
        local_tz=ZoneInfo("UTC"),
        limiter=trio.CapacityLimiter(1),
        notify_store=fake_pyfuse3.notify_store,
        invalidate_inode=fake_pyfuse3.invalidate_inode,
    )
    inode = ops.inodes.get_or_create(channel_path)
    content_pre = await ops.read(inode, 0, 131072)
    assert fake_pyfuse3.notify_calls == []
    assert ops.primed_inodes_snapshot == frozenset()

    # user_added arrives.
    seed_user(client_conn, "U999", "carla")
    content_post = await ops.read(inode, 0, 131072)

    assert b"@carla" in content_post
    assert b"@U999" not in content_post
    # And now priming is recorded.
    assert ops.primed_inodes_snapshot == frozenset({inode})
    assert fake_pyfuse3.notify_calls == []
    # The fallback first read produced bytes that include the literal.
    assert b"@U999" in content_pre


@pytest.mark.trio
async def test_invariant_2_unresolved_channel_fallback_suppresses_notify_store(
    client_conn: Connection[TupleRow],
    fake_pyfuse3: FakePyfuse3,
    channel_path: str,
) -> None:
    """Same invariant but for <#C…> placeholders against the channels table."""
    seed_channel(client_conn, "C1", "general", tier="hot")
    # The chunk mentions a different channel that hasn't yet arrived.
    # NB: <@U…>/<#C…> placeholders require the ID to match `[A-Z0-9]+`
    # (see slack_fuse_render.mrkdwn.USER_MENTION / CHANNEL_MENTION). The
    # users.list/channels.list streams haven't delivered U999/C999 yet.
    seed_chunk(
        client_conn,
        "C1",
        _ts(datetime(2026, 6, 8, 14, 30, tzinfo=UTC)),
        "## 14:30 <@U999>\n\nFollow up in <#C999>\n",
        mentioned_channel_ids=["C999"],
        mentioned_user_ids=["U999"],
    )
    set_connection_state(client_conn, last_slurper_health="healthy", last_frame_at_offset_s=1.0)
    mark_stream_caught_up(client_conn, "channel:C1")
    ops = SlackFuseOpsV2(
        conn=client_conn,
        local_tz=ZoneInfo("UTC"),
        limiter=trio.CapacityLimiter(1),
        notify_store=fake_pyfuse3.notify_store,
        invalidate_inode=fake_pyfuse3.invalidate_inode,
    )
    inode = ops.inodes.get_or_create(channel_path)
    content = await ops.read(inode, 0, 131072)
    # Both fallbacks appear.
    assert b"#C999" in content
    assert b"@U999" in content
    assert fake_pyfuse3.notify_calls == []


# ============================================================================
# Invariant 3: connection_state / stream_caught_up changes invalidate primed
# ============================================================================


@pytest.mark.trio
async def test_invariant_3_connection_state_change_invalidates_primed(
    client_conn: Connection[TupleRow],
    fake_pyfuse3: FakePyfuse3,
    channel_path: str,
) -> None:
    """Prime an inode via a clean read; then mutate connection_state and
    verify the health subscriber invalidates every primed inode (via the
    on_change callback wiring).
    """
    _seed_clean_world(client_conn)
    ops = SlackFuseOpsV2(
        conn=client_conn,
        local_tz=ZoneInfo("UTC"),
        limiter=trio.CapacityLimiter(1),
        notify_store=fake_pyfuse3.notify_store,
        invalidate_inode=fake_pyfuse3.invalidate_inode,
    )
    inode = ops.inodes.get_or_create(channel_path)
    _ = await ops.read(inode, 0, 131072)
    assert ops.primed_inodes_snapshot == frozenset({inode})

    # Take initial signature and force a transition (any field change counts).
    baseline = read_signature(client_conn)
    set_connection_state(client_conn, last_slurper_health="degraded")

    updated = watch_health_once(client_conn, baseline, ops.invalidate_all_primed)
    assert updated != baseline
    assert fake_pyfuse3.invalidate_calls == [inode]
    # Drained.
    assert ops.primed_inodes_snapshot == frozenset()


@pytest.mark.trio
async def test_invariant_3_stream_caught_up_insert_invalidates_primed(
    client_conn: Connection[TupleRow],
    fake_pyfuse3: FakePyfuse3,
    channel_path: str,
) -> None:
    """A new ``stream_caught_up`` row is also a transition that must
    invalidate every primed inode."""
    _seed_clean_world(client_conn, catch_up=False)
    # Even without catch-up, this chunk renders. We'll prime then catch up.
    # First mark a partial connection_state so the trailer fires initially.
    set_connection_state(client_conn, last_slurper_health="healthy", last_frame_at_offset_s=1.0)

    ops = SlackFuseOpsV2(
        conn=client_conn,
        local_tz=ZoneInfo("UTC"),
        limiter=trio.CapacityLimiter(1),
        notify_store=fake_pyfuse3.notify_store,
        invalidate_inode=fake_pyfuse3.invalidate_inode,
    )
    inode = ops.inodes.get_or_create(channel_path)
    # First read happens before catch-up → trailer present → not primed.
    content_pre = await ops.read(inode, 0, 131072)
    assert b"catching up after reconnect" in content_pre
    assert ops.primed_inodes_snapshot == frozenset()

    # Mark caught up. Next read should be clean and prime the inode.
    mark_stream_caught_up(client_conn, "channel:C1", at_offset=42)
    content_post = await ops.read(inode, 0, 131072)
    assert b"catching up after reconnect" not in content_post
    assert ops.primed_inodes_snapshot == frozenset({inode})

    # The stream_caught_up insert above is exactly the kind of event the
    # subscriber should detect. Take a baseline AFTER priming and force
    # another caught_up bump.
    baseline = read_signature(client_conn)
    mark_stream_caught_up(client_conn, "channel:C2", at_offset=1)
    _ = watch_health_once(client_conn, baseline, ops.invalidate_all_primed)
    assert fake_pyfuse3.invalidate_calls == [inode]


def test_invariant_3_signature_equality(client_conn: Connection[TupleRow]) -> None:
    """Stable signature when nothing changes (sanity for the polling loop)."""
    _seed_clean_world(client_conn)
    a = read_signature(client_conn)
    b = read_signature(client_conn)
    assert a == b
    assert isinstance(a, HealthSignature)


# ============================================================================
# Invariant 4: persistent inodes across mount restarts
# ============================================================================


def test_invariant_4_inodes_survive_simulated_restart(
    client_conn: Connection[TupleRow],
) -> None:
    """Construct ``ops``, traverse some paths, then construct a SECOND ``ops``
    against the SAME connection (simulating a restart against the same DB).
    Inodes for the same paths must come back identical.
    """
    seed_channel(client_conn, "C1", "general", tier="hot")
    seed_channel(client_conn, "C2", "engineering", tier="hot")

    ops1 = SlackFuseOpsV2(
        conn=client_conn,
        local_tz=ZoneInfo("UTC"),
        limiter=trio.CapacityLimiter(1),
        notify_store=NOOP_NOTIFY_STORE,
        invalidate_inode=NOOP_INVALIDATE_INODE,
    )
    paths = [
        "/channels",
        "/channels/general",
        "/channels/general/2026-06",
        "/channels/general/2026-06/08",
        "/channels/engineering",
    ]
    before = {p: ops1.inodes.get_or_create(p) for p in paths}
    # Sanity: every inode is unique and greater than 1.
    assert len(set(before.values())) == len(paths)
    assert all(v > 1 for v in before.values())

    # Simulated restart: fresh ops, same DB.
    ops2 = SlackFuseOpsV2(
        conn=client_conn,
        local_tz=ZoneInfo("UTC"),
        limiter=trio.CapacityLimiter(1),
        notify_store=NOOP_NOTIFY_STORE,
        invalidate_inode=NOOP_INVALIDATE_INODE,
    )
    after = {p: ops2.inodes.get_or_create(p) for p in paths}
    assert after == before

    # Reverse lookup also works.
    for p, ino in after.items():
        assert ops2.inodes.get_path(ino) == p


def test_invariant_4_root_inode_is_one(client_conn: Connection[TupleRow]) -> None:
    ops = SlackFuseOpsV2(
        conn=client_conn,
        local_tz=ZoneInfo("UTC"),
        limiter=trio.CapacityLimiter(1),
        notify_store=NOOP_NOTIFY_STORE,
        invalidate_inode=NOOP_INVALIDATE_INODE,
    )
    assert ops.inodes.get_or_create("/") == 1
    assert ops.inodes.get_path(1) == "/"


# ============================================================================
# Invariant 5: hot / hidden / blocked tier semantics
# ============================================================================


def test_invariant_5_readdir_filters_hot_only(
    client_conn: Connection[TupleRow],
) -> None:
    """``readdir`` of a conv-root only emits channels with ``tier='hot'``."""
    seed_channel(client_conn, "C1", "alpha", tier="hot")
    seed_channel(client_conn, "C2", "bravo", tier="hidden")
    seed_channel(client_conn, "C3", "charlie", tier="blocked", is_archived=True)
    ops = SlackFuseOpsV2(
        conn=client_conn,
        local_tz=ZoneInfo("UTC"),
        limiter=trio.CapacityLimiter(1),
        notify_store=NOOP_NOTIFY_STORE,
        invalidate_inode=NOOP_INVALIDATE_INODE,
    )
    listing = {name for name, _ in ops.list_dir_for_test("/channels")}
    assert listing == {"alpha"}


def test_invariant_5_lookup_allows_hidden(
    client_conn: Connection[TupleRow],
) -> None:
    """``lookup`` of ``/channels/<hidden-slug>`` succeeds even though
    ``readdir`` doesn't list it."""
    seed_channel(client_conn, "C1", "alpha", tier="hot")
    seed_channel(client_conn, "C2", "bravo", tier="hidden")
    ops = SlackFuseOpsV2(
        conn=client_conn,
        local_tz=ZoneInfo("UTC"),
        limiter=trio.CapacityLimiter(1),
        notify_store=NOOP_NOTIFY_STORE,
        invalidate_inode=NOOP_INVALIDATE_INODE,
    )
    # The hidden slug is_dir → True (lookup-by-name path).
    assert ops.is_dir_for_test("/channels/bravo") is True


def test_invariant_5_lookup_blocked_is_enoent(
    client_conn: Connection[TupleRow],
) -> None:
    """``blocked`` channels are ENOENT to both readdir and lookup."""
    seed_channel(client_conn, "C1", "alpha", tier="hot")
    seed_channel(client_conn, "C3", "charlie", tier="blocked", is_archived=True)
    ops = SlackFuseOpsV2(
        conn=client_conn,
        local_tz=ZoneInfo("UTC"),
        limiter=trio.CapacityLimiter(1),
        notify_store=NOOP_NOTIFY_STORE,
        invalidate_inode=NOOP_INVALIDATE_INODE,
    )
    listing = {name for name, _ in ops.list_dir_for_test("/channels")}
    assert listing == {"alpha"}
    # Blocked slug must NOT resolve as a dir, AND content lookup must fail.
    assert ops.is_dir_for_test("/channels/charlie") is False
    assert ops.resolve_content_for_test("/channels/charlie/channel.md") is None


def test_invariant_5_hot_priming_gated_on_tier(
    client_conn: Connection[TupleRow],
    fake_pyfuse3: FakePyfuse3,
) -> None:
    """``notify_store`` only fires when the channel's tier is ``hot``."""
    seed_channel(client_conn, "C1", "alpha", tier="hidden")
    seed_chunk(
        client_conn,
        "C1",
        _ts(datetime(2026, 6, 8, 14, 30, tzinfo=UTC)),
        "## 14:30 <@U1>\n\nHi\n",
    )
    seed_user(client_conn, "U1", "alice")
    set_connection_state(client_conn, last_slurper_health="healthy", last_frame_at_offset_s=1.0)
    mark_stream_caught_up(client_conn, "channel:C1")
    ops = SlackFuseOpsV2(
        conn=client_conn,
        local_tz=ZoneInfo("UTC"),
        limiter=trio.CapacityLimiter(1),
        notify_store=fake_pyfuse3.notify_store,
        invalidate_inode=fake_pyfuse3.invalidate_inode,
    )
    path = "/channels/alpha/2026-06/08/channel.md"
    inode = ops.inodes.get_or_create(path)

    async def _go() -> None:
        _ = await ops.read(inode, 0, 131072)

    trio.run(_go)
    # Hidden tier → no priming.
    assert fake_pyfuse3.notify_calls == []


# ============================================================================
# Format-of-trailer sanity (paired with invariant 1 — a content check)
# ============================================================================


def test_trailer_format_matches_rfc_template() -> None:
    state = StalenessState(
        last_frame_at=datetime(2026, 5, 26, 9, 42, 11, tzinfo=UTC),
        last_slurper_health="disconnected",
        last_health_update_at=datetime(2026, 5, 26, 9, 42, 11, tzinfo=UTC),
        initial_catch_up_done_for_stream=True,
    )
    reason = staleness_reason(state, now=datetime(2026, 5, 26, 12, 42, 11, tzinfo=UTC))
    assert reason == "socket-mode disconnected"
    out = format_trailer(reason, state.last_frame_at)
    assert "---" in out
    assert "2026-05-26 09:42:11 UTC" in out
