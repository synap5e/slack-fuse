"""End-to-end smoke: a populated channel + date assembles to expected bytes.

This is the spec's "integration smoke against synthetic chunks" gate. The
inputs (channels rows + chunk content_md) mirror what Sprint 2E's projector
would write; the outputs (assembled FUSE bytes) mirror what a `cat` would
print today.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import pytest
import trio

from slack_fuse.fuse_ops_v2 import SlackFuseOpsV2
from tests.fuse_v2.conftest import (
    NOOP_INVALIDATE_INODE,
    NOOP_NOTIFY_STORE,
    FakePyfuse3,
    mark_stream_caught_up,
    seed_channel,
    seed_chunk,
    seed_thread_chunk,
    seed_user,
    set_connection_state,
)

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow


def _ts(dt: datetime) -> Decimal:
    return Decimal(str(dt.timestamp()))


def _seed_populated_day(conn: Connection[TupleRow]) -> None:
    """Populate ``general`` on 2026-06-08 UTC with three messages, one a thread."""
    seed_channel(conn, "C-GEN", "general", tier="hot")
    seed_user(conn, "UALICE", "alice")
    seed_user(conn, "UBOB", "bob")
    seed_chunk(
        conn,
        "C-GEN",
        _ts(datetime(2026, 6, 8, 9, 0, tzinfo=UTC)),
        "## 09:00 <@UALICE>\n\nMorning standup time\n",
        mentioned_user_ids=["UALICE"],
    )
    seed_chunk(
        conn,
        "C-GEN",
        _ts(datetime(2026, 6, 8, 9, 5, tzinfo=UTC)),
        "## 09:05 <@UBOB>\n\nHey <@UALICE>, can you double check the deploy?\n\n> Thread: 1 reply\n",
        reply_count=1,
        mentioned_user_ids=["UBOB", "UALICE"],
    )
    seed_chunk(
        conn,
        "C-GEN",
        _ts(datetime(2026, 6, 8, 17, 30, tzinfo=UTC)),
        "## 17:30 <@UALICE>\n\nEnd of day; see you tomorrow\n",
        mentioned_user_ids=["UALICE"],
    )
    # Thread reply on the 09:05 message.
    seed_thread_chunk(
        conn,
        "C-GEN",
        _ts(datetime(2026, 6, 8, 9, 5, tzinfo=UTC)),
        _ts(datetime(2026, 6, 8, 9, 5, tzinfo=UTC)),
        "parent",
        "## 09:05 <@UBOB>\n\nHey <@UALICE>, can you double check the deploy?\n",
    )
    seed_thread_chunk(
        conn,
        "C-GEN",
        _ts(datetime(2026, 6, 8, 9, 5, tzinfo=UTC)),
        _ts(datetime(2026, 6, 8, 9, 8, tzinfo=UTC)),
        "reply",
        "## 09:08 <@UALICE>\n\nOn it now\n",
    )
    set_connection_state(conn, last_slurper_health="healthy", last_frame_at_offset_s=1.0)
    mark_stream_caught_up(conn, "channel:C-GEN", at_offset=100)
    # channel.md is staleness-aware off the channel-list stream (P1-5), so a
    # fully-healthy world must mark it caught up too.
    mark_stream_caught_up(conn, "channel-list", at_offset=100)


@pytest.fixture
def populated_ops(
    client_conn: Connection[TupleRow],
    fake_pyfuse3: FakePyfuse3,
) -> SlackFuseOpsV2:
    _seed_populated_day(client_conn)
    return SlackFuseOpsV2(
        conn=client_conn,
        local_tz=ZoneInfo("UTC"),
        limiter=trio.CapacityLimiter(1),
        notify_store=fake_pyfuse3.notify_store,
        invalidate_inode=fake_pyfuse3.invalidate_inode,
    )


# ============================================================================
# Smoke: readdir traversal
# ============================================================================


def test_smoke_root_listing(populated_ops: SlackFuseOpsV2) -> None:
    listing = {name for name, _ in populated_ops.list_dir_for_test("/")}
    assert listing == {"channels", "dms", "group-dms", "other-channels"}


def test_smoke_channels_listing(populated_ops: SlackFuseOpsV2) -> None:
    listing = dict(populated_ops.list_dir_for_test("/channels"))
    assert listing == {"general": True}


def test_smoke_channel_root_lists_months(populated_ops: SlackFuseOpsV2) -> None:
    entries = populated_ops.list_dir_for_test("/channels/general")
    names = {name for name, _ in entries}
    assert "channel.md" in names
    assert "2026-06" in names


def test_smoke_month_dir_lists_day(populated_ops: SlackFuseOpsV2) -> None:
    entries = populated_ops.list_dir_for_test("/channels/general/2026-06")
    assert entries == [("08", True)]


def test_smoke_day_dir_lists_channel_md_and_thread(populated_ops: SlackFuseOpsV2) -> None:
    entries = populated_ops.list_dir_for_test("/channels/general/2026-06/08")
    names = {name for name, _ in entries}
    assert "channel.md" in names
    # Exactly one thread parent (the 09:05 message).
    threads = [n for n, d in entries if d]
    assert len(threads) == 1


# ============================================================================
# Smoke: channel.md bytes
# ============================================================================


def test_smoke_channel_md_bytes(populated_ops: SlackFuseOpsV2) -> None:
    resolved = populated_ops.resolve_content_for_test("/channels/general/2026-06/08/channel.md")
    assert resolved is not None
    content, had_trailer, had_fallback = resolved
    assert not had_trailer
    assert not had_fallback
    text = content.decode()
    # Frontmatter present.
    assert text.startswith("---\nchannel: general\nchannel_id: C-GEN\ndate: 2026-06-08\n---\n")
    # Mentions are resolved.
    assert "@alice" in text
    assert "@bob" in text
    # Three messages concatenated in ts order.
    assert text.index("Morning standup time") < text.index("double check the deploy") < text.index("End of day")


def test_smoke_thread_md_bytes(populated_ops: SlackFuseOpsV2) -> None:
    entries = populated_ops.list_dir_for_test("/channels/general/2026-06/08")
    thread_dir = next(name for name, is_dir in entries if is_dir)
    thread_path = f"/channels/general/2026-06/08/{thread_dir}/thread.md"
    resolved = populated_ops.resolve_content_for_test(thread_path)
    assert resolved is not None
    content, had_trailer, had_fallback = resolved
    assert not had_trailer
    assert not had_fallback
    text = content.decode()
    assert "thread_ts:" in text
    assert "reply_count: 1" in text
    assert "On it now" in text
    assert "@alice" in text
    assert "@bob" in text


def test_smoke_channel_meta_md(populated_ops: SlackFuseOpsV2) -> None:
    resolved = populated_ops.resolve_content_for_test("/channels/general/channel.md")
    assert resolved is not None
    content, had_trailer, had_fallback = resolved
    assert not had_trailer
    assert not had_fallback
    text = content.decode()
    assert "channel: general" in text
    assert "channel_id: C-GEN" in text
    assert "tier: hot" in text


# ============================================================================
# Smoke: pyfuse3-style read() returns the same bytes
# ============================================================================


@pytest.mark.trio
async def test_smoke_read_call_returns_full_bytes(populated_ops: SlackFuseOpsV2) -> None:
    path = "/channels/general/2026-06/08/channel.md"
    inode = populated_ops.inodes.get_or_create(path)
    content = await populated_ops.read(inode, 0, 1_000_000)
    assert content.startswith(b"---\nchannel: general")
    assert b"Morning standup time" in content


@pytest.mark.trio
async def test_smoke_read_slice_offset_size(populated_ops: SlackFuseOpsV2) -> None:
    path = "/channels/general/2026-06/08/channel.md"
    inode = populated_ops.inodes.get_or_create(path)
    full = await populated_ops.read(inode, 0, 1_000_000)
    sliced = await populated_ops.read(inode, 10, 30)
    assert sliced == full[10:40]


# ============================================================================
# Empty / negative cases
# ============================================================================


def test_unknown_slug_no_listing(client_conn: Connection[TupleRow], fake_pyfuse3: FakePyfuse3) -> None:
    seed_channel(client_conn, "C-GEN", "general", tier="hot")
    ops = SlackFuseOpsV2(
        conn=client_conn,
        local_tz=ZoneInfo("UTC"),
        limiter=trio.CapacityLimiter(1),
        notify_store=fake_pyfuse3.notify_store,
        invalidate_inode=fake_pyfuse3.invalidate_inode,
    )
    assert ops.list_dir_for_test("/channels/nope") == []
    assert ops.resolve_content_for_test("/channels/nope/channel.md") is None


def test_dm_uses_user_display_name(client_conn: Connection[TupleRow]) -> None:
    """DM slug derivation pulls from the local `users` table by im_user_id."""
    seed_channel(
        client_conn,
        "D-ALICE",
        "",
        tier="hot",
        is_im=True,
        im_user_id="UALICE",
        is_member=False,
    )
    seed_user(client_conn, "UALICE", "alice")
    ops = SlackFuseOpsV2(
        conn=client_conn,
        local_tz=ZoneInfo("UTC"),
        limiter=trio.CapacityLimiter(1),
        notify_store=NOOP_NOTIFY_STORE,
        invalidate_inode=NOOP_INVALIDATE_INODE,
    )
    listing = dict(ops.list_dir_for_test("/dms"))
    assert listing == {"alice": True}
