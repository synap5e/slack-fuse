"""Sprint 3E: the V2 ``InvalidationSink`` (``fuse_ops_v2.V2InvalidationSink``).

The sink maps the projector's post-commit ``ChunkRef`` / ``ThreadChunkRef`` /
channel-list intents onto V2 FUSE inodes and drops their kernel page cache.
These tests assert the path-resolution + inode-drop behaviour against a real
migrated client schema, plus the end-to-end original cross-stream race
(message-before-user_added) flowing through apply → read → sink → read.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, cast
from zoneinfo import ZoneInfo

import httpx
import pytest

from slack_fuse.fuse_ops_v2 import SlackFuseOpsV2, V2InvalidationSink
from slack_fuse.fuse_v2_helpers import dedup_thread_slug_map, fetch_day_thread_parents
from slack_fuse.models import JsonObject
from slack_fuse.projector.apply import ChunkRef, ThreadChunkRef, apply_event
from slack_fuse.projector.snapshot_fetch import SnapshotRedirect, fetch_and_apply_snapshot
from slack_fuse_server.wire.frames import EventFrame
from tests.fuse_v2.conftest import (
    FakePyfuse3,
    mark_stream_caught_up,
    seed_channel,
    seed_chunk,
    set_connection_state,
)

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow


def _ts(dt: datetime) -> Decimal:
    return Decimal(str(dt.timestamp()))


def _payload(**fields: object) -> JsonObject:
    return cast("JsonObject", dict(fields))


def _make_sink(conn: Connection[TupleRow], fake: FakePyfuse3) -> V2InvalidationSink:
    return V2InvalidationSink(conn, ZoneInfo("UTC"), invalidate_inode=fake.invalidate_inode)


# ============================================================================
# chunk_changed → day-file inode
# ============================================================================


def test_chunk_changed_invalidates_materialized_day_file_inode(
    client_conn: Connection[TupleRow],
    ops: SlackFuseOpsV2,
    fake_pyfuse3: FakePyfuse3,
) -> None:
    seed_channel(client_conn, "C1", "general", tier="hot")
    ts = _ts(datetime(2026, 6, 8, 14, 30, tzinfo=UTC))
    day_path = "/channels/general/2026-06/08/channel.md"
    inode = ops.inodes.get_or_create(day_path)

    sink = _make_sink(client_conn, fake_pyfuse3)
    sink.chunk_changed(ChunkRef("C1", ts))

    assert fake_pyfuse3.invalidate_calls == [inode]


def test_chunk_changed_skips_unmaterialized_inode(
    client_conn: Connection[TupleRow],
    fake_pyfuse3: FakePyfuse3,
) -> None:
    """A ChunkRef whose day-file inode was never allocated → no invalidate.

    The kernel can't be holding bytes for a path it never looked up.
    """
    seed_channel(client_conn, "C1", "general", tier="hot")
    ts = _ts(datetime(2026, 6, 8, 14, 30, tzinfo=UTC))

    sink = _make_sink(client_conn, fake_pyfuse3)
    sink.chunk_changed(ChunkRef("C1", ts))

    assert fake_pyfuse3.invalidate_calls == []


def test_chunk_changed_unknown_channel_is_noop(
    client_conn: Connection[TupleRow],
    fake_pyfuse3: FakePyfuse3,
) -> None:
    sink = _make_sink(client_conn, fake_pyfuse3)
    sink.chunk_changed(ChunkRef("CNOPE", _ts(datetime(2026, 6, 8, 14, 30, tzinfo=UTC))))
    assert fake_pyfuse3.invalidate_calls == []


def test_chunk_changed_blocked_channel_is_noop(
    client_conn: Connection[TupleRow],
    ops: SlackFuseOpsV2,
    fake_pyfuse3: FakePyfuse3,
) -> None:
    """Blocked channels have no reachable subtree, so nothing to invalidate even
    if a day-file inode was somehow allocated while it was still hot."""
    seed_channel(client_conn, "C1", "blocked-chan", tier="blocked", is_archived=True)
    ts = _ts(datetime(2026, 6, 8, 14, 30, tzinfo=UTC))
    _ = ops.inodes.get_or_create("/channels/blocked-chan/2026-06/08/channel.md")

    sink = _make_sink(client_conn, fake_pyfuse3)
    sink.chunk_changed(ChunkRef("C1", ts))

    assert fake_pyfuse3.invalidate_calls == []


# ============================================================================
# thread_chunk_changed → thread.md inode
# ============================================================================


def test_thread_chunk_changed_invalidates_thread_file_inode(
    client_conn: Connection[TupleRow],
    ops: SlackFuseOpsV2,
    fake_pyfuse3: FakePyfuse3,
) -> None:
    seed_channel(client_conn, "C1", "general", tier="hot")
    parent_ts = _ts(datetime(2026, 6, 8, 9, 0, tzinfo=UTC))
    reply_ts = _ts(datetime(2026, 6, 8, 9, 5, tzinfo=UTC))
    # A thread parent (reply_count > 0) so the day folder lists a thread dir.
    seed_chunk(
        client_conn,
        "C1",
        parent_ts,
        "## 09:00 <@U1>\n\nLets discuss the plan\n\n> Thread: 1 replies\n",
        reply_count=1,
    )
    # Derive the slug exactly as the read path / sink does.
    parents = fetch_day_thread_parents(client_conn, "C1", datetime(2026, 6, 8).date(), ZoneInfo("UTC"))
    slug_map = dedup_thread_slug_map(parents)
    [(thread_slug, mapped_ts)] = list(slug_map.items())
    assert mapped_ts == parent_ts
    thread_path = f"/channels/general/2026-06/08/{thread_slug}/thread.md"
    inode = ops.inodes.get_or_create(thread_path)

    sink = _make_sink(client_conn, fake_pyfuse3)
    sink.thread_chunk_changed(ThreadChunkRef("C1", parent_ts, reply_ts))

    assert fake_pyfuse3.invalidate_calls == [inode]


def test_thread_chunk_changed_unknown_thread_is_noop(
    client_conn: Connection[TupleRow],
    fake_pyfuse3: FakePyfuse3,
) -> None:
    seed_channel(client_conn, "C1", "general", tier="hot")
    # No thread parent seeded → slug resolution misses → no invalidate.
    sink = _make_sink(client_conn, fake_pyfuse3)
    sink.thread_chunk_changed(
        ThreadChunkRef(
            "C1",
            _ts(datetime(2026, 6, 8, 9, 0, tzinfo=UTC)),
            _ts(datetime(2026, 6, 8, 9, 5, tzinfo=UTC)),
        )
    )
    assert fake_pyfuse3.invalidate_calls == []


# ============================================================================
# channel_list_changed → ALL materialized inodes (review P1-F)
# ============================================================================


def test_channel_list_changed_invalidates_all_materialized_inodes(
    client_conn: Connection[TupleRow],
    ops: SlackFuseOpsV2,
    fake_pyfuse3: FakePyfuse3,
) -> None:
    """Review P1-F: a channel-list change must drop EVERY materialized inode —
    channel.md AND thread.md AND directory inodes — not just channel.md. A
    rename rewrites thread.md frontmatter; an archive/block makes the whole
    subtree ENOENT; a membership change reslugs descendants. The old behaviour
    left thread.md serving stale kernel-cached bytes."""
    seed_channel(client_conn, "C1", "general", tier="hot")
    seed_channel(client_conn, "C2", "random", tier="hot")
    meta_inode = ops.inodes.get_or_create("/channels/general/channel.md")
    day1_inode = ops.inodes.get_or_create("/channels/general/2026-06/08/channel.md")
    day2_inode = ops.inodes.get_or_create("/channels/random/2026-06/08/channel.md")
    conv_root_inode = ops.inodes.get_or_create("/channels")
    day_dir_inode = ops.inodes.get_or_create("/channels/general/2026-06/08")
    thread_inode = ops.inodes.get_or_create("/channels/general/2026-06/08/some-thread/thread.md")

    sink = _make_sink(client_conn, fake_pyfuse3)
    sink.channel_list_changed()

    invalidated = set(fake_pyfuse3.invalidate_calls)
    # Every materialized inode is dropped, including thread.md and directories.
    assert {meta_inode, day1_inode, day2_inode, conv_root_inode, day_dir_inode, thread_inode} <= invalidated


def test_channel_archived_invalidates_thread_md_inode_end_to_end(
    client_conn: Connection[TupleRow],
    ops: SlackFuseOpsV2,
    fake_pyfuse3: FakePyfuse3,
) -> None:
    """Review P1-F regression: prime a hot thread.md, then apply
    ``channel_archived`` for its channel and drive the sink as the applier
    would. ``invalidate_inode`` must fire for the thread.md inode — not just
    channel.md — so the kernel stops serving the (now-blocked) thread bytes."""
    seed_channel(client_conn, "C1", "general", tier="hot")
    thread_inode = ops.inodes.get_or_create("/channels/general/2026-06/08/some-thread/thread.md")

    result = apply_event(
        client_conn,
        EventFrame(
            stream="channel-list",
            offset=1,
            kind="channel_archived",
            ts=None,
            payload=_payload(channel_id="C1"),
        ),
    )
    assert result.channel_list_changed is True

    sink = _make_sink(client_conn, fake_pyfuse3)
    # Mirror StreamApplier._fire_invalidations for a channel_list_changed result.
    if result.channel_list_changed:
        sink.channel_list_changed()

    assert thread_inode in fake_pyfuse3.invalidate_calls


# ============================================================================
# Snapshot-delete invalidations end-to-end (review hole 2): a snapshot that
# removes a chunk for a *materialized* file must drop that file's kernel cache.
# ============================================================================


@pytest.mark.trio
async def test_snapshot_delete_invalidates_materialized_day_file_inode(
    client_conn: Connection[TupleRow],
    ops: SlackFuseOpsV2,
    fake_pyfuse3: FakePyfuse3,
) -> None:
    """Review hole 2 (top-level): an empty channel snapshot removes the day's
    only chunk; the materialized day-file inode must be invalidated so the
    kernel stops serving the stale pre-delete bytes (V2 ``fi.keep_cache=True``).

    On the pre-fix tree the empty body short-circuited to a cursor-only advance
    (no delete, no invalidation) AND the delete path returned no refs — so the
    day file's inode was never dropped.
    """
    seed_channel(client_conn, "C1", "general", tier="hot")
    ts = _ts(datetime(2026, 6, 8, 14, 30, tzinfo=UTC))
    seed_chunk(client_conn, "C1", ts, "## 14:30 <@U1>\n\nstale content\n")
    day_path = "/channels/general/2026-06/08/channel.md"
    inode = ops.inodes.get_or_create(day_path)

    sink = _make_sink(client_conn, fake_pyfuse3)

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"")

    async with httpx.AsyncClient(base_url="http://snapshot.test", transport=httpx.MockTransport(handler)) as http:
        await fetch_and_apply_snapshot(
            http,
            client_conn,
            SnapshotRedirect(stream="channel:C1", at_offset=50, url="/streams/X/snapshot?at=50"),
            sink=sink,
        )

    assert inode in fake_pyfuse3.invalidate_calls
    with client_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM chunks WHERE channel_id = 'C1'")
        row = cur.fetchone()
    assert row is not None and int(row[0]) == 0


# NOTE: the thread-side of hole 2 is covered at the projector level by
# tests/projector/test_snapshot_fetch.py::test_snapshot_delete_fires_invalidation_for_removed_rows
# (asserts a deleted ThreadChunkRef reaches the sink — fails pre-fix) composed
# with test_thread_chunk_changed_invalidates_thread_file_inode above (asserts a
# ThreadChunkRef resolves to the thread.md inode). An end-to-end thread test
# can't *isolate* the deleted-ref path: a surviving reply in the same snapshot
# re-upserts and fires thread_chunk_changed for the same inode, while removing
# the last reply drops reply_count to 0 and the thread dir (and its slug
# resolvability) with it. So no thread-only end-to-end case both fails pre-fix
# and resolves an inode — the two-part coverage above is the faithful guard.


# ============================================================================
# End-to-end original race (AC3 #1 / AC6): message-before-user_added.
# ============================================================================


@pytest.mark.trio
async def test_end_to_end_message_before_user_added(
    client_conn: Connection[TupleRow],
    ops: SlackFuseOpsV2,
    fake_pyfuse3: FakePyfuse3,
) -> None:
    """Prime a hot day file holding a ``<@UNEW>`` fallback, then apply
    ``user_added`` and drive the sink: ``invalidate_inode`` fires, and the next
    read renders the resolved name and primes.

    Spec acceptance criterion 6: the originally-broken sequence end-to-end.
    """
    seed_channel(client_conn, "C1", "general", tier="hot")
    set_connection_state(client_conn, last_slurper_health="healthy", last_frame_at_offset_s=1.0)
    mark_stream_caught_up(client_conn, "channel:C1")

    ts = str(_ts(datetime(2026, 6, 8, 14, 30, tzinfo=UTC)))
    # The message arrives BEFORE user_added; its author UNEW is unknown.
    _ = apply_event(
        client_conn,
        EventFrame(
            stream="channel:C1",
            offset=1,
            kind="message",
            ts=ts,
            payload=_payload(type="message", ts=ts, user="UNEW", text="hello there", thread_ts=None),
        ),
    )

    day_path = "/channels/general/2026-06/08/channel.md"
    inode = ops.inodes.get_or_create(day_path)

    # First read: UNEW is unresolved → UID-literal fallback, notify_store SKIPPED
    # (unresolved-fallback invariant), so the inode is NOT primed.
    content_pre = await ops.read(inode, 0, 131072)
    assert b"@UNEW" in content_pre
    assert b"\xe2\x9a\xa0 Content may be stale" not in content_pre  # no trailer
    assert fake_pyfuse3.notify_calls == []
    assert ops.primed_inodes_snapshot == frozenset()

    # user_added(UNEW) arrives. Its same-TX chunk_mentions lookup finds the
    # already-written chunk and surfaces it for invalidation.
    result = apply_event(
        client_conn,
        EventFrame(
            stream="users",
            offset=1,
            kind="user_added",
            ts=None,
            payload=_payload(id="UNEW", name="newbie", profile=_payload(display_name="Alice", real_name="Alice R")),
        ),
    )
    assert ChunkRef("C1", Decimal(ts)) in result.chunks

    # Drive the sink exactly as StreamApplier._fire_invalidations would.
    sink = _make_sink(client_conn, fake_pyfuse3)
    for ref in result.chunks:
        sink.chunk_changed(ref)
    for thread_ref in result.thread_chunks:
        sink.thread_chunk_changed(thread_ref)

    # The day-file inode the kernel cached the fallback into is dropped.
    assert inode in fake_pyfuse3.invalidate_calls

    # Next read renders the resolved display name AND marks the inode primed
    # (clean bytes). 2026-06-24: notify_store was removed from the read path
    # to fix a folio_wait deadlock; ``primed_inodes`` is the surviving signal.
    content_post = await ops.read(inode, 0, 131072)
    assert b"@Alice" in content_post
    assert b"@UNEW" not in content_post
    assert fake_pyfuse3.notify_calls == []
    assert ops.primed_inodes_snapshot == frozenset({inode})
