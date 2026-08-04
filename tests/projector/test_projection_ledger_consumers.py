"""Correctness and contention tests for projection-ledger consumers."""

from __future__ import annotations

import functools
import logging
import statistics
import threading
import time
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, cast
from zoneinfo import ZoneInfo

import pytest
import trio
from psycopg import Cursor
from psycopg.rows import TupleRow

import slack_fuse.projector.apply as apply_module
import slack_fuse.projector.disk_projection as projection_module
from slack_fuse.fuse_ops_v2 import SlackFuseOpsV2
from slack_fuse.models import JsonObject
from slack_fuse.projector.apply import ApplyResult, apply_event
from slack_fuse.projector.block_sync import apply_blocked_channel_sync
from slack_fuse.projector.disk_projection import DiskProjection
from slack_fuse.projector.pool import ConnectionPool
from slack_fuse.projector.projection_ledger import (
    RENDERER_VERSION,
    TargetKey,
)
from slack_fuse_server.wire.frames import EventFrame
from tests.fuse_v2.conftest import mark_stream_caught_up, seed_channel, seed_chunk, set_connection_state

if TYPE_CHECKING:
    from collections.abc import Iterable

    from psycopg import Connection

    from tests.projector.conftest import ClientConnFactory

_TZ = ZoneInfo("UTC")
_DAY = date(2026, 8, 2)
_TS = Decimal("1785661200.000000")
_PATH = "/channels/general/2026-08/02/channel.md"
_KEY = TargetKey("day", "C1", _DAY, None)
_LAYOUT = TargetKey("layout", None, None, None)
_READ_SIZE = 131072
log = logging.getLogger(__name__)


def _set_target(
    conn: Connection[TupleRow],
    key: TargetKey,
    *,
    target_generation: int,
    rendered_generation: int,
    renderer_version: str = RENDERER_VERSION,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO projection_targets ("
            " target_kind, channel_id, local_day, thread_ts, target_generation, "
            " rendered_generation, renderer_version"
            ") VALUES (%s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT ON CONSTRAINT projection_targets_identity DO UPDATE SET "
            " target_generation = EXCLUDED.target_generation, "
            " rendered_generation = EXCLUDED.rendered_generation, "
            " renderer_version = EXCLUDED.renderer_version, updated_at = now()",
            (
                key.target_kind,
                key.channel_id,
                key.local_day,
                key.thread_ts,
                target_generation,
                rendered_generation,
                renderer_version,
            ),
        )


def _target_row(conn: Connection[TupleRow], key: TargetKey) -> tuple[int, int, str] | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT target_generation, rendered_generation, renderer_version "
            "FROM projection_targets WHERE target_kind = %s "
            "AND channel_id IS NOT DISTINCT FROM %s "
            "AND local_day IS NOT DISTINCT FROM %s "
            "AND thread_ts IS NOT DISTINCT FROM %s",
            (key.target_kind, key.channel_id, key.local_day, key.thread_ts),
        )
        row = cur.fetchone()
    return None if row is None else (int(row[0]), int(row[1]), str(row[2]))


def _seed_world(conn: Connection[TupleRow], *, channel_id: str = "C1", name: str = "general") -> TargetKey:
    seed_channel(conn, channel_id, name, tier="hot")
    seed_chunk(conn, channel_id, _TS, f"## 09:00 @bot\n\nhello from {channel_id}\n")
    set_connection_state(conn, last_slurper_health="healthy", last_frame_at_offset_s=1.0)
    mark_stream_caught_up(conn, f"channel:{channel_id}", at_offset=10)
    key = TargetKey("day", channel_id, _DAY, None)
    _set_target(conn, _LAYOUT, target_generation=1, rendered_generation=1)
    return key


def _clean_projection(conn: Connection[TupleRow], tmp_path: Path) -> DiskProjection:
    projection = DiskProjection(conn, _TZ, root=tmp_path / "projection")
    _set_target(conn, _KEY, target_generation=1, rendered_generation=0)
    projection.mark_target_dirty(_KEY)
    assert projection.flush_dirty(1) == [_PATH]
    assert _target_row(conn, _KEY) == (1, 1, RENDERER_VERSION)
    return projection


def _ops(
    conn: Connection[TupleRow],
    projection: DiskProjection,
    *,
    pool: ConnectionPool | None = None,
) -> SlackFuseOpsV2:
    return SlackFuseOpsV2(
        conn,
        _TZ,
        trio.CapacityLimiter(16),
        pool=pool,
        disk_projection=projection,
        disk_projection_enabled=True,
        trailer_enabled=False,
    )


async def _read(ops: SlackFuseOpsV2, path: str = _PATH) -> bytes:
    inode = ops.inodes.get_or_create(path)
    return await ops.read(inode, 0, _READ_SIZE)


def _message_frame(channel_id: str, offset: int, text: str) -> EventFrame:
    payload = cast(
        "JsonObject",
        {"type": "message", "ts": str(_TS), "user": "U1", "text": text},
    )
    return EventFrame(
        stream=f"channel:{channel_id}",
        offset=offset,
        kind="message",
        ts=str(_TS),
        payload=payload,
    )


def _forbidden_disk_read(_path: str) -> bytes | None:
    pytest.fail("ledger-dirty bytes must not reach the disk tier")


@pytest.mark.trio
async def test_reader_clean_check_uses_ledger_not_dirty_set(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
) -> None:
    _seed_world(client_conn)
    projection = _clean_projection(client_conn, tmp_path)
    projection.mark_target_dirty(_KEY)

    content = await _read(_ops(client_conn, projection))

    assert content == projection.path_for(_PATH).read_bytes()
    assert b"hello from C1" in content


@pytest.mark.trio
async def test_reader_dirty_on_target_generation_greater_than_rendered(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_world(client_conn)
    projection = _clean_projection(client_conn, tmp_path)
    _set_target(client_conn, _KEY, target_generation=2, rendered_generation=1)
    monkeypatch.setattr(projection, "read_bytes", _forbidden_disk_read)

    assert b"hello from C1" in await _read(_ops(client_conn, projection))


@pytest.mark.trio
async def test_reader_dirty_on_renderer_version_mismatch(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_world(client_conn)
    projection = _clean_projection(client_conn, tmp_path)
    _set_target(
        client_conn,
        _KEY,
        target_generation=1,
        rendered_generation=1,
        renderer_version="old-renderer",
    )
    monkeypatch.setattr(projection, "read_bytes", _forbidden_disk_read)

    assert b"hello from C1" in await _read(_ops(client_conn, projection))


@pytest.mark.trio
async def test_reader_dirty_on_missing_target_row(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_world(client_conn)
    projection = _clean_projection(client_conn, tmp_path)
    with client_conn.cursor() as cur:
        cur.execute("DELETE FROM projection_targets WHERE target_kind = 'day' AND channel_id = 'C1'")
    monkeypatch.setattr(projection, "read_bytes", _forbidden_disk_read)

    assert b"hello from C1" in await _read(_ops(client_conn, projection))


@pytest.mark.trio
async def test_reader_dirty_while_layout_singleton_is_pending(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_world(client_conn)
    projection = _clean_projection(client_conn, tmp_path)
    _set_target(client_conn, _LAYOUT, target_generation=2, rendered_generation=1)
    monkeypatch.setattr(projection, "read_bytes", _forbidden_disk_read)

    assert b"hello from C1" in await _read(_ops(client_conn, projection))


@pytest.mark.trio
async def test_event_landing_during_write_stays_dirty_and_reader_jits_latest(
    client_conn_factory: ClientConnFactory,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    projection_conn = client_conn_factory()
    invalidation_conn = client_conn_factory()
    _seed_world(projection_conn)
    projection = DiskProjection(projection_conn, _TZ, root=tmp_path / "projection")
    _set_target(projection_conn, _KEY, target_generation=1, rendered_generation=0)
    projection.mark_target_dirty(_KEY)
    write_started = threading.Event()
    release_write = threading.Event()
    real_write = projection_module._atomic_write_bytes  # pyright: ignore[reportPrivateUsage]

    def gated_write(path: Path, data: bytes) -> None:
        write_started.set()
        assert release_write.wait(5.0)
        real_write(path, data)

    monkeypatch.setattr(projection_module, "_atomic_write_bytes", gated_write)
    flushed: list[str] = []

    async def flush() -> None:
        flushed.extend(await trio.to_thread.run_sync(projection.flush_dirty, 1))

    async with trio.open_nursery() as nursery:
        nursery.start_soon(flush)
        assert await trio.to_thread.run_sync(write_started.wait, 5.0)
        _ = apply_event(
            invalidation_conn,
            _message_frame("C1", 11, "new event during projection write"),
            tz=_TZ,
        )
        release_write.set()

    assert flushed == [_PATH]
    assert _target_row(projection_conn, _KEY) == (2, 0, RENDERER_VERSION)
    assert projection.pending_count == 1
    reader_conn = client_conn_factory()
    assert b"new event during projection write" in await _read(_ops(reader_conn, projection))


def test_coalescer_cas_refuses_to_mark_clean_after_concurrent_invalidation(
    client_conn_factory: ClientConnFactory,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    projection_conn = client_conn_factory()
    invalidation_conn = client_conn_factory()
    _seed_world(projection_conn)
    projection = DiskProjection(projection_conn, _TZ, root=tmp_path / "projection")
    _set_target(projection_conn, _KEY, target_generation=1, rendered_generation=0)
    projection.mark_target_dirty(_KEY)
    real_render = projection._render_target  # pyright: ignore[reportPrivateUsage]

    def invalidate_during_render(key: TargetKey) -> bytes | None:
        rendered = real_render(key)
        with invalidation_conn.transaction(), invalidation_conn.cursor() as cur:
            apply_module.bump_targets(cur, (key,), RENDERER_VERSION)
        return rendered

    monkeypatch.setattr(projection, "_render_target", invalidate_during_render)

    assert projection.flush_dirty(1) == [_PATH]
    assert _target_row(projection_conn, _KEY) == (2, 0, RENDERER_VERSION)
    assert projection.pending_count == 1


def test_startup_epoch_reconciliation_marks_pre_ledger_files_pending(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
) -> None:
    _seed_world(client_conn)
    projection = DiskProjection(client_conn, _TZ, root=tmp_path / "projection")
    backing = projection.path_for(_PATH)
    backing.parent.mkdir(parents=True)
    backing.write_bytes(
        b"---\nchannel: general\nchannel_id: C1\ndate: 2026-08-02\n---\nold bytes\n"
    )
    _set_target(
        client_conn,
        _KEY,
        target_generation=1,
        rendered_generation=1,
        renderer_version="pre-ledger",
    )

    removed, recovered, duration_ms = projection.reconcile_startup()

    assert removed == []
    assert recovered == 1
    assert duration_ms >= 0
    assert _target_row(client_conn, _KEY) == (2, 1, RENDERER_VERSION)
    assert projection.pending_count == 1


@pytest.mark.trio
async def test_disjoint_channel_apply_concurrency(
    client_conn_factory: ClientConnFactory,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    projection_conn = client_conn_factory()
    first_conn = client_conn_factory()
    second_conn = client_conn_factory()
    seed_channel(projection_conn, "CA", "a", tier="hot")
    seed_channel(projection_conn, "CB", "b", tier="hot")
    projection = DiskProjection(projection_conn, _TZ, root=tmp_path / "projection")
    real_bump = apply_module.bump_targets
    rendezvous = threading.Barrier(2)
    active = 0
    max_active = 0
    active_lock = threading.Lock()

    def overlapping_bump(
        cur: Cursor[TupleRow],
        targets: Iterable[TargetKey],
        renderer_version: str,
    ) -> None:
        nonlocal active, max_active
        with active_lock:
            active += 1
            max_active = max(max_active, active)
        rendezvous.wait(timeout=5.0)
        time.sleep(0.05)
        real_bump(cur, targets, renderer_version)
        with active_lock:
            active -= 1

    monkeypatch.setattr(apply_module, "bump_targets", overlapping_bump)

    async def apply_one(conn: Connection[TupleRow], channel_id: str) -> None:
        call = functools.partial(
            apply_event,
            conn,
            _message_frame(channel_id, 1, channel_id),
            tz=_TZ,
            projection=projection,
        )
        _ = await trio.to_thread.run_sync(call)

    started = time.perf_counter()
    async with trio.open_nursery() as nursery:
        nursery.start_soon(apply_one, first_conn, "CA")
        nursery.start_soon(apply_one, second_conn, "CB")
    elapsed_ms = (time.perf_counter() - started) * 1000

    log.info(
        "projector-benchmark op=projection.disjoint_apply samples=2 duration_ms=%.3f max_active=%d",
        elapsed_ms,
        max_active,
    )
    assert max_active == 2
    assert elapsed_ms < 200


@pytest.mark.trio
async def test_reader_cannot_admit_old_bytes_between_commit_and_ledger_write(
    client_conn_factory: ClientConnFactory,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    projection_conn = client_conn_factory()
    apply_conn = client_conn_factory()
    reader_conn = client_conn_factory()
    _seed_world(projection_conn)
    projection = _clean_projection(projection_conn, tmp_path)
    post_commit_mark_entered = threading.Event()
    release_heap_mark = threading.Event()
    real_mark = projection.mark_apply_result

    def delayed_heap_mark(result: ApplyResult) -> None:
        post_commit_mark_entered.set()
        assert release_heap_mark.wait(5.0)
        real_mark(result)

    monkeypatch.setattr(projection, "mark_apply_result", delayed_heap_mark)
    old_disk_reads: list[str] = []
    real_read = projection.read_bytes

    def record_disk_read(path: str) -> bytes | None:
        old_disk_reads.append(path)
        return real_read(path)

    monkeypatch.setattr(projection, "read_bytes", record_disk_read)

    async def apply_new() -> None:
        call = functools.partial(
            apply_event,
            apply_conn,
            _message_frame("C1", 2, "new committed bytes"),
            tz=_TZ,
            projection=projection,
        )
        _ = await trio.to_thread.run_sync(call)

    content: list[bytes] = []
    async with trio.open_nursery() as nursery:
        nursery.start_soon(apply_new)
        assert await trio.to_thread.run_sync(post_commit_mark_entered.wait, 5.0)
        with trio.fail_after(1.0):
            content.append(await _read(_ops(reader_conn, projection)))
        release_heap_mark.set()

    assert b"new committed bytes" in content[0]
    assert old_disk_reads == []


def test_layout_bump_reconciliation_enqueues_affected_channels(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
) -> None:
    _seed_world(client_conn)
    projection = _clean_projection(client_conn, tmp_path)
    old_alias = projection.path_for("/channels/stale-general/2026-08/02/channel.md")
    old_alias.parent.mkdir(parents=True)
    old_alias.write_bytes(projection.path_for(_PATH).read_bytes())
    _set_target(client_conn, _LAYOUT, target_generation=2, rendered_generation=1)

    removed = projection.reconcile_layout()

    assert removed == ["/channels/stale-general/2026-08/02/channel.md"]
    assert not old_alias.exists()
    target_row = _target_row(client_conn, _KEY)
    assert target_row is not None and target_row[0] > target_row[1]
    assert _target_row(client_conn, _LAYOUT) == (2, 2, RENDERER_VERSION)
    assert projection.pending_count >= 1


def test_block_sync_visibility_change_via_ledger_reaches_coalescer(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
) -> None:
    _seed_world(client_conn)
    projection = _clean_projection(client_conn, tmp_path)

    changes = apply_blocked_channel_sync(client_conn, {"C1"})
    removed = projection.reconcile_layout()
    discovered = projection.discover_pending(100)

    assert changes.newly_blocked == frozenset({"C1"})
    assert _PATH in removed
    assert _KEY in discovered
    row = _target_row(client_conn, _KEY)
    assert row is not None and row[0] > row[1]


def test_coalescer_renders_from_stable_key_not_mutable_path(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_world(client_conn)
    seed_channel(client_conn, "C2", "other", tier="hot")
    seed_chunk(client_conn, "C2", _TS, "## 09:00 @bot\n\nwrong channel\n")
    projection = DiskProjection(client_conn, _TZ, root=tmp_path / "projection")
    _set_target(client_conn, _KEY, target_generation=1, rendered_generation=0)
    projection.mark_target_dirty(_KEY)
    post_render_path = "/channels/post-render/2026-08/02/channel.md"
    render_finished = False
    real_render = projection._render_target  # pyright: ignore[reportPrivateUsage]

    def gated_render(key: TargetKey) -> bytes | None:
        nonlocal render_finished
        rendered = real_render(key)
        render_finished = True
        return rendered

    def post_render_path_for(_cur: Cursor[TupleRow], _key: TargetKey, _tz: ZoneInfo) -> str:
        assert render_finished, "mutable path was resolved before stable-key rendering finished"
        return post_render_path

    monkeypatch.setattr(projection, "_render_target", gated_render)
    monkeypatch.setattr(projection_module, "path_for_target", post_render_path_for)

    assert projection.flush_dirty(1) == [post_render_path]
    rendered = projection.path_for(post_render_path).read_bytes()
    assert b"channel_id: C1" in rendered
    assert b"hello from C1" in rendered
    assert b"wrong channel" not in rendered


def test_later_batch_failure_does_not_suppress_earlier_kernel_invalidation(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_key = _seed_world(client_conn)
    second_key = _seed_world(client_conn, channel_id="C2", name="second")
    projection = DiskProjection(client_conn, _TZ, root=tmp_path / "projection")
    _set_target(client_conn, first_key, target_generation=1, rendered_generation=0)
    _set_target(client_conn, second_key, target_generation=1, rendered_generation=0)
    projection.mark_target_dirty(first_key)
    projection.mark_target_dirty(second_key)
    real_write = projection_module._atomic_write_bytes  # pyright: ignore[reportPrivateUsage]
    writes = 0

    def fail_second(path: Path, data: bytes) -> None:
        nonlocal writes
        writes += 1
        if writes == 2:
            raise OSError("second target failed")
        real_write(path, data)

    monkeypatch.setattr(projection_module, "_atomic_write_bytes", fail_second)
    invalidated: list[str] = []

    flushed = projection.flush_dirty(2, invalidated.append)

    assert flushed == ["/channels/general/2026-08/02/channel.md"]
    assert invalidated == flushed
    assert projection.pending_count == 1


def test_replace_is_invalidated_before_cas_failure(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_world(client_conn)
    projection = DiskProjection(client_conn, _TZ, root=tmp_path / "projection")
    _set_target(client_conn, _KEY, target_generation=1, rendered_generation=0)
    projection.mark_target_dirty(_KEY)
    invalidated: list[str] = []

    def fail_cas(*_args: object, **_kwargs: object) -> bool:
        raise OSError("database failed after atomic replace")

    monkeypatch.setattr(projection_module, "mark_target_rendered", fail_cas)

    assert projection.flush_dirty(1, invalidated.append) == []
    assert projection.path_for(_PATH).is_file()
    assert invalidated == [_PATH]
    assert _target_row(client_conn, _KEY) == (1, 0, RENDERER_VERSION)
    assert projection.pending_count == 1


def test_thread_slug_drift_removes_and_invalidates_old_alias_before_cas(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed_channel(client_conn, "C1", "general", tier="hot")
    _set_target(client_conn, _LAYOUT, target_generation=1, rendered_generation=1)
    key = TargetKey("thread", "C1", _DAY, _TS)
    _set_target(client_conn, key, target_generation=1, rendered_generation=0)
    projection = DiskProjection(client_conn, _TZ, root=tmp_path / "projection")
    old_path = "/channels/general/2026-08/02/old-slug/thread.md"
    new_path = "/channels/general/2026-08/02/new-slug/thread.md"
    current_path = old_path
    rendered = (
        b"---\nchannel: general\nchannel_id: C1\ndate: 2026-08-02\n"
        b"thread_ts: 1785661200.000000\n---\nthread bytes\n"
    )

    def resolve_current(_cur: Cursor[TupleRow], _key: TargetKey, _tz: ZoneInfo) -> str:
        return current_path

    def render_thread(_key: TargetKey) -> bytes:
        return rendered

    monkeypatch.setattr(projection_module, "path_for_target", resolve_current)
    monkeypatch.setattr(projection, "_render_target", render_thread)
    invalidated: list[str] = []
    projection.mark_target_dirty(key)
    assert projection.flush_dirty(1, invalidated.append) == [old_path]

    current_path = new_path
    _set_target(client_conn, key, target_generation=2, rendered_generation=1)
    projection.mark_target_dirty(key)
    assert projection.flush_dirty(1, invalidated.append) == [new_path]

    assert not projection.path_for(old_path).exists()
    assert projection.path_for(new_path).read_bytes() == rendered
    assert invalidated == [old_path, new_path, old_path]
    assert _target_row(client_conn, key) == (2, 2, RENDERER_VERSION)


def test_layout_cleanup_retries_failed_kernel_invalidation_before_marking_clean(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
) -> None:
    _seed_world(client_conn)
    projection = _clean_projection(client_conn, tmp_path)
    stale_path = "/channels/stale-general/2026-08/02/channel.md"
    stale_alias = projection.path_for(stale_path)
    stale_alias.parent.mkdir(parents=True)
    stale_alias.write_bytes(projection.path_for(_PATH).read_bytes())
    _set_target(client_conn, _LAYOUT, target_generation=2, rendered_generation=1)
    calls = 0

    def fail_once(path: str) -> None:
        nonlocal calls
        calls += 1
        assert path == stale_path
        if calls == 1:
            raise OSError("transient kernel invalidation failure")

    with pytest.raises(OSError, match="transient kernel invalidation failure"):
        projection.reconcile_layout(invalidate_path=fail_once)

    assert not stale_alias.exists()
    assert _target_row(client_conn, _LAYOUT) == (2, 1, RENDERER_VERSION)

    assert projection.reconcile_layout(invalidate_path=fail_once) == []
    assert calls == 2
    assert _target_row(client_conn, _LAYOUT) == (2, 2, RENDERER_VERSION)


@pytest.mark.trio
async def test_rapid_generation_flapping_converges_to_last_event(
    client_conn_factory: ClientConnFactory,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    projection_conn = client_conn_factory()
    apply_conn = client_conn_factory()
    _seed_world(projection_conn)
    projection = _clean_projection(projection_conn, tmp_path)
    write_started = threading.Event()
    release_write = threading.Event()
    real_write = projection_module._atomic_write_bytes  # pyright: ignore[reportPrivateUsage]

    def gated_write(path: Path, data: bytes) -> None:
        write_started.set()
        assert release_write.wait(5.0)
        real_write(path, data)

    monkeypatch.setattr(projection_module, "_atomic_write_bytes", gated_write)
    _ = apply_event(apply_conn, _message_frame("C1", 11, "offset 11"), tz=_TZ)
    projection.mark_target_dirty(_KEY)

    async def first_flush() -> None:
        _ = await trio.to_thread.run_sync(projection.flush_dirty, 1)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(first_flush)
        assert await trio.to_thread.run_sync(write_started.wait, 5.0)
        for offset in range(12, 112):
            _ = apply_event(
                apply_conn,
                _message_frame("C1", offset, f"offset {offset}"),
                tz=_TZ,
            )
        release_write.set()

    row = _target_row(projection_conn, _KEY)
    assert row is not None and row[0] > row[1]
    monkeypatch.setattr(projection_module, "_atomic_write_bytes", real_write)
    assert projection.flush_dirty(1) == [_PATH]
    row = _target_row(projection_conn, _KEY)
    assert row is not None and row[0] == row[1]
    assert b"offset 111" in projection.path_for(_PATH).read_bytes()


@pytest.mark.trio
async def test_clean_disk_read_equals_bytes_rendered_for_completed_generation(
    client_conn_factory: ClientConnFactory,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    projection_conn = client_conn_factory()
    reader_conn = client_conn_factory()
    _seed_world(projection_conn)
    projection = DiskProjection(projection_conn, _TZ, root=tmp_path / "projection")
    _set_target(projection_conn, _KEY, target_generation=1, rendered_generation=0)
    projection.mark_target_dirty(_KEY)
    rendered_at_start: list[bytes] = []
    real_render = projection._render_target  # pyright: ignore[reportPrivateUsage]

    def capture_render(key: TargetKey) -> bytes | None:
        rendered = real_render(key)
        if rendered is not None:
            rendered_at_start.append(rendered)
        return rendered

    monkeypatch.setattr(projection, "_render_target", capture_render)
    assert projection.flush_dirty(1) == [_PATH]

    disk_read = await _read(_ops(reader_conn, projection))
    assert rendered_at_start == [projection.path_for(_PATH).read_bytes()]
    assert disk_read == rendered_at_start[0]


def test_kernel_invalidation_precedes_ledger_completion(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_world(client_conn)
    projection = DiskProjection(client_conn, _TZ, root=tmp_path / "projection")
    _set_target(client_conn, _KEY, target_generation=1, rendered_generation=0)
    projection.mark_target_dirty(_KEY)
    order: list[str] = []
    real_write = projection_module._atomic_write_bytes  # pyright: ignore[reportPrivateUsage]
    real_mark = projection_module.mark_target_rendered

    def record_write(path: Path, data: bytes) -> None:
        real_write(path, data)
        order.append("replace")

    def record_invalidation(path: str) -> None:
        assert path == _PATH
        assert _target_row(client_conn, _KEY) == (1, 0, RENDERER_VERSION)
        order.append("invalidate")

    def record_cas(
        cur: Cursor[TupleRow],
        key: TargetKey,
        rendered_generation: int,
        expected_renderer_version: str,
    ) -> bool:
        order.append("cas")
        return real_mark(cur, key, rendered_generation, expected_renderer_version)

    monkeypatch.setattr(projection_module, "_atomic_write_bytes", record_write)
    monkeypatch.setattr(projection_module, "mark_target_rendered", record_cas)

    assert projection.flush_dirty(1, record_invalidation) == [_PATH]
    assert order == ["replace", "invalidate", "cas"]


@pytest.mark.trio
async def test_flush_dirty_is_single_writer_when_target_is_remarked(
    client_conn_factory: ClientConnFactory,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    projection_conn = client_conn_factory()
    invalidation_conn = client_conn_factory()
    _seed_world(projection_conn)
    projection = DiskProjection(projection_conn, _TZ, root=tmp_path / "projection")
    _set_target(projection_conn, _KEY, target_generation=1, rendered_generation=0)
    projection.mark_target_dirty(_KEY)
    first_render_started = threading.Event()
    release_first = threading.Event()
    activity_lock = threading.Lock()
    real_render = projection._render_target  # pyright: ignore[reportPrivateUsage]
    active = 0
    max_active = 0
    renders = 0

    def gated_render(key: TargetKey) -> bytes | None:
        nonlocal active, max_active, renders
        with activity_lock:
            active += 1
            max_active = max(max_active, active)
            renders += 1
            render_number = renders
        if render_number == 1:
            first_render_started.set()
            assert release_first.wait(5.0)
        rendered = real_render(key)
        with activity_lock:
            active -= 1
        return rendered

    monkeypatch.setattr(projection, "_render_target", gated_render)
    results: list[list[str]] = []

    async def flush() -> None:
        results.append(await trio.to_thread.run_sync(projection.flush_dirty, 1))

    async with trio.open_nursery() as nursery:
        nursery.start_soon(flush)
        assert await trio.to_thread.run_sync(first_render_started.wait, 5.0)
        with invalidation_conn.transaction(), invalidation_conn.cursor() as cur:
            apply_module.bump_targets(cur, (_KEY,), RENDERER_VERSION)
        projection.mark_target_dirty(_KEY)
        nursery.start_soon(flush)
        release_first.set()

    assert max_active == 1
    assert results == [[_PATH], [_PATH]]
    assert _target_row(projection_conn, _KEY) == (2, 2, RENDERER_VERSION)


@pytest.mark.trio
async def test_reader_clean_check_median_latency_under_five_ms_for_100_channels(
    client_conn_factory: ClientConnFactory,
    tmp_path: Path,
) -> None:
    inode_conn = client_conn_factory()
    projection_conn = client_conn_factory()
    _set_target(inode_conn, _LAYOUT, target_generation=1, rendered_generation=1)
    paths: list[str] = []
    for index in range(100):
        channel_id = f"CLAT{index:03d}"
        name = f"latency-{index:03d}"
        seed_channel(inode_conn, channel_id, name, tier="hot")
        _set_target(
            inode_conn,
            TargetKey("channel-meta", channel_id, None, None),
            target_generation=1,
            rendered_generation=1,
        )
        path = f"/channels/{name}/channel.md"
        paths.append(path)
    projection = DiskProjection(projection_conn, _TZ, root=tmp_path / "projection")
    pool = ConnectionPool(client_conn_factory, max_size=4)
    ops = _ops(inode_conn, projection, pool=pool)
    for index, path in enumerate(paths):
        backing = projection.path_for(path)
        backing.parent.mkdir(parents=True, exist_ok=True)
        backing.write_bytes(
            f"---\nchannel: latency-{index:03d}\nchannel_id: CLAT{index:03d}\n---\n".encode()
        )
    latencies_ms: list[float] = []
    try:
        for path in paths:
            started = time.perf_counter()
            content = await _read(ops, path)
            latencies_ms.append((time.perf_counter() - started) * 1000)
            assert b"channel_id: CLAT" in content
    finally:
        await pool.aclose()

    median_ms = statistics.median(latencies_ms)
    log.info(
        "projector-benchmark op=projection.full_fuse_read samples=100 median_ms=%.3f",
        median_ms,
    )
    assert median_ms < 5.0
