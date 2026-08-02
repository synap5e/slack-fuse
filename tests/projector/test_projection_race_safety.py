"""Deterministic adversarial tests for disk-projection freshness ordering."""

from __future__ import annotations

import functools
import threading
from pathlib import Path
from typing import TYPE_CHECKING, cast
from zoneinfo import ZoneInfo

import pytest
import trio
from trio.testing import MockClock

import slack_fuse.fuse_ops_v2 as fuse_ops_module
import slack_fuse.projector.coalescer as coalescer_module
import slack_fuse.projector.disk_projection as projection_module
from slack_fuse.fuse_ops_v2 import SlackFuseOpsV2, V2InvalidationSink, synchronous_read_for_test
from slack_fuse.models import JsonObject
from slack_fuse.projector.apply import ApplyResult, apply_event
from slack_fuse.projector.coalescer import run_coalescer
from slack_fuse.projector.disk_projection import DiskProjection, OffsetSnapshot
from slack_fuse_server.wire.frames import EventFrame
from tests.fuse_v2.conftest import mark_stream_caught_up, seed_channel, seed_user, set_connection_state

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow

    from tests.projector.conftest import ClientConnFactory


_PATH = "/channels/general/2026-08/02/channel.md"
_READ_SIZE = 131072
_MESSAGE_TS = "1785661200.000000"


class _StopCoalescer(Exception):
    pass


def _event(offset: int, text: str) -> EventFrame:
    payload = cast(
        "JsonObject",
        {
            "type": "message",
            "ts": _MESSAGE_TS,
            "user": "U1",
            "text": text,
        },
    )
    return EventFrame(
        stream="channel:C1",
        offset=offset,
        kind="message",
        ts=_MESSAGE_TS,
        payload=payload,
    )


def _seed_world(conn: Connection[TupleRow]) -> None:
    seed_channel(conn, "C1", "general", tier="hot")
    seed_user(conn, "U1", "alice")
    set_connection_state(conn, last_slurper_health="healthy", last_frame_at_offset_s=1.0)
    mark_stream_caught_up(conn, "channel:C1", at_offset=1000)


def _projection(conn: Connection[TupleRow], tmp_path: Path) -> DiskProjection:
    return DiskProjection(conn, ZoneInfo("UTC"), root=tmp_path / "projection")


def _ops(
    conn: Connection[TupleRow],
    projection: DiskProjection,
    monkeypatch: pytest.MonkeyPatch,
    *,
    enabled: bool,
) -> SlackFuseOpsV2:
    monkeypatch.setenv("SLACK_FUSE_DISK_PROJECTION_ENABLED", "true" if enabled else "false")
    return SlackFuseOpsV2(
        conn,
        ZoneInfo("UTC"),
        trio.CapacityLimiter(16),
        disk_projection=projection,
        trailer_enabled=False,
    )


def _make_clean(projection: DiskProjection, apply_conn: Connection[TupleRow], *, text: str = "offset one") -> bytes:
    _ = apply_event(apply_conn, _event(1, text), projection=projection)
    assert projection.flush_dirty(10) == [_PATH]
    assert projection.is_clean(_PATH)
    return projection.path_for(_PATH).read_bytes()


async def _read(ops: SlackFuseOpsV2) -> bytes:
    inode = ops.inodes.get_or_create(_PATH)
    return await ops.read(inode, 0, _READ_SIZE)


async def _wait_thread(event: threading.Event) -> None:
    reached = await trio.to_thread.run_sync(event.wait, 5.0)
    assert reached, "worker did not reach deterministic race gate"


@pytest.mark.trio
async def test_event_landing_during_write_stays_dirty_and_reader_jits_latest(
    client_conn_factory: ClientConnFactory,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mock_clock: MockClock,
) -> None:
    projection_conn = client_conn_factory()
    apply_conn = client_conn_factory()
    reader_conn = client_conn_factory()
    _seed_world(projection_conn)
    projection = _projection(projection_conn, tmp_path)
    old_bytes = _make_clean(projection, apply_conn)
    projection.mark_dirty(_PATH)

    write_started = threading.Event()
    release_write = threading.Event()
    event_committed = threading.Event()
    check_results: list[bool] = []
    flushed: list[str] = []
    real_atomic_write = projection_module._atomic_write_bytes  # pyright: ignore[reportPrivateUsage]
    real_mark = projection.mark_apply_result
    real_check = projection.check_and_mark_clean_if_no_drift

    def gated_write(path: Path, data: bytes) -> None:
        assert data == old_bytes
        write_started.set()
        assert release_write.wait(5.0)
        real_atomic_write(path, data)

    def signal_committed(result: ApplyResult) -> None:
        # apply_event invokes this only after COMMIT, while still holding the
        # shared invalidation barrier introduced by D3.
        event_committed.set()
        real_mark(result)

    def record_check(path: str, at_offset: OffsetSnapshot) -> bool:
        clean = real_check(path, at_offset)
        check_results.append(clean)
        return clean

    monkeypatch.setattr(projection_module, "_atomic_write_bytes", gated_write)
    monkeypatch.setattr(projection, "mark_apply_result", signal_committed)
    monkeypatch.setattr(projection, "check_and_mark_clean_if_no_drift", record_check)

    async def flush_once() -> None:
        flushed.extend(await trio.to_thread.run_sync(projection.flush_dirty, 1))

    async def apply_fresh_event() -> None:
        apply_second = functools.partial(apply_event, apply_conn, _event(2, "offset two"), projection=projection)
        _ = await trio.to_thread.run_sync(apply_second)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(flush_once)
        await trio.lowlevel.checkpoint()
        await _wait_thread(write_started)
        nursery.start_soon(apply_fresh_event)
        await trio.lowlevel.checkpoint()
        await _wait_thread(event_committed)
        release_write.set()

    assert flushed == [_PATH]
    assert check_results == [False]
    assert projection.path_for(_PATH).read_bytes() == old_bytes
    assert not projection.is_clean(_PATH)

    def forbidden_disk_read(_path: str) -> bytes | None:
        pytest.fail("dirty projection bytes must never be served")

    monkeypatch.setattr(projection, "read_bytes", forbidden_disk_read)
    content = await _read(_ops(reader_conn, projection, monkeypatch, enabled=True))
    assert b"offset two" in content


@pytest.mark.trio
async def test_reader_cannot_pass_clean_gate_between_commit_and_dirty_mark(
    client_conn_factory: ClientConnFactory,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mock_clock: MockClock,
) -> None:
    projection_conn = client_conn_factory()
    apply_conn = client_conn_factory()
    reader_conn = client_conn_factory()
    _seed_world(projection_conn)
    projection = _projection(projection_conn, tmp_path)
    _ = _make_clean(projection, apply_conn)
    ops = _ops(reader_conn, projection, monkeypatch, enabled=True)
    event_committed = threading.Event()
    release_marker = threading.Event()
    reader_finished = threading.Event()
    tier_results: list[bytes | None] = []
    real_mark = projection.mark_apply_result

    def gated_mark(result: ApplyResult) -> None:
        event_committed.set()
        assert release_marker.wait(5.0)
        real_mark(result)

    monkeypatch.setattr(projection, "mark_apply_result", gated_mark)

    async def apply_fresh_event() -> None:
        apply_second = functools.partial(apply_event, apply_conn, _event(2, "after commit"), projection=projection)
        _ = await trio.to_thread.run_sync(apply_second)

    async def tier_read() -> None:
        tier_results.append(
            await trio.to_thread.run_sync(
                ops._read_from_disk_if_clean,  # pyright: ignore[reportPrivateUsage]
                _PATH,
            )
        )
        reader_finished.set()

    async with trio.open_nursery() as nursery:
        nursery.start_soon(apply_fresh_event)
        await trio.lowlevel.checkpoint()
        await _wait_thread(event_committed)
        nursery.start_soon(tier_read)
        await trio.lowlevel.checkpoint()
        assert not reader_finished.is_set()
        release_marker.set()

    assert tier_results == [None]
    assert b"after commit" in await _read(ops)


@pytest.mark.parametrize("open_before_replace", [True, False])
@pytest.mark.trio
async def test_reader_straddling_atomic_replace_sees_only_complete_inode(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mock_clock: MockClock,
    *,
    open_before_replace: bool,
) -> None:
    projection = _projection(client_conn, tmp_path)
    backing = projection.path_for(_PATH)
    backing.parent.mkdir(parents=True)
    old_bytes = b"old-complete\n"
    new_bytes = b"new-complete\n"
    backing.write_bytes(old_bytes)
    ops = _ops(client_conn, projection, monkeypatch, enabled=True)
    read_started = threading.Event()
    release_read = threading.Event()
    result: list[bytes | None] = []

    def straddling_read(_path: str) -> bytes:
        if open_before_replace:
            with backing.open("rb") as handle:
                read_started.set()
                assert release_read.wait(5.0)
                return handle.read()
        read_started.set()
        assert release_read.wait(5.0)
        return backing.read_bytes()

    monkeypatch.setattr(projection, "read_bytes", straddling_read)

    async def tier_read() -> None:
        result.append(
            await trio.to_thread.run_sync(
                ops._read_from_disk_if_clean,  # pyright: ignore[reportPrivateUsage]
                _PATH,
            )
        )

    async with trio.open_nursery() as nursery:
        nursery.start_soon(tier_read)
        await trio.lowlevel.checkpoint()
        await _wait_thread(read_started)
        projection_module._atomic_write_bytes(backing, new_bytes)  # pyright: ignore[reportPrivateUsage]
        release_read.set()

    expected = old_bytes if open_before_replace else new_bytes
    assert result == [expected]
    assert result[0] in {old_bytes, new_bytes}


@pytest.mark.trio
async def test_two_enoent_results_fall_through_to_jit_without_fuse_error(
    client_conn_factory: ClientConnFactory,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mock_clock: MockClock,
) -> None:
    projection_conn = client_conn_factory()
    reader_conn = client_conn_factory()
    _seed_world(projection_conn)
    projection = _projection(projection_conn, tmp_path)
    expected = _make_clean(projection, reader_conn)
    calls: list[str] = []

    def missing_twice(path: str) -> None:
        calls.append(path)
        return None

    monkeypatch.setattr(projection, "read_bytes", missing_twice)
    content = await _read(_ops(reader_conn, projection, monkeypatch, enabled=True))
    await trio.lowlevel.checkpoint()

    assert calls == [_PATH, _PATH]
    assert content == expected


@pytest.mark.trio
async def test_rapid_dirty_flapping_converges_to_last_event(
    client_conn_factory: ClientConnFactory,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mock_clock: MockClock,
) -> None:
    projection_conn = client_conn_factory()
    apply_conn = client_conn_factory()
    _seed_world(projection_conn)
    projection = _projection(projection_conn, tmp_path)
    old_bytes = _make_clean(projection, apply_conn)
    projection.mark_dirty(_PATH)
    write_started = threading.Event()
    release_write = threading.Event()
    check_results: list[bool] = []
    real_atomic_write = projection_module._atomic_write_bytes  # pyright: ignore[reportPrivateUsage]
    real_check = projection.check_and_mark_clean_if_no_drift

    def first_write_gated(path: Path, data: bytes) -> None:
        write_started.set()
        assert release_write.wait(5.0)
        real_atomic_write(path, data)

    def record_check(path: str, at_offset: OffsetSnapshot) -> bool:
        result = real_check(path, at_offset)
        check_results.append(result)
        return result

    def apply_one_hundred() -> None:
        for offset in range(2, 102):
            _ = apply_event(apply_conn, _event(offset, f"offset {offset}"))
            projection.mark_dirty(_PATH)

    monkeypatch.setattr(projection_module, "_atomic_write_bytes", first_write_gated)
    monkeypatch.setattr(projection, "check_and_mark_clean_if_no_drift", record_check)

    async def flush_once() -> None:
        _ = await trio.to_thread.run_sync(projection.flush_dirty, 1)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(flush_once)
        await trio.lowlevel.checkpoint()
        await _wait_thread(write_started)
        await trio.to_thread.run_sync(apply_one_hundred)
        release_write.set()

    assert check_results == [False]
    assert not projection.is_clean(_PATH)
    assert projection.path_for(_PATH).read_bytes() == old_bytes

    monkeypatch.setattr(projection_module, "_atomic_write_bytes", real_atomic_write)
    assert projection.flush_dirty(1) == [_PATH]
    assert projection.is_clean(_PATH)
    jit = synchronous_read_for_test(
        SlackFuseOpsV2(projection_conn, ZoneInfo("UTC"), trio.CapacityLimiter(1), trailer_enabled=False),
        _PATH,
    )
    assert jit is not None
    assert projection.path_for(_PATH).read_bytes() == jit[0]
    assert b"offset 101" in jit[0]


@pytest.mark.trio
async def test_ten_readers_during_flush_never_observe_torn_bytes(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mock_clock: MockClock,
) -> None:
    projection = _projection(client_conn, tmp_path)
    backing = projection.path_for(_PATH)
    backing.parent.mkdir(parents=True)
    old_bytes = b"old-complete\n"
    new_bytes = b"new-complete\n"
    backing.write_bytes(old_bytes)
    ops = _ops(client_conn, projection, monkeypatch, enabled=True)
    release_reads = threading.Event()
    all_readers_waiting = threading.Event()
    count_lock = threading.Lock()
    waiting = 0
    results: list[bytes | None] = []
    result_lock = threading.Lock()
    real_read = projection.read_bytes

    def gated_read(path: str) -> bytes | None:
        nonlocal waiting
        with count_lock:
            waiting += 1
            if waiting == 10:
                all_readers_waiting.set()
        assert release_reads.wait(5.0)
        return real_read(path)

    def render_new(_path: str) -> bytes:
        return new_bytes

    monkeypatch.setattr(projection, "read_bytes", gated_read)
    monkeypatch.setattr(projection, "_render_path", render_new)

    async def one_reader() -> None:
        value = await trio.to_thread.run_sync(
            ops._read_from_disk_if_clean,  # pyright: ignore[reportPrivateUsage]
            _PATH,
        )
        with result_lock:
            results.append(value)

    async with trio.open_nursery() as nursery:
        for _ in range(10):
            nursery.start_soon(one_reader)
        await trio.lowlevel.checkpoint()
        await _wait_thread(all_readers_waiting)
        projection.mark_dirty(_PATH)
        assert await trio.to_thread.run_sync(projection.flush_dirty, 1) == [_PATH]
        release_reads.set()

    assert len(results) == 10
    assert all(result in {old_bytes, new_bytes} for result in results)
    assert all(result not in {b"", b"old-", b"new-"} for result in results)


@pytest.mark.trio
async def test_dirty_to_clean_disk_read_equals_flush_start_jit_bytes(
    client_conn_factory: ClientConnFactory,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mock_clock: MockClock,
) -> None:
    projection_conn = client_conn_factory()
    reader_conn = client_conn_factory()
    _seed_world(projection_conn)
    projection = _projection(projection_conn, tmp_path)
    _ = apply_event(reader_conn, _event(1, "byte equality"), projection=projection)
    rendered_at_start: list[bytes] = []
    real_render = projection._render_path  # pyright: ignore[reportPrivateUsage]

    def capture_render(path: str) -> bytes | None:
        rendered = real_render(path)
        if rendered is not None:
            rendered_at_start.append(rendered)
        return rendered

    monkeypatch.setattr(projection, "_render_path", capture_render)
    assert await trio.to_thread.run_sync(projection.flush_dirty, 1) == [_PATH]
    await trio.lowlevel.checkpoint()
    disk_read = await _read(_ops(reader_conn, projection, monkeypatch, enabled=True))

    assert projection.is_clean(_PATH)
    assert rendered_at_start == [projection.path_for(_PATH).read_bytes()]
    assert disk_read == rendered_at_start[0]


@pytest.mark.trio
async def test_kernel_cache_invalidation_is_last(
    client_conn_factory: ClientConnFactory,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mock_clock: MockClock,
) -> None:
    projection_conn = client_conn_factory()
    apply_conn = client_conn_factory()
    sink_conn = client_conn_factory()
    _seed_world(projection_conn)
    projection = _projection(projection_conn, tmp_path)
    _ = apply_event(apply_conn, _event(1, "ordered invalidate"), projection=projection)
    materializer = SlackFuseOpsV2(sink_conn, ZoneInfo("UTC"), trio.CapacityLimiter(1))
    inode = materializer.inodes.get_or_create(_PATH)
    order: list[str] = []
    real_atomic_write = projection_module._atomic_write_bytes  # pyright: ignore[reportPrivateUsage]
    real_check = projection.check_and_mark_clean_if_no_drift

    def record_write(path: Path, data: bytes) -> None:
        real_atomic_write(path, data)
        order.append("replace")

    def record_clean(path: str, at_offset: OffsetSnapshot) -> bool:
        result = real_check(path, at_offset)
        order.append("clean")
        return result

    def fake_pyfuse3_invalidate(actual_inode: int) -> None:
        assert actual_inode == inode
        assert projection.is_clean(_PATH)
        order.append("invalidate")

    async def stop_after_tick(_seconds: float) -> None:
        await trio.lowlevel.checkpoint()
        raise _StopCoalescer

    def no_bootstrap(_conn: Connection[TupleRow]) -> list[str]:
        return []

    monkeypatch.setattr(projection_module, "_atomic_write_bytes", record_write)
    monkeypatch.setattr(projection, "check_and_mark_clean_if_no_drift", record_clean)
    monkeypatch.setattr(projection, "bootstrap", no_bootstrap)
    monkeypatch.setattr(coalescer_module.trio, "sleep", stop_after_tick)
    monkeypatch.setattr(fuse_ops_module.pyfuse3, "invalidate_inode", fake_pyfuse3_invalidate)
    invalidator = V2InvalidationSink(sink_conn, ZoneInfo("UTC"))

    with pytest.raises(_StopCoalescer):
        await run_coalescer(projection, projection_conn, invalidator, initial_flush_batch=1)

    assert order == ["replace", "clean", "invalidate"]


@pytest.mark.trio
async def test_feature_disabled_concurrent_reads_never_touch_projection(
    client_conn_factory: ClientConnFactory,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mock_clock: MockClock,
) -> None:
    projection_conn = client_conn_factory()
    reader_conn = client_conn_factory()
    _seed_world(projection_conn)
    projection = _projection(projection_conn, tmp_path)
    expected = _make_clean(projection, reader_conn, text="jit only")
    ops = _ops(reader_conn, projection, monkeypatch, enabled=False)

    def forbidden(*_args: object) -> object:
        pytest.fail("disabled feature must not touch the disk projection")

    monkeypatch.setattr(projection, "is_clean", forbidden)
    monkeypatch.setattr(projection, "read_bytes", forbidden)
    monkeypatch.setattr(projection, "flush_dirty", forbidden)
    results: list[bytes] = []

    async def one_reader() -> None:
        results.append(await _read(ops))

    async with trio.open_nursery() as nursery:
        for _ in range(10):
            nursery.start_soon(one_reader)
        await trio.lowlevel.checkpoint()

    assert results == [expected] * 10


@pytest.mark.trio
async def test_flush_dirty_is_single_writer_even_when_path_is_remarked(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mock_clock: MockClock,
) -> None:
    projection = _projection(client_conn, tmp_path)
    projection.mark_dirty(_PATH)
    first_render_started = threading.Event()
    release_first = threading.Event()
    activity_lock = threading.Lock()
    active = 0
    max_active = 0
    renders = 0

    def gated_render(_path: str) -> bytes:
        nonlocal active, max_active, renders
        with activity_lock:
            active += 1
            max_active = max(max_active, active)
            renders += 1
            render_number = renders
        if render_number == 1:
            first_render_started.set()
            assert release_first.wait(5.0)
        with activity_lock:
            active -= 1
        return f"generation-{render_number}\n".encode()

    monkeypatch.setattr(projection, "_render_path", gated_render)
    results: list[list[str]] = []

    async def flush() -> None:
        result = await trio.to_thread.run_sync(projection.flush_dirty, 1)
        results.append(result)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(flush)
        await trio.lowlevel.checkpoint()
        await _wait_thread(first_render_started)
        projection.mark_dirty(_PATH)
        nursery.start_soon(flush)
        await trio.lowlevel.checkpoint()
        release_first.set()

    assert max_active == 1
    assert results == [[_PATH], [_PATH]]
    assert projection.is_clean(_PATH)
    assert projection.path_for(_PATH).read_bytes() == b"generation-2\n"
