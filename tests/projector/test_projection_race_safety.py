"""Adversarial filesystem races that remain below the ledger protocol."""

from __future__ import annotations

import threading
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import pytest
import trio

import slack_fuse.projector.disk_projection as projection_module
from slack_fuse.fuse_ops_v2 import SlackFuseOpsV2
from slack_fuse.projector.disk_projection import DiskProjection
from slack_fuse.projector.projection_ledger import RENDERER_VERSION, TargetKey
from tests.fuse_v2.conftest import mark_stream_caught_up, seed_channel, seed_chunk, set_connection_state

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow

_TZ = ZoneInfo("UTC")
_DAY = date(2026, 8, 2)
_TS = Decimal("1785661200.000000")
_KEY = TargetKey("day", "C1", _DAY, None)
_LAYOUT = TargetKey("layout", None, None, None)
_PATH = "/channels/general/2026-08/02/channel.md"


def _set_target(
    conn: Connection[TupleRow],
    key: TargetKey,
    target_generation: int,
    rendered_generation: int,
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
            " renderer_version = EXCLUDED.renderer_version",
            (
                key.target_kind,
                key.channel_id,
                key.local_day,
                key.thread_ts,
                target_generation,
                rendered_generation,
                RENDERER_VERSION,
            ),
        )


def _seed_clean_projection(conn: Connection[TupleRow], tmp_path: Path) -> DiskProjection:
    seed_channel(conn, "C1", "general", tier="hot")
    seed_chunk(conn, "C1", _TS, "## 09:00 @bot\n\ncomplete bytes\n")
    set_connection_state(conn, last_slurper_health="healthy", last_frame_at_offset_s=1.0)
    mark_stream_caught_up(conn, "channel:C1", at_offset=10)
    _set_target(conn, _LAYOUT, 1, 1)
    _set_target(conn, _KEY, 1, 0)
    projection = DiskProjection(conn, _TZ, root=tmp_path / "projection")
    projection.mark_target_dirty(_KEY)
    assert projection.flush_dirty(1) == [_PATH]
    return projection


def _ops(conn: Connection[TupleRow], projection: DiskProjection, *, enabled: bool = True) -> SlackFuseOpsV2:
    return SlackFuseOpsV2(
        conn,
        _TZ,
        trio.CapacityLimiter(16),
        disk_projection=projection,
        disk_projection_enabled=enabled,
        trailer_enabled=False,
    )


@pytest.mark.parametrize("open_before_replace", [True, False])
@pytest.mark.trio
async def test_reader_straddling_atomic_replace_sees_only_complete_inode(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    open_before_replace: bool,
) -> None:
    projection = _seed_clean_projection(client_conn, tmp_path)
    backing = projection.path_for(_PATH)
    old_bytes = backing.read_bytes()
    new_bytes = old_bytes.replace(b"complete bytes", b"new complete bytes")
    ops = _ops(client_conn, projection)
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
        assert await trio.to_thread.run_sync(read_started.wait, 5.0)
        projection_module._atomic_write_bytes(backing, new_bytes)  # pyright: ignore[reportPrivateUsage]
        release_read.set()

    expected = old_bytes if open_before_replace else new_bytes
    assert result == [expected]


@pytest.mark.trio
async def test_two_enoent_results_fall_through_to_jit_without_fuse_error(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    projection = _seed_clean_projection(client_conn, tmp_path)
    expected = projection.path_for(_PATH).read_bytes()
    calls: list[str] = []

    def missing_twice(path: str) -> None:
        calls.append(path)
        return None

    monkeypatch.setattr(projection, "read_bytes", missing_twice)
    ops = _ops(client_conn, projection)
    inode = ops.inodes.get_or_create(_PATH)
    content = await ops.read(inode, 0, 131072)

    assert calls == [_PATH, _PATH]
    assert content == expected


@pytest.mark.trio
async def test_feature_disabled_concurrent_reads_never_touch_projection(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    projection = _seed_clean_projection(client_conn, tmp_path)
    ops = _ops(client_conn, projection, enabled=False)

    def forbidden(*_args: object) -> object:
        pytest.fail("disabled feature must not touch disk projection")

    monkeypatch.setattr(projection, "read_bytes", forbidden)
    monkeypatch.setattr(projection, "backing_matches_target", forbidden)
    results: list[bytes] = []

    async def one_reader() -> None:
        inode = ops.inodes.get_or_create(_PATH)
        results.append(await ops.read(inode, 0, 131072))

    async with trio.open_nursery() as nursery:
        for _ in range(10):
            nursery.start_soon(one_reader)

    assert len(results) == 10
    assert all(b"complete bytes" in content for content in results)
