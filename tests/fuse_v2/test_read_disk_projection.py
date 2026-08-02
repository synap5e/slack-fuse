"""Read-side tier tests for the coalesced disk projection."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pyfuse3
import pytest
import trio

from slack_fuse.fuse_ops_v2 import SlackFuseOpsV2
from slack_fuse.projector.disk_projection import DiskProjection
from slack_fuse.projector.trailer import TrailerDecision
from tests.fuse_v2.conftest import mark_stream_caught_up, seed_channel, seed_chunk, seed_user, set_connection_state

if TYPE_CHECKING:
    from collections.abc import Callable

    from psycopg import Connection
    from psycopg.rows import TupleRow


_PATH = "/channels/general/2026-08/02/channel.md"
_READ_SIZE = 131072

ReadDecision = tuple[bytes, bool, bool, TrailerDecision] | None


def _ts(value: datetime) -> Decimal:
    return Decimal(str(value.timestamp()))


def _seed_day(
    conn: Connection[TupleRow],
    *,
    health: str = "healthy",
    seed_alice: bool = True,
) -> None:
    seed_channel(conn, "C1", "general", tier="hot")
    if seed_alice:
        seed_user(conn, "UALICE", "alice")
    seed_chunk(
        conn,
        "C1",
        _ts(datetime(2026, 8, 2, 9, 30, tzinfo=UTC)),
        "## 09:30 <@UALICE>\n\nHello <@UALICE>\n",
        mentioned_user_ids=["UALICE"],
    )
    set_connection_state(conn, last_slurper_health=health, last_frame_at_offset_s=1.0)
    mark_stream_caught_up(conn, "channel:C1", at_offset=10)


def _projection(conn: Connection[TupleRow], tmp_path: Path) -> DiskProjection:
    projection = DiskProjection(conn, ZoneInfo("UTC"), root=tmp_path / "projection")
    projection.mark_dirty(_PATH)
    assert projection.flush_dirty(1) == [_PATH]
    return projection


def _ops(
    conn: Connection[TupleRow],
    projection: DiskProjection | None,
    *,
    now_fn: Callable[[], datetime] | None = None,
    disk_projection_enabled: bool = True,
) -> SlackFuseOpsV2:
    effective_now_fn = now_fn if now_fn is not None else lambda: datetime.now(UTC)
    return SlackFuseOpsV2(
        conn,
        ZoneInfo("UTC"),
        trio.CapacityLimiter(1),
        disk_projection=projection,
        disk_projection_enabled=disk_projection_enabled,
        now_fn=effective_now_fn,
    )


def _spy_jit(
    ops: SlackFuseOpsV2,
    monkeypatch: pytest.MonkeyPatch,
) -> list[str]:
    calls: list[str] = []
    original = ops._resolve_decision  # pyright: ignore[reportPrivateUsage]

    def spy(path: str, now: datetime | None = None) -> ReadDecision:
        calls.append(path)
        return original(path, now)

    monkeypatch.setattr(ops, "_resolve_decision", spy)
    return calls


async def _read(ops: SlackFuseOpsV2) -> bytes:
    inode = ops.inodes.get_or_create(_PATH)
    return await ops.read(inode, 0, _READ_SIZE)


@pytest.mark.trio
async def test_disabled_flag_uses_jit_even_when_projection_file_exists(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_day(client_conn)
    projection = _projection(client_conn, tmp_path)
    ops = _ops(client_conn, projection, disk_projection_enabled=False)
    jit_calls = _spy_jit(ops, monkeypatch)

    content = await _read(ops)

    assert content == projection.path_for(_PATH).read_bytes()
    assert jit_calls == [_PATH]


@pytest.mark.trio
async def test_enabled_clean_path_serves_disk_without_jit(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_day(client_conn)
    projection = _projection(client_conn, tmp_path)
    expected = projection.path_for(_PATH).read_bytes()
    ops = _ops(client_conn, projection)
    jit_calls = _spy_jit(ops, monkeypatch)

    content = await _read(ops)

    assert content == expected
    assert jit_calls == []


@pytest.mark.trio
async def test_enabled_dirty_path_uses_jit_without_reading_disk(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_day(client_conn)
    projection = _projection(client_conn, tmp_path)
    projection.mark_dirty(_PATH)
    disk_read_calls: list[str] = []
    original_read = projection.read_bytes

    def spy_disk_read(path: str) -> bytes | None:
        disk_read_calls.append(path)
        return original_read(path)

    monkeypatch.setattr(projection, "read_bytes", spy_disk_read)
    ops = _ops(client_conn, projection)
    jit_calls = _spy_jit(ops, monkeypatch)

    _ = await _read(ops)

    assert jit_calls == [_PATH]
    assert disk_read_calls == []


@pytest.mark.trio
async def test_enabled_clean_missing_file_retries_then_warns_and_uses_jit(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _seed_day(client_conn)
    projection = _projection(client_conn, tmp_path)
    disk_read_calls: list[str] = []

    def missing_read(path: str) -> None:
        disk_read_calls.append(path)
        return None

    def observed_clean(_path: str) -> bool:
        return True

    # Model the post-is_clean disappearance boundary: the backing file was
    # observed during the clean check, then both bounded reads miss it.
    monkeypatch.setattr(projection, "is_clean", observed_clean)
    monkeypatch.setattr(projection, "read_bytes", missing_read)
    ops = _ops(client_conn, projection)
    jit_calls = _spy_jit(ops, monkeypatch)

    with caplog.at_level(logging.WARNING, logger="slack_fuse.fuse_ops_v2"):
        _ = await _read(ops)

    assert disk_read_calls == [_PATH, _PATH]
    assert jit_calls == [_PATH]
    assert "clean disk projection path is missing" in caplog.text


@pytest.mark.trio
async def test_disk_and_jit_reads_are_byte_equal_with_frozen_live_trailer(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
) -> None:
    _seed_day(client_conn, health="disconnected")
    projection = _projection(client_conn, tmp_path)
    frozen_now = datetime.now(UTC)

    disk_ops = _ops(client_conn, projection, now_fn=lambda: frozen_now)
    disk_content = await _read(disk_ops)

    jit_ops = _ops(client_conn, projection, now_fn=lambda: frozen_now, disk_projection_enabled=False)
    jit_content = await _read(jit_ops)

    assert b"socket-mode disconnected" in disk_content
    assert disk_content == jit_content


@pytest.mark.trio
async def test_getattr_clean_size_comes_from_disk_stat(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_day(client_conn)
    projection = _projection(client_conn, tmp_path)
    ops = _ops(client_conn, projection)
    jit_calls = _spy_jit(ops, monkeypatch)
    inode = ops.inodes.get_or_create(_PATH)

    attr = await ops.getattr(inode, MagicMock(spec=pyfuse3.RequestContext))

    assert attr.st_size == projection.path_for(_PATH).stat().st_size
    assert jit_calls == []


@pytest.mark.trio
async def test_getattr_includes_live_trailer_without_jit_render(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_day(client_conn, health="disconnected")
    projection = _projection(client_conn, tmp_path)
    frozen_now = datetime.now(UTC)
    ops = _ops(client_conn, projection, now_fn=lambda: frozen_now)
    jit_calls = _spy_jit(ops, monkeypatch)
    inode = ops.inodes.get_or_create(_PATH)

    attr = await ops.getattr(inode, MagicMock(spec=pyfuse3.RequestContext))
    content = await ops.read(inode, 0, _READ_SIZE)

    assert attr.st_size == len(content)
    assert attr.st_size > projection.path_for(_PATH).stat().st_size
    assert jit_calls == []


@pytest.mark.trio
async def test_clean_read_retries_once_when_first_disk_read_misses(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_day(client_conn)
    projection = _projection(client_conn, tmp_path)
    expected = projection.path_for(_PATH).read_bytes()
    responses: list[bytes | None] = [None, expected]
    disk_read_calls: list[str] = []

    def racing_read(path: str) -> bytes | None:
        disk_read_calls.append(path)
        return responses.pop(0)

    monkeypatch.setattr(projection, "read_bytes", racing_read)
    ops = _ops(client_conn, projection)
    jit_calls = _spy_jit(ops, monkeypatch)

    content = await _read(ops)

    assert content == expected
    assert disk_read_calls == [_PATH, _PATH]
    assert jit_calls == []


@pytest.mark.trio
async def test_disk_fallback_bytes_preserve_kernel_cache_gate(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_day(client_conn, seed_alice=False)
    projection = _projection(client_conn, tmp_path)
    ops = _ops(client_conn, projection)

    content = await _read(ops)

    assert b"@UALICE" in content
    assert ops.primed_inodes_snapshot == frozenset()
