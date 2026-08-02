"""Trio scheduling tests for the bounded disk-projection coalescer."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest

import slack_fuse.projector.coalescer as coalescer_module
from slack_fuse.config import ClientConfig
from slack_fuse.projector.coalescer import run_coalescer
from slack_fuse.projector.disk_projection import DiskProjection

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow


class _StopTicks(Exception):
    pass


class _FileWritingProjection:
    def __init__(self, root: Path, count: int) -> None:
        self._root = root
        self._dirty = [f"/channels/c-{index}/channel.md" for index in range(count)]
        self.batch_sizes: list[int] = []
        self.bootstrap_calls = 0

    def bootstrap(self, _conn: object) -> list[str]:
        self.bootstrap_calls += 1
        return []

    def flush_dirty(self, limit: int) -> list[str]:
        batch, self._dirty = self._dirty[:limit], self._dirty[limit:]
        self.batch_sizes.append(len(batch))
        for path in batch:
            backing = self._root.joinpath(*Path(path.lstrip("/")).parts)
            backing.parent.mkdir(parents=True, exist_ok=True)
            backing.write_bytes(path.encode())
        return batch


class _RecordingInvalidator:
    def __init__(self) -> None:
        self.paths: list[str] = []

    def path_changed(self, path: str) -> None:
        self.paths.append(path)


@pytest.mark.trio
async def test_coalescer_drains_500_paths_in_three_bounded_ticks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FileWritingProjection(tmp_path, 500)
    invalidator = _RecordingInvalidator()
    sleeps = 0

    async def stop_after_three(_seconds: float) -> None:
        nonlocal sleeps
        await coalescer_module.trio.lowlevel.checkpoint()
        sleeps += 1
        if sleeps == 3:
            raise _StopTicks

    monkeypatch.setattr(coalescer_module.trio, "sleep", stop_after_three)
    with pytest.raises(_StopTicks):
        await run_coalescer(
            cast("DiskProjection", fake),
            cast("Connection[TupleRow]", object()),
            invalidator,
            initial_flush_batch=200,
        )

    assert fake.bootstrap_calls == 1
    assert fake.batch_sizes == [200, 200, 100]
    assert len(list(tmp_path.glob("channels/*/channel.md"))) == 500
    assert len(invalidator.paths) == 500


@pytest.mark.trio
async def test_coalescer_sleeps_five_seconds_between_drains(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FileWritingProjection(Path("/unused"), 0)
    invalidator = _RecordingInvalidator()
    sleep_args: list[float] = []

    async def record_sleep(seconds: float) -> None:
        await coalescer_module.trio.lowlevel.checkpoint()
        sleep_args.append(seconds)
        if len(sleep_args) == 2:
            raise _StopTicks

    monkeypatch.setattr(coalescer_module.trio, "sleep", record_sleep)
    with pytest.raises(_StopTicks):
        await run_coalescer(
            cast("DiskProjection", fake),
            cast("Connection[TupleRow]", object()),
            invalidator,
            tick_s=5.0,
        )

    assert sleep_args == [5.0, 5.0]
    assert fake.batch_sizes == [0, 0]


def test_disk_projection_is_disabled_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SLACK_FUSE_DISK_PROJECTION_ENABLED", raising=False)
    monkeypatch.setenv("SLACK_FUSE_SHARED_SECRET", "test-secret")
    assert not ClientConfig().disk_projection_enabled  # pyright: ignore[reportCallIssue]


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on"])
def test_disk_projection_explicit_opt_in(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv("SLACK_FUSE_SHARED_SECRET", "test-secret")
    monkeypatch.setenv("SLACK_FUSE_DISK_PROJECTION_ENABLED", value)
    assert ClientConfig().disk_projection_enabled  # pyright: ignore[reportCallIssue]
