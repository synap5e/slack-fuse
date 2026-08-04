"""Live-cache invalidation for server-triggered channel blocks."""

from __future__ import annotations

import functools
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import httpx
import pytest
import trio

import slack_fuse.projector.block_sync as block_sync_module
from slack_fuse.fuse_ops_v2 import SlackFuseOpsV2, V2InvalidationSink, synchronous_read_for_test
from slack_fuse.projector.disk_projection import DiskProjection
from slack_fuse.projector.projection_ledger import RENDERER_VERSION, TargetKey, is_target_clean
from tests.fuse_v2.conftest import seed_channel, seed_chunk

if TYPE_CHECKING:
    from pathlib import Path

    from slack_fuse.projector.reconnecting_conn import TupleConnection
    from tests.projector.conftest import ClientConnFactory


_PATH = "/channels/general/2026-08/02/channel.md"
_CHANNEL_ID = "CBLOCK"
_TARGET = TargetKey("day", _CHANNEL_ID, date(2026, 8, 2), None)


@dataclass(slots=True)
class _RecordingVisibilitySink:
    delegate: V2InvalidationSink
    channel_list_changes: int = 0

    def channel_list_changed(self) -> None:
        self.channel_list_changes += 1
        self.delegate.channel_list_changed()


@pytest.mark.trio
async def test_live_cached_file_is_invalidated_and_projection_removed_on_block(
    client_conn_factory: ClientConnFactory,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    projection_conn = client_conn_factory()
    block_conn = client_conn_factory()
    seed_channel(projection_conn, _CHANNEL_ID, "general", tier="hot")
    message_ts = Decimal(str(datetime(2026, 8, 2, 9, 30, tzinfo=UTC).timestamp()))
    seed_chunk(projection_conn, _CHANNEL_ID, message_ts, "## 09:30 @alice\n\nCached secret\n")

    projection = DiskProjection(projection_conn, ZoneInfo("UTC"), root=tmp_path / "projection")
    _removed, _recovered, _duration_ms = projection.reconcile_startup()
    assert projection.reconcile_layout() == []
    _ = projection.flush_dirty(100)
    projection.mark_dirty(_PATH)
    assert projection.flush_dirty(1) == [_PATH]
    assert projection.path_for(_PATH).is_file()

    ops = SlackFuseOpsV2(
        projection_conn,
        ZoneInfo("UTC"),
        trio.CapacityLimiter(1),
        disk_projection=projection,
        trailer_enabled=False,
    )
    inode = ops.inodes.get_or_create(_PATH)
    read = synchronous_read_for_test(ops, _PATH)
    assert read is not None
    assert b"Cached secret" in read[0]

    invalidated_inodes: list[int] = []
    sink = _RecordingVisibilitySink(
        V2InvalidationSink(
            projection_conn,
            ZoneInfo("UTC"),
            invalidate_inode=invalidated_inodes.append,
        )
    )

    def fake_get_blocked_channels(
        _http_client: httpx.Client,
        _base_http_url: str,
        *,
        shared_secret: str | None = None,
    ) -> tuple[int, dict[str, object]]:
        _ = shared_secret
        return 200, {"blocked": [{"channel_id": _CHANNEL_ID}]}

    monkeypatch.setattr(block_sync_module, "get_blocked_channels", fake_get_blocked_channels)
    observed_changes: list[block_sync_module.VisibilityChanges] = []
    real_apply = block_sync_module.apply_blocked_channel_sync

    def recording_apply(conn: TupleConnection, blocked_ids: set[str]) -> block_sync_module.VisibilityChanges:
        changes = real_apply(conn, blocked_ids)
        observed_changes.append(changes)
        return changes

    monkeypatch.setattr(block_sync_module, "apply_blocked_channel_sync", recording_apply)
    callback_ids: list[frozenset[str]] = []
    callback_finished = trio.Event()

    async def on_newly_blocked(ids: frozenset[str]) -> None:
        callback_ids.append(ids)

        def invalidate_visibility() -> None:
            sink.channel_list_changed()

        await trio.to_thread.run_sync(invalidate_visibility)
        callback_finished.set()

    http_client = httpx.Client()
    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            functools.partial(
                block_sync_module.sync_blocked_channels_periodically,
                lambda: http_client,
                "http://server.invalid",
                lambda: block_conn,
                on_newly_blocked=on_newly_blocked,
            )
        )
        await callback_finished.wait()
        nursery.cancel_scope.cancel()

    assert observed_changes[0].newly_blocked == frozenset({_CHANNEL_ID})
    assert callback_ids == [frozenset({_CHANNEL_ID})]
    assert sink.channel_list_changes == 1
    assert inode in invalidated_inodes
    with projection_conn.cursor() as cur:
        assert not is_target_clean(cur, _TARGET, RENDERER_VERSION)
    removed = projection.reconcile_layout()
    assert _PATH in removed
    assert "/channels/general/channel.md" in removed
    assert projection.discover_pending(100)
    _ = projection.flush_dirty(100)
    assert not projection.path_for(_PATH).exists()
