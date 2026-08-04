"""DB-backed correctness tests for the coalesced disk projection."""

from __future__ import annotations

import functools
import os
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, cast
from zoneinfo import ZoneInfo

import pytest
import trio

import slack_fuse.projector.disk_projection as projection_module
from slack_fuse.fuse_ops_v2 import SlackFuseOpsV2, synchronous_read_for_test
from slack_fuse.models import JsonObject
from slack_fuse.projector.apply import apply_event as _apply_event
from slack_fuse.projector.disk_projection import DiskProjection
from slack_fuse_server.wire.frames import EventFrame
from tests.fuse_v2.conftest import seed_channel, seed_chunk, seed_thread_chunk, seed_user

apply_event = functools.partial(_apply_event, tz=ZoneInfo("UTC"))

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow


def _ts(value: datetime) -> Decimal:
    return Decimal(str(value.timestamp()))


def _projection(
    conn: Connection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> DiskProjection:
    monkeypatch.setattr(projection_module, "PROJECTION_ROOT", tmp_path / "projection")
    return DiskProjection(conn, ZoneInfo("UTC"))


def _seed_day(conn: Connection[TupleRow]) -> tuple[Decimal, str]:
    seed_channel(conn, "C-GEN", "general", tier="hot")
    seed_user(conn, "UALICE", "alice")
    timestamp = _ts(datetime(2026, 8, 2, 9, 30, tzinfo=UTC))
    structural = "## 09:30 <@UALICE>\n\nHello <@UALICE>\n"
    seed_chunk(conn, "C-GEN", timestamp, structural, mentioned_user_ids=["UALICE"])
    return timestamp, structural


def test_writes_right_path_and_resolved_bytes(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _timestamp, _structural = _seed_day(client_conn)
    projection = _projection(client_conn, tmp_path, monkeypatch)
    fuse_path = "/channels/general/2026-08/02/channel.md"

    projection.mark_dirty(fuse_path)
    assert projection.flush_dirty(10) == [fuse_path]

    backing = tmp_path / "projection/channels/general/2026-08/02/channel.md"
    assert projection.path_for(fuse_path) == backing
    assert backing.read_bytes() == (
        b"---\nchannel: general\nchannel_id: C-GEN\ndate: 2026-08-02\n---\n"
        b"## 09:30 @alice\n\nHello @alice\n"
    )
    assert projection.is_clean(fuse_path)


def test_atomic_replace_failure_leaves_no_visible_file_and_requeues(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_day(client_conn)
    projection = _projection(client_conn, tmp_path, monkeypatch)
    fuse_path = "/channels/general/2026-08/02/channel.md"
    projection.mark_dirty(fuse_path)
    real_replace = os.replace

    def fail_replace(_source: str | bytes | os.PathLike[str] | os.PathLike[bytes], _dest: object) -> None:
        raise OSError("simulated rename crash")

    monkeypatch.setattr(projection_module.os, "replace", fail_replace)
    with pytest.raises(OSError, match="simulated rename crash"):
        projection.flush_dirty(1)

    backing = projection.path_for(fuse_path)
    assert not backing.exists()
    assert backing.with_suffix(".md.tmp").exists()
    assert not projection.is_clean(fuse_path)

    monkeypatch.setattr(projection_module.os, "replace", real_replace)
    assert projection.flush_dirty(1) == [fuse_path]
    assert backing.is_file()
    assert projection.is_clean(fuse_path)


def test_bootstrap_marks_today_for_hot_channels_only(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    today = date(2026, 8, 2)
    timestamp = _ts(datetime(2026, 8, 2, 10, 0, tzinfo=UTC))
    for index in range(3):
        channel_id = f"CHOT{index}"
        seed_channel(client_conn, channel_id, f"hot-{index}", tier="hot")
        seed_chunk(client_conn, channel_id, timestamp, f"## 10:00 @bot\n\nhot {index}\n")
    seed_channel(client_conn, "CHIDDEN", "hidden-one", tier="hidden")
    seed_chunk(client_conn, "CHIDDEN", timestamp, "## 10:00 @bot\n\nhidden\n")

    projection = _projection(client_conn, tmp_path, monkeypatch)
    marked = projection.bootstrap(today=today)

    assert all("hidden-one" not in path for path in marked)
    for index in range(3):
        assert f"/channels/hot-{index}/2026-08/02/channel.md" in marked
    assert projection.pending_count == 6  # channel metadata + today's file per hot channel


def test_bootstrap_marks_thread_paths_with_canonical_dedup_slug(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    timestamp = _ts(datetime(2026, 8, 2, 10, 0, tzinfo=UTC))
    reply_timestamp = _ts(datetime(2026, 8, 2, 10, 1, tzinfo=UTC))
    seed_channel(client_conn, "CTHREAD", "threads", tier="hot")
    seed_chunk(
        client_conn,
        "CTHREAD",
        timestamp,
        "## 10:00 @bot\n\nProjection design\n\n> Thread: 1 replies\n",
        reply_count=1,
    )
    seed_thread_chunk(client_conn, "CTHREAD", timestamp, reply_timestamp, "reply", "## 10:01 @bot\n\nReply\n")
    projection = _projection(client_conn, tmp_path, monkeypatch)

    marked = projection.bootstrap(today=date(2026, 8, 2))

    thread_path = "/channels/threads/2026-08/02/projection-design/thread.md"
    assert thread_path in marked
    assert len(projection.flush_dirty(10)) == 3
    assert projection.path_for(thread_path).read_bytes() == (
        b"---\nchannel: threads\nchannel_id: CTHREAD\n"
        b'thread_ts: "1785664800.000000"\nreply_count: 1\ndate: 2026-08-02\n---\n'
        b"## 10:00 @bot\n\nProjection design\n\n> Thread: 1 replies\n\n"
        b"## 10:01 @bot\n\nReply\n"
    )


def test_apply_event_marks_corresponding_day_dirty_after_commit(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed_channel(client_conn, "CAPPLY", "apply-integration", tier="hot")
    projection = _projection(client_conn, tmp_path, monkeypatch)
    payload = cast(
        "JsonObject",
        {
            "type": "message",
            "ts": "1785661200.000000",
            "user": "UAPPLY",
            "text": "committed bytes",
        },
    )
    frame = EventFrame(
        stream="channel:CAPPLY",
        offset=1,
        kind="message",
        ts="1785661200.000000",
        payload=payload,
    )

    _ = apply_event(client_conn, frame, projection=projection)

    assert projection.flush_dirty(1) == ["/channels/apply-integration/2026-08/02/channel.md"]


def test_projection_bytes_equal_jit_base_render(
    client_conn: Connection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_day(client_conn)
    projection = _projection(client_conn, tmp_path, monkeypatch)
    fuse_path = "/channels/general/2026-08/02/channel.md"
    projection.mark_dirty(fuse_path)
    _ = projection.flush_dirty(1)

    ops = SlackFuseOpsV2(
        client_conn,
        ZoneInfo("UTC"),
        trio.CapacityLimiter(1),
        trailer_enabled=False,
    )
    jit = synchronous_read_for_test(ops, fuse_path)

    assert jit is not None
    assert projection.path_for(fuse_path).read_bytes() == jit[0]
