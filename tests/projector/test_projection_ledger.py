"""Transactional durability tests for the projection-target ledger."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import psycopg
import pytest
import trio
from psycopg import Cursor
from psycopg.rows import TupleRow

import slack_fuse.migrations as client_migrations
import slack_fuse.projector.apply as apply_module
from slack_fuse.migrations.runner import apply_migrations
from slack_fuse.projector.apply import apply_event
from slack_fuse.projector.block_sync import apply_blocked_channel_sync
from slack_fuse.projector.cursor import advance_cursor
from slack_fuse.projector.projection_ledger import TargetKey
from slack_fuse.projector.rerender import rerender_channel
from slack_fuse.projector.snapshot_fetch import SnapshotRedirect, fetch_and_apply_snapshot
from slack_fuse_server.wire.frames import EventFrame
from tests.projector.conftest import ClientConnFactory

_UTC = ZoneInfo("UTC")
_CLIENT_MIGRATIONS_DIR = Path(client_migrations.__file__).parent


def _timestamp(value: datetime) -> Decimal:
    return Decimal(f"{value.timestamp():.6f}")


def _message_frame(channel_id: str, timestamp: Decimal, *, offset: int = 1) -> EventFrame:
    ts = f"{timestamp:.6f}"
    return EventFrame(
        stream=f"channel:{channel_id}",
        offset=offset,
        kind="message",
        payload={
            "type": "message",
            "ts": ts,
            "user": "U0001",
            "text": "ledger test",
            "thread_ts": None,
        },
    )


def _snapshot_body(*timestamps: Decimal) -> bytes:
    return b"\n".join(
        json.dumps({
            "type": "message",
            "ts": f"{timestamp:.6f}",
            "user": "U0001",
            "text": f"snapshot {timestamp}",
            "thread_ts": None,
        }).encode()
        for timestamp in timestamps
    )


def _target_row(
    conn: psycopg.Connection[TupleRow],
    target: TargetKey,
) -> tuple[int, int, str] | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT target_generation, rendered_generation, renderer_version "
            "FROM projection_targets WHERE target_kind = %s "
            "AND channel_id IS NOT DISTINCT FROM %s "
            "AND local_day IS NOT DISTINCT FROM %s "
            "AND thread_ts IS NOT DISTINCT FROM %s",
            (
                target.target_kind,
                target.channel_id,
                target.local_day,
                target.thread_ts,
            ),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return int(row[0]), int(row[1]), str(row[2])


def _cursor(conn: psycopg.Connection[TupleRow], stream: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT applied_offset FROM cursors WHERE stream = %s", (stream,))
        row = cur.fetchone()
    return 0 if row is None else int(row[0])


def _chunk_count(conn: psycopg.Connection[TupleRow], channel_id: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM chunks WHERE channel_id = %s", (channel_id,))
        row = cur.fetchone()
    assert row is not None
    return int(row[0])


def _seed_channel(conn: psycopg.Connection[TupleRow], channel_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO channels ("
            "  channel_id, name, is_im, is_mpim, is_member, is_archived, tier, tier_source, subscribed"
            ") VALUES (%s, %s, FALSE, FALSE, TRUE, FALSE, 'hot', 'auto', TRUE)",
            (channel_id, channel_id.lower()),
        )


def test_apply_event_writes_ledger_targets_in_same_transaction(
    client_conn: psycopg.Connection[TupleRow],
) -> None:
    channel_id = "CLEDGER"
    timestamp = _timestamp(datetime(2026, 8, 4, 12, tzinfo=UTC))
    _seed_channel(client_conn, channel_id)

    apply_event(client_conn, _message_frame(channel_id, timestamp, offset=42), tz=_UTC)

    assert _chunk_count(client_conn, channel_id) == 1
    assert _cursor(client_conn, f"channel:{channel_id}") == 42
    assert _target_row(
        client_conn,
        TargetKey("day", channel_id, date(2026, 8, 4), None),
    ) == (2, 0, "v1")


def test_snapshot_replacement_bumps_targets_for_all_touched_rows(
    client_conn_factory: ClientConnFactory,
) -> None:
    channel_id = "CSNAPSHOTLEDGER"
    stream = f"channel:{channel_id}"
    timestamps = (
        _timestamp(datetime(2026, 8, 4, 1, tzinfo=UTC)),
        _timestamp(datetime(2026, 8, 4, 23, tzinfo=UTC)),
        _timestamp(datetime(2026, 8, 5, 1, tzinfo=UTC)),
    )
    body = _snapshot_body(*timestamps)

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=body)

    conn = client_conn_factory()

    async def run() -> None:
        async with httpx.AsyncClient(
            base_url="http://snapshot.test",
            transport=httpx.MockTransport(handler),
        ) as http:
            await fetch_and_apply_snapshot(
                http,
                conn,
                SnapshotRedirect(stream=stream, at_offset=100, url="/snapshot"),
                tz=_UTC,
            )

    trio.run(run)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT local_day, target_generation FROM projection_targets "
            "WHERE target_kind = 'day' AND channel_id = %s ORDER BY local_day",
            (channel_id,),
        )
        rows = cur.fetchall()
    assert rows == [(date(2026, 8, 4), 2), (date(2026, 8, 5), 2)]


def test_snapshot_refuses_to_replace_content_when_cursor_moved_past_at_offset(
    client_conn_factory: ClientConnFactory,
    caplog: pytest.LogCaptureFixture,
) -> None:
    channel_id = "CSTALESNAPSHOT"
    stream = f"channel:{channel_id}"
    old_ts = _timestamp(datetime(2026, 8, 3, 12, tzinfo=UTC))
    new_ts = _timestamp(datetime(2026, 8, 4, 12, tzinfo=UTC))
    conn = client_conn_factory()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO chunks (channel_id, message_ts, content_md, reply_count) VALUES (%s, %s, 'old', 0)",
            (channel_id, old_ts),
        )
        advance_cursor(cur, stream, 105)

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=_snapshot_body(new_ts))

    caplog.set_level(logging.WARNING, logger="slack_fuse.projector.snapshot_fetch")

    async def run() -> None:
        async with httpx.AsyncClient(
            base_url="http://snapshot.test",
            transport=httpx.MockTransport(handler),
        ) as http:
            result = await fetch_and_apply_snapshot(
                http,
                conn,
                SnapshotRedirect(stream=stream, at_offset=100, url="/snapshot"),
                tz=_UTC,
            )
            assert result.at_offset == 105
            assert result.records_applied == 0

    trio.run(run)

    assert _cursor(conn, stream) == 105
    assert _chunk_count(conn, channel_id) == 1
    with conn.cursor() as cur:
        cur.execute(
            "SELECT content_md FROM chunks WHERE channel_id = %s AND message_ts = %s",
            (channel_id, old_ts),
        )
        assert cur.fetchone() == ("old",)
    assert _target_row(conn, TargetKey("day", channel_id, date(2026, 8, 4), None)) is None
    assert "snapshot refused stale replacement" in caplog.text


def test_rerender_bumps_day_and_thread_targets_without_advancing_cursor(
    client_conn_factory: ClientConnFactory,
) -> None:
    channel_id = "CRERENDERLEDGER"
    stream = f"channel:{channel_id}"
    timestamp = _timestamp(datetime(2026, 8, 4, 12, tzinfo=UTC))
    conn = client_conn_factory()
    with conn.cursor() as cur:
        advance_cursor(cur, stream, 77)

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=_snapshot_body(timestamp))

    with httpx.Client(
        base_url="http://snapshot.test",
        transport=httpx.MockTransport(handler),
    ) as http:
        result = rerender_channel(
            http,
            "http://snapshot.test",
            conn,
            channel_id,
            tz=_UTC,
        )

    assert result.status == "rerendered"
    assert _cursor(conn, stream) == 77
    assert _target_row(
        conn,
        TargetKey("day", channel_id, date(2026, 8, 4), None),
    ) == (2, 0, "v1")
    assert _target_row(
        conn,
        TargetKey("thread", channel_id, date(2026, 8, 4), timestamp),
    ) == (2, 0, "v1")


def test_block_sync_bumps_channel_meta_target_on_block_and_unblock(
    client_conn: psycopg.Connection[TupleRow],
) -> None:
    channel_id = "CBLOCKLEDGER"
    _seed_channel(client_conn, channel_id)
    target = TargetKey("channel-meta", channel_id, None, None)
    layout = TargetKey("layout", None, None, None)

    apply_blocked_channel_sync(client_conn, {channel_id})
    assert _target_row(client_conn, target) == (2, 0, "v1")
    assert _target_row(client_conn, layout) == (2, 0, "v1")

    apply_blocked_channel_sync(client_conn, set())
    assert _target_row(client_conn, target) == (3, 0, "v1")
    assert _target_row(client_conn, layout) == (3, 0, "v1")


def test_block_bumps_layout_singleton(
    client_conn: psycopg.Connection[TupleRow],
) -> None:
    channel_id = "CBLOCKLAYOUT"
    layout = TargetKey("layout", None, None, None)
    _seed_channel(client_conn, channel_id)

    assert _target_row(client_conn, layout) == (1, 0, "pre-ledger")
    apply_blocked_channel_sync(client_conn, {channel_id})
    assert _target_row(client_conn, layout) == (2, 0, "v1")


def test_unblock_bumps_layout_singleton(
    client_conn: psycopg.Connection[TupleRow],
) -> None:
    channel_id = "CUNBLOCKLAYOUT"
    layout = TargetKey("layout", None, None, None)
    _seed_channel(client_conn, channel_id)
    apply_blocked_channel_sync(client_conn, {channel_id})

    assert _target_row(client_conn, layout) == (2, 0, "v1")
    apply_blocked_channel_sync(client_conn, set())
    assert _target_row(client_conn, layout) == (3, 0, "v1")


def test_channel_list_change_bumps_layout_singleton_target(
    client_conn: psycopg.Connection[TupleRow],
) -> None:
    frame = EventFrame(
        stream="channel-list",
        offset=9,
        kind="channel_added",
        payload={"id": "CLAYOUT", "name": "layout", "is_member": True},
    )

    apply_event(client_conn, frame, tz=_UTC)

    assert _target_row(client_conn, TargetKey("layout", None, None, None)) == (2, 0, "v1")


def test_layout_singleton_row_exists_after_migration(
    client_conn: psycopg.Connection[TupleRow],
) -> None:
    assert _target_row(client_conn, TargetKey("layout", None, None, None)) == (
        1,
        0,
        "pre-ledger",
    )


def test_renderer_version_epoch_isolates_pre_ledger_state(
    client_conn: psycopg.Connection[TupleRow],
) -> None:
    target = TargetKey("day", "CSTALEEPoch", date(2026, 8, 4), None)
    with client_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO projection_targets ("
            "  target_kind, channel_id, local_day, thread_ts, renderer_version"
            ") VALUES (%s, %s, %s, %s, 'stale-renderer')",
            (target.target_kind, target.channel_id, target.local_day, target.thread_ts),
        )

    assert apply_migrations(client_conn, _CLIENT_MIGRATIONS_DIR) == []
    assert _target_row(client_conn, target) == (1, 0, "stale-renderer")


def test_dual_write_is_atomic_with_source_data(
    client_conn_factory: ClientConnFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    channel_id = "CATOMICLEDGER"
    stream = f"channel:{channel_id}"
    timestamp = _timestamp(datetime(2026, 8, 4, 12, tzinfo=UTC))
    real_bump_targets = apply_module.bump_targets

    def fail_after_ledger_write(
        cur: Cursor[TupleRow],
        targets: Iterable[TargetKey],
        renderer_version: str,
    ) -> None:
        real_bump_targets(cur, targets, renderer_version)
        msg = "fault injection after ledger write"
        raise RuntimeError(msg)

    monkeypatch.setattr(apply_module, "bump_targets", fail_after_ledger_write)
    apply_conn = client_conn_factory()
    with pytest.raises(RuntimeError, match="fault injection after ledger write"):
        apply_event(apply_conn, _message_frame(channel_id, timestamp, offset=12), tz=_UTC)

    verify_conn = client_conn_factory()
    assert _chunk_count(verify_conn, channel_id) == 0
    assert _cursor(verify_conn, stream) == 0
    assert (
        _target_row(
            verify_conn,
            TargetKey("day", channel_id, date(2026, 8, 4), None),
        )
        is None
    )
