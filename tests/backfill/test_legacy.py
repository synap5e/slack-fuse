# pyright: reportPrivateUsage=false
"""Legacy cache backfill source.

Exercises the legacy-disk-cache reader itself plus one integration run through
`backfill_channel`, proving payload conformance + idempotent dedup writes.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import psycopg
import trio
from psycopg.rows import TupleRow

from slack_fuse.models import Message
from slack_fuse_render import ChannelId
from slack_fuse_server.backfill.api import BackfillContext, backfill_channel
from slack_fuse_server.backfill.legacy import LegacyCacheBackfiller
from slack_fuse_server.slurper.__main__ import _build_parser
from slack_fuse_server.slurper.health import HealthEmitter
from slack_fuse_server.wire.frames import EventFrame
from tests.conftest import make_test_limiters, make_test_writer


def _write_day(cache_dir: Path, channel_id: str, day: str, messages: list[Message]) -> None:
    d = cache_dir / "messages" / channel_id
    d.mkdir(parents=True, exist_ok=True)
    payload = [m.model_dump(mode="json") for m in messages]
    (d / f"{day}.json").write_text(json.dumps(payload))


async def _collect_channels(backfiller: LegacyCacheBackfiller) -> list[str]:
    out: list[str] = []
    async for channel_id in backfiller.channels_to_backfill():
        out.append(channel_id.value)
    return out


async def _collect_messages(
    backfiller: LegacyCacheBackfiller,
    channel_id: str,
    since_ts: float | None = None,
) -> list[Message]:
    out: list[Message] = []
    async for wrapped in backfiller.messages_for_channel(ChannelId(channel_id), since_ts=since_ts):
        out.append(wrapped.model)
    return out


def _event_payloads(conn: psycopg.Connection[TupleRow], stream: str) -> list[object]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT offset_in_stream, kind, ts, payload FROM events WHERE stream = %s ORDER BY offset_in_stream",
            (stream,),
        )
        rows = cur.fetchall()

    payloads: list[object] = []
    for row in rows:
        offset = int(row[0])
        kind = str(row[1])
        ts = None if row[2] is None else str(row[2])
        payload = cast("object", row[3])
        assert isinstance(payload, dict)
        msg = Message.model_validate(payload)
        payload_json = msg.model_dump(mode="json")
        EventFrame(stream=stream, offset=offset, kind=kind, ts=ts, payload=payload_json)
        payloads.append(payload_json)
    return payloads


def _health_kinds(conn: psycopg.Connection[TupleRow]) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT kind FROM health_log ORDER BY id")
        return [str(r[0]) for r in cur.fetchall()]


def test_channels_to_backfill_only_yields_dirs_with_content(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"

    _write_day(cache_dir, "C_HAS", "2026-06-07", [Message(ts="100.000001", user="U1", text="x")])

    empty = cache_dir / "messages" / "C_EMPTY"
    empty.mkdir(parents=True, exist_ok=True)
    (empty / "2026-06-07.json").write_text("[]")

    garbage = cache_dir / "messages" / "C_GARBAGE"
    garbage.mkdir(parents=True, exist_ok=True)
    (garbage / "note.txt").write_text("no day cache here")

    channels = trio.run(_collect_channels, LegacyCacheBackfiller(cache_dir))
    assert channels == ["C_HAS"]


def test_messages_for_channel_order_and_since_filter(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"
    _write_day(
        cache_dir,
        "C1",
        "2026-06-06",
        [
            Message(ts="100.000001", user="U1", text="d1-a"),
            Message(ts="100.000002", user="U1", text="d1-b"),
        ],
    )
    _write_day(
        cache_dir,
        "C1",
        "2026-06-07",
        [
            Message(ts="101.000001", user="U1", text="d2-a"),
            Message(ts="101.000002", user="U1", text="d2-b"),
        ],
    )
    # Invalid day json is ignored, but does not break iteration.
    invalid = cache_dir / "messages" / "C1" / "2026-06-08.json"
    invalid.write_text("{not-json")

    backfiller = LegacyCacheBackfiller(cache_dir)
    all_messages = trio.run(_collect_messages, backfiller, "C1")
    assert [m.ts for m in all_messages] == [
        "100.000001",
        "100.000002",
        "101.000001",
        "101.000002",
    ]

    filtered = trio.run(_collect_messages, backfiller, "C1", 100.000001)
    assert [m.ts for m in filtered] == ["100.000002", "101.000001", "101.000002"]


def test_legacy_backfill_channel_is_idempotent_and_payloads_validate(
    server_conn: psycopg.Connection[TupleRow],
    tmp_path: Path,
) -> None:
    cache_dir = tmp_path / "cache"
    _write_day(
        cache_dir,
        "CLEG",
        "2026-06-07",
        [
            Message(ts="1700000000.000100", user="U1", text="first"),
            Message(ts="1700000100.000200", user="U2", text="second"),
            Message(ts="1700000200.000300", user="U3", text="third"),
        ],
    )

    backfiller = LegacyCacheBackfiller(cache_dir, limiter=trio.CapacityLimiter(1))
    writer = make_test_writer(server_conn)
    health = HealthEmitter(writer)
    ctx = BackfillContext(writer=writer, health=health, limiters=make_test_limiters(), warn_at=1000, abort_at=20000)

    first = trio.run(backfill_channel, backfiller, ChannelId("CLEG"), ctx)
    second = trio.run(backfill_channel, backfiller, ChannelId("CLEG"), ctx)

    assert first.events_written == 3
    assert second.events_written == 0

    payloads = _event_payloads(server_conn, "channel:CLEG")
    assert len(payloads) == 3
    assert [Message.model_validate(p).ts for p in payloads] == [
        "1700000000.000100",
        "1700000100.000200",
        "1700000200.000300",
    ]
    assert _health_kinds(server_conn) == [
        "backfill_started",
        "backfill_completed",
        "backfill_started",
        "backfill_completed",
    ]


def test_backfill_parser_accepts_legacy_cache_source() -> None:
    parser = _build_parser()
    args = parser.parse_args(["backfill", "C12345", "--source", "legacy-cache"])
    assert args.command == "backfill"
    assert args.source == "legacy-cache"
