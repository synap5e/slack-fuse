# pyright: reportPrivateUsage=false
"""Backfill source + driver.

`SlackApiBackfiller` is exercised over the fake Slack transport (history +
thread expansion). `backfill_channel` is driven against an in-test stub
`Backfiller` to pin the threshold / dedup / health-event behaviour without
needing the API at all.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import psycopg
import trio
from psycopg.rows import TupleRow

from slack_fuse.models import Message
from slack_fuse_render import ChannelId
from slack_fuse_server.backfill.api import BackfillContext, SlackApiBackfiller, SleepBounds, backfill_channel
from slack_fuse_server.slurper.api import SlackClient
from slack_fuse_server.slurper.health import HealthEmitter
from slack_fuse_server.slurper.offsets import OffsetWriter

_NO_SLEEP = SleepBounds(page_min_s=0.0, page_max_s=0.0, thread_min_s=0.0, thread_max_s=0.0)


def _fake_client(http: httpx.Client) -> SlackClient:
    client = SlackClient("xoxp-test")
    client._http = http
    return client


# === Source: SlackApiBackfiller over the fake transport ===


def test_messages_for_channel_yields_history_and_thread_replies(fake_slack_http: httpx.Client) -> None:
    backfiller = SlackApiBackfiller(_fake_client(fake_slack_http), trio.CapacityLimiter(1), _NO_SLEEP)

    async def collect() -> list[Message]:
        out: list[Message] = []
        async for msg in backfiller.messages_for_channel(ChannelId("C0001")):
            out.append(msg)
        return out

    messages = trio.run(collect)
    # 2 top-level (fixture) + 1 thread reply (replies[1:]).
    by_ts = {m.ts for m in messages}
    assert by_ts == {"1700000000.000100", "1700000100.000200", "1700000200.000300"}


# === Driver: backfill_channel against a stub Backfiller ===


class _StubBackfiller:
    """Yields `count` synthetic messages with distinct ts. No API."""

    def __init__(self, count: int) -> None:
        self._count = count

    @property
    def name(self) -> str:
        return "stub"

    async def channels_to_backfill(self) -> AsyncIterator[ChannelId]:
        return
        yield  # pragma: no cover — present only to make this an async generator

    async def messages_for_channel(
        self,
        channel_id: ChannelId,
        since_ts: float | None = None,
    ) -> AsyncIterator[Message]:
        for i in range(self._count):
            ts = f"{1000 + i}.{i:06d}"
            yield Message(ts=ts, user="U1", text=f"m{i}")


def _events_count(conn: psycopg.Connection[TupleRow], stream: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM events WHERE stream = %s", (stream,))
        row = cur.fetchone()
    assert row is not None
    return int(row[0])


def _health_kinds(conn: psycopg.Connection[TupleRow]) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT kind FROM health_log ORDER BY id")
        return [str(r[0]) for r in cur.fetchall()]


def test_backfill_channel_writes_events_and_emits_health(server_conn: psycopg.Connection[TupleRow]) -> None:
    writer = OffsetWriter(server_conn, trio.CapacityLimiter(1))
    health = HealthEmitter(writer)
    ctx = BackfillContext(writer=writer, health=health, warn_at=1000, abort_at=20000)

    result = trio.run(backfill_channel, _StubBackfiller(5), ChannelId("CX"), ctx)

    assert (result.messages, result.events_written, result.aborted) == (5, 5, False)
    assert _events_count(server_conn, "channel:CX") == 5
    assert _health_kinds(server_conn) == ["backfill_started", "backfill_completed"]


def test_backfill_channel_aborts_at_threshold(server_conn: psycopg.Connection[TupleRow]) -> None:
    writer = OffsetWriter(server_conn, trio.CapacityLimiter(1))
    health = HealthEmitter(writer)
    ctx = BackfillContext(writer=writer, health=health, warn_at=2, abort_at=3)

    result = trio.run(backfill_channel, _StubBackfiller(100), ChannelId("CBIG"), ctx)

    assert result.aborted is True
    assert result.abort_reason is not None and str(result.abort_reason) == "exceeded_default_limit"
    # Stops after abort_at messages — only the truncated head is written.
    assert result.messages == 3
    assert _events_count(server_conn, "channel:CBIG") == 3
    assert _health_kinds(server_conn) == ["backfill_started", "slack_degraded", "backfill_aborted"]


def test_backfill_channel_is_idempotent_on_rerun(server_conn: psycopg.Connection[TupleRow]) -> None:
    writer = OffsetWriter(server_conn, trio.CapacityLimiter(1))
    health = HealthEmitter(writer)
    ctx = BackfillContext(writer=writer, health=health, warn_at=1000, abort_at=20000)

    first = trio.run(backfill_channel, _StubBackfiller(4), ChannelId("CY"), ctx)
    second = trio.run(backfill_channel, _StubBackfiller(4), ChannelId("CY"), ctx)

    assert first.events_written == 4
    # Re-run: same ts values dedup to no-ops, no new rows.
    assert second.messages == 4
    assert second.events_written == 0
    assert _events_count(server_conn, "channel:CY") == 4
