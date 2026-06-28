# pyright: reportPrivateUsage=false
"""Reconnect/restart catchup: the bounded gap-fill that recovers events Slack
dropped while the slurper was down.

Four behaviours are pinned:

1. Gap detection (`should_catchup`) — only downtimes past the threshold trigger
   a catchup; graceful sub-threshold reconnects are skipped.
2. Resume-point resolution (`resolve_since_ts`) — resume from the last seen ts
   when we have one, else from the bounded lookback floor (so an empty channel
   never becomes a full backfill).
3. Per-channel gap-fill (`catchup_channel`) over the fake Slack transport —
   writes deduped message events, idempotent on re-run, honours `since_ts`.
4. The member-channel sweep (`run_catchup_once`) — iterates every channel the
   backfiller yields, resolves each resume point, and counts recovered events.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, cast

import httpx
import psycopg
import pytest
import trio

from slack_fuse.models import Message
from slack_fuse_render import ChannelId
from slack_fuse_server._json import JsonObject
from slack_fuse_server.backfill.api import SlackApiBackfiller, SleepBounds
from slack_fuse_server.slurper.api import SlackAPIError, SlackClient, Validated
from slack_fuse_server.slurper.catchup import (
    CatchupConfig,
    CatchupDeps,
    CatchupResult,
    CatchupTrigger,
    catchup_channel,
    last_seen_ts_by_stream,
    resolve_since_ts,
    run_catchup_once,
    should_catchup,
)
from slack_fuse_server.slurper.offsets import EventRecord, OffsetWriter, write_event
from tests.conftest import make_test_limiters, make_test_writer

if TYPE_CHECKING:
    import psycopg
    from psycopg.rows import TupleRow

_NO_SLEEP = SleepBounds(page_min_s=0.0, page_max_s=0.0, thread_min_s=0.0, thread_max_s=0.0)
_FAST = CatchupConfig(channel_gap_s=0.0, startup_delay_s=0.0)


def _fake_client(http: httpx.Client) -> SlackClient:
    client = SlackClient("xoxp-test")
    client._http = http
    return client


def _seed_message(conn: psycopg.Connection[TupleRow], stream: str, ts: str) -> None:
    write_event(
        conn,
        EventRecord(stream=stream, kind="message", ts=ts, payload={"ts": ts}, dedup=True),
    )


def _events_count(conn: psycopg.Connection[TupleRow], stream: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM events WHERE stream = %s", (stream,))
        row = cur.fetchone()
    assert row is not None
    return int(row[0])


# === 1. Gap detection ===


def test_should_catchup_only_past_threshold() -> None:
    # Graceful Slack-initiated reconnects have tiny gaps — skip.
    assert should_catchup(2.0, threshold_s=300.0) is False
    # Exactly the threshold is not "past" it.
    assert should_catchup(300.0, threshold_s=300.0) is False
    # A real outage crosses it.
    assert should_catchup(301.0, threshold_s=300.0) is True
    assert should_catchup(3600.0, threshold_s=300.0) is True


# === 2. Resume-point resolution ===


def test_resolve_since_ts_resumes_from_last_seen() -> None:
    last_seen = {"channel:C1": 1700000000.5}
    # A channel we've seen messages for resumes from its tip, ignoring the floor.
    since = resolve_since_ts("C1", last_seen, now_epoch=1700100000.0, max_lookback_s=3600.0)
    assert abs(since - 1700000000.5) < 1e-6


def test_resolve_since_ts_falls_back_to_lookback_floor() -> None:
    # No prior message events → bounded floor, never a full-history fetch.
    since = resolve_since_ts("CNEW", {}, now_epoch=1700100000.0, max_lookback_s=3600.0)
    assert abs(since - (1700100000.0 - 3600.0)) < 1e-6


# === 3. last_seen_ts_by_stream ===


def test_last_seen_ts_by_stream_takes_max_per_channel(server_conn: psycopg.Connection[TupleRow]) -> None:
    _seed_message(server_conn, "channel:C1", "100.500000")
    _seed_message(server_conn, "channel:C1", "200.500000")
    _seed_message(server_conn, "channel:C2", "150.000000")
    # Non-message events and non-channel streams must be ignored.
    write_event(server_conn, EventRecord(stream="channel-list", kind="channel_added", ts=None, payload={"id": "C1"}))
    write_event(server_conn, EventRecord(stream="slurper-health", kind="slack_healthy", ts=None, payload={}))

    last_seen = last_seen_ts_by_stream(server_conn)

    assert set(last_seen) == {"channel:C1", "channel:C2"}
    assert abs(last_seen["channel:C1"] - 200.5) < 1e-6
    assert abs(last_seen["channel:C2"] - 150.0) < 1e-6


# === 4. catchup_channel over the fake transport ===


def test_catchup_channel_writes_history_and_thread_events(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    limiter = trio.CapacityLimiter(1)
    writer = make_test_writer(server_conn)
    backfiller = SlackApiBackfiller(_fake_client(fake_slack_http), limiter, _NO_SLEEP)

    async def go() -> int:
        return await catchup_channel(backfiller, writer, ChannelId("C0001"), since_ts=0.0)

    written = trio.run(go)

    # 2 top-level history messages + 1 thread reply (replies[1:]).
    assert written == 3
    assert _events_count(server_conn, "channel:C0001") == 3


def test_catchup_channel_is_idempotent_on_rerun(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    limiter = trio.CapacityLimiter(1)
    writer = make_test_writer(server_conn)
    backfiller = SlackApiBackfiller(_fake_client(fake_slack_http), limiter, _NO_SLEEP)

    async def go() -> int:
        return await catchup_channel(backfiller, writer, ChannelId("C0001"), since_ts=0.0)

    first = trio.run(go)
    second = trio.run(go)

    assert first == 3
    # Re-run: same ts values dedup to no-ops, nothing new recovered.
    assert second == 0
    assert _events_count(server_conn, "channel:C0001") == 3


def test_catchup_channel_since_ts_filters_old_messages(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    limiter = trio.CapacityLimiter(1)
    writer = make_test_writer(server_conn)
    backfiller = SlackApiBackfiller(_fake_client(fake_slack_http), limiter, _NO_SLEEP)

    async def go() -> int:
        # A resume point past every fixture ts → nothing passes the since filter.
        return await catchup_channel(backfiller, writer, ChannelId("C0001"), since_ts=1700000300.0)

    written = trio.run(go)

    assert written == 0
    assert _events_count(server_conn, "channel:C0001") == 0


# === 5. The member-channel sweep ===


class _RecordingBackfiller:
    """Stub backfiller that records the resume ts each channel was swept with
    and yields one synthetic message per channel. No API."""

    def __init__(self, channel_ids: list[str]) -> None:
        self._channel_ids = channel_ids
        self.since_by_channel: dict[str, float | None] = {}

    @property
    def name(self) -> str:
        return "recording"

    async def channels_to_backfill(self) -> AsyncIterator[ChannelId]:
        for cid in self._channel_ids:
            yield ChannelId(cid)

    async def messages_for_channel(
        self,
        channel_id: ChannelId,
        since_ts: float | None = None,
    ) -> AsyncIterator[Validated[Message]]:
        self.since_by_channel[channel_id.value] = since_ts
        # Distinct ts per channel so each lands on its own stream uniquely.
        ts = f"{1800000000 + hash(channel_id.value) % 1000}.000000"
        msg = Message(ts=ts, user="U1", text=f"m-{channel_id.value}")
        yield Validated(raw=cast(JsonObject, msg.model_dump(mode="json")), model=msg)


class _FlakyCatchupWriter:
    def __init__(self, conn: psycopg.Connection[TupleRow], *, failures: int) -> None:
        self.conn = conn
        self.limiter = trio.CapacityLimiter(1)
        self.failures_remaining = failures
        self.calls = 0

    async def write_message_or_corrective(self, _record: EventRecord) -> int | None:
        self.calls += 1
        if self.failures_remaining > 0:
            self.failures_remaining -= 1
            raise psycopg.errors.QueryCanceled("test statement timeout")
        return 1


def _disable_catchup_pg_retry_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    def _zero(_minimum: float, _maximum: float) -> float:
        return 0.0

    monkeypatch.setattr("slack_fuse_server.slurper.catchup.random.uniform", _zero)


def test_catchup_channel_retries_pg_timeout_once_then_continues(
    server_conn: psycopg.Connection[TupleRow],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _disable_catchup_pg_retry_sleep(monkeypatch)
    writer = _FlakyCatchupWriter(server_conn, failures=1)
    backfiller = _RecordingBackfiller(["CRETRY"])

    written = trio.run(
        catchup_channel,
        backfiller,
        cast(OffsetWriter, writer),
        ChannelId("CRETRY"),
        0.0,
    )

    assert writer.calls == 2
    assert written == 1


def test_run_catchup_once_sweeps_every_channel_with_resolved_resume_points(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    # C1 has a tip in the events table → resume from it. CNEW has none → floor.
    _seed_message(server_conn, "channel:C1", "1700000000.000000")
    writer = make_test_writer(server_conn)
    backfiller = _RecordingBackfiller(["C1", "CNEW"])
    deps = CatchupDeps(writer=writer, backfiller=backfiller, config=_FAST, limiters=make_test_limiters())

    async def go() -> CatchupResult:
        return await run_catchup_once(deps, now_epoch=1700100000.0)

    result = trio.run(go)

    assert result.channels == 2
    assert result.events == 2
    assert result.errors == 0
    # C1 resumes from its persisted tip; CNEW falls back to now - lookback.
    c1_since = backfiller.since_by_channel["C1"]
    cnew_since = backfiller.since_by_channel["CNEW"]
    assert c1_since is not None and abs(c1_since - 1700000000.0) < 1e-6
    assert cnew_since is not None and abs(cnew_since - (1700100000.0 - _FAST.max_lookback_s)) < 1e-6


class _FlakyBackfiller(_RecordingBackfiller):
    """Like the recording stub, but one channel raises a Slack API error."""

    def __init__(self, channel_ids: list[str], failing: str) -> None:
        super().__init__(channel_ids)
        self._failing = failing

    async def messages_for_channel(
        self,
        channel_id: ChannelId,
        since_ts: float | None = None,
    ) -> AsyncIterator[Validated[Message]]:
        if channel_id.value == self._failing:
            raise SlackAPIError(f"boom for {channel_id.value}")
        async for item in super().messages_for_channel(channel_id, since_ts):
            yield item


def test_run_catchup_once_isolates_per_channel_errors(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    writer = make_test_writer(server_conn)
    backfiller = _FlakyBackfiller(["CA", "CBAD", "CC"], failing="CBAD")
    deps = CatchupDeps(writer=writer, backfiller=backfiller, config=_FAST, limiters=make_test_limiters())

    async def go() -> CatchupResult:
        return await run_catchup_once(deps, now_epoch=1700100000.0)

    result = trio.run(go)

    # One channel failed; the other two still recovered their events.
    assert result.channels == 3
    assert result.events == 2
    assert result.errors == 1


def test_run_catchup_once_integration_over_fake_transport(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    limiter = trio.CapacityLimiter(1)
    writer = make_test_writer(server_conn)
    backfiller = SlackApiBackfiller(_fake_client(fake_slack_http), limiter, _NO_SLEEP)
    deps = CatchupDeps(writer=writer, backfiller=backfiller, config=_FAST, limiters=make_test_limiters())

    async def go() -> CatchupResult:
        # now close to the fixture timestamps so the lookback floor (now - 1h)
        # sits below them and the fixture history passes the since filter.
        return await run_catchup_once(deps, now_epoch=1700001000.0)

    result = trio.run(go)

    # Fixture conversations.list yields C0001 (member) and D0001 (DM); the fake
    # transport answers conversations.history per-method, so each channel
    # recovers the same 3 events (2 top-level + 1 thread reply).
    assert result.channels == 2
    assert result.errors == 0
    assert result.events == 6
    assert _events_count(server_conn, "channel:C0001") == 3
    assert _events_count(server_conn, "channel:D0001") == 3


# === CatchupTrigger ===


def test_catchup_trigger_coalesces_requests() -> None:
    trigger = CatchupTrigger()
    # First request takes the single buffer slot; a second while it's pending
    # is dropped (the queued run will cover whatever the running one misses).
    assert trigger.request(600.0) is True
    assert trigger.request(700.0) is False


def test_catchup_trigger_runs_startup_catchup(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    writer = make_test_writer(server_conn)
    backfiller = _RecordingBackfiller(["CS1", "CS2"])
    deps = CatchupDeps(writer=writer, backfiller=backfiller, config=_FAST, limiters=make_test_limiters())
    trigger = CatchupTrigger()

    async def go() -> None:
        async with trio.open_nursery() as nursery:
            nursery.start_soon(trigger.consume, deps)
            # consume runs the startup sweep then parks on the channel forever;
            # give it room to finish the sweep, then tear the nursery down.
            await trio.sleep(0.2)
            nursery.cancel_scope.cancel()

    trio.run(go)

    # Startup catchup ran without any request() — both channels were swept.
    assert set(backfiller.since_by_channel) == {"CS1", "CS2"}
