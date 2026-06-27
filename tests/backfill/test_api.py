# pyright: reportPrivateUsage=false
"""Backfill source + driver.

`SlackApiBackfiller` is exercised over the fake Slack transport (history +
thread expansion). `backfill_channel` is driven against an in-test stub
`Backfiller` to pin the threshold / dedup / health-event behaviour without
needing the API at all.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import cast

import httpx
import psycopg
import trio
from psycopg.rows import TupleRow

from slack_fuse.models import Message
from slack_fuse_render import ChannelId
from slack_fuse_server._json import JsonObject
from slack_fuse_server.backfill.api import BackfillContext, SlackApiBackfiller, SleepBounds, backfill_channel
from slack_fuse_server.slurper.api import SlackClient, Validated
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
        async for wrapped in backfiller.messages_for_channel(ChannelId("C0001")):
            out.append(wrapped.model)
        return out

    messages = trio.run(collect)
    # 2 top-level (fixture) + 1 thread reply (replies[1:]).
    by_ts = {m.ts for m in messages}
    assert by_ts == {"1700000000.000100", "1700000100.000200", "1700000200.000300"}


def test_thread_reply_raw_is_lossless_not_model_dump(fake_slack_http: httpx.Client) -> None:
    """Pin the 2026-06-27 promotion: thread replies must persist the
    actual wire dict (with all fields, including ones the ``Message``
    model doesn't declare), not a ``Message.model_dump`` round-trip
    that drops anything not declared on the model.

    Before the promotion, ``_expand_threads`` bridged via
    ``Validated(raw=msg.model_dump(...), model=msg)`` — so any field
    the model lacked (notably ``attachments`` for bot/app-unfurl posts)
    was silently dropped at persist time. This test exercises the fake
    transport's ``conversations.replies`` fixture which carries the
    real Slack reply shape; the raw should include the wire ``type``
    field even though the ``Message`` model doesn't declare it.
    """
    backfiller = SlackApiBackfiller(_fake_client(fake_slack_http), trio.CapacityLimiter(1), _NO_SLEEP)

    async def collect_thread_reply() -> Validated[Message] | None:
        async for wrapped in backfiller.messages_for_channel(ChannelId("C0001")):
            if wrapped.model.ts == "1700000200.000300":
                return wrapped
        return None

    reply = trio.run(collect_thread_reply)
    assert reply is not None
    # The wire response includes ``type: "message"`` which the ``Message``
    # model doesn't declare. A ``model_dump`` bridge would drop it; raw
    # preserves it.
    assert reply.raw.get("type") == "message"
    # Model fields still round-trip correctly.
    assert reply.model.user == "U0001"
    assert reply.model.text == "You're most welcome."


def test_get_replies_preserves_attachments_lossless() -> None:
    """The motivating production shape: a thread-reply whose content lives
    in ``attachments`` (Linear unfurls, GitHub alerts, Datadog events…).
    ``SlackClient.get_replies`` must preserve the raw wire dict so the
    backfill persistence site captures attachment data — including fields
    the ``Attachment`` model doesn't declare.

    Drives the client directly against a stubbed ``httpx.Client`` so we
    control the exact response shape; the wider fake transport's static
    fixture doesn't carry attachments.
    """
    bot_reply: dict[str, object] = {
        "type": "message",
        "ts": "1700000300.000400",
        "user": "U_BOT",
        "bot_id": "B_LINEAR",
        "text": "",
        "thread_ts": "1700000100.000200",
        "attachments": [
            {
                "fallback": "FE-740 Bug: …",
                "from_url": "https://linear.app/comfyorg/issue/FE-740",
                "is_app_unfurl": True,
                # Fields the ``Attachment`` model does NOT declare —
                # a ``model_dump`` bridge would silently drop these.
                "footer_icon": "https://example/footer.png",
                "color": "#5E6AD2",
            },
        ],
    }
    response_body: dict[str, object] = {
        "ok": True,
        "messages": [
            {
                "type": "message",
                "ts": "1700000100.000200",
                "user": "U0001",
                "text": "parent",
                "thread_ts": "1700000100.000200",
                "reply_count": 1,
            },
            bot_reply,
        ],
        "has_more": False,
        "response_metadata": {"next_cursor": ""},
    }

    def _handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=response_body)

    client = SlackClient("xoxp-test")
    client._http = httpx.Client(transport=httpx.MockTransport(_handler))  # pyright: ignore[reportPrivateUsage]

    replies = client.get_replies("C0001", "1700000100.000200")
    # [parent, bot_reply]
    assert len(replies) == 2
    bot = replies[1]

    raw_atts = bot.raw.get("attachments")
    assert isinstance(raw_atts, list) and len(raw_atts) == 1
    raw_att = raw_atts[0]
    assert isinstance(raw_att, dict)
    # Fields the ``Attachment`` model declares survive…
    assert raw_att.get("fallback") == "FE-740 Bug: …"
    assert raw_att.get("from_url") == "https://linear.app/comfyorg/issue/FE-740"
    # …and so do fields it doesn't declare. This is the whole point of
    # raw-persistence — projections can read these later without
    # re-ingesting.
    assert raw_att.get("footer_icon") == "https://example/footer.png"
    assert raw_att.get("color") == "#5E6AD2"


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
    ) -> AsyncIterator[Validated[Message]]:
        for i in range(self._count):
            ts = f"{1000 + i}.{i:06d}"
            msg = Message(ts=ts, user="U1", text=f"m{i}")
            yield Validated(raw=cast(JsonObject, msg.model_dump(mode="json")), model=msg)


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
    # warn_at triggers a per-channel BACKFILL_WARN_LARGE (not SLACK_DEGRADED —
    # one channel hitting its size cap is observability, not a global
    # ingestion-health signal; see BACKLOG entry on health hysteresis).
    assert _health_kinds(server_conn) == ["backfill_started", "backfill_warn_large", "backfill_aborted"]


def _health_rows(conn: psycopg.Connection[TupleRow]) -> list[tuple[str, object]]:
    with conn.cursor() as cur:
        cur.execute("SELECT kind, payload FROM health_log ORDER BY id")
        return [(str(r[0]), r[1]) for r in cur.fetchall()]


def test_backfill_channel_emits_progress_every_n_messages(server_conn: psycopg.Connection[TupleRow]) -> None:
    writer = OffsetWriter(server_conn, trio.CapacityLimiter(1))
    health = HealthEmitter(writer)
    ctx = BackfillContext(writer=writer, health=health, warn_at=1000, abort_at=20000, progress_every=2)

    result = trio.run(backfill_channel, _StubBackfiller(5), ChannelId("CP"), ctx)

    assert (result.messages, result.aborted) == (5, False)
    rows = _health_rows(server_conn)
    progress = [payload for kind, payload in rows if kind == "backfill_progress"]
    # 5 messages, progress every 2 → emitted at the 2nd and 4th message.
    assert progress == [
        {"channel_id": "CP", "messages_so_far": 2},
        {"channel_id": "CP", "messages_so_far": 4},
    ]
    assert [kind for kind, _ in rows] == [
        "backfill_started",
        "backfill_progress",
        "backfill_progress",
        "backfill_completed",
    ]


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
