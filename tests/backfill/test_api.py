# pyright: reportPrivateUsage=false
"""Backfill source + driver.

`SlackApiBackfiller` is exercised over the fake Slack transport (history +
thread expansion). `backfill_channel` is driven against an in-test stub
`Backfiller` to pin the threshold / dedup / health-event behaviour without
needing the API at all.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from typing import cast

import httpx
import psycopg
import pytest
import trio
from psycopg.conninfo import make_conninfo
from psycopg.rows import TupleRow

from slack_fuse.models import Message
from slack_fuse_render import ChannelId
from slack_fuse_server._json import JsonObject
from slack_fuse_server.backfill import api as backfill_api
from slack_fuse_server.backfill.api import (
    BackfillContext,
    SlackApiBackfiller,
    SleepBounds,
    _write_batch_with_retry,
    backfill_channel,
)
from slack_fuse_server.backfill.types import BackfillAbortReason, MessageBatch, MessageBatchOrigin
from slack_fuse_server.slurper.api import ChannelNotFoundError, RateLimitedError, SlackClient, Validated
from slack_fuse_server.slurper.health import HealthEmitter, HealthKind
from slack_fuse_server.slurper.offsets import EventRecord, OffsetWriter, write_event
from slack_fuse_server.wire.tail import EventTailer
from tests.conftest import ServerConnFactory, make_test_limiters, make_test_writer

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


def test_messages_pages_for_channel_yields_one_batch_per_slack_response(fake_slack_http: httpx.Client) -> None:
    backfiller = SlackApiBackfiller(_fake_client(fake_slack_http), trio.CapacityLimiter(1), _NO_SLEEP)

    async def collect() -> list[MessageBatch]:
        out: list[MessageBatch] = []
        async for batch in backfiller.messages_pages_for_channel(ChannelId("C0001")):
            out.append(batch)
        return out

    batches = trio.run(collect)

    assert [batch.kind for batch in batches] == ["history_page", "replies_page"]
    # The replies batch leads with the corrective parent (Slack's current
    # thread metadata), atomic with the replies it justifies.
    assert [len(batch.records) for batch in batches] == [2, 2]
    assert [record.ts for record in batches[0].records] == ["1700000100.000200", "1700000000.000100"]
    assert [record.ts for record in batches[1].records] == ["1700000100.000200", "1700000200.000300"]
    parent_record = batches[1].records[0]
    assert parent_record.source is not None
    assert parent_record.source["producer"] == "backfill-corrective-parent"
    assert batches[1].origin.thread_ts == "1700000100.000200"


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


def test_channels_to_backfill_skips_blocked_ids(fake_slack_http: httpx.Client) -> None:
    async def _blocked_channel_ids() -> set[str]:
        await trio.lowlevel.checkpoint()
        return {"C0001"}

    backfiller = SlackApiBackfiller(
        _fake_client(fake_slack_http),
        trio.CapacityLimiter(1),
        _NO_SLEEP,
        blocked_channel_ids=_blocked_channel_ids,
    )

    async def collect() -> list[str]:
        out: list[str] = []
        async for channel_id in backfiller.channels_to_backfill():
            out.append(channel_id.value)
        return out

    assert trio.run(collect) == ["D0001"]


def test_history_page_logs_rate_limited_span(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _RateLimitedClient:
        def get_history_page(self, _channel_id: str, _cursor: str, _oldest: float | None) -> object:
            raise RateLimitedError(4.0)

    async def _no_sleep(_retry_after: float | None) -> None:
        await trio.lowlevel.checkpoint()

    monkeypatch.setattr("slack_fuse_server.backfill.api._sleep_rate_limited", _no_sleep)
    caplog.set_level("INFO", logger="slack_fuse_server.slurper.spans")
    backfiller = SlackApiBackfiller(cast(SlackClient, _RateLimitedClient()), trio.CapacityLimiter(1), _NO_SLEEP)

    result = trio.run(backfiller._history_page, "C_RATE", "", None, 2)

    assert result is None
    assert "op=slurper.backfill.history_page" in caplog.text
    assert "result=rate_limited" in caplog.text
    assert "retry_after_s=4.0" in caplog.text
    assert "channel_id=C_RATE" in caplog.text
    assert "page=2" in caplog.text


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


def test_iter_replies_pages_yields_pages_and_get_replies_flattens_them() -> None:
    first_page: JsonObject = {
        "ok": True,
        "messages": [
            {
                "type": "message",
                "ts": "1700000100.000200",
                "user": "U0001",
                "text": "parent",
                "thread_ts": "1700000100.000200",
                "reply_count": 2,
            },
            {
                "type": "message",
                "ts": "1700000200.000300",
                "user": "U0002",
                "text": "reply 1",
                "thread_ts": "1700000100.000200",
            },
        ],
        "has_more": True,
        "response_metadata": {"next_cursor": "cursor-2"},
    }
    second_page: JsonObject = {
        "ok": True,
        "messages": [
            {
                "type": "message",
                "ts": "1700000300.000400",
                "user": "U0003",
                "text": "reply 2",
                "thread_ts": "1700000100.000200",
            },
        ],
        "has_more": False,
        "response_metadata": {"next_cursor": ""},
    }

    def _handler(request: httpx.Request) -> httpx.Response:
        cursor = request.url.params.get("cursor")
        return httpx.Response(200, json=second_page if cursor == "cursor-2" else first_page)

    client = SlackClient("xoxp-test")
    client._http = httpx.Client(transport=httpx.MockTransport(_handler))  # pyright: ignore[reportPrivateUsage]

    pages = list(client.iter_replies_pages("C0001", "1700000100.000200"))
    replies = client.get_replies("C0001", "1700000100.000200")

    assert len(pages) == 2
    assert [message.ts for page in pages for message in page.model.messages] == [
        "1700000100.000200",
        "1700000200.000300",
        "1700000300.000400",
    ]
    assert [wrapped.model.ts for wrapped in replies] == [
        "1700000100.000200",
        "1700000200.000300",
        "1700000300.000400",
    ]


# === Driver: backfill_channel against a stub Backfiller ===


class _StubBackfiller:
    """Yields `count` synthetic messages with distinct ts. No API."""

    def __init__(self, count: int, *, page_size: int | None = None) -> None:
        self._count = count
        self._page_size = page_size if page_size is not None else count

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
            yield _validated_stub_message(i)

    async def messages_pages_for_channel(
        self,
        channel_id: ChannelId,
        since_ts: float | None = None,
    ) -> AsyncIterator[MessageBatch]:
        stream = f"channel:{channel_id.value}"
        page_size = max(self._page_size, 1)
        for page_index, start in enumerate(range(0, self._count, page_size)):
            messages = [_validated_stub_message(i) for i in range(start, min(start + page_size, self._count))]
            records = tuple(
                EventRecord(stream=stream, kind="message", ts=wrapped.model.ts, payload=wrapped.raw, dedup=True)
                for wrapped in messages
            )
            yield MessageBatch(
                kind="history_page",
                channel_id=channel_id.value,
                records=records,
                origin=MessageBatchOrigin(
                    channel_id=channel_id.value,
                    thread_ts=None,
                    page_index=page_index,
                    slack_cursor=f"stub-{page_index}",
                ),
            )


def _validated_stub_message(i: int) -> Validated[Message]:
    ts = f"{1000 + i}.{i:06d}"
    msg = Message(ts=ts, user="U1", text=f"m{i}")
    return Validated(raw=cast(JsonObject, msg.model_dump(mode="json")), model=msg)


class _OneMessageBackfiller:
    """Yields one message with caller-controlled raw payload."""

    def __init__(self, raw: JsonObject) -> None:
        self._raw = raw

    @property
    def name(self) -> str:
        return "one-message"

    async def channels_to_backfill(self) -> AsyncIterator[ChannelId]:
        return
        yield  # pragma: no cover — present only to make this an async generator

    async def messages_for_channel(
        self,
        channel_id: ChannelId,
        since_ts: float | None = None,
    ) -> AsyncIterator[Validated[Message]]:
        msg = Message.model_validate(self._raw)
        yield Validated(raw=self._raw, model=msg)

    async def messages_pages_for_channel(
        self,
        channel_id: ChannelId,
        since_ts: float | None = None,
    ) -> AsyncIterator[MessageBatch]:
        msg = Message.model_validate(self._raw)
        yield MessageBatch(
            kind="history_page",
            channel_id=channel_id.value,
            records=(
                EventRecord(
                    stream=f"channel:{channel_id.value}",
                    kind="message",
                    ts=msg.ts,
                    payload=self._raw,
                    dedup=True,
                ),
            ),
            origin=MessageBatchOrigin(
                channel_id=channel_id.value,
                thread_ts=None,
                page_index=0,
                slack_cursor="one-message",
            ),
        )


class _NullHealth:
    def __init__(self) -> None:
        self.kinds: list[str] = []

    async def emit(self, kind: HealthKind, _payload: JsonObject | None = None) -> int:
        self.kinds.append(str(kind))
        return len(self.kinds)


class _RunTransactionOnlyWriter:
    def __init__(self) -> None:
        self.run_transaction_calls = 0

    async def run_read(
        self,
        func: Callable[[psycopg.Connection[TupleRow]], object],
        *,
        limiter: trio.CapacityLimiter,
    ) -> object:
        del func, limiter
        return False

    async def run_transaction(self, func: Callable[[object], int], **_kwargs: object) -> int:
        self.run_transaction_calls += 1
        return func(object())

    def acquire_transaction(self, **_kwargs: object) -> object:
        raise AssertionError("backfill driver must use run_transaction")

    async def write_message_or_corrective(self, _record: EventRecord, **_kwargs: object) -> int | None:
        raise AssertionError("backfill driver must write batches via run_transaction")


def _disable_pg_retry_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    def _zero(_minimum: float, _maximum: float) -> float:
        return 0.0

    monkeypatch.setattr("slack_fuse_server.backfill.api.random.uniform", _zero)


def _events_count(conn: psycopg.Connection[TupleRow], stream: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM events WHERE stream = %s", (stream,))
        row = cur.fetchone()
    assert row is not None
    return int(row[0])


def _batch_for_records(channel_id: str, count: int) -> MessageBatch:
    stream = f"channel:{channel_id}"
    records = tuple(
        EventRecord(
            stream=stream,
            kind="message",
            ts=f"1800000{i:03d}.000000",
            payload={"ts": f"1800000{i:03d}.000000", "user": "U1", "text": f"m{i}"},
            dedup=True,
        )
        for i in range(count)
    )
    return MessageBatch(
        kind="history_page",
        channel_id=channel_id,
        records=records,
        origin=MessageBatchOrigin(channel_id=channel_id, thread_ts=None, page_index=0, slack_cursor="test"),
    )


class _FailingRunTransactionWriter:
    def __init__(self, *, failures: int) -> None:
        self.failures_remaining = failures
        self.calls = 0

    async def run_transaction(self, _func: object, **_kwargs: object) -> int:
        self.calls += 1
        if self.failures_remaining > 0:
            self.failures_remaining -= 1
            raise psycopg.errors.LockNotAvailable("test lock timeout")
        return 1


async def _write_batch_for_test(writer: OffsetWriter, batch: MessageBatch) -> int:
    return await _write_batch_with_retry(writer, batch, task_name="backfill")


def _channel_events(conn: psycopg.Connection[TupleRow], stream: str) -> list[tuple[str, JsonObject]]:
    with conn.cursor() as cur:
        cur.execute("SELECT kind, payload FROM events WHERE stream = %s ORDER BY id", (stream,))
        rows = cur.fetchall()
    out: list[tuple[str, JsonObject]] = []
    for kind, payload in rows:
        assert isinstance(payload, dict)
        out.append((str(kind), cast(JsonObject, payload)))
    return out


def _health_kinds(conn: psycopg.Connection[TupleRow]) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT kind FROM health_log ORDER BY id")
        return [str(r[0]) for r in cur.fetchall()]


def _database_url_for_conn(conn: psycopg.Connection[TupleRow]) -> str:
    with conn.cursor() as cur:
        cur.execute("SELECT current_schema()")
        row = cur.fetchone()
    assert row is not None
    return make_conninfo(conn.info.dsn, options=f"-c search_path={row[0]}")


def test_backfill_channel_writes_events_and_emits_health(server_conn: psycopg.Connection[TupleRow]) -> None:
    writer = make_test_writer(server_conn)
    health = HealthEmitter(writer)
    ctx = BackfillContext(writer=writer, health=health, limiters=make_test_limiters(), warn_at=1000, abort_at=20000)

    result = trio.run(backfill_channel, _StubBackfiller(5), ChannelId("CX"), ctx)

    assert (result.messages, result.events_written, result.aborted) == (5, 5, False)
    assert _events_count(server_conn, "channel:CX") == 5
    assert _health_kinds(server_conn) == ["backfill_started", "backfill_completed"]


def test_backfill_channel_write_batch_span_shape(
    server_conn: psycopg.Connection[TupleRow],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level("INFO", logger="slack_fuse_server.slurper.spans")
    writer = make_test_writer(server_conn)
    health = HealthEmitter(writer)
    ctx = BackfillContext(writer=writer, health=health, limiters=make_test_limiters(), warn_at=1000, abort_at=20000)

    result = trio.run(backfill_channel, _StubBackfiller(3), ChannelId("CSPAN"), ctx)

    assert result.events_written == 3
    assert "op=slurper.backfill.write_batch" in caplog.text
    assert "messages_in_batch=3" in caplog.text
    assert "batch_kind=history_page" in caplog.text
    assert "channel_id=CSPAN" in caplog.text
    assert "events_written=3" in caplog.text


class _ChannelNotFoundBackfiller:
    """Yields nothing — first iteration step raises ``ChannelNotFoundError``.

    Mirrors what happens in production when the user token can no longer see a
    channel (archived / kicked / id renamed) — the first ``conversations.history``
    page raises before any batch is produced.
    """

    @property
    def name(self) -> str:
        return "channel-not-found"

    async def channels_to_backfill(self) -> AsyncIterator[ChannelId]:
        return
        yield  # pragma: no cover — present only to make this an async generator

    async def messages_for_channel(
        self,
        channel_id: ChannelId,
        since_ts: float | None = None,
    ) -> AsyncIterator[Validated[Message]]:
        del channel_id, since_ts
        raise ChannelNotFoundError("Slack API error on conversations.history: channel_not_found")
        yield  # pragma: no cover

    async def messages_pages_for_channel(
        self,
        channel_id: ChannelId,
        since_ts: float | None = None,
    ) -> AsyncIterator[MessageBatch]:
        del channel_id, since_ts
        raise ChannelNotFoundError("Slack API error on conversations.history: channel_not_found")
        yield  # pragma: no cover


def test_backfill_channel_channel_not_found_aborts_channel_not_slurper(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    """A single channel returning channel_not_found must NOT crash the run.

    Regression for the 2026-07-06 CrashLoopBackOff: an archived / access-lost
    channel raised ``ChannelNotFoundError`` from ``get_history_page``, which
    the previous ``except FatalAPIError`` block did NOT catch (they are sibling
    subclasses of ``SlackAPIError``, not parent/child), so the exception blew
    through the trio nursery and killed the whole slurper. The auto-backfill
    loop needs the per-channel abort so it can keep iterating over the rest.
    """
    writer = make_test_writer(server_conn)
    health = HealthEmitter(writer)
    ctx = BackfillContext(writer=writer, health=health, limiters=make_test_limiters(), warn_at=1000, abort_at=20000)

    result = trio.run(backfill_channel, _ChannelNotFoundBackfiller(), ChannelId("CGONE"), ctx)

    assert result.aborted is True
    assert result.abort_reason == BackfillAbortReason.CHANNEL_NOT_FOUND
    assert result.events_written == 0
    assert _events_count(server_conn, "channel:CGONE") == 0
    assert _health_kinds(server_conn) == ["backfill_started", "backfill_aborted"]


def test_write_batch_mid_batch_runtime_error_rolls_back_all_records(
    server_conn: psycopg.Connection[TupleRow],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writer = make_test_writer(server_conn)
    batch = _batch_for_records("CATOMIC", 5)
    original = backfill_api.write_message_or_corrective
    calls = 0

    def _fail_after_third(conn: psycopg.Connection[TupleRow], record: EventRecord) -> int | None:
        nonlocal calls
        calls += 1
        offset = original(conn, record)
        if calls == 3:
            raise RuntimeError("boom mid-page")
        return offset

    monkeypatch.setattr(backfill_api, "write_message_or_corrective", _fail_after_third)

    with pytest.raises(RuntimeError, match="boom mid-page"):
        trio.run(_write_batch_for_test, writer, batch)

    assert _events_count(server_conn, "channel:CATOMIC") == 0


def test_write_batch_retries_pg_timeout_once_for_whole_page(
    server_conn: psycopg.Connection[TupleRow],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _disable_pg_retry_sleep(monkeypatch)
    writer = make_test_writer(server_conn)
    batch = _batch_for_records("CRETRY", 5)
    original = backfill_api.write_message_or_corrective
    calls = 0
    injected = False

    def _timeout_after_third(conn: psycopg.Connection[TupleRow], record: EventRecord) -> int | None:
        nonlocal calls, injected
        calls += 1
        offset = original(conn, record)
        if not injected and calls == 3:
            injected = True
            raise psycopg.errors.LockNotAvailable("test lock timeout")
        return offset

    monkeypatch.setattr(backfill_api, "write_message_or_corrective", _timeout_after_third)

    inserted = trio.run(_write_batch_for_test, writer, batch)

    assert inserted == 5
    assert injected is True
    assert calls == 8
    assert _events_count(server_conn, "channel:CRETRY") == 5


def test_pg_notify_wakes_once_for_page_transaction_and_tailer_reads_all_offsets(
    server_conn_factory: ServerConnFactory,
) -> None:
    conn = server_conn_factory()
    database_url = _database_url_for_conn(conn)
    listen_conn: psycopg.Connection[TupleRow] = psycopg.connect(database_url, autocommit=True)
    try:
        listen_conn.execute("LISTEN new_event")
        writer = make_test_writer(conn)
        batch = _batch_for_records("CNOTIFY", 5)

        inserted = trio.run(_write_batch_for_test, writer, batch)
        notifications = list(listen_conn.notifies(timeout=2.0))

        async def collect_offsets() -> list[int]:
            tailer = EventTailer(database_url)
            return [event.offset async for event in tailer.iter_events_after("channel:CNOTIFY", 0)]

        offsets = trio.run(collect_offsets)
    finally:
        listen_conn.close()

    assert inserted == 5
    assert [notify.payload for notify in notifications] == ["channel:CNOTIFY"]
    assert offsets == [1, 2, 3, 4, 5]


def test_backfill_channel_uses_writer_run_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writer = _RunTransactionOnlyWriter()
    health = _NullHealth()
    ctx = BackfillContext(
        writer=cast(OffsetWriter, writer),
        health=cast(HealthEmitter, health),
        limiters=make_test_limiters(),
        warn_at=1000,
        abort_at=20000,
    )

    def _fake_write_batch_sync(_conn: object, records: object) -> int:
        return len(cast(tuple[object, ...], records))

    monkeypatch.setattr(backfill_api, "_write_batch_sync", _fake_write_batch_sync)

    result = trio.run(backfill_channel, _StubBackfiller(1), ChannelId("CRUN"), ctx)

    assert writer.run_transaction_calls == 1
    assert result.messages == 1
    assert result.events_written == 1
    assert health.kinds == ["backfill_started", "backfill_completed"]


def test_write_batch_propagates_second_pg_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _disable_pg_retry_sleep(monkeypatch)
    batch = _batch_for_records("CFAIL", 1)
    writer = _FailingRunTransactionWriter(failures=2)

    with pytest.raises(psycopg.errors.LockNotAvailable):
        trio.run(_write_batch_for_test, cast(OffsetWriter, writer), batch)

    assert writer.calls == 2


def test_backfill_channel_aborts_at_history_page_boundary(server_conn: psycopg.Connection[TupleRow]) -> None:
    writer = make_test_writer(server_conn)
    health = HealthEmitter(writer)
    ctx = BackfillContext(
        writer=writer,
        health=health,
        limiters=make_test_limiters(),
        warn_at=1000,
        abort_at=1500,
        progress_every=0,
    )

    result = trio.run(backfill_channel, _StubBackfiller(1600, page_size=200), ChannelId("CBIG"), ctx)

    assert result.aborted is True
    assert result.abort_reason is not None and str(result.abort_reason) == "exceeded_default_limit"
    # 1400 committed; the next 200-message page would cross 1500 and is skipped.
    assert result.messages == 1400
    assert _events_count(server_conn, "channel:CBIG") == 1400
    # warn_at triggers a per-channel BACKFILL_WARN_LARGE (not SLACK_DEGRADED —
    # one channel hitting its size cap is observability, not a global
    # ingestion-health signal; see BACKLOG entry on health hysteresis).
    assert _health_kinds(server_conn) == ["backfill_started", "backfill_warn_large", "backfill_aborted"]


def test_backfill_channel_skips_blocked_channel(server_conn: psycopg.Connection[TupleRow]) -> None:
    with server_conn.cursor() as cur:
        cur.execute("INSERT INTO blocked_channels (channel_id, reason) VALUES ('CBLOCK', 'noisy')")
    writer = make_test_writer(server_conn)
    health = HealthEmitter(writer)
    ctx = BackfillContext(writer=writer, health=health, limiters=make_test_limiters(), warn_at=1000, abort_at=20000)

    result = trio.run(backfill_channel, _StubBackfiller(5), ChannelId("CBLOCK"), ctx)

    assert result.aborted is True
    assert result.abort_reason == BackfillAbortReason.OPERATOR_BLOCKED
    assert result.messages == 0
    assert _events_count(server_conn, "channel:CBLOCK") == 0
    assert _health_kinds(server_conn) == ["backfill_skipped"]


def _health_rows(conn: psycopg.Connection[TupleRow]) -> list[tuple[str, object]]:
    with conn.cursor() as cur:
        cur.execute("SELECT kind, payload FROM health_log ORDER BY id")
        return [(str(r[0]), r[1]) for r in cur.fetchall()]


def test_backfill_writes_message_when_no_prior_event(server_conn: psycopg.Connection[TupleRow]) -> None:
    writer = make_test_writer(server_conn)
    health = HealthEmitter(writer)
    ctx = BackfillContext(writer=writer, health=health, limiters=make_test_limiters(), warn_at=1000, abort_at=20000)
    raw: JsonObject = {"ts": "2000.000001", "user": "U1", "text": "fresh"}

    result = trio.run(backfill_channel, _OneMessageBackfiller(raw), ChannelId("CFRESH"), ctx)

    assert result.events_written == 1
    assert _channel_events(server_conn, "channel:CFRESH") == [("message", raw)]


def test_backfill_writes_message_changed_when_ts_already_has_message(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    stream = "channel:CCORR"
    legacy: JsonObject = {"ts": "2000.000002", "user": "U1", "text": ""}
    fresh: JsonObject = {
        "ts": "2000.000002",
        "user": "U1",
        "text": "",
        "attachments": [{"fallback": "FE-740", "text": "Linear unfurl body"}],
    }
    assert (
        write_event(
            server_conn,
            EventRecord(stream=stream, kind="message", ts="2000.000002", payload=legacy, dedup=True),
        )
        == 1
    )
    writer = make_test_writer(server_conn)
    health = HealthEmitter(writer)
    ctx = BackfillContext(writer=writer, health=health, limiters=make_test_limiters(), warn_at=1000, abort_at=20000)

    result = trio.run(backfill_channel, _OneMessageBackfiller(fresh), ChannelId("CCORR"), ctx)

    assert result.events_written == 1
    assert _channel_events(server_conn, stream) == [
        ("message", legacy),
        ("message_changed", {"message": fresh, "previous_ts": "2000.000002"}),
    ]


def test_backfill_is_still_idempotent_against_message_changed(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    stream = "channel:CIDEM"
    legacy: JsonObject = {"ts": "2000.000003", "user": "U1", "text": ""}
    fresh: JsonObject = {
        "ts": "2000.000003",
        "user": "U1",
        "text": "",
        "attachments": [{"fallback": "FE-814", "text": "Another Linear unfurl"}],
    }
    assert (
        write_event(
            server_conn,
            EventRecord(stream=stream, kind="message", ts="2000.000003", payload=legacy, dedup=True),
        )
        == 1
    )
    writer = make_test_writer(server_conn)
    health = HealthEmitter(writer)
    ctx = BackfillContext(writer=writer, health=health, limiters=make_test_limiters(), warn_at=1000, abort_at=20000)
    backfiller = _OneMessageBackfiller(fresh)

    first = trio.run(backfill_channel, backfiller, ChannelId("CIDEM"), ctx)
    second = trio.run(backfill_channel, backfiller, ChannelId("CIDEM"), ctx)

    assert first.events_written == 1
    assert second.events_written == 0
    assert _channel_events(server_conn, stream) == [
        ("message", legacy),
        ("message_changed", {"message": fresh, "previous_ts": "2000.000003"}),
    ]


def test_backfill_channel_emits_progress_every_n_messages(server_conn: psycopg.Connection[TupleRow]) -> None:
    writer = make_test_writer(server_conn)
    health = HealthEmitter(writer)
    ctx = BackfillContext(
        writer=writer,
        health=health,
        limiters=make_test_limiters(),
        warn_at=1000,
        abort_at=20000,
        progress_every=2,
    )

    result = trio.run(backfill_channel, _StubBackfiller(5), ChannelId("CP"), ctx)

    assert (result.messages, result.aborted) == (5, False)
    rows = _health_rows(server_conn)
    progress = [payload for kind, payload in rows if kind == "backfill_progress"]
    # Progress is emitted after the whole batch commits, not per record.
    assert progress == [{"channel_id": "CP", "messages_so_far": 5}]
    assert [kind for kind, _ in rows] == [
        "backfill_started",
        "backfill_progress",
        "backfill_completed",
    ]


def test_backfill_channel_is_idempotent_on_rerun(server_conn: psycopg.Connection[TupleRow]) -> None:
    writer = make_test_writer(server_conn)
    health = HealthEmitter(writer)
    ctx = BackfillContext(writer=writer, health=health, limiters=make_test_limiters(), warn_at=1000, abort_at=20000)

    first = trio.run(backfill_channel, _StubBackfiller(4), ChannelId("CY"), ctx)
    second = trio.run(backfill_channel, _StubBackfiller(4), ChannelId("CY"), ctx)

    assert first.events_written == 4
    # Re-run: same ts values dedup to no-ops, no new rows.
    assert second.messages == 4
    assert second.events_written == 0
    assert _events_count(server_conn, "channel:CY") == 4
