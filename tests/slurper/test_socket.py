# pyright: reportPrivateUsage=false
"""Socket-mode event translation: pure mapping + DB-backed write integration.

The pure tests pin the `SocketEventPayload` -> `EventRecord` mapping. The
integration tests drive `SocketModeRunner._handle_event` against a real schema
and the fake Slack transport, asserting the right rows land (and, for a
structural event, that `conversations.info` enrichment runs).
"""

from __future__ import annotations

import json
from typing import cast

import httpx
import psycopg
import pytest
import trio
from psycopg.rows import TupleRow

from slack_fuse.models import JsonObject, Message, SocketEventPayload
from slack_fuse_server.slurper.api import SlackClient
from slack_fuse_server.slurper.health import HealthEmitter
from slack_fuse_server.slurper.offsets import EventRecord, OffsetWriter, WriterPoolExhausted
from slack_fuse_server.slurper.socket import (
    SocketModeOptions,
    SocketModeRunner,
    _parse_envelope,
    translate_message_event,
)
from tests._fake_slack import load_fixtures
from tests.conftest import RecordingSupervisor, make_test_limiters, make_test_writer

# === Pure translation ===


def _raw_for(event: SocketEventPayload) -> JsonObject:
    """Build a raw event dict from a typed event for tests that don't care
    about lossless preservation (they're testing the translate dispatch
    logic, not the raw-persistence guarantee)."""
    return cast(JsonObject, event.model_dump(mode="json", exclude_none=True))


def test_translate_new_top_level_message() -> None:
    event = SocketEventPayload(type="message", channel="C1", ts="100.0001", user="U1", text="hello")
    write = translate_message_event(event, _raw_for(event))
    assert write is not None
    assert (write.stream, write.kind, write.ts, write.dedup) == ("channel:C1", "message", "100.0001", True)
    assert write.payload["ts"] == "100.0001"
    assert write.payload["text"] == "hello"


def test_translate_thread_reply_keeps_thread_ts() -> None:
    event = SocketEventPayload(type="message", channel="C1", ts="101.0", user="U1", text="re", thread_ts="100.0")
    write = translate_message_event(event, _raw_for(event))
    assert write is not None
    assert write.kind == "message"
    assert write.payload["thread_ts"] == "100.0"


def test_translate_message_changed() -> None:
    new = Message(ts="100.0001", user="U1", text="edited")
    event = SocketEventPayload(type="message", subtype="message_changed", channel="C1", message=new)
    write = translate_message_event(event, _raw_for(event))
    assert write is not None
    assert (write.kind, write.ts, write.dedup) == ("message_changed", "100.0001", False)
    assert write.payload["previous_ts"] == "100.0001"


def test_translate_message_deleted() -> None:
    event = SocketEventPayload(type="message", subtype="message_deleted", channel="C1", deleted_ts="100.0001")
    write = translate_message_event(event, _raw_for(event))
    assert write is not None
    assert (write.kind, write.ts) == ("message_deleted", "100.0001")
    assert write.payload["deleted_ts"] == "100.0001"


def test_translate_message_replied_parent_refresh() -> None:
    raw_event: JsonObject = {
        "type": "message",
        "subtype": "message_replied",
        "channel": "C1",
        "ts": "1700000300.000400",
        "event_ts": "1700000300.000400",
        "message": {
            "type": "message",
            "user": "U1",
            "text": "parent",
            "ts": "1700000000.000100",
            "thread_ts": "1700000000.000100",
            "reply_count": 2,
            "latest_reply": "1700000200.000300",
        },
    }
    event = SocketEventPayload.model_validate(raw_event)

    write = translate_message_event(event, raw_event)

    assert write is not None
    assert (write.stream, write.kind, write.ts, write.dedup) == (
        "channel:C1",
        "parent_replied",
        "1700000000.000100",
        True,
    )
    assert write.payload["channel_id"] == "C1"
    assert write.payload["parent_ts"] == "1700000000.000100"
    assert write.payload["reply_count"] == 2
    assert write.payload["latest_reply"] == "1700000200.000300"
    assert write.payload["probed_at"] == "1700000300.000400"


def test_translate_message_replied_without_subtype() -> None:
    raw_event: JsonObject = {
        "type": "message",
        "channel": "C1",
        "hidden": True,
        "ts": "1700000300.000400",
        "event_ts": "1700000300.000400",
        "message": {
            "type": "message",
            "user": "U1",
            "text": "parent",
            "ts": "1700000000.000100",
            "thread_ts": "1700000000.000100",
            "reply_count": 2,
            "replies": [{"user": "U2", "ts": "1700000200.000300"}],
        },
    }
    event = SocketEventPayload.model_validate(raw_event)

    write = translate_message_event(event, raw_event)

    assert write is not None
    assert write.kind == "parent_replied"
    assert write.payload["latest_reply"] == "1700000200.000300"


def test_translate_missing_channel_returns_none() -> None:
    event = SocketEventPayload(type="message", ts="1.0")
    assert translate_message_event(event, _raw_for(event)) is None


def test_translate_message_payload_matches_backfill_shape() -> None:
    fixtures = load_fixtures()
    history_fixture = fixtures["conversations.history"]
    messages = history_fixture.get("messages")
    assert isinstance(messages, list) and messages, "conversations.history fixture must include messages"
    base_message = messages[0]
    assert isinstance(base_message, dict)

    rich_message_event: JsonObject = cast(
        JsonObject,
        {
            **base_message,
            "type": "message",
            "channel": "C1",
            "thread_ts": "1700000000.000100",
            "reply_count": 2,
            "latest_reply": "1700000200.000300",
            "edited": {"user": "U0001", "ts": "1700000300.000400"},
            "files": [
                {
                    "id": "F0001",
                    "name": "notes.md",
                    "title": "notes",
                    "filetype": "md",
                    "mimetype": "text/markdown",
                    "size": 123,
                    "url_private": "https://slack.example/private/F0001",
                    "url_private_download": "https://slack.example/download/F0001",
                },
            ],
            "subtype": "file_share",
        },
    )
    raw_envelope = json.dumps(
        {
            "type": "events_api",
            "envelope_id": "env-1",
            "payload": {"event": rich_message_event},
        },
    )
    parsed = _parse_envelope(raw_envelope)
    assert parsed is not None
    envelope, raw_env = parsed
    assert envelope.payload is not None

    payload_dict = raw_env["payload"]
    assert isinstance(payload_dict, dict)
    event_dict = payload_dict["event"]
    assert isinstance(event_dict, dict)
    raw_event_dict = cast(JsonObject, event_dict)
    write = translate_message_event(envelope.payload.event, raw_event_dict)
    assert write is not None

    # 2026-06-27: the payload is now the RAW event dict (lossless), not
    # ``Message.model_dump`` output. We persist what Slack sent so future
    # projections can read fields the model doesn't declare today.
    assert write.payload == rich_message_event


# === DB-backed integration ===


def _make_runner(conn: psycopg.Connection[TupleRow], http: httpx.Client) -> SocketModeRunner:
    client = SlackClient("xoxp-test")
    client._http = http  # swap in the fake transport
    writer = make_test_writer(conn)
    return SocketModeRunner(writer, HealthEmitter(writer), client, "xapp-test", limiters=make_test_limiters())


class _NullHealth:
    async def emit(self, *_args: object, **_kwargs: object) -> int:
        return 1


class _TimeoutWriter:
    def __init__(self) -> None:
        self.records: list[EventRecord] = []

    async def write_event(self, record: EventRecord, **_kwargs: object) -> int | None:
        self.records.append(record)
        raise WriterPoolExhausted("test writer pool exhausted")


def _rows(conn: psycopg.Connection[TupleRow], stream: str) -> list[tuple[str, object]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT kind, payload FROM events WHERE stream = %s ORDER BY offset_in_stream",
            (stream,),
        )
        return [(str(r[0]), r[1]) for r in cur.fetchall()]


def test_handle_message_event_writes_channel_stream(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    runner = _make_runner(server_conn, fake_slack_http)
    event = SocketEventPayload(type="message", channel="C1", ts="100.0001", user="U1", text="hi")

    trio.run(runner._handle_event, event, _raw_for(event))

    rows = _rows(server_conn, "channel:C1")
    assert len(rows) == 1
    kind, payload = rows[0]
    assert kind == "message"
    assert isinstance(payload, dict)
    assert payload["ts"] == "100.0001"


def test_handle_message_replied_writes_parent_replied_idempotently(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    runner = _make_runner(server_conn, fake_slack_http)
    parent = SocketEventPayload(type="message", channel="C1", ts="100.0001", user="U1", text="parent")
    reply_refresh: JsonObject = {
        "type": "message",
        "subtype": "message_replied",
        "channel": "C1",
        "ts": "101.0002",
        "event_ts": "101.0002",
        "message": {
            "type": "message",
            "user": "U1",
            "text": "parent",
            "ts": "100.0001",
            "thread_ts": "100.0001",
            "reply_count": 1,
            "latest_reply": "101.0002",
        },
    }
    event = SocketEventPayload.model_validate(reply_refresh)

    trio.run(runner._handle_event, parent, _raw_for(parent))
    trio.run(runner._handle_event, event, reply_refresh)
    trio.run(runner._handle_event, event, reply_refresh)

    rows = _rows(server_conn, "channel:C1")
    assert [kind for kind, _ in rows] == ["message", "parent_replied"]
    parent_replied = rows[1][1]
    assert isinstance(parent_replied, dict)
    assert parent_replied["parent_ts"] == "100.0001"
    assert parent_replied["reply_count"] == 1
    assert parent_replied["latest_reply"] == "101.0002"


def test_handle_event_declares_handling_phase(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    client = SlackClient("xoxp-test")
    client._http = fake_slack_http
    writer = make_test_writer(server_conn)
    supervisor = RecordingSupervisor()
    runner = SocketModeRunner(
        writer,
        HealthEmitter(writer),
        client,
        "xapp-test",
        limiters=make_test_limiters(),
        supervisor=supervisor,
    )
    event = SocketEventPayload(type="message", channel="C1", ts="100.0001", user="U1", text="hi")

    trio.run(runner._handle_event, event, _raw_for(event))

    assert ("socket", "handling_event", {"kind": "message"}) in [
        (item.task_name, item.phase, item.details) for item in supervisor.declarations
    ]


def test_handle_message_event_drops_pg_timeout_with_warning(
    fake_slack_http: httpx.Client,
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = SlackClient("xoxp-test")
    client._http = fake_slack_http
    writer = _TimeoutWriter()
    runner = SocketModeRunner(
        cast(OffsetWriter, writer),
        cast(HealthEmitter, _NullHealth()),
        client,
        "xapp-test",
        limiters=make_test_limiters(),
    )
    event = SocketEventPayload(type="message", channel="C1", ts="100.0001", user="U1", text="hi")
    caplog.set_level("INFO")

    trio.run(runner._handle_event, event, _raw_for(event))

    assert len(writer.records) == 1
    assert "dropped event after PostgreSQL timeout" in caplog.text
    assert "stream=channel:C1" in caplog.text
    assert "kind=message" in caplog.text
    assert "channel_id=C1" in caplog.text
    span_messages = [
        record.getMessage()
        for record in caplog.records
        if record.name == "slack_fuse_server.slurper.spans"
    ]
    assert any(
        "op=slurper.socket.handle_event" in message
        and "result=timeout" in message
        and "timeout_type=WriterPoolExhausted" in message
        for message in span_messages
    )


def test_handle_channel_id_changed_writes_channel_list(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    runner = _make_runner(server_conn, fake_slack_http)
    raw_event: JsonObject = {
        "type": "channel_id_changed",
        "old_channel_id": "COLD",
        "new_channel_id": "CNEW",
        "event_ts": "1700000000.000100",
    }
    event = SocketEventPayload.model_validate(raw_event)

    trio.run(runner._handle_event, event, raw_event)

    rows = _rows(server_conn, "channel-list")
    assert len(rows) == 1
    kind, payload = rows[0]
    assert kind == "channel_id_changed"
    assert payload == {
        "old_channel_id": "COLD",
        "new_channel_id": "CNEW",
        "event_ts": "1700000000.000100",
    }


def test_handle_channel_history_changed_writes_channel_list_idempotently(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    runner = _make_runner(server_conn, fake_slack_http)
    raw_event: JsonObject = {
        "type": "channel_history_changed",
        "channel": "C0001",
        "latest": "1700000300.000400",
        "ts": "1700000200.000300",
        "event_ts": "1700000400.000500",
    }
    event = SocketEventPayload.model_validate(raw_event)

    trio.run(runner._handle_event, event, raw_event)
    trio.run(runner._handle_event, event, raw_event)

    rows = _rows(server_conn, "channel-list")
    assert len(rows) == 1
    kind, payload = rows[0]
    assert kind == "channel_history_changed"
    assert payload == {
        "channel_id": "C0001",
        "latest": "1700000300.000400",
        "ts": "1700000200.000300",
        "event_ts": "1700000400.000500",
    }


def test_handle_tokens_revoked_writes_payload_and_auth_health(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    runner = _make_runner(server_conn, fake_slack_http)
    raw_event: JsonObject = {
        "type": "tokens_revoked",
        "tokens": {"oauth": ["U0001"], "bot": ["B0001"]},
    }
    event = SocketEventPayload.model_validate(raw_event)

    trio.run(runner._handle_event, event, raw_event)
    trio.run(runner._handle_event, event, raw_event)

    rows = _rows(server_conn, "slurper-health")
    assert [kind for kind, _ in rows] == ["tokens_revoked", "auth_token_invalid", "auth_token_invalid"]
    tokens_payload = rows[0][1]
    assert isinstance(tokens_payload, dict)
    assert tokens_payload["tokens"] == {"oauth": ["U0001"], "bot": ["B0001"]}
    auth_payloads = [payload for kind, payload in rows if kind == "auth_token_invalid"]
    assert all(payload == {"reason": "tokens_revoked"} for payload in auth_payloads)


def test_handle_structural_event_enriches_via_conversations_info(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    runner = _make_runner(server_conn, fake_slack_http)
    # channel_rename → fetch conversations.info → emit channel_renamed with the
    # current name from the fetched channel object.
    event = SocketEventPayload(type="channel_rename", channel="C0001")

    trio.run(runner._handle_event, event, _raw_for(event))

    rows = _rows(server_conn, "channel-list")
    assert len(rows) == 1
    kind, payload = rows[0]
    assert kind == "channel_renamed"
    assert isinstance(payload, dict)
    assert payload == {"channel_id": "C0001", "new_name": "general"}


def test_handle_member_joined_channel_for_unknown_self_only_writes_user_membership(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    runner = _make_runner(server_conn, fake_slack_http)
    raw_event: JsonObject = {
        "type": "member_joined_channel",
        "channel": "C0001",
        "user": "UOTHER",
        "inviter": "UINVITER",
        "event_ts": "1700000000.000100",
    }
    event = SocketEventPayload.model_validate(raw_event)

    trio.run(runner._handle_event, event, raw_event)
    trio.run(runner._handle_event, event, raw_event)

    rows = _rows(server_conn, "channel-list")
    joined = [payload for kind, payload in rows if kind == "channel_member_joined"]
    assert len(joined) == 1
    assert joined[0] == {
        "channel_id": "C0001",
        "user_id": "UOTHER",
        "inviter_id": "UINVITER",
        "event_ts": "1700000000.000100",
    }
    assert [kind for kind, _ in rows] == ["channel_member_joined"]


def test_handle_member_left_channel_writes_user_membership(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    runner = _make_runner(server_conn, fake_slack_http)
    raw_event: JsonObject = {
        "type": "member_left_channel",
        "channel": "C0001",
        "user": "UOTHER",
        "event_ts": "1700000001.000200",
    }
    event = SocketEventPayload.model_validate(raw_event)

    trio.run(runner._handle_event, event, raw_event)

    rows = _rows(server_conn, "channel-list")
    left = [payload for kind, payload in rows if kind == "channel_member_left"]
    assert left == [
        {
            "channel_id": "C0001",
            "user_id": "UOTHER",
            "inviter_id": None,
            "event_ts": "1700000001.000200",
        }
    ]


def test_on_hello_fires_reconnect_hook_only_after_a_disconnect(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    """The reconnect hook (catchup trigger in prod) fires with the downtime on a
    reconnect, but never on the first connect of a fresh process — a startup has
    no prior connection whose gap to fill via this path."""
    fired: list[float] = []
    client = SlackClient("xoxp-test")
    client._http = fake_slack_http
    writer = make_test_writer(server_conn)
    options = SocketModeOptions(on_reconnect=fired.append)
    runner = SocketModeRunner(
        writer,
        HealthEmitter(writer),
        client,
        "xapp-test",
        limiters=make_test_limiters(),
        options=options,
    )

    async def go() -> None:
        # First hello = fresh connect (disconnected_at is None) → no hook.
        await runner._on_hello()
        # Simulate a disconnect 42s ago, then reconnect.
        runner._disconnected_at = trio.current_time() - 42.0
        await runner._on_hello()

    trio.run(go)

    assert len(fired) == 1
    assert fired[0] >= 41.0
