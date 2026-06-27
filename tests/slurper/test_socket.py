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
import trio
from psycopg.rows import TupleRow

from slack_fuse.models import JsonObject, Message, SocketEventPayload
from slack_fuse_server.slurper.api import SlackClient
from slack_fuse_server.slurper.health import HealthEmitter
from slack_fuse_server.slurper.offsets import OffsetWriter
from slack_fuse_server.slurper.socket import (
    SocketModeOptions,
    SocketModeRunner,
    _parse_envelope,
    translate_message_event,
)
from tests._fake_slack import load_fixtures

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
    writer = OffsetWriter(conn, trio.CapacityLimiter(1))
    return SocketModeRunner(writer, HealthEmitter(writer), client, "xapp-test")


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
    writer = OffsetWriter(server_conn, trio.CapacityLimiter(1))
    options = SocketModeOptions(on_reconnect=fired.append)
    runner = SocketModeRunner(writer, HealthEmitter(writer), client, "xapp-test", options=options)

    async def go() -> None:
        # First hello = fresh connect (disconnected_at is None) → no hook.
        await runner._on_hello()
        # Simulate a disconnect 42s ago, then reconnect.
        runner._disconnected_at = trio.current_time() - 42.0
        await runner._on_hello()

    trio.run(go)

    assert len(fired) == 1
    assert fired[0] >= 41.0
