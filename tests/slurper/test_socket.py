# pyright: reportPrivateUsage=false
"""Socket-mode event translation: pure mapping + DB-backed write integration.

The pure tests pin the `SocketEventPayload` -> `EventRecord` mapping. The
integration tests drive `SocketModeRunner._handle_event` against a real schema
and the fake Slack transport, asserting the right rows land (and, for a
structural event, that `conversations.info` enrichment runs).
"""

from __future__ import annotations

import httpx
import psycopg
import trio
from psycopg.rows import TupleRow

from slack_fuse.models import Message, SocketEventPayload
from slack_fuse_server.slurper.api import SlackClient
from slack_fuse_server.slurper.health import HealthEmitter
from slack_fuse_server.slurper.offsets import OffsetWriter
from slack_fuse_server.slurper.socket import SocketModeRunner, translate_message_event

# === Pure translation ===


def test_translate_new_top_level_message() -> None:
    event = SocketEventPayload(type="message", channel="C1", ts="100.0001", user="U1", text="hello")
    write = translate_message_event(event)
    assert write is not None
    assert (write.stream, write.kind, write.ts, write.dedup) == ("channel:C1", "message", "100.0001", True)
    assert write.payload["ts"] == "100.0001"
    assert write.payload["text"] == "hello"


def test_translate_thread_reply_keeps_thread_ts() -> None:
    event = SocketEventPayload(type="message", channel="C1", ts="101.0", user="U1", text="re", thread_ts="100.0")
    write = translate_message_event(event)
    assert write is not None
    assert write.kind == "message"
    assert write.payload["thread_ts"] == "100.0"


def test_translate_message_changed() -> None:
    new = Message(ts="100.0001", user="U1", text="edited")
    event = SocketEventPayload(type="message", subtype="message_changed", channel="C1", message=new)
    write = translate_message_event(event)
    assert write is not None
    assert (write.kind, write.ts, write.dedup) == ("message_changed", "100.0001", False)
    assert write.payload["previous_ts"] == "100.0001"


def test_translate_message_deleted() -> None:
    event = SocketEventPayload(type="message", subtype="message_deleted", channel="C1", deleted_ts="100.0001")
    write = translate_message_event(event)
    assert write is not None
    assert (write.kind, write.ts) == ("message_deleted", "100.0001")
    assert write.payload["deleted_ts"] == "100.0001"


def test_translate_missing_channel_returns_none() -> None:
    assert translate_message_event(SocketEventPayload(type="message", ts="1.0")) is None


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

    trio.run(runner._handle_event, event)

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

    trio.run(runner._handle_event, event)

    rows = _rows(server_conn, "channel-list")
    assert len(rows) == 1
    kind, payload = rows[0]
    assert kind == "channel_renamed"
    assert isinstance(payload, dict)
    assert payload == {"channel_id": "C0001", "new_name": "general"}
