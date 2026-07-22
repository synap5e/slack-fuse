# pyright: reportPrivateUsage=false
"""Transport-neutral dispatch, typed failures, and universal event-id dedup."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Literal, cast

import httpx
import psycopg
import pytest
import trio

import slack_fuse_server.slack_events.dispatcher as dispatcher_module
from slack_fuse.models import EventsApiPayload, SocketEventPayload
from slack_fuse_server._json import JsonObject
from slack_fuse_server.slack_events.dispatcher import SlackEventDispatcher
from slack_fuse_server.slack_events.types import DispatchErrorCode, DispatchTransientError, SlackEventSource
from slack_fuse_server.slurper.api import SlackClient
from slack_fuse_server.slurper.health import HealthEmitter
from slack_fuse_server.slurper.offsets import EventRecord, OffsetWriter, WriterPoolExhausted
from tests.conftest import make_test_limiters, make_test_writer

if TYPE_CHECKING:
    from psycopg.rows import TupleRow


class _NullHealth:
    async def emit(self, *_args: object, **_kwargs: object) -> int:
        return 1


class _TimeoutWriter:
    async def write_event(self, _record: EventRecord, **_kwargs: object) -> int | None:
        raise WriterPoolExhausted("synthetic timeout")


def _dispatcher(
    conn: psycopg.Connection[TupleRow],
    http: httpx.Client,
    *,
    self_user_id: str = "U_SELF",
    on_self_join: Callable[[str], bool] | None = None,
) -> SlackEventDispatcher:
    client = SlackClient("xoxp-test")
    client._http = http
    writer = make_test_writer(conn)
    return SlackEventDispatcher(
        writer,
        client,
        self_user_id,
        make_test_limiters(),
        HealthEmitter(writer),
        on_self_join,
    )


async def _dispatch(
    dispatcher: SlackEventDispatcher,
    payload: EventsApiPayload,
    raw_event: JsonObject,
    *,
    transport: Literal["socket", "http"] = "http",
) -> None:
    source = SlackEventSource(
        transport=transport,
        event_id=payload.event_id,
    )
    await dispatcher.dispatch(payload, raw_event, source)


@pytest.mark.trio
async def test_same_event_id_across_socket_and_http_writes_once(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    dispatcher = _dispatcher(server_conn, fake_slack_http)
    raw_event: JsonObject = {
        "type": "message",
        "channel": "C1",
        "ts": "1700000000.000001",
        "event_ts": "1700000000.000002",
        "text": "same delivery",
    }
    payload = EventsApiPayload(event_id="EvCrossTransport", event=SocketEventPayload.model_validate(raw_event))
    await _dispatch(dispatcher, payload, raw_event, transport="socket")
    await _dispatch(dispatcher, payload, raw_event, transport="http")

    with server_conn.cursor() as cur:
        cur.execute(
            "SELECT kind, source->>'slack_event_id', source->>'transport' "
            "FROM events WHERE stream = 'channel:C1' ORDER BY id"
        )
        rows = cur.fetchall()
    # Dedup keeps only the first delivery (socket); transport label reflects
    # which transport actually won the write, not the last one attempted.
    assert rows == [("message", "EvCrossTransport", "socket")]


@pytest.mark.trio
async def test_transport_label_stamped_per_dispatch(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    """Each dispatch stamps its own transport into every emitted row's source."""
    dispatcher = _dispatcher(server_conn, fake_slack_http)
    raw_socket: JsonObject = {
        "type": "message",
        "channel": "C1",
        "ts": "1700000001.000001",
        "event_ts": "1700000001.000002",
        "text": "via socket",
    }
    raw_http: JsonObject = {
        "type": "message",
        "channel": "C1",
        "ts": "1700000002.000001",
        "event_ts": "1700000002.000002",
        "text": "via webhook",
    }
    await _dispatch(
        dispatcher,
        EventsApiPayload(event_id="EvSocket1", event=SocketEventPayload.model_validate(raw_socket)),
        raw_socket,
        transport="socket",
    )
    await _dispatch(
        dispatcher,
        EventsApiPayload(event_id="EvHttp1", event=SocketEventPayload.model_validate(raw_http)),
        raw_http,
        transport="http",
    )

    with server_conn.cursor() as cur:
        cur.execute(
            "SELECT source->>'slack_event_id', source->>'transport' "
            "FROM events WHERE stream = 'channel:C1' ORDER BY id"
        )
        rows = cur.fetchall()
    assert rows == [("EvSocket1", "socket"), ("EvHttp1", "http")]


@pytest.mark.trio
async def test_contextvar_dedup_reaches_record_without_make_source(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    dispatcher = _dispatcher(server_conn, fake_slack_http)
    raw_event: JsonObject = {"type": "channel_left", "channel": "C_LEFT", "event_ts": "1.000001"}
    payload = EventsApiPayload(event_id="EvNoMakeSource", event=SocketEventPayload.model_validate(raw_event))
    await _dispatch(dispatcher, payload, raw_event)
    await _dispatch(dispatcher, payload, raw_event)

    with server_conn.cursor() as cur:
        cur.execute(
            "SELECT kind, source->>'slack_event_id' FROM events "
            "WHERE stream = 'channel-list' AND kind = 'channel_member_changed'"
        )
        rows = cur.fetchall()
    assert rows == [("channel_member_changed", "EvNoMakeSource")]


@pytest.mark.trio
async def test_webhook_self_join_seeds_channel_membership_and_backfill(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    channel_info: JsonObject = {
        "ok": True,
        "channel": {
            "id": "C_NEW",
            "name": "incident-webhook",
            "is_channel": True,
            "is_private": False,
            "is_archived": False,
            "is_member": True,
            "topic": {"value": "", "creator": "", "last_set": 0},
            "purpose": {"value": "", "creator": "", "last_set": 0},
            "num_members": 2,
        },
    }

    def respond(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=channel_info)

    queued: list[str] = []
    with httpx.Client(base_url="https://slack.com/api", transport=httpx.MockTransport(respond)) as http:
        dispatcher = _dispatcher(
            server_conn,
            http,
            on_self_join=lambda channel_id: not queued.append(channel_id),
        )
        raw: JsonObject = {
            "type": "member_joined_channel",
            "channel": "C_NEW",
            "user": "U_SELF",
            "event_ts": "1700000000.000100",
        }
        payload = EventsApiPayload(event_id="EvSelfJoin", event=SocketEventPayload.model_validate(raw))
        await _dispatch(dispatcher, payload, raw)

    with server_conn.cursor() as cur:
        cur.execute(
            "SELECT kind, source->>'slack_event_id' FROM events "
            "WHERE stream = 'channel-list' ORDER BY offset_in_stream"
        )
        rows = cur.fetchall()
    assert rows == [
        ("channel_added", "EvSelfJoin"),
        ("channel_member_joined", "EvSelfJoin"),
    ]
    assert queued == ["C_NEW"]


@pytest.mark.trio
async def test_webhook_self_join_retries_when_backfill_queue_is_busy(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    channel_info: JsonObject = {
        "ok": True,
        "channel": {
            "id": "C_BUSY",
            "name": "incident-busy",
            "is_channel": True,
            "is_member": True,
        },
    }

    def respond(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=channel_info)

    with httpx.Client(base_url="https://slack.com/api", transport=httpx.MockTransport(respond)) as http:
        dispatcher = _dispatcher(server_conn, http, on_self_join=lambda _channel_id: False)
        raw: JsonObject = {
            "type": "member_joined_channel",
            "channel": "C_BUSY",
            "user": "U_SELF",
            "event_ts": "1700000000.000200",
        }
        payload = EventsApiPayload(event_id="EvBusyJoin", event=SocketEventPayload.model_validate(raw))
        with pytest.raises(DispatchTransientError) as caught:
            await _dispatch(dispatcher, payload, raw)

    assert caught.value.code is DispatchErrorCode.UNKNOWN_TRANSIENT
    with server_conn.cursor() as cur:
        cur.execute(
            "SELECT kind FROM events WHERE stream = 'channel-list' "
            "AND source->>'slack_event_id' = 'EvBusyJoin' ORDER BY offset_in_stream"
        )
        rows = cur.fetchall()
    assert rows == [("channel_added",), ("channel_member_joined",)]


@pytest.mark.trio
async def test_pg_timeout_becomes_typed_transient(fake_slack_http: httpx.Client) -> None:
    client = SlackClient("xoxp-test")
    client._http = fake_slack_http
    dispatcher = SlackEventDispatcher(
        cast(OffsetWriter, _TimeoutWriter()),
        client,
        "U_SELF",
        make_test_limiters(),
        cast(HealthEmitter, _NullHealth()),
    )
    event = SocketEventPayload(type="message", channel="C1", ts="1.000001")
    with pytest.raises(DispatchTransientError) as caught:
        await dispatcher.dispatch(
            EventsApiPayload(event_id="EvTimeout", event=event),
            {"type": "message", "channel": "C1", "ts": "1.000001"},
            SlackEventSource(transport="http", event_id="EvTimeout"),
        )
    assert caught.value.code is DispatchErrorCode.PG_TIMEOUT


@pytest.mark.trio
async def test_conversations_info_failure_becomes_typed_transient(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    def fail(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("unavailable", request=request)

    with httpx.Client(base_url="https://slack.com/api", transport=httpx.MockTransport(fail)) as http:
        dispatcher = _dispatcher(server_conn, http)
        raw: JsonObject = {"type": "channel_rename", "channel": "C1"}
        payload = EventsApiPayload(event_id="EvInfo", event=SocketEventPayload.model_validate(raw))
        with pytest.raises(DispatchTransientError) as caught:
            await _dispatch(dispatcher, payload, raw)
    assert caught.value.code is DispatchErrorCode.CONVERSATIONS_INFO_FAILED


@pytest.mark.trio
async def test_team_join_apply_failure_has_sanitized_code(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fail(*_args: object) -> None:
        await trio.lowlevel.checkpoint()
        raise WriterPoolExhausted("payload-like secret must not escape")

    monkeypatch.setattr(dispatcher_module, "apply_team_join_event", fail)
    dispatcher = _dispatcher(server_conn, fake_slack_http)
    raw: JsonObject = {"type": "team_join", "user": {"id": "U_NEW"}}
    payload = EventsApiPayload(event_id="EvTeam", event=SocketEventPayload.model_validate(raw))
    with pytest.raises(DispatchTransientError) as caught:
        await _dispatch(dispatcher, payload, raw)
    assert caught.value.code is DispatchErrorCode.TEAM_JOIN_APPLY_FAILED


@pytest.mark.trio
async def test_user_change_apply_failure_has_sanitized_code(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fail(*_args: object) -> None:
        await trio.lowlevel.checkpoint()
        raise httpx.ConnectError("payload-like secret must not escape")

    monkeypatch.setattr(dispatcher_module, "apply_user_change_event", fail)
    dispatcher = _dispatcher(server_conn, fake_slack_http)
    raw: JsonObject = {"type": "user_change", "user": {"id": "U_CHANGED"}}
    payload = EventsApiPayload(event_id="EvUser", event=SocketEventPayload.model_validate(raw))
    with pytest.raises(DispatchTransientError) as caught:
        await _dispatch(dispatcher, payload, raw)
    assert caught.value.code is DispatchErrorCode.USER_CHANGE_APPLY_FAILED
