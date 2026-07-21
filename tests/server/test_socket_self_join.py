# pyright: reportPrivateUsage=false
"""Socket-mode self membership transitions."""

from __future__ import annotations

from typing import cast

import httpx
import psycopg
import pytest
import trio
from psycopg.rows import TupleRow

from slack_fuse.models import JsonObject, SocketEventPayload
from slack_fuse_server.slurper.api import SlackClient
from slack_fuse_server.slurper.health import HealthEmitter
from slack_fuse_server.slurper.socket import SocketModeOptions, SocketModeRunner
from tests._fake_slack import make_fake_slack_transport
from tests.conftest import make_test_limiters, make_test_writer


class _RecordingBackfill:
    def __init__(self) -> None:
        self.channel_ids: list[str] = []

    def __call__(self, channel_id: str) -> bool:
        self.channel_ids.append(channel_id)
        return True


def _channel_info(channel_id: str, *, is_member: bool = True) -> JsonObject:
    return {
        "ok": True,
        "channel": {
            "id": channel_id,
            "name": "incident-88",
            "is_channel": True,
            "is_private": False,
            "is_archived": False,
            "is_member": is_member,
            "topic": {"value": "incident coordination", "creator": "U_SELF", "last_set": 1},
            "purpose": {"value": "", "creator": "", "last_set": 0},
            "num_members": 2,
        },
    }


def _make_runner(
    conn: psycopg.Connection[TupleRow],
    http: httpx.Client,
    backfill: _RecordingBackfill,
    *,
    self_user_id: str | None = "U_SELF",
) -> SocketModeRunner:
    client = SlackClient("xoxp-test")
    client._http = http
    writer = make_test_writer(conn)
    options = SocketModeOptions(self_user_id=self_user_id, on_self_join=backfill)
    return SocketModeRunner(
        writer,
        HealthEmitter(writer),
        client,
        "xapp-test",
        limiters=make_test_limiters(),
        options=options,
    )


def _rows(conn: psycopg.Connection[TupleRow]) -> list[tuple[str, JsonObject, JsonObject | None]]:
    with conn.cursor() as cur:
        cur.execute("SELECT kind, payload, source FROM events WHERE stream = 'channel-list' ORDER BY offset_in_stream")
        rows = cur.fetchall()
    return [
        (
            str(kind),
            cast(JsonObject, payload),
            cast(JsonObject | None, source),
        )
        for kind, payload, source in rows
    ]


def test_self_join_seeds_channel_records_membership_and_queues_backfill(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    transport = make_fake_slack_transport(overrides={"conversations.info": _channel_info("C_NEW")})
    backfill = _RecordingBackfill()
    with httpx.Client(base_url="https://slack.com/api", transport=transport) as http:
        runner = _make_runner(server_conn, http, backfill)
        raw_event: JsonObject = {
            "type": "member_joined_channel",
            "channel": "C_NEW",
            "user": "U_SELF",
            "inviter": "U_INVITER",
            "event_ts": "1700000000.000100",
        }
        event = SocketEventPayload.model_validate(raw_event)

        trio.run(runner._handle_event, event, raw_event)
        trio.run(runner._handle_event, event, raw_event)

    rows = _rows(server_conn)
    assert [kind for kind, _, _ in rows] == ["channel_added", "channel_member_joined"]
    added = rows[0]
    assert added[1]["id"] == "C_NEW"
    assert added[1]["is_member"] is True
    assert added[2] is not None and added[2]["triggered_by"] == "self-join"
    assert rows[1][1] == {
        "channel_id": "C_NEW",
        "user_id": "U_SELF",
        "inviter_id": "U_INVITER",
        "event_ts": "1700000000.000100",
    }
    assert backfill.channel_ids and set(backfill.channel_ids) == {"C_NEW"}


def test_other_user_join_only_records_membership(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return httpx.Response(500)

    backfill = _RecordingBackfill()
    with httpx.Client(base_url="https://slack.com/api", transport=httpx.MockTransport(handler)) as http:
        runner = _make_runner(server_conn, http, backfill)
        raw_event: JsonObject = {
            "type": "member_joined_channel",
            "channel": "C_NEW",
            "user": "U_OTHER",
            "event_ts": "1700000000.000100",
        }
        event = SocketEventPayload.model_validate(raw_event)

        trio.run(runner._handle_event, event, raw_event)

    assert [kind for kind, _, _ in _rows(server_conn)] == ["channel_member_joined"]
    assert calls == []
    assert backfill.channel_ids == []


def test_self_leave_marks_membership_false_without_purging_history(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    backfill = _RecordingBackfill()
    transport = make_fake_slack_transport()
    with httpx.Client(base_url="https://slack.com/api", transport=transport) as http:
        runner = _make_runner(server_conn, http, backfill)
        raw_event: JsonObject = {
            "type": "member_left_channel",
            "channel": "C_KNOWN",
            "user": "U_SELF",
            "event_ts": "1700000001.000200",
        }
        event = SocketEventPayload.model_validate(raw_event)

        trio.run(runner._handle_event, event, raw_event)

    rows = _rows(server_conn)
    assert [kind for kind, _, _ in rows] == ["channel_member_left", "channel_member_changed"]
    assert rows[1][1] == {"channel_id": "C_KNOWN", "is_member": False}
    assert backfill.channel_ids == []


def test_self_join_info_failure_still_records_membership(
    server_conn: psycopg.Connection[TupleRow],
    caplog: pytest.LogCaptureFixture,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("conversations.info unavailable", request=request)

    backfill = _RecordingBackfill()
    caplog.set_level("INFO", logger="slack_fuse_server.slurper.socket")
    with httpx.Client(base_url="https://slack.com/api", transport=httpx.MockTransport(handler)) as http:
        runner = _make_runner(server_conn, http, backfill)
        raw_event: JsonObject = {
            "type": "member_joined_channel",
            "channel": "C_NEW",
            "user": "U_SELF",
            "event_ts": "1700000000.000100",
        }
        event = SocketEventPayload.model_validate(raw_event)

        trio.run(runner._handle_event, event, raw_event)

    assert [kind for kind, _, _ in _rows(server_conn)] == ["channel_member_joined"]
    assert backfill.channel_ids == []
    assert "conversations.info failed for C_NEW" in caplog.text


def test_open_socket_identifies_current_user_via_auth_test(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    transport = make_fake_slack_transport(
        overrides={
            "auth.test": {"ok": True, "user_id": "U_SELF"},
            "apps.connections.open": {"ok": True, "url": "wss://socket.test/link"},
        }
    )
    backfill = _RecordingBackfill()
    with httpx.Client(base_url="https://slack.com/api", transport=transport) as http:
        runner = _make_runner(server_conn, http, backfill, self_user_id=None)

        assert runner._open_socket() == "wss://socket.test/link"

    assert runner.self_user_id == "U_SELF"
