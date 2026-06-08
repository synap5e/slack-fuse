# pyright: reportPrivateUsage=false
"""Users stream ingestion: startup populate + live user-change handling."""

from __future__ import annotations

import json
from typing import cast

import httpx
import psycopg
import trio
from psycopg.rows import TupleRow

from slack_fuse.models import JsonObject, SocketEventPayload
from slack_fuse_server.slurper.api import SlackClient
from slack_fuse_server.slurper.offsets import OffsetWriter
from slack_fuse_server.slurper.users import (
    _parse_envelope_allow_user_change,
    apply_user_change_event,
    populate_users_once,
)
from tests._fake_slack import make_fake_slack_transport


def _make_client(http: httpx.Client) -> SlackClient:
    client = SlackClient("xoxp-test")
    client._http = http
    return client


def _user_rows(conn: psycopg.Connection[TupleRow]) -> list[tuple[int, str, object]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT offset_in_stream, kind, payload FROM events WHERE stream = 'users' ORDER BY offset_in_stream",
        )
        return [(int(r[0]), str(r[1]), r[2]) for r in cur.fetchall()]


def _users_next_offset(conn: psycopg.Connection[TupleRow]) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT next_offset FROM stream_heads WHERE stream = 'users'")
        row = cur.fetchone()
    assert row is not None
    return int(row[0])


def test_populate_users_once_writes_user_added_events(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    writer = OffsetWriter(server_conn, trio.CapacityLimiter(1))
    trio.run(populate_users_once, writer, _make_client(fake_slack_http))

    rows = _user_rows(server_conn)
    assert [kind for _, kind, _ in rows] == ["user_added", "user_added"]
    user_ids: list[str] = []
    for _, _, payload in rows:
        assert isinstance(payload, dict)
        payload_dict = cast(dict[str, object], payload)
        raw_id = payload_dict.get("id")
        assert isinstance(raw_id, str)
        user_ids.append(raw_id)
    assert user_ids == ["U0001", "U0002"]
    assert _users_next_offset(server_conn) == 3


def test_populate_users_once_is_idempotent_on_restart(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    writer = OffsetWriter(server_conn, trio.CapacityLimiter(1))
    client = _make_client(fake_slack_http)

    trio.run(populate_users_once, writer, client)
    trio.run(populate_users_once, writer, client)

    rows = _user_rows(server_conn)
    assert len(rows) == 2
    assert _users_next_offset(server_conn) == 3


def test_apply_user_change_event_emits_user_renamed(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    writer = OffsetWriter(server_conn, trio.CapacityLimiter(1))
    trio.run(populate_users_once, writer, _make_client(fake_slack_http))

    users_info_override = cast(
        JsonObject,
        {
            "ok": True,
            "user": {
                "id": "U0001",
                "name": "alice",
                "profile": {"display_name": "Alice Renamed", "real_name": "Alice Anderson"},
            },
        },
    )
    transport = make_fake_slack_transport(overrides={"users.info": users_info_override})
    with httpx.Client(base_url="https://slack.com/api", transport=transport) as renamed_http:
        renamed_client = _make_client(renamed_http)
        event = SocketEventPayload(type="user_change", user="U0001")
        trio.run(apply_user_change_event, writer, renamed_client, event)

    rows = _user_rows(server_conn)
    renamed_payloads = [payload for _, kind, payload in rows if kind == "user_renamed"]
    assert len(renamed_payloads) == 1
    renamed = renamed_payloads[0]
    assert isinstance(renamed, dict)
    assert renamed == {"user_id": "U0001", "new_display_name": "Alice Renamed"}


def test_parse_envelope_allows_user_change_payload_with_nested_user() -> None:
    raw = json.dumps(
        {
            "type": "events_api",
            "envelope_id": "env-1",
            "payload": {
                "event": {
                    "type": "user_change",
                    "user": {
                        "id": "U0001",
                        "name": "alice",
                        "profile": {"display_name": "Alice", "real_name": "Alice Anderson"},
                    },
                }
            },
        },
    )
    envelope = _parse_envelope_allow_user_change(raw)
    assert envelope is not None
    assert envelope.payload is not None
    assert envelope.payload.event.type == "user_change"
    assert envelope.payload.event.user == "U0001"
