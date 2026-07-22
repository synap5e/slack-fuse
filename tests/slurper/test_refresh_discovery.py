# pyright: reportPrivateUsage=false
"""Periodic refresh discovers channels before refreshing known metadata."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, cast

import httpx
import pytest
import trio

from slack_fuse_server._json import JsonObject
from slack_fuse_server.slurper.api import SlackClient
from slack_fuse_server.slurper.ingestion import IngestionContext, ingesting
from slack_fuse_server.slurper.offsets import EventRecord, write_event
from slack_fuse_server.slurper.refresh import _refresh_all_once
from tests.conftest import make_test_limiters, make_test_writer

if TYPE_CHECKING:
    import psycopg
    from psycopg.rows import TupleRow


def _channel(channel_id: str) -> JsonObject:
    return {
        "id": channel_id,
        "name": channel_id.lower(),
        "is_im": False,
        "is_mpim": False,
        "is_member": True,
        "is_archived": False,
        "topic": {"value": "", "creator": "", "last_set": 0},
        "purpose": {"value": "", "creator": "", "last_set": 0},
        "num_members": 1,
    }


def _client(handler: Callable[[httpx.Request], httpx.Response]) -> tuple[SlackClient, httpx.Client]:
    http = httpx.Client(base_url="https://slack.com/api", transport=httpx.MockTransport(handler))
    client = SlackClient("xoxp-test")
    client._http = http
    return client, http


def _seed(conn: psycopg.Connection[TupleRow], channel_id: str) -> None:
    offset = write_event(
        conn,
        EventRecord(stream="channel-list", kind="channel_added", ts=None, payload=_channel(channel_id)),
    )
    assert offset is not None


def _added_rows(conn: psycopg.Connection[TupleRow]) -> list[tuple[str, JsonObject, JsonObject | None]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT payload->>'id', payload, source
            FROM events
            WHERE stream = 'channel-list' AND kind = 'channel_added'
            ORDER BY offset_in_stream
            """
        )
        rows = cur.fetchall()
    return [
        (
            str(channel_id),
            cast(JsonObject, payload),
            cast("JsonObject | None", source),
        )
        for channel_id, payload, source in rows
    ]


@pytest.fixture(autouse=True)
def _no_refresh_sleep(monkeypatch: pytest.MonkeyPatch) -> None:  # pyright: ignore[reportUnusedFunction]
    monkeypatch.setattr("slack_fuse_server.slurper.refresh._PER_CHANNEL_SLEEP_S", 0.0)


def test_discovery_inserts_only_new_channels_and_preserves_scheduled_source(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    known_ids = ("C_A", "C_B")
    discovered_ids = (*known_ids, "C_C_NEW")
    for channel_id in known_ids:
        _seed(server_conn, channel_id)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/conversations.list":
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "channels": [_channel(channel_id) for channel_id in discovered_ids],
                    "response_metadata": {"next_cursor": ""},
                },
            )
        channel_id = request.url.params.get("channel", "")
        return httpx.Response(200, json={"ok": True, "channel": _channel(channel_id)})

    client, http = _client(handler)
    try:
        context = IngestionContext(
            producer="refresh-test",
            boot_id="boot-refresh-test",
            task_id="task-refresh-test",
            triggered_by="scheduled",
        )
        with ingesting(context):
            trio.run(_refresh_all_once, make_test_writer(server_conn), client, make_test_limiters())
    finally:
        http.close()

    rows = _added_rows(server_conn)
    assert [channel_id for channel_id, _payload, _source in rows] == [*known_ids, "C_C_NEW"]
    new_source = rows[-1][2]
    assert new_source is not None
    assert new_source["triggered_by"] == "scheduled"


def test_discovery_failure_warns_and_still_refreshes_known_channels(
    server_conn: psycopg.Connection[TupleRow],
    caplog: pytest.LogCaptureFixture,
) -> None:
    _seed(server_conn, "C_A")
    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(request.url.path)
        if request.url.path == "/api/conversations.list":
            raise httpx.ConnectError("inventory unavailable", request=request)
        channel_id = request.url.params.get("channel", "")
        return httpx.Response(200, json={"ok": True, "channel": _channel(channel_id)})

    client, http = _client(handler)
    caplog.set_level("WARNING", logger="slack_fuse_server.slurper.channels")
    try:
        trio.run(_refresh_all_once, make_test_writer(server_conn), client, make_test_limiters())
    finally:
        http.close()

    assert requested == ["/api/conversations.list", "/api/conversations.info"]
    assert "channels: startup populate failed" in caplog.text


def test_discovery_span_reports_newly_added_count(
    server_conn: psycopg.Connection[TupleRow],
    caplog: pytest.LogCaptureFixture,
) -> None:
    _seed(server_conn, "C_A")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/conversations.list":
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "channels": [_channel("C_A"), _channel("C_NEW")],
                    "response_metadata": {"next_cursor": ""},
                },
            )
        channel_id = request.url.params.get("channel", "")
        return httpx.Response(200, json={"ok": True, "channel": _channel(channel_id)})

    client, http = _client(handler)
    caplog.set_level("INFO", logger="slack_fuse_server.slurper.spans")
    try:
        trio.run(_refresh_all_once, make_test_writer(server_conn), client, make_test_limiters())
    finally:
        http.close()

    assert "op=slurper.refresh.discover_channels" in caplog.text
    assert "result=ok" in caplog.text
    assert "newly_added_count=1" in caplog.text
