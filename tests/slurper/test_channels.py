# pyright: reportPrivateUsage=false
"""Channel-list stream ingestion: startup populate (conversations.list)."""

from __future__ import annotations

from typing import cast

import httpx
import psycopg
import pytest
import trio
from psycopg import Cursor
from psycopg.rows import TupleRow

import slack_fuse_server.slurper.channels as channels_module
from slack_fuse_server.slurper.api import ChannelNotFoundError, SlackAPIError, SlackClient
from slack_fuse_server.slurper.channels import ensure_channel_added, populate_channels_once
from slack_fuse_server.slurper.socket import _channel_added_write
from tests._fake_slack import load_fixtures, make_fake_slack_transport
from tests.conftest import make_test_limiters, make_test_writer


def _make_client(http: httpx.Client) -> SlackClient:
    client = SlackClient("xoxp-test")
    client._http = http
    return client


def _channel_rows(conn: psycopg.Connection[TupleRow]) -> list[tuple[int, str, object]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT offset_in_stream, kind, payload FROM events "
            "WHERE stream = 'channel-list' ORDER BY offset_in_stream",
        )
        return [(int(r[0]), str(r[1]), r[2]) for r in cur.fetchall()]


def _channel_list_next_offset(conn: psycopg.Connection[TupleRow]) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT next_offset FROM stream_heads WHERE stream = 'channel-list'")
        row = cur.fetchone()
    assert row is not None
    return int(row[0])


def test_populate_channels_once_writes_channel_added_events(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    writer = make_test_writer(server_conn)
    trio.run(populate_channels_once, writer, _make_client(fake_slack_http), make_test_limiters())

    rows = _channel_rows(server_conn)
    assert [kind for _, kind, _ in rows] == ["channel_added", "channel_added"]
    channel_ids: list[str] = []
    for _, _, payload in rows:
        assert isinstance(payload, dict)
        payload_dict = cast(dict[str, object], payload)
        raw_id = payload_dict.get("id")
        assert isinstance(raw_id, str)
        channel_ids.append(raw_id)
    # Fixture conversations.list returns a public channel + an IM; populate emits
    # one channel_added per conversation regardless of type.
    assert channel_ids == ["C0001", "D0001"]
    assert _channel_list_next_offset(server_conn) == 3


def test_populate_channels_once_is_idempotent_on_restart(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    writer = make_test_writer(server_conn)
    client = _make_client(fake_slack_http)

    trio.run(populate_channels_once, writer, client, make_test_limiters())
    trio.run(populate_channels_once, writer, client, make_test_limiters())

    rows = _channel_rows(server_conn)
    assert len(rows) == 2
    assert _channel_list_next_offset(server_conn) == 3


def test_populate_payload_matches_live_socket_mode_shape(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    """A populate `channel_added` payload is byte-identical to the live path's.

    The client projector's `apply_event` only ever sees `(kind, payload)`, so
    proving the payloads match proves a startup populate event is processed
    identically to a live socket-mode `channel_created` / `im_created`.
    """
    client = _make_client(fake_slack_http)
    writer = make_test_writer(server_conn)
    trio.run(populate_channels_once, writer, client, make_test_limiters())

    populated: dict[str, dict[str, object]] = {}
    for _, kind, payload in _channel_rows(server_conn):
        if kind != "channel_added":
            continue
        assert isinstance(payload, dict)
        payload_dict = cast(dict[str, object], payload)
        raw_id = payload_dict["id"]
        assert isinstance(raw_id, str)
        populated[raw_id] = payload_dict

    # Build the live-path payload for the same channels and compare.
    for validated in client.list_conversations():
        live_record = _channel_added_write(validated.raw)
        assert live_record.kind == "channel_added"
        assert live_record.stream == "channel-list"
        assert populated[validated.model.id] == live_record.payload


# ----------------------------------------------------------------------
# ensure_channel_added — pre-backfill channel discovery
# ----------------------------------------------------------------------


def test_ensure_channel_added_emits_when_channel_not_previously_seen(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    """Fresh server: no prior channel_added events. ensure_channel_added calls
    conversations.info, writes the synthetic event, returns True.
    """
    writer = make_test_writer(server_conn)
    client = _make_client(fake_slack_http)

    emitted = trio.run(ensure_channel_added, writer, client, "C0001", make_test_limiters())

    assert emitted is True
    rows = _channel_rows(server_conn)
    assert len(rows) == 1
    _, kind, payload = rows[0]
    assert kind == "channel_added"
    assert isinstance(payload, dict)
    payload_dict = cast(dict[str, object], payload)
    assert payload_dict["id"] == "C0001"


def test_ensure_channel_added_fetches_channel_info_before_stream_lock(
    server_conn: psycopg.Connection[TupleRow],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    order: list[str] = []
    info_response = load_fixtures()["conversations.info"]

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/conversations.info"
        order.append("http")
        return httpx.Response(200, json=info_response)

    original_lock = channels_module._lock_channel_list_stream

    def recording_lock(cur: Cursor[TupleRow]) -> None:
        order.append("lock")
        original_lock(cur)

    monkeypatch.setattr(channels_module, "_lock_channel_list_stream", recording_lock)
    writer = make_test_writer(server_conn)
    with httpx.Client(base_url="https://slack.com/api", transport=httpx.MockTransport(handler)) as http_client:
        emitted = trio.run(ensure_channel_added, writer, _make_client(http_client), "C0001", make_test_limiters())

    assert emitted is True
    assert order == ["http", "lock"]


def test_ensure_channel_added_is_idempotent_on_repeat(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    """Second call sees the existing event and returns False without writing."""
    writer = make_test_writer(server_conn)
    client = _make_client(fake_slack_http)

    first = trio.run(ensure_channel_added, writer, client, "C0001", make_test_limiters())
    second = trio.run(ensure_channel_added, writer, client, "C0001", make_test_limiters())

    assert first is True
    assert second is False
    assert len(_channel_rows(server_conn)) == 1


def test_ensure_channel_added_is_noop_after_populate(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    """If `populate_channels_once` has already emitted channel_added for this
    channel, ensure_channel_added detects it and skips the conversations.info
    call. (Exercises the same dedup path as repeat ensure_channel_added.)"""
    writer = make_test_writer(server_conn)
    client = _make_client(fake_slack_http)

    trio.run(populate_channels_once, writer, client, make_test_limiters())
    before_offset = _channel_list_next_offset(server_conn)

    emitted = trio.run(ensure_channel_added, writer, client, "C0001", make_test_limiters())

    assert emitted is False
    assert _channel_list_next_offset(server_conn) == before_offset


def test_ensure_channel_added_raises_channel_not_found_for_inaccessible(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    """conversations.info ok=false channel_not_found → ChannelNotFoundError
    (a SlackAPIError subclass). The admin backfill flow catches this specific
    subclass and skips the channel cleanly so the Job exits 0 — the user
    token no longer has access to the channel; failing the whole Job for an
    expected condition is unhelpful. The existing broad SlackAPIError catch
    still works for the same reason."""
    transport = make_fake_slack_transport(overrides={"conversations.info": {"ok": False, "error": "channel_not_found"}})
    http_client = httpx.Client(transport=transport, base_url="https://slack.com/api")
    client = SlackClient("xoxp-test")
    client._http = http_client
    writer = make_test_writer(server_conn)

    # Specific subclass for callers that want skip-not-fail semantics.
    with pytest.raises(ChannelNotFoundError):
        _ = trio.run(ensure_channel_added, writer, client, "C-GONE", make_test_limiters())

    # Subclass relationship: existing broad handlers still catch it.
    assert issubclass(ChannelNotFoundError, SlackAPIError)

    # No row was written.
    assert _channel_rows(server_conn) == []
