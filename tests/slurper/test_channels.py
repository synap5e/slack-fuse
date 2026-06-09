# pyright: reportPrivateUsage=false
"""Channel-list stream ingestion: startup populate (conversations.list)."""

from __future__ import annotations

from typing import cast

import httpx
import psycopg
import trio
from psycopg.rows import TupleRow

from slack_fuse_server.slurper.api import SlackClient
from slack_fuse_server.slurper.channels import populate_channels_once
from slack_fuse_server.slurper.offsets import OffsetWriter
from slack_fuse_server.slurper.socket import _channel_added_write


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
    writer = OffsetWriter(server_conn, trio.CapacityLimiter(1))
    trio.run(populate_channels_once, writer, _make_client(fake_slack_http))

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
    writer = OffsetWriter(server_conn, trio.CapacityLimiter(1))
    client = _make_client(fake_slack_http)

    trio.run(populate_channels_once, writer, client)
    trio.run(populate_channels_once, writer, client)

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
    writer = OffsetWriter(server_conn, trio.CapacityLimiter(1))
    trio.run(populate_channels_once, writer, client)

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
    for channel in client.list_conversations():
        live_record = _channel_added_write(channel)
        assert live_record.kind == "channel_added"
        assert live_record.stream == "channel-list"
        assert populated[channel.id] == live_record.payload
