# pyright: reportPrivateUsage=false
"""search.messages channel-total sweep persistence and pacing."""

from __future__ import annotations

from collections.abc import Mapping

import httpx
import psycopg
import pytest
import trio
from psycopg.rows import TupleRow
from psycopg.types.json import Jsonb

from slack_fuse_server.search_messages import SearchMessagesError, SearchMessageTotal
from slack_fuse_server.slurper.__main__ import _build_parser
from slack_fuse_server.slurper.api import SlackClient
from slack_fuse_server.slurper.channel_totals import refresh_channel_totals_once
from slack_fuse_server.slurper.limiters import SlackTierPacer
from tests.conftest import make_test_limiters, make_test_writer


def _insert_channel(
    conn: psycopg.Connection[TupleRow],
    offset: int,
    channel_id: str,
    name: str,
    *,
    is_im: bool = False,
) -> None:
    payload: Mapping[str, object] = {
        "id": channel_id,
        "name": name,
        "is_im": is_im,
        "is_member": True,
        "is_archived": False,
    }
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO events (stream, offset_in_stream, kind, payload)
            VALUES ('channel-list', %s, 'channel_added', %s)
            """,
            (offset, Jsonb(dict(payload))),
        )


def _empty_listing_client() -> SlackClient:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["exclude_archived"] == "false"
        return httpx.Response(
            200,
            json={"ok": True, "channels": [], "response_metadata": {"next_cursor": ""}},
        )

    transport = httpx.MockTransport(handler)
    client = SlackClient("xoxp-test")
    client._http.close()
    client._http = httpx.Client(transport=transport)
    return client


@pytest.mark.trio
async def test_refresh_sweep_throttles_upserts_and_preserves_old_total_on_error(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    _insert_channel(server_conn, 1, "C1", "alpha")
    _insert_channel(server_conn, 2, "C2", "beta")
    _insert_channel(server_conn, 3, "C3", "gamma")
    _insert_channel(server_conn, 4, "D1", "dm-user", is_im=True)
    with server_conn.cursor() as cur:
        cur.execute("INSERT INTO channel_message_totals (channel_id, total, refresh_status) VALUES ('C2', 42, 'ok')")

    sleeps: list[float] = []
    now = 0.0

    async def fake_sleep(delay: float) -> None:
        nonlocal now
        sleeps.append(delay)
        now += delay
        await trio.lowlevel.checkpoint()

    tier2 = SlackTierPacer(3.5, clock=lambda: now, sleep=fake_sleep)
    calls: list[str] = []

    def search(_http: httpx.Client, name: str) -> SearchMessageTotal:
        calls.append(name)
        if name == "beta":
            raise SearchMessagesError("temporary failure")
        return SearchMessageTotal(total={"alpha": 10, "gamma": 12_000}[name], approximate=name == "gamma")

    client = _empty_listing_client()
    try:
        await refresh_channel_totals_once(
            make_test_writer(server_conn),
            client,
            make_test_limiters(slack_tier2=tier2),
            search_fn=search,
        )
    finally:
        client.close()

    assert calls == ["alpha", "beta", "gamma"]
    assert sleeps == [3.5, 3.5]
    with server_conn.cursor() as cur:
        cur.execute("SELECT channel_id, total, refresh_status FROM channel_message_totals ORDER BY channel_id")
        rows = [(str(row[0]), int(row[1]), str(row[2])) for row in cur.fetchall()]
    assert rows == [
        ("C1", 10, "ok"),
        ("C2", 42, "error"),
        ("C3", 12_000, "approximate"),
        ("D1", 0, "unavailable"),
    ]


def test_refresh_channel_totals_cli_registered() -> None:
    args = _build_parser().parse_args(["refresh-channel-totals"])
    assert args.command == "refresh-channel-totals"


@pytest.mark.trio
async def test_refresh_channel_totals_rejects_bot_token(server_conn: psycopg.Connection[TupleRow]) -> None:
    client = SlackClient("xoxb-test")
    try:
        with pytest.raises(ValueError, match="user token"):
            await refresh_channel_totals_once(
                make_test_writer(server_conn),
                client,
                make_test_limiters(),
            )
    finally:
        client.close()
