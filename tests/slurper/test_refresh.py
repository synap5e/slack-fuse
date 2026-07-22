# pyright: reportPrivateUsage=false
"""``conversations.info`` refresh sweep semantics.

The sweep is the legacy-backfill + drift catcher for channel metadata.
Pinning the three behaviours it MUST preserve:

1. Diff-and-emit only on change — a stable workspace shouldn't grow the
   events table on every cycle.
2. Idempotent vs legacy-vs-raw payload shape — when an existing
   ``channel_added`` event was written via the old lossy ``model_dump``
   path, a new raw refresh DOES differ and we emit (this is how legacy
   channels finally get their ``created`` field captured).
3. Skip cleanly on ``channel_not_found`` — channels we no longer have
   access to don't poison the cycle.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import httpx
import pytest
import trio

from slack_fuse_server._json import JsonObject
from slack_fuse_server.slurper.api import SlackClient
from slack_fuse_server.slurper.offsets import EventRecord, OffsetWriter, write_event
from slack_fuse_server.slurper.refresh import RefreshTrigger, _refresh_all_once, refresh_channels_once
from tests._fake_slack import load_fixtures
from tests.conftest import RecordingSupervisor, make_test_limiters, make_test_writer

if TYPE_CHECKING:
    import psycopg
    from psycopg.rows import TupleRow


@pytest.fixture(autouse=True)
def _isolate_info_refresh_from_inventory_discovery(  # pyright: ignore[reportUnusedFunction]
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Discovery has dedicated integration tests; keep these info-diff tests focused."""

    async def skip_discovery(*_args: object, **_kwargs: object) -> None:
        await trio.lowlevel.checkpoint()

    monkeypatch.setattr("slack_fuse_server.slurper.refresh.populate_channels_once", skip_discovery)


def _fake_client(http: httpx.Client) -> SlackClient:
    client = SlackClient("xoxp-test")
    client._http = http  # pyright: ignore[reportPrivateUsage]
    return client


def _seed_channel_added(
    conn: psycopg.Connection[TupleRow],
    *,
    payload: JsonObject,
) -> None:
    record = EventRecord(stream="channel-list", kind="channel_added", ts=None, payload=payload)
    write_event(conn, record)


def _channel_list_events(
    conn: psycopg.Connection[TupleRow],
) -> list[tuple[str, dict[str, object]]]:
    with conn.cursor() as cur:
        cur.execute("SELECT kind, payload FROM events WHERE stream='channel-list' ORDER BY offset_in_stream")
        return [(str(r[0]), cast("dict[str, object]", r[1])) for r in cur.fetchall()]


def _conversations_info_fixture() -> JsonObject:
    """Pull the conversations.info channel fixture used by the fake transport."""
    fixtures = load_fixtures()
    info = fixtures.get("conversations.info") or fixtures["conversations.list"]
    if "channel" in info:
        return cast(JsonObject, info["channel"])
    # Fall back to the first conversations.list entry.
    channels = info.get("channels")
    assert isinstance(channels, list) and channels
    return cast(JsonObject, channels[0])


def test_refresh_emits_channel_info_refreshed_when_legacy_payload_differs(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Legacy backfill case: existing channel_added has the old
    ``model_dump`` shape (missing fields like ``created``). The refresh
    cycle's raw payload differs → emit ``channel_info_refreshed``."""
    fixture_channel = _conversations_info_fixture()
    channel_id = str(fixture_channel["id"])
    # Seed a LOSSY channel_added payload (drop fields that the raw would
    # carry, e.g. ``created``, ``is_general``).
    lossy: JsonObject = {
        "id": channel_id,
        "name": str(fixture_channel.get("name", "")),
        "is_im": False,
        "is_mpim": False,
        "is_member": True,
        # No `created`, no `is_general`, no nested topic dict — the lossy
        # shape the slurper produced pre-2026-06-27.
    }
    _seed_channel_added(server_conn, payload=lossy)
    caplog.set_level("INFO", logger="slack_fuse_server.slurper.spans")

    writer = make_test_writer(server_conn)
    client = _fake_client(fake_slack_http)
    trio.run(refresh_channels_once, writer, client, make_test_limiters())

    events = _channel_list_events(server_conn)
    refreshed = [(k, p) for k, p in events if k == "channel_info_refreshed"]
    assert len(refreshed) == 1
    refreshed_payload = refreshed[0][1]
    # The refreshed payload comes from the raw conversations.info response
    # which DOES carry whatever Slack sends — so it must differ from the
    # lossy seed.
    assert refreshed_payload != lossy
    assert refreshed_payload["id"] == channel_id
    assert "op=slurper.refresh.refresh_channel" in caplog.text
    assert "result=ok" in caplog.text
    assert "changed=True" in caplog.text
    assert f"channel_id={channel_id}" in caplog.text


def test_refresh_is_idempotent_when_payload_unchanged(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    """Steady-state case: if the most recent payload already matches the
    fresh fetch, no new event is emitted. Two consecutive cycles produce
    one event total (the first), not two."""
    writer = make_test_writer(server_conn)
    client = _fake_client(fake_slack_http)
    # Seed the events table with the EXACT raw payload Slack will return,
    # so the first refresh sees no diff.
    fixture_channel = _conversations_info_fixture()
    _seed_channel_added(server_conn, payload=fixture_channel)

    trio.run(refresh_channels_once, writer, client, make_test_limiters())
    trio.run(refresh_channels_once, writer, client, make_test_limiters())

    events = _channel_list_events(server_conn)
    refreshed = [(k, p) for k, p in events if k == "channel_info_refreshed"]
    assert refreshed == []


def test_refresh_skips_blocked_channels(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    fixture_channel = _conversations_info_fixture()
    channel_id = str(fixture_channel["id"])
    _seed_channel_added(
        server_conn,
        payload={
            "id": channel_id,
            "name": str(fixture_channel.get("name", "")),
            "is_member": True,
        },
    )
    with server_conn.cursor() as cur:
        cur.execute("INSERT INTO blocked_channels (channel_id) VALUES (%s)", (channel_id,))

    writer = make_test_writer(server_conn)
    client = _fake_client(fake_slack_http)
    trio.run(refresh_channels_once, writer, client, make_test_limiters())

    events = _channel_list_events(server_conn)
    assert [kind for kind, _payload in events] == ["channel_added"]


def test_refresh_all_once_declares_phase_sequence(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    fixture_channel = _conversations_info_fixture()
    channel_id = str(fixture_channel["id"])
    _seed_channel_added(server_conn, payload=fixture_channel)
    supervisor = RecordingSupervisor()

    trio.run(
        _refresh_all_once,
        make_test_writer(server_conn),
        _fake_client(fake_slack_http),
        make_test_limiters(),
        supervisor,
    )

    phases = [(item.task_name, item.phase, item.details) for item in supervisor.declarations]
    assert ("refresh", "listing_channels", None) in phases
    assert ("refresh", "refreshing_channel", {"channel_id": channel_id}) in phases


def test_refresh_trigger_consume_declares_waiting_and_running(monkeypatch: pytest.MonkeyPatch) -> None:
    trigger = RefreshTrigger()
    supervisor = RecordingSupervisor()
    ran: list[str] = []

    async def _fake_refresh_one(
        _writer: object,
        _client: object,
        channel_id: str,
        _limiters: object,
        **_kwargs: object,
    ) -> bool:
        ran.append(channel_id)
        await trio.lowlevel.checkpoint()
        return False

    monkeypatch.setattr("slack_fuse_server.slurper.refresh._refresh_one", _fake_refresh_one)

    async def go() -> None:
        async with trio.open_nursery() as nursery:
            nursery.start_soon(
                trigger.consume,
                cast(OffsetWriter, object()),
                cast(SlackClient, object()),
                make_test_limiters(),
                supervisor,
            )
            await trio.sleep(0.01)
            assert trigger.request_channel("C_PHASE") is True
            await trio.sleep(0.05)
            nursery.cancel_scope.cancel()

    trio.run(go)

    assert ran == ["C_PHASE"]
    phases = [(item.task_name, item.phase, item.details) for item in supervisor.declarations]
    assert ("refresh-trigger", "waiting_for_trigger", None) in phases
    assert ("refresh-trigger", "running", {"channel_id": "C_PHASE"}) in phases
