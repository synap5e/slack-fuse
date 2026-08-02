"""Channel message-count probe fact shape and channel selection."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import httpx
import pytest
import trio

import slack_fuse_server.probes.channel_message_count as probe_module
from slack_fuse_server.probes.channel_message_count import probe_channel_message_counts
from slack_fuse_server.probes.registry import ProbeDeps
from slack_fuse_server.probes.sweep import EventsTableProbeCursor
from slack_fuse_server.search_messages import SearchMessageTotal
from slack_fuse_server.slurper.api import SlackClient
from slack_fuse_server.slurper.offsets import EventRecord, write_event
from tests.conftest import make_test_limiters, make_test_writer

if TYPE_CHECKING:
    import psycopg
    from psycopg.rows import TupleRow


@dataclass(frozen=True, slots=True)
class _Channel:
    channel_id: str
    name: str
    is_im: bool = False
    is_archived: bool = False


def _seed_channel(conn: psycopg.Connection[TupleRow], channel: _Channel) -> None:
    write_event(
        conn,
        EventRecord(
            stream="channel-list",
            kind="channel_added",
            ts=None,
            payload={
                "id": channel.channel_id,
                "name": channel.name,
                "is_member": True,
                "is_im": channel.is_im,
                "is_archived": channel.is_archived,
            },
        ),
    )


def _make_deps(conn: psycopg.Connection[TupleRow], client: SlackClient) -> ProbeDeps:
    writer = make_test_writer(conn)
    limiters = make_test_limiters()

    async def no_sleep(_delay: float) -> None:
        await trio.lowlevel.checkpoint()

    return ProbeDeps(
        client=client,
        writer=writer,
        limiters=limiters,
        cursor=EventsTableProbeCursor(writer=writer, limiter=limiters.admin_read),
        clock=lambda: datetime(2026, 8, 2, tzinfo=UTC),
        sleep=no_sleep,
    )


@pytest.mark.trio
async def test_one_event_per_visible_channel_skips_dms_and_archived(
    server_conn: psycopg.Connection[TupleRow],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    channels = (
        _Channel("C1", "alpha"),
        _Channel("C2", "beta"),
        _Channel("D1", "direct-message", is_im=True),
        _Channel("C3", "archived", is_archived=True),
    )
    for channel in channels:
        _seed_channel(server_conn, channel)

    calls: list[str] = []

    def fake_search(_http: httpx.Client, name: str) -> SearchMessageTotal:
        calls.append(name)
        return SearchMessageTotal(total={"alpha": 11, "beta": 22}[name], approximate=False)

    monkeypatch.setattr(probe_module, "search_channel_message_total", fake_search)
    client = SlackClient("xoxp-test")
    try:
        records = await probe_channel_message_counts(_make_deps(server_conn, client))
    finally:
        client.close()

    assert calls == ["alpha", "beta"]
    assert [(record.stream, record.kind, record.payload) for record in records] == [
        (
            "channel-list",
            "channel_message_count_probed",
            {"channel_id": "C1", "message_total": 11, "approximate": False},
        ),
        (
            "channel-list",
            "channel_message_count_probed",
            {"channel_id": "C2", "message_total": 22, "approximate": False},
        ),
    ]
    assert all(record.ts == "2026-08-02T00:00:00.000000Z" for record in records)


@pytest.mark.trio
async def test_approximate_flag_propagates_to_event_payload(
    server_conn: psycopg.Connection[TupleRow],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_channel(server_conn, _Channel("C1", "large"))

    def fake_search(_http: httpx.Client, _name: str) -> SearchMessageTotal:
        return SearchMessageTotal(total=10_000, approximate=True)

    monkeypatch.setattr(probe_module, "search_channel_message_total", fake_search)
    client = SlackClient("xoxp-test")
    try:
        records = await probe_channel_message_counts(_make_deps(server_conn, client))
    finally:
        client.close()

    assert len(records) == 1
    assert records[0].payload == {"channel_id": "C1", "message_total": 10_000, "approximate": True}
