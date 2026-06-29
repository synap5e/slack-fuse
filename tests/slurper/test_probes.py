# pyright: reportPrivateUsage=false
"""Probe sweep raw API capture semantics."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import httpx
import pytest
import trio

from slack_fuse_server._json import JsonObject, JsonValue
from slack_fuse_server.slurper.api import SlackClient
from slack_fuse_server.slurper.offsets import EventRecord, write_event
from slack_fuse_server.slurper.probes import (
    CONVERSATIONS_HISTORY_SAMPLED,
    CONVERSATIONS_LIST_SAMPLED,
    JOB_CHANNEL_INVENTORY,
    JOB_CHANNEL_NEWEST_MESSAGE,
    JOB_CHANNEL_OLDER_THAN_OLDEST,
    JOB_WORKSPACE_USER_COUNT,
    PROBE_SWEEP_COMPLETED,
    USERS_LIST_SAMPLED,
    ProbeTarget,
    _sample_channel_inventory,
    _sample_newest_history,
    _sample_older_than_oldest_history,
    _sample_workspace_users,
    probe_sweep,
)
from tests.conftest import RecordingSupervisor, make_test_limiters, make_test_writer

if TYPE_CHECKING:
    import psycopg
    from psycopg.rows import TupleRow


@dataclass(frozen=True, slots=True)
class _ProbeConfig:
    probe_sweep_interval_s: float = 3600.0
    probe_channel_older_than_oldest_cadence_s: float = 7 * 86400.0
    probe_channel_newest_message_cadence_s: float = 86400.0
    probe_channel_inventory_cadence_s: float = 86400.0
    probe_workspace_user_count_cadence_s: float = 86400.0


def _fake_client(http: httpx.Client) -> SlackClient:
    client = SlackClient("xoxp-test")
    client._http = http
    return client


def _seed_channel(conn: psycopg.Connection[TupleRow], channel_id: str, **overrides: JsonValue) -> None:
    payload: JsonObject = {
        "id": channel_id,
        "name": channel_id.lower(),
        "is_member": True,
        "is_im": False,
        "is_mpim": False,
        "is_archived": False,
    }
    payload.update(overrides)
    write_event(conn, EventRecord(stream="channel-list", kind="channel_added", ts=None, payload=payload))


def _seed_message(conn: psycopg.Connection[TupleRow], channel_id: str, ts: str) -> None:
    payload: JsonObject = {"type": "message", "ts": ts, "user": "U0001", "text": f"message {ts}"}
    write_event(conn, EventRecord(stream=f"channel:{channel_id}", kind="message", ts=ts, payload=payload))


def _health_events(conn: psycopg.Connection[TupleRow]) -> list[tuple[str, JsonObject]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT kind, payload
            FROM events
            WHERE stream = 'slurper-health'
            ORDER BY offset_in_stream
            """
        )
        return [(str(row[0]), cast(JsonObject, row[1])) for row in cur.fetchall()]


def _health_events_of(conn: psycopg.Connection[TupleRow], kind: str) -> list[JsonObject]:
    return [payload for event_kind, payload in _health_events(conn) if event_kind == kind]


def test_older_than_oldest_writes_raw_history_sample(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    _seed_channel(server_conn, "C0001")
    _seed_message(server_conn, "C0001", "1700000100.000200")
    _seed_message(server_conn, "C0001", "not-a-slack-ts")
    write_event(
        server_conn,
        EventRecord(
            stream="channel:C0001",
            kind="message_deleted",
            ts="1700000000.000100",
            payload={"deleted_ts": "1700000000.000100", "previous_message": None},
        ),
    )

    async def body() -> None:
        await _sample_older_than_oldest_history(
            make_test_writer(server_conn),
            _fake_client(fake_slack_http),
            make_test_limiters(),
            ProbeTarget("C0001", "channel_id"),
            None,
        )

    trio.run(body)

    samples = _health_events_of(server_conn, CONVERSATIONS_HISTORY_SAMPLED)
    assert len(samples) == 1
    payload = samples[0]
    assert payload["call_params"] == {"channel": "C0001", "latest": "1700000100.000200", "limit": 1}
    response = cast(JsonObject, payload["response"])
    assert response["ok"] is True
    assert isinstance(response["messages"], list)
    assert isinstance(payload["captured_at"], str)


def test_newest_writes_raw_history_sample_without_latest(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    async def body() -> None:
        await _sample_newest_history(
            make_test_writer(server_conn),
            _fake_client(fake_slack_http),
            make_test_limiters(),
            ProbeTarget("C0001", "channel_id"),
            None,
        )

    trio.run(body)

    payload = _health_events_of(server_conn, CONVERSATIONS_HISTORY_SAMPLED)[0]
    assert payload["call_params"] == {"channel": "C0001", "limit": 1}
    assert "latest" not in cast(dict[str, object], payload["call_params"])
    assert "oldest" not in cast(dict[str, object], payload["call_params"])


def test_inventory_writes_raw_conversations_list_sample(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    async def body() -> None:
        await _sample_channel_inventory(
            make_test_writer(server_conn),
            _fake_client(fake_slack_http),
            make_test_limiters(),
            ProbeTarget("workspace"),
            None,
        )

    trio.run(body)

    payload = _health_events_of(server_conn, CONVERSATIONS_LIST_SAMPLED)[0]
    assert payload["call_params"] == {
        "types": "public_channel,private_channel,im,mpim",
        "exclude_archived": True,
    }
    response = cast(JsonObject, payload["response"])
    channels = response["channels"]
    assert isinstance(channels, list)
    assert {cast(dict[str, object], channel)["id"] for channel in channels} == {"C0001", "D0001"}
    assert response["page_count"] == 1


def test_user_count_writes_raw_users_list_sample(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    async def body() -> None:
        await _sample_workspace_users(
            make_test_writer(server_conn),
            _fake_client(fake_slack_http),
            make_test_limiters(),
            ProbeTarget("workspace"),
            None,
        )

    trio.run(body)

    payload = _health_events_of(server_conn, USERS_LIST_SAMPLED)[0]
    assert payload["call_params"] == {"limit": 200}
    response = cast(JsonObject, payload["response"])
    members = response["members"]
    assert isinstance(members, list)
    assert {cast(dict[str, object], member)["id"] for member in members} == {"U0001", "U0002"}
    assert response["page_count"] == 1


def test_probe_sweep_once_writes_all_samples_and_then_skips_when_not_due(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _seed_channel(server_conn, "C0001")
    _seed_channel(server_conn, "D0001", is_im=True, user="U0002")
    _seed_message(server_conn, "C0001", "1700000000.000100")
    supervisor = RecordingSupervisor()
    caplog.set_level("INFO", logger="slack_fuse_server.slurper.spans")

    async def body() -> None:
        writer = make_test_writer(server_conn)
        client = _fake_client(fake_slack_http)
        limiters = make_test_limiters()
        await probe_sweep(writer, client, limiters, supervisor, _ProbeConfig(), run_once=True)
        await probe_sweep(writer, client, limiters, supervisor, _ProbeConfig(), run_once=True)

    trio.run(body)

    events = _health_events(server_conn)
    kinds = [kind for kind, _payload in events]
    assert kinds.count(CONVERSATIONS_HISTORY_SAMPLED) == 3
    assert kinds.count(CONVERSATIONS_LIST_SAMPLED) == 1
    assert kinds.count(USERS_LIST_SAMPLED) == 1
    assert kinds.count(PROBE_SWEEP_COMPLETED) == 2

    first_heartbeat = _health_events_of(server_conn, PROBE_SWEEP_COMPLETED)[0]
    second_heartbeat = _health_events_of(server_conn, PROBE_SWEEP_COMPLETED)[1]
    first_counts = cast(JsonObject, first_heartbeat["probes"])
    second_counts = cast(JsonObject, second_heartbeat["probes"])
    assert first_counts[JOB_CHANNEL_OLDER_THAN_OLDEST] == {"succeeded": 1, "failed": 0, "skipped": 0}
    assert first_counts[JOB_CHANNEL_NEWEST_MESSAGE] == {"succeeded": 2, "failed": 0, "skipped": 0}
    assert first_counts[JOB_CHANNEL_INVENTORY] == {"succeeded": 1, "failed": 0, "skipped": 0}
    assert first_counts[JOB_WORKSPACE_USER_COUNT] == {"succeeded": 1, "failed": 0, "skipped": 0}
    assert second_counts[JOB_CHANNEL_OLDER_THAN_OLDEST] == {"succeeded": 0, "failed": 0, "skipped": 1}
    assert second_counts[JOB_CHANNEL_NEWEST_MESSAGE] == {"succeeded": 0, "failed": 0, "skipped": 2}
    assert second_counts[JOB_CHANNEL_INVENTORY] == {"succeeded": 0, "failed": 0, "skipped": 1}
    assert second_counts[JOB_WORKSPACE_USER_COUNT] == {"succeeded": 0, "failed": 0, "skipped": 1}

    phases = [(item.task_name, item.phase) for item in supervisor.declarations]
    assert ("probe-sweep", JOB_CHANNEL_OLDER_THAN_OLDEST) in phases
    assert ("probe-sweep", JOB_CHANNEL_NEWEST_MESSAGE) in phases
    assert ("probe-sweep", JOB_CHANNEL_INVENTORY) in phases
    assert ("probe-sweep", JOB_WORKSPACE_USER_COUNT) in phases
    assert "op=slurper.probe.conversations_history" in caplog.text
    assert f"job_id={JOB_CHANNEL_NEWEST_MESSAGE}" in caplog.text
    assert f"event_kind={CONVERSATIONS_HISTORY_SAMPLED}" in caplog.text
