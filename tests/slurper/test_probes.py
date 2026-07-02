# pyright: reportPrivateUsage=false
"""Probe sweep raw API capture semantics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

import httpx
import pytest
import trio

from slack_fuse_server._json import JsonObject, JsonValue
from slack_fuse_server.slurper import probes
from slack_fuse_server.slurper.api import SlackClient
from slack_fuse_server.slurper.offsets import EventRecord, write_event
from slack_fuse_server.slurper.probes import (
    CONVERSATIONS_HISTORY_SAMPLED,
    CONVERSATIONS_LIST_SAMPLED,
    JOB_CHANNEL_DAY_PRESENCE,
    JOB_CHANNEL_INVENTORY,
    JOB_CHANNEL_NEWEST_MESSAGE,
    JOB_CHANNEL_OLDER_THAN_OLDEST,
    JOB_WORKSPACE_USER_COUNT,
    PROBE_SWEEP_COMPLETED,
    USERS_LIST_SAMPLED,
    ProbeTarget,
    ProbeTrigger,
    _day_presence_due_sync,
    _history_older_due_sync,
    _presence_day_windows,
    _sample_channel_inventory,
    _sample_day_presence_history,
    _sample_newest_history,
    _sample_older_than_oldest_history,
    _sample_workspace_users,
    build_probe_registry,
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
    probe_channel_day_presence_cadence_s: float = 7 * 86400.0


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


async def _wait_for_health_event_count(conn: psycopg.Connection[TupleRow], kind: str, count: int) -> None:
    for _ in range(100):
        if len(_health_events_of(conn, kind)) >= count:
            return
        await trio.sleep(0.01)
    raise AssertionError(f"timed out waiting for {count} {kind} event(s)")


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


def test_presence_day_windows_are_complete_utc_days_excluding_today() -> None:
    now = datetime(2026, 7, 3, 12, 30, tzinfo=UTC)
    windows = _presence_day_windows(now)
    assert len(windows) == 30
    # Most recent complete day first: 2026-07-02T00:00:00Z .. 23:59:59.999999.
    assert windows[0].oldest == "1782950400.000000"
    assert windows[0].latest == "1783036799.999999"
    # Oldest window day: 2026-06-03.
    assert windows[-1].oldest == "1780444800.000000"
    assert all(w.oldest.endswith(".000000") and w.latest.endswith(".999999") for w in windows)
    assert all(int(w.latest.split(".")[0]) - int(w.oldest.split(".")[0]) == 86399 for w in windows)


def test_day_presence_samples_most_recent_unsampled_day_first(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    windows = _presence_day_windows(datetime.now(UTC))

    async def body() -> None:
        writer = make_test_writer(server_conn)
        client = _fake_client(fake_slack_http)
        limiters = make_test_limiters()
        await _sample_day_presence_history(writer, client, limiters, ProbeTarget("C0001", "channel_id"), None)
        await _sample_day_presence_history(writer, client, limiters, ProbeTarget("C0001", "channel_id"), None)

    trio.run(body)

    samples = _health_events_of(server_conn, CONVERSATIONS_HISTORY_SAMPLED)
    assert len(samples) == 2
    assert samples[0]["call_params"] == {
        "channel": "C0001",
        "oldest": windows[0].oldest,
        "latest": windows[0].latest,
        "limit": 1,
    }
    assert samples[1]["call_params"] == {
        "channel": "C0001",
        "oldest": windows[1].oldest,
        "latest": windows[1].latest,
        "limit": 1,
    }
    response = cast(JsonObject, samples[0]["response"])
    assert response["ok"] is True
    assert isinstance(response["messages"], list)
    assert isinstance(samples[0]["captured_at"], str)


def test_day_presence_due_and_older_than_oldest_cadence_are_independent(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(probes, "_DAY_PRESENCE_WINDOW_DAYS", 2)
    target = ProbeTarget("C0001", "channel_id")
    cadence_s = 7 * 86400.0
    assert _day_presence_due_sync(server_conn, target, cadence_s) is True

    async def body() -> None:
        writer = make_test_writer(server_conn)
        client = _fake_client(fake_slack_http)
        limiters = make_test_limiters()
        await _sample_day_presence_history(writer, client, limiters, target, None)
        await _sample_day_presence_history(writer, client, limiters, target, None)

    trio.run(body)

    assert len(_health_events_of(server_conn, CONVERSATIONS_HISTORY_SAMPLED)) == 2
    assert _day_presence_due_sync(server_conn, target, cadence_s) is False
    # Day-presence samples carry `latest` too; they must not reset the
    # older-than-oldest job's cadence for the same channel.
    assert _history_older_due_sync(server_conn, target, cadence_s) is True


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
    # First sweep: older(1) + newest(2) + day-presence(2). Second sweep: the
    # older/newest jobs are within cadence, but day-presence still has 29
    # unsampled window days per channel, so it samples one more day each (+2).
    assert kinds.count(CONVERSATIONS_HISTORY_SAMPLED) == 7
    assert kinds.count(CONVERSATIONS_LIST_SAMPLED) == 1
    assert kinds.count(USERS_LIST_SAMPLED) == 1
    assert kinds.count(PROBE_SWEEP_COMPLETED) == 2

    first_heartbeat = _health_events_of(server_conn, PROBE_SWEEP_COMPLETED)[0]
    second_heartbeat = _health_events_of(server_conn, PROBE_SWEEP_COMPLETED)[1]
    first_counts = cast(JsonObject, first_heartbeat["probes"])
    second_counts = cast(JsonObject, second_heartbeat["probes"])
    assert first_counts[JOB_CHANNEL_OLDER_THAN_OLDEST] == {"succeeded": 1, "failed": 0, "skipped": 0}
    assert first_counts[JOB_CHANNEL_NEWEST_MESSAGE] == {"succeeded": 2, "failed": 0, "skipped": 0}
    assert first_counts[JOB_CHANNEL_DAY_PRESENCE] == {"succeeded": 2, "failed": 0, "skipped": 0}
    assert first_counts[JOB_CHANNEL_INVENTORY] == {"succeeded": 1, "failed": 0, "skipped": 0}
    assert first_counts[JOB_WORKSPACE_USER_COUNT] == {"succeeded": 1, "failed": 0, "skipped": 0}
    assert second_counts[JOB_CHANNEL_OLDER_THAN_OLDEST] == {"succeeded": 0, "failed": 0, "skipped": 1}
    assert second_counts[JOB_CHANNEL_NEWEST_MESSAGE] == {"succeeded": 0, "failed": 0, "skipped": 2}
    assert second_counts[JOB_CHANNEL_DAY_PRESENCE] == {"succeeded": 2, "failed": 0, "skipped": 0}
    assert second_counts[JOB_CHANNEL_INVENTORY] == {"succeeded": 0, "failed": 0, "skipped": 1}
    assert second_counts[JOB_WORKSPACE_USER_COUNT] == {"succeeded": 0, "failed": 0, "skipped": 1}
    assert first_heartbeat["triggered_by"] == "scheduled"
    assert first_heartbeat["requested"] is None

    phases = [(item.task_name, item.phase) for item in supervisor.declarations]
    assert ("probe-sweep", JOB_CHANNEL_OLDER_THAN_OLDEST) in phases
    assert ("probe-sweep", JOB_CHANNEL_NEWEST_MESSAGE) in phases
    assert ("probe-sweep", JOB_CHANNEL_DAY_PRESENCE) in phases
    assert ("probe-sweep", JOB_CHANNEL_INVENTORY) in phases
    assert ("probe-sweep", JOB_WORKSPACE_USER_COUNT) in phases
    assert "op=slurper.probe.conversations_history" in caplog.text
    assert f"job_id={JOB_CHANNEL_NEWEST_MESSAGE}" in caplog.text
    assert f"event_kind={CONVERSATIONS_HISTORY_SAMPLED}" in caplog.text
    assert "trigger=scheduled" in caplog.text


def test_probe_trigger_rejects_when_buffer_full() -> None:
    trigger = ProbeTrigger(max_buffer_size=1)
    assert trigger.request(job_id=JOB_CHANNEL_NEWEST_MESSAGE) is True
    assert trigger.request(job_id=JOB_CHANNEL_NEWEST_MESSAGE) is False


def test_manual_probe_trigger_bypasses_due_and_emits_manual_heartbeat(
    server_conn: psycopg.Connection[TupleRow],
    fake_slack_http: httpx.Client,
) -> None:
    _seed_channel(server_conn, "C0001")
    registry = tuple(
        descriptor
        for descriptor in build_probe_registry(_ProbeConfig())
        if descriptor.job_id == JOB_CHANNEL_NEWEST_MESSAGE
    )
    assert len(registry) == 1

    async def body() -> None:
        writer = make_test_writer(server_conn)
        client = _fake_client(fake_slack_http)
        limiters = make_test_limiters()
        await probe_sweep(
            writer,
            client,
            limiters,
            None,
            _ProbeConfig(),
            registry=registry,
            run_once=True,
        )

        trigger = ProbeTrigger(max_buffer_size=1)
        async with trio.open_nursery() as nursery:
            nursery.start_soon(
                trigger.consume,
                writer,
                client,
                limiters,
                None,
                registry,
                _ProbeConfig().probe_sweep_interval_s,
            )
            await trio.sleep(0.01)
            assert trigger.request(job_id=JOB_CHANNEL_NEWEST_MESSAGE, target="C0001") is True
            await _wait_for_health_event_count(server_conn, CONVERSATIONS_HISTORY_SAMPLED, 2)
            await _wait_for_health_event_count(server_conn, PROBE_SWEEP_COMPLETED, 2)
            nursery.cancel_scope.cancel()

    trio.run(body)

    samples = _health_events_of(server_conn, CONVERSATIONS_HISTORY_SAMPLED)
    assert len(samples) == 2
    completions = _health_events_of(server_conn, PROBE_SWEEP_COMPLETED)
    assert completions[0]["triggered_by"] == "scheduled"
    assert completions[1]["triggered_by"] == "manual"
    assert completions[1]["requested"] == {"job_id": JOB_CHANNEL_NEWEST_MESSAGE, "target": "C0001"}
    counts = cast(JsonObject, completions[1]["probes"])
    assert counts[JOB_CHANNEL_NEWEST_MESSAGE] == {"succeeded": 1, "failed": 0, "skipped": 0}
