# pyright: reportPrivateUsage=false
"""Immutable probe registry scheduling and event-backed cadence tests."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import httpx
import pytest
import trio

import slack_fuse_server.probes.channel_message_count as channel_message_count_module
from slack_fuse_server.probes import register_default_probes
from slack_fuse_server.probes.registry import (
    ProbeDeps,
    ProbeKind,
    ProbeScope,
    SlackTier,
    probe_timestamp,
)
from slack_fuse_server.probes.sweep import EventsTableProbeCursor, run_probe_sweep_once
from slack_fuse_server.search_messages import SearchMessageTotal
from slack_fuse_server.slurper.api import SlackClient
from slack_fuse_server.slurper.offsets import EventRecord, write_event
from tests.conftest import RecordingSupervisor, make_test_limiters, make_test_writer

if TYPE_CHECKING:
    import psycopg
    from psycopg.rows import TupleRow


@dataclass(slots=True)
class _Clock:
    value: datetime

    def __call__(self) -> datetime:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += timedelta(seconds=seconds)


async def _checkpoint_sleep(_delay: float) -> None:
    await trio.lowlevel.checkpoint()


def _deps(
    conn: psycopg.Connection[TupleRow],
    client: SlackClient,
    clock: _Clock,
) -> ProbeDeps:
    writer = make_test_writer(conn)
    limiters = make_test_limiters()
    return ProbeDeps(
        client=client,
        writer=writer,
        limiters=limiters,
        cursor=EventsTableProbeCursor(writer=writer, limiter=limiters.admin_read),
        clock=clock,
        sleep=_checkpoint_sleep,
    )


def _stub_probe(
    kind: str,
    calls: list[str],
    *,
    interval_s: float = 60.0,
    count: int = 1,
    error: Exception | None = None,
) -> ProbeKind:
    async def run(deps: ProbeDeps) -> tuple[EventRecord, ...]:
        await trio.lowlevel.checkpoint()
        calls.append(kind)
        if error is not None:
            raise error
        ts = probe_timestamp(deps.clock())
        return tuple(
            EventRecord(
                stream="slurper-health",
                kind=kind,
                ts=ts,
                payload={"sequence": sequence},
            )
            for sequence in range(count)
        )

    return ProbeKind(
        kind=kind,
        interval_s=interval_s,
        tier=SlackTier.TIER_2,
        scope=ProbeScope.PER_CHANNEL,
        run=run,
    )


def _events(conn: psycopg.Connection[TupleRow], kind: str) -> list[tuple[str, int]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT ts, (payload->>'sequence')::int FROM events WHERE kind = %s ORDER BY offset_in_stream",
            (kind,),
        )
        return [(str(row[0]), int(row[1])) for row in cur.fetchall()]


@pytest.mark.trio
async def test_registry_dispatches_writes_and_updates_event_backed_last_run(
    server_conn: psycopg.Connection[TupleRow],
    caplog: pytest.LogCaptureFixture,
) -> None:
    clock = _Clock(datetime(2026, 8, 2, 1, 2, 3, tzinfo=UTC))
    calls: list[str] = []
    probe = _stub_probe("stub_channel_probed", calls, count=2)
    client = SlackClient("xoxp-test")
    deps = _deps(server_conn, client, clock)
    caplog.set_level(logging.INFO, logger="slack_fuse_server.slurper.spans")
    try:
        await run_probe_sweep_once(RecordingSupervisor(), deps, (probe,))
    finally:
        client.close()

    expected_ts = probe_timestamp(clock())
    assert calls == [probe.kind]
    assert _events(server_conn, probe.kind) == [(expected_ts, 0), (expected_ts, 1)]
    assert await deps.cursor.last_run_at(probe.kind) == clock()
    assert f"op=slurper.probe.{probe.kind}" in caplog.text
    assert "duration_ms=" in caplog.text
    assert "events_written=2" in caplog.text
    assert "outcome=ok" in caplog.text


@pytest.mark.trio
async def test_interval_is_respected_across_three_minute_ticks(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    clock = _Clock(datetime(2026, 8, 2, tzinfo=UTC))
    calls: list[str] = []
    probe = _stub_probe("hourly_stub_probed", calls, interval_s=3600.0)
    client = SlackClient("xoxp-test")
    deps = _deps(server_conn, client, clock)
    try:
        for _ in range(3):
            await run_probe_sweep_once(RecordingSupervisor(), deps, (probe,))
            clock.advance(60.0)
    finally:
        client.close()

    assert calls == [probe.kind]
    assert len(_events(server_conn, probe.kind)) == 1


@pytest.mark.trio
async def test_failure_isolated_and_failed_kind_retries_without_last_run(
    server_conn: psycopg.Connection[TupleRow],
    caplog: pytest.LogCaptureFixture,
) -> None:
    clock = _Clock(datetime(2026, 8, 2, tzinfo=UTC))
    calls: list[str] = []
    failed = _stub_probe("failed_stub_probed", calls, error=RuntimeError("expected failure"))
    succeeded = _stub_probe("healthy_stub_probed", calls, interval_s=3600.0)
    client = SlackClient("xoxp-test")
    deps = _deps(server_conn, client, clock)
    caplog.set_level(logging.ERROR, logger="slack_fuse_server.probes.sweep")
    caplog.set_level(logging.INFO, logger="slack_fuse_server.slurper.spans")
    try:
        await run_probe_sweep_once(RecordingSupervisor(), deps, (failed, succeeded))
        clock.advance(60.0)
        await run_probe_sweep_once(RecordingSupervisor(), deps, (failed, succeeded))
    finally:
        client.close()

    assert calls == [failed.kind, succeeded.kind, failed.kind]
    assert _events(server_conn, failed.kind) == []
    assert len(_events(server_conn, succeeded.kind)) == 1
    assert await deps.cursor.last_run_at(failed.kind) is None
    assert await deps.cursor.last_run_at(succeeded.kind) == datetime(2026, 8, 2, tzinfo=UTC)
    assert f"op=slurper.probe.{failed.kind}" in caplog.text
    assert "outcome=error" in caplog.text


def _seed_channel(conn: psycopg.Connection[TupleRow], channel_id: str, name: str) -> None:
    write_event(
        conn,
        EventRecord(
            stream="channel-list",
            kind="channel_added",
            ts=None,
            payload={
                "id": channel_id,
                "name": name,
                "is_member": True,
                "is_im": False,
                "is_archived": False,
            },
        ),
    )


@pytest.mark.trio
async def test_second_probe_is_registry_entry_only_no_framework_change(
    server_conn: psycopg.Connection[TupleRow],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A second registry entry fires beside the default with no sweep changes."""
    _seed_channel(server_conn, "C1", "general")
    calls: list[str] = []
    second = _stub_probe("second_kind_probed", calls)

    def fake_search(_http: httpx.Client, name: str) -> SearchMessageTotal:
        assert name == "general"
        return SearchMessageTotal(total=42, approximate=False)

    monkeypatch.setattr(channel_message_count_module, "search_channel_message_total", fake_search)
    client = SlackClient("xoxp-test")
    deps = _deps(server_conn, client, _Clock(datetime(2026, 8, 2, tzinfo=UTC)))
    registry = (*register_default_probes(), second)
    try:
        await run_probe_sweep_once(RecordingSupervisor(), deps, registry)
    finally:
        client.close()

    with server_conn.cursor() as cur:
        cur.execute(
            "SELECT kind FROM events WHERE kind IN ('channel_message_count_probed', 'second_kind_probed') ORDER BY kind"
        )
        kinds = [str(row[0]) for row in cur.fetchall()]
    assert kinds == ["channel_message_count_probed", "second_kind_probed"]
    assert calls == [second.kind]
