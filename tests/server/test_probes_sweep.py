# pyright: reportPrivateUsage=false
"""Immutable probe registry scheduling and event-backed cadence tests."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest
import trio

from slack_fuse_server.probes import register_fact_probes
from slack_fuse_server.probes.registry import (
    EventFactsSink,
    ProbeDeps,
    ProbeKind,
    ProbeScope,
    ProbeTarget,
    SlackTier,
    probe_timestamp,
)
from slack_fuse_server.slurper.api import SlackClient
from slack_fuse_server.slurper.offsets import EventRecord, write_event
from slack_fuse_server.slurper.probes import run_probe_registry_once
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
        clock=clock,
        sleep=_checkpoint_sleep,
    )


def _stub_probe(  # noqa: PLR0913 - test factory exposes independent failure modes.
    kind: str,
    calls: list[str],
    *,
    interval_s: float = 60.0,
    count: int = 1,
    error: Exception | None = None,
    due_error: Exception | None = None,
) -> ProbeKind:
    async def targets(_deps: ProbeDeps) -> tuple[ProbeTarget, ...]:
        await trio.lowlevel.checkpoint()
        return (ProbeTarget("workspace"),)

    def due(
        conn: psycopg.Connection[TupleRow],
        _target: ProbeTarget,
        cadence_s: float,
        now: datetime,
    ) -> bool:
        if due_error is not None:
            raise due_error
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(ts) FROM events WHERE kind = %s", (kind,))
            row = cur.fetchone()
        if row is None or row[0] is None:
            return True
        last_run_at = datetime.fromisoformat(str(row[0]).replace("Z", "+00:00"))
        return (now - last_run_at).total_seconds() >= cadence_s

    async def run(deps: ProbeDeps, _target: ProbeTarget) -> tuple[EventRecord, ...]:
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
        job_id=kind,
        kind=kind,
        interval_s=interval_s,
        tier=SlackTier.TIER_2,
        scope=ProbeScope.PER_CHANNEL,
        run=run,
        targets=targets,
        due=due,
        sink=EventFactsSink(),
        op=f"slurper.probe.{kind}",
    )


def _events(conn: psycopg.Connection[TupleRow], kind: str) -> list[tuple[str, int]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT ts, (payload->>'sequence')::int FROM events WHERE kind = %s ORDER BY offset_in_stream",
            (kind,),
        )
        return [(str(row[0]), int(row[1])) for row in cur.fetchall()]


def _last_run_at(conn: psycopg.Connection[TupleRow], kind: str) -> datetime | None:
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(ts) FROM events WHERE kind = %s", (kind,))
        row = cur.fetchone()
    if row is None or row[0] is None:
        return None
    return datetime.fromisoformat(str(row[0]).replace("Z", "+00:00"))


def test_fact_last_run_query_uses_partial_index(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    with server_conn.cursor() as cur:
        cur.execute("SET enable_seqscan = off")
        try:
            cur.execute(
                """
                EXPLAIN (COSTS OFF)
                SELECT MAX(ts)
                FROM events
                WHERE kind = 'channel_message_count_probed'
                  AND kind IN ('channel_message_count_probed')
                """
            )
            plan = "\n".join(str(row[0]) for row in cur.fetchall())
        finally:
            cur.execute("RESET enable_seqscan")

    assert "events_probe_fact_latest_idx" in plan


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
        await run_probe_registry_once(RecordingSupervisor(), deps, (probe,))
    finally:
        client.close()

    expected_ts = probe_timestamp(clock())
    assert calls == [probe.kind]
    assert _events(server_conn, probe.kind) == [(expected_ts, 0), (expected_ts, 1)]
    assert _last_run_at(server_conn, probe.kind) == clock()
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
            await run_probe_registry_once(RecordingSupervisor(), deps, (probe,))
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
    caplog.set_level(logging.ERROR, logger="slack_fuse_server.slurper.probes")
    caplog.set_level(logging.INFO, logger="slack_fuse_server.slurper.spans")
    try:
        await run_probe_registry_once(RecordingSupervisor(), deps, (failed, succeeded))
        clock.advance(60.0)
        await run_probe_registry_once(RecordingSupervisor(), deps, (failed, succeeded))
    finally:
        client.close()

    assert calls == [failed.kind, succeeded.kind, failed.kind]
    assert _events(server_conn, failed.kind) == []
    assert len(_events(server_conn, succeeded.kind)) == 1
    assert _last_run_at(server_conn, failed.kind) is None
    assert _last_run_at(server_conn, succeeded.kind) == datetime(2026, 8, 2, tzinfo=UTC)
    assert f"op=slurper.probe.{failed.kind}" in caplog.text
    assert "outcome=error" in caplog.text


@pytest.mark.trio
async def test_cadence_read_failure_isolated_from_later_probe(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    clock = _Clock(datetime(2026, 8, 2, tzinfo=UTC))
    calls: list[str] = []
    failed = _stub_probe("cadence_failed_stub_probed", calls, due_error=RuntimeError("expected due failure"))
    succeeded = _stub_probe("cadence_healthy_stub_probed", calls)
    client = SlackClient("xoxp-test")
    deps = _deps(server_conn, client, clock)
    try:
        counters = await run_probe_registry_once(RecordingSupervisor(), deps, (failed, succeeded))
    finally:
        client.close()

    assert calls == [succeeded.kind]
    assert counters[failed.job_id] == {"succeeded": 0, "failed": 1, "skipped": 0}
    assert counters[succeeded.job_id] == {"succeeded": 1, "failed": 0, "skipped": 0}
    assert _last_run_at(server_conn, failed.kind) is None
    assert _last_run_at(server_conn, succeeded.kind) == clock()


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
async def test_new_probe_is_registry_entry_only_no_framework_change(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    """A stub probe fires cleanly through the sweep with only a registry entry.

    Before the ``channel_message_count_probed`` deletion (2026-08-03) this
    test proved a second probe fires beside the default. `register_fact_probes`
    now returns () so the framework has 0 built-in fact probes; the
    extensibility invariant is the same — the framework accepts any
    ``ProbeKind`` in its registry tuple.
    """
    _seed_channel(server_conn, "C1", "general")
    calls: list[str] = []
    stub = _stub_probe("stub_probe_probed", calls)

    client = SlackClient("xoxp-test")
    deps = _deps(server_conn, client, _Clock(datetime(2026, 8, 2, tzinfo=UTC)))
    registry = (*register_fact_probes(), stub)
    try:
        await run_probe_registry_once(RecordingSupervisor(), deps, registry)
    finally:
        client.close()

    with server_conn.cursor() as cur:
        cur.execute("SELECT kind FROM events WHERE kind = 'stub_probe_probed'")
        kinds = [str(row[0]) for row in cur.fetchall()]
    assert kinds == ["stub_probe_probed"]
    assert calls == [stub.kind]
