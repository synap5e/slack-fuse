"""Task supervisor registry semantics."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from slack_fuse_server.slurper.supervisor import TaskSupervisor, phase


def test_declare_and_all_phases_round_trip() -> None:
    now = datetime(2026, 6, 28, 3, 14, 15, tzinfo=UTC)
    supervisor = TaskSupervisor(clock=lambda: now)

    supervisor.declare("socket", "connecting", details={"attempt": 1}, deadline_s=15)

    phases = supervisor.all_phases()
    assert set(phases) == {"socket"}
    assert phases["socket"].task_name == "socket"
    assert phases["socket"].phase == "connecting"
    assert phases["socket"].details == {"attempt": 1}
    assert phases["socket"].entered_at == now
    assert phases["socket"].deadline == now + timedelta(seconds=15)


def test_overdue_identifies_past_deadlines() -> None:
    current = datetime(2026, 6, 28, 3, 14, 15, tzinfo=UTC)
    supervisor = TaskSupervisor(clock=lambda: current)
    supervisor.declare("auto-backfill", "channel", deadline_s=5)

    current += timedelta(seconds=6)

    assert [phase.task_name for phase in supervisor.overdue()] == ["auto-backfill"]


def test_overdue_skips_phases_without_deadline() -> None:
    current = datetime(2026, 6, 28, 3, 14, 15, tzinfo=UTC)
    supervisor = TaskSupervisor(clock=lambda: current)
    supervisor.declare("refresh", "sleeping_until", deadline_s=None)

    current += timedelta(days=1)

    assert supervisor.overdue() == []


def test_latest_declare_wins() -> None:
    current = datetime(2026, 6, 28, 3, 14, 15, tzinfo=UTC)
    supervisor = TaskSupervisor(clock=lambda: current)
    supervisor.declare("socket", "connecting", deadline_s=15)
    current += timedelta(seconds=1)
    supervisor.declare("socket", "connected_waiting_for_frame", deadline_s=None)

    phases = supervisor.all_phases()
    assert list(phases) == ["socket"]
    assert phases["socket"].phase == "connected_waiting_for_frame"
    assert phases["socket"].deadline is None


def test_clock_injection_controls_deadline_math() -> None:
    current = datetime(2026, 6, 28, 3, 14, 15, tzinfo=UTC)
    supervisor = TaskSupervisor(clock=lambda: current)

    supervisor.declare("catchup", "listing_channels", deadline_s=0)

    phase_record = supervisor.all_phases()["catchup"]
    assert phase_record.entered_at == current
    assert phase_record.deadline == current
    assert supervisor.overdue() == [phase_record]


@pytest.mark.trio
async def test_phase_context_manager_declares_without_done_marker() -> None:
    now = datetime(2026, 6, 28, 3, 14, 15, tzinfo=UTC)
    supervisor = TaskSupervisor(clock=lambda: now)

    async with phase(supervisor, "refresh", "refreshing_channel", details={"channel_id": "C1"}, deadline_s=10):
        assert supervisor.all_phases()["refresh"].phase == "refreshing_channel"

    phases = supervisor.all_phases()
    assert phases["refresh"].phase == "refreshing_channel"
    assert not phases["refresh"].phase.endswith("_done")


@pytest.mark.trio
async def test_phase_context_manager_declares_before_exception() -> None:
    now = datetime(2026, 6, 28, 3, 14, 15, tzinfo=UTC)
    supervisor = TaskSupervisor(clock=lambda: now)

    with pytest.raises(RuntimeError, match="boom"):
        async with phase(supervisor, "snapshot", "generating", deadline_s=300):
            raise RuntimeError("boom")

    assert supervisor.all_phases()["snapshot"].phase == "generating"
