# pyright: reportPrivateUsage=false
"""HTTP/control-surface manual backfill trigger."""

from __future__ import annotations

import pytest
import trio

from slack_fuse_server.backfill.types import BackfillRunTrigger
from slack_fuse_server.config import ServerConfig
from slack_fuse_server.slurper import __main__ as slurper_main
from tests.conftest import RecordingSupervisor


def _config() -> ServerConfig:
    return ServerConfig(
        slack_user_token="xoxp-test",
        slack_app_token="xapp-test",
        shared_secret="sek",
    )


def test_manual_backfill_trigger_consume_declares_waiting_and_running(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trigger = slurper_main.ManualBackfillTrigger()
    supervisor = RecordingSupervisor()
    ran: list[tuple[str, BackfillRunTrigger | None]] = []

    async def _fake_run_backfill(  # noqa: PLR0913 - mirrors production call shape.
        _config: object,
        channel_id: str,
        *,
        allow_large: bool,
        max_messages: int | None,
        source: object,
        since_ts: float | None = None,
        triggered_by: BackfillRunTrigger | None = None,
    ) -> None:
        assert allow_large is False
        assert max_messages is None
        assert source == "slack-api"
        assert since_ts is None
        ran.append((channel_id, triggered_by))
        await trio.lowlevel.checkpoint()

    monkeypatch.setattr(slurper_main, "_run_backfill", _fake_run_backfill)

    async def go() -> None:
        async with trio.open_nursery() as nursery:
            nursery.start_soon(trigger.consume, _config(), supervisor)
            await trio.sleep(0.01)
            assert trigger.request_channel("C_BACKFILL") is True
            with trio.fail_after(1):
                while len(ran) < 1:
                    await trio.sleep(0.01)
            assert trigger.request_channel("C_SELF", triggered_by=BackfillRunTrigger.SELF_JOIN) is True
            with trio.fail_after(1):
                while len(ran) < 2:
                    await trio.sleep(0.01)
            nursery.cancel_scope.cancel()

    trio.run(go)

    assert ran == [
        ("C_BACKFILL", BackfillRunTrigger.CONTROL_SURFACE),
        ("C_SELF", BackfillRunTrigger.SELF_JOIN),
    ]
    phases = [(item.task_name, item.phase, item.details) for item in supervisor.declarations]
    assert ("backfill-trigger", "waiting_for_trigger", None) in phases
    assert ("backfill-trigger", "running", {"channel_id": "C_BACKFILL"}) in phases
