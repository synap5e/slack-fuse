from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Protocol, cast

import pytest

from slack_fuse_server.config import ServerConfig
from slack_fuse_server.slurper import __main__ as slurper_main


class _SourcePlan(Protocol):
    socket_mode: bool
    webhook: bool


_log_slurper_started = cast("Callable[[], None]", vars(slurper_main)["_log_slurper_started"])
_event_source_plan = cast(
    "Callable[[ServerConfig], _SourcePlan]",
    vars(slurper_main)["_event_source_plan"],
)


def test_log_slurper_started_emits_canonical_info_line(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="slack_fuse_server.slurper.__main__")

    _log_slurper_started()

    assert any(record.levelno == logging.INFO and "slurper-started" in record.getMessage() for record in caplog.records)


def test_http_only_boot_plan_does_not_start_socket_mode() -> None:
    config = ServerConfig(
        slack_user_token="xoxp-user",
        slack_app_token="",
        shared_secret="shared",
        socket_mode_enabled=False,
        webhook_port=18766,
        signing_secret="signing-secret",
    )

    plan = _event_source_plan(config)

    assert plan.socket_mode is False
    assert plan.webhook is True
