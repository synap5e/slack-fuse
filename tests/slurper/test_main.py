from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path
from typing import Protocol, cast

import pytest

from slack_fuse_server.config import ServerConfig
from slack_fuse_server.slurper import __main__ as slurper_main


class _SourcePlan(Protocol):
    socket_mode: bool
    webhook: bool
    nats_shim: bool


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
    assert plan.nats_shim is False


def test_nats_boot_plan_enables_nats_without_webhook() -> None:
    config = ServerConfig(
        slack_user_token="xoxp-user",
        slack_app_token="",
        shared_secret="shared",
        socket_mode_enabled=False,
        webhook_port=0,
        signing_secret="signing-secret",
        nats_shim_enabled=True,
        nats_url="tls://nats.example:4222",
        nats_ca_path=Path("/secret/ca.crt"),
        nats_cert_path=Path("/secret/tls.crt"),
        nats_key_path=Path("/secret/tls.key"),
    )

    plan = _event_source_plan(config)

    assert plan.socket_mode is False
    assert plan.webhook is False
    assert plan.nats_shim is True
