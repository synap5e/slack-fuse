from __future__ import annotations

import logging
from collections.abc import Callable
from typing import cast

import pytest

from slack_fuse_server.slurper import __main__ as slurper_main

_log_slurper_started = cast("Callable[[], None]", vars(slurper_main)["_log_slurper_started"])


def test_log_slurper_started_emits_canonical_info_line(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="slack_fuse_server.slurper.__main__")

    _log_slurper_started()

    assert any(record.levelno == logging.INFO and "slurper-started" in record.getMessage() for record in caplog.records)
