"""Structured slurper span logging."""

from __future__ import annotations

import logging

import pytest
import trio

from slack_fuse_server.slurper.api import RateLimitedError
from slack_fuse_server.slurper.spans import span


def _span_messages(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [record.getMessage() for record in caplog.records if "slurper-span" in record.getMessage()]


def test_span_happy_path_logs_ok(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="slack_fuse_server.slurper.spans")

    async def body() -> None:
        async with span(op="slurper.test.ok", task="unit", extra={"channel_id": "C1"}) as recorder:
            recorder.set("events_written", 2)
            await trio.lowlevel.checkpoint()

    trio.run(body)

    messages = _span_messages(caplog)
    assert len(messages) == 1
    assert "op=slurper.test.ok" in messages[0]
    assert "task=unit" in messages[0]
    assert "result=ok" in messages[0]
    assert "duration_ms=" in messages[0]
    assert "limiter_wait_ms=0" in messages[0]
    assert "sync_ms=0" in messages[0]
    assert "channel_id=C1" in messages[0]
    assert "events_written=2" in messages[0]


def test_span_exception_logs_error_and_reraises(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="slack_fuse_server.slurper.spans")

    async def body() -> None:
        async with span(op="slurper.test.error", task="unit"):
            raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        trio.run(body)

    messages = _span_messages(caplog)
    assert len(messages) == 1
    assert "result=error" in messages[0]
    assert "error_type=ValueError" in messages[0]


def test_span_markers_set_terminal_results(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="slack_fuse_server.slurper.spans")

    async def body() -> None:
        async with span(op="slurper.test.skipped", task="unit") as recorder:
            recorder.mark_skipped()
        async with span(op="slurper.test.rate_limited", task="unit") as recorder:
            recorder.mark_rate_limited(12.5)
        async with span(op="slurper.test.timeout", task="unit") as recorder:
            recorder.mark_timeout("WriterPoolExhausted")

    trio.run(body)

    messages = _span_messages(caplog)
    assert any("op=slurper.test.skipped" in msg and "result=skipped" in msg for msg in messages)
    assert any(
        "op=slurper.test.rate_limited" in msg
        and "result=rate_limited" in msg
        and "retry_after_s=12.5" in msg
        for msg in messages
    )
    assert any(
        "op=slurper.test.timeout" in msg
        and "result=timeout" in msg
        and "timeout_type=WriterPoolExhausted" in msg
        for msg in messages
    )


def test_span_rate_limited_exception_is_categorized(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="slack_fuse_server.slurper.spans")

    async def body() -> None:
        async with span(op="slurper.test.rate_limited_exception", task="unit"):
            raise RateLimitedError(3.0)

    with pytest.raises(RateLimitedError):
        trio.run(body)

    messages = _span_messages(caplog)
    assert len(messages) == 1
    assert "result=rate_limited" in messages[0]
    assert "retry_after_s=3.0" in messages[0]


def test_span_slow_path_logs_warning(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="slack_fuse_server.slurper.spans")

    async def body() -> None:
        async with span(op="slurper.test.slow", task="unit", slow_threshold_ms=0):
            await trio.sleep(0.01)

    trio.run(body)

    records = [record for record in caplog.records if "op=slurper.test.slow" in record.getMessage()]
    assert len(records) == 1
    assert records[0].levelno == logging.WARNING
