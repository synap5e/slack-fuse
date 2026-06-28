"""Structured operation spans for slurper runtime evidence.

Each span emits one terminal log line with low-cardinality labels (`op`,
`task`, `result`) plus timing fields. The implementation deliberately stays on
plain stdlib logging so Alloy can tokenize stdout without a second logging
framework in the hot path.
"""

from __future__ import annotations

import logging
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Protocol, cast

import trio

from slack_fuse_server._json import JsonObject
from slack_fuse_server.slurper.api import RateLimitedError
from slack_fuse_server.slurper.offsets import PG_TIMEOUT_EXCEPTIONS

log = logging.getLogger(__name__)

_RESULT_OK = "ok"
_RESULT_ERROR = "error"
_RESULT_TIMEOUT = "timeout"
_RESULT_RATE_LIMITED = "rate_limited"
_RESULT_SKIPPED = "skipped"
_VALID_RESULTS = frozenset(
    {
        _RESULT_OK,
        _RESULT_ERROR,
        _RESULT_TIMEOUT,
        _RESULT_RATE_LIMITED,
        _RESULT_SKIPPED,
    }
)


@dataclass(frozen=True, slots=True)
class SpanResult:
    """One terminal event from a wrapped operation."""

    op: str
    task: str
    result: str
    duration_ms: int
    limiter_wait_ms: int
    sync_ms: int
    extra: JsonObject


@dataclass(frozen=True, slots=True)
class _SpanThresholds:
    default_ms: int = 5000
    backfill_channel_ms: int = 300000
    snapshot_ms: int = 60000
    socket_event_ms: int = 1000


class _SpanThresholdConfig(Protocol):
    span_slow_threshold_default_ms: int
    span_slow_threshold_backfill_channel_ms: int
    span_slow_threshold_snapshot_ms: int
    span_slow_threshold_socket_event_ms: int


_thresholds = _SpanThresholds()

_OP_THRESHOLD_KEYS = {
    "slurper.auto_backfill.channel": "backfill_channel_ms",
    "slurper.snapshot.generate": "snapshot_ms",
    "slurper.socket.handle_event": "socket_event_ms",
}


def configure_span_thresholds(
    *,
    default_ms: int,
    backfill_channel_ms: int,
    snapshot_ms: int,
    socket_event_ms: int,
) -> None:
    """Install process-wide slow-span thresholds from server config."""
    global _thresholds
    _thresholds = _SpanThresholds(
        default_ms=default_ms,
        backfill_channel_ms=backfill_channel_ms,
        snapshot_ms=snapshot_ms,
        socket_event_ms=socket_event_ms,
    )


def configure_span_thresholds_from_config(config: _SpanThresholdConfig) -> None:
    """Read span threshold fields from `ServerConfig` without importing it here."""
    configure_span_thresholds(
        default_ms=int(config.span_slow_threshold_default_ms),
        backfill_channel_ms=int(config.span_slow_threshold_backfill_channel_ms),
        snapshot_ms=int(config.span_slow_threshold_snapshot_ms),
        socket_event_ms=int(config.span_slow_threshold_socket_event_ms),
    )


class SpanRecorder:
    """Mutable recorder yielded by `span()` for operation-specific fields."""

    def __init__(self, extra: JsonObject | None = None) -> None:
        self.extra: dict[str, object] = dict(extra or {})
        self.result_label = _RESULT_OK
        self.limiter_wait_ms = 0
        self.sync_ms = 0

    @property
    def result(self) -> str:
        return self.result_label

    def set(self, key: str, value: object) -> None:
        self.extra[key] = value

    def mark_skipped(self) -> None:
        self.result_label = _RESULT_SKIPPED

    def mark_rate_limited(self, retry_after_s: float | None) -> None:
        self.result_label = _RESULT_RATE_LIMITED
        self.extra["retry_after_s"] = retry_after_s

    def mark_timeout(self, timeout_type: str | None = None) -> None:
        self.result_label = _RESULT_TIMEOUT
        if timeout_type is not None:
            self.extra["timeout_type"] = timeout_type

    def add_timing(self, *, limiter_wait_ms: int, sync_ms: int) -> None:
        self.limiter_wait_ms += max(limiter_wait_ms, 0)
        self.sync_ms += max(sync_ms, 0)

    def record_exception(self, exc: BaseException) -> None:
        if isinstance(exc, RateLimitedError):
            self.mark_rate_limited(exc.retry_after)
            return
        if isinstance(exc, PG_TIMEOUT_EXCEPTIONS):
            self.mark_timeout(type(exc).__name__)
            return
        self.result_label = _RESULT_ERROR
        self.extra["error_type"] = type(exc).__name__

    def build_result(self, *, op: str, task: str, duration_ms: int) -> SpanResult:
        result = self.result_label if self.result_label in _VALID_RESULTS else _RESULT_ERROR
        return SpanResult(
            op=op,
            task=task,
            result=result,
            duration_ms=duration_ms,
            limiter_wait_ms=self.limiter_wait_ms,
            sync_ms=self.sync_ms,
            extra=cast(JsonObject, dict(self.extra)),
        )


_SpanRecorder = SpanRecorder


@asynccontextmanager
async def span(  # noqa: RUF029 - async context manager API; timing/logging happens around the yield.
    *,
    op: str,
    task: str,
    extra: JsonObject | None = None,
    slow_threshold_ms: int | None = None,
) -> AsyncIterator[SpanRecorder]:
    """Time an operation body and emit one structured line on exit."""
    recorder = SpanRecorder(extra)
    started_ns = time.monotonic_ns()
    try:
        yield recorder
    except BaseException as exc:
        recorder.record_exception(exc)
        raise
    finally:
        duration_ms = _duration_ms(started_ns, time.monotonic_ns())
        result = recorder.build_result(op=op, task=task, duration_ms=duration_ms)
        _emit_result(result, slow_threshold_ms=slow_threshold_ms)


async def run_sync_with_span[T](
    func: Callable[[], T],
    *,
    limiter: trio.CapacityLimiter,
    span: SpanRecorder | None,
) -> T:
    """Run a sync callable under a Trio limiter and add wait/body timings."""
    started_ns = time.monotonic_ns()
    sync_ns = 0

    def _timed() -> T:
        nonlocal sync_ns
        sync_started_ns = time.monotonic_ns()
        try:
            return func()
        finally:
            sync_ns += time.monotonic_ns() - sync_started_ns

    try:
        return await trio.to_thread.run_sync(_timed, limiter=limiter)
    finally:
        if span is not None:
            _record_timing(span, started_ns=started_ns, finished_ns=time.monotonic_ns(), sync_ns=sync_ns)


def _record_timing(
    recorder: SpanRecorder,
    *,
    started_ns: int,
    finished_ns: int,
    sync_ns: int,
) -> None:
    total_ms = _duration_ms(started_ns, finished_ns)
    sync_ms = int(sync_ns / 1_000_000)
    recorder.add_timing(limiter_wait_ms=max(total_ms - sync_ms, 0), sync_ms=sync_ms)


def _duration_ms(started_ns: int, finished_ns: int) -> int:
    return int((finished_ns - started_ns) / 1_000_000)


def _slow_threshold_ms(op: str, override: int | None) -> int:
    if override is not None:
        return override
    key = _OP_THRESHOLD_KEYS.get(op)
    if key is None:
        return _thresholds.default_ms
    return int(getattr(_thresholds, key))


def _emit_result(result: SpanResult, *, slow_threshold_ms: int | None) -> None:
    level = logging.WARNING if result.duration_ms > _slow_threshold_ms(result.op, slow_threshold_ms) else logging.INFO
    fields: dict[str, object] = {
        "op": result.op,
        "task": result.task,
        "result": result.result,
        "duration_ms": result.duration_ms,
        "limiter_wait_ms": result.limiter_wait_ms,
        "sync_ms": result.sync_ms,
    }
    fields.update(result.extra)
    extra_format = " ".join(f"{key}=%({key})s" for key in sorted(result.extra))
    message = (
        "slurper-span op=%(op)s task=%(task)s result=%(result)s "
        "duration_ms=%(duration_ms)d limiter_wait_ms=%(limiter_wait_ms)d sync_ms=%(sync_ms)d"
    )
    if extra_format:
        message = f"{message} {extra_format}"
    log.log(level, message, fields)
