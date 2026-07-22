"""Durable PostgreSQL inbox for Slack HTTPS Events API deliveries.

``enqueue`` inserts the raw, already-verified envelope and calls ``pg_notify``
inside the same transaction. PostgreSQL delivers NOTIFY only after COMMIT, so
the consumer can never wake for an uncommitted row. Notifications are only a
latency optimization: a two-second polling fallback covers resets and listener
startup races.

The consumer owns a dedicated connection. It intentionally holds the selected
row lock while dispatch runs; this is the feature-specific exception to the
repository's usual "no external I/O in transactions" rule. The connection is
never borrowed from ``OffsetWriter``, so dispatch writes cannot deadlock on a
pool of size one.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, cast

import psycopg
import trio
from psycopg import Connection
from psycopg.rows import TupleRow
from psycopg.types.json import Jsonb
from pydantic import ValidationError

from slack_fuse.models import EventsApiPayload
from slack_fuse_server._json import JsonObject
from slack_fuse_server.slack_events.types import (
    DispatchErrorCode,
    DispatchPermanentError,
    DispatchTransientError,
    SlackEventSource,
)
from slack_fuse_server.slurper.health import HealthEmitter, HealthKind
from slack_fuse_server.slurper.offsets import OffsetWriter, WriterPoolExhausted
from slack_fuse_server.slurper.spans import span
from slack_fuse_server.slurper.supervisor import TaskSupervisor, phase

if TYPE_CHECKING:
    from slack_fuse_server.slurper.spans import SpanRecorder

log = logging.getLogger(__name__)

INBOX_NOTIFY_CHANNEL = "slack_event_inbox_new"
DEFAULT_POLL_INTERVAL_S = 2.0
DEFAULT_ATTEMPT_TIMEOUT_S = 30.0
MAX_DISPATCH_ATTEMPTS = 12
PROCESSED_RETENTION_HOURS = 48


class InboxDispatcher(Protocol):
    async def dispatch(
        self,
        payload: EventsApiPayload,
        raw_event: JsonObject,
        source_ctx: SlackEventSource,
        span: SpanRecorder | None = None,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class InboxRow:
    event_id: str
    envelope: JsonObject
    attempt_count: int


@dataclass(frozen=True, slots=True)
class InboxMetrics:
    depth: int
    oldest_pending_age_s: float


class InboxWriter:
    """Serialize fast inbox commits on a connection owned by the webhook."""

    def __init__(self, conn: Connection[TupleRow], limiter: trio.CapacityLimiter | None = None) -> None:
        if not conn.autocommit:
            msg = "InboxWriter requires conn.autocommit=True"
            raise ValueError(msg)
        self._conn = conn
        self._limiter = limiter or trio.CapacityLimiter(1)

    async def enqueue(self, event_id: str, envelope: JsonObject) -> bool:
        """Commit an envelope before ACK; return False for a Slack retry."""
        return await trio.to_thread.run_sync(
            lambda: enqueue(self._conn, event_id, envelope),
            limiter=self._limiter,
            abandon_on_cancel=True,
        )


def enqueue(conn: Connection[TupleRow], event_id: str, envelope: JsonObject) -> bool:
    """Insert + notify atomically; duplicates neither insert nor notify."""
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "INSERT INTO slack_event_inbox (event_id, envelope) VALUES (%s, %s) "
            "ON CONFLICT (event_id) DO NOTHING RETURNING event_id",
            (event_id, Jsonb(envelope)),
        )
        inserted = cur.fetchone() is not None
        if inserted:
            cur.execute("SELECT pg_notify(%s, %s)", (INBOX_NOTIFY_CHANNEL, event_id))
    return inserted


def dispatch_backoff_seconds(attempt_count: int) -> float:
    """Exponential retry delay, capped after attempt eight (~8.5 min)."""
    return float(2 * (2 ** min(attempt_count, 8)))


def _listen(conn: Connection[TupleRow]) -> None:
    with conn.cursor() as cur:
        cur.execute(f"LISTEN {INBOX_NOTIFY_CHANNEL}")


def _claim_pending(conn: Connection[TupleRow]) -> InboxRow | None:
    """Begin a transaction and lock one due row; caller must COMMIT/ROLLBACK."""
    with conn.cursor() as cur:
        cur.execute("BEGIN")
        cur.execute(
            "SELECT event_id, envelope, attempt_count "
            "FROM slack_event_inbox "
            "WHERE processed_at IS NULL AND dead_lettered_at IS NULL AND next_attempt_at <= NOW() "
            "ORDER BY received_at, event_id "
            "FOR UPDATE SKIP LOCKED LIMIT 1"
        )
        row = cur.fetchone()
        if row is None:
            cur.execute("COMMIT")
            return None
    envelope_raw = row[1]
    if not isinstance(envelope_raw, dict):
        # JSONB guarantees an object was originally inserted, but retain a
        # sanitized malformed path if manual SQL violated that convention.
        envelope: JsonObject = {}
    else:
        envelope = cast(JsonObject, envelope_raw)
    return InboxRow(event_id=str(row[0]), envelope=envelope, attempt_count=int(row[2]))


def _mark_processed(conn: Connection[TupleRow], event_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE slack_event_inbox SET processed_at = NOW(), last_attempt_at = NOW(), dispatch_error = NULL "
            "WHERE event_id = %s",
            (event_id,),
        )
        cur.execute("COMMIT")


def _mark_transient(
    conn: Connection[TupleRow],
    event_id: str,
    attempt_count: int,
    code: DispatchErrorCode,
) -> None:
    delay = dispatch_backoff_seconds(attempt_count)
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE slack_event_inbox SET attempt_count = %s, last_attempt_at = NOW(), "
            "next_attempt_at = NOW() + (%s * INTERVAL '1 second'), dispatch_error = %s "
            "WHERE event_id = %s",
            (attempt_count, delay, code.value, event_id),
        )
        cur.execute("COMMIT")


def _mark_dead_letter(
    conn: Connection[TupleRow],
    event_id: str,
    attempt_count: int,
    code: DispatchErrorCode,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE slack_event_inbox SET attempt_count = %s, last_attempt_at = NOW(), "
            "dead_lettered_at = NOW(), dispatch_error = %s WHERE event_id = %s",
            (attempt_count, code.value, event_id),
        )
        cur.execute("COMMIT")


def _rollback(conn: Connection[TupleRow]) -> None:
    with conn.cursor() as cur:
        cur.execute("ROLLBACK")


def _wait_for_notify(conn: Connection[TupleRow], timeout_s: float) -> None:
    # The generator times out cleanly. Polling after it returns covers a lost
    # NOTIFY, so payload contents never drive correctness.
    next(conn.notifies(timeout=timeout_s, stop_after=1), None)


def _raw_inner_event(envelope: JsonObject) -> JsonObject:
    raw = envelope.get("event")
    if not isinstance(raw, dict):
        raise DispatchPermanentError(DispatchErrorCode.MALFORMED_PAYLOAD)
    return cast(JsonObject, raw)


async def _emit_failure_health(
    health: HealthEmitter,
    kind: HealthKind,
    row: InboxRow,
    attempt_count: int,
    code: DispatchErrorCode,
) -> None:
    try:
        await health.emit(
            kind,
            {"event_id": row.event_id, "attempt_count": attempt_count, "error_code": code.value},
        )
    except (psycopg.Error, WriterPoolExhausted) as exc:
        # Observability is best-effort after the inbox state has committed.
        # Log only the exception class: DB messages may contain statement data.
        log.error("webhook inbox health emission failed exception_type=%s", type(exc).__name__)


async def _dispatch_claimed(  # noqa: PLR0913, PLR0917 - queue state and transport capabilities stay explicit.
    conn: Connection[TupleRow],
    row: InboxRow,
    dispatcher: InboxDispatcher,
    health: HealthEmitter,
    supervisor: TaskSupervisor | None,
    attempt_timeout_s: float,
) -> None:
    tx_open = True
    error: DispatchTransientError | DispatchPermanentError | None = None
    try:
        async with span(
            op="slurper.webhook.dispatch",
            task="webhook-consumer",
            extra={"event_id": row.event_id},
        ) as recorder:
            phase_scope = (
                phase(
                    supervisor,
                    "webhook-consumer",
                    "dispatching",
                    details={"event_id": row.event_id},
                    deadline_s=attempt_timeout_s,
                )
                if supervisor is not None
                else _null_async_context()
            )
            async with phase_scope:
                try:
                    payload = EventsApiPayload.model_validate(row.envelope)
                    raw_event = _raw_inner_event(row.envelope)
                    with trio.fail_after(attempt_timeout_s):
                        await dispatcher.dispatch(
                            payload,
                            raw_event,
                            SlackEventSource(transport="http", event_id=row.event_id),
                            span=recorder,
                        )
                except DispatchPermanentError as exc:
                    error = exc
                except DispatchTransientError as exc:
                    error = exc
                except ValidationError:
                    error = DispatchPermanentError(DispatchErrorCode.MALFORMED_PAYLOAD)
                except trio.TooSlowError:
                    error = DispatchTransientError(DispatchErrorCode.UNKNOWN_TRANSIENT)
                except Exception as exc:  # noqa: BLE001 - interpose a sanitized retry code at the queue boundary.
                    log.error("webhook dispatch raised unexpected exception_type=%s", type(exc).__name__)
                    error = DispatchTransientError(DispatchErrorCode.UNKNOWN_TRANSIENT)

        if error is None:
            await trio.to_thread.run_sync(lambda: _mark_processed(conn, row.event_id))
            tx_open = False
            return

        new_attempts = row.attempt_count + 1
        dead_letter = isinstance(error, DispatchPermanentError) or new_attempts >= MAX_DISPATCH_ATTEMPTS
        if dead_letter:
            await trio.to_thread.run_sync(lambda: _mark_dead_letter(conn, row.event_id, new_attempts, error.code))
        else:
            await trio.to_thread.run_sync(lambda: _mark_transient(conn, row.event_id, new_attempts, error.code))
        tx_open = False
        await _emit_failure_health(health, HealthKind.WEBHOOK_DISPATCH_FAILED, row, new_attempts, error.code)
        if dead_letter:
            await _emit_failure_health(health, HealthKind.WEBHOOK_DEAD_LETTER, row, new_attempts, error.code)
    finally:
        if tx_open:
            await trio.to_thread.run_sync(lambda: _rollback(conn))


class _null_async_context:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *_args: object) -> None:
        return None


async def consume(  # noqa: PLR0913 - public worker wiring keeps dedicated resources explicit.
    conn: Connection[TupleRow],
    dispatcher: InboxDispatcher,
    health: HealthEmitter,
    supervisor: TaskSupervisor | None = None,
    *,
    poll_interval_s: float = DEFAULT_POLL_INTERVAL_S,
    attempt_timeout_s: float = DEFAULT_ATTEMPT_TIMEOUT_S,
) -> None:
    """Consume forever; infrastructure failures deliberately escape."""
    if not conn.autocommit:
        msg = "inbox consumer requires conn.autocommit=True"
        raise ValueError(msg)
    await trio.to_thread.run_sync(lambda: _listen(conn))
    while True:
        if supervisor is not None:
            supervisor.declare("webhook-consumer", "selecting", deadline_s=5)
        row = await trio.to_thread.run_sync(lambda: _claim_pending(conn))
        if row is not None:
            await _dispatch_claimed(conn, row, dispatcher, health, supervisor, attempt_timeout_s)
            continue
        if supervisor is not None:
            supervisor.declare("webhook-consumer", "waiting", deadline_s=poll_interval_s + 1)
        await trio.to_thread.run_sync(lambda: _wait_for_notify(conn, poll_interval_s), abandon_on_cancel=True)


async def ensure_consumer_never_returns(consumer: Callable[[], Awaitable[None]]) -> None:
    """Turn an accidental clean return into a nursery-fatal liveness failure."""
    await consumer()
    raise RuntimeError("inbox consumer must not exit")


def delete_processed_older_than(conn: Connection[TupleRow], *, hours: int = PROCESSED_RETENTION_HOURS) -> int:
    """Delete old successful rows; dead letters are retained indefinitely."""
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "DELETE FROM slack_event_inbox "
            "WHERE processed_at < NOW() - (%s * INTERVAL '1 hour') "
            "AND dead_lettered_at IS NULL",
            (hours,),
        )
        return int(cur.rowcount)


async def run_retention(
    connect: Callable[[], Connection[TupleRow]],
    *,
    interval_s: float = 3600.0,
) -> None:
    """Hourly cleanup using a fresh short-lived connection each pass."""
    while True:
        conn = await trio.to_thread.run_sync(connect)
        try:
            deleted = await trio.to_thread.run_sync(
                lambda retention_conn=conn: delete_processed_older_than(retention_conn)
            )
            if deleted:
                log.info("webhook inbox retention deleted=%d", deleted)
        finally:
            await trio.to_thread.run_sync(conn.close)
        await trio.sleep(interval_s)


def read_inbox_metrics(conn: Connection[TupleRow]) -> InboxMetrics:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*), COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(received_at))), 0) "
            "FROM slack_event_inbox WHERE processed_at IS NULL AND dead_lettered_at IS NULL"
        )
        row = cur.fetchone()
    if row is None:  # pragma: no cover - aggregate SELECT always returns one row
        return InboxMetrics(depth=0, oldest_pending_age_s=0.0)
    return InboxMetrics(depth=int(row[0]), oldest_pending_age_s=max(0.0, float(row[1])))


async def emit_telemetry(
    writer: OffsetWriter,
    health: HealthEmitter,
    read_limiter: trio.CapacityLimiter,
    supervisor: TaskSupervisor | None = None,
    *,
    interval_s: float = 30.0,
) -> None:
    """Publish queue pressure and a monotonically increasing liveness pulse."""
    counter = 0
    while True:
        metrics = await writer.run_read(read_inbox_metrics, limiter=read_limiter)
        counter += 1
        await health.emit(HealthKind.WEBHOOK_INBOX_DEPTH, {"value": metrics.depth})
        await health.emit(
            HealthKind.WEBHOOK_INBOX_OLDEST_PENDING_AGE_S,
            {"value": round(metrics.oldest_pending_age_s, 3)},
        )
        await health.emit(HealthKind.WEBHOOK_CONSUMER_ALIVE, {"counter": counter})
        if supervisor is not None:
            supervisor.declare(
                "webhook-telemetry",
                "sleeping",
                details={"counter": counter},
                deadline_s=interval_s + 5,
            )
        await trio.sleep(interval_s)


__all__ = [
    "InboxMetrics",
    "InboxRow",
    "InboxWriter",
    "consume",
    "delete_processed_older_than",
    "dispatch_backoff_seconds",
    "emit_telemetry",
    "enqueue",
    "ensure_consumer_never_returns",
    "read_inbox_metrics",
    "run_retention",
]
