"""Reconnect / restart catchup: a bounded gap-fill for dropped events.

Slack's Socket Mode delivery buffers events for only a few minutes. Any
downtime longer than that — a rollout, a crash, a network partition — drops
the events that occurred while we were away. The polling-free live path never
recovers them; before this module the only fix was a manual
``slack-fuse-server backfill <channel>`` after the fact.

This task closes the gap automatically. For every member channel it calls
``conversations.history`` with ``oldest=<resume point>`` and writes the result
through the normal offset-assignment path (deduped on ``(stream, ts)``, so a
re-run is a no-op). Startup catchup writes a per-channel
``backfill-run:<channel>`` lifecycle; reconnect catchup keeps the older
message-only behavior because ``reconnect`` is not a backfill-run trigger.

**Two triggers**

- *Startup* — the slurper restarts on every deploy, so a fresh process always
  runs one catchup. This is the case the in-process reconnect path cannot see
  (a new process has no record of the previous connection's disconnect), and
  it is the one that motivated the work.
- *In-process reconnect* — when a live connection re-establishes after a
  downtime longer than ``gap_threshold_s``, the socket runner nudges the
  trigger (`should_catchup`). Brief, graceful Slack-initiated reconnects have a
  tiny gap and are skipped — Slack's buffer covered them.

**Resume point** (`resolve_since_ts`)

- If we have message events for the channel, resume from ``MAX(ts)`` — this
  captures the whole gap no matter how long ago we last saw activity (a quiet
  channel is one cheap page thanks to Slack's ``oldest`` bound).
- If we have none (a never-active or never-backfilled channel), fall back to a
  bounded ``now - max_lookback_s`` floor so the sweep stays a *small* job and
  never degrades into a full initial ingest — that remains admin-CLI-only.
"""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

import trio

from slack_fuse_render import ChannelId
from slack_fuse_server.backfill.api import write_backfill_batch_with_retry
from slack_fuse_server.backfill.run_events import (
    new_backfill_run_id,
    resolve_trigger,
    run_finished_record,
    run_started_record,
    started_params,
)
from slack_fuse_server.backfill.types import Backfiller, BackfillRunOutcome, BackfillRunTrigger
from slack_fuse_server.slurper.ingestion import current_ingestion_context, ingesting_run
from slack_fuse_server.slurper.limiters import SlurperLimiters
from slack_fuse_server.slurper.offsets import PG_TIMEOUT_EXCEPTIONS, EventRecord, OffsetWriter
from slack_fuse_server.slurper.spans import span
from slack_fuse_server.slurper.supervisor import TaskSupervisor, phase

if TYPE_CHECKING:
    import psycopg
    from psycopg.rows import TupleRow

    from slack_fuse_server.slurper.spans import SpanRecorder

log = logging.getLogger(__name__)

#: Reconnect downtime (seconds) beyond which Slack's event buffer has drained
#: and a catchup is warranted. Below this, Slack redelivers on reconnect.
DEFAULT_GAP_THRESHOLD_S = 300.0

#: Lookback floor for channels with no prior message events: bound the resume
#: point so a fresh-state catchup grabs only recent history, never a full
#: backfill.
DEFAULT_MAX_LOOKBACK_S = 3600.0

#: Sleep between channels so a 100+-channel sweep stays under the
#: ``conversations.history`` Tier 3 budget (~50/min). 1.5s ≈ 40/min.
DEFAULT_CHANNEL_GAP_S = 1.5

#: Delay before the startup catchup, so the populate one-shots and the live
#: socket connection settle before we add history traffic.
DEFAULT_STARTUP_DELAY_S = 30.0

_PG_TIMEOUT_RETRY_MIN_S = 0.5
_PG_TIMEOUT_RETRY_MAX_S = 2.0


@dataclass(frozen=True, slots=True)
class CatchupConfig:
    """Tunables for the catchup sweep (defaults mirror ``ServerConfig``)."""

    gap_threshold_s: float = DEFAULT_GAP_THRESHOLD_S
    max_lookback_s: float = DEFAULT_MAX_LOOKBACK_S
    channel_gap_s: float = DEFAULT_CHANNEL_GAP_S
    startup_delay_s: float = DEFAULT_STARTUP_DELAY_S


@dataclass(frozen=True, slots=True)
class CatchupResult:
    """Per-cycle outcome, mirrored into the ``catchup: cycle complete`` log."""

    channels: int
    events: int
    errors: int
    elapsed_s: float


@dataclass(frozen=True, slots=True)
class CatchupDeps:
    """Everything the sweep needs: the write sink, the history source, tunables."""

    writer: OffsetWriter
    backfiller: Backfiller
    config: CatchupConfig
    limiters: SlurperLimiters


_CHANNEL_STREAM_PREFIX = "channel:"


def should_catchup(gap_seconds: float, *, threshold_s: float) -> bool:
    """Pure gap-detection: a reconnect whose downtime exceeds ``threshold_s``
    needs a catchup (Slack's event buffer has drained). Graceful, Slack-
    initiated reconnects have a tiny gap and return False."""
    return gap_seconds > threshold_s


def resolve_since_ts(
    channel_id: str,
    last_seen: dict[str, float],
    *,
    now_epoch: float,
    max_lookback_s: float,
) -> float:
    """Resume point (Slack ``oldest``) for one channel's catchup.

    Resume from the last message we persisted when we have one; otherwise from
    a bounded ``now - max_lookback_s`` floor so an empty channel never triggers
    a full-history fetch.
    """
    prev = last_seen.get(f"{_CHANNEL_STREAM_PREFIX}{channel_id}")
    if prev is not None:
        return prev
    return now_epoch - max_lookback_s


def last_seen_ts_by_stream(conn: psycopg.Connection[TupleRow]) -> dict[str, float]:
    """One batched ``MAX(ts)`` per ``channel:*`` stream over message events.

    A single query for the whole workspace rather than one per channel —
    catchups are rare enough that the grouped scan is cheaper than 100+ point
    lookups. ``ts`` is stored as text; the cast orders it numerically.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT stream, MAX(ts::double precision)
            FROM events
            WHERE kind = 'message'
              AND stream LIKE 'channel:%'
              AND ts IS NOT NULL
            GROUP BY stream
            """,
        )
        result: dict[str, float] = {}
        for stream_raw, max_ts in cur.fetchall():
            if max_ts is not None:
                result[str(stream_raw)] = float(max_ts)
        return result


def latest_ingest_head_by_channel(conn: psycopg.Connection[TupleRow]) -> dict[str, float]:
    """Read the derived ingest-head view used by startup catchup."""
    with conn.cursor() as cur:
        cur.execute("SELECT channel_id, latest_ts FROM channel_ingest_head")
        result: dict[str, float] = {}
        for channel_id_raw, latest_ts_raw in cur.fetchall():
            if latest_ts_raw is not None:
                result[str(channel_id_raw)] = float(str(latest_ts_raw))
        return result


def has_in_progress_backfill_run_for_boot(
    conn: psycopg.Connection[TupleRow],
    channel_id: str,
    boot_id: str,
) -> bool:
    """Whether this boot already started this channel's latest run and has no terminator."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM channel_backfill_state state
            JOIN events started
              ON started.stream = 'backfill-run:' || state.channel_id
             AND started.kind = 'backfill_run_started'
             AND started.payload->>'run_id' = state.last_run_id
            WHERE state.channel_id = %s
              AND state.last_run_finished_at IS NULL
              AND started.source->>'boot_id' = %s
            LIMIT 1
            """,
            (channel_id, boot_id),
        )
        return cur.fetchone() is not None


async def catchup_channel(
    backfiller: Backfiller,
    writer: OffsetWriter,
    channel_id: ChannelId,
    since_ts: float,
    *,
    run_trigger: BackfillRunTrigger | None = None,
) -> int:
    """Gap-fill one channel from ``since_ts``; return the number of NEW events.

    Drives the same paginated history + thread-walk the backfiller uses, but
    writes each message directly (deduped on ``(stream, ts)``) instead of going
    through the health-emitting ``backfill_channel`` driver. Already-present
    messages dedup to no-ops, so the count is genuinely-recovered events only.
    """
    async with span(
        op="slurper.catchup.catch_up_channel",
        task="catchup",
        extra={"channel_id": channel_id.value},
    ) as recorder:
        if run_trigger is None:
            stream = f"{_CHANNEL_STREAM_PREFIX}{channel_id.value}"
            events = 0
            async for wrapped in backfiller.messages_for_channel(channel_id, since_ts):
                record = EventRecord(
                    stream=stream, kind="message", ts=wrapped.model.ts, payload=wrapped.raw, dedup=True
                )
                offset = await _write_message_or_corrective_retry_once(writer, record, span=recorder)
                if offset is not None:
                    events += 1
        else:
            events = await _catchup_channel_with_run_events(
                backfiller,
                writer,
                channel_id,
                since_ts,
                run_trigger=run_trigger,
            )
        recorder.set("events_written", events)
        return events


async def _catchup_channel_with_run_events(
    backfiller: Backfiller,
    writer: OffsetWriter,
    channel_id: ChannelId,
    since_ts: float,
    *,
    run_trigger: BackfillRunTrigger,
) -> int:
    with ingesting_run(triggered_by=str(run_trigger)):
        run_id = new_backfill_run_id()
        trigger = resolve_trigger(run_trigger, default=BackfillRunTrigger.STARTUP)
        cid = channel_id.value
        await writer.write_event(
            run_started_record(
                channel_id=cid,
                run_id=run_id,
                triggered_by=trigger,
                params=started_params(since_ts=since_ts),
            )
        )
        start = trio.current_time()
        events = 0
        try:
            async for batch in backfiller.messages_pages_for_channel(channel_id, since_ts):
                events += await write_backfill_batch_with_retry(writer, batch, task_name="catchup", run_id=run_id)
        except Exception as exc:
            outcome = BackfillRunOutcome.FATAL_ERROR
            await writer.write_event(
                run_finished_record(
                    channel_id=cid,
                    run_id=run_id,
                    outcome=outcome,
                    messages_written_total=events,
                    elapsed_s=trio.current_time() - start,
                    error_reason=type(exc).__name__,
                )
            )
            raise
        await writer.write_event(
            run_finished_record(
                channel_id=cid,
                run_id=run_id,
                outcome=BackfillRunOutcome.COMPLETED,
                messages_written_total=events,
                elapsed_s=trio.current_time() - start,
            )
        )
        return events


async def _write_message_or_corrective_retry_once(
    writer: OffsetWriter,
    record: EventRecord,
    *,
    span: SpanRecorder | None = None,
) -> int | None:
    try:
        return await writer.write_message_or_corrective(record, span=span)
    except PG_TIMEOUT_EXCEPTIONS:
        wait = random.uniform(_PG_TIMEOUT_RETRY_MIN_S, _PG_TIMEOUT_RETRY_MAX_S)
        log.warning(
            "catchup: PostgreSQL timeout writing stream=%s kind=%s; retrying once in %.2fs",
            record.stream,
            record.kind,
            wait,
            exc_info=True,
        )
        await trio.sleep(wait)
        return await writer.write_message_or_corrective(record, span=span)


async def run_catchup_once(
    deps: CatchupDeps,
    *,
    now_epoch: float | None = None,
    supervisor: TaskSupervisor | None = None,
    backfill_trigger: BackfillRunTrigger | None = None,
) -> CatchupResult:
    """One full sweep: gap-fill every member channel from its resume point.

    ``now_epoch`` is injectable for tests; it defaults to wall-clock time and is
    used only for the ``max_lookback`` floor. A single channel's API failure is
    logged and counted, never fatal — one unreachable channel must not abort the
    recovery of the rest.
    """
    now = now_epoch if now_epoch is not None else time.time()
    if supervisor is None:
        last_seen = await deps.writer.run_read(
            latest_ingest_head_by_channel if backfill_trigger is not None else last_seen_ts_by_stream,
            limiter=deps.limiters.admin_read,
        )
    else:
        async with phase(supervisor, "catchup", "listing_channels", deadline_s=60):
            last_seen = await deps.writer.run_read(
                latest_ingest_head_by_channel if backfill_trigger is not None else last_seen_ts_by_stream,
                limiter=deps.limiters.admin_read,
            )
    boot_id = _current_boot_id() if backfill_trigger is not None else None
    start = trio.current_time()
    channels = 0
    events = 0
    errors = 0
    first = True
    async for channel_id in deps.backfiller.channels_to_backfill():
        if not first:
            await trio.sleep(deps.config.channel_gap_s)
        first = False
        channels += 1
        if boot_id is not None:
            already_running = await deps.writer.run_read(
                lambda conn, cid=channel_id.value, bid=boot_id: has_in_progress_backfill_run_for_boot(conn, cid, bid),
                limiter=deps.limiters.admin_read,
            )
            if already_running:
                log.info("catchup: skipping %s; startup run already in progress for this boot", channel_id.value)
                continue
        since_ts = resolve_since_ts(
            channel_id.value,
            _last_seen_for_resolve(last_seen, backfill_trigger=backfill_trigger),
            now_epoch=now,
            max_lookback_s=deps.config.max_lookback_s,
        )
        try:
            if supervisor is None:
                events += await catchup_channel(
                    deps.backfiller,
                    deps.writer,
                    channel_id,
                    since_ts,
                    run_trigger=backfill_trigger,
                )
            else:
                async with phase(
                    supervisor,
                    "catchup",
                    "catching_up_channel",
                    details={"channel_id": channel_id.value},
                    deadline_s=300,
                ):
                    events += await catchup_channel(
                        deps.backfiller,
                        deps.writer,
                        channel_id,
                        since_ts,
                        run_trigger=backfill_trigger,
                    )
        except Exception:
            log.info("catchup: channel %s failed; continuing", channel_id.value, exc_info=True)
            errors += 1
    elapsed = trio.current_time() - start
    log.info(
        "catchup: cycle complete channels=%d events=%d errors=%d elapsed=%.1fs",
        channels,
        events,
        errors,
        elapsed,
    )
    return CatchupResult(channels=channels, events=events, errors=errors, elapsed_s=elapsed)


def _current_boot_id() -> str | None:
    ctx = current_ingestion_context()
    return None if ctx is None else ctx.boot_id


def _last_seen_for_resolve(
    last_seen: dict[str, float],
    *,
    backfill_trigger: BackfillRunTrigger | None,
) -> dict[str, float]:
    if backfill_trigger is None:
        return last_seen
    return {f"{_CHANNEL_STREAM_PREFIX}{channel_id}": latest_ts for channel_id, latest_ts in last_seen.items()}


class CatchupTrigger:
    """Single-slot rendezvous so a catchup runs at startup and on gap reconnect.

    ``request(gap)`` is a non-blocking nudge from the socket loop; a buffer of
    one means an in-flight sweep keeps at most one follow-up queued (a second
    reconnect while a catchup runs coalesces — the queued run picks up whatever
    the running one didn't). The consumer always runs once at startup before
    waiting, because a restart is the primary gap source.
    """

    def __init__(self) -> None:
        self._send, self._recv = trio.open_memory_channel[float](max_buffer_size=1)

    def request(self, gap_seconds: float) -> bool:
        """Ask for a catchup. Returns False if one is already queued (dropped)."""
        try:
            self._send.send_nowait(gap_seconds)
        except trio.WouldBlock:
            return False
        return True

    async def consume(self, deps: CatchupDeps, supervisor: TaskSupervisor | None = None) -> None:
        """Trio task: startup catchup, then one per queued reconnect request.

        Spawned in the main nursery. Supervisor catches inside the cycle so a
        single bad sweep doesn't take the task down.
        """
        if supervisor is not None:
            supervisor.declare("catchup", "startup_delay", deadline_s=None)
        await trio.sleep(deps.config.startup_delay_s)
        await self._safe_run(deps, "startup", "startup", supervisor)
        while True:
            if supervisor is not None:
                supervisor.declare("catchup", "idle", deadline_s=None)
            try:
                gap = await self._recv.receive()
            except trio.EndOfChannel:
                return
            await self._safe_run(deps, f"reconnect gap={gap:.0f}s", "reconnect", supervisor)

    async def _safe_run(
        self,
        deps: CatchupDeps,
        trigger: str,
        triggered_by: str,
        supervisor: TaskSupervisor | None,
    ) -> None:
        log.info("catchup: starting cycle (%s)", trigger)
        try:
            # One sweep = one logical run: every event it writes shares a
            # fresh source run_id and the startup/reconnect trigger fact.
            with ingesting_run(triggered_by=triggered_by):
                await run_catchup_once(
                    deps,
                    supervisor=supervisor,
                    backfill_trigger=BackfillRunTrigger.STARTUP if triggered_by == "startup" else None,
                )
        except Exception:
            log.exception("catchup: cycle failed")
