"""Unified Slack probe registry and its single periodic sweep.

Raw data-loss detection probes use ``SlurperHealthSink`` to write API captures
to the singleton ``slurper-health`` stream. Interpreted operational facts use
``EventFactsSink`` to atomically append their purpose-specific event kinds.
Both families share this scheduler, task supervision, spans, and API budgets;
there is no second fact-probe nursery task.

The hourly registry scan is inherited from the existing raw-detection system.
Per-kind event-derived cadence means the six-hour fact probe can be at most one
scan late, and avoids folding raw per-channel targets every minute merely to
discover that their daily/weekly jobs are not due.

Cadence is event-derived: due checks read the latest persisted sample for the
same job/target using ``events.created_at``. A restart therefore resumes from
the event log rather than from in-memory timers.

Slack ``conversations.history`` timestamp bounds are exclusive unless callers
pass ``inclusive=true``. These probes deliberately do not pass ``inclusive``:
``latest=<local_oldest_ts>`` asks Slack for messages strictly older than the
oldest local active message, and the day-presence windows tolerate excluding
a message landing on an exact microsecond boundary (that can only delay a
detection, never fabricate one).

The three ``conversations_history_sampled`` sampling purposes are disjoint on
``call_params`` keys: newest-message samples carry neither bound,
older-than-oldest samples carry only ``latest``, day-presence samples carry
both ``oldest`` and ``latest``. Due checks and detection SQL rely on this.
"""

from __future__ import annotations

import contextlib
import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Literal, LiteralString, Protocol, cast

import httpx
import trio
from psycopg import Connection
from psycopg.rows import TupleRow

from slack_fuse_server._json import JsonObject
from slack_fuse_server.probes import (
    ProbeDeps,
    ProbeKind,
    ProbeScope,
    ProbeTarget,
    SlackTier,
    SlurperHealthSink,
    register_fact_probes,
    validate_registry,
)
from slack_fuse_server.slurper.api import ChannelNotFoundError, SlackAPIError, SlackClient
from slack_fuse_server.slurper.ingestion import ingesting_run
from slack_fuse_server.slurper.limiters import SlurperLimiters
from slack_fuse_server.slurper.offsets import EventRecord, OffsetWriter
from slack_fuse_server.slurper.spans import SpanRecorder, run_sync_with_span, span
from slack_fuse_server.slurper.supervisor import TaskSupervisor, phase

log = logging.getLogger(__name__)

HEALTH_STREAM = "slurper-health"
HEALTH_SINK = SlurperHealthSink(HEALTH_STREAM)

CONVERSATIONS_HISTORY_SAMPLED = "conversations_history_sampled"
CONVERSATIONS_LIST_SAMPLED = "conversations_list_sampled"
USERS_LIST_SAMPLED = "users_list_sampled"
PROBE_SWEEP_COMPLETED = "probe_sweep_completed"

JOB_CHANNEL_OLDER_THAN_OLDEST = "channel_older_than_oldest_exists"
JOB_CHANNEL_NEWEST_MESSAGE = "channel_newest_message"
JOB_CHANNEL_INVENTORY = "channel_inventory"
JOB_WORKSPACE_USER_COUNT = "workspace_user_count"
JOB_CHANNEL_DAY_PRESENCE = "channel_day_presence"

DEFAULT_PROBE_SWEEP_INTERVAL_S = 60 * 60.0
DEFAULT_CHANNEL_OLDER_THAN_OLDEST_CADENCE_S = 7 * 24 * 60 * 60.0
DEFAULT_CHANNEL_NEWEST_MESSAGE_CADENCE_S = 24 * 60 * 60.0
DEFAULT_CHANNEL_INVENTORY_CADENCE_S = 24 * 60 * 60.0
DEFAULT_WORKSPACE_USER_COUNT_CADENCE_S = 24 * 60 * 60.0
DEFAULT_CHANNEL_DAY_PRESENCE_CADENCE_S = 7 * 24 * 60 * 60.0

_TS_RE = re.compile(r"^[0-9]+\.[0-9]+$")
_WORKSPACE_TARGET = "workspace"
_CONVERSATION_TYPES = "public_channel,private_channel,im,mpim"
_USERS_LIST_LIMIT = 200
_HISTORY_SAMPLE_LIMIT = 1
_DAY_S = 86400
# Rolling detection window: the last N complete UTC days. Today is excluded
# because a partial day cannot prove a gap. Each run samples exactly one day
# (the stalest), so per-channel API spend self-paces to window/cadence calls
# per day (30/7 ≈ 4.3) once the window is fully sampled.
_DAY_PRESENCE_WINDOW_DAYS = 30


class ProbeCadenceConfig(Protocol):
    @property
    def probe_sweep_interval_s(self) -> float: ...

    @property
    def probe_channel_older_than_oldest_cadence_s(self) -> float: ...

    @property
    def probe_channel_newest_message_cadence_s(self) -> float: ...

    @property
    def probe_channel_inventory_cadence_s(self) -> float: ...

    @property
    def probe_workspace_user_count_cadence_s(self) -> float: ...

    @property
    def probe_channel_day_presence_cadence_s(self) -> float: ...


ProbeDescriptor = ProbeKind


@dataclass(frozen=True, slots=True)
class ProbeSweepRequest:
    """One manual probe-sweep request.

    ``None`` means "all" for both fields. A non-``None`` target is valid only
    for per-channel probe jobs.
    """

    job_id: str | None = None
    target: str | None = None

    def details(self) -> JsonObject:
        return {"job_id": self.job_id, "target": self.target}


class ProbeTrigger:
    """Bounded trigger used by ``POST /probe-sweep`` to request manual runs."""

    def __init__(self, *, max_buffer_size: int = 1) -> None:
        self._send, self._recv = trio.open_memory_channel[ProbeSweepRequest](max_buffer_size=max_buffer_size)

    def request(self, *, job_id: str | None = None, target: str | None = None) -> bool:
        try:
            self._send.send_nowait(ProbeSweepRequest(job_id=job_id, target=target))
        except trio.WouldBlock:
            return False
        return True

    async def __aexit__(self, *_args: object) -> None:
        await self._send.aclose()
        await self._recv.aclose()

    async def consume(  # noqa: PLR0913, PLR0917 - mirrors refresh trigger consumer wiring.
        self,
        writer: OffsetWriter,
        client: SlackClient,
        limiters: SlurperLimiters,
        supervisor: TaskSupervisor | None,
        registry: Sequence[ProbeDescriptor],
        interval_s: float,
    ) -> None:
        """Drain manual probe-sweep requests one at a time."""
        active_registry = tuple(registry)
        while True:
            if supervisor is not None:
                supervisor.declare("probe-sweep-trigger", "waiting_for_trigger", deadline_s=None)
            try:
                request = await self._recv.receive()
            except trio.EndOfChannel:
                return
            try:
                await _run_probe_cycle(
                    writer,
                    client,
                    limiters,
                    supervisor,
                    active_registry,
                    trigger="manual",
                    requested=request,
                    bypass_cadence=True,
                    task_name="probe-sweep-trigger",
                    deadline_s=interval_s * 0.5,
                )
            except Exception:
                log.exception("probe-sweep: manual trigger failed for %s", request.details())


def build_probe_registry(config: ProbeCadenceConfig) -> tuple[ProbeDescriptor, ...]:
    """Apply ``ServerConfig`` cadence fields to the static probe registry."""
    return tuple(
        replace(
            descriptor,
            interval_s=(
                descriptor.interval_s
                if descriptor.cadence_config_field is None
                else float(getattr(config, descriptor.cadence_config_field))
            ),
        )
        for descriptor in PROBE_REGISTRY
    )


async def probe_sweep(  # noqa: PLR0913, PLR0917 - nursery task wiring passes explicit service dependencies.
    writer: OffsetWriter,
    client: SlackClient,
    limiters: SlurperLimiters,
    supervisor: TaskSupervisor | None,
    config: ProbeCadenceConfig,
    trigger: ProbeTrigger | None = None,
    *,
    registry: Sequence[ProbeDescriptor] | None = None,
    run_once: bool = False,
) -> None:
    """Run probe cycles forever, or once when ``run_once`` is true for tests."""
    active_registry = tuple(registry) if registry is not None else build_probe_registry(config)
    sweep_interval_s = float(config.probe_sweep_interval_s)

    if run_once:
        await _run_probe_cycle(
            writer,
            client,
            limiters,
            supervisor,
            active_registry,
            trigger="scheduled",
            requested=None,
            bypass_cadence=False,
            task_name="probe-sweep",
            deadline_s=None,
        )
        return

    if trigger is not None:
        async with trio.open_nursery() as nursery:
            nursery.start_soon(
                _run_probe_periodic_loop,
                writer,
                client,
                limiters,
                supervisor,
                active_registry,
                sweep_interval_s,
            )
            nursery.start_soon(
                trigger.consume,
                writer,
                client,
                limiters,
                supervisor,
                active_registry,
                sweep_interval_s,
            )
        return

    await _run_probe_periodic_loop(
        writer,
        client,
        limiters,
        supervisor,
        active_registry,
        sweep_interval_s,
    )


async def _run_probe_periodic_loop(  # noqa: PLR0913, PLR0917 - task wiring keeps dependencies explicit.
    writer: OffsetWriter,
    client: SlackClient,
    limiters: SlurperLimiters,
    supervisor: TaskSupervisor | None,
    registry: Sequence[ProbeDescriptor],
    sweep_interval_s: float,
) -> None:
    while True:
        await _run_probe_cycle(
            writer,
            client,
            limiters,
            supervisor,
            registry,
            trigger="scheduled",
            requested=None,
            bypass_cadence=False,
            task_name="probe-sweep",
            deadline_s=None,
        )
        if supervisor is not None:
            supervisor.declare("probe-sweep", "sleeping_until", deadline_s=None)
        await trio.sleep(sweep_interval_s)


async def _run_probe_cycle(  # noqa: PLR0913 - common scheduled/manual runner.
    writer: OffsetWriter,
    client: SlackClient,
    limiters: SlurperLimiters,
    supervisor: TaskSupervisor | None,
    registry: Sequence[ProbeDescriptor],
    *,
    trigger: Literal["scheduled", "manual"],
    requested: ProbeSweepRequest | None,
    bypass_cadence: bool,
    task_name: str,
    deadline_s: float | None,
) -> None:
    started_at = _utc_iso()
    active_registry = tuple(registry)
    counters: dict[str, dict[str, int]] = {
        descriptor.job_id: {"succeeded": 0, "failed": 0, "skipped": 0} for descriptor in active_registry
    }
    selected = _select_probe_descriptors(active_registry, requested, trigger=trigger)
    with ingesting_run(triggered_by="scheduled" if trigger == "scheduled" else "control-surface"):
        await _run_probe_cycle_body(
            writer,
            client,
            limiters,
            supervisor,
            selected,
            counters,
            started_at=started_at,
            trigger=trigger,
            requested=requested,
            bypass_cadence=bypass_cadence,
            task_name=task_name,
            deadline_s=deadline_s,
        )


async def _run_probe_cycle_body(  # noqa: PLR0913, PLR0917 - mirrors _run_probe_cycle's explicit knobs.
    writer: OffsetWriter,
    client: SlackClient,
    limiters: SlurperLimiters,
    supervisor: TaskSupervisor | None,
    selected: tuple[ProbeDescriptor, ...],
    counters: dict[str, dict[str, int]],
    *,
    started_at: str,
    trigger: Literal["scheduled", "manual"],
    requested: ProbeSweepRequest | None,
    bypass_cadence: bool,
    task_name: str,
    deadline_s: float | None,
) -> None:
    deps = ProbeDeps(client=client, writer=writer, limiters=limiters)
    for descriptor in selected:
        target = None if requested is None else requested.target
        if supervisor is None:
            await _run_probe_descriptor(
                deps,
                descriptor,
                counters[descriptor.job_id],
                requested_target=target,
                bypass_cadence=bypass_cadence,
                trigger=trigger,
            )
        else:
            details: JsonObject = {} if target is None else {"target": target}
            async with phase(supervisor, task_name, descriptor.job_id, details=details, deadline_s=deadline_s):
                await _run_probe_descriptor(
                    deps,
                    descriptor,
                    counters[descriptor.job_id],
                    requested_target=target,
                    bypass_cadence=bypass_cadence,
                    trigger=trigger,
                )

    await _emit_probe_sweep_completed(
        writer,
        started_at=started_at,
        counters=counters,
        triggered_by=trigger,
        requested=requested,
    )


def _select_probe_descriptors(
    registry: Sequence[ProbeDescriptor],
    requested: ProbeSweepRequest | None,
    *,
    trigger: Literal["scheduled", "manual"],
) -> tuple[ProbeDescriptor, ...]:
    selectable = tuple(
        descriptor for descriptor in registry if trigger == "scheduled" or descriptor.manual_triggerable
    )
    if requested is None or requested.job_id is None:
        return selectable
    return tuple(descriptor for descriptor in selectable if descriptor.job_id == requested.job_id)


async def run_probe_registry_once(
    supervisor: TaskSupervisor,
    deps: ProbeDeps,
    registry: Sequence[ProbeDescriptor],
) -> dict[str, dict[str, int]]:
    """Dispatch one scheduled registry tick through the production runner.

    The long-lived sweep and deterministic framework tests share
    ``_run_probe_descriptor``; this helper omits only the health heartbeat and
    outer ingestion envelope owned by ``_run_probe_cycle``.
    """
    active_registry = validate_registry(registry)
    counters = {probe.job_id: {"succeeded": 0, "failed": 0, "skipped": 0} for probe in active_registry}
    for descriptor in active_registry:
        async with phase(supervisor, "probe-sweep", descriptor.job_id, deadline_s=None):
            await _run_probe_descriptor(
                deps,
                descriptor,
                counters[descriptor.job_id],
                trigger="scheduled",
            )
    return counters


async def _run_probe_descriptor(  # noqa: C901, PLR0913 - target error isolation keeps knobs explicit.
    deps: ProbeDeps,
    descriptor: ProbeDescriptor,
    counts: dict[str, int],
    *,
    requested_target: str | None = None,
    bypass_cadence: bool = False,
    trigger: Literal["scheduled", "manual"] = "scheduled",
) -> None:
    if requested_target is not None:
        targets: Sequence[ProbeTarget] = (ProbeTarget(requested_target, "channel_id"),)
    else:
        try:
            targets = await descriptor.targets(deps)
        except Exception:
            log.exception("probe job %s failed while listing targets", descriptor.job_id)
            counts["failed"] += 1
            return

    for target in targets:
        if not bypass_cadence:
            try:
                due = await is_due(deps, descriptor, target)
            except Exception:
                log.exception("probe job %s failed while checking cadence for %s", descriptor.job_id, target.value)
                counts["failed"] += 1
                continue
            if not due:
                counts["skipped"] += 1
                continue
        extra = target.span_extra()
        extra["job_id"] = descriptor.job_id
        extra["event_kind"] = descriptor.event_kind
        extra["tier"] = descriptor.tier
        extra["trigger"] = trigger
        probe_span: SpanRecorder | None = None
        try:
            async with span(op=descriptor.op, task="probe-sweep", extra=extra) as probe_span:
                probe_span.set("events_written", 0)
                probe_span.set("outcome", "error")
                run_deps = replace(deps, recorder=probe_span)
                records = tuple(await descriptor.run(run_deps, target))
                events_written = await descriptor.sink.write(run_deps, descriptor, records)
                probe_span.set("outcome", "ok")
                if not events_written:
                    probe_span.mark_skipped()
        except ChannelNotFoundError:
            # Channel deleted, archived-and-purged, or the token was removed
            # from it. Archive the target so future folds stop polling it.
            log.info(
                "probe job %s: channel %s no longer accessible (channel_not_found); evicting",
                descriptor.job_id,
                target.value,
            )
            if target.payload_field == "channel_id":
                with contextlib.suppress(Exception):
                    await deps.writer.write_event(
                        EventRecord(
                            stream="channel-list",
                            kind="channel_archived",
                            ts=None,
                            payload={"channel_id": target.value},
                        ),
                        span=probe_span,
                    )
            counts["failed"] += 1
        except (SlackAPIError, httpx.HTTPError, ValueError):
            log.warning("probe job %s failed on %s", descriptor.job_id, target.value, exc_info=True)
            counts["failed"] += 1
        except Exception:
            log.exception("probe job %s failed on %s", descriptor.job_id, target.value)
            counts["failed"] += 1
        else:
            if events_written:
                counts["succeeded"] += 1
            else:
                counts["skipped"] += 1


async def is_due(
    deps: ProbeDeps,
    descriptor: ProbeDescriptor,
    target: ProbeTarget,
) -> bool:
    """Return true when the latest persisted sample is older than the cadence."""
    return await deps.writer.run_read(
        lambda conn: descriptor.due(conn, target, descriptor.cadence_s, deps.clock()),
        limiter=deps.limiters.admin_read,
    )


async def _channel_targets(deps: ProbeDeps) -> Sequence[ProbeTarget]:
    channel_ids = await deps.writer.run_read(
        _list_in_scope_channel_ids_sync,
        limiter=deps.limiters.admin_read,
    )
    return tuple(ProbeTarget(channel_id, "channel_id") for channel_id in channel_ids)


async def _channel_targets_with_local_messages(
    deps: ProbeDeps,
) -> Sequence[ProbeTarget]:
    channel_ids = await deps.writer.run_read(
        _list_in_scope_channel_ids_with_local_messages_sync,
        limiter=deps.limiters.admin_read,
    )
    return tuple(ProbeTarget(channel_id, "channel_id") for channel_id in channel_ids)


async def _workspace_targets(_deps: ProbeDeps) -> Sequence[ProbeTarget]:
    await trio.lowlevel.checkpoint()
    return (ProbeTarget(_WORKSPACE_TARGET, None),)


def _list_in_scope_channel_ids_sync(conn: Connection[TupleRow]) -> list[str]:
    """Fold channel-list events to the current probe target set.

    A channel is in scope when the latest facts say it is not archived and is
    either a joined channel, a DM, or an MPIM. Operator-blocked channels are
    excluded because they are deliberately out of ingestion scope.
    """
    states: dict[str, dict[str, object]] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT kind, payload
            FROM events
            WHERE stream = 'channel-list'
            ORDER BY offset_in_stream
            """
        )
        rows = cur.fetchall()
        cur.execute("SELECT channel_id FROM blocked_channels")
        blocked = {str(row[0]) for row in cur.fetchall() if row[0] is not None}

    for kind_raw, payload_raw in rows:
        _apply_channel_list_event(states, str(kind_raw), payload_raw)
    return sorted(channel_id for channel_id, state in states.items() if _is_probe_target(channel_id, state, blocked))


def _apply_channel_list_event(states: dict[str, dict[str, object]], kind: str, payload_raw: object) -> None:
    if not isinstance(payload_raw, dict):
        return
    payload = cast(dict[str, object], payload_raw)
    if kind in {"channel_added", "channel_info_refreshed"}:
        channel_id = payload.get("id")
        if isinstance(channel_id, str) and channel_id:
            states[channel_id] = dict(payload)
        return

    channel_id = payload.get("channel_id")
    if not isinstance(channel_id, str) or channel_id not in states:
        return
    state = dict(states[channel_id])
    if kind == "channel_archived":
        state["is_archived"] = True
    elif kind == "channel_unarchived":
        state["is_archived"] = False
    elif kind == "channel_member_changed":
        is_member = payload.get("is_member")
        if isinstance(is_member, bool):
            state["is_member"] = is_member
    states[channel_id] = state


def _is_probe_target(channel_id: str, state: dict[str, object], blocked: set[str]) -> bool:
    if channel_id in blocked:
        return False
    is_archived = state.get("is_archived") is True
    is_member = state.get("is_member") is True
    is_im = state.get("is_im") is True
    is_mpim = state.get("is_mpim") is True
    return not is_archived and (is_member or is_im or is_mpim)


def _list_in_scope_channel_ids_with_local_messages_sync(conn: Connection[TupleRow]) -> list[str]:
    return [
        channel_id
        for channel_id in _list_in_scope_channel_ids_sync(conn)
        if _local_oldest_ts_sync(conn, channel_id) is not None
    ]


async def _sample_older_than_oldest_history(
    deps: ProbeDeps,
    target: ProbeTarget,
) -> tuple[EventRecord, ...]:
    channel_id = target.value
    local_oldest_ts = await deps.writer.run_read(
        lambda conn: _local_oldest_ts_sync(conn, channel_id),
        limiter=deps.limiters.admin_read,
        span=deps.recorder,
    )
    if local_oldest_ts is None:
        return ()
    call_params: JsonObject = {"channel": channel_id, "latest": local_oldest_ts, "limit": _HISTORY_SAMPLE_LIMIT}
    response = await run_sync_with_span(
        lambda: deps.client.sample_conversations_history(
            channel_id=channel_id,
            latest=local_oldest_ts,
            limit=_HISTORY_SAMPLE_LIMIT,
        ),
        limiter=deps.limiters.slack_api,
        span=deps.recorder,
    )
    _record_sample_stats(deps.recorder, response, "messages")
    return (
        EventRecord(
            stream=HEALTH_STREAM,
            kind=CONVERSATIONS_HISTORY_SAMPLED,
            ts=None,
            payload={"call_params": call_params, "response": response, "captured_at": _utc_iso()},
        ),
    )


async def _sample_newest_history(
    deps: ProbeDeps,
    target: ProbeTarget,
) -> tuple[EventRecord, ...]:
    channel_id = target.value
    call_params: JsonObject = {"channel": channel_id, "limit": _HISTORY_SAMPLE_LIMIT}
    response = await run_sync_with_span(
        lambda: deps.client.sample_conversations_history(channel_id=channel_id, limit=_HISTORY_SAMPLE_LIMIT),
        limiter=deps.limiters.slack_api,
        span=deps.recorder,
    )
    _record_sample_stats(deps.recorder, response, "messages")
    return (
        EventRecord(
            stream=HEALTH_STREAM,
            kind=CONVERSATIONS_HISTORY_SAMPLED,
            ts=None,
            payload={"call_params": call_params, "response": response, "captured_at": _utc_iso()},
        ),
    )


@dataclass(frozen=True, slots=True)
class _DayWindow:
    """One complete UTC day expressed as Slack ts-string bounds."""

    oldest: str
    latest: str


def _presence_day_windows(now: datetime) -> tuple[_DayWindow, ...]:
    """The last ``_DAY_PRESENCE_WINDOW_DAYS`` complete UTC days, most recent first."""
    today_start = datetime(now.year, now.month, now.day, tzinfo=UTC)
    windows: list[_DayWindow] = []
    for offset in range(1, _DAY_PRESENCE_WINDOW_DAYS + 1):
        day_epoch = int((today_start - timedelta(days=offset)).timestamp())
        windows.append(_DayWindow(oldest=f"{day_epoch}.000000", latest=f"{day_epoch + _DAY_S - 1}.999999"))
    return tuple(windows)


def _presence_sample_ages_sync(
    conn: Connection[TupleRow],
    channel_id: str,
    day_starts: Sequence[str],
) -> dict[str, float]:
    """Seconds since the latest day-presence sample, keyed by day-start ts.

    Days never sampled are absent from the result. Only rows carrying both
    ``oldest`` and ``latest`` count: that key shape is exclusive to the
    day-presence job.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT payload->'call_params'->>'oldest',
                   EXTRACT(EPOCH FROM (now() - max(created_at)))
            FROM events
            WHERE stream = %s
              AND kind = %s
              AND payload->'call_params'->>'channel' = %s
              AND payload->'call_params' ? 'oldest'
              AND payload->'call_params' ? 'latest'
              AND payload->'call_params'->>'oldest' = ANY(%s)
            GROUP BY 1
            """,
            (HEALTH_STREAM, CONVERSATIONS_HISTORY_SAMPLED, channel_id, list(day_starts)),
        )
        rows = cur.fetchall()
    return {str(row[0]): float(row[1]) for row in rows if row[0] is not None and row[1] is not None}


def _day_presence_due_sync(
    conn: Connection[TupleRow],
    target: ProbeTarget,
    cadence_s: float,
    now: datetime | None = None,
) -> bool:
    windows = _presence_day_windows(datetime.now(UTC) if now is None else now)
    ages = _presence_sample_ages_sync(conn, target.value, [window.oldest for window in windows])
    return any(ages.get(window.oldest, float("inf")) >= cadence_s for window in windows)


async def _sample_day_presence_history(
    deps: ProbeDeps,
    target: ProbeTarget,
) -> tuple[EventRecord, ...]:
    """Sample one (channel, day) window for message presence.

    Detects mid-stream gaps that head/tail probes cannot: a single
    post-reconnect live message advances the local newest ts past a lost
    window, but a day probe still sees Slack holding messages where the local
    ``active_messages`` view has none. Each call samples the stalest window
    (never-sampled first, most-recent-day tiebreak) so recent days — the most
    likely to hold fresh gaps — fill in first.
    """
    channel_id = target.value
    windows = _presence_day_windows(datetime.now(UTC))
    ages = await deps.writer.run_read(
        lambda conn: _presence_sample_ages_sync(conn, channel_id, [window.oldest for window in windows]),
        limiter=deps.limiters.admin_read,
        span=deps.recorder,
    )
    window = max(windows, key=lambda candidate: ages.get(candidate.oldest, float("inf")))
    call_params: JsonObject = {
        "channel": channel_id,
        "oldest": window.oldest,
        "latest": window.latest,
        "limit": _HISTORY_SAMPLE_LIMIT,
    }
    response = await run_sync_with_span(
        lambda: deps.client.sample_conversations_history(
            channel_id=channel_id,
            oldest=window.oldest,
            latest=window.latest,
            limit=_HISTORY_SAMPLE_LIMIT,
        ),
        limiter=deps.limiters.slack_api,
        span=deps.recorder,
    )
    _record_sample_stats(deps.recorder, response, "messages")
    return (
        EventRecord(
            stream=HEALTH_STREAM,
            kind=CONVERSATIONS_HISTORY_SAMPLED,
            ts=None,
            payload={"call_params": call_params, "response": response, "captured_at": _utc_iso()},
        ),
    )


async def _sample_channel_inventory(
    deps: ProbeDeps,
    _target: ProbeTarget,
) -> tuple[EventRecord, ...]:
    call_params: JsonObject = {"types": _CONVERSATION_TYPES, "exclude_archived": True}
    response = await run_sync_with_span(
        lambda: deps.client.sample_conversations_list(types=_CONVERSATION_TYPES, exclude_archived=True),
        limiter=deps.limiters.slack_api,
        span=deps.recorder,
    )
    _record_sample_stats(deps.recorder, response, "channels")
    return (
        EventRecord(
            stream=HEALTH_STREAM,
            kind=CONVERSATIONS_LIST_SAMPLED,
            ts=None,
            payload={"call_params": call_params, "response": response, "captured_at": _utc_iso()},
        ),
    )


async def _sample_workspace_users(
    deps: ProbeDeps,
    _target: ProbeTarget,
) -> tuple[EventRecord, ...]:
    call_params: JsonObject = {"limit": _USERS_LIST_LIMIT}
    response = await run_sync_with_span(
        lambda: deps.client.sample_users_list(limit=_USERS_LIST_LIMIT),
        limiter=deps.limiters.slack_api,
        span=deps.recorder,
    )
    _record_sample_stats(deps.recorder, response, "members")
    return (
        EventRecord(
            stream=HEALTH_STREAM,
            kind=USERS_LIST_SAMPLED,
            ts=None,
            payload={"call_params": call_params, "response": response, "captured_at": _utc_iso()},
        ),
    )


def _local_oldest_ts_sync(conn: Connection[TupleRow], channel_id: str) -> str | None:
    timestamps = _active_message_timestamps_sync(conn, channel_id)
    if not timestamps:
        return None
    return min(timestamps, key=lambda ts: Decimal(ts))


def _active_message_timestamps_sync(conn: Connection[TupleRow], channel_id: str) -> list[str]:
    active: set[str] = set()
    stream = f"channel:{channel_id}"
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT kind, payload
            FROM events
            WHERE stream = %s
              AND kind IN ('message', 'message_changed', 'message_deleted')
            ORDER BY offset_in_stream
            """,
            (stream,),
        )
        rows = cur.fetchall()

    for kind_raw, payload_raw in rows:
        if not isinstance(payload_raw, dict):
            continue
        kind = str(kind_raw)
        payload = cast(dict[str, object], payload_raw)
        if kind == "message":
            ts = _valid_ts(payload.get("ts"))
            if ts is not None:
                active.add(ts)
            continue
        if kind == "message_changed":
            message_raw = payload.get("message")
            message = cast(dict[str, object], message_raw) if isinstance(message_raw, dict) else None
            new_ts = None if message is None else _valid_ts(message.get("ts"))
            previous_ts = _valid_ts(payload.get("previous_ts"))
            if previous_ts is not None and previous_ts != new_ts:
                active.discard(previous_ts)
            if new_ts is not None:
                active.add(new_ts)
            continue
        if kind == "message_deleted":
            deleted_ts = _valid_ts(payload.get("deleted_ts"))
            if deleted_ts is not None:
                active.discard(deleted_ts)
    return sorted(active, key=lambda ts: Decimal(ts))


def _valid_ts(value: object) -> str | None:
    if not isinstance(value, str) or _TS_RE.fullmatch(value) is None:
        return None
    return value


def _history_older_due_sync(
    conn: Connection[TupleRow],
    target: ProbeTarget,
    cadence_s: float,
    _now: datetime | None = None,
) -> bool:
    # latest-only: day-presence samples also carry `latest` (plus `oldest`)
    # and must not reset this job's cadence.
    return _latest_sample_is_due(
        conn,
        """
        SELECT EXTRACT(EPOCH FROM (now() - created_at))
        FROM events
        WHERE stream = %s
          AND kind = %s
          AND payload->'call_params'->>'channel' = %s
          AND payload->'call_params'->>'latest' IS NOT NULL
          AND NOT (COALESCE(payload->'call_params', '{}'::jsonb) ? 'oldest')
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (HEALTH_STREAM, CONVERSATIONS_HISTORY_SAMPLED, target.value),
        cadence_s,
    )


def _history_newest_due_sync(
    conn: Connection[TupleRow],
    target: ProbeTarget,
    cadence_s: float,
    _now: datetime | None = None,
) -> bool:
    return _latest_sample_is_due(
        conn,
        """
        SELECT EXTRACT(EPOCH FROM (now() - created_at))
        FROM events
        WHERE stream = %s
          AND kind = %s
          AND payload->'call_params'->>'channel' = %s
          AND NOT (COALESCE(payload->'call_params', '{}'::jsonb) ? 'latest')
          AND NOT (COALESCE(payload->'call_params', '{}'::jsonb) ? 'oldest')
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (HEALTH_STREAM, CONVERSATIONS_HISTORY_SAMPLED, target.value),
        cadence_s,
    )


def _conversations_list_due_sync(
    conn: Connection[TupleRow],
    _target: ProbeTarget,
    cadence_s: float,
    _now: datetime | None = None,
) -> bool:
    return _event_kind_due_sync(conn, CONVERSATIONS_LIST_SAMPLED, cadence_s)


def _users_list_due_sync(
    conn: Connection[TupleRow],
    _target: ProbeTarget,
    cadence_s: float,
    _now: datetime | None = None,
) -> bool:
    return _event_kind_due_sync(conn, USERS_LIST_SAMPLED, cadence_s)


def _event_kind_due_sync(conn: Connection[TupleRow], event_kind: str, cadence_s: float) -> bool:
    return _latest_sample_is_due(
        conn,
        """
        SELECT EXTRACT(EPOCH FROM (now() - created_at))
        FROM events
        WHERE stream = %s
          AND kind = %s
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (HEALTH_STREAM, event_kind),
        cadence_s,
    )


def _latest_sample_is_due(
    conn: Connection[TupleRow],
    query: LiteralString,
    params: tuple[object, ...],
    cadence_s: float,
) -> bool:
    with conn.cursor() as cur:
        cur.execute(query, params)
        row = cur.fetchone()
    if row is None or row[0] is None:
        return True
    return float(row[0]) >= cadence_s


async def _write_probe_event(
    writer: OffsetWriter,
    kind: str,
    payload: JsonObject,
    recorder: SpanRecorder | None,
) -> None:
    await writer.write_event(EventRecord(stream=HEALTH_STREAM, kind=kind, ts=None, payload=payload), span=recorder)


async def _emit_probe_sweep_completed(
    writer: OffsetWriter,
    *,
    started_at: str,
    counters: dict[str, dict[str, int]],
    triggered_by: Literal["scheduled", "manual"] = "scheduled",
    requested: ProbeSweepRequest | None = None,
) -> None:
    payload: JsonObject = {
        "started_at": started_at,
        "ended_at": _utc_iso(),
        "triggered_by": triggered_by,
        "requested": None if requested is None else requested.details(),
        "probes": cast(JsonObject, {job_id: dict(counts) for job_id, counts in counters.items()}),
    }
    await _write_probe_event(writer, PROBE_SWEEP_COMPLETED, payload, None)


def _record_sample_stats(recorder: SpanRecorder | None, response: JsonObject, collection_key: str) -> None:
    if recorder is None:
        return
    collection = response.get(collection_key)
    if isinstance(collection, list):
        recorder.set(collection_key, len(collection))
    page_count = response.get("page_count")
    if isinstance(page_count, int):
        recorder.set("page_count", page_count)


RAW_PROBE_REGISTRY: tuple[ProbeDescriptor, ...] = (
    ProbeKind(
        job_id=JOB_CHANNEL_OLDER_THAN_OLDEST,
        kind=CONVERSATIONS_HISTORY_SAMPLED,
        interval_s=DEFAULT_CHANNEL_OLDER_THAN_OLDEST_CADENCE_S,
        run=_sample_older_than_oldest_history,
        targets=_channel_targets_with_local_messages,
        due=_history_older_due_sync,
        op="slurper.probe.conversations_history",
        tier=SlackTier.TIER_3,
        scope=ProbeScope.PER_CHANNEL,
        sink=HEALTH_SINK,
        cadence_config_field="probe_channel_older_than_oldest_cadence_s",
    ),
    ProbeKind(
        job_id=JOB_CHANNEL_NEWEST_MESSAGE,
        kind=CONVERSATIONS_HISTORY_SAMPLED,
        interval_s=DEFAULT_CHANNEL_NEWEST_MESSAGE_CADENCE_S,
        run=_sample_newest_history,
        targets=_channel_targets,
        due=_history_newest_due_sync,
        op="slurper.probe.conversations_history",
        tier=SlackTier.TIER_3,
        scope=ProbeScope.PER_CHANNEL,
        sink=HEALTH_SINK,
        cadence_config_field="probe_channel_newest_message_cadence_s",
    ),
    ProbeKind(
        job_id=JOB_CHANNEL_DAY_PRESENCE,
        kind=CONVERSATIONS_HISTORY_SAMPLED,
        interval_s=DEFAULT_CHANNEL_DAY_PRESENCE_CADENCE_S,
        run=_sample_day_presence_history,
        targets=_channel_targets,
        due=_day_presence_due_sync,
        op="slurper.probe.conversations_history",
        tier=SlackTier.TIER_3,
        scope=ProbeScope.PER_CHANNEL,
        sink=HEALTH_SINK,
        cadence_config_field="probe_channel_day_presence_cadence_s",
    ),
    ProbeKind(
        job_id=JOB_CHANNEL_INVENTORY,
        kind=CONVERSATIONS_LIST_SAMPLED,
        interval_s=DEFAULT_CHANNEL_INVENTORY_CADENCE_S,
        run=_sample_channel_inventory,
        targets=_workspace_targets,
        due=_conversations_list_due_sync,
        op="slurper.probe.conversations_list",
        tier=SlackTier.TIER_2,
        scope=ProbeScope.WORKSPACE,
        sink=HEALTH_SINK,
        cadence_config_field="probe_channel_inventory_cadence_s",
    ),
    ProbeKind(
        job_id=JOB_WORKSPACE_USER_COUNT,
        kind=USERS_LIST_SAMPLED,
        interval_s=DEFAULT_WORKSPACE_USER_COUNT_CADENCE_S,
        run=_sample_workspace_users,
        targets=_workspace_targets,
        due=_users_list_due_sync,
        op="slurper.probe.users_list",
        tier=SlackTier.TIER_2,
        scope=ProbeScope.WORKSPACE,
        sink=HEALTH_SINK,
        cadence_config_field="probe_workspace_user_count_cadence_s",
    ),
)

PROBE_REGISTRY: tuple[ProbeDescriptor, ...] = (*RAW_PROBE_REGISTRY, *register_fact_probes())


def _utc_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")
