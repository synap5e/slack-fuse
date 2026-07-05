"""`SlackApiBackfiller` — historical ingestion via the Slack Web API.

Per RFC §Backfill. Lifts the pagination + throttling from
`slack_fuse/backfill.py` (30-180s between `conversations.history` pages, 2-8s
between `conversations.replies` calls, one channel at a time) but changes the
write target: instead of writing JSON to the disk cache, the driver
(`backfill_channel`) writes each source page as one transaction of `message`
events via the `OffsetWriter`, deduped on `(stream, ts)` so re-running is a
no-op.

The backfiller is a pure *source* of message batches (the `Backfiller`
protocol). The driver owns orchestration: it counts messages, honours the
`BACKFILL_WARN_AT` / `BACKFILL_ABORT_AT` thresholds at history-page
boundaries, and emits the `backfill_started` / `backfill_completed` /
`backfill_aborted` / `slack_degraded` health events. Yielding newest-first
means an aborted huge channel keeps the truncated *head* (most-recent
messages) per the RFC.
"""

from __future__ import annotations

import logging
import random
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Sequence
from dataclasses import dataclass
from typing import cast

import trio
from psycopg import Connection
from psycopg.rows import TupleRow

from slack_fuse.models import ConversationsHistoryResponse, ConversationsRepliesResponse, Message
from slack_fuse_render import ChannelId
from slack_fuse_server._json import JsonObject
from slack_fuse_server.backfill.resume import ResumePlan, ThreadResume
from slack_fuse_server.backfill.types import (
    BackfillAbortReason,
    Backfiller,
    BackfillResult,
    MessageBatch,
    MessageBatchOrigin,
)
from slack_fuse_server.blocked_channels import is_channel_blocked
from slack_fuse_server.slurper.api import ChannelNotFoundError, FatalAPIError, RateLimitedError, SlackClient, Validated
from slack_fuse_server.slurper.health import HealthEmitter, HealthKind
from slack_fuse_server.slurper.ingestion import ingesting_run, make_source
from slack_fuse_server.slurper.limiters import SlurperLimiters
from slack_fuse_server.slurper.offsets import (
    PG_TIMEOUT_EXCEPTIONS,
    EventRecord,
    OffsetWriter,
    write_message_or_corrective,
)
from slack_fuse_server.slurper.spans import run_sync_with_span, span

log = logging.getLogger(__name__)

# RFC §Backfill → Throttling parameters. Tightened 2026-06-27 from 30-180s to
# 15-90s — the original conservative defaults predated the 429 / Retry-After
# handling, which already sleeps the Slack-mandated wait + jitter on rate-limit.
# 15-90s keeps us well under Slack's tier 3 ceiling (50 conversations.history
# calls/min ≈ one per 1.2s) while compressing workspace-wipe wall-clock time by
# roughly half.
_DEFAULT_PAGE_SLEEP_MIN = 15.0
_DEFAULT_PAGE_SLEEP_MAX = 90.0
_DEFAULT_THREAD_SLEEP_MIN = 2.0
_DEFAULT_THREAD_SLEEP_MAX = 8.0

# Extra jitter added to a Slack-provided retry-after before resuming.
_RATE_LIMIT_JITTER_MIN = 10.0
_RATE_LIMIT_JITTER_MAX = 30.0

_PG_TIMEOUT_RETRY_MIN_S = 0.5
_PG_TIMEOUT_RETRY_MAX_S = 2.0


@dataclass(frozen=True, slots=True)
class SleepBounds:
    """Throttle bounds for the API backfiller (RFC §Backfill → Throttling).

    Injected so tests can run with near-zero sleeps; the slurper wires the RFC
    defaults or the operator's config overrides.
    """

    page_min_s: float = _DEFAULT_PAGE_SLEEP_MIN
    page_max_s: float = _DEFAULT_PAGE_SLEEP_MAX
    thread_min_s: float = _DEFAULT_THREAD_SLEEP_MIN
    thread_max_s: float = _DEFAULT_THREAD_SLEEP_MAX


# How often `backfill_channel` emits a `backfill_progress` health event, in
# messages. Small enough that `/metrics` `in_progress.messages_so_far` advances
# visibly during a run, large enough that the per-emit DB write stays negligible
# against the API-page throttle.
_DEFAULT_PROGRESS_EVERY = 500


@dataclass(frozen=True, slots=True)
class BackfillContext:
    """Write sink + thresholds for the `backfill_channel` driver.

    `abort_at=None` lifts the per-channel size limit (operator override).
    Size thresholds are checked only at history-page boundaries: a page that
    would push the cumulative count past `abort_at` is skipped entirely, making
    the threshold a soft ceiling at the last fully committed page.
    `progress_every` controls the `backfill_progress` emission cadence.
    """

    writer: OffsetWriter
    health: HealthEmitter
    limiters: SlurperLimiters
    warn_at: int
    abort_at: int | None
    progress_every: int = _DEFAULT_PROGRESS_EVERY
    task_name: str = "backfill"


def _ts_float(ts: str) -> float | None:
    try:
        return float(ts)
    except ValueError:
        return None


def _is_thread_parent(msg: Message) -> bool:
    return msg.reply_count > 0 and (msg.thread_ts is None or msg.thread_ts == msg.ts)


def _validated_messages_from_page(
    raw: JsonObject,
    messages: Sequence[Message],
    *,
    reverse: bool = False,
) -> list[Validated[Message]]:
    raw_msgs = raw.get("messages")
    raw_list: list[object] = list(raw_msgs) if isinstance(raw_msgs, list) else []
    paired = list(zip(raw_list, messages, strict=False))
    if reverse:
        paired.reverse()
    out: list[Validated[Message]] = []
    for raw_msg, msg in paired:
        if isinstance(raw_msg, dict):
            out.append(Validated(raw=cast(JsonObject, raw_msg), model=msg))
    return out


def _records_from_validated(
    channel_id: str,
    messages: Sequence[Validated[Message]],
    source: JsonObject | None = None,
) -> tuple[EventRecord, ...]:
    stream = f"channel:{channel_id}"
    return tuple(
        EventRecord(stream=stream, kind="message", ts=wrapped.model.ts, payload=wrapped.raw, dedup=True, source=source)
        for wrapped in messages
    )


def _oldest_field(since_ts: float | None) -> str | None:
    """`--since` runs mark their pages so resume never anchors on a bounded walk."""
    return None if since_ts is None else f"{since_ts:.6f}"


def _parent_record_from_page(
    channel_id: str,
    wrapped: Validated[ConversationsRepliesResponse],
    thread_ts: str,
) -> EventRecord | None:
    """The thread parent as a corrective-capable record, when the page carries it.

    ``conversations.replies`` returns the parent as the first message of the
    first page only; resumed mid-thread pages don't include it.
    """
    for wrapped_msg in _validated_messages_from_page(wrapped.raw, wrapped.model.messages):
        if wrapped_msg.model.ts != thread_ts:
            continue
        return EventRecord(
            stream=f"channel:{channel_id}",
            kind="message",
            ts=thread_ts,
            payload=wrapped_msg.raw,
            dedup=True,
            source=make_source(producer="backfill-corrective-parent", thread_ts=thread_ts),
        )
    return None


def _merge_thread_worklist(plan: ResumePlan | None, discovered: Sequence[str]) -> tuple[ThreadResume, ...]:
    """DB-known worklist (with per-thread cursors) plus freshly discovered parents.

    Threads that already reached a `final_page=true` replies row are excluded
    even when a resumed history walk rediscovers their parents.
    """
    if plan is None:
        return tuple(ThreadResume(thread_ts=ts) for ts in discovered)
    known = {t.thread_ts for t in plan.threads}
    extra = tuple(ThreadResume(thread_ts=ts) for ts in discovered if ts not in known and ts not in plan.done_thread_ts)
    return plan.threads + extra


class SlackApiBackfiller:
    """A `Backfiller` that fetches history from the Slack Web API.

    Sleep bounds are injected so tests can run with near-zero throttling; the
    slurper wires the RFC defaults (or the operator's config overrides).
    """

    def __init__(  # noqa: PLR0913, PLR0917 - injected dependencies stay explicit, mirroring the socket runner.
        self,
        client: SlackClient,
        limiter: trio.CapacityLimiter,
        sleeps: SleepBounds | None = None,
        blocked_channel_ids: Callable[[], Awaitable[set[str]]] | None = None,
        task_name: str = "backfill",
        resume_plan: Callable[[str], Awaitable[ResumePlan | None]] | None = None,
    ) -> None:
        self._client = client
        self._limiter = limiter
        self._sleeps = sleeps if sleeps is not None else SleepBounds()
        self._blocked_channel_ids = blocked_channel_ids
        self._task_name = task_name
        self._resume_plan = resume_plan

    @property
    def name(self) -> str:
        return "slack-api"

    async def channels_to_backfill(self) -> AsyncIterator[ChannelId]:
        """Yield every member channel (non-archived) the user can see."""
        yielded: list[ChannelId] = []
        op = (
            "slurper.auto_backfill.list_channels"
            if self._task_name == "auto-backfill"
            else "slurper.backfill.list_channels"
        )
        async with span(op=op, task=self._task_name) as recorder:
            blocked: set[str]
            blocked = await self._blocked_channel_ids() if self._blocked_channel_ids is not None else set()
            channels = await run_sync_with_span(self._client.list_conversations, limiter=self._limiter, span=recorder)
            for validated in channels:
                channel = validated.model
                if channel.id in blocked:
                    continue
                # DMs / group DMs are always accessible; public/private need membership.
                if channel.is_member or channel.is_im or channel.is_mpim:
                    yielded.append(ChannelId(channel.id))
            recorder.set("channels", len(yielded))
        for channel_id in yielded:
            yield channel_id

    async def messages_for_channel(
        self,
        channel_id: ChannelId,
        since_ts: float | None = None,
    ) -> AsyncIterator[Validated[Message]]:
        """Yield historical messages for `channel_id`, newest pages first.

        Top-level messages stream out as history pages arrive (so an aborting
        driver stops early and keeps the recent head); thread replies follow
        once full-history pagination completes.
        """
        thread_parents: list[str] = []
        async for msg in self._paginate_history(channel_id.value, since_ts, thread_parents):
            yield msg
        async for reply in self._expand_threads(channel_id.value, since_ts, thread_parents):
            yield reply

    async def messages_pages_for_channel(
        self,
        channel_id: ChannelId,
        since_ts: float | None = None,
    ) -> AsyncIterator[MessageBatch]:
        """Yield one atomic write batch per Slack API response.

        When a resume-plan reader is wired and this is a full-history run,
        a crashed prior run's committed pages (recorded in `events.source`)
        skip straight to the last Slack cursor / the unfinished threads.
        """
        plan: ResumePlan | None = None
        if self._resume_plan is not None and since_ts is None:
            plan = await self._resume_plan(channel_id.value)
            if plan is not None:
                log.info(
                    "backfill: resuming %s (history_done=%s, threads=%d, done_threads=%d)",
                    channel_id.value,
                    plan.history_done,
                    len(plan.threads),
                    len(plan.done_thread_ts),
                )
        thread_parents: list[str] = []
        page_index = 0
        if plan is None or not plan.history_done:
            start_cursor = "" if plan is None else plan.history_cursor
            async for batch in self._history_batches(
                channel_id.value, since_ts, thread_parents, page_index, start_cursor=start_cursor
            ):
                page_index += 1
                yield batch
        threads = _merge_thread_worklist(plan, thread_parents)
        async for batch in self._reply_batches(channel_id.value, since_ts, threads, page_index):
            page_index += 1
            yield batch

    async def _paginate_history(
        self,
        channel_id: str,
        since_ts: float | None,
        thread_parents: list[str],
    ) -> AsyncIterator[Validated[Message]]:
        cursor = ""
        page = 0
        while True:
            if page > 0:
                await trio.sleep(random.uniform(self._sleeps.page_min_s, self._sleeps.page_max_s))
            wrapped = await self._history_page(channel_id, cursor, oldest=since_ts, page=page)
            if wrapped is None:  # rate-limited; _history_page already slept — retry same cursor
                continue
            resp = wrapped.model
            for wrapped_msg in _validated_messages_from_page(wrapped.raw, resp.messages, reverse=True):
                msg = wrapped_msg.model
                if _is_thread_parent(msg):
                    thread_parents.append(msg.ts)
                if not _passes_since(msg.ts, since_ts):
                    continue
                yield wrapped_msg
            page += 1
            if not resp.has_more:
                break
            cursor = resp.response_metadata.next_cursor
            if not cursor:
                break

    async def _expand_threads(
        self,
        channel_id: str,
        since_ts: float | None,
        thread_parents: list[str],
    ) -> AsyncIterator[Validated[Message]]:
        for i, thread_ts in enumerate(thread_parents):
            if i > 0:
                await trio.sleep(random.uniform(self._sleeps.thread_min_s, self._sleeps.thread_max_s))
            thread_msgs = await self._replies(channel_id, thread_ts)
            if thread_msgs is None:
                continue
            # ``get_replies`` returns ``[parent, *replies]`` lossless. Skip
            # the parent — the top-level history pass already yielded it —
            # and pass the genuine raw dicts through (no model_dump bridge).
            # This is what lets attachment-carrying bot replies (Linear,
            # GitHub, Datadog) actually persist their content.
            for reply in thread_msgs[1:]:
                if _passes_since(reply.model.ts, since_ts):
                    yield reply

    async def _history_batches(
        self,
        channel_id: str,
        since_ts: float | None,
        thread_parents: list[str],
        first_page_index: int,
        *,
        start_cursor: str = "",
    ) -> AsyncIterator[MessageBatch]:
        cursor = start_cursor
        page = 0
        attempt = 1
        while True:
            if page > 0:
                await trio.sleep(random.uniform(self._sleeps.page_min_s, self._sleeps.page_max_s))
            request_cursor = cursor
            wrapped = await self._history_page(channel_id, cursor, oldest=since_ts, page=page)
            if wrapped is None:  # rate-limited; _history_page already slept — retry same cursor
                attempt += 1
                continue
            resp = wrapped.model
            messages: list[Validated[Message]] = []
            for wrapped_msg in _validated_messages_from_page(wrapped.raw, resp.messages, reverse=True):
                msg = wrapped_msg.model
                if _is_thread_parent(msg):
                    thread_parents.append(msg.ts)
                if _passes_since(msg.ts, since_ts):
                    messages.append(wrapped_msg)
            next_cursor = resp.response_metadata.next_cursor
            meta = wrapped.meta
            source = make_source(
                producer="backfill-history-page",
                slack_cursor=next_cursor,
                prior_cursor=request_cursor or None,
                page_index=first_page_index + page,
                has_more=resp.has_more,
                # The loop-termination fact, not just NOT has_more: Slack can
                # report has_more with an empty cursor, and resume must see the
                # same signal the pagination loop acted on.
                final_page=not resp.has_more or not next_cursor,
                oldest=_oldest_field(since_ts),
                attempt=attempt if attempt > 1 else None,
                api_endpoint=None if meta is None else meta.endpoint,
                api_latency_ms=None if meta is None else meta.latency_ms,
                slack_request_id=None if meta is None else meta.request_id,
            )
            yield MessageBatch(
                kind="history_page",
                channel_id=channel_id,
                records=_records_from_validated(channel_id, messages, source),
                origin=MessageBatchOrigin(
                    channel_id=channel_id,
                    thread_ts=None,
                    page_index=first_page_index + page,
                    slack_cursor=request_cursor,
                ),
            )
            page += 1
            attempt = 1
            if not resp.has_more:
                break
            cursor = next_cursor
            if not cursor:
                break

    async def _reply_batches(
        self,
        channel_id: str,
        since_ts: float | None,
        threads: Sequence[ThreadResume],
        first_page_index: int,
    ) -> AsyncIterator[MessageBatch]:
        page_index = first_page_index
        for i, thread in enumerate(threads):
            if i > 0:
                await trio.sleep(random.uniform(self._sleeps.thread_min_s, self._sleeps.thread_max_s))
            thread_ts = thread.thread_ts
            request_cursor = thread.cursor
            async for wrapped in self._replies_pages(channel_id, thread_ts, start_cursor=thread.cursor):
                resp = wrapped.model
                next_cursor = resp.response_metadata.next_cursor
                meta = wrapped.meta
                source = make_source(
                    producer="backfill-replies-page",
                    thread_ts=thread_ts,
                    slack_cursor=next_cursor,
                    prior_cursor=request_cursor or None,
                    page_index=page_index,
                    has_more=resp.has_more,
                    final_page=not resp.has_more or not next_cursor,
                    oldest=_oldest_field(since_ts),
                    api_endpoint=None if meta is None else meta.endpoint,
                    api_latency_ms=None if meta is None else meta.latency_ms,
                    slack_request_id=None if meta is None else meta.request_id,
                )
                messages = [
                    wrapped_msg
                    for wrapped_msg in _validated_messages_from_page(wrapped.raw, resp.messages)
                    if wrapped_msg.model.ts != thread_ts and _passes_since(wrapped_msg.model.ts, since_ts)
                ]
                records = _records_from_validated(channel_id, messages, source)
                # The first page (cursor "") carries the thread parent with
                # Slack's *current* thread metadata. Persist it in the same
                # atomic batch as its replies: through the corrective write
                # path a stale parent (e.g. reply_count high after a real
                # reply deletion) is repaired, and a crash can never leave a
                # corrected parent without the replies that justified it.
                parent = _parent_record_from_page(channel_id, wrapped, thread_ts)
                if parent is not None:
                    records = (parent, *records)
                yield MessageBatch(
                    kind="replies_page",
                    channel_id=channel_id,
                    records=records,
                    origin=MessageBatchOrigin(
                        channel_id=channel_id,
                        thread_ts=thread_ts,
                        page_index=page_index,
                        slack_cursor=next_cursor,
                    ),
                )
                page_index += 1
                request_cursor = next_cursor

    async def _history_page(
        self,
        channel_id: str,
        cursor: str,
        oldest: float | None = None,
        page: int = 0,
    ) -> Validated[ConversationsHistoryResponse] | None:
        async with span(
            op="slurper.backfill.history_page",
            task=self._task_name,
            extra={"channel_id": channel_id, "page": page},
        ) as recorder:
            try:
                wrapped = await run_sync_with_span(
                    lambda: self._client.get_history_page(channel_id, cursor, oldest),
                    limiter=self._limiter,
                    span=recorder,
                )
            except RateLimitedError as exc:
                recorder.mark_rate_limited(exc.retry_after)
                await _sleep_rate_limited(exc.retry_after)
                return None
            recorder.set("messages", len(wrapped.model.messages))
            recorder.set("has_more", wrapped.model.has_more)
            return wrapped

    async def _replies(self, channel_id: str, thread_ts: str) -> list[Validated[Message]] | None:
        async with span(
            op="slurper.backfill.thread_replies",
            task=self._task_name,
            extra={"channel_id": channel_id, "thread_ts": thread_ts},
        ) as recorder:
            try:
                replies = await run_sync_with_span(
                    lambda: self._client.get_replies(channel_id, thread_ts),
                    limiter=self._limiter,
                    span=recorder,
                )
            except RateLimitedError as exc:
                recorder.mark_rate_limited(exc.retry_after)
                await _sleep_rate_limited(exc.retry_after)
                return None
            recorder.set("messages", len(replies))
            return replies

    async def _replies_pages(
        self,
        channel_id: str,
        thread_ts: str,
        *,
        start_cursor: str = "",
    ) -> AsyncIterator[Validated[ConversationsRepliesResponse]]:
        async with span(
            op="slurper.backfill.thread_replies",
            task=self._task_name,
            extra={"channel_id": channel_id, "thread_ts": thread_ts},
        ) as recorder:
            iterator = self._client.iter_replies_pages(channel_id, thread_ts, start_cursor)
            page = 0
            while True:
                try:
                    wrapped = await run_sync_with_span(
                        lambda: _next_replies_page(iterator),
                        limiter=self._limiter,
                        span=recorder,
                    )
                except RateLimitedError as exc:
                    recorder.mark_rate_limited(exc.retry_after)
                    await _sleep_rate_limited(exc.retry_after)
                    return
                if wrapped is None:
                    return
                recorder.set("messages", len(wrapped.model.messages))
                recorder.set("pages", page + 1)
                page += 1
                yield wrapped


def _passes_since(ts: str, since_ts: float | None) -> bool:
    if since_ts is None:
        return True
    value = _ts_float(ts)
    if value is None:
        return True
    return value > since_ts


async def _sleep_rate_limited(retry_after: float | None) -> None:
    wait = (retry_after or 60.0) + random.uniform(_RATE_LIMIT_JITTER_MIN, _RATE_LIMIT_JITTER_MAX)
    log.warning("backfill: rate limited, waiting %.0fs", wait)
    await trio.sleep(wait)


def _next_replies_page(
    iterator: Iterator[Validated[ConversationsRepliesResponse]],
) -> Validated[ConversationsRepliesResponse] | None:
    try:
        return next(iterator)
    except StopIteration:
        return None


def _write_batch_sync(conn: Connection[TupleRow], records: Sequence[EventRecord]) -> int:
    inserted = 0
    for record in records:
        offset = write_message_or_corrective(conn, record)
        if offset is not None:
            inserted += 1
    return inserted


async def _write_batch_with_retry(
    writer: OffsetWriter,
    batch: MessageBatch,
    *,
    task_name: str,
) -> int:
    extra: JsonObject = {
        "messages_in_batch": len(batch.records),
        "batch_kind": batch.kind,
        "channel_id": batch.channel_id,
    }
    if batch.origin.thread_ts is not None:
        extra["thread_ts"] = batch.origin.thread_ts
    async with span(
        op="slurper.backfill.write_batch",
        task=task_name,
        extra=extra,
    ) as recorder:
        try:
            inserted = await writer.run_transaction(lambda conn: _write_batch_sync(conn, batch.records), span=recorder)
        except PG_TIMEOUT_EXCEPTIONS:
            wait = random.uniform(_PG_TIMEOUT_RETRY_MIN_S, _PG_TIMEOUT_RETRY_MAX_S)
            log.warning(
                "backfill: PostgreSQL timeout writing batch channel=%s kind=%s records=%d; retrying once in %.2fs",
                batch.channel_id,
                batch.kind,
                len(batch.records),
                wait,
                exc_info=True,
            )
            await trio.sleep(wait)
            inserted = await writer.run_transaction(lambda conn: _write_batch_sync(conn, batch.records), span=recorder)
        recorder.set("events_written", inserted)
        return inserted


async def backfill_channel(
    backfiller: Backfiller,
    channel_id: ChannelId,
    ctx: BackfillContext,
    *,
    since_ts: float | None = None,
) -> BackfillResult:
    """Drive one channel's backfill: write `message` events, honour thresholds.

    Emits the `backfill_started` / `backfill_warn_large{channel_id}` /
    `backfill_progress{channel_id, messages_so_far}` (every `ctx.progress_every`
    messages) / `backfill_completed` / `backfill_aborted` health events around
    the run. All five are per-channel observability and do NOT affect the
    client's global ingestion-health state — see `HealthKind` for the split.
    `/metrics` reads the latest `backfill_progress` payload to populate
    `in_progress.messages_so_far`.

    One channel backfill is one logical ingestion run: every event it writes
    (messages, correctives, and the health sentinels) shares a fresh
    `source->>'run_id'`.
    """
    with ingesting_run():
        return await _backfill_channel_run(backfiller, channel_id, ctx, since_ts=since_ts)


async def _backfill_channel_run(
    backfiller: Backfiller,
    channel_id: ChannelId,
    ctx: BackfillContext,
    *,
    since_ts: float | None = None,
) -> BackfillResult:
    cid = channel_id.value
    if await ctx.writer.run_read(lambda conn: is_channel_blocked(conn, cid), limiter=ctx.limiters.admin_read):
        await ctx.health.emit(
            HealthKind.BACKFILL_SKIPPED,
            {"channel_id": cid, "reason": str(BackfillAbortReason.OPERATOR_BLOCKED)},
        )
        return BackfillResult(
            channel_id=channel_id,
            messages=0,
            events_written=0,
            elapsed_s=0.0,
            aborted=True,
            abort_reason=BackfillAbortReason.OPERATOR_BLOCKED,
        )
    await ctx.health.emit(HealthKind.BACKFILL_STARTED, {"channel_id": cid})
    start = trio.current_time()

    messages = 0
    events_written = 0
    last_progress_messages = 0
    warned = False
    aborted = False

    try:
        async for batch in backfiller.messages_pages_for_channel(channel_id, since_ts):
            batch_count = len(batch.records)
            if batch.kind == "history_page" and ctx.abort_at is not None and messages + batch_count > ctx.abort_at:
                aborted = True
                break
            inserted = await _write_batch_with_retry(ctx.writer, batch, task_name=ctx.task_name)
            events_written += inserted
            messages += batch_count
            if not warned and messages >= ctx.warn_at:
                warned = True
                # Per-channel size warning; doesn't affect global slurper health.
                # (Previously emitted SLACK_DEGRADED here, which flipped the
                # workspace-wide trailer for hours after a single large
                # backfill — see BACKLOG entry on health hysteresis.)
                await ctx.health.emit(HealthKind.BACKFILL_WARN_LARGE, {"channel_id": cid})
            if ctx.progress_every > 0 and messages >= last_progress_messages + ctx.progress_every:
                await ctx.health.emit(HealthKind.BACKFILL_PROGRESS, {"channel_id": cid, "messages_so_far": messages})
                last_progress_messages = messages
    except ChannelNotFoundError:
        # Token can no longer see this channel (archived, kicked, id renamed
        # without the projector picking it up yet). Abort just this channel —
        # NOT the whole slurper — so the auto-backfill loop can keep going.
        log.info("backfill: channel_not_found on %s; skipping", cid)
        await ctx.health.emit(
            HealthKind.BACKFILL_ABORTED,
            {"channel_id": cid, "reason": str(BackfillAbortReason.CHANNEL_NOT_FOUND), "message_count": messages},
        )
        return BackfillResult(
            channel_id=channel_id,
            messages=messages,
            events_written=events_written,
            elapsed_s=trio.current_time() - start,
            aborted=True,
            abort_reason=BackfillAbortReason.CHANNEL_NOT_FOUND,
        )
    except FatalAPIError:
        log.error("backfill: fatal API error on %s; stopping", cid)
        raise

    elapsed = trio.current_time() - start
    if aborted:
        await ctx.health.emit(
            HealthKind.BACKFILL_ABORTED,
            {"channel_id": cid, "reason": str(BackfillAbortReason.EXCEEDED_DEFAULT_LIMIT), "message_count": messages},
        )
        return BackfillResult(
            channel_id=channel_id,
            messages=messages,
            events_written=events_written,
            elapsed_s=elapsed,
            aborted=True,
            abort_reason=BackfillAbortReason.EXCEEDED_DEFAULT_LIMIT,
        )

    await ctx.health.emit(HealthKind.BACKFILL_COMPLETED, {"channel_id": cid, "events_written": events_written})
    return BackfillResult(
        channel_id=channel_id,
        messages=messages,
        events_written=events_written,
        elapsed_s=elapsed,
    )
