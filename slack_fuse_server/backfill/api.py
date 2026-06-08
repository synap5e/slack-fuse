"""`SlackApiBackfiller` — historical ingestion via the Slack Web API.

Per RFC §Backfill. Lifts the pagination + throttling from
`slack_fuse/backfill.py` (30-180s between `conversations.history` pages, 2-8s
between `conversations.replies` calls, one channel at a time) but changes the
write target: instead of writing JSON to the disk cache, the driver
(`backfill_channel`) writes each historical message as a `message` event via
the `OffsetWriter`, deduped on `(stream, ts)` so re-running is a no-op.

The backfiller is a pure *source* of `Message` items (the `Backfiller`
protocol). The driver owns orchestration: it counts messages, honours the
`BACKFILL_WARN_AT` / `BACKFILL_ABORT_AT` thresholds, and emits the
`backfill_started` / `backfill_completed` / `backfill_aborted` /
`slack_degraded` health events. Yielding newest-first means an aborted huge
channel keeps the truncated *head* (most-recent messages) per the RFC.
"""

from __future__ import annotations

import logging
import random
from collections.abc import AsyncIterator
from dataclasses import dataclass

import trio

from slack_fuse.models import ConversationsHistoryResponse, Message, Thread
from slack_fuse_render import ChannelId
from slack_fuse_server.backfill.types import BackfillAbortReason, Backfiller, BackfillResult
from slack_fuse_server.slurper.api import FatalAPIError, RateLimitedError, SlackClient
from slack_fuse_server.slurper.health import HealthEmitter, HealthKind
from slack_fuse_server.slurper.offsets import EventRecord, OffsetWriter

log = logging.getLogger(__name__)

# RFC §Backfill → Throttling parameters.
_DEFAULT_PAGE_SLEEP_MIN = 30.0
_DEFAULT_PAGE_SLEEP_MAX = 180.0
_DEFAULT_THREAD_SLEEP_MIN = 2.0
_DEFAULT_THREAD_SLEEP_MAX = 8.0

# Extra jitter added to a Slack-provided retry-after before resuming.
_RATE_LIMIT_JITTER_MIN = 10.0
_RATE_LIMIT_JITTER_MAX = 30.0


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


@dataclass(frozen=True, slots=True)
class BackfillContext:
    """Write sink + thresholds for the `backfill_channel` driver.

    `abort_at=None` lifts the per-channel size limit (operator override).
    """

    writer: OffsetWriter
    health: HealthEmitter
    warn_at: int
    abort_at: int | None


def _ts_float(ts: str) -> float | None:
    try:
        return float(ts)
    except ValueError:
        return None


def _is_thread_parent(msg: Message) -> bool:
    return msg.reply_count > 0 and (msg.thread_ts is None or msg.thread_ts == msg.ts)


class SlackApiBackfiller:
    """A `Backfiller` that fetches history from the Slack Web API.

    Sleep bounds are injected so tests can run with near-zero throttling; the
    slurper wires the RFC defaults (or the operator's config overrides).
    """

    def __init__(
        self,
        client: SlackClient,
        limiter: trio.CapacityLimiter,
        sleeps: SleepBounds | None = None,
    ) -> None:
        self._client = client
        self._limiter = limiter
        self._sleeps = sleeps if sleeps is not None else SleepBounds()

    @property
    def name(self) -> str:
        return "slack-api"

    async def channels_to_backfill(self) -> AsyncIterator[ChannelId]:
        """Yield every member channel (non-archived) the user can see."""
        channels = await trio.to_thread.run_sync(self._client.list_conversations, limiter=self._limiter)
        for channel in channels:
            # DMs / group DMs are always accessible; public/private need membership.
            if channel.is_member or channel.is_im or channel.is_mpim:
                yield ChannelId(channel.id)

    async def messages_for_channel(
        self,
        channel_id: ChannelId,
        since_ts: float | None = None,
    ) -> AsyncIterator[Message]:
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

    async def _paginate_history(
        self,
        channel_id: str,
        since_ts: float | None,
        thread_parents: list[str],
    ) -> AsyncIterator[Message]:
        cursor = ""
        page = 0
        while True:
            if page > 0:
                await trio.sleep(random.uniform(self._sleeps.page_min_s, self._sleeps.page_max_s))
            resp = await self._history_page(channel_id, cursor)
            if resp is None:  # rate-limited; _history_page already slept — retry same cursor
                continue
            for msg in reversed(resp.messages):
                if _is_thread_parent(msg):
                    thread_parents.append(msg.ts)
                if not _passes_since(msg.ts, since_ts):
                    continue
                yield msg
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
    ) -> AsyncIterator[Message]:
        for i, thread_ts in enumerate(thread_parents):
            if i > 0:
                await trio.sleep(random.uniform(self._sleeps.thread_min_s, self._sleeps.thread_max_s))
            thread = await self._replies(channel_id, thread_ts)
            if thread is None:
                continue
            for reply in thread.replies:
                if _passes_since(reply.ts, since_ts):
                    yield reply

    async def _history_page(self, channel_id: str, cursor: str) -> ConversationsHistoryResponse | None:
        try:
            return await trio.to_thread.run_sync(
                lambda: self._client.get_history_page(channel_id, cursor), limiter=self._limiter
            )
        except RateLimitedError as exc:
            await _sleep_rate_limited(exc.retry_after)
            return None

    async def _replies(self, channel_id: str, thread_ts: str) -> Thread | None:
        try:
            return await trio.to_thread.run_sync(
                lambda: self._client.get_replies(channel_id, thread_ts), limiter=self._limiter
            )
        except RateLimitedError as exc:
            await _sleep_rate_limited(exc.retry_after)
            return None


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


async def backfill_channel(
    backfiller: Backfiller,
    channel_id: ChannelId,
    ctx: BackfillContext,
    *,
    since_ts: float | None = None,
) -> BackfillResult:
    """Drive one channel's backfill: write `message` events, honour thresholds.

    Emits the `backfill_started` / `slack_degraded(backfill_large)` /
    `backfill_completed` / `backfill_aborted` health events around the run.
    """
    cid = channel_id.value
    stream = f"channel:{cid}"
    await ctx.health.emit(HealthKind.BACKFILL_STARTED, {"channel_id": cid})
    start = trio.current_time()

    messages = 0
    events_written = 0
    warned = False
    aborted = False

    try:
        async for msg in backfiller.messages_for_channel(channel_id, since_ts):
            if ctx.abort_at is not None and messages >= ctx.abort_at:
                aborted = True
                break
            messages += 1
            if not warned and messages >= ctx.warn_at:
                warned = True
                await ctx.health.emit(HealthKind.SLACK_DEGRADED, {"reason": "backfill_large", "channel_id": cid})
            record = EventRecord(
                stream=stream, kind="message", ts=msg.ts, payload=msg.model_dump(mode="json"), dedup=True
            )
            offset = await ctx.writer.write_event(record)
            if offset is not None:
                events_written += 1
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
