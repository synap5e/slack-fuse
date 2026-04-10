"""Background backfill of historical messages and threads into disk cache.

Two phases per channel:

1. **Day backfill** - paginate full `conversations.history` and write each
   day's messages to disk. Slow random sleeps (30-180s) between pages and
   between channels to stay well under rate limits. Tracked by
   `<channel_id>.done` markers.

2. **Thread backfill** - walk the cached day messages, find every thread
   parent (`reply_count > 0` and `thread_ts == ts`), and fetch its replies
   via `conversations.replies`. Faster sleeps (2-8s) since each call is
   cheap. Tracked by `<channel_id>.threads.done` markers so existing
   day-only-backfilled channels get re-walked for threads only.

Both phases are resumable across restarts. Already-on-disk threads are
skipped on every pass, so a partially-completed thread backfill picks up
where it left off.
"""

from __future__ import annotations

import json
import logging
import random
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import trio

from . import disk_cache
from .api import FatalAPIError, RateLimitedError, SlackClient
from .models import JsonObject, Message
from .store import ChannelEntry, SlackStore

log = logging.getLogger(__name__)

_BACKFILL_DIR = Path.home() / ".cache" / "slack-fuse" / "backfill"
_MESSAGES_DIR = Path.home() / ".cache" / "slack-fuse" / "messages"

# Skip channels whose name contains any of these substrings
SKIP_SUBSTRINGS = frozenset({
    "notification",
    "alert",
    "prod-alerts",
})

# Day backfill: glacial, big API responses
_DAY_MIN_SLEEP = 30.0
_DAY_MAX_SLEEP = 180.0

# Thread backfill: cheaper calls, much shorter sleeps
_THREAD_MIN_SLEEP = 2.0
_THREAD_MAX_SLEEP = 8.0


# === Marker files ===


def _is_day_backfilled(channel_id: str) -> bool:
    return (_BACKFILL_DIR / f"{channel_id}.done").exists()


def _mark_day_backfilled(channel_id: str) -> None:
    _BACKFILL_DIR.mkdir(parents=True, exist_ok=True)
    (_BACKFILL_DIR / f"{channel_id}.done").write_text(datetime.now(UTC).isoformat())


def _are_threads_backfilled(channel_id: str) -> bool:
    return (_BACKFILL_DIR / f"{channel_id}.threads.done").exists()


def _mark_threads_backfilled(channel_id: str) -> None:
    _BACKFILL_DIR.mkdir(parents=True, exist_ok=True)
    (_BACKFILL_DIR / f"{channel_id}.threads.done").write_text(datetime.now(UTC).isoformat())


def _should_skip(channel_name: str) -> bool:
    name = channel_name.lower()
    return any(s in name for s in SKIP_SUBSTRINGS)


# === Top-level loop ===


def _filter_channels(
    all_channels: dict[str, ChannelEntry],
) -> list[tuple[str, ChannelEntry]]:
    """Return channels that still need backfill work, logging stats."""
    to_do: list[tuple[str, ChannelEntry]] = []
    skipped = 0
    fully_done = 0
    for ch_id, entry in all_channels.items():
        if _should_skip(entry.channel.name):
            skipped += 1
        elif _is_day_backfilled(ch_id) and _are_threads_backfilled(ch_id):
            fully_done += 1
        else:
            to_do.append((ch_id, entry))
    log.info(
        "Backfill: %d channels to process (%d skipped, %d already complete)",
        len(to_do),
        skipped,
        fully_done,
    )
    return to_do


async def backfill_all(client: SlackClient, store: SlackStore, limiter: trio.CapacityLimiter) -> None:
    """Slowly backfill full history (days + thread replies) for all member channels."""
    # Let normal startup settle first
    await trio.sleep(30)

    log.info("Backfill: collecting channels")

    all_channels: dict[str, ChannelEntry] = {}
    for kind in ("channels", "dms", "group-dms", "other-channels"):
        channels = await trio.to_thread.run_sync(lambda k=kind: store.list_channels(kind=k), limiter=limiter)
        all_channels.update(channels)

    to_do = _filter_channels(all_channels)

    for i, (ch_id, entry) in enumerate(to_do):
        log.info("Backfill: [%d/%d] %s", i + 1, len(to_do), entry.channel.name)
        try:
            if not _is_day_backfilled(ch_id):
                await _day_backfill_channel(client, store, ch_id, entry.channel.name, limiter)
                _mark_day_backfilled(ch_id)
            if not _are_threads_backfilled(ch_id):
                complete = await _thread_backfill_channel(client, ch_id, entry.channel.name, limiter)
                if complete:
                    _mark_threads_backfilled(ch_id)
        except FatalAPIError:
            log.error("Backfill: fatal API error, stopping")
            return
        except Exception:
            log.warning(
                "Backfill: error on %s, skipping",
                entry.channel.name,
                exc_info=True,
            )
        # Sleep between channels too
        await trio.sleep(random.uniform(_DAY_MIN_SLEEP, _DAY_MAX_SLEEP))

    log.info("Backfill: all channels complete!")


# === Day backfill ===


async def _day_backfill_channel(
    client: SlackClient,
    store: SlackStore,
    channel_id: str,
    channel_name: str,
    limiter: trio.CapacityLimiter,
) -> None:
    """Paginate full history for one channel and write to disk cache."""
    cursor = ""
    page = 0
    total_msgs = 0
    by_date: dict[str, list[Message]] = {}

    while True:
        if page > 0:
            await trio.sleep(random.uniform(_DAY_MIN_SLEEP, _DAY_MAX_SLEEP))

        try:
            resp = await trio.to_thread.run_sync(
                lambda c=cursor: client.get_history_page(channel_id, c),
                limiter=limiter,
            )
        except RateLimitedError as e:
            wait = (e.retry_after or 60) + random.uniform(10, 30)
            log.warning(
                "Backfill: rate limited on %s, waiting %.0fs",
                channel_name,
                wait,
            )
            await trio.sleep(wait)
            continue

        for msg in resp.messages:
            try:
                ts = float(msg.ts)
                dt = datetime.fromtimestamp(ts, tz=UTC).astimezone()
                date_str = dt.strftime("%Y-%m-%d")
                by_date.setdefault(date_str, []).append(msg)
                total_msgs += 1
            except (ValueError, OSError):
                pass

        page += 1

        if not resp.has_more:
            break
        cursor = resp.response_metadata.next_cursor
        if not cursor:
            break

    # API returns newest-first within each page; reverse each day to chronological
    all_dates: set[str] = set()
    for date_str, day_msgs in by_date.items():
        day_msgs.reverse()
        dumped: list[JsonObject] = [m.model_dump(mode="json") for m in day_msgs]
        disk_cache.put_day_messages(channel_id, date_str, dumped)
        all_dates.add(date_str)

    # Update store's known dates so new dates appear in directory listings
    store.merge_known_dates(channel_id, all_dates)

    log.info(
        "Backfill: %s — %d msgs, %d days, %d pages",
        channel_name,
        total_msgs,
        len(all_dates),
        page,
    )


# === Thread backfill ===


def _collect_thread_parents(channel_id: str) -> list[str]:
    """Walk the channel's cached day messages and return all thread parent ts values.

    A thread parent is a message with `reply_count > 0` whose `thread_ts == ts`.
    Pure I/O over the disk cache; no API calls.
    """
    cache_dir = _MESSAGES_DIR / channel_id
    if not cache_dir.exists():
        return []

    seen: set[str] = set()
    for f in sorted(cache_dir.glob("*.json")):
        try:
            raw = json.loads(f.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(raw, list):
            continue
        entries = cast("list[object]", raw)
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            d = cast("dict[str, object]", entry)
            ts = d.get("ts")
            thread_ts = d.get("thread_ts")
            reply_count = d.get("reply_count", 0)
            if isinstance(ts, str) and isinstance(reply_count, int) and reply_count > 0 and thread_ts == ts:
                seen.add(ts)
    return sorted(seen)


async def _thread_backfill_channel(
    client: SlackClient,
    channel_id: str,
    channel_name: str,
    limiter: trio.CapacityLimiter,
) -> bool:
    """Fetch replies for every uncached thread parent in this channel.

    Idempotent: skips threads already on disk, so an interrupted run picks
    up where it left off on the next pass. Returns True only if every thread
    was fetched successfully.
    """
    parents = _collect_thread_parents(channel_id)
    if not parents:
        log.info("Backfill: %s — no threads to fetch", channel_name)
        return True

    to_fetch = [ts for ts in parents if disk_cache.get_thread(channel_id, ts) is None]
    already = len(parents) - len(to_fetch)
    log.info(
        "Backfill: %s — %d threads to fetch (%d already cached)",
        channel_name,
        len(to_fetch),
        already,
    )

    fetched = 0
    skipped = 0
    for i, thread_ts in enumerate(to_fetch):
        if i > 0:
            await trio.sleep(random.uniform(_THREAD_MIN_SLEEP, _THREAD_MAX_SLEEP))

        try:
            thread = await trio.to_thread.run_sync(
                lambda ts=thread_ts: client.get_replies(channel_id, ts),
                limiter=limiter,
            )
        except RateLimitedError as e:
            wait = (e.retry_after or 60) + random.uniform(10, 30)
            log.warning(
                "Backfill: rate limited on %s thread, waiting %.0fs",
                channel_name,
                wait,
            )
            await trio.sleep(wait)
            skipped += 1
            continue
        except FatalAPIError:
            raise
        except Exception:
            log.warning(
                "Backfill: failed to fetch thread %s in %s",
                thread_ts,
                channel_name,
                exc_info=True,
            )
            skipped += 1
            continue

        all_msgs = [thread.parent, *thread.replies]
        dumped: list[JsonObject] = [m.model_dump(mode="json") for m in all_msgs]
        disk_cache.put_thread(channel_id, thread_ts, dumped)
        fetched += 1

    log.info("Backfill: %s — threads complete (%d fetched, %d skipped)", channel_name, fetched, skipped)
    return skipped == 0
