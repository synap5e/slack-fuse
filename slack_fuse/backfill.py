"""Background backfill of historical messages into disk cache.

Slowly paginates full history for each member channel, buckets messages
by date, and writes them to the disk cache. Runs as a background trio
task with long random sleeps between API calls to stay well under rate
limits. Progress is tracked per-channel so it resumes across restarts.
"""

from __future__ import annotations

import logging
import random
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import trio

from .api import FatalAPIError, RateLimitedError, SlackClient, parse_message
from .disk_cache import put_day_messages
from .models import message_to_dict
from .store import ChannelEntry, SlackStore

log = logging.getLogger(__name__)

_BACKFILL_DIR = Path.home() / ".cache" / "slack-fuse" / "backfill"

# Skip channels whose name contains any of these substrings
SKIP_SUBSTRINGS = frozenset({
    "notification",
    "alert",
    "prod-alerts",
})

_MIN_SLEEP = 30.0
_MAX_SLEEP = 180.0


def _is_backfilled(channel_id: str) -> bool:
    return (_BACKFILL_DIR / f"{channel_id}.done").exists()


def _mark_backfilled(channel_id: str) -> None:
    _BACKFILL_DIR.mkdir(parents=True, exist_ok=True)
    (_BACKFILL_DIR / f"{channel_id}.done").write_text(
        datetime.now(UTC).isoformat()
    )


def _should_skip(channel_name: str) -> bool:
    name = channel_name.lower()
    return any(s in name for s in SKIP_SUBSTRINGS)


async def backfill_all(
    client: SlackClient,
    store: SlackStore,
) -> None:
    """Slowly backfill full history for all member channels."""
    # Let normal startup settle first
    await trio.sleep(30)

    log.info("Backfill: collecting channels")

    all_channels: dict[str, ChannelEntry] = {}
    for kind in ("channels", "dms", "group-dms", "other-channels"):
        all_channels.update(store.list_channels(kind=kind))

    to_do: list[tuple[str, ChannelEntry]] = []
    skipped = 0
    already_done = 0
    for ch_id, entry in all_channels.items():
        if _should_skip(entry.channel.name):
            skipped += 1
            continue
        if _is_backfilled(ch_id):
            already_done += 1
            continue
        to_do.append((ch_id, entry))

    log.info(
        "Backfill: %d channels to process (%d skipped, %d already done)",
        len(to_do),
        skipped,
        already_done,
    )

    for i, (ch_id, entry) in enumerate(to_do):
        log.info(
            "Backfill: [%d/%d] %s",
            i + 1,
            len(to_do),
            entry.channel.name,
        )
        try:
            await _backfill_channel(client, store, ch_id, entry.channel.name)
            _mark_backfilled(ch_id)
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
        await trio.sleep(random.uniform(_MIN_SLEEP, _MAX_SLEEP))

    log.info("Backfill: all channels complete!")


async def _backfill_channel(
    client: SlackClient,
    store: SlackStore,
    channel_id: str,
    channel_name: str,
) -> None:
    """Paginate full history for one channel and write to disk cache."""
    cursor = ""
    page = 0
    total_msgs = 0
    by_date: dict[str, list[dict[str, Any]]] = {}

    while True:
        if page > 0:
            await trio.sleep(random.uniform(_MIN_SLEEP, _MAX_SLEEP))

        try:
            data = client.get_history_page(channel_id, cursor)
        except RateLimitedError as e:
            wait = (e.retry_after or 60) + random.uniform(10, 30)
            log.warning(
                "Backfill: rate limited on %s, waiting %.0fs",
                channel_name,
                wait,
            )
            await trio.sleep(wait)
            continue

        for msg_raw in data.get("messages", []):
            try:
                ts = float(msg_raw["ts"])
                dt = datetime.fromtimestamp(ts, tz=UTC).astimezone()
                date_str = dt.strftime("%Y-%m-%d")
                by_date.setdefault(date_str, []).append(msg_raw)
                total_msgs += 1
            except (ValueError, KeyError, OSError):
                pass

        page += 1

        if not data.get("has_more", False):
            break
        cursor = data.get("response_metadata", {}).get("next_cursor", "")
        if not cursor:
            break

    # API returns newest-first within each page; reverse each day to chronological
    all_dates: set[str] = set()
    for date_str, raw_msgs in by_date.items():
        raw_msgs.reverse()
        parsed = [parse_message(m) for m in raw_msgs]
        put_day_messages(
            channel_id,
            date_str,
            [message_to_dict(m) for m in parsed],
        )
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
