"""Pre-render locked-in markdown to ``~/.cache/slack-fuse/archive/``.

Walks every channel's cached day messages + thread replies and writes the
rendered markdown to a flat directory tree so ripgrep can hit native disk
instead of going through FUSE. On a 50K-file corpus that's the difference
between a ~40s whole-tree grep and a sub-second one.

**Strictly excludes today's data.** Only dates strictly before the local
``today`` are archived — matching the infinite-TTL policy in ``store.py``
where any past local date is considered locked-in and never re-fetched.
Today's content is still served live through FUSE; it's intentionally
never written here so stale bytes don't linger on disk.

Layout mirrors the FUSE mount::

    ~/.cache/slack-fuse/archive/
        channels/<slug>/<YYYY-MM>/<DD>/channel.md
        channels/<slug>/<YYYY-MM>/<DD>/feed.md
        channels/<slug>/<YYYY-MM>/<DD>/<thread-slug>/thread.md
        channels/<slug>/<YYYY-MM>/<DD>/<thread-slug>/feed.md
        dms/...
        group-dms/...
        other-channels/...

Idempotent and resumable: skips files already on disk. Runs
``cached_only_mode()`` to guarantee no API traffic — only already-cached
data makes it into the archive.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import trio

from .store import SlackStore

log = logging.getLogger(__name__)

_ARCHIVE_DIR = Path.home() / ".cache" / "slack-fuse" / "archive"

# Re-scan cadence. Picks up newly-locked-in data (yesterday rolls over at
# local midnight) and any channels that the backfill has just completed.
_SCAN_INTERVAL = 600.0  # 10 minutes

# Kinds to archive. Mirrors the FUSE mount's conv roots.
_KINDS = ("channels", "dms", "group-dms", "other-channels")


def _local_today() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d")


async def archive_all(store: SlackStore) -> None:
    """Periodic archive pass — pre-renders locked-in markdown to disk."""
    # Let normal startup settle first.
    await trio.sleep(60)

    while True:
        try:
            await _archive_pass(store)
        except Exception:
            log.warning("Archive: pass failed", exc_info=True)
        await trio.sleep(_SCAN_INTERVAL)


async def _archive_pass(store: SlackStore) -> None:
    """One full walk. Idempotent: skips files that already exist."""
    today = _local_today()
    total_written = 0

    # cached_only_mode prevents any API calls — archive never fetches.
    with store.cached_only_mode():
        for kind in _KINDS:
            channels = store.list_channels(kind=kind)
            for cid, entry in channels.items():
                written = await _archive_channel(
                    store,
                    kind,
                    cid,
                    entry.slug,
                    today,
                )
                total_written += written
                # Yield between channels so FUSE ops stay responsive.
                await trio.sleep(0)

    if total_written:
        log.info("Archive: wrote %d new files", total_written)


async def _archive_channel(
    store: SlackStore,
    kind: str,
    channel_id: str,
    slug: str,
    today: str,
) -> int:
    """Render every locked-in day + thread for one channel."""
    written = 0
    dates = store.get_known_dates(channel_id)

    for date in dates:
        if date >= today:
            continue  # Today and any (impossible) future date stay live-only.

        month = date[:7]
        day = date[8:]
        day_dir = _ARCHIVE_DIR / kind / slug / month / day

        written += await _archive_day_files(store, channel_id, date, day_dir)
        written += await _archive_threads_for_day(
            store,
            channel_id,
            date,
            day_dir,
        )

    return written


async def _archive_day_files(
    store: SlackStore,
    channel_id: str,
    date: str,
    day_dir: Path,
) -> int:
    """Write channel.md and feed.md for a single day if missing."""
    written = 0

    ch_file = day_dir / "channel.md"
    if not ch_file.exists():
        data = store.render_day_channel(channel_id, date)
        if data:
            ch_file.parent.mkdir(parents=True, exist_ok=True)
            ch_file.write_bytes(data)
            written += 1
            await trio.sleep(0)

    feed_file = day_dir / "feed.md"
    if not feed_file.exists():
        data = store.render_day_feed(channel_id, date)
        if data:
            feed_file.parent.mkdir(parents=True, exist_ok=True)
            feed_file.write_bytes(data)
            written += 1
            await trio.sleep(0)

    return written


async def _archive_threads_for_day(
    store: SlackStore,
    channel_id: str,
    date: str,
    day_dir: Path,
) -> int:
    """Write thread.md + feed.md for every thread that started on `date`."""
    written = 0
    thread_slugs = store.get_thread_slugs(channel_id, date)

    for thread_slug, thread_ts in thread_slugs.items():
        thread_dir = day_dir / thread_slug

        t_file = thread_dir / "thread.md"
        if not t_file.exists():
            data = store.render_thread_snapshot(channel_id, thread_ts)
            if data:
                t_file.parent.mkdir(parents=True, exist_ok=True)
                t_file.write_bytes(data)
                written += 1
                await trio.sleep(0)

        f_file = thread_dir / "feed.md"
        if not f_file.exists():
            data = store.render_thread_feed(channel_id, thread_ts)
            if data:
                f_file.parent.mkdir(parents=True, exist_ok=True)
                f_file.write_bytes(data)
                written += 1
                await trio.sleep(0)

    return written
