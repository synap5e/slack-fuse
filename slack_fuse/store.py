"""Slack data store — fetches from API, caches, serves rendered files."""

from __future__ import annotations

import contextlib
import html as html_mod
import logging
import random
import time
from collections import OrderedDict
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TypeVar

import httpx

from . import disk_cache, mrkdwn
from .api import FatalAPIError, RateLimitedError, SlackClient
from .canvas import fetch_canvas_markdown
from .models import (
    Channel,
    HuddleIndexEntry,
    HuddleInfo,
    Message,
    SearchFile,
    SlackFile,
    Thread,
)
from .renderer import (
    render_channel_metadata,
    render_day_feed,
    render_day_snapshot,
    render_thread_feed,
    render_thread_snapshot,
)
from .slug import slugify
from .transcript import fetch_transcript_markdown
from .user_cache import UserCache

log = logging.getLogger(__name__)

# Cache TTLs
_CHANNEL_LIST_TTL = 1800.0  # 30 minutes — channel list rarely changes
_RECENT_MSG_TTL = 300.0  # 5 minutes — applies to today's messages (still being written)
_OLD_MSG_TTL = float("inf")  # any earlier local day is locked forever (see _date_ttl)
_HUDDLE_INDEX_TTL = 1800.0  # 30 minutes

# Render cache: rendered markdown bytes keyed by kind + ids. LRU-capped so a
# full grep of the tree doesn't OOM. Old days/threads are effectively
# immutable, so entries stay valid for their full TTL (infinite).
_RENDER_CACHE_CAP = 50000

# Backoff
_BACKOFF_INITIAL = 30.0
_BACKOFF_MAX = 900.0
_BACKOFF_JITTER = 0.25

_T = TypeVar("_T")


@dataclass
class _BackoffState:
    delay: float = 0.0
    until: float = 0.0
    fatal: bool = False

    def record_success(self) -> None:
        self.delay = 0.0
        self.until = 0.0

    def record_failure(self) -> None:
        if self.delay <= 0.0:
            self.delay = _BACKOFF_INITIAL
        else:
            self.delay = min(self.delay * 2, _BACKOFF_MAX)
        jitter = self.delay * _BACKOFF_JITTER * (2 * random.random() - 1)
        self.until = time.monotonic() + self.delay + jitter

    def record_rate_limit(self, retry_after: float | None) -> None:
        if retry_after and retry_after > 0:
            self.delay = retry_after
        else:
            self.delay = min(max(self.delay * 2, _BACKOFF_INITIAL), _BACKOFF_MAX)
        jitter = self.delay * _BACKOFF_JITTER * (2 * random.random() - 1)
        self.until = time.monotonic() + self.delay + jitter

    def record_fatal(self) -> None:
        self.fatal = True
        log.error("Fatal API error — stopping all retries")

    @property
    def is_backed_off(self) -> bool:
        return self.fatal or time.monotonic() < self.until


@dataclass
class _CachedDay:
    """Cached messages for a single channel+date."""

    messages: list[Message]
    fetched_at: float  # monotonic
    date: str


@dataclass
class _CachedThread:
    """Cached thread data."""

    thread: Thread
    fetched_at: float


@dataclass
class _CachedRender:
    """Cached rendered markdown bytes."""

    data: bytes
    fetched_at: float  # monotonic


@dataclass
class ChannelEntry:
    """A channel with its computed slug."""

    channel: Channel
    slug: str


def _build_slug(channel: Channel, users: UserCache, slug_counts: dict[str, int]) -> str:
    """Compute the directory slug for a channel, deduping within `slug_counts`."""
    if channel.is_im and channel.im_user_id:
        display = users.get_display_name(channel.im_user_id)
        base_slug = slugify(display) or channel.id[:12]
    else:
        base_slug = slugify(channel.name) or channel.id[:12]
    count = slug_counts.get(base_slug, 0)
    slug_counts[base_slug] = count + 1
    return base_slug if count == 0 else f"{base_slug}-{count + 1}"


class SlackStore:
    """Caching layer between Slack API and FUSE ops."""

    def __init__(self, client: SlackClient, users: UserCache) -> None:
        self._client = client
        self._users = users
        self._backoff = _BackoffState()
        self._cached_only = False

        # Channel list cache
        self._channels: dict[str, ChannelEntry] = {}  # channel_id -> entry
        self._channel_list_time: float = 0.0

        # Message cache: (channel_id, date_str) -> cached day
        self._day_cache: dict[tuple[str, str], _CachedDay] = {}

        # Thread cache: (channel_id, thread_ts) -> cached thread
        self._thread_cache: dict[tuple[str, str], _CachedThread] = {}

        # Track which dates we've fetched per channel
        self._known_dates: dict[str, set[str]] = {}  # channel_id -> set of date strings

        # Huddle cache: canvas_file_id -> (HuddleInfo, canvas_md, transcript_md)
        self._huddle_cache: dict[str, tuple[HuddleInfo, str | None, str | None]] = {}

        # Top-level huddle index
        self._huddle_index: list[HuddleIndexEntry] | None = None
        self._huddle_index_time: float = 0.0

        # Rendered-markdown cache (LRU, bounded). Avoids re-running
        # mrkdwn.convert + user resolution + yaml frontmatter on every
        # readdir/lookup/open. Keyed by f"<kind>:<channel_id>:<ident>".
        self._render_cache: OrderedDict[str, _CachedRender] = OrderedDict()

        # Load from disk cache
        self._load_disk_cache()

    def _load_disk_cache(self) -> None:
        """Warm in-memory caches from disk."""
        # Channel list
        cached_channels = disk_cache.get_channel_list()
        if cached_channels:
            slug_counts: dict[str, int] = {}
            for ch_data in cached_channels:
                ch = Channel.model_validate(ch_data)
                slug = _build_slug(ch, self._users, slug_counts)
                self._channels[ch.id] = ChannelEntry(channel=ch, slug=slug)
            self._channel_list_time = time.monotonic()
            log.info("Loaded %d channels from disk cache", len(self._channels))

        # Huddle index
        cached_index = disk_cache.get_huddle_index()
        if cached_index:
            self._huddle_index = [HuddleIndexEntry.model_validate(e) for e in cached_index]
            self._huddle_index_time = time.monotonic()
            log.info("Loaded %d huddle index entries from disk cache", len(self._huddle_index))

        # Known dates
        for ch_id in self._channels:
            cached_dates = disk_cache.get_known_dates(ch_id)
            if cached_dates:
                self._known_dates[ch_id] = cached_dates

    @contextlib.contextmanager
    def cached_only_mode(self) -> Iterator[None]:
        """Suppress all API calls — only serve data already in cache."""
        self._cached_only = True
        try:
            yield
        finally:
            self._cached_only = False

    def _api_call(self, fn: Callable[[], _T]) -> _T | None:
        """Run a typed API call, recording backoff state on failure.

        Returns None when in cached_only mode, currently backed off, or on
        any recoverable error. Fatal errors set a permanent backoff flag.
        """
        if self._cached_only or self._backoff.is_backed_off:
            return None
        try:
            result = fn()
        except RateLimitedError as e:
            self._backoff.record_rate_limit(e.retry_after)
            return None
        except FatalAPIError:
            self._backoff.record_fatal()
            return None
        except httpx.TimeoutException:
            log.warning("Timeout on API call")
            self._backoff.record_failure()
            return None
        except httpx.HTTPError as e:
            log.warning("HTTP error on API call: %s", e)
            self._backoff.record_failure()
            return None
        self._backoff.record_success()
        return result

    # === Channel list ===

    def _refresh_channels(self) -> None:
        now = time.monotonic()
        if now - self._channel_list_time < _CHANNEL_LIST_TTL:
            return
        log.info("API: conversations.list (refreshing channels)")
        channels = self._api_call(self._client.list_conversations)
        if channels is None:
            return
        slug_counts: dict[str, int] = {}
        new_entries: dict[str, ChannelEntry] = {}
        for ch in channels:
            slug = _build_slug(ch, self._users, slug_counts)
            new_entries[ch.id] = ChannelEntry(channel=ch, slug=slug)
        self._channels = new_entries
        self._channel_list_time = now
        disk_cache.put_channel_list(
            [e.channel.model_dump(mode="json") for e in new_entries.values()],
        )
        log.info("Loaded %d channels", len(new_entries))

    def list_channels(self, *, kind: str = "channels") -> dict[str, ChannelEntry]:
        """Return channel_id -> ChannelEntry, filtered by kind.

        kind: "channels" (joined, non-DM), "dms", "group-dms", "other-channels"
        """
        self._refresh_channels()
        return {cid: e for cid, e in self._channels.items() if self._matches_kind(e.channel, kind)}

    @staticmethod
    def _matches_kind(ch: Channel, kind: str) -> bool:
        if kind == "dms":
            return ch.is_im
        if kind == "group-dms":
            return ch.is_mpim
        if kind == "other-channels":
            return not ch.is_im and not ch.is_mpim and not ch.is_member
        # "channels" — joined, non-DM
        return ch.is_member and not ch.is_im and not ch.is_mpim

    def get_channel_by_slug(self, slug: str) -> ChannelEntry | None:
        """Find a channel by its directory slug."""
        self._refresh_channels()
        for entry in self._channels.values():
            if entry.slug == slug:
                return entry
        return None

    def _channel_name_for_log(self, channel_id: str) -> str:
        entry = self._channels.get(channel_id)
        return entry.channel.name if entry else channel_id

    # === Messages ===

    def _date_ttl(self, date_str: str) -> float:
        """Return cache TTL for a given date.

        Today (in the system's local timezone) → recent TTL: messages may
        still be posted/edited, so we re-poll every 5 minutes.

        Any earlier local date → infinite TTL: served from disk forever
        on the assumption that yesterday's messages aren't being edited.

        Local time matters: a UTC midnight boundary would land in the
        middle of a PST workday and lock messages while they were still
        being written. The user's local midnight (NZ) maps to ~04:00 PST,
        comfortably after the previous PST workday ends.
        """
        try:
            date = datetime.strptime(date_str, "%Y-%m-%d").date()
            today = datetime.now().astimezone().date()
        except ValueError:
            return _RECENT_MSG_TTL
        return _OLD_MSG_TTL if date < today else _RECENT_MSG_TTL

    def _thread_ttl(self, thread_ts: str) -> float:
        """TTL for a thread, based on its parent message's local date.

        Same today-vs-not-today policy as `_date_ttl`. New replies on a
        thread that started before today are extremely rare; locking such
        threads avoids re-fetching them on every grep.
        """
        try:
            dt = datetime.fromtimestamp(float(thread_ts), tz=UTC).astimezone()
        except (ValueError, OSError):
            return _RECENT_MSG_TTL
        return _OLD_MSG_TTL if dt.date() < datetime.now().astimezone().date() else _RECENT_MSG_TTL

    def get_day_messages(self, channel_id: str, date_str: str) -> list[Message]:
        """Get messages for a channel on a specific date."""
        key = (channel_id, date_str)
        cached = self._day_cache.get(key)
        if cached is not None:
            age = time.monotonic() - cached.fetched_at
            if age < self._date_ttl(date_str):
                return cached.messages

        # Try disk cache (especially valuable for old messages)
        disk_msgs = disk_cache.get_day_messages(channel_id, date_str)
        if disk_msgs is not None and (self._cached_only or self._date_ttl(date_str) == _OLD_MSG_TTL):
            messages = [Message.model_validate(m) for m in disk_msgs]
            self._day_cache[key] = _CachedDay(
                messages=messages,
                fetched_at=time.monotonic(),
                date=date_str,
            )
            return messages

        if self._cached_only:
            return cached.messages if cached else []

        # Compute time window for the date in LOCAL timezone
        try:
            date = datetime.strptime(date_str, "%Y-%m-%d").astimezone()
        except ValueError:
            return []

        oldest = str(date.timestamp())
        latest = str(date.timestamp() + 86400)

        log.info(
            "API: conversations.history %s (%s)",
            date_str,
            self._channel_name_for_log(channel_id),
        )
        messages = self._api_call(
            lambda: self._client.get_history(channel_id, oldest, latest),
        )
        if messages is None:
            # Fall back to disk cache even for recent messages
            if disk_msgs is not None:
                messages = [Message.model_validate(m) for m in disk_msgs]
                self._day_cache[key] = _CachedDay(
                    messages=messages,
                    fetched_at=time.monotonic(),
                    date=date_str,
                )
                return messages
            return cached.messages if cached else []

        self._day_cache[key] = _CachedDay(
            messages=messages,
            fetched_at=time.monotonic(),
            date=date_str,
        )
        self._invalidate_day_renders(channel_id, date_str)
        self._known_dates.setdefault(channel_id, set()).add(date_str)
        disk_cache.put_day_messages(
            channel_id,
            date_str,
            [m.model_dump(mode="json") for m in messages],
        )
        disk_cache.put_known_dates(channel_id, self._known_dates[channel_id])
        return messages

    def get_known_dates(self, channel_id: str) -> list[str]:
        """Return dates with messages for a channel.

        On first access, fetches recent history (1 page) to discover dates.
        Always includes today (unless in cached_only mode).
        """
        if channel_id not in self._known_dates and not self._cached_only:
            self._discover_recent_dates(channel_id)
        dates = self._known_dates.get(channel_id, set()).copy()
        if not self._cached_only:
            today = datetime.now(UTC).astimezone().strftime("%Y-%m-%d")
            dates.add(today)
        return sorted(dates, reverse=True)

    def _discover_recent_dates(self, channel_id: str) -> None:
        """Fetch 1 page of history to discover which dates have messages."""
        log.info(
            "API: conversations.history (discovering dates for %s)",
            self._channel_name_for_log(channel_id),
        )
        messages = self._api_call(
            lambda: self._client.get_history(channel_id, None, None, 200),
        )
        if messages is None:
            return
        dates: set[str] = set()
        by_date: dict[str, list[Message]] = {}
        for msg in messages:
            try:
                dt = datetime.fromtimestamp(float(msg.ts), tz=UTC).astimezone()
                d = dt.strftime("%Y-%m-%d")
                dates.add(d)
                by_date.setdefault(d, []).append(msg)
            except (ValueError, OSError):
                pass
        self._known_dates.setdefault(channel_id, set()).update(dates)
        disk_cache.put_known_dates(channel_id, self._known_dates[channel_id])
        for date_str, day_msgs in by_date.items():
            key = (channel_id, date_str)
            if key not in self._day_cache:
                self._day_cache[key] = _CachedDay(
                    messages=day_msgs,
                    fetched_at=time.monotonic(),
                    date=date_str,
                )

    # === Threads ===

    def get_thread(self, channel_id: str, thread_ts: str) -> Thread | None:
        """Get a thread, cached.

        Today's threads use a 5-minute in-memory TTL (new replies still
        possible). Threads whose parent message is from a previous local
        day get an infinite TTL: once they're in the in-memory or disk
        cache, they're served forever and never re-fetched.
        """
        key = (channel_id, thread_ts)
        ttl = self._thread_ttl(thread_ts)

        cached = self._thread_cache.get(key)
        if cached is not None:
            age = time.monotonic() - cached.fetched_at
            if age < ttl:
                return cached.thread

        # Disk cache fallback (cached_only or empty in-memory).
        disk_msgs = disk_cache.get_thread(channel_id, thread_ts)
        if disk_msgs is not None and (self._cached_only or cached is None):
            messages = [Message.model_validate(m) for m in disk_msgs]
            if messages:
                thread = Thread(parent=messages[0], replies=tuple(messages[1:]))
                self._thread_cache[key] = _CachedThread(
                    thread=thread,
                    fetched_at=time.monotonic(),
                )
                return thread

        if self._cached_only:
            return cached.thread if cached else None

        log.info(
            "API: conversations.replies %s in %s",
            thread_ts,
            self._channel_name_for_log(channel_id),
        )
        thread = self._api_call(
            lambda: self._client.get_replies(channel_id, thread_ts),
        )
        if thread is None:
            return cached.thread if cached else None

        self._thread_cache[key] = _CachedThread(thread=thread, fetched_at=time.monotonic())
        self._invalidate_thread_renders(channel_id, thread_ts)
        # Persist to disk
        all_msgs = [thread.parent, *thread.replies]
        disk_cache.put_thread(
            channel_id,
            thread_ts,
            [m.model_dump(mode="json") for m in all_msgs],
        )
        return thread

    # === Rendered content ===

    def _render_cached(
        self,
        key: str,
        ttl: float,
        render_fn: Callable[[], bytes],
    ) -> bytes:
        """Return cached render bytes, or render + cache under LRU cap."""
        now = time.monotonic()
        cached = self._render_cache.get(key)
        if cached is not None and now - cached.fetched_at < ttl:
            self._render_cache.move_to_end(key)
            return cached.data
        data = render_fn()
        self._render_cache[key] = _CachedRender(data=data, fetched_at=now)
        self._render_cache.move_to_end(key)
        while len(self._render_cache) > _RENDER_CACHE_CAP:
            self._render_cache.popitem(last=False)
        return data

    def _invalidate_day_renders(self, channel_id: str, date_str: str) -> None:
        self._render_cache.pop(f"day-ch:{channel_id}:{date_str}", None)
        self._render_cache.pop(f"day-fd:{channel_id}:{date_str}", None)

    def _invalidate_thread_renders(self, channel_id: str, thread_ts: str) -> None:
        self._render_cache.pop(f"thr-ch:{channel_id}:{thread_ts}", None)
        self._render_cache.pop(f"thr-fd:{channel_id}:{thread_ts}", None)

    def render_channel_info(self, channel_id: str) -> bytes:
        """Render channel metadata markdown."""

        def _do() -> bytes:
            entry = self._channels.get(channel_id)
            if entry is None:
                return b""
            return render_channel_metadata(entry.channel, self._users).encode()

        return self._render_cached(f"info:{channel_id}:", _CHANNEL_LIST_TTL, _do)

    def render_day_channel(self, channel_id: str, date_str: str) -> bytes:
        """Render channel.md snapshot for a date."""

        def _do() -> bytes:
            entry = self._channels.get(channel_id)
            if entry is None:
                return b""
            messages = self.get_day_messages(channel_id, date_str)
            return render_day_snapshot(
                entry.channel,
                date_str,
                messages,
                self._users,
            ).encode()

        return self._render_cached(
            f"day-ch:{channel_id}:{date_str}",
            self._date_ttl(date_str),
            _do,
        )

    def render_day_feed(self, channel_id: str, date_str: str) -> bytes:
        """Render feed.md for a date."""

        def _do() -> bytes:
            entry = self._channels.get(channel_id)
            if entry is None:
                return b""
            messages = self.get_day_messages(channel_id, date_str)
            return render_day_feed(
                entry.channel,
                date_str,
                messages,
                self._users,
            ).encode()

        return self._render_cached(
            f"day-fd:{channel_id}:{date_str}",
            self._date_ttl(date_str),
            _do,
        )

    def render_thread_snapshot(self, channel_id: str, thread_ts: str) -> bytes:
        """Render thread.md snapshot."""

        def _do() -> bytes:
            entry = self._channels.get(channel_id)
            thread = self.get_thread(channel_id, thread_ts)
            if entry is None or thread is None:
                return b""
            return render_thread_snapshot(thread, entry.channel, self._users).encode()

        return self._render_cached(
            f"thr-ch:{channel_id}:{thread_ts}",
            self._thread_ttl(thread_ts),
            _do,
        )

    def render_thread_feed(self, channel_id: str, thread_ts: str) -> bytes:
        """Render thread feed.md."""

        def _do() -> bytes:
            entry = self._channels.get(channel_id)
            thread = self.get_thread(channel_id, thread_ts)
            if entry is None or thread is None:
                return b""
            return render_thread_feed(thread, entry.channel, self._users).encode()

        return self._render_cached(
            f"thr-fd:{channel_id}:{thread_ts}",
            self._thread_ttl(thread_ts),
            _do,
        )

    def get_thread_slugs(self, channel_id: str, date_str: str) -> dict[str, str]:
        """Return slug -> thread_ts for threads starting on this date."""
        messages = self.get_day_messages(channel_id, date_str)
        threads: dict[str, str] = {}
        for msg in messages:
            if msg.reply_count > 0 and msg.thread_ts == msg.ts:
                text = mrkdwn.convert(msg.text[:80], self._users) if msg.text else msg.ts
                slug = slugify(text) or msg.ts.replace(".", "-")
                # Dedup slugs
                base = slug
                counter = 2
                while slug in threads:
                    slug = f"{base}-{counter}"
                    counter += 1
                threads[slug] = msg.ts
        return threads

    # === Huddles ===

    def get_huddles_for_thread(
        self,
        channel_id: str,
        thread_ts: str,
    ) -> dict[str, tuple[HuddleInfo, str | None, str | None]]:
        """Return slug -> (HuddleInfo, canvas_markdown, transcript_markdown) for huddles in a thread."""
        thread = self.get_thread(channel_id, thread_ts)
        if thread is None:
            return {}
        all_msgs = [thread.parent, *thread.replies]
        return self._find_huddles_in_messages(all_msgs)

    def get_huddles_for_day(
        self,
        channel_id: str,
        date_str: str,
    ) -> dict[str, tuple[HuddleInfo, str | None, str | None]]:
        """Return slug -> huddle bundle for channel-level huddles on a day."""
        messages = self.get_day_messages(channel_id, date_str)
        # Only include huddles from non-threaded messages
        top_level = [m for m in messages if m.thread_ts is None or m.thread_ts == m.ts]
        return self._find_huddles_in_messages(top_level)

    def _find_huddles_in_messages(
        self,
        messages: list[Message],
    ) -> dict[str, tuple[HuddleInfo, str | None, str | None]]:
        """Find huddle canvas attachments in messages and fetch their content."""
        huddles: dict[str, tuple[HuddleInfo, str | None, str | None]] = {}
        for msg in messages:
            for f in msg.files:
                if not f.is_huddle_canvas:
                    continue
                bundle = self._huddle_cache.get(f.id)
                if bundle is None:
                    disk_huddle = disk_cache.get_huddle(f.id)
                    if disk_huddle is not None:
                        md, transcript = disk_huddle
                    elif self._cached_only:
                        continue  # Skip uncached huddles
                    else:
                        md = fetch_canvas_markdown(self._client.token, f.id, self._users)
                        transcript = None
                        if f.huddle_transcript_file_id:
                            transcript = fetch_transcript_markdown(
                                self._client.token,
                                f.huddle_transcript_file_id,
                                self._users,
                            )
                        disk_cache.put_huddle(f.id, md, transcript)
                    info = HuddleInfo(
                        canvas_file_id=f.id,
                        transcript_file_id=f.huddle_transcript_file_id,
                        date_start=0,
                        date_end=0,
                    )
                    bundle = (info, md, transcript)
                    self._huddle_cache[f.id] = bundle

                ts_time = _ts_to_time(msg.ts)
                slug = f"huddle-{ts_time}".replace(":", "")
                huddles[slug] = bundle
        return huddles

    # === Huddle index (top-level /huddles/) ===

    def get_huddle_index(self) -> list[HuddleIndexEntry]:
        """Return all huddle canvases as HuddleIndexEntry rows."""
        now = time.monotonic()
        if self._huddle_index is not None and now - self._huddle_index_time < _HUDDLE_INDEX_TTL:
            return self._huddle_index

        log.info("Searching for huddle canvases")
        matches = self._api_call(self._client.search_huddle_canvases)
        if matches is None:
            return self._huddle_index or []

        entries: list[HuddleIndexEntry] = []
        for match in matches:
            entry = self._huddle_index_entry_from_match(match)
            if entry is not None:
                entries.append(entry)

        # Dedup slugs within the same day
        seen: dict[tuple[str, str, str], int] = {}
        for e in entries:
            key = (e.month, e.day, e.slug)
            count = seen.get(key, 0)
            seen[key] = count + 1
            if count > 0:
                e.slug = f"{e.slug}-{count + 1}"

        self._huddle_index = entries
        self._huddle_index_time = now
        disk_cache.put_huddle_index([e.model_dump(mode="json") for e in entries])
        log.info("Found %d huddle canvases", len(entries))
        return entries

    def _huddle_index_entry_from_match(self, match: SearchFile) -> HuddleIndexEntry | None:
        # Date from timestamp
        try:
            dt = datetime.fromtimestamp(match.timestamp, tz=UTC).astimezone()
        except (ValueError, OSError):
            return None
        month = dt.strftime("%Y-%m")
        day = dt.strftime("%d")

        # Build slug from title — decode HTML entities and resolve mentions
        decoded_title = html_mod.unescape(match.title)
        decoded_title = mrkdwn.convert(decoded_title, self._users)
        clean_title = slugify(decoded_title) or match.id[:12]

        # Resolve channel + thread context via files.info
        channel_id, thread_ts = self._resolve_huddle_context(match)

        channel_slug = ""
        conv_root = "channels"
        if channel_id:
            ch_entry = self._channels.get(channel_id)
            if ch_entry:
                channel_slug = ch_entry.slug
                if ch_entry.channel.is_im:
                    conv_root = "dms"
                elif ch_entry.channel.is_mpim:
                    conv_root = "group-dms"

        return HuddleIndexEntry(
            month=month,
            day=day,
            slug=clean_title,
            channel_id=channel_id,
            channel_slug=channel_slug,
            thread_ts=thread_ts,
            canvas_file_id=match.id,
            conv_root=conv_root,
        )

    def _resolve_huddle_context(self, match: SearchFile) -> tuple[str, str]:
        """Look up the (channel_id, thread_ts) for a huddle canvas via files.info."""
        file_info = self._api_call(lambda: self._client.get_file_info(match.id))
        if file_info is None:
            channel_id = match.channels[0] if match.channels else ""
            return (channel_id, "")
        return _shares_to_context(file_info, match.channels)

    def get_huddle_by_canvas_id(
        self,
        canvas_file_id: str,
    ) -> tuple[HuddleInfo, str | None, str | None] | None:
        """Get or fetch huddle content by canvas file ID. Returns (info, notes_md, transcript_md)."""
        cached = self._huddle_cache.get(canvas_file_id)
        if cached is not None:
            return cached

        # Try disk cache
        disk_huddle = disk_cache.get_huddle(canvas_file_id)
        if disk_huddle is not None:
            md, transcript = disk_huddle
            info = HuddleInfo(
                canvas_file_id=canvas_file_id,
                transcript_file_id=None,
                date_start=0,
                date_end=0,
            )
            bundle = (info, md, transcript)
            self._huddle_cache[canvas_file_id] = bundle
            return bundle

        # Fetch file info to get transcript file ID
        file_info = self._api_call(lambda: self._client.get_file_info(canvas_file_id))
        if file_info is None:
            return None

        info = HuddleInfo(
            canvas_file_id=canvas_file_id,
            transcript_file_id=file_info.huddle_transcript_file_id,
            date_start=file_info.huddle_date_start,
            date_end=file_info.huddle_date_end,
        )
        md = fetch_canvas_markdown(self._client.token, canvas_file_id, self._users)
        transcript: str | None = None
        if file_info.huddle_transcript_file_id:
            transcript = fetch_transcript_markdown(
                self._client.token,
                file_info.huddle_transcript_file_id,
                self._users,
            )
        bundle = (info, md, transcript)
        self._huddle_cache[canvas_file_id] = bundle
        disk_cache.put_huddle(canvas_file_id, md, transcript)
        return bundle

    # === Global ===

    @property
    def is_auth_fatal(self) -> bool:
        return self._backoff.fatal

    def find_huddle_index_entry_by_canvas(self, canvas_file_id: str) -> HuddleIndexEntry | None:
        """Look up a huddle index entry by canvas file ID."""
        for e in self.get_huddle_index():
            if e.canvas_file_id == canvas_file_id:
                return e
        return None

    def resolve_huddle_symlink(self, entry: HuddleIndexEntry) -> str | None:
        """Resolve a huddle index entry to its canonical FUSE path."""
        if not entry.channel_id or not entry.channel_slug:
            return None

        date_str = f"{entry.month}-{entry.day}"

        if not entry.thread_ts:
            return None

        thread_slugs = self.get_thread_slugs(entry.channel_id, date_str)
        thread_slug: str | None = None
        for slug, ts in thread_slugs.items():
            if ts == entry.thread_ts:
                thread_slug = slug
                break
        if thread_slug is None:
            return None

        huddles = self.get_huddles_for_thread(entry.channel_id, entry.thread_ts)
        huddle_slug: str | None = None
        for h_slug, (info, _, _) in huddles.items():
            if info.canvas_file_id == entry.canvas_file_id:
                huddle_slug = h_slug
                break
        if huddle_slug is None:
            return None

        return f"{entry.conv_root}/{entry.channel_slug}/{entry.month}/{entry.day}/{thread_slug}/huddles/{huddle_slug}"

    def merge_known_dates(self, channel_id: str, dates: set[str]) -> None:
        """Merge discovered dates into known dates (used by backfill)."""
        existing = self._known_dates.get(channel_id, set())
        merged = existing | dates
        self._known_dates[channel_id] = merged
        disk_cache.put_known_dates(channel_id, merged)

    def force_refresh(self) -> None:
        """Clear all caches and backoff state."""
        self._channel_list_time = 0.0
        self._day_cache.clear()
        self._thread_cache.clear()
        self._known_dates.clear()
        self._huddle_cache.clear()
        self._huddle_index = None
        self._huddle_index_time = 0.0
        self._render_cache.clear()
        self._backoff = _BackoffState()
        log.info("Force refresh: all caches cleared")


# === Helpers ===


def _shares_to_context(file_info: SlackFile, fallback_channels: tuple[str, ...]) -> tuple[str, str]:
    """Pick a (channel_id, thread_ts) from a file's shares.

    Prefer shares with a thread_ts; fall back to first share; finally fall back
    to the first channel listed in the search match.
    """
    all_shares: list[tuple[str, str]] = []
    for channel_map in (file_info.shares.public, file_info.shares.private):
        for ch_id, share_list in channel_map.items():
            for s in share_list:
                all_shares.append((ch_id, s.thread_ts or ""))

    threaded = [(c, t) for c, t in all_shares if t]
    if threaded:
        return threaded[0]
    if all_shares:
        return all_shares[0]
    if fallback_channels:
        return (fallback_channels[0], "")
    return ("", "")


def _ts_to_time(ts: str) -> str:
    try:
        dt = datetime.fromtimestamp(float(ts), tz=UTC).astimezone()
        return dt.strftime("%H%M")
    except (ValueError, OSError):
        return ts
