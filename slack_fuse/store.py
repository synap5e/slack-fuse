"""Slack data store — fetches from API, caches, serves rendered files."""

from __future__ import annotations

import contextlib
import json
import logging
import random
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import httpx

from .api import FatalAPIError, RateLimitedError, SlackClient
from . import disk_cache, mrkdwn
from .canvas import fetch_canvas_markdown
from .transcript import fetch_transcript_markdown
from .models import Channel, HuddleInfo, Message, Thread, channel_from_dict, channel_to_dict, message_from_dict, message_to_dict
from .renderer import (
    render_channel_metadata,
    render_day_feed,
    render_day_snapshot,
    render_thread_feed,
    render_thread_snapshot,
)
from .slug import slugify
from .user_cache import UserCache

log = logging.getLogger(__name__)

# Cache TTLs
_CHANNEL_LIST_TTL = 1800.0  # 30 minutes — channel list rarely changes
_RECENT_MSG_TTL = 300.0  # 5 minutes for messages < 7 days old
_OLD_MSG_TTL = float("inf")  # messages > 7 days cached indefinitely
_OLD_THRESHOLD_DAYS = 7

# Backoff
_BACKOFF_INITIAL = 30.0
_BACKOFF_MAX = 900.0
_BACKOFF_JITTER = 0.25


@dataclass
class _BackoffState:
    delay: float = 0.0
    until: float = 0.0
    fatal: bool = False

    def record_success(self) -> None:
        self.delay = 0.0
        self.until = 0.0

    def record_failure(self) -> None:
        if self.delay == 0.0:
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
class ChannelEntry:
    """A channel with its computed slug."""
    channel: Channel
    slug: str


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

        # Load from disk cache
        self._load_disk_cache()

    def _load_disk_cache(self) -> None:
        """Warm in-memory caches from disk."""
        # Channel list
        cached_channels = disk_cache.get_channel_list()
        if cached_channels:
            slug_counts: dict[str, int] = {}
            for ch_data in cached_channels:
                ch = channel_from_dict(ch_data)
                if ch.is_im and ch.im_user_id:
                    display = self._users.get_display_name(ch.im_user_id)
                    base_slug = slugify(display) or ch.id[:12]
                else:
                    base_slug = slugify(ch.name) or ch.id[:12]
                count = slug_counts.get(base_slug, 0)
                slug_counts[base_slug] = count + 1
                slug = base_slug if count == 0 else f"{base_slug}-{count + 1}"
                self._channels[ch.id] = ChannelEntry(channel=ch, slug=slug)
            self._channel_list_time = time.monotonic()
            log.info("Loaded %d channels from disk cache", len(self._channels))

        # Huddle index
        cached_index = disk_cache.get_huddle_index()
        if cached_index:
            self._huddle_index = cached_index
            self._huddle_index_time = time.monotonic()
            log.info("Loaded %d huddle index entries from disk cache", len(cached_index))

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

    def _api_call(self, fn: str, *args: object, **kwargs: object) -> object:
        """Wrapper that catches API errors and records backoff."""
        if self._cached_only or self._backoff.is_backed_off:
            return None
        try:
            method = getattr(self._client, fn)
            result = method(*args, **kwargs)
            self._backoff.record_success()
            return result
        except RateLimitedError as e:
            self._backoff.record_rate_limit(e.retry_after)
        except FatalAPIError:
            self._backoff.record_fatal()
        except httpx.TimeoutException:
            log.warning("Timeout on %s", fn)
            self._backoff.record_failure()
        except httpx.HTTPError as e:
            log.warning("HTTP error on %s: %s", fn, e)
            self._backoff.record_failure()
        return None

    # === Channel list ===

    def _refresh_channels(self) -> None:
        now = time.monotonic()
        if now - self._channel_list_time < _CHANNEL_LIST_TTL:
            return
        log.info("API: conversations.list (refreshing channels)")
        result = self._api_call("list_conversations")
        if result is None:
            return
        channels: list[Channel] = result  # type: ignore[assignment]
        slug_counts: dict[str, int] = {}
        new_entries: dict[str, ChannelEntry] = {}
        for ch in channels:
            if ch.is_im and ch.im_user_id:
                # DMs: use the other user's display name
                display = self._users.get_display_name(ch.im_user_id)
                base_slug = slugify(display) or ch.id[:12]
            else:
                base_slug = slugify(ch.name) or ch.id[:12]
            count = slug_counts.get(base_slug, 0)
            slug_counts[base_slug] = count + 1
            slug = base_slug if count == 0 else f"{base_slug}-{count + 1}"
            new_entries[ch.id] = ChannelEntry(channel=ch, slug=slug)
        self._channels = new_entries
        self._channel_list_time = now
        disk_cache.put_channel_list([channel_to_dict(e.channel) for e in new_entries.values()])
        log.info("Loaded %d channels", len(new_entries))

    def list_channels(
        self, *, kind: str = "channels",
    ) -> dict[str, ChannelEntry]:
        """Return channel_id -> ChannelEntry, filtered by kind.

        kind: "channels" (joined, non-DM), "dms", "group-dms", "other-channels"
        """
        self._refresh_channels()
        return {
            cid: e for cid, e in self._channels.items()
            if self._matches_kind(e.channel, kind)
        }

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

    # === Messages ===

    def _date_ttl(self, date_str: str) -> float:
        """Return cache TTL for a given date."""
        try:
            date = datetime.strptime(date_str, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
            age_days = (datetime.now(timezone.utc) - date).days
            if age_days > _OLD_THRESHOLD_DAYS:
                return _OLD_MSG_TTL
        except ValueError:
            pass
        return _RECENT_MSG_TTL

    def get_day_messages(
        self, channel_id: str, date_str: str,
    ) -> list[Message]:
        """Get messages for a channel on a specific date."""
        key = (channel_id, date_str)
        cached = self._day_cache.get(key)
        if cached is not None:
            age = time.monotonic() - cached.fetched_at
            if age < self._date_ttl(date_str):
                return cached.messages

        # Try disk cache (especially valuable for old messages)
        disk_msgs = disk_cache.get_day_messages(channel_id, date_str)
        if disk_msgs is not None and (
            self._cached_only or self._date_ttl(date_str) == _OLD_MSG_TTL
        ):
            messages = [message_from_dict(m) for m in disk_msgs]
            self._day_cache[key] = _CachedDay(
                messages=messages, fetched_at=time.monotonic(), date=date_str,
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

        ch_name = self._channels.get(channel_id, ChannelEntry(channel=Channel(id=channel_id, name=channel_id), slug=channel_id)).channel.name
        log.info("API: conversations.history %s (%s)", date_str, ch_name)
        result = self._api_call("get_history", channel_id, oldest, latest)
        if result is None:
            # Fall back to disk cache even for recent messages
            if disk_msgs is not None:
                messages = [message_from_dict(m) for m in disk_msgs]
                self._day_cache[key] = _CachedDay(
                    messages=messages, fetched_at=time.monotonic(), date=date_str,
                )
                return messages
            return cached.messages if cached else []
        messages: list[Message] = result  # type: ignore[assignment]

        self._day_cache[key] = _CachedDay(
            messages=messages,
            fetched_at=time.monotonic(),
            date=date_str,
        )
        self._known_dates.setdefault(channel_id, set()).add(date_str)
        disk_cache.put_day_messages(channel_id, date_str, [message_to_dict(m) for m in messages])
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
            today = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d")
            dates.add(today)
        return sorted(dates, reverse=True)

    def _discover_recent_dates(self, channel_id: str) -> None:
        """Fetch 1 page of history to discover which dates have messages."""
        ch_name = self._channels.get(channel_id, ChannelEntry(channel=Channel(id=channel_id, name=channel_id), slug=channel_id)).channel.name
        log.info("API: conversations.history (discovering dates for %s)", ch_name)
        result = self._api_call("get_history", channel_id, None, None, 200)
        if result is None:
            return
        messages: list[Message] = result  # type: ignore[assignment]
        dates: set[str] = set()
        for msg in messages:
            try:
                dt = datetime.fromtimestamp(float(msg.ts), tz=timezone.utc).astimezone()
                dates.add(dt.strftime("%Y-%m-%d"))
            except (ValueError, OSError):
                pass
        self._known_dates.setdefault(channel_id, set()).update(dates)
        disk_cache.put_known_dates(channel_id, self._known_dates[channel_id])
        # Also cache these messages by date
        by_date: dict[str, list[Message]] = {}
        for msg in messages:
            try:
                dt = datetime.fromtimestamp(float(msg.ts), tz=timezone.utc).astimezone()
                d = dt.strftime("%Y-%m-%d")
                by_date.setdefault(d, []).append(msg)
            except (ValueError, OSError):
                pass
        for date_str, day_msgs in by_date.items():
            key = (channel_id, date_str)
            if key not in self._day_cache:
                self._day_cache[key] = _CachedDay(
                    messages=day_msgs,
                    fetched_at=time.monotonic(),
                    date=date_str,
                )

    # === Threads ===

    def get_thread(
        self, channel_id: str, thread_ts: str,
    ) -> Thread | None:
        """Get a thread, cached."""
        key = (channel_id, thread_ts)
        cached = self._thread_cache.get(key)
        if cached is not None:
            age = time.monotonic() - cached.fetched_at
            if age < _RECENT_MSG_TTL:
                return cached.thread

        # Try disk cache
        disk_msgs = disk_cache.get_thread(channel_id, thread_ts)
        if disk_msgs is not None and (self._cached_only or cached is None):
            messages = [message_from_dict(m) for m in disk_msgs]
            thread = Thread(parent=messages[0], replies=tuple(messages[1:])) if messages else None
            if thread:
                self._thread_cache[key] = _CachedThread(
                    thread=thread, fetched_at=time.monotonic(),
                )
                return thread

        if self._cached_only:
            return cached.thread if cached else None

        ch_name = self._channels.get(channel_id, ChannelEntry(channel=Channel(id=channel_id, name=channel_id), slug=channel_id)).channel.name
        log.info("API: conversations.replies %s in %s", thread_ts, ch_name)
        result = self._api_call("get_replies", channel_id, thread_ts)
        if result is None:
            return cached.thread if cached else None
        thread: Thread = result  # type: ignore[assignment]

        self._thread_cache[key] = _CachedThread(
            thread=thread, fetched_at=time.monotonic(),
        )
        # Persist to disk
        all_msgs = [thread.parent, *thread.replies]
        disk_cache.put_thread(channel_id, thread_ts, [message_to_dict(m) for m in all_msgs])
        return thread

    # === Rendered content ===

    def render_channel_info(self, channel_id: str) -> bytes:
        """Render channel metadata markdown."""
        entry = self._channels.get(channel_id)
        if entry is None:
            return b""
        return render_channel_metadata(entry.channel, self._users).encode()

    def render_day_channel(
        self, channel_id: str, date_str: str,
    ) -> bytes:
        """Render channel.md snapshot for a date."""
        entry = self._channels.get(channel_id)
        if entry is None:
            return b""
        messages = self.get_day_messages(channel_id, date_str)
        return render_day_snapshot(
            entry.channel, date_str, messages, self._users,
        ).encode()

    def render_day_feed(
        self, channel_id: str, date_str: str,
    ) -> bytes:
        """Render feed.md for a date."""
        entry = self._channels.get(channel_id)
        if entry is None:
            return b""
        messages = self.get_day_messages(channel_id, date_str)
        return render_day_feed(
            entry.channel, date_str, messages, self._users,
        ).encode()

    def render_thread_snapshot(
        self, channel_id: str, thread_ts: str,
    ) -> bytes:
        """Render thread.md snapshot."""
        entry = self._channels.get(channel_id)
        thread = self.get_thread(channel_id, thread_ts)
        if entry is None or thread is None:
            return b""
        return render_thread_snapshot(thread, entry.channel, self._users).encode()

    def render_thread_feed(
        self, channel_id: str, thread_ts: str,
    ) -> bytes:
        """Render thread feed.md."""
        entry = self._channels.get(channel_id)
        thread = self.get_thread(channel_id, thread_ts)
        if entry is None or thread is None:
            return b""
        return render_thread_feed(thread, entry.channel, self._users).encode()

    def get_thread_slugs(
        self, channel_id: str, date_str: str,
    ) -> dict[str, str]:
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
        self, channel_id: str, thread_ts: str,
    ) -> dict[str, tuple[HuddleInfo, str | None, str | None]]:
        """Return slug -> (HuddleInfo, canvas_markdown) for huddles in a thread."""
        thread = self.get_thread(channel_id, thread_ts)
        if thread is None:
            return {}
        all_msgs = [thread.parent, *thread.replies]
        return self._find_huddles_in_messages(all_msgs)

    def get_huddles_for_day(
        self, channel_id: str, date_str: str,
    ) -> dict[str, tuple[HuddleInfo, str | None, str | None]]:
        """Return slug -> (HuddleInfo, canvas_markdown) for channel-level huddles on a day."""
        messages = self.get_day_messages(channel_id, date_str)
        # Only include huddles from non-threaded messages
        top_level = [m for m in messages if m.thread_ts is None or m.thread_ts == m.ts]
        return self._find_huddles_in_messages(top_level)

    def _find_huddles_in_messages(
        self, messages: list[Message],
    ) -> dict[str, tuple[HuddleInfo, str | None, str | None]]:
        """Find huddle canvas attachments in messages and fetch their content."""
        huddles: dict[str, tuple[HuddleInfo, str | None, str | None]] = {}
        for msg in messages:
            for f in msg.files:
                if not f.is_huddle_canvas:
                    continue
                if f.id in self._huddle_cache:
                    info, md, transcript = self._huddle_cache[f.id]
                else:
                    # Try disk cache first
                    disk_huddle = disk_cache.get_huddle(f.id)
                    if disk_huddle is not None:
                        md, transcript = disk_huddle
                    elif self._cached_only:
                        continue  # Skip uncached huddles
                    else:
                        md = fetch_canvas_markdown(
                            self._client._token, f.id, self._users,
                        )
                        transcript = None
                        if f.huddle_transcript_file_id:
                            transcript = fetch_transcript_markdown(
                                self._client._token,
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
                    self._huddle_cache[f.id] = (info, md, transcript)

                ts_time = _ts_to_time(msg.ts)
                slug = f"huddle-{ts_time}".replace(":", "")
                huddles[slug] = (info, md, transcript)
        return huddles

    # === Huddle index (top-level /huddles/) ===

    _huddle_index: list[dict[str, str]] | None = None
    _huddle_index_time: float = 0.0
    _HUDDLE_INDEX_TTL = 1800.0  # 30 minutes

    def get_huddle_index(self) -> list[dict[str, str]]:
        """Return all huddle canvases as dicts with date, slug, channel_slug, etc.

        Each entry has: month, day, slug, channel_id, channel_slug,
        thread_ts, canvas_file_id, transcript_file_id
        """
        now = time.monotonic()
        if self._huddle_index is not None and now - self._huddle_index_time < self._HUDDLE_INDEX_TTL:
            return self._huddle_index

        log.info("Searching for huddle canvases")
        result = self._api_call("search_huddle_canvases")
        if result is None:
            return self._huddle_index or []
        matches: list[dict[str, object]] = result  # type: ignore[assignment]

        entries: list[dict[str, str]] = []
        for match in matches:
            file_id = str(match.get("id", ""))
            title = str(match.get("title", ""))
            ts = match.get("timestamp", 0)
            channels = match.get("channels", [])

            # Get date from timestamp
            try:
                dt = datetime.fromtimestamp(int(ts), tz=timezone.utc).astimezone()  # type: ignore[arg-type]
                month = dt.strftime("%Y-%m")
                day = dt.strftime("%d")
            except (ValueError, OSError, TypeError):
                continue

            # Build slug from title — decode HTML entities first
            import html as html_mod
            decoded_title = html_mod.unescape(title)
            # Resolve channel refs and user mentions for readable slugs
            decoded_title = mrkdwn.convert(decoded_title, self._users)
            clean_title = slugify(decoded_title) or file_id[:12]

            # Get channel and thread info from file shares
            channel_id = ""
            thread_ts = ""
            file_info_data = self._api_call("get_file_info", file_id)
            if file_info_data:
                fi: dict[str, object] = file_info_data  # type: ignore[assignment]
                fi_shares = fi.get("shares", {})
                if isinstance(fi_shares, dict):
                    # Collect all shares, prefer ones with thread_ts
                    all_shares: list[tuple[str, str]] = []  # (channel_id, thread_ts)
                    for stype in ("public", "private"):
                        for ch_id, share_list in fi_shares.get(stype, {}).items():  # type: ignore[union-attr]
                            if isinstance(share_list, list):
                                for s in share_list:
                                    t_ts = s.get("thread_ts") or ""
                                    all_shares.append((ch_id, str(t_ts)))
                    # Pick the first share with a thread_ts, or fall back to first share
                    threaded = [(c, t) for c, t in all_shares if t]
                    if threaded:
                        channel_id, thread_ts = threaded[0]
                    elif all_shares:
                        channel_id, thread_ts = all_shares[0]
                    if not channel_id and channels:
                        channel_id = str(channels[0])  # type: ignore[index]

            channel_slug = ""
            if channel_id:
                ch_entry = self._channels.get(channel_id)
                if ch_entry:
                    channel_slug = ch_entry.slug

            # Determine conversation root (channels/ vs dms/ etc)
            conv_root = "channels"
            if channel_id:
                ch = self._channels.get(channel_id)
                if ch and ch.channel.is_im:
                    conv_root = "dms"
                elif ch and ch.channel.is_mpim:
                    conv_root = "group-dms"

            entries.append({
                "month": month,
                "day": day,
                "slug": clean_title,  # deduped below
                "channel_id": channel_id,
                "channel_slug": channel_slug,
                "thread_ts": thread_ts,
                "canvas_file_id": file_id,
                "conv_root": conv_root,
            })

        # Dedup slugs within the same day
        seen: dict[tuple[str, str, str], int] = {}
        for e in entries:
            key = (e["month"], e["day"], e["slug"])
            count = seen.get(key, 0)
            seen[key] = count + 1
            if count > 0:
                e["slug"] = f"{e['slug']}-{count + 1}"

        self._huddle_index = entries
        self._huddle_index_time = now
        disk_cache.put_huddle_index(entries)
        log.info("Found %d huddle canvases", len(entries))
        return entries

    def resolve_huddle_symlink(self, entry: dict[str, str]) -> str | None:
        """Resolve a huddle index entry to its canonical FUSE path.

        Returns a path like: channels/proj-cloud/2026-03/26/hunter-how-do.../huddles/huddle-0937
        """
        channel_id = entry.get("channel_id", "")
        thread_ts = entry.get("thread_ts", "")
        canvas_file_id = entry.get("canvas_file_id", "")
        conv_root = entry.get("conv_root", "channels")
        channel_slug = entry.get("channel_slug", "")

        if not channel_id or not channel_slug:
            return None

        month = entry["month"]
        day = entry["day"]
        date_str = f"{month}-{day}"

        if thread_ts:
            # Need to find the thread slug — load messages for that day
            thread_slugs = self.get_thread_slugs(channel_id, date_str)
            thread_slug = None
            for slug, ts in thread_slugs.items():
                if ts == thread_ts:
                    thread_slug = slug
                    break
            if thread_slug is None:
                return None

            # Find the huddle slug within the thread
            huddles = self.get_huddles_for_thread(channel_id, thread_ts)
            huddle_slug = None
            for h_slug, (info, _, _) in huddles.items():
                if info.canvas_file_id == canvas_file_id:
                    huddle_slug = h_slug
                    break
            if huddle_slug is None:
                return None

            return f"{conv_root}/{channel_slug}/{month}/{day}/{thread_slug}/huddles/{huddle_slug}"

        return None

    def get_huddle_by_canvas_id(
        self, canvas_file_id: str,
    ) -> tuple[HuddleInfo, str | None, str | None] | None:
        """Get or fetch huddle content by canvas file ID. Returns (info, notes_md, transcript_md)."""
        if canvas_file_id in self._huddle_cache:
            return self._huddle_cache[canvas_file_id]

        # Try disk cache
        disk_huddle = disk_cache.get_huddle(canvas_file_id)
        if disk_huddle is not None:
            md, transcript = disk_huddle
            info = HuddleInfo(canvas_file_id=canvas_file_id, transcript_file_id=None, date_start=0, date_end=0)
            self._huddle_cache[canvas_file_id] = (info, md, transcript)
            return (info, md, transcript)

        # Fetch file info to get transcript file ID
        file_data = self._api_call("get_file_info", canvas_file_id)
        if file_data is None:
            return None
        file_info: dict[str, object] = file_data  # type: ignore[assignment]
        transcript_file_id = file_info.get("huddle_transcript_file_id")

        info = HuddleInfo(
            canvas_file_id=canvas_file_id,
            transcript_file_id=str(transcript_file_id) if transcript_file_id else None,
            date_start=int(file_info.get("huddle_date_start", 0)),  # type: ignore[arg-type]
            date_end=int(file_info.get("huddle_date_end", 0)),  # type: ignore[arg-type]
        )
        md = fetch_canvas_markdown(self._client._token, canvas_file_id, self._users)
        transcript = None
        if transcript_file_id:
            transcript = fetch_transcript_markdown(
                self._client._token, str(transcript_file_id), self._users,
            )
        self._huddle_cache[canvas_file_id] = (info, md, transcript)
        disk_cache.put_huddle(canvas_file_id, md, transcript)
        return (info, md, transcript)

    # === Global ===

    @property
    def is_auth_fatal(self) -> bool:
        return self._backoff.fatal

    def find_huddle_index_entry_by_canvas(self, canvas_file_id: str) -> dict[str, str] | None:
        """Look up a huddle index entry by canvas file ID."""
        for e in self.get_huddle_index():
            if e.get("canvas_file_id") == canvas_file_id:
                return e
        return None

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
        self._backoff = _BackoffState()
        log.info("Force refresh: all caches cleared")


def _ts_to_time(ts: str) -> str:
    try:
        from datetime import datetime, timezone
        dt = datetime.fromtimestamp(float(ts), tz=timezone.utc).astimezone()
        return dt.strftime("%H%M")
    except (ValueError, OSError):
        return ts
