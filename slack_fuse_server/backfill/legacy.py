"""`LegacyCacheBackfiller` — historical ingestion from the legacy disk cache.

Reads the existing single-process cache layout under `~/.cache/slack-fuse/`
and yields `Message` items through the Sprint-0 `Backfiller` protocol:

- channels: `<cache>/messages/<channel_id>/`
- day files: `<cache>/messages/<channel_id>/<YYYY-MM-DD>.json`

The slurper writes yielded messages through `OffsetWriter.write_event()` with
message dedup enabled, so re-running this source against an already-populated
events table is a no-op.
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from pathlib import Path
from typing import cast

import trio
from pydantic import ValidationError

from slack_fuse.models import Message
from slack_fuse_render import ChannelId

log = logging.getLogger(__name__)

_DEFAULT_CACHE_DIR = Path.home() / ".cache" / "slack-fuse"


class LegacyCacheBackfiller:
    """`Backfiller` implementation for `~/.cache/slack-fuse/messages/`."""

    def __init__(
        self,
        cache_dir: Path | None = None,
        *,
        limiter: trio.CapacityLimiter | None = None,
    ) -> None:
        root = cache_dir if cache_dir is not None else _DEFAULT_CACHE_DIR
        self._messages_dir = root / "messages"
        self._limiter = limiter

    @property
    def name(self) -> str:
        return "legacy-cache"

    async def channels_to_backfill(self) -> AsyncIterator[ChannelId]:
        channel_ids = await trio.to_thread.run_sync(
            lambda: _discover_channels_with_content(self._messages_dir),
            limiter=self._limiter,
        )
        for channel_id in channel_ids:
            yield channel_id

    async def messages_for_channel(
        self,
        channel_id: ChannelId,
        since_ts: float | None = None,
    ) -> AsyncIterator[Message]:
        day_files = await trio.to_thread.run_sync(
            lambda: _discover_day_files(self._messages_dir / channel_id.value),
            limiter=self._limiter,
        )
        for day_file in day_files:
            messages = await trio.to_thread.run_sync(
                lambda p=day_file: _read_day_messages(p),
                limiter=self._limiter,
            )
            for message in messages:
                if _passes_since(message.ts, since_ts):
                    yield message


def _discover_channels_with_content(messages_dir: Path) -> list[ChannelId]:
    if not messages_dir.exists():
        return []
    channel_ids: list[ChannelId] = []
    for entry in sorted(messages_dir.iterdir(), key=lambda p: p.name):
        if not entry.is_dir():
            continue
        if _channel_has_content(entry):
            channel_ids.append(ChannelId(entry.name))
    return channel_ids


def _channel_has_content(channel_dir: Path) -> bool:
    for path in channel_dir.glob("*.json"):
        try:
            if path.stat().st_size > 2:
                return True
        except OSError:
            continue
    return False


def _discover_day_files(channel_dir: Path) -> list[Path]:
    if not channel_dir.exists():
        return []
    return sorted((path for path in channel_dir.glob("*.json") if path.is_file()), key=lambda p: p.name)


def _read_day_messages(path: Path) -> list[Message]:
    try:
        raw = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        log.debug("legacy-cache: skipping unreadable json %s", path)
        return []
    if not isinstance(raw, list):
        log.debug("legacy-cache: skipping non-list json %s", path)
        return []

    messages: list[Message] = []
    entries = cast("list[object]", raw)
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        try:
            messages.append(Message.model_validate(entry))
        except ValidationError:
            log.debug("legacy-cache: skipping invalid message in %s", path)
    return messages


def _passes_since(ts: str, since_ts: float | None) -> bool:
    if since_ts is None:
        return True
    value = _ts_float(ts)
    if value is None:
        return True
    return value > since_ts


def _ts_float(ts: str) -> float | None:
    try:
        return float(ts)
    except ValueError:
        return None
