"""Disk cache for Slack data that survives restarts.

JSON files keyed by channel/date/thread_ts/canvas_file_id. Contents are
opaque `JsonObject` to this module — typed validation happens in `store.py`
where the relevant Pydantic model is known.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from .models import JsonObject

log = logging.getLogger(__name__)

_CACHE_DIR = Path.home() / ".cache" / "slack-fuse"


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


# === Huddle content (notes.md, transcript.md) ===
# Keyed by canvas_file_id. Immutable once created.

def get_huddle(canvas_file_id: str) -> tuple[str | None, str | None] | None:
    """Load cached huddle content. Returns (notes_md, transcript_md) or None."""
    d = _CACHE_DIR / "huddles" / canvas_file_id
    if not d.exists():
        return None
    notes = _read_text(d / "notes.md")
    transcript = _read_text(d / "transcript.md")
    if notes is None:
        return None  # At minimum we need notes
    return (notes, transcript)


def put_huddle(canvas_file_id: str, notes_md: str | None, transcript_md: str | None) -> None:
    """Cache huddle content to disk."""
    if notes_md is None:
        return
    d = _CACHE_DIR / "huddles" / canvas_file_id
    _ensure_dir(d)
    (d / "notes.md").write_text(notes_md)
    if transcript_md is not None:
        (d / "transcript.md").write_text(transcript_md)


# === Day messages ===
# Keyed by (channel_id, date_str). Old messages (>7 days) are effectively immutable.

def get_day_messages(channel_id: str, date_str: str) -> list[JsonObject] | None:
    """Load cached day messages. Returns list of raw JSON objects or None."""
    path = _CACHE_DIR / "messages" / channel_id / f"{date_str}.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def put_day_messages(channel_id: str, date_str: str, messages: list[JsonObject]) -> None:
    """Cache day messages to disk."""
    d = _CACHE_DIR / "messages" / channel_id
    _ensure_dir(d)
    (d / f"{date_str}.json").write_text(json.dumps(messages))


# === Threads ===
# Keyed by (channel_id, thread_ts). Old threads are effectively immutable.

def get_thread(channel_id: str, thread_ts: str) -> list[JsonObject] | None:
    """Load cached thread. Returns list of raw JSON objects (parent + replies) or None."""
    safe_ts = thread_ts.replace(".", "-")
    path = _CACHE_DIR / "threads" / channel_id / f"{safe_ts}.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def put_thread(channel_id: str, thread_ts: str, messages: list[JsonObject]) -> None:
    """Cache thread to disk."""
    safe_ts = thread_ts.replace(".", "-")
    d = _CACHE_DIR / "threads" / channel_id
    _ensure_dir(d)
    (d / f"{safe_ts}.json").write_text(json.dumps(messages))


# === Channel list ===

def get_channel_list() -> list[JsonObject] | None:
    """Load cached channel list."""
    path = _CACHE_DIR / "channels.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def put_channel_list(channels: list[JsonObject]) -> None:
    """Cache channel list to disk."""
    _ensure_dir(_CACHE_DIR)
    (_CACHE_DIR / "channels.json").write_text(json.dumps(channels))


# === Huddle index ===

def get_huddle_index() -> list[JsonObject] | None:
    """Load cached huddle index."""
    path = _CACHE_DIR / "huddle_index.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def put_huddle_index(entries: list[JsonObject]) -> None:
    """Cache huddle index to disk."""
    _ensure_dir(_CACHE_DIR)
    (_CACHE_DIR / "huddle_index.json").write_text(json.dumps(entries))


# === Known dates per channel ===

def get_known_dates(channel_id: str) -> set[str] | None:
    """Load cached known dates for a channel."""
    path = _CACHE_DIR / "known_dates" / f"{channel_id}.json"
    if not path.exists():
        return None
    try:
        return set(json.loads(path.read_text()))
    except (json.JSONDecodeError, OSError):
        return None


def put_known_dates(channel_id: str, dates: set[str]) -> None:
    """Cache known dates for a channel."""
    d = _CACHE_DIR / "known_dates"
    _ensure_dir(d)
    (d / f"{channel_id}.json").write_text(json.dumps(sorted(dates)))


def _read_text(path: Path) -> str | None:
    if path.exists():
        try:
            return path.read_text()
        except OSError:
            return None
    return None
