# pyright: reportPrivateUsage=false
"""Tests for the dormant-DM filter and the no-empty-persistence bug fix.

Two related behaviours:

1. ``_is_dormant_dm`` hides DMs that backfill has confirmed are empty,
   so a real conversation can reclaim a bare slug instead of getting
   ``-2`` from a never-used duplicate user account.

2. ``_day_messages_base`` must not write an empty disk file or extend
   ``known_dates`` when the API returns 0 messages — without this, every
   day a user opens a dormant DM accumulates a phantom date that
   ``_is_dormant_dm`` would then have to special-case.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

import pytest

from slack_fuse import disk_cache, store
from slack_fuse.api import SlackClient
from slack_fuse.events import DayAppend, DayEvent
from slack_fuse.models import Channel, Message
from slack_fuse.store import SlackStore, _is_dormant_dm
from slack_fuse.user_cache import UserCache

from .stubs import (
    deterministic_random,
    make_stub_get_history,
    stub_load_from_disk,
)


@pytest.fixture(autouse=True)
def isolate_disk_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect disk_cache at a clean tmp dir so we can write markers freely."""
    monkeypatch.setattr(disk_cache, "_CACHE_DIR", tmp_path)
    return tmp_path


@pytest.fixture(autouse=True)
def deterministic_jitter(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(store.random, "random", deterministic_random)


@pytest.fixture
def fresh_store(monkeypatch: pytest.MonkeyPatch) -> Iterator[SlackStore]:
    # isolate_disk_cache already redirected _CACHE_DIR to tmp_path, so the
    # SlackStore constructor's disk reads return None naturally. We only
    # stub UserCache because it has its own load path.
    monkeypatch.setattr(UserCache, "_load_from_disk", stub_load_from_disk)
    client = SlackClient(token="xoxp-fake")
    users = UserCache(client.http)
    yield SlackStore(client=client, users=users)


def _dm(channel_id: str, user_id: str) -> Channel:
    return Channel.model_validate({"id": channel_id, "is_im": True, "user": user_id})


def _channel(channel_id: str, name: str) -> Channel:
    return Channel.model_validate({"id": channel_id, "name": name, "is_member": True})


def _mark_backfilled(cache_dir: Path, channel_id: str) -> None:
    backfill = cache_dir / "backfill"
    backfill.mkdir(parents=True, exist_ok=True)
    (backfill / f"{channel_id}.done").touch()


# === _is_dormant_dm predicate ===


def test_dormant_dm_with_marker_and_no_messages_is_filtered(
    isolate_disk_cache: Path,
) -> None:
    _mark_backfilled(isolate_disk_cache, "D-empty")
    assert _is_dormant_dm(_dm("D-empty", "U1"), day_events={}, thread_events={}) is True


def test_dm_without_backfill_marker_is_not_filtered(isolate_disk_cache: Path) -> None:
    # No marker → we don't know if it's empty or just unscanned.
    assert _is_dormant_dm(_dm("D-new", "U1"), day_events={}, thread_events={}) is False


def test_dm_with_cached_messages_is_not_filtered(isolate_disk_cache: Path) -> None:
    _mark_backfilled(isolate_disk_cache, "D-active")
    msgs = [Message.model_validate({"ts": "1.0", "user": "U1"})]
    disk_cache.put_day_messages("D-active", "2026-04-01", [m.model_dump(mode="json") for m in msgs])
    assert _is_dormant_dm(_dm("D-active", "U1"), day_events={}, thread_events={}) is False


def test_dm_with_only_empty_cache_files_is_filtered(isolate_disk_cache: Path) -> None:
    """Existing accumulated `[]` files don't count as activity."""
    _mark_backfilled(isolate_disk_cache, "D-empty")
    d = isolate_disk_cache / "messages" / "D-empty"
    d.mkdir(parents=True)
    (d / "2026-04-01.json").write_text("[]")
    (d / "2026-04-02.json").write_text("[]")
    assert _is_dormant_dm(_dm("D-empty", "U1"), day_events={}, thread_events={}) is True


def test_in_memory_day_event_unhides_dm(isolate_disk_cache: Path) -> None:
    _mark_backfilled(isolate_disk_cache, "D-fresh")
    msg = Message(ts="1.0", user="U1")
    day_events: dict[tuple[str, str], list[DayEvent]] = {
        ("D-fresh", "2026-04-01"): [DayAppend(message=msg)],
    }
    assert _is_dormant_dm(_dm("D-fresh", "U1"), day_events=day_events, thread_events={}) is False


def test_non_dm_is_never_filtered(isolate_disk_cache: Path) -> None:
    """Channels and MPIMs aren't affected by the DM filter."""
    _mark_backfilled(isolate_disk_cache, "C-empty")
    assert _is_dormant_dm(_channel("C-empty", "empty-channel"), day_events={}, thread_events={}) is False


# === Slug reassignment when a dormant duplicate is filtered ===


def test_dormant_duplicate_releases_bare_slug_for_real_dm(
    fresh_store: SlackStore,
    isolate_disk_cache: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two DMs with users sharing a display name: dormant one filtered, real one gets bare slug."""
    fresh_store._users._users = {"U-dormant": "Jacob Segal", "U-real": "Jacob Segal"}

    # Dormant: marker exists, no messages.
    _mark_backfilled(isolate_disk_cache, "D-dormant")
    # Real: marker exists AND has at least one cached non-empty day.
    _mark_backfilled(isolate_disk_cache, "D-real")
    real_msg = Message.model_validate({"ts": "1.0", "user": "U-real", "text": "hi"})
    disk_cache.put_day_messages("D-real", "2026-04-01", [real_msg.model_dump(mode="json")])

    channels = [_dm("D-dormant", "U-dormant"), _dm("D-real", "U-real")]
    entries = fresh_store._build_channel_entries(channels)

    assert "D-dormant" not in entries
    assert entries["D-real"].slug == "jacob-segal"


def test_both_dms_visible_when_neither_filtered(
    fresh_store: SlackStore,
    isolate_disk_cache: Path,
) -> None:
    """Without a dormant filter trigger, slug-dedup still appends -2 to the second."""
    fresh_store._users._users = {"U-a": "Jacob Segal", "U-b": "Jacob Segal"}
    # No backfill markers → neither filtered.
    channels = [_dm("D-a", "U-a"), _dm("D-b", "U-b")]
    entries = fresh_store._build_channel_entries(channels)
    assert entries["D-a"].slug == "jacob-segal"
    assert entries["D-b"].slug == "jacob-segal-2"


# === _day_messages_base: empty API response must not pollute disk ===


def test_empty_api_response_does_not_persist_known_date_or_disk(
    fresh_store: SlackStore,
    monkeypatch: pytest.MonkeyPatch,
    isolate_disk_cache: Path,
) -> None:
    """The accumulation bug: every empty fetch used to add today to known_dates forever."""
    channel_id = "D-empty"
    date_str = datetime.now().astimezone().strftime("%Y-%m-%d")
    monkeypatch.setattr(fresh_store._client, "get_history", make_stub_get_history([]))

    result = fresh_store._day_messages_base(channel_id, date_str)

    assert result == []
    assert channel_id not in fresh_store._known_dates
    # Disk file should NOT have been created for the empty result.
    assert disk_cache.get_day_messages(channel_id, date_str) is None
    assert disk_cache.get_known_dates(channel_id) is None


def test_nonempty_api_response_still_persists(
    fresh_store: SlackStore,
    monkeypatch: pytest.MonkeyPatch,
    isolate_disk_cache: Path,
) -> None:
    channel_id = "C-real"
    date_str = datetime.now().astimezone().strftime("%Y-%m-%d")
    msg = Message.model_validate({"ts": "1.0", "user": "U1", "text": "hello"})
    monkeypatch.setattr(fresh_store._client, "get_history", make_stub_get_history([msg]))

    result = fresh_store._day_messages_base(channel_id, date_str)

    assert result == [msg]
    assert date_str in fresh_store._known_dates[channel_id]
    assert disk_cache.get_day_messages(channel_id, date_str) is not None
    assert disk_cache.get_known_dates(channel_id) == {date_str}
