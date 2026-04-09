"""Tests for slack_fuse.disk_cache — round-trip + corruption handling.

Uses monkeypatch to redirect ``_CACHE_DIR`` to a tmp_path so we never touch
the user's real ~/.cache/slack-fuse/.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from slack_fuse import disk_cache
from slack_fuse.models import Channel, HuddleIndexEntry, Message


@pytest.fixture(autouse=True, name="cache_dir")
def fixture_cache_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point disk_cache at a fresh tmp dir for every test."""
    monkeypatch.setattr(disk_cache, "_CACHE_DIR", tmp_path)
    return tmp_path


def test_day_messages_round_trip_via_pydantic() -> None:
    msgs = [
        Message.model_validate({"ts": "1.0", "user": "U1", "text": "hello"}),
        Message.model_validate({"ts": "2.0", "bot_id": "B1", "text": "from bot"}),
    ]
    payload = [m.model_dump(mode="json") for m in msgs]
    disk_cache.put_day_messages("C1", "2026-04-09", payload)
    loaded = disk_cache.get_day_messages("C1", "2026-04-09")
    assert loaded is not None
    assert [Message.model_validate(m) for m in loaded] == msgs


def test_thread_round_trip_handles_dot_in_thread_ts() -> None:
    msgs = [
        Message.model_validate({"ts": "1700000000.000100", "user": "U1", "text": "p"}),
        Message.model_validate({"ts": "1700000001.000200", "user": "U2", "text": "r"}),
    ]
    payload = [m.model_dump(mode="json") for m in msgs]
    disk_cache.put_thread("C1", "1700000000.000100", payload)
    loaded = disk_cache.get_thread("C1", "1700000000.000100")
    assert loaded is not None
    assert [Message.model_validate(m) for m in loaded] == msgs


def test_channel_list_round_trip_via_pydantic() -> None:
    channels = [
        Channel.model_validate({
            "id": "C1",
            "name": "general",
            "is_member": True,
            "topic": {"value": "T", "creator": "", "last_set": 0},
        }),
        Channel.model_validate({"id": "D1", "is_im": True, "user": "U2"}),
    ]
    payload = [c.model_dump(mode="json") for c in channels]
    disk_cache.put_channel_list(payload)
    loaded = disk_cache.get_channel_list()
    assert loaded is not None
    assert [Channel.model_validate(c) for c in loaded] == channels


def test_huddle_index_round_trip_via_pydantic() -> None:
    entries = [
        HuddleIndexEntry(
            month="2026-04",
            day="09",
            slug="huddle-1",
            channel_id="C1",
            channel_slug="general",
            thread_ts="1700000000.000100",
            canvas_file_id="F1",
            conv_root="channels",
        ),
        HuddleIndexEntry(month="2026-04", day="09", slug="huddle-2", canvas_file_id="F2", conv_root="dms"),
    ]
    disk_cache.put_huddle_index([e.model_dump(mode="json") for e in entries])
    loaded = disk_cache.get_huddle_index()
    assert loaded is not None
    restored = [HuddleIndexEntry.model_validate(e) for e in loaded]
    assert restored[0].canvas_file_id == "F1"
    assert restored[1].conv_root == "dms"


def test_known_dates_round_trip_set_semantics() -> None:
    dates = {"2026-04-09", "2026-04-08", "2025-12-31"}
    disk_cache.put_known_dates("C1", dates)
    assert disk_cache.get_known_dates("C1") == dates


def test_huddle_round_trip_with_and_without_transcript() -> None:
    disk_cache.put_huddle("F1", "# Notes\n\nA", "## Transcript\n\nB")
    disk_cache.put_huddle("F2", "# Only notes", None)
    loaded1 = disk_cache.get_huddle("F1")
    loaded2 = disk_cache.get_huddle("F2")
    assert loaded1 == ("# Notes\n\nA", "## Transcript\n\nB")
    assert loaded2 == ("# Only notes", None)


def test_huddle_put_with_none_notes_writes_nothing() -> None:
    """Without notes there's no useful huddle to cache."""
    disk_cache.put_huddle("F1", None, "transcript")
    assert disk_cache.get_huddle("F1") is None


@pytest.mark.parametrize(
    ("subpath", "getter_args"),
    [
        ("messages/C1/2026-04-09.json", ("get_day_messages", ("C1", "2026-04-09"))),
        ("channels.json", ("get_channel_list", ())),
        ("known_dates/C1.json", ("get_known_dates", ("C1",))),
    ],
)
def test_corrupt_files_return_none_instead_of_raising(
    cache_dir: Path,
    subpath: str,
    getter_args: tuple[str, tuple[str, ...]],
) -> None:
    """All disk_cache getters must swallow JSON corruption."""
    target = cache_dir / subpath
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("{not valid json")
    fn_name, args = getter_args
    fn = getattr(disk_cache, fn_name)
    assert fn(*args) is None
