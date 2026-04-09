# pyright: reportPrivateUsage=false
"""Tests for the thread-backfill helper.

Focus on `_collect_thread_parents` because it's the only piece of the
backfill flow that's pure (no trio, no random sleeps, no live API). The
fetch loop itself is exercised by the post-restart smoke test.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from slack_fuse import backfill


@pytest.fixture
def messages_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect backfill._MESSAGES_DIR to a tmp dir for the duration of the test."""
    monkeypatch.setattr(backfill, "_MESSAGES_DIR", tmp_path)
    return tmp_path


def _write_day(messages_dir: Path, channel_id: str, date_str: str, msgs: list[dict[str, object]]) -> None:
    d = messages_dir / channel_id
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{date_str}.json").write_text(json.dumps(msgs))


def test_collect_thread_parents_empty_when_no_cache_dir(messages_dir: Path) -> None:
    assert backfill._collect_thread_parents("C_missing") == []


def test_collect_thread_parents_finds_threads_with_replies(messages_dir: Path) -> None:
    _write_day(messages_dir, "C1", "2026-04-08", [
        {"ts": "1.0", "user": "U1", "text": "thread parent", "thread_ts": "1.0", "reply_count": 3},
        {"ts": "2.0", "user": "U1", "text": "plain message", "reply_count": 0},
        {"ts": "3.0", "user": "U2", "text": "another thread", "thread_ts": "3.0", "reply_count": 1},
    ])
    parents = backfill._collect_thread_parents("C1")
    assert parents == ["1.0", "3.0"]


def test_collect_thread_parents_skips_messages_without_replies(messages_dir: Path) -> None:
    _write_day(messages_dir, "C1", "2026-04-08", [
        {"ts": "1.0", "user": "U1", "text": "no replies"},
        {"ts": "2.0", "user": "U1", "text": "explicitly zero", "reply_count": 0},
    ])
    assert backfill._collect_thread_parents("C1") == []


def test_collect_thread_parents_skips_replies_themselves(messages_dir: Path) -> None:
    """A reply has thread_ts != ts. Don't treat it as a parent."""
    _write_day(messages_dir, "C1", "2026-04-08", [
        {"ts": "1.0", "user": "U1", "text": "parent", "thread_ts": "1.0", "reply_count": 2},
        {"ts": "1.5", "user": "U2", "text": "reply", "thread_ts": "1.0", "reply_count": 0},
    ])
    assert backfill._collect_thread_parents("C1") == ["1.0"]


def test_collect_thread_parents_dedupes_across_days(messages_dir: Path) -> None:
    """The same thread parent could appear in multiple day files (rare but possible)."""
    _write_day(messages_dir, "C1", "2026-04-07", [
        {"ts": "1.0", "user": "U1", "text": "parent", "thread_ts": "1.0", "reply_count": 1},
    ])
    _write_day(messages_dir, "C1", "2026-04-08", [
        {"ts": "1.0", "user": "U1", "text": "parent", "thread_ts": "1.0", "reply_count": 1},
    ])
    assert backfill._collect_thread_parents("C1") == ["1.0"]


def test_collect_thread_parents_tolerates_corrupt_files(messages_dir: Path) -> None:
    d = messages_dir / "C1"
    d.mkdir(parents=True, exist_ok=True)
    (d / "garbage.json").write_text("not json")
    (d / "2026-04-08.json").write_text(json.dumps([
        {"ts": "1.0", "user": "U1", "text": "ok", "thread_ts": "1.0", "reply_count": 1},
    ]))
    assert backfill._collect_thread_parents("C1") == ["1.0"]


def test_collect_thread_parents_tolerates_non_list_root(messages_dir: Path) -> None:
    d = messages_dir / "C1"
    d.mkdir(parents=True, exist_ok=True)
    (d / "weird.json").write_text(json.dumps({"not": "a list"}))
    assert backfill._collect_thread_parents("C1") == []


def test_collect_thread_parents_tolerates_non_dict_entries(messages_dir: Path) -> None:
    # Mixed list — _collect_thread_parents has to skip non-dict entries silently.
    d = messages_dir / "C1"
    d.mkdir(parents=True, exist_ok=True)
    (d / "2026-04-08.json").write_text(json.dumps([
        "garbage",
        {"ts": "1.0", "user": "U1", "thread_ts": "1.0", "reply_count": 1},
    ]))
    assert backfill._collect_thread_parents("C1") == ["1.0"]
