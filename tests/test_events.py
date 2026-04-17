"""Tests for the pure event merge functions in slack_fuse.events.

These functions are the heart of the socket-mode push-liveness model: read
paths take a base snapshot from the API/disk cache and layer in-memory
events on top. The merge must be idempotent, ts-keyed, and never drop the
thread parent.
"""

from __future__ import annotations

from slack_fuse.events import (
    EVENT_LOG_CAP,
    DayAppend,
    DayBumpParent,
    DayDelete,
    DayReplace,
    ThreadAppend,
    ThreadDelete,
    ThreadReplace,
    cap_log,
    merge_day,
    merge_thread,
)
from slack_fuse.models import Message, Thread


def _msg(ts: str, text: str = "", **kwargs: object) -> Message:
    return Message.model_validate({"ts": ts, "user": "U1", "text": text, **kwargs})


# === merge_day ===


def test_merge_day_empty_events_returns_sorted_base() -> None:
    base = [_msg("2.0"), _msg("1.0")]
    result = merge_day(base, [])
    assert [m.ts for m in result] == ["1.0", "2.0"]


def test_merge_day_append_adds_new_message() -> None:
    base = [_msg("1.0")]
    new = _msg("2.0", "hello")
    result = merge_day(base, [DayAppend(message=new)])
    assert [m.ts for m in result] == ["1.0", "2.0"]
    assert result[1].text == "hello"


def test_merge_day_append_same_ts_overwrites() -> None:
    """A second append with the same ts supersedes the first (last-write-wins)."""
    base = [_msg("1.0", "old")]
    result = merge_day(base, [DayAppend(message=_msg("1.0", "new"))])
    assert len(result) == 1
    assert result[0].text == "new"


def test_merge_day_replace_updates_by_ts() -> None:
    base = [_msg("1.0", "before"), _msg("2.0")]
    result = merge_day(base, [DayReplace(message=_msg("1.0", "after"))])
    assert result[0].text == "after"
    assert result[1].ts == "2.0"


def test_merge_day_replace_on_unknown_ts_inserts() -> None:
    """Replace acts like append if the ts is not in the base."""
    base = [_msg("1.0")]
    result = merge_day(base, [DayReplace(message=_msg("3.0", "new"))])
    assert [m.ts for m in result] == ["1.0", "3.0"]


def test_merge_day_delete_drops_by_ts() -> None:
    base = [_msg("1.0"), _msg("2.0"), _msg("3.0")]
    result = merge_day(base, [DayDelete(ts="2.0")])
    assert [m.ts for m in result] == ["1.0", "3.0"]


def test_merge_day_delete_unknown_ts_is_noop() -> None:
    base = [_msg("1.0"), _msg("2.0")]
    result = merge_day(base, [DayDelete(ts="99.0")])
    assert [m.ts for m in result] == ["1.0", "2.0"]


def test_merge_day_bump_parent_increments_reply_count_and_latest_reply() -> None:
    parent = Message.model_validate({
        "ts": "1.0",
        "user": "U1",
        "reply_count": 2,
        "latest_reply": "5.0",
    })
    base = [parent]
    result = merge_day(
        base,
        [DayBumpParent(parent_ts="1.0", delta_count=1, latest_reply="6.0")],
    )
    assert result[0].reply_count == 3
    assert result[0].latest_reply == "6.0"


def test_merge_day_bump_parent_negative_delta_clamped_at_zero() -> None:
    parent = Message.model_validate({"ts": "1.0", "user": "U1", "reply_count": 0})
    result = merge_day(
        [parent],
        [DayBumpParent(parent_ts="1.0", delta_count=-1)],
    )
    assert result[0].reply_count == 0


def test_merge_day_bump_parent_keeps_latest_reply_when_event_has_none() -> None:
    """On delete we don't know the new last reply — keep the old value."""
    parent = Message.model_validate({
        "ts": "1.0",
        "user": "U1",
        "reply_count": 3,
        "latest_reply": "5.0",
    })
    result = merge_day(
        [parent],
        [DayBumpParent(parent_ts="1.0", delta_count=-1, latest_reply=None)],
    )
    assert result[0].reply_count == 2
    assert result[0].latest_reply == "5.0"


def test_merge_day_bump_parent_for_unknown_parent_is_noop() -> None:
    base = [_msg("1.0")]
    result = merge_day(
        base,
        [DayBumpParent(parent_ts="99.0", delta_count=1)],
    )
    assert len(result) == 1
    assert result[0].reply_count == 0


def test_merge_day_multiple_events_apply_in_order() -> None:
    base = [_msg("1.0", "v0")]
    events = [
        DayReplace(message=_msg("1.0", "v1")),
        DayAppend(message=_msg("2.0", "new")),
        DayDelete(ts="1.0"),
    ]
    result = merge_day(base, events)
    assert [m.ts for m in result] == ["2.0"]


def test_merge_day_is_pure_does_not_mutate_base() -> None:
    original = _msg("1.0", "hello")
    base = [original]
    _ = merge_day(base, [DayReplace(message=_msg("1.0", "edited"))])
    # Original list and object are untouched
    assert len(base) == 1
    assert base[0].text == "hello"


# === merge_thread ===


def test_merge_thread_empty_events_returns_base() -> None:
    parent = _msg("1.0", "parent", thread_ts="1.0")
    replies = (_msg("2.0", thread_ts="1.0"), _msg("3.0", thread_ts="1.0"))
    base = Thread(parent=parent, replies=replies)
    result = merge_thread(base, [])
    assert result == base


def test_merge_thread_append_adds_reply_in_order() -> None:
    parent = _msg("1.0", thread_ts="1.0")
    base = Thread(parent=parent, replies=(_msg("2.0", thread_ts="1.0"),))
    new_reply = _msg("3.0", "new", thread_ts="1.0")
    result = merge_thread(base, [ThreadAppend(message=new_reply)])
    assert [m.ts for m in result.replies] == ["2.0", "3.0"]


def test_merge_thread_replace_updates_parent() -> None:
    parent = _msg("1.0", "before", thread_ts="1.0")
    base = Thread(parent=parent, replies=())
    result = merge_thread(
        base,
        [ThreadReplace(message=_msg("1.0", "after", thread_ts="1.0"))],
    )
    assert result.parent.text == "after"


def test_merge_thread_replace_updates_reply() -> None:
    parent = _msg("1.0", thread_ts="1.0")
    old_reply = _msg("2.0", "old", thread_ts="1.0")
    base = Thread(parent=parent, replies=(old_reply,))
    result = merge_thread(
        base,
        [ThreadReplace(message=_msg("2.0", "new", thread_ts="1.0"))],
    )
    assert result.replies[0].text == "new"


def test_merge_thread_delete_reply_drops_it() -> None:
    parent = _msg("1.0", thread_ts="1.0")
    base = Thread(
        parent=parent,
        replies=(_msg("2.0", thread_ts="1.0"), _msg("3.0", thread_ts="1.0")),
    )
    result = merge_thread(base, [ThreadDelete(ts="2.0")])
    assert [m.ts for m in result.replies] == ["3.0"]


def test_merge_thread_delete_parent_is_ignored() -> None:
    """The parent is the anchor of the thread — never delete it."""
    parent = _msg("1.0", "parent", thread_ts="1.0")
    base = Thread(parent=parent, replies=(_msg("2.0", thread_ts="1.0"),))
    result = merge_thread(base, [ThreadDelete(ts="1.0")])
    assert result.parent.ts == "1.0"
    assert result.parent.text == "parent"
    assert [m.ts for m in result.replies] == ["2.0"]


def test_merge_thread_is_pure() -> None:
    parent = _msg("1.0", thread_ts="1.0")
    reply = _msg("2.0", "orig", thread_ts="1.0")
    base = Thread(parent=parent, replies=(reply,))
    _ = merge_thread(
        base,
        [ThreadReplace(message=_msg("2.0", "changed", thread_ts="1.0"))],
    )
    assert base.replies[0].text == "orig"


# === cap_log ===


def test_cap_log_does_nothing_below_cap() -> None:
    log: list[int] = list(range(10))
    cap_log(log)
    assert log == list(range(10))


def test_cap_log_trims_oldest_entries_above_cap() -> None:
    log: list[int] = list(range(EVENT_LOG_CAP + 10))
    cap_log(log)
    assert len(log) == EVENT_LOG_CAP
    assert log[0] == 10
    assert log[-1] == EVENT_LOG_CAP + 9
