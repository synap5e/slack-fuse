"""In-memory event log types and pure merge functions for socket-mode liveness.

Slack socket-mode events are collected into per-key logs (one per day, one
per thread). Reads materialize the view by applying the log on top of the
existing snapshot — so a busy channel under active reads costs zero extra
fetches.

These types are internal-only and never persisted. They are frozen
dataclasses (not Pydantic models) because they don't cross an I/O boundary.

A log is bounded by `EVENT_LOG_CAP` per key; the oldest entries are dropped
when the cap is exceeded. The polling TTL that still runs alongside is the
correctness floor — if a cap drops something we missed, the next polled
refetch picks it up.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from .models import Message, Thread

EVENT_LOG_CAP: int = 500


# === Day events ===


@dataclass(frozen=True)
class DayAppend:
    """A new message landed on this day. Also used for edits applied before
    a refetch — a second Append with the same ts overwrites the first."""

    message: Message


@dataclass(frozen=True)
class DayReplace:
    """A `message_changed` edit. Replaces by ts."""

    message: Message


@dataclass(frozen=True)
class DayDelete:
    """A `message_deleted`. Drops by ts."""

    ts: str


@dataclass(frozen=True)
class DayBumpParent:
    """A reply arrived (or was deleted). Shifts the parent's reply_count and
    optionally updates its latest_reply.

    `delta_count` is typically +1 for a new reply, -1 for a deleted reply.
    `latest_reply` is the new reply's ts on add, or None on delete (since we
    don't know the new last reply without re-examining the thread).
    """

    parent_ts: str
    delta_count: int
    latest_reply: str | None = None


type DayEvent = DayAppend | DayReplace | DayDelete | DayBumpParent


# === Thread events ===


@dataclass(frozen=True)
class ThreadAppend:
    message: Message


@dataclass(frozen=True)
class ThreadReplace:
    message: Message


@dataclass(frozen=True)
class ThreadDelete:
    ts: str


type ThreadEvent = ThreadAppend | ThreadReplace | ThreadDelete


# === Merge functions (pure) ===


def merge_day(base: Sequence[Message], events: Sequence[DayEvent]) -> list[Message]:
    """Apply day events to a base snapshot. Pure — returns a new list.

    Messages are keyed by ts for idempotent append/replace/delete. A parent
    bump only fires if the parent is present in the snapshot; a bump that
    references an unknown parent is ignored (the next refetch will fix it).
    """
    by_ts: dict[str, Message] = {m.ts: m for m in base}
    for event in events:
        _apply_day_event(by_ts, event)
    return sorted(by_ts.values(), key=lambda m: m.ts)


def _apply_day_event(by_ts: dict[str, Message], event: DayEvent) -> None:
    if isinstance(event, (DayAppend, DayReplace)):
        by_ts[event.message.ts] = event.message
        return
    if isinstance(event, DayDelete):
        by_ts.pop(event.ts, None)
        return
    parent = by_ts.get(event.parent_ts)
    if parent is None:
        return
    updates: dict[str, object] = {"reply_count": max(0, parent.reply_count + event.delta_count)}
    if event.latest_reply is not None:
        updates["latest_reply"] = event.latest_reply
    by_ts[event.parent_ts] = parent.model_copy(update=updates)


def merge_thread(base: Thread, events: Sequence[ThreadEvent]) -> Thread:
    """Apply thread events to a base Thread snapshot. Pure — returns a new Thread.

    The parent message is never deleted via this path even if an event tries
    to; tombstoning the thread parent would make the whole thread disappear
    from the render. Edits to the parent ARE respected.
    """
    all_msgs = [base.parent, *base.replies]
    by_ts: dict[str, Message] = {m.ts: m for m in all_msgs}
    parent_ts = base.parent.ts
    for event in events:
        _apply_thread_event(by_ts, event, parent_ts)
    sorted_msgs = sorted(by_ts.values(), key=lambda m: m.ts)
    parent = next((m for m in sorted_msgs if m.ts == parent_ts), base.parent)
    replies = tuple(m for m in sorted_msgs if m.ts != parent_ts)
    return Thread(parent=parent, replies=replies)


def _apply_thread_event(by_ts: dict[str, Message], event: ThreadEvent, parent_ts: str) -> None:
    if isinstance(event, (ThreadAppend, ThreadReplace)):
        by_ts[event.message.ts] = event.message
        return
    if event.ts == parent_ts:
        return
    by_ts.pop(event.ts, None)


def cap_log[E](log: list[E]) -> None:
    """Trim a mutable log in place to stay under EVENT_LOG_CAP."""
    if len(log) > EVENT_LOG_CAP:
        del log[: len(log) - EVENT_LOG_CAP]
