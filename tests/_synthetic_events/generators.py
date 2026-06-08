"""Deterministic synthetic event generators. Re-exported from `__init__`."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field

from slack_fuse_server._json import JsonObject
from slack_fuse_server.wire.frames import EventFrame


@dataclass(frozen=True, slots=True)
class SyntheticEvent:
    stream: str
    offset: int
    kind: str
    ts: str | None
    payload: JsonObject = field(default_factory=dict)

    def to_frame(self) -> EventFrame:
        return EventFrame(stream=self.stream, offset=self.offset, kind=self.kind, ts=self.ts, payload=self.payload)


def synthetic_ts(index: int) -> str:
    """Deterministic Slack-style ts (UTC epoch seconds + microsecond frac)."""
    return f"{1700000000 + index}.{index % 1000000:06d}"


def channel_message_events(
    channel_id: str,
    count: int,
    *,
    start_offset: int = 1,
    start_index: int = 0,
) -> Iterator[SyntheticEvent]:
    """A run of top-level `message` events on `channel:<channel_id>`."""
    stream = f"channel:{channel_id}"
    for i in range(count):
        index = start_index + i
        ts = synthetic_ts(index)
        payload: JsonObject = {
            "type": "message",
            "ts": ts,
            "user": f"U{index:04d}",
            "text": f"synthetic message {index} mentioning <@U{(index + 1):04d}>",
            "thread_ts": None,
        }
        yield SyntheticEvent(stream=stream, offset=start_offset + i, kind="message", ts=ts, payload=payload)


def channel_reply_events(
    channel_id: str,
    thread_ts: str,
    count: int,
    *,
    start_offset: int = 1,
    start_index: int = 0,
) -> Iterator[SyntheticEvent]:
    """A run of thread-reply `message` events (payload `thread_ts != ts`)."""
    stream = f"channel:{channel_id}"
    for i in range(count):
        index = start_index + i
        ts = synthetic_ts(index)
        payload: JsonObject = {
            "type": "message",
            "ts": ts,
            "user": f"U{index:04d}",
            "text": f"synthetic reply {index}",
            "thread_ts": thread_ts,
        }
        yield SyntheticEvent(stream=stream, offset=start_offset + i, kind="message", ts=ts, payload=payload)


def user_events(count: int, *, start_offset: int = 1) -> Iterator[SyntheticEvent]:
    """A run of `user_added` events on the singleton `users` stream."""
    for i in range(count):
        uid = f"U{i:04d}"
        payload: JsonObject = {"user_id": uid, "display_name": f"User {i}"}
        yield SyntheticEvent(stream="users", offset=start_offset + i, kind="user_added", ts=None, payload=payload)
