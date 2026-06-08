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


def _message_payload(index: int, ts: str, text: str, *, thread_ts: str | None) -> JsonObject:
    return {
        "ts": ts,
        "user": f"U{index:04d}",
        "text": text,
        "thread_ts": thread_ts,
    }


def _channel_payload(index: int, channel_id: str, *, name: str) -> JsonObject:
    return {
        "id": channel_id,
        "name": name,
        "is_private": False,
        "is_im": False,
        "is_mpim": False,
        "topic": "",
        "purpose": "",
        "num_members": 5 + index,
        "is_member": True,
        "im_user_id": None,
    }


def _user_payload(index: int, user_id: str) -> JsonObject:
    return {
        "id": user_id,
        "name": f"user-{index}",
        "profile": {
            "display_name": f"User {index}",
            "real_name": f"Synthetic User {index}",
        },
    }


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
        payload = _message_payload(
            index,
            ts,
            f"synthetic message {index} mentioning <@U{(index + 1):04d}>",
            thread_ts=None,
        )
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
        payload = _message_payload(index, ts, f"synthetic reply {index}", thread_ts=thread_ts)
        yield SyntheticEvent(stream=stream, offset=start_offset + i, kind="message", ts=ts, payload=payload)


def message_changed_events(
    channel_id: str,
    count: int,
    *,
    start_offset: int = 1,
    start_index: int = 0,
    thread_ts: str | None = None,
) -> Iterator[SyntheticEvent]:
    """A run of `message_changed` edit events on `channel:<channel_id>`."""
    stream = f"channel:{channel_id}"
    for i in range(count):
        index = start_index + i
        ts = synthetic_ts(index)
        edited = _message_payload(index, ts, f"synthetic edited message {index}", thread_ts=thread_ts)
        payload: JsonObject = {"message": edited, "previous_ts": ts}
        yield SyntheticEvent(stream=stream, offset=start_offset + i, kind="message_changed", ts=ts, payload=payload)


def message_deleted_events(
    channel_id: str,
    count: int,
    *,
    start_offset: int = 1,
    start_index: int = 0,
    thread_ts: str | None = None,
) -> Iterator[SyntheticEvent]:
    """A run of `message_deleted` events on `channel:<channel_id>`."""
    stream = f"channel:{channel_id}"
    for i in range(count):
        index = start_index + i
        ts = synthetic_ts(index)
        previous = _message_payload(index, ts, f"synthetic deleted message {index}", thread_ts=thread_ts)
        payload: JsonObject = {"deleted_ts": ts, "previous_message": previous}
        yield SyntheticEvent(stream=stream, offset=start_offset + i, kind="message_deleted", ts=ts, payload=payload)


def channel_added_events(count: int, *, start_offset: int = 1, start_index: int = 0) -> Iterator[SyntheticEvent]:
    """A run of `channel_added` events on the singleton `channel-list` stream."""
    for i in range(count):
        index = start_index + i
        channel_id = f"C{index:08d}"
        payload = _channel_payload(index, channel_id, name=f"synthetic-channel-{index}")
        yield SyntheticEvent(
            stream="channel-list",
            offset=start_offset + i,
            kind="channel_added",
            ts=None,
            payload=payload,
        )


def channel_renamed_events(
    count: int,
    *,
    start_offset: int = 1,
    start_index: int = 0,
) -> Iterator[SyntheticEvent]:
    """A run of `channel_renamed` events on `channel-list`."""
    for i in range(count):
        index = start_index + i
        channel_id = f"C{index:08d}"
        payload: JsonObject = {"channel_id": channel_id, "new_name": f"synthetic-channel-{index}-renamed"}
        yield SyntheticEvent(
            stream="channel-list",
            offset=start_offset + i,
            kind="channel_renamed",
            ts=None,
            payload=payload,
        )


def channel_archived_events(
    count: int,
    *,
    start_offset: int = 1,
    start_index: int = 0,
) -> Iterator[SyntheticEvent]:
    """A run of `channel_archived` events on `channel-list`."""
    for i in range(count):
        index = start_index + i
        payload: JsonObject = {"channel_id": f"C{index:08d}"}
        yield SyntheticEvent(
            stream="channel-list",
            offset=start_offset + i,
            kind="channel_archived",
            ts=None,
            payload=payload,
        )


def user_added_events(
    count: int,
    *,
    start_offset: int = 1,
    start_index: int = 0,
) -> Iterator[SyntheticEvent]:
    """A run of `user_added` events on the singleton `users` stream."""
    for i in range(count):
        index = start_index + i
        user_id = f"U{index:04d}"
        payload = _user_payload(index, user_id)
        yield SyntheticEvent(stream="users", offset=start_offset + i, kind="user_added", ts=None, payload=payload)


def user_renamed_events(
    count: int,
    *,
    start_offset: int = 1,
    start_index: int = 0,
) -> Iterator[SyntheticEvent]:
    """A run of `user_renamed` events on the singleton `users` stream."""
    for i in range(count):
        index = start_index + i
        payload: JsonObject = {"user_id": f"U{index:04d}", "new_display_name": f"Renamed User {index}"}
        yield SyntheticEvent(stream="users", offset=start_offset + i, kind="user_renamed", ts=None, payload=payload)


def user_events(count: int, *, start_offset: int = 1) -> Iterator[SyntheticEvent]:
    """Back-compat alias: yields `user_added` events."""
    yield from user_added_events(count, start_offset=start_offset, start_index=0)


def reaction_added_events(
    channel_id: str,
    count: int,
    *,
    start_offset: int = 1,
    start_index: int = 0,
) -> Iterator[SyntheticEvent]:
    """A run of `reaction_added` events (v2: not yet emitted by the slurper)."""
    stream = f"channel:{channel_id}"
    for i in range(count):
        index = start_index + i
        target_ts = synthetic_ts(index)
        payload: JsonObject = {"target_ts": target_ts, "user": f"U{index:04d}", "emoji": "thumbsup"}
        yield SyntheticEvent(
            stream=stream,
            offset=start_offset + i,
            kind="reaction_added",
            ts=target_ts,
            payload=payload,
        )


def reaction_removed_events(
    channel_id: str,
    count: int,
    *,
    start_offset: int = 1,
    start_index: int = 0,
) -> Iterator[SyntheticEvent]:
    """A run of `reaction_removed` events (v2: not yet emitted by the slurper)."""
    stream = f"channel:{channel_id}"
    for i in range(count):
        index = start_index + i
        target_ts = synthetic_ts(index)
        payload: JsonObject = {"target_ts": target_ts, "user": f"U{index:04d}", "emoji": "thumbsup"}
        yield SyntheticEvent(
            stream=stream,
            offset=start_offset + i,
            kind="reaction_removed",
            ts=target_ts,
            payload=payload,
        )
