# pyright: reportPrivateUsage=false
"""Tests for socket-mode envelope parsing and SlackStore.apply_event dispatch.

Covers the path from a raw Slack Socket Mode frame → validated Pydantic
envelope → per-key event log, without any network or FUSE moving parts.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from datetime import datetime

import pytest

from slack_fuse import disk_cache
from slack_fuse.api import SlackClient
from slack_fuse.events import (
    DayAppend,
    DayBumpParent,
    DayDelete,
    DayReplace,
    ThreadAppend,
    ThreadDelete,
    ThreadReplace,
)
from slack_fuse.models import SocketEnvelope, SocketEventPayload
from slack_fuse.store import SlackStore
from slack_fuse.user_cache import UserCache

from .stubs import (
    stub_get_channel_list,
    stub_get_huddle_index,
    stub_get_known_dates,
    stub_load_from_disk,
    stub_put_known_dates,
)


@pytest.fixture(autouse=True)
def disable_disk_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(disk_cache, "get_channel_list", stub_get_channel_list)
    monkeypatch.setattr(disk_cache, "get_huddle_index", stub_get_huddle_index)
    monkeypatch.setattr(disk_cache, "get_known_dates", stub_get_known_dates)
    monkeypatch.setattr(disk_cache, "put_known_dates", stub_put_known_dates)


@pytest.fixture
def fresh_store(monkeypatch: pytest.MonkeyPatch) -> Iterator[SlackStore]:
    monkeypatch.setattr(UserCache, "_load_from_disk", stub_load_from_disk)
    client = SlackClient(token="xoxp-fake")
    users = UserCache(client.http)
    yield SlackStore(client=client, users=users)


def _today_ts() -> str:
    """Slack ts (seconds since epoch) that falls within today's local date."""
    return f"{datetime.now().timestamp():.6f}"


def _today_date() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d")


# === SocketEnvelope parsing ===


def test_envelope_hello_parses() -> None:
    env = SocketEnvelope.model_validate({
        "type": "hello",
        "num_connections": 1,
        "debug_info": {"host": "applink-1"},
    })
    assert env.type == "hello"
    assert env.num_connections == 1
    assert env.envelope_id is None
    assert env.payload is None


def test_envelope_disconnect_with_reason() -> None:
    env = SocketEnvelope.model_validate({
        "type": "disconnect",
        "reason": "refresh_requested",
    })
    assert env.type == "disconnect"
    assert env.reason == "refresh_requested"


def test_envelope_events_api_round_trip() -> None:
    env = SocketEnvelope.model_validate({
        "type": "events_api",
        "envelope_id": "env-1",
        "payload": {
            "event": {
                "type": "message",
                "channel": "C1",
                "channel_type": "channel",
                "ts": "1700000001.000001",
                "user": "U1",
                "text": "hello",
            }
        },
    })
    assert env.envelope_id == "env-1"
    assert env.payload is not None
    inner = env.payload.event
    assert inner.type == "message"
    assert inner.channel == "C1"
    assert inner.ts == "1700000001.000001"


def test_envelope_channel_created_flattens_nested_channel_obj() -> None:
    """`channel_created` wire shape nests the channel — we only care about the id."""
    env = SocketEnvelope.model_validate({
        "type": "events_api",
        "envelope_id": "env-2",
        "payload": {
            "event": {
                "type": "channel_created",
                "channel": {"id": "C77", "name": "new-channel"},
            }
        },
    })
    assert env.payload is not None
    assert env.payload.event.channel == "C77"


def test_envelope_from_raw_json_bytes() -> None:
    raw = b'{"type": "hello", "num_connections": 2}'
    env = SocketEnvelope.model_validate_json(raw)
    assert env.type == "hello"


# === apply_event: message events ===


def test_apply_event_new_top_level_message_appends_to_day_log(
    fresh_store: SlackStore,
) -> None:
    ts = _today_ts()
    event = SocketEventPayload.model_validate({
        "type": "message",
        "channel": "C1",
        "ts": ts,
        "user": "U1",
        "text": "hi",
    })
    fresh_store.apply_event(event)
    log = fresh_store._day_events["C1", _today_date()]
    assert len(log) == 1
    assert isinstance(log[0], DayAppend)
    assert log[0].message.ts == ts
    assert log[0].message.text == "hi"
    # Date is marked known for the channel
    assert _today_date() in fresh_store._known_dates["C1"]


def test_apply_event_threaded_reply_adds_thread_append_and_day_bump(
    fresh_store: SlackStore,
) -> None:
    parent_ts = _today_ts()
    reply_ts = f"{float(parent_ts) + 5:.6f}"
    event = SocketEventPayload.model_validate({
        "type": "message",
        "channel": "C1",
        "ts": reply_ts,
        "user": "U2",
        "text": "reply",
        "thread_ts": parent_ts,
    })
    fresh_store.apply_event(event)

    thread_log = fresh_store._thread_events["C1", parent_ts]
    assert len(thread_log) == 1
    assert isinstance(thread_log[0], ThreadAppend)
    assert thread_log[0].message.ts == reply_ts

    day_log = fresh_store._day_events["C1", _today_date()]
    assert len(day_log) == 1
    bump = day_log[0]
    assert isinstance(bump, DayBumpParent)
    assert bump.parent_ts == parent_ts
    assert bump.delta_count == 1
    assert bump.latest_reply == reply_ts


def test_apply_event_thread_broadcast_also_appends_to_day(
    fresh_store: SlackStore,
) -> None:
    parent_ts = _today_ts()
    reply_ts = f"{float(parent_ts) + 5:.6f}"
    event = SocketEventPayload.model_validate({
        "type": "message",
        "subtype": "thread_broadcast",
        "channel": "C1",
        "ts": reply_ts,
        "user": "U2",
        "text": "broadcast reply",
        "thread_ts": parent_ts,
    })
    fresh_store.apply_event(event)
    day_log = fresh_store._day_events["C1", _today_date()]
    # One bump (reply) + one append (broadcast to channel)
    assert len(day_log) == 2
    kinds = {type(e).__name__ for e in day_log}
    assert kinds == {"DayBumpParent", "DayAppend"}


def test_apply_event_message_changed_top_level_replaces(
    fresh_store: SlackStore,
) -> None:
    ts = _today_ts()
    event = SocketEventPayload.model_validate({
        "type": "message",
        "subtype": "message_changed",
        "channel": "C1",
        "message": {"ts": ts, "user": "U1", "text": "edited"},
    })
    fresh_store.apply_event(event)
    log = fresh_store._day_events["C1", _today_date()]
    assert len(log) == 1
    assert isinstance(log[0], DayReplace)
    assert log[0].message.text == "edited"


def test_apply_event_message_changed_in_thread_replaces_in_thread_log(
    fresh_store: SlackStore,
) -> None:
    parent_ts = _today_ts()
    reply_ts = f"{float(parent_ts) + 10:.6f}"
    event = SocketEventPayload.model_validate({
        "type": "message",
        "subtype": "message_changed",
        "channel": "C1",
        "message": {
            "ts": reply_ts,
            "user": "U2",
            "text": "edited reply",
            "thread_ts": parent_ts,
        },
    })
    fresh_store.apply_event(event)
    thread_log = fresh_store._thread_events["C1", parent_ts]
    assert len(thread_log) == 1
    assert isinstance(thread_log[0], ThreadReplace)
    assert thread_log[0].message.text == "edited reply"


def test_apply_event_message_deleted_top_level_drops_by_ts(
    fresh_store: SlackStore,
) -> None:
    ts = _today_ts()
    event = SocketEventPayload.model_validate({
        "type": "message",
        "subtype": "message_deleted",
        "channel": "C1",
        "deleted_ts": ts,
        "previous_message": {"ts": ts, "user": "U1", "text": "gone"},
    })
    fresh_store.apply_event(event)
    log = fresh_store._day_events["C1", _today_date()]
    assert len(log) == 1
    assert isinstance(log[0], DayDelete)
    assert log[0].ts == ts


def test_apply_event_message_deleted_in_thread_drops_reply_and_bumps_parent(
    fresh_store: SlackStore,
) -> None:
    parent_ts = _today_ts()
    reply_ts = f"{float(parent_ts) + 5:.6f}"
    event = SocketEventPayload.model_validate({
        "type": "message",
        "subtype": "message_deleted",
        "channel": "C1",
        "deleted_ts": reply_ts,
        "previous_message": {
            "ts": reply_ts,
            "user": "U2",
            "text": "was a reply",
            "thread_ts": parent_ts,
        },
    })
    fresh_store.apply_event(event)
    thread_log = fresh_store._thread_events["C1", parent_ts]
    assert len(thread_log) == 1
    assert isinstance(thread_log[0], ThreadDelete)

    day_log = fresh_store._day_events["C1", _today_date()]
    assert len(day_log) == 1
    bump = day_log[0]
    assert isinstance(bump, DayBumpParent)
    assert bump.delta_count == -1
    assert bump.latest_reply is None


def test_apply_event_ignores_message_with_empty_channel(
    fresh_store: SlackStore,
) -> None:
    event = SocketEventPayload.model_validate({
        "type": "message",
        "channel": "",
        "ts": _today_ts(),
        "user": "U1",
    })
    fresh_store.apply_event(event)
    assert not fresh_store._day_events
    assert not fresh_store._thread_events


# === apply_event: channel-list invalidation ===


@pytest.mark.parametrize(
    "event_type",
    [
        "channel_created",
        "channel_rename",
        "channel_archive",
        "channel_unarchive",
        "channel_deleted",
        "channel_left",
        "member_joined_channel",
        "member_left_channel",
        "group_archive",
        "group_unarchive",
        "group_rename",
        "group_deleted",
        "im_created",
    ],
)
def test_apply_event_structural_event_invalidates_channel_list(fresh_store: SlackStore, event_type: str) -> None:
    fresh_store._channel_list_time = time.monotonic()
    event = SocketEventPayload.model_validate({"type": event_type, "channel": "C1"})
    fresh_store.apply_event(event)
    assert fresh_store._channel_list_time <= 0.0


def test_apply_event_unrelated_event_type_is_noop(fresh_store: SlackStore) -> None:
    prior = time.monotonic()
    fresh_store._channel_list_time = prior
    event = SocketEventPayload.model_validate({"type": "reaction_added", "channel": "C1"})
    fresh_store.apply_event(event)
    assert fresh_store._channel_list_time >= prior
    assert not fresh_store._day_events


# === flush_event_logs ===


def test_flush_event_logs_clears_logs_and_day_cache_for_affected_keys(
    fresh_store: SlackStore,
) -> None:
    ts = _today_ts()
    fresh_store.apply_event(
        SocketEventPayload.model_validate({
            "type": "message",
            "channel": "C1",
            "ts": ts,
            "user": "U1",
        })
    )
    assert fresh_store._day_events
    fresh_store.flush_event_logs()
    assert not fresh_store._day_events
    assert not fresh_store._thread_events
