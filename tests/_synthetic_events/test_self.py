"""Self-tests for synthetic event generators."""

from __future__ import annotations

from slack_fuse_server.wire.frames import EventFrame, FrameAdapter
from tests._synthetic_events import (
    SyntheticEvent,
    channel_added_events,
    channel_archived_events,
    channel_renamed_events,
    message_changed_events,
    message_deleted_events,
    reaction_added_events,
    reaction_removed_events,
    user_added_events,
    user_renamed_events,
)


def _assert_event_frames(events: list[SyntheticEvent]) -> None:
    assert events
    for event in events:
        restored = FrameAdapter.validate_python(event.to_frame().model_dump())
        assert isinstance(restored, EventFrame)
        assert restored.stream == event.stream
        assert restored.offset == event.offset
        assert restored.kind == event.kind
        assert restored.ts == event.ts
        assert restored.payload == event.payload


def test_new_generators_emit_eventframe_shaped_events() -> None:
    edits = list(message_changed_events("C1", 2))
    deletes = list(message_deleted_events("C1", 2, start_index=10))
    added = list(channel_added_events(1))
    renamed = list(channel_renamed_events(1))
    archived = list(channel_archived_events(1))
    users_added = list(user_added_events(1))
    users_renamed = list(user_renamed_events(1))
    reactions_added = list(reaction_added_events("C1", 1))
    reactions_removed = list(reaction_removed_events("C1", 1, start_index=1))

    for chunk in (
        edits,
        deletes,
        added,
        renamed,
        archived,
        users_added,
        users_renamed,
        reactions_added,
        reactions_removed,
    ):
        _assert_event_frames(chunk)

    assert set(edits[0].payload) >= {"message", "previous_ts"}
    assert set(deletes[0].payload) >= {"deleted_ts", "previous_message"}
    assert set(added[0].payload) >= {"id", "name", "is_member"}
    assert set(renamed[0].payload) == {"channel_id", "new_name"}
    assert set(archived[0].payload) == {"channel_id"}
    assert set(users_added[0].payload) >= {"id", "profile"}
    assert set(users_renamed[0].payload) == {"user_id", "new_display_name"}
    assert set(reactions_added[0].payload) == {"target_ts", "user", "emoji"}
    assert set(reactions_removed[0].payload) == {"target_ts", "user", "emoji"}
