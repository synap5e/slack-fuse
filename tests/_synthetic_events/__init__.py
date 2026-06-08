"""Deterministic synthetic event-stream generator.

Produces `SyntheticEvent` records (`stream`, `offset`, `kind`, `ts`, `payload`)
matching the wire `EventFrame` shape, so projector tests can drive chunk-write
logic without the slurper. Covers message edits/deletes, channel-list events,
users events, and reaction events (v2 placeholders). Everything is
offset/index-derived (no randomness), so streams are reproducible.
`SyntheticEvent.to_frame()` yields a real `EventFrame`.

Implementation lives in `generators.py`; re-exported here.
"""

from __future__ import annotations

from tests._synthetic_events.generators import (
    SyntheticEvent,
    channel_added_events,
    channel_archived_events,
    channel_message_events,
    channel_renamed_events,
    channel_reply_events,
    message_changed_events,
    message_deleted_events,
    reaction_added_events,
    reaction_removed_events,
    synthetic_ts,
    user_added_events,
    user_events,
    user_renamed_events,
)

__all__ = [
    "SyntheticEvent",
    "channel_added_events",
    "channel_archived_events",
    "channel_message_events",
    "channel_renamed_events",
    "channel_reply_events",
    "message_changed_events",
    "message_deleted_events",
    "reaction_added_events",
    "reaction_removed_events",
    "synthetic_ts",
    "user_added_events",
    "user_events",
    "user_renamed_events",
]
