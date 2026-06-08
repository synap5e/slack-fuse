"""Deterministic synthetic event-stream generator.

Produces `SyntheticEvent` records (`stream`, `offset`, `kind`, `ts`, `payload`)
matching the wire `EventFrame` shape, so projector tests can drive chunk-write
logic without the slurper. Everything is offset/index-derived (no randomness),
so streams are reproducible. `SyntheticEvent.to_frame()` yields a real
`EventFrame`.

Implementation lives in `generators.py`; re-exported here.
"""

from __future__ import annotations

from tests._synthetic_events.generators import (
    SyntheticEvent,
    channel_message_events,
    channel_reply_events,
    synthetic_ts,
    user_events,
)

__all__ = [
    "SyntheticEvent",
    "channel_message_events",
    "channel_reply_events",
    "synthetic_ts",
    "user_events",
]
