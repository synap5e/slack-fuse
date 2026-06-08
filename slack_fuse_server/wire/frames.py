"""Pydantic models for every WebSocket frame in the event-stream protocol.

Per RFC §Wire protocol → Frame types. JSON-encoded, one frame per WS message,
multiplexing many stream subscriptions onto one connection. The `Frame`
discriminated union dispatches on the `type` field; `FrameAdapter` validates
an arbitrary inbound frame into the right concrete model.

All frames are frozen and forbid unknown fields — this is our own protocol
(not Slack's), so strictness catches malformed frames at the boundary.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from slack_fuse_server._json import JsonObject


class ErrorCode(StrEnum):
    """Stream/connection-level error codes carried by `ErrorFrame`."""

    STREAM_NOT_FOUND = "stream_not_found"
    SINCE_TOO_HIGH = "since_too_high"
    SNAPSHOT_REQUIRED = "snapshot_required"
    AUTH_FAILED = "auth_failed"


class _Frame(BaseModel):
    """Base for all wire frames: immutable, reject unknown fields."""

    model_config = ConfigDict(frozen=True, extra="forbid")


class SubscribeFrame(_Frame):
    """Client → server: open a subscription, or resume an existing one.

    `since` is the client's last applied offset; 0 means from the beginning.
    """

    type: Literal["subscribe"] = "subscribe"
    stream: str
    since: int = 0


class EventFrame(_Frame):
    """Server → client: an individual event.

    Offsets are strictly increasing within a stream. `ts` is the Slack message
    `ts` when applicable (None for structural events that carry no message ts).
    """

    type: Literal["event"] = "event"
    stream: str
    offset: int
    kind: str
    ts: str | None = None
    payload: JsonObject = Field(default_factory=dict)


class CaughtUpFrame(_Frame):
    """Server → client: catch-up boundary marker.

    After this frame the client has seen every event up to `head_offset`;
    subsequent `event` frames on the stream are live. Informational — the
    projector applies every event identically; the trailer logic uses this to
    clear the "catching up after reconnect" degradation reason.
    """

    type: Literal["caught_up"] = "caught_up"
    stream: str
    head_offset: int


class SnapshotAtFrame(_Frame):
    """Server → client: catch-up redirect.

    Used when `since` is too far behind for cheap replay. The client fetches
    the snapshot over HTTP (`url`), applies it as one transaction, advances its
    cursor to `at`, then resumes the WS subscription from `at + 1`.
    """

    type: Literal["snapshot_at"] = "snapshot_at"
    stream: str
    at: int
    url: str


class ErrorFrame(_Frame):
    """Server → client: stream/connection-level error.

    `stream` is set for stream-scoped errors. `head_offset` accompanies
    `since_too_high` so the client can reset its cursor.
    """

    type: Literal["error"] = "error"
    code: ErrorCode
    stream: str | None = None
    head_offset: int | None = None


class PingFrame(_Frame):
    """Bidirectional heartbeat. Sent every 30s; peer is dead after 90s silent."""

    type: Literal["ping"] = "ping"


class PongFrame(_Frame):
    """Bidirectional heartbeat reply."""

    type: Literal["pong"] = "pong"


type Frame = Annotated[
    SubscribeFrame | EventFrame | CaughtUpFrame | SnapshotAtFrame | ErrorFrame | PingFrame | PongFrame,
    Field(discriminator="type"),
]

# Validates an arbitrary inbound frame dict/JSON into the right concrete model.
FrameAdapter: TypeAdapter[Frame] = TypeAdapter(Frame)
