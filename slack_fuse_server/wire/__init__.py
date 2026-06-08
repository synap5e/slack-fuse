"""WebSocket wire-protocol models for the event stream.

See RFC §Wire protocol. Frames are JSON-encoded, one frame per WebSocket
message, discriminated on the `type` field.
"""

from __future__ import annotations

from slack_fuse_server.wire.frames import (
    CaughtUpFrame,
    ErrorCode,
    ErrorFrame,
    EventFrame,
    Frame,
    FrameAdapter,
    PingFrame,
    PongFrame,
    SnapshotAtFrame,
    SubscribeFrame,
)
from slack_fuse_server.wire.server import (
    ListenAddress,
    WireServer,
    WireServerOptions,
    parse_listen_addr,
    serve_wire_server,
)
from slack_fuse_server.wire.subscriptions import ConnectionSubscriptions, Subscription
from slack_fuse_server.wire.tail import EventTailer

__all__ = [
    "CaughtUpFrame",
    "ConnectionSubscriptions",
    "ErrorCode",
    "ErrorFrame",
    "EventFrame",
    "EventTailer",
    "Frame",
    "FrameAdapter",
    "ListenAddress",
    "PingFrame",
    "PongFrame",
    "SnapshotAtFrame",
    "SubscribeFrame",
    "Subscription",
    "WireServer",
    "WireServerOptions",
    "parse_listen_addr",
    "serve_wire_server",
]
