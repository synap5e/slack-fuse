"""Every WS frame model round-trips, and the discriminated union dispatches.

Sprint 0 contract: each frame survives `model_validate(model_dump())`
byte-exactly, and `FrameAdapter` resolves an arbitrary dumped frame back to the
right concrete model via the `type` discriminator.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

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

_FRAMES: list[Frame] = [
    SubscribeFrame(stream="channel:C0AKQ5DS0FQ", since=184523),
    SubscribeFrame(stream="users"),  # default since=0
    EventFrame(
        stream="channel:C0AKQ5DS0FQ",
        offset=184524,
        kind="message",
        ts="1779000000.000100",
        payload={"type": "message", "text": "hi", "edited": None, "reactions": [{"name": "wave", "count": 2}]},
    ),
    EventFrame(stream="users", offset=12, kind="user_added", ts=None, payload={"user_id": "U1", "display_name": "A"}),
    CaughtUpFrame(stream="channel:C0AKQ5DS0FQ", head_offset=184600),
    SnapshotAtFrame(stream="channel:C0AKQ5DS0FQ", at=184500, url="/streams/channel%3AC0AKQ5DS0FQ/snapshot?at=184500"),
    ErrorFrame(code=ErrorCode.STREAM_NOT_FOUND, stream="channel:CDELETED"),
    ErrorFrame(code=ErrorCode.SINCE_TOO_HIGH, stream="channel:C1", head_offset=184523),
    ErrorFrame(code=ErrorCode.AUTH_FAILED),
    PingFrame(),
    PongFrame(),
]


@pytest.mark.parametrize("frame", _FRAMES, ids=lambda f: f.type)
def test_concrete_model_roundtrip(frame: Frame) -> None:
    dumped = frame.model_dump()
    restored = type(frame).model_validate(dumped)
    assert restored == frame
    # And the dump is stable across a second cycle.
    assert restored.model_dump() == dumped


@pytest.mark.parametrize("frame", _FRAMES, ids=lambda f: f.type)
def test_discriminated_union_dispatch(frame: Frame) -> None:
    restored = FrameAdapter.validate_python(frame.model_dump())
    assert type(restored) is type(frame)
    assert restored == frame


def test_discriminator_is_the_type_field() -> None:
    dumped = SubscribeFrame(stream="users", since=3).model_dump()
    assert dumped["type"] == "subscribe"
    assert isinstance(FrameAdapter.validate_python(dumped), SubscribeFrame)


def test_unknown_frame_type_rejected() -> None:
    with pytest.raises(ValidationError):
        FrameAdapter.validate_python({"type": "nonsense", "stream": "x"})


def test_extra_field_rejected() -> None:
    with pytest.raises(ValidationError):
        SubscribeFrame.model_validate({"type": "subscribe", "stream": "x", "since": 0, "bogus": 1})


def test_error_codes_present() -> None:
    # The three codes the RFC requires at minimum.
    assert {c.value for c in ErrorCode} >= {"stream_not_found", "since_too_high", "auth_failed"}
