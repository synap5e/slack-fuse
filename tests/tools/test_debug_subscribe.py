from __future__ import annotations

import pytest

from slack_fuse_server.wire.frames import CaughtUpFrame, ErrorCode, ErrorFrame, EventFrame
from tools.debug_subscribe import CliArgs, format_frame, parse_args, render_output_line


def test_parse_args_accepts_repeatable_streams_and_json_flag() -> None:
    parsed = parse_args(
        [
            "--server-url",
            "ws://localhost:8765/ws",
            "--stream",
            "channel:C123",
            "--stream",
            "users",
            "--since",
            "99",
            "--shared-secret",
            "sekrit",
            "--json",
        ]
    )
    assert parsed == CliArgs(
        server_url="ws://localhost:8765/ws",
        streams=("channel:C123", "users"),
        since=99,
        shared_secret="sekrit",
        json_output=True,
    )


def test_parse_args_rejects_negative_since() -> None:
    with pytest.raises(SystemExit):
        parse_args(
            [
                "--server-url",
                "ws://localhost:8765/ws",
                "--stream",
                "channel:C123",
                "--since",
                "-1",
            ]
        )


def test_parse_args_requires_stream() -> None:
    with pytest.raises(SystemExit):
        parse_args(["--server-url", "ws://localhost:8765/ws"])


def test_format_frame_for_event_is_stable() -> None:
    frame = EventFrame(
        stream="channel:C123",
        offset=184524,
        kind="message",
        ts="1779000000.000100",
        payload={"text": "hello", "user": "U123"},
    )
    assert (
        format_frame(frame)
        == '[event] channel:C123 offset=184524 kind=message ts=1779000000.000100 payload={"text":"hello","user":"U123"}'
    )


def test_format_frame_for_caught_up() -> None:
    frame = CaughtUpFrame(stream="channel:C123", head_offset=184600)
    assert format_frame(frame) == "[caught_up] channel:C123 head=184600"


def test_format_frame_for_error_includes_optional_fields() -> None:
    frame = ErrorFrame(code=ErrorCode.SINCE_TOO_HIGH, stream="users", head_offset=12)
    assert format_frame(frame) == "[error] code=since_too_high stream=users head_offset=12"


def test_render_output_line_returns_raw_json_when_json_mode_enabled() -> None:
    frame = CaughtUpFrame(stream="users", head_offset=12)
    raw_json = '{"type":"caught_up","stream":"users","head_offset":12}'
    assert render_output_line(frame, raw_json, json_output=True) == raw_json
