"""Debug CLI for subscribing to slack-fuse-server WebSocket streams."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import dataclass

import trio
from pydantic import ValidationError
from trio_websocket import ConnectionClosed, WebSocketConnection, open_websocket_url

from slack_fuse_server.wire.frames import (
    CaughtUpFrame,
    ErrorFrame,
    EventFrame,
    Frame,
    FrameAdapter,
    PingFrame,
    PongFrame,
    SnapshotAtFrame,
    SubscribeFrame,
)

_SECRET_HEADER = b"x-slack-fuse-secret"


@dataclass(frozen=True, slots=True)
class CliArgs:
    server_url: str
    streams: tuple[str, ...]
    since: int
    shared_secret: str | None
    json_output: bool


def _non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("since must be >= 0")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m tools.debug_subscribe",
        description="Subscribe to one or more slack-fuse-server streams and print received frames.",
    )
    parser.add_argument(
        "--server-url",
        required=True,
        help="WebSocket URL (for example: ws://localhost:8765/ws).",
    )
    parser.add_argument(
        "--stream",
        dest="streams",
        action="append",
        required=True,
        help="Stream id to subscribe to (repeat for multiple streams).",
    )
    parser.add_argument(
        "--since",
        type=_non_negative_int,
        default=0,
        help="Replay cursor offset to resume from (default: 0).",
    )
    parser.add_argument(
        "--shared-secret",
        help="Optional shared secret sent as x-slack-fuse-secret header.",
    )
    parser.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        help="Print raw frame JSON instead of formatted lines.",
    )
    return parser


def parse_args(argv: Sequence[str] | None = None) -> CliArgs:
    namespace = build_parser().parse_args(argv)
    return CliArgs(
        server_url=str(namespace.server_url),
        streams=tuple(str(stream) for stream in namespace.streams),
        since=int(namespace.since),
        shared_secret=None if namespace.shared_secret is None else str(namespace.shared_secret),
        json_output=bool(namespace.json_output),
    )


def _auth_headers(shared_secret: str | None) -> list[tuple[bytes, bytes]] | None:
    if shared_secret is None:
        return None
    return [(_SECRET_HEADER, shared_secret.encode())]


def _decode_message(message: str | bytes) -> str:
    if isinstance(message, str):
        return message
    return message.decode("utf-8")


def parse_inbound_frame(message: str | bytes) -> tuple[Frame, str]:
    raw_json = _decode_message(message)
    frame = FrameAdapter.validate_json(raw_json)
    return frame, raw_json


def _compact_json(value: object) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _format_event_frame(frame: EventFrame) -> str:
    parts = [
        "[event]",
        frame.stream,
        f"offset={frame.offset}",
        f"kind={frame.kind}",
    ]
    if frame.ts is not None:
        parts.append(f"ts={frame.ts}")
    parts.append(f"payload={_compact_json(frame.payload)}")
    return " ".join(parts)


def _format_error_frame(frame: ErrorFrame) -> str:
    parts = ["[error]", f"code={frame.code.value}"]
    if frame.stream is not None:
        parts.append(f"stream={frame.stream}")
    if frame.head_offset is not None:
        parts.append(f"head_offset={frame.head_offset}")
    return " ".join(parts)


def format_frame(frame: Frame) -> str:
    if isinstance(frame, EventFrame):
        return _format_event_frame(frame)
    if isinstance(frame, CaughtUpFrame):
        return f"[caught_up] {frame.stream} head={frame.head_offset}"
    if isinstance(frame, SnapshotAtFrame):
        return f"[snapshot_at] {frame.stream} at={frame.at} url={frame.url}"
    if isinstance(frame, ErrorFrame):
        return _format_error_frame(frame)
    if isinstance(frame, PingFrame):
        return "[ping]"
    if isinstance(frame, PongFrame):
        return "[pong]"
    if isinstance(frame, SubscribeFrame):
        return f"[subscribe] {frame.stream} since={frame.since}"
    msg = f"Unsupported frame type: {type(frame)!r}"
    raise TypeError(msg)


def render_output_line(frame: Frame, raw_json: str, *, json_output: bool) -> str:
    if json_output:
        return raw_json
    return format_frame(frame)


async def _send_subscriptions(ws: WebSocketConnection, streams: Sequence[str], since: int) -> None:
    for stream in streams:
        await ws.send_message(SubscribeFrame(stream=stream, since=since).model_dump_json())


async def run(args: CliArgs) -> None:
    async with open_websocket_url(args.server_url, extra_headers=_auth_headers(args.shared_secret)) as ws:
        await _send_subscriptions(ws, args.streams, args.since)
        while True:
            try:
                message = await ws.get_message()
            except ConnectionClosed:
                return
            try:
                frame, raw_json = parse_inbound_frame(message)
            except ValidationError as exc:
                raise ValueError("received malformed frame JSON from server") from exc

            if isinstance(frame, PingFrame):
                await ws.send_message(PongFrame().model_dump_json())
            print(render_output_line(frame, raw_json, json_output=args.json_output), flush=True)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    try:
        trio.run(run, args)
    except KeyboardInterrupt:
        return


if __name__ == "__main__":
    main()
