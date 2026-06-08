"""Trio WebSocket server for the slack-fuse event-stream wire protocol."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import NoReturn, Protocol

import trio
from pydantic import ValidationError
from trio_websocket import ConnectionClosed, WebSocketConnection, WebSocketRequest, WebSocketServer, serve_websocket

from slack_fuse_server.wire.frames import (
    CaughtUpFrame,
    ErrorCode,
    ErrorFrame,
    EventFrame,
    FrameAdapter,
    PingFrame,
    PongFrame,
    SubscribeFrame,
)
from slack_fuse_server.wire.subscriptions import ConnectionSubscriptions
from slack_fuse_server.wire.tail import DEFAULT_MAX_REPLAY_EVENTS, EventTailer

_DEFAULT_HEARTBEAT_INTERVAL_S = 30.0
_DEFAULT_CLIENT_TIMEOUT_S = 90.0
_SECRET_HEADER = b"x-slack-fuse-secret"
_AUTHORIZATION_HEADER = b"authorization"


class _OutgoingFrame(Protocol):
    def model_dump_json(self) -> str: ...


@dataclass(frozen=True, slots=True)
class ListenAddress:
    host: str
    port: int


@dataclass(frozen=True, slots=True)
class WireServerOptions:
    max_replay_events: int = DEFAULT_MAX_REPLAY_EVENTS
    heartbeat_interval_s: float = _DEFAULT_HEARTBEAT_INTERVAL_S
    client_timeout_s: float = _DEFAULT_CLIENT_TIMEOUT_S


@dataclass(frozen=True, slots=True)
class ConnectionInfo:
    """A live WS connection's `/metrics` snapshot (see `MetricsAggregator`)."""

    client_id: str
    connected_since: datetime
    subscriptions: int


class WireServer:
    """Accepts client subscriptions and streams event-log frames over `/ws`."""

    def __init__(
        self,
        database_url: str,
        *,
        host: str = "127.0.0.1",
        port: int = 8765,
        shared_secret: str | None = None,
        options: WireServerOptions | None = None,
    ) -> None:
        options = options or WireServerOptions()
        self._host = host
        self._port = port
        self._shared_secret = shared_secret
        self._tailer = EventTailer(database_url, max_replay_events=options.max_replay_events)
        self._heartbeat_interval_s = options.heartbeat_interval_s
        self._client_timeout_s = options.client_timeout_s
        # Live connections, keyed by a per-server monotonic id, for `/metrics`.
        self._connections: dict[int, _ConnectionHandler] = {}
        self._next_conn_id = 0

    async def serve(
        self,
        *,
        task_status: trio.TaskStatus[WebSocketServer] = trio.TASK_STATUS_IGNORED,
    ) -> NoReturn:
        await serve_websocket(
            self.handle_request,
            self._host,
            self._port,
            None,
            task_status=task_status,
        )

    def connection_infos(self) -> list[ConnectionInfo]:
        """Snapshot of live WS connections. Read from the trio loop (no locking)."""
        return [
            ConnectionInfo(
                client_id=handler.client_id,
                connected_since=handler.connected_since,
                subscriptions=handler.subscription_count,
            )
            for handler in self._connections.values()
        ]

    async def handle_request(self, request: WebSocketRequest) -> None:
        """Accept (or reject) one WS handshake and run the connection to close.

        Public so the same-port dispatch (`slack_fuse_server.dispatch`) can drive
        a handshake it peeked itself, not only the standalone `serve()` loop.
        """
        if request.path != "/ws":
            await request.reject(404, body=b"not found")
            return

        ws = await request.accept()
        if not _is_authorized(request.headers, self._shared_secret):
            await ws.send_message(ErrorFrame(code=ErrorCode.AUTH_FAILED).model_dump_json())
            await ws.aclose(1008, "auth failed")
            return

        conn_id = self._next_conn_id
        self._next_conn_id += 1
        handler = _ConnectionHandler(
            ws,
            self._tailer,
            client_id=str(conn_id),
            heartbeat_interval_s=self._heartbeat_interval_s,
            client_timeout_s=self._client_timeout_s,
        )
        self._connections[conn_id] = handler
        try:
            await handler.run()
        finally:
            self._connections.pop(conn_id, None)


class _ConnectionHandler:
    def __init__(
        self,
        ws: WebSocketConnection,
        tailer: EventTailer,
        *,
        client_id: str,
        heartbeat_interval_s: float,
        client_timeout_s: float,
    ) -> None:
        self._ws = ws
        self._tailer = tailer
        self._heartbeat_interval_s = heartbeat_interval_s
        self._client_timeout_s = client_timeout_s
        self._subscriptions = ConnectionSubscriptions()
        self._send_lock = trio.Lock()
        self._tail_lock = trio.Lock()
        self.client_id = client_id
        self.connected_since = datetime.now(UTC)

    @property
    def subscription_count(self) -> int:
        return self._subscriptions.count()

    async def run(self) -> None:
        try:
            async with trio.open_nursery() as nursery:
                nursery.start_soon(self._heartbeat_loop)
                nursery.start_soon(self._live_tail_loop)
                await self._receive_loop(nursery)
                nursery.cancel_scope.cancel()
        finally:
            await self._ws.aclose()

    async def _receive_loop(self, nursery: trio.Nursery) -> None:
        while True:
            message = await self._receive_message()
            if message is None:
                return
            try:
                frame = FrameAdapter.validate_json(message)
            except ValidationError:
                await self._ws.aclose(1003, "malformed frame")
                return

            if isinstance(frame, SubscribeFrame):
                nursery.start_soon(self._handle_subscribe, frame)
            elif isinstance(frame, PingFrame):
                await self._send_frame(PongFrame())
            elif isinstance(frame, PongFrame):
                continue
            else:
                await self._ws.aclose(1003, "client sent server-only frame")
                return

    async def _receive_message(self) -> str | bytes | None:
        message: str | bytes | None = None
        with trio.move_on_after(self._client_timeout_s) as cancel_scope:
            try:
                message = await self._ws.get_message()
            except ConnectionClosed:
                return None
        if cancel_scope.cancelled_caught:
            await self._ws.aclose(1001, "heartbeat timeout")
            return None
        return message

    async def _handle_subscribe(self, frame: SubscribeFrame) -> None:
        if frame.since < 0:
            await self._ws.aclose(1003, "negative since")
            return

        head_offset = await self._tailer.get_head_offset(frame.stream)
        if head_offset is None:
            self._subscriptions.remove(frame.stream)
            await self._send_frame(ErrorFrame(code=ErrorCode.STREAM_NOT_FOUND, stream=frame.stream))
            return

        if frame.since > head_offset:
            self._subscriptions.remove(frame.stream)
            await self._send_frame(
                ErrorFrame(code=ErrorCode.SINCE_TOO_HIGH, stream=frame.stream, head_offset=head_offset)
            )
            return

        if self._tailer.replay_is_too_old(frame.since, head_offset):
            self._subscriptions.remove(frame.stream)
            await self._send_frame(
                ErrorFrame(code=ErrorCode.SNAPSHOT_REQUIRED, stream=frame.stream, head_offset=head_offset)
            )
            return

        subscription = self._subscriptions.subscribe(frame.stream, frame.since)
        async for event in self._tailer.iter_events_after(frame.stream, frame.since, through=head_offset):
            if not self._subscriptions.is_current(frame.stream, subscription.generation):
                return
            await self._send_event(event, subscription.generation)

        if not self._subscriptions.is_current(frame.stream, subscription.generation):
            return
        await self._send_frame(CaughtUpFrame(stream=frame.stream, head_offset=head_offset))
        pending = self._subscriptions.mark_caught_up(frame.stream, head_offset)
        if pending:
            await self._drain_live_stream(frame.stream)
        else:
            # Covers inserts committed between the head read and caught_up send.
            await self._drain_live_stream(frame.stream)

    async def _live_tail_loop(self) -> None:
        try:
            async for stream in self._tailer.listen():
                self._subscriptions.mark_live_pending(stream)
                for subscription in self._subscriptions.caught_up_streams(stream):
                    await self._drain_live_stream(subscription.stream)
        except ConnectionClosed:
            return

    async def _drain_live_stream(self, stream: str) -> None:
        async with self._tail_lock:
            subscription = self._subscriptions.get(stream)
            if subscription is None or not subscription.caught_up:
                return
            generation = subscription.generation
            since = subscription.last_sent_offset
            async for event in self._tailer.iter_events_after(stream, since):
                if not self._subscriptions.is_current(stream, generation):
                    return
                await self._send_event(event, generation)

    async def _send_event(self, event: EventFrame, generation: int) -> None:
        if not self._subscriptions.is_current(event.stream, generation):
            return
        await self._send_frame(event)
        self._subscriptions.mark_sent(event.stream, event.offset)

    async def _heartbeat_loop(self) -> None:
        while True:
            await trio.sleep(self._heartbeat_interval_s)
            await self._send_frame(PingFrame())

    async def _send_frame(self, frame: _OutgoingFrame) -> None:
        async with self._send_lock:
            await self._ws.send_message(frame.model_dump_json())


def parse_listen_addr(listen_addr: str) -> ListenAddress:
    host, separator, port_text = listen_addr.rpartition(":")
    if not separator or not host or not port_text:
        raise ValueError(f"listen_addr must be host:port, got {listen_addr!r}")
    return ListenAddress(host=host, port=int(port_text))


async def serve_wire_server(
    database_url: str,
    listen_addr: str,
    *,
    shared_secret: str | None = None,
    task_status: trio.TaskStatus[WebSocketServer] = trio.TASK_STATUS_IGNORED,
) -> NoReturn:
    address = parse_listen_addr(listen_addr)
    server = WireServer(database_url, host=address.host, port=address.port, shared_secret=shared_secret)
    await server.serve(task_status=task_status)


def _is_authorized(headers: Sequence[tuple[bytes, bytes]], shared_secret: str | None) -> bool:
    if not shared_secret:
        return True
    expected_direct = shared_secret.encode()
    expected_bearer = f"Bearer {shared_secret}".encode()
    for name, value in headers:
        lowered = name.lower()
        if lowered == _SECRET_HEADER and value == expected_direct:
            return True
        if lowered == _AUTHORIZATION_HEADER and value == expected_bearer:
            return True
    return False
