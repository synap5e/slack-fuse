"""WebSocket subscriber + per-stream dispatcher.

Per RFC §Wire protocol. One WebSocket connection per client. Subscribe frames
are sent for every known stream; incoming frames are routed by stream-id into
per-stream applier queues so a slow apply on one stream cannot block live
events on another (see `per_stream.py`).

This module is the boundary between the wire protocol and the local DB. It:

- speaks the frame protocol with `slack_fuse_server.wire.frames`
- maintains per-stream `StreamApplier`s (queue + worker)
- handles `snapshot_at` redirects by deferring the fetch to a side task that
  uses `snapshot_fetch.fetch_and_apply_snapshot`
- bumps `connection_state.last_frame_at` on every received frame so the FUSE
  read-side trailer logic sees the connection is live

The receive loop never blocks on per-stream backpressure (review P1-E). Each
`StreamApplier`'s queue is unbounded and `enqueue` is non-blocking, so routing a
frame for a saturated stream A can never stop the loop from reading stream B's
frames off the socket. The appliers share a bounded `ConnectionPool` rather than
opening one DB connection per stream (review P0-A), so subscribing to a
320-channel workspace no longer exhausts a stock local Postgres
`max_connections`.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Final

import httpx
import trio
import trio_websocket
from psycopg import Connection
from psycopg.rows import TupleRow
from pydantic import ValidationError

from slack_fuse.projector.apply import InvalidationSink, NullInvalidationSink
from slack_fuse.projector.cursor import read_cursor
from slack_fuse.projector.per_stream import ConnectionFactory, StreamApplier
from slack_fuse.projector.pool import DEFAULT_POOL_SIZE, ConnectionPool
from slack_fuse.projector.snapshot_fetch import SnapshotFetchError, SnapshotRedirect, fetch_and_apply_snapshot
from slack_fuse_server.wire.frames import (
    CaughtUpFrame,
    ErrorFrame,
    EventFrame,
    FrameAdapter,
    PingFrame,
    PongFrame,
    SnapshotAtFrame,
    SubscribeFrame,
)

log = logging.getLogger(__name__)


#: Singleton streams every client subscribes to at startup. Per-channel streams
#: are added as `channel_added` events land on `channel-list`.
SINGLETON_STREAMS: Final[tuple[str, ...]] = ("channel-list", "users", "slurper-health")


#: Default heartbeat. The server pings every 30s and treats us as dead at 90s
#: silent; we mirror that.
_HEARTBEAT_INTERVAL_S: Final = 30.0
_CONNECTION_TIMEOUT_S: Final = 90.0


@dataclass(frozen=True, slots=True)
class WSClientOptions:
    server_url: str
    shared_secret: str | None = None
    base_http_url: str | None = None  # for snapshot URL resolution; None ⇒ derive from server_url
    pool_size: int = DEFAULT_POOL_SIZE  # bounded applier connection pool (review P0-A)


class WSClient:
    """Trio WebSocket subscriber. Routes frames into per-stream appliers."""

    def __init__(  # noqa: PLR0913  (keyword-only injection points)
        self,
        options: WSClientOptions,
        connection_factory: ConnectionFactory,
        state_conn: Connection[TupleRow],
        *,
        sink: InvalidationSink | None = None,
        http_client: httpx.AsyncClient | None = None,
        always_blocked: frozenset[str] = frozenset(),
    ) -> None:
        self._options = options
        # Bounded pool the appliers borrow from (review P0-A): ~pool_size
        # connections regardless of how many channel streams are subscribed.
        self._pool = ConnectionPool(connection_factory, max_size=options.pool_size)
        # The `state_conn` is used for `connection_state` bookkeeping and
        # one-off cursor reads at startup — never for chunk writes.
        self._state_conn = state_conn
        self._sink: InvalidationSink = sink if sink is not None else NullInvalidationSink()
        self._http: httpx.AsyncClient | None = http_client
        self._always_blocked = always_blocked
        self._appliers: dict[str, StreamApplier] = {}
        self._ws: trio_websocket.WebSocketConnection | None = None
        self._send_lock = trio.Lock()
        self._nursery: trio.Nursery | None = None

    async def run(
        self,
        initial_streams: Iterable[str] | None = None,
        *,
        task_status: trio.TaskStatus[None] = trio.TASK_STATUS_IGNORED,
    ) -> None:
        """Connect, subscribe, and process frames until cancelled."""
        owned_http: httpx.AsyncClient | None = None
        if self._http is None:
            base = self._options.base_http_url or _derive_http_base(self._options.server_url)
            owned_http = httpx.AsyncClient(base_url=base, timeout=60.0)
            self._http = owned_http
        try:
            async with trio.open_nursery() as nursery:
                self._nursery = nursery
                streams = list(initial_streams) if initial_streams is not None else list(SINGLETON_STREAMS)
                for stream in streams:
                    await self._ensure_applier(stream)
                headers = _build_headers(self._options.shared_secret)
                async with trio_websocket.open_websocket_url(self._options.server_url, extra_headers=headers) as ws:
                    self._ws = ws
                    for stream in streams:
                        since = await trio.to_thread.run_sync(self._read_cursor_sync, stream)
                        await self._send_frame(SubscribeFrame(stream=stream, since=since))
                    nursery.start_soon(self._heartbeat_loop)
                    task_status.started()
                    await self._receive_loop()
                    nursery.cancel_scope.cancel()
        finally:
            self._nursery = None
            self._ws = None
            for applier in self._appliers.values():
                await applier.close()
            self._appliers.clear()
            await self._pool.aclose()
            if owned_http is not None:
                await owned_http.aclose()
                self._http = None

    # === connection bookkeeping ===

    def _read_cursor_sync(self, stream: str) -> int:
        with self._state_conn.cursor() as cur:
            return read_cursor(cur, stream)

    def _bump_last_frame_sync(self) -> None:
        with self._state_conn.cursor() as cur:
            cur.execute("UPDATE connection_state SET last_frame_at = now() WHERE id = 1")

    # === appliers ===

    def _make_applier(self, stream: str) -> StreamApplier:
        """Construct a `StreamApplier` for `stream`. Test seam: subclasses
        override this to inject a `before_apply` hook (e.g. the HoL test)."""
        return StreamApplier(stream, self._pool, self._sink, always_blocked=self._always_blocked)

    async def _ensure_applier(self, stream: str) -> StreamApplier:
        existing = self._appliers.get(stream)
        if existing is not None:
            return existing
        nursery = self._nursery
        if nursery is None:  # pragma: no cover - only invoked inside run()
            msg = "WSClient._ensure_applier called outside run() nursery"
            raise RuntimeError(msg)
        applier = self._make_applier(stream)
        self._appliers[stream] = applier
        await nursery.start(applier.serve)
        return applier

    # === wire IO ===

    async def _receive_loop(self) -> None:
        ws = self._ws
        if ws is None:  # pragma: no cover - run() sets this before calling
            return
        while True:
            try:
                message = await ws.get_message()
            except trio_websocket.ConnectionClosed:
                return
            await trio.to_thread.run_sync(self._bump_last_frame_sync)
            try:
                frame = FrameAdapter.validate_json(message)
            except ValidationError:
                log.warning("ws: malformed frame; closing")
                await ws.aclose(1003, "malformed frame")
                return
            await self._dispatch_frame(frame)

    async def _dispatch_frame(  # noqa: C901 - dispatch hub
        self,
        frame: EventFrame | CaughtUpFrame | SnapshotAtFrame | ErrorFrame | PingFrame | PongFrame | SubscribeFrame,
    ) -> None:
        if isinstance(frame, EventFrame):
            applier = await self._ensure_applier(frame.stream)
            await applier.enqueue(frame)
            # On `channel_added`, eagerly subscribe to the new channel's stream.
            if frame.stream == "channel-list" and frame.kind == "channel_added":
                channel_id = frame.payload.get("id")
                if isinstance(channel_id, str):
                    new_stream = f"channel:{channel_id}"
                    if new_stream not in self._appliers:
                        await self._ensure_applier(new_stream)
                        since = await trio.to_thread.run_sync(self._read_cursor_sync, new_stream)
                        await self._send_frame(SubscribeFrame(stream=new_stream, since=since))
            return
        if isinstance(frame, CaughtUpFrame):
            applier = await self._ensure_applier(frame.stream)
            await applier.enqueue(frame)
            return
        if isinstance(frame, SnapshotAtFrame):
            nursery = self._nursery
            if nursery is None:  # pragma: no cover
                return
            nursery.start_soon(self._handle_snapshot, frame)
            return
        if isinstance(frame, PingFrame):
            await self._send_frame(PongFrame())
            return
        if isinstance(frame, PongFrame):
            return
        if isinstance(frame, ErrorFrame):
            log.warning("ws: server error %s stream=%s head=%s", frame.code, frame.stream, frame.head_offset)
            return
        log.warning("ws: unexpected frame %r", type(frame).__name__)

    async def _handle_snapshot(self, frame: SnapshotAtFrame) -> None:
        """Fetch + apply a snapshot, then re-subscribe at the new cursor."""
        http = self._http
        if http is None:  # pragma: no cover
            return
        # Borrow a pooled connection for the snapshot apply (review P0-A). The
        # apply runs one big TX on an autocommit connection (it does not flip
        # autocommit), so a pooled connection is safe; bounding it through the
        # pool keeps the total connection budget intact even mid-snapshot.
        snapshot_conn = await self._pool.acquire()
        discard = False
        try:
            redirect = SnapshotRedirect(stream=frame.stream, at_offset=frame.at, url=frame.url)
            try:
                await fetch_and_apply_snapshot(
                    http,
                    snapshot_conn,
                    redirect,
                    base_url=self._options.base_http_url,
                    sink=self._sink,
                )
            except (httpx.HTTPError, SnapshotFetchError, ValueError) as exc:
                # ValueError: a stale server offering a snapshot for a stream the
                # client cannot replace-apply (e.g. a singleton stream — review
                # P0-C). Log + leave the stream subscribed via the replay path
                # rather than tearing the client down.
                log.warning("ws: snapshot fetch for %s failed: %s", frame.stream, exc)
                discard = True
                return
        finally:
            await self._pool.release(snapshot_conn, discard=discard)
        # Resume the WS subscription from the snapshot offset.
        await self._send_frame(SubscribeFrame(stream=frame.stream, since=frame.at))

    async def _send_frame(self, frame: object) -> None:
        ws = self._ws
        if ws is None:  # pragma: no cover
            return
        payload = _frame_to_json(frame)
        async with self._send_lock:
            await ws.send_message(payload)

    async def _heartbeat_loop(self) -> None:
        ws = self._ws
        if ws is None:  # pragma: no cover
            return
        while True:
            await trio.sleep(_HEARTBEAT_INTERVAL_S)
            try:
                await self._send_frame(PingFrame())
            except trio_websocket.ConnectionClosed:
                return


def _build_headers(shared_secret: str | None) -> list[tuple[bytes, bytes]]:
    headers: list[tuple[bytes, bytes]] = []
    if shared_secret:
        headers.append((b"x-slack-fuse-secret", shared_secret.encode()))
    return headers


def derive_http_base(ws_url: str) -> str:
    """Best-effort: `ws://host:port` → `http://host:port`.

    Public for reuse by the originals fetcher (``slack_fuse.__main__`` wires
    a sync httpx client for the ``channel.original.md`` ghost file against
    the same server origin).
    """
    if ws_url.startswith("wss://"):
        return "https://" + ws_url.removeprefix("wss://").split("/", maxsplit=1)[0]
    if ws_url.startswith("ws://"):
        return "http://" + ws_url.removeprefix("ws://").split("/", maxsplit=1)[0]
    return ws_url


# Backwards-compat alias for the previous private name.
_derive_http_base = derive_http_base


def _frame_to_json(frame: object) -> str:
    """Frames are Pydantic models with `model_dump_json()`; helper exists for
    test injection of bare objects."""
    if hasattr(frame, "model_dump_json"):
        return frame.model_dump_json()  # type: ignore[no-any-return,attr-defined]
    msg = f"cannot serialise frame of type {type(frame).__name__}"
    raise TypeError(msg)


# Keep `_CONNECTION_TIMEOUT_S` referenced so flake8 doesn't flag it; reserved
# for a future receive-timeout patch (the server already enforces 90s silent).
_ = _CONNECTION_TIMEOUT_S
