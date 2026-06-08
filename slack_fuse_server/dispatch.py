"""Same-port HTTP + WebSocket dispatch.

The RFC mandates that `/health`, `/metrics`, and `/ws` all live behind a single
listen address ("Same process, same port"). 1B and 1C each bound their own
accept loop on a separate port; this module is the shared front door that the
integrated `slack-fuse-server` binary actually runs.

How it works: one `trio.serve_tcp` accept loop hands every connection to
`serve_connection`, which

1. *peeks* the HTTP request line + headers with `h11`, buffering the raw bytes
   it reads off the socket (`_peek`),
2. classifies it — a `GET /ws` carrying `Upgrade: websocket` is a WebSocket
   handshake; anything else is plain HTTP,
3. replays the peeked bytes into the chosen handler over a `_PrefixedStream`,
   so neither `trio_websocket` (which re-parses the upgrade from scratch) nor
   the HTTP layer (`serve_http_connection`) ever sees a truncated request.

This mirrors how hypercorn shapes its h11/wsproto dispatch without taking on the
dependency: the peek is non-destructive because every byte we consume is handed
back via the prefix stream.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import partial

import h11
import trio
from trio_websocket import wrap_server_stream

from slack_fuse_server.http.handlers import ResolvePermalinkDeps
from slack_fuse_server.http.metrics import MetricsSource
from slack_fuse_server.http.server import parse_listen_addr, serve_http_connection
from slack_fuse_server.wire.server import WireServer

_READ_CHUNK_SIZE = 16_384
_WS_PATH = "/ws"


@dataclass(frozen=True, slots=True)
class _PeekedRequest:
    method: str
    path: str
    headers: list[tuple[bytes, bytes]]


class _PrefixedStream(trio.abc.Stream):
    """A stream that replays already-read `prefix` bytes before delegating.

    Lets the dispatch consume the request line + headers to classify a
    connection, then hand the *whole* request (peeked bytes included) to the
    chosen handler as if nothing had been read.
    """

    def __init__(self, prefix: bytes, underlying: trio.SocketStream) -> None:
        self._prefix = prefix
        self._underlying = underlying

    @property
    def socket(self) -> trio.socket.SocketType:
        # trio_websocket reads `.socket` to populate WebSocketRequest.local/remote.
        return self._underlying.socket

    async def send_all(self, data: bytes | bytearray | memoryview) -> None:
        await self._underlying.send_all(data)

    async def wait_send_all_might_not_block(self) -> None:
        await self._underlying.wait_send_all_might_not_block()

    async def receive_some(self, max_bytes: int | None = None) -> bytes:
        if self._prefix:
            if max_bytes is None or max_bytes >= len(self._prefix):
                chunk = self._prefix
                self._prefix = b""
            else:
                chunk = self._prefix[:max_bytes]
                self._prefix = self._prefix[max_bytes:]
            return chunk
        return await self._underlying.receive_some(max_bytes)

    async def aclose(self) -> None:
        await self._underlying.aclose()


async def _peek(stream: trio.SocketStream) -> tuple[bytes, _PeekedRequest | None]:
    """Read just enough to parse the request line + headers, returning the raw bytes.

    The returned bytes are everything read off the socket so far; replaying them
    reproduces the request exactly. Returns `(raw, None)` when the bytes don't
    form a parseable HTTP request (malformed, or the peer closed early) — the
    caller still replays `raw` into the HTTP handler, which answers 400/closes.
    """
    conn = h11.Connection(h11.SERVER)
    raw = bytearray()
    eof = False
    while True:
        try:
            event = conn.next_event()
        except h11.RemoteProtocolError:
            return bytes(raw), None
        if event is h11.NEED_DATA:
            if eof:
                return bytes(raw), None
            chunk = await stream.receive_some(_READ_CHUNK_SIZE)
            if chunk:
                raw.extend(chunk)
                conn.receive_data(chunk)
            else:
                eof = True
                conn.receive_data(b"")
            continue
        if isinstance(event, h11.Request):
            target = event.target.decode("latin-1")
            path = target.split("?", 1)[0]
            return bytes(raw), _PeekedRequest(
                method=event.method.decode("ascii").upper(),
                path=path,
                headers=list(event.headers),
            )
        # Any other event before a Request (ConnectionClosed, paused, etc.) means
        # there's nothing routable here.
        return bytes(raw), None


def _is_websocket_upgrade(request: _PeekedRequest) -> bool:
    if request.method != "GET" or request.path != _WS_PATH:
        return False
    return any(name.lower() == b"upgrade" and b"websocket" in value.lower() for name, value in request.headers)


async def _serve_websocket(stream: trio.abc.Stream, wire_server: WireServer) -> None:
    """Complete the WS handshake on `stream` and run the connection to close.

    `wrap_server_stream` re-parses the upgrade request from the replayed prefix
    and starts the connection's reader task in our nursery; `WireServer` owns
    accept/reject and the eventual `aclose`. The outer `finally` guarantees the
    socket is closed even if the handshake itself raises.
    """
    try:
        async with trio.open_nursery() as nursery:
            request = await wrap_server_stream(nursery, stream)
            try:
                await wire_server.handle_request(request)
            finally:
                nursery.cancel_scope.cancel()
    finally:
        await stream.aclose()


async def serve_connection(
    stream: trio.SocketStream,
    *,
    wire_server: WireServer,
    metrics_source: MetricsSource,
    resolve_permalink_deps: ResolvePermalinkDeps | None = None,
) -> None:
    """Classify one accepted connection and route it to WS or HTTP."""
    raw, peeked = await _peek(stream)
    prefixed = _PrefixedStream(raw, stream)
    if peeked is not None and _is_websocket_upgrade(peeked):
        await _serve_websocket(prefixed, wire_server)
    else:
        await serve_http_connection(
            prefixed,
            metrics_source=metrics_source,
            resolve_permalink_deps=resolve_permalink_deps,
        )


async def serve_dispatch(
    *,
    listen_addr: str,
    wire_server: WireServer,
    metrics_source: MetricsSource,
    resolve_permalink_deps: ResolvePermalinkDeps | None = None,
    task_status: trio.TaskStatus[list[trio.SocketListener]] = trio.TASK_STATUS_IGNORED,
) -> None:
    """Bind `listen_addr` and serve HTTP + WS on the one port."""
    host, port = parse_listen_addr(listen_addr)
    handler = partial(
        serve_connection,
        wire_server=wire_server,
        metrics_source=metrics_source,
        resolve_permalink_deps=resolve_permalink_deps,
    )
    await trio.serve_tcp(handler, port=port, host=host, task_status=task_status)


async def serve_dispatch_on_listeners(
    listeners: list[trio.SocketListener],
    *,
    wire_server: WireServer,
    metrics_source: MetricsSource,
    resolve_permalink_deps: ResolvePermalinkDeps | None = None,
) -> None:
    """Serve on already-open listeners (tests bind port 0 and read the port back)."""
    handler = partial(
        serve_connection,
        wire_server=wire_server,
        metrics_source=metrics_source,
        resolve_permalink_deps=resolve_permalink_deps,
    )
    await trio.serve_listeners(handler, listeners)
