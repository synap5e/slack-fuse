"""Minimal trio HTTP server for `/health` and `/metrics`."""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import partial

import h11
import trio
from pydantic import BaseModel

from slack_fuse_server.http.handlers import handle_health, handle_metrics
from slack_fuse_server.http.metrics import MetricsSource

_JSON_CONTENT_TYPE = "application/json"
_READ_CHUNK_SIZE = 16_384


@dataclass(frozen=True, slots=True)
class HttpResponse:
    status_code: int
    body: bytes
    content_type: str = _JSON_CONTENT_TYPE


@dataclass(frozen=True, slots=True)
class HttpRequest:
    method: str
    target: str

    @property
    def path(self) -> str:
        path, _, _query = self.target.partition("?")
        return path


def parse_listen_addr(listen_addr: str) -> tuple[str, int]:
    """Parse `host:port` or `[ipv6]:port` listen addresses."""
    if listen_addr.startswith("["):
        closing_idx = listen_addr.find("]")
        if closing_idx == -1:
            raise ValueError(f"Invalid listen_addr: {listen_addr!r}")
        if closing_idx + 1 >= len(listen_addr) or listen_addr[closing_idx + 1] != ":":
            raise ValueError(f"Invalid listen_addr: {listen_addr!r}")
        host = listen_addr[1:closing_idx]
        port_text = listen_addr[closing_idx + 2 :]
    else:
        if ":" not in listen_addr:
            raise ValueError(f"Invalid listen_addr: {listen_addr!r}")
        host, port_text = listen_addr.rsplit(":", 1)
    if not host:
        raise ValueError(f"Invalid listen_addr: {listen_addr!r}")
    return host, int(port_text)


def route_request(request: HttpRequest, *, metrics_source: MetricsSource) -> HttpResponse:
    """Pure routing table for supported HTTP endpoints."""
    if request.path == "/health":
        if request.method != "GET":
            return _error_response(status_code=405, code="method_not_allowed")
        return _dto_response(status_code=200, payload=handle_health())

    if request.path == "/metrics":
        if request.method != "GET":
            return _error_response(status_code=405, code="method_not_allowed")
        return _dto_response(status_code=200, payload=handle_metrics(metrics_source))

    return _error_response(status_code=404, code="not_found")


async def serve_http(
    *,
    host: str,
    port: int,
    metrics_source: MetricsSource,
) -> None:
    """Serve HTTP endpoints on the given host/port."""
    handler = partial(_serve_connection, metrics_source=metrics_source)
    await trio.serve_tcp(handler, port=port, host=host)


async def serve_http_on_listeners(
    listeners: list[trio.SocketListener],
    metrics_source: MetricsSource,
) -> None:
    """Serve on already-open listeners (useful for tests and shared-port setups)."""
    handler = partial(_serve_connection, metrics_source=metrics_source)
    await trio.serve_listeners(handler, listeners)


async def serve_http_from_listen_addr(*, listen_addr: str, metrics_source: MetricsSource) -> None:
    """Serve HTTP endpoints using an RFC-style `listen_addr` string."""
    host, port = parse_listen_addr(listen_addr)
    await serve_http(host=host, port=port, metrics_source=metrics_source)


async def serve_http_connection(stream: trio.abc.Stream, *, metrics_source: MetricsSource) -> None:
    """Serve a single already-accepted connection as HTTP.

    Public entry for the same-port dispatch (`slack_fuse_server.dispatch`), which
    classifies the request then replays it into this handler over a stream that
    prepends the bytes it already peeked.
    """
    await _serve_connection(stream, metrics_source=metrics_source)


async def _serve_connection(stream: trio.abc.Stream, *, metrics_source: MetricsSource) -> None:
    conn = h11.Connection(h11.SERVER)
    try:
        request = await _read_request(conn, stream)
        if request is None:
            return
        response = route_request(request, metrics_source=metrics_source)
        await _send_response(conn, stream, response)
    except h11.RemoteProtocolError:
        if conn.our_state is not h11.ERROR:
            await _send_response(conn, stream, _error_response(status_code=400, code="bad_request"))
    finally:
        await stream.aclose()


async def _read_request(conn: h11.Connection, stream: trio.abc.Stream) -> HttpRequest | None:
    method: str | None = None
    target: str | None = None
    while True:
        event = conn.next_event()
        if event is h11.NEED_DATA:
            chunk = await stream.receive_some(_READ_CHUNK_SIZE)
            conn.receive_data(bytes(chunk) if chunk else b"")
            continue
        if isinstance(event, h11.Request):
            method = event.method.decode("ascii").upper()
            target = event.target.decode("ascii")
            continue
        if isinstance(event, h11.Data):
            # Endpoint surface is GET-only; consume any body and ignore it.
            continue
        if isinstance(event, h11.EndOfMessage):
            if method is None or target is None:
                return None
            return HttpRequest(method=method, target=target)
        if isinstance(event, h11.ConnectionClosed):
            return None


async def _send_response(conn: h11.Connection, stream: trio.abc.Stream, response: HttpResponse) -> None:
    header_tuples: list[tuple[bytes, bytes]] = [
        (b"content-type", response.content_type.encode("ascii")),
        (b"content-length", str(len(response.body)).encode("ascii")),
        (b"connection", b"close"),
    ]
    encoded = b"".join((
        conn.send(h11.Response(status_code=response.status_code, headers=header_tuples)),
        conn.send(h11.Data(data=response.body)),
        conn.send(h11.EndOfMessage()),
    ))
    await stream.send_all(encoded)


def _dto_response(*, status_code: int, payload: BaseModel) -> HttpResponse:
    return HttpResponse(status_code=status_code, body=payload.model_dump_json().encode("utf-8"))


def _error_response(*, status_code: int, code: str) -> HttpResponse:
    body = json.dumps({"error": code}, separators=(",", ":")).encode("utf-8")
    return HttpResponse(status_code=status_code, body=body)
