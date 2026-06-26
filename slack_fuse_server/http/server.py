"""Minimal trio HTTP server for `/health`, `/metrics`, `/resolve`, `/permalink`, `/streams/*/snapshot`."""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import partial
from urllib.parse import parse_qs, unquote, urlsplit

import h11
import psycopg
import trio
from pydantic import BaseModel, ValidationError

from slack_fuse_server.http.dto import (
    SNAPSHOT_CONTENT_ENCODING,
    SNAPSHOT_CONTENT_TYPE,
    PermalinkRequest,
    ResolveRequest,
)
from slack_fuse_server.http.handlers import (
    GapsDeps,
    OriginalsDeps,
    ResolvePermalinkDeps,
    SnapshotDeps,
    handle_channel_gaps,
    handle_health,
    handle_metrics,
    handle_originals,
    handle_permalink,
    handle_resolve,
    handle_snapshot,
    handle_workspace_gaps,
)
from slack_fuse_server.http.metrics import MetricsSource
from slack_fuse_server.http.snapshot import SnapshotNotFoundError
from slack_fuse_server.slurper.api import SlackAPIError

_JSON_CONTENT_TYPE = "application/json"
_READ_CHUNK_SIZE = 16_384


@dataclass(frozen=True, slots=True)
class HttpResponse:
    status_code: int
    body: bytes
    content_type: str = _JSON_CONTENT_TYPE
    headers: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class HttpRequest:
    method: str
    target: str
    body: bytes = b""

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


def route_request(  # noqa: C901, PLR0913 - endpoint routing dispatch hub.
    request: HttpRequest,
    *,
    metrics_source: MetricsSource,
    resolve_permalink_deps: ResolvePermalinkDeps | None = None,
    snapshot_deps: SnapshotDeps | None = None,
    originals_deps: OriginalsDeps | None = None,
    gaps_deps: GapsDeps | None = None,
) -> HttpResponse:
    """Pure routing table for supported HTTP endpoints."""
    if request.path == "/health":
        if request.method != "GET":
            return _error_response(status_code=405, code="method_not_allowed")
        return _dto_response(status_code=200, payload=handle_health())

    if request.path == "/metrics":
        if request.method != "GET":
            return _error_response(status_code=405, code="method_not_allowed")
        return _dto_response(status_code=200, payload=handle_metrics(metrics_source))

    if request.path == "/resolve":
        if request.method != "POST":
            return _error_response(status_code=405, code="method_not_allowed")
        if resolve_permalink_deps is None:
            return _error_response(status_code=503, code="service_unavailable")
        return _handle_resolve(request, resolve_permalink_deps)

    if request.path == "/permalink":
        if request.method != "POST":
            return _error_response(status_code=405, code="method_not_allowed")
        if resolve_permalink_deps is None:
            return _error_response(status_code=503, code="service_unavailable")
        return _handle_permalink(request, resolve_permalink_deps)

    snapshot_stream = _snapshot_stream_from_path(request.path)
    if snapshot_stream is not None:
        if request.method != "GET":
            return _error_response(status_code=405, code="method_not_allowed")
        if snapshot_deps is None:
            return _error_response(status_code=503, code="service_unavailable")
        try:
            at, since = _parse_snapshot_query(request.target)
        except ValueError:
            return _error_response(status_code=400, code="bad_request")
        return _handle_snapshot(snapshot_stream, at=at, since=since, deps=snapshot_deps)

    originals_channel = _originals_channel_from_path(request.path)
    if originals_channel is not None:
        if request.method != "GET":
            return _error_response(status_code=405, code="method_not_allowed")
        if originals_deps is None:
            return _error_response(status_code=503, code="service_unavailable")
        try:
            from_epoch, to_epoch = _parse_originals_query(request.target)
        except ValueError:
            return _error_response(status_code=400, code="bad_request")
        return _handle_originals(
            originals_channel, from_epoch=from_epoch, to_epoch=to_epoch, deps=originals_deps
        )

    if request.path == "/gaps":
        if request.method != "GET":
            return _error_response(status_code=405, code="method_not_allowed")
        if gaps_deps is None:
            return _error_response(status_code=503, code="service_unavailable")
        return _handle_workspace_gaps(deps=gaps_deps)

    gaps_channel = _gaps_channel_from_path(request.path)
    if gaps_channel is not None:
        if request.method != "GET":
            return _error_response(status_code=405, code="method_not_allowed")
        if gaps_deps is None:
            return _error_response(status_code=503, code="service_unavailable")
        return _handle_channel_gaps(gaps_channel, deps=gaps_deps)

    return _error_response(status_code=404, code="not_found")


async def serve_http(  # noqa: PLR0913 - HTTP wiring needs explicit deps.
    *,
    host: str,
    port: int,
    metrics_source: MetricsSource,
    resolve_permalink_deps: ResolvePermalinkDeps | None = None,
    snapshot_deps: SnapshotDeps | None = None,
    originals_deps: OriginalsDeps | None = None,
    gaps_deps: GapsDeps | None = None,
) -> None:
    """Serve HTTP endpoints on the given host/port."""
    handler = partial(
        _serve_connection,
        metrics_source=metrics_source,
        resolve_permalink_deps=resolve_permalink_deps,
        snapshot_deps=snapshot_deps,
        originals_deps=originals_deps,
        gaps_deps=gaps_deps,
    )
    await trio.serve_tcp(handler, port=port, host=host)


async def serve_http_on_listeners(  # noqa: PLR0913, PLR0917 - HTTP wiring needs explicit deps.
    listeners: list[trio.SocketListener],
    metrics_source: MetricsSource,
    resolve_permalink_deps: ResolvePermalinkDeps | None = None,
    snapshot_deps: SnapshotDeps | None = None,
    originals_deps: OriginalsDeps | None = None,
    gaps_deps: GapsDeps | None = None,
) -> None:
    """Serve on already-open listeners (useful for tests and shared-port setups)."""
    handler = partial(
        _serve_connection,
        metrics_source=metrics_source,
        resolve_permalink_deps=resolve_permalink_deps,
        snapshot_deps=snapshot_deps,
        originals_deps=originals_deps,
        gaps_deps=gaps_deps,
    )
    await trio.serve_listeners(handler, listeners)


async def serve_http_from_listen_addr(  # noqa: PLR0913 - HTTP wiring needs explicit deps.
    *,
    listen_addr: str,
    metrics_source: MetricsSource,
    resolve_permalink_deps: ResolvePermalinkDeps | None = None,
    snapshot_deps: SnapshotDeps | None = None,
    originals_deps: OriginalsDeps | None = None,
    gaps_deps: GapsDeps | None = None,
) -> None:
    """Serve HTTP endpoints using an RFC-style `listen_addr` string."""
    host, port = parse_listen_addr(listen_addr)
    await serve_http(
        host=host,
        port=port,
        metrics_source=metrics_source,
        resolve_permalink_deps=resolve_permalink_deps,
        snapshot_deps=snapshot_deps,
        originals_deps=originals_deps,
        gaps_deps=gaps_deps,
    )


async def serve_http_connection(  # noqa: PLR0913 - HTTP wiring needs explicit deps.
    stream: trio.abc.Stream,
    *,
    metrics_source: MetricsSource,
    resolve_permalink_deps: ResolvePermalinkDeps | None = None,
    snapshot_deps: SnapshotDeps | None = None,
    originals_deps: OriginalsDeps | None = None,
    gaps_deps: GapsDeps | None = None,
) -> None:
    """Serve a single already-accepted connection as HTTP.

    Public entry for the same-port dispatch (`slack_fuse_server.dispatch`), which
    classifies the request then replays it into this handler over a stream that
    prepends the bytes it already peeked.
    """
    await _serve_connection(
        stream,
        metrics_source=metrics_source,
        resolve_permalink_deps=resolve_permalink_deps,
        snapshot_deps=snapshot_deps,
        originals_deps=originals_deps,
        gaps_deps=gaps_deps,
    )


async def _serve_connection(  # noqa: PLR0913 - HTTP wiring needs explicit deps.
    stream: trio.abc.Stream,
    *,
    metrics_source: MetricsSource,
    resolve_permalink_deps: ResolvePermalinkDeps | None = None,
    snapshot_deps: SnapshotDeps | None = None,
    originals_deps: OriginalsDeps | None = None,
    gaps_deps: GapsDeps | None = None,
) -> None:
    conn = h11.Connection(h11.SERVER)
    try:
        request = await _read_request(conn, stream)
        if request is None:
            return
        response = route_request(
            request,
            metrics_source=metrics_source,
            resolve_permalink_deps=resolve_permalink_deps,
            snapshot_deps=snapshot_deps,
            originals_deps=originals_deps,
            gaps_deps=gaps_deps,
        )
        await _send_response(conn, stream, response)
    except h11.RemoteProtocolError:
        if conn.our_state is not h11.ERROR:
            await _send_response(conn, stream, _error_response(status_code=400, code="bad_request"))
    finally:
        await stream.aclose()


async def _read_request(conn: h11.Connection, stream: trio.abc.Stream) -> HttpRequest | None:
    method: str | None = None
    target: str | None = None
    body = bytearray()
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
            body.extend(bytes(event.data))
            continue
        if isinstance(event, h11.EndOfMessage):
            if method is None or target is None:
                return None
            return HttpRequest(method=method, target=target, body=bytes(body))
        if isinstance(event, h11.ConnectionClosed):
            return None


async def _send_response(conn: h11.Connection, stream: trio.abc.Stream, response: HttpResponse) -> None:
    header_tuples: list[tuple[bytes, bytes]] = [
        (b"content-type", response.content_type.encode("ascii")),
        (b"content-length", str(len(response.body)).encode("ascii")),
        (b"connection", b"close"),
    ]
    header_tuples.extend((name.encode("ascii"), value.encode("ascii")) for name, value in response.headers)
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


def _handle_resolve(request: HttpRequest, deps: ResolvePermalinkDeps) -> HttpResponse:
    try:
        payload = ResolveRequest.model_validate_json(request.body)
    except ValidationError:
        return _error_response(status_code=400, code="bad_request")

    try:
        response = handle_resolve(payload, deps)
    except LookupError:
        return _error_response(status_code=404, code="not_found")
    except ValueError:
        return _error_response(status_code=400, code="bad_request")
    except SlackAPIError:
        return _error_response(status_code=502, code="slack_api_error")
    return _dto_response(status_code=200, payload=response)


def _handle_permalink(request: HttpRequest, deps: ResolvePermalinkDeps) -> HttpResponse:
    try:
        payload = PermalinkRequest.model_validate_json(request.body)
    except ValidationError:
        return _error_response(status_code=400, code="bad_request")

    try:
        response = handle_permalink(payload, deps)
    except LookupError:
        return _error_response(status_code=404, code="not_found")
    except ValueError:
        return _error_response(status_code=400, code="bad_request")
    except SlackAPIError:
        return _error_response(status_code=502, code="slack_api_error")
    return _dto_response(status_code=200, payload=response)


def _handle_snapshot(stream: str, *, at: int, since: int | None, deps: SnapshotDeps) -> HttpResponse:
    try:
        payload = handle_snapshot(stream, at=at, since=since, deps=deps)
    except SnapshotNotFoundError:
        return _error_response(status_code=404, code="not_found")
    except ValueError:
        return _error_response(status_code=400, code="bad_request")
    except psycopg.Error:
        return _error_response(status_code=503, code="service_unavailable")
    return HttpResponse(
        status_code=200,
        body=payload.body,
        content_type=SNAPSHOT_CONTENT_TYPE,
        headers=(("content-encoding", SNAPSHOT_CONTENT_ENCODING),),
    )


def _snapshot_stream_from_path(path: str) -> str | None:
    parts = path.split("/")
    if len(parts) != 4 or parts[1] != "streams" or parts[3] != "snapshot":
        return None
    encoded_stream = parts[2]
    if not encoded_stream:
        return None
    stream = unquote(encoded_stream)
    return stream or None


def _parse_snapshot_query(target: str) -> tuple[int, int | None]:
    query = parse_qs(urlsplit(target).query, keep_blank_values=False)
    at_raw = _single_query_value(query, "at")
    if at_raw is None:  # pragma: no cover - required=True guarantees this.
        raise ValueError("missing 'at' query parameter")
    at = _parse_non_negative_int(at_raw)
    since_raw = _single_query_value(query, "since", required=False)
    since = None if since_raw is None else _parse_non_negative_int(since_raw)
    return at, since


def _single_query_value(query: dict[str, list[str]], key: str, *, required: bool = True) -> str | None:
    values = query.get(key)
    if values is None:
        if required:
            raise ValueError(f"missing {key!r} query parameter")
        return None
    if len(values) != 1:
        raise ValueError(f"duplicate {key!r} query parameter")
    return values[0]


def _parse_non_negative_int(text: str) -> int:
    value = int(text)
    if value < 0:
        raise ValueError("snapshot query offsets must be >= 0")
    return value


def _originals_channel_from_path(path: str) -> str | None:
    parts = path.split("/")
    if len(parts) != 3 or parts[1] != "originals":
        return None
    encoded = parts[2]
    if not encoded:
        return None
    channel = unquote(encoded)
    return channel or None


def _parse_originals_query(target: str) -> tuple[float, float]:
    query = parse_qs(urlsplit(target).query, keep_blank_values=False)
    from_raw = _single_query_value(query, "from")
    to_raw = _single_query_value(query, "to")
    assert from_raw is not None and to_raw is not None  # required=True
    from_epoch = float(from_raw)
    to_epoch = float(to_raw)
    if not (from_epoch >= 0.0 and to_epoch >= 0.0):
        raise ValueError("originals query epochs must be >= 0")
    if to_epoch <= from_epoch:
        raise ValueError("originals query 'to' must be > 'from'")
    return from_epoch, to_epoch


def _handle_originals(
    channel_id: str,
    *,
    from_epoch: float,
    to_epoch: float,
    deps: OriginalsDeps,
) -> HttpResponse:
    try:
        body = handle_originals(channel_id, from_epoch=from_epoch, to_epoch=to_epoch, deps=deps)
    except psycopg.Error:
        return _error_response(status_code=503, code="service_unavailable")
    return HttpResponse(status_code=200, body=body, content_type="text/markdown; charset=utf-8")


def _gaps_channel_from_path(path: str) -> str | None:
    parts = path.split("/")
    if len(parts) != 3 or parts[1] != "gaps":
        return None
    encoded = parts[2]
    if not encoded:
        return None
    channel = unquote(encoded)
    return channel or None


def _handle_channel_gaps(channel_id: str, *, deps: GapsDeps) -> HttpResponse:
    try:
        body = handle_channel_gaps(channel_id, deps=deps)
    except psycopg.Error:
        return _error_response(status_code=503, code="service_unavailable")
    return HttpResponse(status_code=200, body=body, content_type="text/markdown; charset=utf-8")


def _handle_workspace_gaps(*, deps: GapsDeps) -> HttpResponse:
    try:
        body = handle_workspace_gaps(deps=deps)
    except psycopg.Error:
        return _error_response(status_code=503, code="service_unavailable")
    return HttpResponse(status_code=200, body=body, content_type="text/markdown; charset=utf-8")
