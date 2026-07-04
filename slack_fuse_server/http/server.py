"""Minimal trio HTTP server for `/health`, `/metrics`, `/resolve`, `/permalink`, `/streams/*/snapshot`."""

from __future__ import annotations

import contextlib
import json
import logging
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
    BackfillDeps,
    BlockedChannelsDeps,
    GapsDeps,
    LivezDeps,
    OriginalsDeps,
    ProbeDeps,
    RefreshDeps,
    ResolvePermalinkDeps,
    SnapshotDeps,
    handle_backfill_channel,
    handle_block_channel,
    handle_channel_gaps,
    handle_health,
    handle_list_blocked_channels,
    handle_livez,
    handle_metrics,
    handle_originals,
    handle_permalink,
    handle_probe_sweep,
    handle_refresh_channel,
    handle_refresh_channels,
    handle_resolve,
    handle_snapshot,
    handle_unblock_channel,
    handle_workspace_gaps,
)
from slack_fuse_server.http.metrics import MetricsSource
from slack_fuse_server.http.snapshot import SnapshotNotFoundError
from slack_fuse_server.slurper.api import SlackAPIError

log = logging.getLogger(__name__)

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
    headers: tuple[tuple[bytes, bytes], ...] = ()

    @property
    def path(self) -> str:
        path, _, _query = self.target.partition("?")
        return path


class BlockChannelRequest(BaseModel):
    channel_id: str
    reason: str | None = None


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
    refresh_deps: RefreshDeps | None = None,
    blocked_channels_deps: BlockedChannelsDeps | None = None,
    backfill_deps: BackfillDeps | None = None,
    probe_deps: ProbeDeps | None = None,
    livez_deps: LivezDeps | None = None,
) -> HttpResponse:
    """Pure routing table for supported HTTP endpoints."""
    if request.path == "/health":
        if request.method != "GET":
            return _error_response(status_code=405, code="method_not_allowed")
        return _dto_response(status_code=200, payload=handle_health())

    if request.path == "/livez":
        if request.method != "GET":
            return _error_response(status_code=405, code="method_not_allowed")
        if livez_deps is None:
            return _error_response(status_code=503, code="service_unavailable")
        status_code, payload = handle_livez(livez_deps)
        return _json_response(status_code=status_code, payload=payload)

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

    if request.path == "/refresh-channels":
        if request.method != "POST":
            return _error_response(status_code=405, code="method_not_allowed")
        if refresh_deps is None:
            return _error_response(status_code=503, code="service_unavailable")
        status_code, message = handle_refresh_channels(request.headers, deps=refresh_deps)
        body = json.dumps({"status": message}, separators=(",", ":")).encode("utf-8")
        return HttpResponse(status_code=status_code, body=body)

    if request.path == "/probe-sweep":
        if request.method != "POST":
            return _error_response(status_code=405, code="method_not_allowed")
        if probe_deps is None:
            return _error_response(status_code=503, code="service_unavailable")
        status_code, message = handle_probe_sweep(request.headers, deps=probe_deps)
        body = json.dumps({"status": message}, separators=(",", ":")).encode("utf-8")
        return HttpResponse(status_code=status_code, body=body)

    if request.path == "/blocked-channels":
        if request.method == "GET":
            if blocked_channels_deps is None:
                return _error_response(status_code=503, code="service_unavailable")
            status_code, payload = handle_list_blocked_channels(
                request.headers, deps=blocked_channels_deps
            )
            return _json_response(status_code=status_code, payload=payload)
        if request.method == "POST":
            if blocked_channels_deps is None:
                return _error_response(status_code=503, code="service_unavailable")
            return _handle_blocked_channels_post(request, blocked_channels_deps)
        return _error_response(status_code=405, code="method_not_allowed")

    blocked_channel_delete = _blocked_channel_from_path(request.path)
    if blocked_channel_delete is not None:
        if request.method != "DELETE":
            return _error_response(status_code=405, code="method_not_allowed")
        if blocked_channels_deps is None:
            return _error_response(status_code=503, code="service_unavailable")
        status_code, payload = handle_unblock_channel(
            blocked_channel_delete, request.headers, deps=blocked_channels_deps
        )
        return _json_response(status_code=status_code, payload=payload)

    refresh_channel_id = _refresh_channel_from_path(request.path)
    if refresh_channel_id is not None:
        if request.method != "POST":
            return _error_response(status_code=405, code="method_not_allowed")
        if refresh_deps is None:
            return _error_response(status_code=503, code="service_unavailable")
        status_code, message = handle_refresh_channel(
            refresh_channel_id, request.headers, deps=refresh_deps
        )
        body = json.dumps({"status": message}, separators=(",", ":")).encode("utf-8")
        return HttpResponse(status_code=status_code, body=body)

    backfill_channel_id = _backfill_channel_from_path(request.path)
    if backfill_channel_id is not None:
        if request.method != "POST":
            return _error_response(status_code=405, code="method_not_allowed")
        if backfill_deps is None:
            return _error_response(status_code=503, code="service_unavailable")
        status_code, message = handle_backfill_channel(
            backfill_channel_id, request.headers, deps=backfill_deps
        )
        body = json.dumps({"status": message}, separators=(",", ":")).encode("utf-8")
        return HttpResponse(status_code=status_code, body=body)

    probe_sweep_request = _probe_sweep_from_path(request.path)
    if probe_sweep_request is not None:
        if request.method != "POST":
            return _error_response(status_code=405, code="method_not_allowed")
        if probe_deps is None:
            return _error_response(status_code=503, code="service_unavailable")
        job_id, target = probe_sweep_request
        status_code, message = handle_probe_sweep(
            request.headers,
            deps=probe_deps,
            job_id=job_id,
            target=target,
        )
        body = json.dumps({"status": message}, separators=(",", ":")).encode("utf-8")
        return HttpResponse(status_code=status_code, body=body)

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
    refresh_deps: RefreshDeps | None = None,
    blocked_channels_deps: BlockedChannelsDeps | None = None,
    backfill_deps: BackfillDeps | None = None,
    probe_deps: ProbeDeps | None = None,
    livez_deps: LivezDeps | None = None,
) -> None:
    """Serve HTTP endpoints on the given host/port."""
    handler = partial(
        _serve_connection,
        metrics_source=metrics_source,
        resolve_permalink_deps=resolve_permalink_deps,
        snapshot_deps=snapshot_deps,
        originals_deps=originals_deps,
        gaps_deps=gaps_deps,
        refresh_deps=refresh_deps,
        blocked_channels_deps=blocked_channels_deps,
        backfill_deps=backfill_deps,
        probe_deps=probe_deps,
        livez_deps=livez_deps,
    )
    await trio.serve_tcp(handler, port=port, host=host)


async def serve_http_on_listeners(  # noqa: PLR0913, PLR0917 - HTTP wiring needs explicit deps.
    listeners: list[trio.SocketListener],
    metrics_source: MetricsSource,
    resolve_permalink_deps: ResolvePermalinkDeps | None = None,
    snapshot_deps: SnapshotDeps | None = None,
    originals_deps: OriginalsDeps | None = None,
    gaps_deps: GapsDeps | None = None,
    refresh_deps: RefreshDeps | None = None,
    blocked_channels_deps: BlockedChannelsDeps | None = None,
    backfill_deps: BackfillDeps | None = None,
    probe_deps: ProbeDeps | None = None,
    livez_deps: LivezDeps | None = None,
) -> None:
    """Serve on already-open listeners (useful for tests and shared-port setups)."""
    handler = partial(
        _serve_connection,
        metrics_source=metrics_source,
        resolve_permalink_deps=resolve_permalink_deps,
        snapshot_deps=snapshot_deps,
        originals_deps=originals_deps,
        gaps_deps=gaps_deps,
        refresh_deps=refresh_deps,
        blocked_channels_deps=blocked_channels_deps,
        backfill_deps=backfill_deps,
        probe_deps=probe_deps,
        livez_deps=livez_deps,
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
    refresh_deps: RefreshDeps | None = None,
    blocked_channels_deps: BlockedChannelsDeps | None = None,
    backfill_deps: BackfillDeps | None = None,
    probe_deps: ProbeDeps | None = None,
    livez_deps: LivezDeps | None = None,
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
        refresh_deps=refresh_deps,
        blocked_channels_deps=blocked_channels_deps,
        backfill_deps=backfill_deps,
        probe_deps=probe_deps,
        livez_deps=livez_deps,
    )


async def serve_http_connection(  # noqa: PLR0913 - HTTP wiring needs explicit deps.
    stream: trio.abc.Stream,
    *,
    metrics_source: MetricsSource,
    resolve_permalink_deps: ResolvePermalinkDeps | None = None,
    snapshot_deps: SnapshotDeps | None = None,
    originals_deps: OriginalsDeps | None = None,
    gaps_deps: GapsDeps | None = None,
    refresh_deps: RefreshDeps | None = None,
    blocked_channels_deps: BlockedChannelsDeps | None = None,
    backfill_deps: BackfillDeps | None = None,
    probe_deps: ProbeDeps | None = None,
    livez_deps: LivezDeps | None = None,
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
        refresh_deps=refresh_deps,
        blocked_channels_deps=blocked_channels_deps,
        backfill_deps=backfill_deps,
        probe_deps=probe_deps,
        livez_deps=livez_deps,
    )


async def _serve_connection(  # noqa: PLR0913 - HTTP wiring needs explicit deps.
    stream: trio.abc.Stream,
    *,
    metrics_source: MetricsSource,
    resolve_permalink_deps: ResolvePermalinkDeps | None = None,
    snapshot_deps: SnapshotDeps | None = None,
    originals_deps: OriginalsDeps | None = None,
    gaps_deps: GapsDeps | None = None,
    refresh_deps: RefreshDeps | None = None,
    blocked_channels_deps: BlockedChannelsDeps | None = None,
    backfill_deps: BackfillDeps | None = None,
    probe_deps: ProbeDeps | None = None,
    livez_deps: LivezDeps | None = None,
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
            refresh_deps=refresh_deps,
            blocked_channels_deps=blocked_channels_deps,
            backfill_deps=backfill_deps,
            probe_deps=probe_deps,
            livez_deps=livez_deps,
        )
        await _send_response(conn, stream, response)
    except h11.RemoteProtocolError:
        if conn.our_state is not h11.ERROR:
            # Client may already have hung up while we try to send the 400.
            with contextlib.suppress(trio.BrokenResourceError, trio.ClosedResourceError):
                await _send_response(conn, stream, _error_response(status_code=400, code="bad_request"))
    except (trio.BrokenResourceError, trio.ClosedResourceError) as exc:
        # Client disconnected mid-request or mid-response. Normal HTTP behaviour
        # (kubelet probes give up under load; curl aborts; projector times out
        # and retries). Nothing to send back; just close the stream. Log at INFO
        # so a spike is visible in Loki without drowning steady-state noise.
        log.info("http: connection dropped by peer: %s", exc)
    finally:
        with trio.CancelScope(shield=True), contextlib.suppress(
            trio.BrokenResourceError, trio.ClosedResourceError
        ):
            await stream.aclose()


async def _read_request(conn: h11.Connection, stream: trio.abc.Stream) -> HttpRequest | None:
    method: str | None = None
    target: str | None = None
    headers: tuple[tuple[bytes, bytes], ...] = ()
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
            # Capture headers — used by the mutating /refresh-channels route
            # for shared-secret auth. Cheap to always carry; the read-only
            # routes just ignore the field.
            headers = tuple((bytes(name), bytes(value)) for name, value in event.headers)
            continue
        if isinstance(event, h11.Data):
            body.extend(bytes(event.data))
            continue
        if isinstance(event, h11.EndOfMessage):
            if method is None or target is None:
                return None
            return HttpRequest(method=method, target=target, body=bytes(body), headers=headers)
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


def _json_response(*, status_code: int, payload: dict[str, object]) -> HttpResponse:
    return HttpResponse(
        status_code=status_code,
        body=json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8"),
    )


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


def _handle_blocked_channels_post(request: HttpRequest, deps: BlockedChannelsDeps) -> HttpResponse:
    try:
        payload = BlockChannelRequest.model_validate_json(request.body)
    except ValidationError:
        return _error_response(status_code=400, code="bad_request")
    if not payload.channel_id.strip():
        return _error_response(status_code=400, code="bad_request")
    status_code, response = handle_block_channel(
        payload.channel_id.strip(),
        payload.reason,
        request.headers,
        deps=deps,
    )
    return _json_response(status_code=status_code, payload=response)


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


def _refresh_channel_from_path(path: str) -> str | None:
    """Parse ``/refresh-channels/<channel_id>`` (note: hyphenated, same as
    the workspace route). Returns the decoded channel id or ``None`` when
    the path doesn't match."""
    parts = path.split("/")
    if len(parts) != 3 or parts[1] != "refresh-channels":
        return None
    encoded = parts[2]
    if not encoded:
        return None
    return unquote(encoded) or None


def _blocked_channel_from_path(path: str) -> str | None:
    parts = path.split("/")
    if len(parts) != 3 or parts[1] != "blocked-channels":
        return None
    encoded = parts[2]
    if not encoded:
        return None
    return unquote(encoded) or None


def _backfill_channel_from_path(path: str) -> str | None:
    parts = path.split("/")
    if len(parts) != 3 or parts[1] != "backfill-channel":
        return None
    encoded = parts[2]
    if not encoded:
        return None
    return unquote(encoded) or None


def _probe_sweep_from_path(path: str) -> tuple[str, str | None] | None:
    parts = path.split("/")
    if len(parts) not in (3, 4) or parts[1] != "probe-sweep":
        return None
    encoded_job = parts[2]
    if not encoded_job:
        return None
    job_id = unquote(encoded_job)
    if not job_id:
        return None
    if len(parts) == 3:
        return job_id, None
    encoded_target = parts[3]
    if not encoded_target:
        return None
    target = unquote(encoded_target)
    if not target:
        return None
    return job_id, target


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
