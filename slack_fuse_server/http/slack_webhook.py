"""Dedicated public HTTP listener for Slack Events API callbacks."""

from __future__ import annotations

import contextlib
import hashlib
import hmac
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from typing import cast

import h11
import trio
from pydantic import ValidationError

from slack_fuse.models import EventsApiPayload
from slack_fuse_server._json import JsonObject
from slack_fuse_server.http.server import HttpRequest, HttpResponse
from slack_fuse_server.slack_events.inbox import InboxWriter

log = logging.getLogger(__name__)

MAX_SLACK_BODY_BYTES = 1024 * 1024
REQUEST_DEADLINE_S = 2.5
_READ_CHUNK_SIZE = 16_384
_JSON_CONTENT_TYPE = "application/json"
_TEXT_CONTENT_TYPE = "text/plain; charset=utf-8"


@dataclass(frozen=True, slots=True)
class SlackWebhookDeps:
    signing_secret: str
    inbox: InboxWriter
    clock: Callable[[], float] = time.time


class _BodyTooLarge(Exception):
    pass


def _header(request: HttpRequest, name: bytes) -> bytes | None:
    wanted = name.lower()
    for key, value in request.headers:
        if key.lower() == wanted:
            return value
    return None


def verify_slack_signature(
    request: HttpRequest,
    signing_secret: str,
    *,
    now: float,
) -> int | None:
    """Return an HTTP error status, or None when signature + skew are valid."""
    signature_raw = _header(request, b"x-slack-signature")
    if signature_raw is None:
        return 401
    try:
        signature = signature_raw.decode("ascii")
    except UnicodeDecodeError:
        return 401
    if not signature.startswith("v0=") or len(signature) != 67:
        return 401
    digest = signature[3:]
    try:
        bytes.fromhex(digest)
    except ValueError:
        return 401

    timestamp_raw = _header(request, b"x-slack-request-timestamp")
    if timestamp_raw is None:
        return 400
    try:
        timestamp_text = timestamp_raw.decode("ascii")
        timestamp = int(timestamp_text)
    except (UnicodeDecodeError, ValueError):
        return 400
    if abs(now - timestamp) > 300:
        return 400

    base = b"v0:" + timestamp_text.encode("ascii") + b":" + request.body
    computed = "v0=" + hmac.new(signing_secret.encode("utf-8"), base, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(computed, signature):
        return 401
    return None


def _response(status_code: int, code: str) -> HttpResponse:
    return HttpResponse(
        status_code=status_code,
        body=json.dumps({"error": code}, separators=(",", ":")).encode(),
    )


async def route_slack_webhook(  # noqa: C901 - deliberately tiny explicit public routing table.
    request: HttpRequest, deps: SlackWebhookDeps
) -> HttpResponse:
    """Async public router: exactly POST /slack/events and GET /healthz."""
    if request.path == "/healthz" and request.method == "GET":
        return HttpResponse(status_code=200, body=b'{"ok":true}')
    if request.path != "/slack/events" or request.method != "POST":
        return _response(404, "not_found")

    verification_error = verify_slack_signature(request, deps.signing_secret, now=deps.clock())
    if verification_error is not None:
        code = "unauthorized" if verification_error == 401 else "bad_timestamp"
        return _response(verification_error, code)

    try:
        parsed_raw = json.loads(request.body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return _response(400, "bad_request")
    if not isinstance(parsed_raw, dict):
        return _response(400, "bad_request")
    envelope = cast(JsonObject, parsed_raw)
    try:
        payload = EventsApiPayload.model_validate(envelope)
    except ValidationError:
        return _response(400, "bad_request")

    if payload.type == "url_verification":
        if payload.challenge is None:
            return _response(400, "bad_request")
        return HttpResponse(status_code=200, body=payload.challenge.encode(), content_type=_TEXT_CONTENT_TYPE)
    if payload.type == "app_rate_limited":
        log.warning("slack webhook: app_rate_limited")
        return HttpResponse(status_code=200, body=b"", content_type=_TEXT_CONTENT_TYPE)
    if payload.type != "event_callback" or not payload.event_id or payload.event is None:
        return _response(400, "bad_request")

    await deps.inbox.enqueue(payload.event_id, envelope)
    return HttpResponse(status_code=200, body=b"", content_type=_TEXT_CONTENT_TYPE)


async def _read_request(  # noqa: C901 - h11's event state machine is inherently branch-shaped.
    conn: h11.Connection, stream: trio.abc.Stream
) -> HttpRequest | None:
    request_event: h11.Request | None = None
    body = bytearray()
    while True:  # noqa: PLR1702 - h11's event state machine is inherently nested.
        event = conn.next_event()
        if event is h11.NEED_DATA:
            data = await stream.receive_some(_READ_CHUNK_SIZE)
            if not data:
                conn.receive_data(b"")
            else:
                conn.receive_data(bytes(data))
            continue
        if isinstance(event, h11.Request):
            request_event = event
            for key, value in event.headers:
                if key.lower() == b"content-length":
                    try:
                        if int(value) > MAX_SLACK_BODY_BYTES:
                            raise _BodyTooLarge
                    except ValueError as exc:
                        raise h11.RemoteProtocolError("invalid content-length") from exc
            continue
        if isinstance(event, h11.Data):
            body.extend(event.data)
            if len(body) > MAX_SLACK_BODY_BYTES:
                raise _BodyTooLarge
            continue
        if isinstance(event, h11.EndOfMessage):
            if request_event is None:
                return None
            return HttpRequest(
                method=request_event.method.decode("ascii", errors="replace"),
                target=request_event.target.decode("ascii", errors="replace"),
                body=bytes(body),
                headers=tuple(request_event.headers),
            )
        if isinstance(event, h11.ConnectionClosed):
            return None


async def _send_response(conn: h11.Connection, stream: trio.abc.Stream, response: HttpResponse) -> None:
    headers = [
        (b"content-type", response.content_type.encode("ascii")),
        (b"content-length", str(len(response.body)).encode("ascii")),
        (b"connection", b"close"),
    ]
    headers.extend((key.encode("ascii"), value.encode("ascii")) for key, value in response.headers)
    await stream.send_all(conn.send(h11.Response(status_code=response.status_code, headers=headers)))
    if response.body:
        await stream.send_all(conn.send(h11.Data(data=response.body)))
    await stream.send_all(conn.send(h11.EndOfMessage()))


async def serve_slack_webhook_connection(stream: trio.abc.Stream, deps: SlackWebhookDeps) -> None:
    conn = h11.Connection(h11.SERVER)
    try:
        try:
            with trio.fail_after(REQUEST_DEADLINE_S):
                try:
                    request = await _read_request(conn, stream)
                    if request is None:
                        return
                    response = await route_slack_webhook(request, deps)
                except _BodyTooLarge:
                    response = _response(413, "payload_too_large")
                except h11.RemoteProtocolError:
                    response = _response(400, "bad_request")
                except Exception as exc:  # noqa: BLE001 - public boundary returns generic 500; logs class only.
                    log.error("slack webhook request failed exception_type=%s", type(exc).__name__)
                    response = _response(500, "internal_error")
                with contextlib.suppress(
                    trio.BrokenResourceError,
                    trio.ClosedResourceError,
                    h11.LocalProtocolError,
                ):
                    await _send_response(conn, stream, response)
        except trio.TooSlowError:
            # The hard deadline includes response transmission. Closing the
            # connection without an ACK makes Slack retry the delivery.
            pass
    finally:
        with contextlib.suppress(trio.BrokenResourceError, trio.ClosedResourceError):
            await stream.aclose()


async def serve_slack_webhook_on_listeners(
    listeners: list[trio.SocketListener],
    deps: SlackWebhookDeps,
) -> None:
    await trio.serve_listeners(partial(serve_slack_webhook_connection, deps=deps), listeners)


async def serve_slack_webhook(
    host: str,
    port: int,
    deps: SlackWebhookDeps,
    *,
    task_status: trio.TaskStatus[list[trio.SocketListener]] = trio.TASK_STATUS_IGNORED,
) -> None:
    await trio.serve_tcp(
        partial(serve_slack_webhook_connection, deps=deps),
        host=host,
        port=port,
        task_status=task_status,
    )


__all__ = [
    "MAX_SLACK_BODY_BYTES",
    "REQUEST_DEADLINE_S",
    "SlackWebhookDeps",
    "route_slack_webhook",
    "serve_slack_webhook",
    "serve_slack_webhook_connection",
    "serve_slack_webhook_on_listeners",
    "verify_slack_signature",
]
