"""Socket-mode push liveness loop.

Opens a Slack Socket Mode websocket and hands each `events_api` envelope off
to `SlackStore.apply_event`. The connection-open handshake reuses the shared
sync `httpx.Client` through `trio.to_thread.run_sync`; the websocket itself
is trio-native via `trio-websocket`.

`disconnect` envelopes with a reason in `GRACEFUL_DISCONNECT_REASONS` are
Slack's cue that the peer is about to close cleanly. We simply reconnect in
the next loop iteration without flushing the in-memory event log. Any other
close path — unexpected `ConnectionClosed`, handshake failure, bad
disconnect reason — is treated as a gap: we flush the event log so the
existing polling TTL becomes the next source of truth.

This task owns its own error handling; it is started inside the mount
nursery and exits cleanly when the nursery is cancelled.
"""

from __future__ import annotations

import json
import logging

import httpx
import trio
from pydantic import ValidationError
from trio_websocket import (
    ConnectionClosed,
    HandshakeError,
    WebSocketConnection,
    open_websocket_url,
)

from .models import GRACEFUL_DISCONNECT_REASONS, AppsConnectionsOpenResponse, SocketEnvelope
from .store import SlackStore

log = logging.getLogger(__name__)

_OPEN_URL = "https://slack.com/api/apps.connections.open"
_RECONNECT_MIN = 2.0
_RECONNECT_MAX = 300.0


async def run_socket_mode(
    store: SlackStore,
    app_token: str,
    http: httpx.Client,
    limiter: trio.CapacityLimiter,
) -> None:
    """Keep a Slack Socket Mode connection open for the lifetime of the nursery."""
    backoff = _RECONNECT_MIN
    while True:
        try:
            ws_url = await trio.to_thread.run_sync(_open_socket, app_token, http, limiter=limiter)
        except (httpx.HTTPError, ValueError) as exc:
            log.warning("socket-mode: apps.connections.open failed: %s", exc)
            await trio.sleep(backoff)
            backoff = min(backoff * 2.0, _RECONNECT_MAX)
            continue

        graceful = await _connect_and_run(ws_url, store, limiter)
        if not graceful:
            log.info("socket-mode: unclean close; flushing event log")
            await trio.to_thread.run_sync(store.flush_event_logs, limiter=limiter)
            await trio.sleep(backoff)
            backoff = min(backoff * 2.0, _RECONNECT_MAX)
            continue

        log.info("socket-mode: graceful disconnect; reconnecting without flush")
        backoff = _RECONNECT_MIN


def _open_socket(app_token: str, http: httpx.Client) -> str:
    """Sync: POST apps.connections.open, return the websocket URL."""
    resp = http.post(
        _OPEN_URL,
        headers={"Authorization": f"Bearer {app_token}"},
        timeout=30.0,
    )
    resp.raise_for_status()
    parsed = AppsConnectionsOpenResponse.model_validate_json(resp.content)
    if not parsed.ok or not parsed.url:
        msg = f"apps.connections.open failed: {parsed.error or 'missing url'}"
        raise ValueError(msg)
    return parsed.url


async def _connect_and_run(
    ws_url: str,
    store: SlackStore,
    limiter: trio.CapacityLimiter,
) -> bool:
    """Connect, run the message loop, and report whether the close was graceful."""
    try:
        async with open_websocket_url(ws_url) as ws:
            return await _message_loop(ws, store, limiter)
    except (ConnectionClosed, HandshakeError, OSError) as exc:
        log.info("socket-mode: connection ended (%s)", exc)
        return False


async def _message_loop(
    ws: WebSocketConnection,
    store: SlackStore,
    limiter: trio.CapacityLimiter,
) -> bool:
    """Pump frames off the socket until a disconnect (or the peer closes)."""
    try:
        while True:
            message = await ws.get_message()
            envelope = _parse_envelope(message)
            if envelope is None:
                continue
            if envelope.type == "hello":
                log.info(
                    "socket-mode: hello (num_connections=%d)",
                    envelope.num_connections,
                )
                continue
            if envelope.type == "disconnect":
                return envelope.reason in GRACEFUL_DISCONNECT_REASONS
            if envelope.envelope_id is None:
                continue
            await ws.send_message(_ack(envelope.envelope_id))
            if envelope.type == "events_api" and envelope.payload is not None:
                await trio.to_thread.run_sync(
                    store.apply_event,
                    envelope.payload.event,
                    limiter=limiter,
                )
    except ConnectionClosed:
        return False


def _parse_envelope(message: str | bytes) -> SocketEnvelope | None:
    """Validate a raw frame into a typed envelope, logging and skipping on error."""
    try:
        return SocketEnvelope.model_validate_json(message)
    except ValidationError as exc:
        log.warning("socket-mode: envelope parse error: %s", exc)
        return None


def _ack(envelope_id: str) -> str:
    return json.dumps({"envelope_id": envelope_id})
