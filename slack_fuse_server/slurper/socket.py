"""Socket-mode ingestion loop, writing events to postgres.

Lifted from `slack_fuse/socket_mode.py` (connection handshake, reconnect with
exponential backoff, graceful-vs-unclean disconnect handling) with one change:
each `events_api` envelope is no longer dispatched to an in-memory store, but
*translated to wire events and written to the `events` table* via the
offset-assignment pattern (`OffsetWriter`).

Health transitions are published on the `slurper-health` stream as they
happen:

- `slack_healthy` on the first `hello` of a fresh connection.
- `socket_mode_disconnected` when a connection ends.
- `socket_mode_reconnected {gap_seconds}` on the `hello` after a disconnect.
- `auth_token_invalid` when `apps.connections.open` reports a bad token.

Translation of a `SocketEventPayload` to wire events:

- `message` family → `channel:<id>` stream (`message` / `message_changed` /
  `message_deleted`). `message` events are deduped on `(stream, ts)`.
- channel-structure events → `channel-list` stream. The slurper owns the Slack
  token, so it enriches via `conversations.info` to produce the full channel
  object / current name / membership the wire kinds carry.

Reaction events are not subscribed to by the v1 app config, so they never
arrive here; if one ever does it is logged and ignored (the payload model
carries no reaction fields to translate).
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

from slack_fuse.models import (
    CHANNEL_LIST_EVENT_TYPES,
    GRACEFUL_DISCONNECT_REASONS,
    AppsConnectionsOpenResponse,
    Channel,
    Message,
    SocketEnvelope,
    SocketEventPayload,
)
from slack_fuse_server._json import JsonObject
from slack_fuse_server.slurper.api import SlackAPIError, SlackClient
from slack_fuse_server.slurper.health import HealthEmitter, HealthKind
from slack_fuse_server.slurper.offsets import EventRecord, OffsetWriter

log = logging.getLogger(__name__)

_OPEN_URL = "https://slack.com/api/apps.connections.open"
_RECONNECT_MIN = 2.0
_RECONNECT_MAX = 300.0

# apps.connections.open `error` values that mean the app token is bad — emit
# auth_token_invalid rather than silently retrying forever.
_AUTH_ERRORS = frozenset({"invalid_auth", "token_revoked", "not_authed", "account_inactive"})

# Slack structure-event types that imply the user lost access to the channel
# (no conversations.info enrichment possible — the channel may be gone).
_MEMBERSHIP_LOST_EVENTS = frozenset({"channel_deleted", "channel_left", "group_deleted"})
_ARCHIVE_EVENTS = frozenset({"channel_archive", "group_archive"})
_UNARCHIVE_EVENTS = frozenset({"channel_unarchive", "group_unarchive"})
_RENAME_EVENTS = frozenset({"channel_rename", "group_rename"})
_CREATE_EVENTS = frozenset({"channel_created", "im_created"})
_MEMBER_EVENTS = frozenset({"member_joined_channel", "member_left_channel"})


class _AuthFailed(Exception):
    """apps.connections.open reported a bad app token."""


def translate_message_event(event: SocketEventPayload) -> EventRecord | None:
    """Translate a `message`-type socket event to a `channel:<id>` write.

    Pure: no API calls. Returns None when the event carries no usable channel
    or timestamp (nothing to write).
    """
    channel_id = event.channel
    if not channel_id:
        return None
    stream = f"channel:{channel_id}"

    if event.subtype == "message_changed":
        new_msg = event.message
        if new_msg is None:
            return None
        payload: JsonObject = {"message": new_msg.model_dump(mode="json"), "previous_ts": new_msg.ts}
        return EventRecord(stream=stream, kind="message_changed", ts=new_msg.ts, payload=payload)

    if event.subtype == "message_deleted":
        deleted_ts = event.deleted_ts
        if not deleted_ts:
            return None
        prev = event.previous_message
        del_payload: JsonObject = {
            "deleted_ts": deleted_ts,
            "previous_message": prev.model_dump(mode="json") if prev is not None else None,
        }
        return EventRecord(stream=stream, kind="message_deleted", ts=deleted_ts, payload=del_payload)

    ts = event.ts
    if not ts:
        return None
    msg = Message(
        ts=ts,
        user=event.user or "unknown",
        text=event.text,
        thread_ts=event.thread_ts,
        subtype=event.subtype,
    )
    return EventRecord(stream=stream, kind="message", ts=ts, payload=msg.model_dump(mode="json"), dedup=True)


def _channel_added_write(channel: Channel) -> EventRecord:
    return EventRecord(stream="channel-list", kind="channel_added", ts=None, payload=channel.model_dump(mode="json"))


class SocketModeRunner:
    """Owns the Socket Mode connection lifecycle and its health transitions."""

    def __init__(
        self,
        writer: OffsetWriter,
        health: HealthEmitter,
        client: SlackClient,
        app_token: str,
    ) -> None:
        self._writer = writer
        self._health = health
        self._client = client
        self._app_token = app_token
        self._limiter = writer.limiter
        # trio clock time of the most recent disconnect; None while connected
        # for the first time (initial connect emits slack_healthy, not reconnected).
        self._disconnected_at: float | None = None

    async def run(self) -> None:
        """Keep a Socket Mode connection open for the lifetime of the nursery."""
        backoff = _RECONNECT_MIN
        while True:
            try:
                ws_url = await trio.to_thread.run_sync(self._open_socket, limiter=self._limiter)
            except _AuthFailed:
                await self._health.emit(HealthKind.AUTH_TOKEN_INVALID)
                await trio.sleep(backoff)
                backoff = min(backoff * 2.0, _RECONNECT_MAX)
                continue
            except (httpx.HTTPError, ValueError) as exc:
                log.warning("socket-mode: apps.connections.open failed: %s", exc)
                await trio.sleep(backoff)
                backoff = min(backoff * 2.0, _RECONNECT_MAX)
                continue

            graceful = await self._connect_and_run(ws_url)
            await self._health.emit(HealthKind.SOCKET_MODE_DISCONNECTED)
            self._disconnected_at = trio.current_time()
            if graceful:
                log.info("socket-mode: graceful disconnect; reconnecting")
                backoff = _RECONNECT_MIN
            else:
                log.info("socket-mode: unclean close; backing off")
                await trio.sleep(backoff)
                backoff = min(backoff * 2.0, _RECONNECT_MAX)

    def _open_socket(self) -> str:
        """Sync: POST apps.connections.open, return the websocket URL."""
        resp = self._client.http.post(
            _OPEN_URL,
            headers={"Authorization": f"Bearer {self._app_token}"},
            timeout=30.0,
        )
        resp.raise_for_status()
        parsed = AppsConnectionsOpenResponse.model_validate_json(resp.content)
        if not parsed.ok:
            if parsed.error in _AUTH_ERRORS:
                raise _AuthFailed(parsed.error)
            raise ValueError(f"apps.connections.open failed: {parsed.error or 'unknown'}")
        if not parsed.url:
            raise ValueError("apps.connections.open returned no url")
        return parsed.url

    async def _connect_and_run(self, ws_url: str) -> bool:
        """Connect, run the message loop, and report whether the close was graceful."""
        try:
            async with open_websocket_url(ws_url) as ws:
                return await self._message_loop(ws)
        except (ConnectionClosed, HandshakeError, OSError) as exc:
            log.info("socket-mode: connection ended (%s)", exc)
            return False

    async def _on_hello(self) -> None:
        if self._disconnected_at is None:
            await self._health.emit(HealthKind.SLACK_HEALTHY)
        else:
            gap = max(0.0, trio.current_time() - self._disconnected_at)
            await self._health.emit(HealthKind.SOCKET_MODE_RECONNECTED, {"gap_seconds": round(gap, 3)})
            self._disconnected_at = None

    async def _message_loop(self, ws: WebSocketConnection) -> bool:
        """Pump frames off the socket until a disconnect (or the peer closes)."""
        try:
            while True:
                message = await ws.get_message()
                envelope = _parse_envelope(message)
                if envelope is None:
                    continue
                if envelope.type == "hello":
                    log.info("socket-mode: hello (num_connections=%d)", envelope.num_connections)
                    await self._on_hello()
                    continue
                if envelope.type == "disconnect":
                    return envelope.reason in GRACEFUL_DISCONNECT_REASONS
                if envelope.envelope_id is None:
                    continue
                await ws.send_message(_ack(envelope.envelope_id))
                if envelope.type == "events_api" and envelope.payload is not None:
                    await self._handle_event(envelope.payload.event)
        except ConnectionClosed:
            return False

    async def _handle_event(self, event: SocketEventPayload) -> None:
        """Translate one socket event and write the resulting wire events."""
        if event.type == "message":
            write = translate_message_event(event)
            if write is not None:
                await self._writer.write_event(write)
            return
        if event.type in CHANNEL_LIST_EVENT_TYPES:
            await self._handle_structural_event(event)
            return
        log.debug("socket-mode: ignoring event type %s", event.type)

    async def _handle_structural_event(self, event: SocketEventPayload) -> None:
        """Translate a channel-structure event to a `channel-list` write.

        Enriches via `conversations.info` where the wire kind needs the channel
        object / name / membership. A failed enrichment skips the event (logged)
        rather than crashing the loop.
        """
        channel_id = event.channel
        if not channel_id:
            return
        write = await self._build_structural_write(event, channel_id)
        if write is not None:
            await self._writer.write_event(write)

    async def _build_structural_write(self, event: SocketEventPayload, channel_id: str) -> EventRecord | None:
        etype = event.type
        if etype in _MEMBERSHIP_LOST_EVENTS:
            payload: JsonObject = {"channel_id": channel_id, "is_member": False}
            return EventRecord(stream="channel-list", kind="channel_member_changed", ts=None, payload=payload)
        if etype in _ARCHIVE_EVENTS:
            return EventRecord(
                stream="channel-list", kind="channel_archived", ts=None, payload={"channel_id": channel_id}
            )
        if etype in _UNARCHIVE_EVENTS:
            return EventRecord(
                stream="channel-list", kind="channel_unarchived", ts=None, payload={"channel_id": channel_id}
            )

        channel = await self._fetch_channel(channel_id)
        if channel is None:
            return None
        if etype in _CREATE_EVENTS:
            return _channel_added_write(channel)
        if etype in _RENAME_EVENTS:
            payload = {"channel_id": channel_id, "new_name": channel.name}
            return EventRecord(stream="channel-list", kind="channel_renamed", ts=None, payload=payload)
        if etype in _MEMBER_EVENTS:
            payload = {"channel_id": channel_id, "is_member": channel.is_member}
            return EventRecord(stream="channel-list", kind="channel_member_changed", ts=None, payload=payload)
        log.debug("socket-mode: no structural translation for %s", etype)
        return None

    async def _fetch_channel(self, channel_id: str) -> Channel | None:
        try:
            return await trio.to_thread.run_sync(
                lambda: self._client.get_channel_info(channel_id), limiter=self._limiter
            )
        except (SlackAPIError, httpx.HTTPError):
            log.warning("socket-mode: conversations.info failed for %s", channel_id, exc_info=True)
            return None


def _parse_envelope(message: str | bytes) -> SocketEnvelope | None:
    """Validate a raw frame into a typed envelope, logging and skipping on error."""
    try:
        return SocketEnvelope.model_validate_json(message)
    except ValidationError as exc:
        log.warning("socket-mode: envelope parse error: %s", exc)
        return None


def _ack(envelope_id: str) -> str:
    return json.dumps({"envelope_id": envelope_id})


async def run_socket_mode(
    writer: OffsetWriter,
    health: HealthEmitter,
    client: SlackClient,
    app_token: str,
) -> None:
    """Entry point: build a `SocketModeRunner` and run it forever."""
    await SocketModeRunner(writer, health, client, app_token).run()
