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
from collections.abc import Callable
from dataclasses import dataclass
from typing import cast

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
    SocketEnvelope,
    SocketEventPayload,
)
from slack_fuse_server._json import JsonObject
from slack_fuse_server.slurper.api import SlackAPIError, SlackClient, Validated
from slack_fuse_server.slurper.health import HealthEmitter, HealthKind, SlackDegradedTracker
from slack_fuse_server.slurper.limiters import SlurperLimiters
from slack_fuse_server.slurper.offsets import PG_TIMEOUT_EXCEPTIONS, EventRecord, OffsetWriter

log = logging.getLogger(__name__)

_OPEN_URL = "https://slack.com/api/apps.connections.open"
_RECONNECT_MIN = 2.0
_RECONNECT_MAX = 300.0
DEFAULT_DEGRADED_MIN_DURATION_S = 30.0

# apps.connections.open `error` values that mean the app token is bad — emit
# auth_token_invalid rather than silently retrying forever.
_AUTH_ERRORS = frozenset({"invalid_auth", "token_revoked", "not_authed", "account_inactive"})

# Slack structure-event types that imply the user lost access to the channel.
# v1 mapping (RFC has no dedicated `channel_deleted` wire kind): a deleted or
# departed channel is surfaced as `channel_member_changed{is_member: False}`,
# i.e. "you can no longer see this channel" — the same signal as being removed.
# `conversations.info` enrichment is skipped because the channel may be gone.
_MEMBERSHIP_LOST_EVENTS = frozenset({"channel_deleted", "channel_left", "group_deleted"})
_ARCHIVE_EVENTS = frozenset({"channel_archive", "group_archive"})
_UNARCHIVE_EVENTS = frozenset({"channel_unarchive", "group_unarchive"})
_RENAME_EVENTS = frozenset({"channel_rename", "group_rename"})
_CREATE_EVENTS = frozenset({"channel_created", "im_created"})
_MEMBER_EVENTS = frozenset({"member_joined_channel", "member_left_channel"})


class _AuthFailed(Exception):
    """apps.connections.open reported a bad app token."""


@dataclass(slots=True)
class SocketModeStatus:
    """Live socket-mode connection state, surfaced in `/metrics`.

    Mutated only from the trio event loop by `SocketModeRunner`; read from the
    same loop by the metrics provider, so no locking is needed.
    """

    state: str = "connecting"


@dataclass(frozen=True, slots=True)
class SocketModeOptions:
    """Tunables threaded into the socket-mode runner from server config.

    Bundled so the runner constructor and entry points stay within the
    argument-count budget. `status` is shared with the metrics layer when the
    integrated server wires `/metrics`; left `None` (a fresh holder) in tests.

    `on_reconnect` is invoked from the trio loop with the downtime in seconds
    each time a connection re-establishes after a disconnect (never on the
    first connect). The slurper wires it to the reconnect-catchup trigger; left
    `None` in tests and when catchup is disabled.
    """

    degraded_min_duration_s: float = DEFAULT_DEGRADED_MIN_DURATION_S
    status: SocketModeStatus | None = None
    on_reconnect: Callable[[float], None] | None = None


def _classify_open_failure(exc: BaseException) -> str:
    """Map an `apps.connections.open` failure to a `slack_degraded` reason."""
    if isinstance(exc, httpx.TimeoutException):
        return "timeout"
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        if status == 429:
            return "rate_limited"
        if 500 <= status < 600:
            return "api_5xx"
        return "api_error"
    if isinstance(exc, httpx.HTTPError):
        return "network"
    return "api_error"


def translate_message_event(event: SocketEventPayload, raw_event: JsonObject) -> EventRecord | None:
    """Translate a `message`-type socket event to a `channel:<id>` write.

    Persists the RAW event dict (lossless) — ``event`` is used only for
    in-process logic (membership / typing checks), the payload comes from
    ``raw_event``. Pydantic ``model_dump`` reshapes nested fields and
    drops anything we don't declare; the events table stays the source
    of truth so future projections can read fields we don't know yet.

    Pure: no API calls. Returns None when the event carries no usable
    channel or timestamp (nothing to write).
    """
    channel_id = event.channel
    if not channel_id:
        return None
    stream = f"channel:{channel_id}"

    if event.subtype == "message_changed":
        new_msg = event.message
        if new_msg is None:
            return None
        # Persist the raw nested message + the previous_ts marker. The
        # nested message dict is the wire shape under
        # ``raw_event["message"]``; defensively fall back to a dump if
        # the wire payload is shaped unexpectedly.
        raw_msg = raw_event.get("message")
        if isinstance(raw_msg, dict):
            msg_dict: JsonObject = cast(JsonObject, raw_msg)
        else:
            msg_dict = cast(JsonObject, new_msg.model_dump(mode="json"))
        payload: JsonObject = {"message": msg_dict, "previous_ts": new_msg.ts}
        return EventRecord(stream=stream, kind="message_changed", ts=new_msg.ts, payload=payload)

    if event.subtype == "message_deleted":
        deleted_ts = event.deleted_ts
        if not deleted_ts:
            return None
        prev = event.previous_message
        raw_prev = raw_event.get("previous_message")
        prev_dict: JsonObject | None
        if isinstance(raw_prev, dict):
            prev_dict = cast(JsonObject, raw_prev)
        elif prev is not None:
            prev_dict = cast(JsonObject, prev.model_dump(mode="json"))
        else:
            prev_dict = None
        del_payload: JsonObject = {"deleted_ts": deleted_ts, "previous_message": prev_dict}
        return EventRecord(stream=stream, kind="message_deleted", ts=deleted_ts, payload=del_payload)

    ts = event.ts
    if not ts:
        return None
    # For the top-level "message" subtype, the raw event dict already IS
    # the message shape (after _normalize_message_event flattens it for
    # validation). Persist as-is.
    nested = raw_event.get("message")
    msg_payload: JsonObject = cast(JsonObject, nested) if isinstance(nested, dict) else raw_event
    return EventRecord(stream=stream, kind="message", ts=ts, payload=msg_payload, dedup=True)


def extract_raw_event(raw_envelope: JsonObject) -> JsonObject:
    """Pull the raw per-event dict out of the events_api envelope. Returns
    an empty dict if the envelope shape is unexpected — the typed model
    has already passed validation by the time we call this, so a missing
    event field is genuinely unusual but shouldn't crash the loop."""
    payload = raw_envelope.get("payload")
    if isinstance(payload, dict):
        event = payload.get("event")
        if isinstance(event, dict):
            return cast(JsonObject, event)
    return {}


def _channel_added_write(channel_raw: JsonObject) -> EventRecord:
    """Persist the RAW channel dict (lossless). See the
    ``_insert_channel_added`` docstring in ``slurper/channels.py`` for the
    full rationale — Pydantic ``model_dump`` reshapes nested fields and
    silently drops anything we haven't declared, so we keep the wire dict.
    """
    return EventRecord(stream="channel-list", kind="channel_added", ts=None, payload=channel_raw)


def _record_channel_id(record: EventRecord) -> str | None:
    if record.stream.startswith("channel:"):
        return record.stream.removeprefix("channel:")
    channel_id = record.payload.get("channel_id")
    return channel_id if isinstance(channel_id, str) else None


class SocketModeRunner:
    """Owns the Socket Mode connection lifecycle and its health transitions."""

    def __init__(  # noqa: PLR0913 - runner dependencies are explicit and shared with tests.
        self,
        writer: OffsetWriter,
        health: HealthEmitter,
        client: SlackClient,
        app_token: str,
        *,
        limiters: SlurperLimiters,
        options: SocketModeOptions | None = None,
    ) -> None:
        options = options or SocketModeOptions()
        self._writer = writer
        self._health = health
        self._client = client
        self._app_token = app_token
        self._limiters = limiters
        # trio clock time of the most recent disconnect; None while connected
        # for the first time (initial connect emits slack_healthy, not reconnected).
        self._disconnected_at: float | None = None
        self._degraded = SlackDegradedTracker(health, options.degraded_min_duration_s)
        self._status = options.status if options.status is not None else SocketModeStatus()
        self._on_reconnect = options.on_reconnect

    async def run(self) -> None:
        """Keep a Socket Mode connection open for the lifetime of the nursery."""
        backoff = _RECONNECT_MIN
        while True:
            self._status.state = "connecting"
            try:
                ws_url = await trio.to_thread.run_sync(self._open_socket, limiter=self._limiters.slack_api)
            except _AuthFailed:
                self._status.state = "auth_failed"
                await self._health.emit(HealthKind.AUTH_TOKEN_INVALID)
                await trio.sleep(backoff)
                backoff = min(backoff * 2.0, _RECONNECT_MAX)
                continue
            except (httpx.HTTPError, ValueError) as exc:
                self._status.state = "disconnected"
                reason = _classify_open_failure(exc)
                log.warning("socket-mode: apps.connections.open failed (%s): %s", reason, exc)
                await self._degraded.record_failure(reason)
                await trio.sleep(backoff)
                backoff = min(backoff * 2.0, _RECONNECT_MAX)
                continue

            graceful = await self._connect_and_run(ws_url)
            self._status.state = "disconnected"
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
        self._status.state = "connected"
        self._degraded.record_healthy()
        if self._disconnected_at is None:
            await self._health.emit(HealthKind.SLACK_HEALTHY)
        else:
            gap = max(0.0, trio.current_time() - self._disconnected_at)
            await self._health.emit(HealthKind.SOCKET_MODE_RECONNECTED, {"gap_seconds": round(gap, 3)})
            self._disconnected_at = None
            # Hand the downtime to the catchup trigger (if wired). The handler
            # decides whether the gap is long enough to warrant a gap-fill;
            # it never blocks the socket loop (a non-blocking nudge).
            if self._on_reconnect is not None:
                self._on_reconnect(gap)

    async def _message_loop(self, ws: WebSocketConnection) -> bool:
        """Pump frames off the socket until a disconnect (or the peer closes)."""
        try:
            while True:
                message = await ws.get_message()
                parsed = _parse_envelope(message)
                if parsed is None:
                    continue
                envelope, raw_envelope = parsed
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
                    # Pull the raw per-event dict alongside the validated
                    # model so message persistence is lossless.
                    raw_event = extract_raw_event(raw_envelope)
                    await self._handle_event(envelope.payload.event, raw_event)
        except ConnectionClosed:
            return False

    async def _handle_event(self, event: SocketEventPayload, raw_event: JsonObject) -> None:
        """Translate one socket event and write the resulting wire events."""
        if event.type == "message":
            write = translate_message_event(event, raw_event)
            if write is not None:
                await self._write_event_or_drop_timeout(write)
            return
        if event.type in CHANNEL_LIST_EVENT_TYPES:
            await self._handle_structural_event(event)
            return
        log.debug("socket-mode: ignoring event type %s", event.type)

    async def _write_event_or_drop_timeout(self, record: EventRecord) -> None:
        try:
            await self._writer.write_event(record)
        except PG_TIMEOUT_EXCEPTIONS:
            log.warning(
                "socket-mode: dropped event after PostgreSQL timeout stream=%s kind=%s channel_id=%s",
                record.stream,
                record.kind,
                _record_channel_id(record),
                exc_info=True,
            )

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
            await self._write_event_or_drop_timeout(write)

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

        validated = await self._fetch_channel(channel_id)
        if validated is None:
            return None
        channel = validated.model
        if etype in _CREATE_EVENTS:
            return _channel_added_write(validated.raw)
        if etype in _RENAME_EVENTS:
            payload = {"channel_id": channel_id, "new_name": channel.name}
            return EventRecord(stream="channel-list", kind="channel_renamed", ts=None, payload=payload)
        if etype in _MEMBER_EVENTS:
            payload = {"channel_id": channel_id, "is_member": channel.is_member}
            return EventRecord(stream="channel-list", kind="channel_member_changed", ts=None, payload=payload)
        log.debug("socket-mode: no structural translation for %s", etype)
        return None

    async def _fetch_channel(self, channel_id: str) -> Validated[Channel] | None:
        try:
            return await trio.to_thread.run_sync(
                lambda: self._client.get_channel_info(channel_id), limiter=self._limiters.slack_api
            )
        except (SlackAPIError, httpx.HTTPError):
            log.warning("socket-mode: conversations.info failed for %s", channel_id, exc_info=True)
            return None


def _normalize_message_event(envelope: JsonObject) -> JsonObject:
    """Mirror top-level `message` fields into `event.message` when omitted.

    Slack's top-level `message` events carry message fields directly on the
    `event` object, while `message_changed`/`message_deleted` place a nested
    `message` object. `SocketEventPayload` does not declare all message-shaped
    fields, so we inject the top-level event as `event.message` before model
    validation to preserve the full payload via the shared `Message` model.
    """
    payload_raw = envelope.get("payload")
    if not isinstance(payload_raw, dict):
        return envelope
    payload = cast("dict[str, object]", payload_raw)
    event_raw = payload.get("event")
    if not isinstance(event_raw, dict):
        return envelope
    event = cast("dict[str, object]", event_raw)
    if event.get("type") != "message" or "message" in event:
        return envelope
    message_payload: dict[str, object] = {**event}
    normalized_event: dict[str, object] = {**event, "message": message_payload}
    normalized_payload: dict[str, object] = {**payload, "event": normalized_event}
    normalized_envelope: dict[str, object] = {**envelope, "payload": normalized_payload}
    return cast(JsonObject, normalized_envelope)


def _parse_envelope(message: str | bytes) -> tuple[SocketEnvelope, JsonObject] | None:
    """Validate a raw frame into a typed envelope, returning the validated
    model alongside the raw normalized envelope dict.

    The raw is what gets persisted by the message handlers — Pydantic
    ``model_dump`` reshapes nested fields and drops anything we don't
    declare, so we keep the wire dict and use the model only for in-process
    logic.
    """
    try:
        raw = json.loads(message)
    except json.JSONDecodeError as exc:
        log.warning("socket-mode: envelope parse error: %s", exc)
        return None
    if not isinstance(raw, dict):
        log.warning("socket-mode: envelope parse error: expected JSON object frame")
        return None
    normalized = _normalize_message_event(cast(JsonObject, raw))
    try:
        return SocketEnvelope.model_validate(normalized), normalized
    except ValidationError as exc:
        log.warning("socket-mode: envelope parse error: %s", exc)
        return None


def _ack(envelope_id: str) -> str:
    return json.dumps({"envelope_id": envelope_id})


async def run_socket_mode(  # noqa: PLR0913 - public wrapper mirrors runner dependencies.
    writer: OffsetWriter,
    health: HealthEmitter,
    client: SlackClient,
    app_token: str,
    *,
    limiters: SlurperLimiters,
    options: SocketModeOptions | None = None,
) -> None:
    """Entry point: build a `SocketModeRunner` and run it forever."""
    await SocketModeRunner(writer, health, client, app_token, limiters=limiters, options=options).run()
