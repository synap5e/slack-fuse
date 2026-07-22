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
  `message_deleted` / `parent_replied`). `message` events are deduped on
  `(stream, ts)`; `parent_replied` is deduped on parent ts + reply count so
  socket replays collapse without losing each reply-count transition.
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
from typing import TYPE_CHECKING, Protocol, cast
from uuid import uuid4

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
    GRACEFUL_DISCONNECT_REASONS,
    AppsConnectionsOpenResponse,
    EventsApiPayload,
    SocketEnvelope,
    SocketEventPayload,
)
from slack_fuse_server._json import JsonObject
from slack_fuse_server.slack_events.types import (
    DispatchErrorCode,
    DispatchPermanentError,
    DispatchTransientError,
    SlackEventSource,
)
from slack_fuse_server.slurper.api import FatalAPIError, SlackAPIError, SlackClient
from slack_fuse_server.slurper.health import HealthEmitter, HealthKind, SlackDegradedTracker
from slack_fuse_server.slurper.ingestion import make_source
from slack_fuse_server.slurper.limiters import SlurperLimiters
from slack_fuse_server.slurper.offsets import EventRecord, OffsetWriter
from slack_fuse_server.slurper.spans import run_sync_with_span, span
from slack_fuse_server.slurper.supervisor import TaskSupervisor, phase

if TYPE_CHECKING:
    from slack_fuse_server.slurper.spans import SpanRecorder

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
_RAW_CHANNEL_LIST_EVENTS = frozenset({"channel_history_changed", "channel_id_changed"})
_TOKEN_REVOKED_EVENT = "tokens_revoked"


class _AuthFailed(Exception):
    """apps.connections.open reported a bad app token."""


class SlackEventDispatcherProtocol(Protocol):
    async def dispatch(
        self,
        payload: EventsApiPayload,
        raw_event: JsonObject,
        source_ctx: SlackEventSource,
        span: SpanRecorder | None = None,
    ) -> None: ...


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

    `on_self_join` queues history backfill after the channel inventory has
    been seeded. `self_user_id` is a test seam; production discovers it with
    `auth.test` before opening the first Socket Mode connection.
    """

    degraded_min_duration_s: float = DEFAULT_DEGRADED_MIN_DURATION_S
    status: SocketModeStatus | None = None
    on_reconnect: Callable[[float], None] | None = None
    on_self_join: Callable[[str], bool] | None = None
    self_user_id: str | None = None


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


def _str_or_none(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _event_ts(raw_event: JsonObject) -> str | None:
    """Best-effort event timestamp from the inner Slack event payload.

    Most Events API event objects carry `event_ts`; a few older examples only
    show `ts`. If neither exists we leave the field null rather than inventing a
    timestamp, because replay-dedup indexes should not collapse distinct real
    events that Slack failed to timestamp.
    """
    return _str_or_none(raw_event.get("event_ts")) or _str_or_none(raw_event.get("ts"))


def _latest_reply_from_parent(parent: JsonObject) -> str | None:
    latest = _str_or_none(parent.get("latest_reply"))
    if latest is not None:
        return latest
    replies = parent.get("replies")
    if not isinstance(replies, list) or not replies:
        return None
    last = replies[-1]
    if not isinstance(last, dict):
        return None
    return _str_or_none(last.get("ts"))


def _looks_like_message_replied_without_subtype(event: SocketEventPayload, raw_event: JsonObject) -> bool:
    """Detect Slack's Events API `message_replied` subtype omission.

    Slack's docs currently warn that Events API dispatch can omit
    `subtype=message_replied`. In that shape the event still carries a nested
    parent `message` object with refreshed thread metadata; treating it as a
    normal message would collide with the original parent row and be dropped by
    message dedup. The shape check stays conservative so ordinary top-level
    messages and edit/delete subtypes keep their existing semantics.
    """
    if event.subtype is not None:
        return False
    parent = raw_event.get("message")
    if not isinstance(parent, dict):
        return False
    parent_ts = _str_or_none(parent.get("ts"))
    top_level_ts = _str_or_none(raw_event.get("ts"))
    if parent_ts is None or top_level_ts == parent_ts:
        return False
    return "reply_count" in parent or "latest_reply" in parent or "replies" in parent


def _build_parent_replied_write(event: SocketEventPayload, raw_event: JsonObject, stream: str) -> EventRecord | None:
    """Build a `parent_replied` event from a refreshed thread parent.

    We choose schema-backed replay dedup on `(stream, kind, parent_ts,
    reply_count)`. That preserves every observed reply-count transition while
    collapsing identical socket replays. If Slack emitted two distinct parent
    refreshes with the same count but different ancillary metadata, the latter
    would be treated as a replay; that is acceptable for v1 because the capture
    guarantee here is specifically the parent count/latest-reply signal.
    """
    parent_raw = raw_event.get("message")
    if isinstance(parent_raw, dict):
        parent: JsonObject = cast(JsonObject, parent_raw)
    elif event.message is not None:
        parent = cast(JsonObject, event.message.model_dump(mode="json"))
    else:
        return None

    parent_ts = _str_or_none(parent.get("ts"))
    if parent_ts is None:
        return None
    reply_count_raw = parent.get("reply_count")
    reply_count = reply_count_raw if isinstance(reply_count_raw, int) else None
    payload: JsonObject = {
        "channel_id": stream.removeprefix("channel:"),
        "parent_ts": parent_ts,
        "reply_count": reply_count,
        "latest_reply": _latest_reply_from_parent(parent),
        "probed_at": _event_ts(raw_event),
        "message": parent,
    }
    return EventRecord(
        stream=stream,
        kind="parent_replied",
        ts=parent_ts,
        payload=payload,
        dedup=True,
        source=make_source(slack_event_ts=_event_ts(raw_event)),
    )


def _build_message_changed_write(
    event: SocketEventPayload,
    raw_event: JsonObject,
    stream: str,
) -> EventRecord | None:
    new_msg = event.message
    if new_msg is None:
        return None
    # Persist the raw nested message + the previous_ts marker. The nested
    # message dict is the wire shape under ``raw_event["message"]``;
    # defensively fall back to a dump if the wire payload is shaped
    # unexpectedly.
    raw_msg = raw_event.get("message")
    if isinstance(raw_msg, dict):
        msg_dict: JsonObject = cast(JsonObject, raw_msg)
    else:
        msg_dict = cast(JsonObject, new_msg.model_dump(mode="json"))
    payload: JsonObject = {"message": msg_dict, "previous_ts": new_msg.ts}
    return EventRecord(
        stream=stream,
        kind="message_changed",
        ts=new_msg.ts,
        payload=payload,
        # The outer event's event_ts: the EDIT time, distinct from the edited
        # message's own ts.
        source=make_source(slack_event_ts=_event_ts(raw_event)),
    )


def _build_message_deleted_write(
    event: SocketEventPayload,
    raw_event: JsonObject,
    stream: str,
) -> EventRecord | None:
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
    return EventRecord(
        stream=stream,
        kind="message_deleted",
        ts=deleted_ts,
        payload=del_payload,
        source=make_source(slack_event_ts=_event_ts(raw_event)),
    )


def _build_message_write(event: SocketEventPayload, raw_event: JsonObject, stream: str) -> EventRecord | None:
    ts = event.ts
    if not ts:
        return None
    # For the top-level "message" subtype, the raw event dict already IS
    # the message shape (after _normalize_message_event flattens it for
    # validation). Persist as-is.
    nested = raw_event.get("message")
    msg_payload: JsonObject = cast(JsonObject, nested) if isinstance(nested, dict) else raw_event
    return EventRecord(
        stream=stream,
        kind="message",
        ts=ts,
        payload=msg_payload,
        dedup=True,
        source=make_source(slack_event_ts=_event_ts(raw_event)),
    )


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

    if event.subtype == "message_replied" or _looks_like_message_replied_without_subtype(event, raw_event):
        return _build_parent_replied_write(event, raw_event, stream)

    if event.subtype == "message_changed":
        return _build_message_changed_write(event, raw_event, stream)

    if event.subtype == "message_deleted":
        return _build_message_deleted_write(event, raw_event, stream)

    return _build_message_write(event, raw_event, stream)


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


def channel_added_write(channel_raw: JsonObject) -> EventRecord:
    """Persist the RAW channel dict (lossless). See the
    ``_insert_channel_added`` docstring in ``slurper/channels.py`` for the
    full rationale — Pydantic ``model_dump`` reshapes nested fields and
    silently drops anything we haven't declared, so we keep the wire dict.
    """
    # ``dedup=True`` matches every other structural socket writer + the
    # ``events_channels_added_dedup`` unique index shipped in migration 0003.
    # Without it, a Slack Socket Mode redelivery (or a race with startup
    # ``populate_channels_once``) turns a benign replay into a UniqueViolation
    # that tears down the slurper. Nothing catches UniqueViolation; on
    # restart Slack redelivers → crash loop. Pinned by
    # ``test_channel_added_dedup_survives_redelivery`` (FINDING-03).
    return EventRecord(stream="channel-list", kind="channel_added", ts=None, payload=channel_raw, dedup=True)


def _channel_id_changed_write(raw_event: JsonObject) -> EventRecord | None:
    old_channel_id = _str_or_none(raw_event.get("old_channel_id"))
    new_channel_id = _str_or_none(raw_event.get("new_channel_id"))
    if old_channel_id is None or new_channel_id is None:
        return None
    payload: JsonObject = {
        "old_channel_id": old_channel_id,
        "new_channel_id": new_channel_id,
        "event_ts": _event_ts(raw_event),
    }
    return EventRecord(stream="channel-list", kind="channel_id_changed", ts=None, payload=payload, dedup=True)


def _channel_history_changed_write(event: SocketEventPayload, raw_event: JsonObject) -> EventRecord:
    channel_id = event.channel or _str_or_none(raw_event.get("channel")) or _str_or_none(raw_event.get("channel_id"))
    payload: JsonObject = {
        "channel_id": channel_id,
        "latest": _str_or_none(raw_event.get("latest")),
        "ts": _str_or_none(raw_event.get("ts")),
        "event_ts": _event_ts(raw_event),
    }
    return EventRecord(stream="channel-list", kind="channel_history_changed", ts=None, payload=payload, dedup=True)


def _member_event_write(event: SocketEventPayload, raw_event: JsonObject) -> EventRecord | None:
    if event.type not in _MEMBER_EVENTS:
        return None
    user_id = _str_or_none(raw_event.get("user")) or event.user
    if not event.channel or not user_id:
        return None
    payload: JsonObject = {
        "channel_id": event.channel,
        "user_id": user_id,
        "inviter_id": _str_or_none(raw_event.get("inviter")),
        "event_ts": _event_ts(raw_event),
    }
    kind = "channel_member_joined" if event.type == "member_joined_channel" else "channel_member_left"
    return EventRecord(stream="channel-list", kind=kind, ts=None, payload=payload, dedup=True)


def raw_channel_list_write(event: SocketEventPayload, raw_event: JsonObject) -> EventRecord | None:
    if event.type == "channel_id_changed":
        return _channel_id_changed_write(raw_event)
    if event.type == "channel_history_changed":
        return _channel_history_changed_write(event, raw_event)
    return _member_event_write(event, raw_event)


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
        dispatcher: SlackEventDispatcherProtocol | None = None,
        options: SocketModeOptions | None = None,
        supervisor: TaskSupervisor | None = None,
    ) -> None:
        options = options or SocketModeOptions()
        self._writer = writer
        self._health = health
        self._client = client
        self._app_token = app_token
        self._limiters = limiters
        self._supervisor = supervisor or TaskSupervisor()
        # trio clock time of the most recent disconnect; None while connected
        # for the first time (initial connect emits slack_healthy, not reconnected).
        self._disconnected_at: float | None = None
        self._degraded = SlackDegradedTracker(health, options.degraded_min_duration_s)
        self._status = options.status if options.status is not None else SocketModeStatus()
        self._on_reconnect = options.on_reconnect
        self._on_self_join = options.on_self_join
        self._self_user_id = options.self_user_id
        if dispatcher is None:
            # Compatibility for direct runner unit tests. Production discovers
            # self identity at boot and injects one shared dispatcher.
            from slack_fuse_server.slack_events.dispatcher import SlackEventDispatcher  # noqa: PLC0415

            dispatcher = SlackEventDispatcher(
                writer,
                client,
                options.self_user_id or "unknown-self-user",
                limiters,
                health,
                options.on_self_join,
            )
        self._dispatcher = dispatcher

    @property
    def self_user_id(self) -> str | None:
        """Slack user id represented by the user token, once identified."""
        return self._self_user_id

    async def run(self) -> None:
        """Keep a Socket Mode connection open for the lifetime of the nursery."""
        backoff = _RECONNECT_MIN
        while True:
            self._status.state = "connecting"
            self._supervisor.declare("socket", "connecting", deadline_s=15)
            try:
                async with span(op="slurper.socket.reconnect", task="socket") as reconnect_span:
                    ws_url = await run_sync_with_span(
                        self._open_socket,
                        limiter=self._limiters.slack_api,
                        span=reconnect_span,
                    )
            except _AuthFailed:
                self._status.state = "auth_failed"
                await self._health.emit(HealthKind.AUTH_TOKEN_INVALID)
                self._supervisor.declare("socket", "reconnecting", deadline_s=None)
                await trio.sleep(backoff)
                backoff = min(backoff * 2.0, _RECONNECT_MAX)
                continue
            except (httpx.HTTPError, ValueError) as exc:
                self._status.state = "disconnected"
                reason = _classify_open_failure(exc)
                log.warning("socket-mode: apps.connections.open failed (%s): %s", reason, exc)
                await self._degraded.record_failure(reason)
                self._supervisor.declare("socket", "reconnecting", deadline_s=None)
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
                self._supervisor.declare("socket", "reconnecting", deadline_s=None)
                await trio.sleep(backoff)
                backoff = min(backoff * 2.0, _RECONNECT_MAX)

    def _open_socket(self) -> str:
        """Sync: POST apps.connections.open, return the websocket URL."""
        if self._self_user_id is None:
            try:
                self._self_user_id = self._client.auth_test()
            except FatalAPIError as exc:
                raise _AuthFailed(str(exc)) from exc
            except SlackAPIError as exc:
                raise ValueError(f"auth.test failed: {exc}") from exc
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
                self._supervisor.declare("socket", "connected_waiting_for_frame", deadline_s=None)
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
                    await self._handle_event(envelope.payload, raw_event)
        except ConnectionClosed:
            return False

    async def _handle_event(
        self,
        payload_or_event: EventsApiPayload | SocketEventPayload,
        raw_event: JsonObject,
    ) -> None:
        """Attach Socket Mode span/phase state and delegate event routing."""
        if isinstance(payload_or_event, SocketEventPayload):
            event = payload_or_event
            # Direct legacy test/caller invocations have no outer Slack
            # envelope. Production always passes EventsApiPayload.event_id.
            payload = EventsApiPayload(event_id=f"legacy-call:{uuid4()}", event=event)
        else:
            payload = payload_or_event
            event = payload.event
            if event is None:
                log.warning("socket-mode: ignored events_api envelope without inner event")
                return
        extra: JsonObject = {"kind": event.type}
        if event.channel:
            extra["channel_id"] = event.channel
        async with (
            span(op="slurper.socket.handle_event", task="socket", extra=extra) as event_span,
            phase(
                self._supervisor,
                "socket",
                "handling_event",
                details={"kind": event.type},
                deadline_s=10,
            ),
        ):
            try:
                await self._dispatcher.dispatch(
                    payload,
                    raw_event,
                    SlackEventSource(transport="socket", event_id=payload.event_id),
                    span=event_span,
                )
            except DispatchTransientError as exc:
                if exc.code is DispatchErrorCode.PG_TIMEOUT:
                    event_span.mark_timeout()
                    log.warning(
                        "socket-mode: dropped event after PostgreSQL timeout stream=%s kind=%s channel_id=%s code=%s",
                        f"channel:{event.channel}" if event.type == "message" and event.channel else "unknown",
                        event.type,
                        event.channel or None,
                        exc.code.value,
                    )
                    return
                if exc.code is DispatchErrorCode.CONVERSATIONS_INFO_FAILED:
                    log.warning(
                        "socket-mode: conversations.info failed for %s; dropped event code=%s",
                        event.channel or "unknown-channel",
                        exc.code.value,
                    )
                    return
                log.warning(
                    "socket-mode: dropped event after dispatch failure kind=%s channel_id=%s code=%s",
                    event.type,
                    event.channel or None,
                    exc.code.value,
                )
            except DispatchPermanentError as exc:
                log.error(
                    "socket-mode: rejected event kind=%s channel_id=%s code=%s",
                    event.type,
                    event.channel or None,
                    exc.code.value,
                )


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
        log.warning("socket-mode: envelope validation failed exception_type=%s", type(exc).__name__)
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
    dispatcher: SlackEventDispatcherProtocol | None = None,
    options: SocketModeOptions | None = None,
    supervisor: TaskSupervisor | None = None,
) -> None:
    """Entry point: build a `SocketModeRunner` and run it forever."""
    await SocketModeRunner(
        writer,
        health,
        client,
        app_token,
        limiters=limiters,
        dispatcher=dispatcher,
        options=options,
        supervisor=supervisor,
    ).run()
