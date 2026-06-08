"""Users-stream ingestion for the slurper.

Closes the Sprint-1A deferral: the server now emits `users` stream events from
both a startup one-shot users.list pass and live `user_change` socket events.

- Startup: one `user_added` event per workspace user (idempotent on restart).
- Live: `user_change` emits `user_renamed` and/or `user_profile_changed`.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import cast

import httpx
import trio
from psycopg import Cursor
from psycopg.rows import TupleRow
from pydantic import ValidationError
from trio_websocket import ConnectionClosed, WebSocketConnection

from slack_fuse.models import (
    GRACEFUL_DISCONNECT_REASONS,
    SlackUser,
    SocketEnvelope,
    SocketEventPayload,
    UsersInfoResponse,
    UsersListResponse,
)
from slack_fuse_server._json import JsonObject
from slack_fuse_server.slurper.api import SlackAPIError, SlackClient
from slack_fuse_server.slurper.health import HealthEmitter
from slack_fuse_server.slurper.offsets import EventRecord, OffsetWriter, assign_offset, insert_event
from slack_fuse_server.slurper.socket import SocketModeOptions, SocketModeRunner

log = logging.getLogger(__name__)

_USERS_STREAM = "users"
_USERS_LIST_URL = "https://slack.com/api/users.list"
_USERS_INFO_URL = "https://slack.com/api/users.info"


@dataclass(frozen=True, slots=True)
class _UserState:
    display_name: str
    profile_fields: JsonObject


def _user_payload(user: SlackUser) -> JsonObject:
    return cast(JsonObject, user.model_dump(mode="json"))


def _profile_payload(user: SlackUser) -> JsonObject:
    return cast(JsonObject, user.profile.model_dump(mode="json"))


def _fetch_workspace_users(client: SlackClient) -> list[SlackUser]:
    users: list[SlackUser] = []
    cursor = ""
    while True:
        params: dict[str, str] = {"limit": "200"}
        if cursor:
            params["cursor"] = cursor
        resp = client.http.get(_USERS_LIST_URL, params=params, timeout=30.0)
        resp.raise_for_status()
        parsed = UsersListResponse.model_validate_json(resp.content)
        if not parsed.ok:
            raise SlackAPIError(f"users.list failed: {parsed.error or 'unknown'}")
        users.extend(parsed.members)
        cursor = parsed.response_metadata.next_cursor
        if not cursor:
            break
    return users


def _fetch_user(client: SlackClient, user_id: str) -> SlackUser:
    resp = client.http.get(_USERS_INFO_URL, params={"user": user_id}, timeout=30.0)
    resp.raise_for_status()
    parsed = UsersInfoResponse.model_validate_json(resp.content)
    if not parsed.ok:
        raise SlackAPIError(f"users.info failed for {user_id}: {parsed.error or 'unknown'}")
    if parsed.user is None:
        raise SlackAPIError(f"users.info returned no user for {user_id}")
    return parsed.user


def _lock_users_stream(cur: Cursor[TupleRow]) -> None:
    cur.execute(
        "INSERT INTO stream_heads (stream) VALUES (%s) ON CONFLICT (stream) DO NOTHING",
        (_USERS_STREAM,),
    )
    cur.execute(
        "SELECT next_offset FROM stream_heads WHERE stream = %s FOR UPDATE",
        (_USERS_STREAM,),
    )
    if cur.fetchone() is None:  # pragma: no cover - row is guaranteed by upsert above
        msg = f"stream_heads row vanished for {_USERS_STREAM!r}"
        raise RuntimeError(msg)


def _existing_user_added_ids(cur: Cursor[TupleRow]) -> set[str]:
    cur.execute(
        "SELECT payload->>'id' FROM events WHERE stream = %s AND kind = 'user_added'",
        (_USERS_STREAM,),
    )
    existing: set[str] = set()
    for row in cur.fetchall():
        raw_id = row[0]
        if isinstance(raw_id, str) and raw_id:
            existing.add(raw_id)
    return existing


def _insert_user_added(cur: Cursor[TupleRow], user: SlackUser) -> int:
    offset = assign_offset(cur, _USERS_STREAM)
    record = EventRecord(stream=_USERS_STREAM, kind="user_added", ts=None, payload=_user_payload(user))
    insert_event(cur, offset, record)
    return offset


def _populate_users_once_sync(writer: OffsetWriter, client: SlackClient) -> tuple[int, int]:
    users = _fetch_workspace_users(client)
    inserted = 0
    with writer.conn.transaction(), writer.conn.cursor() as cur:
        _lock_users_stream(cur)
        existing = _existing_user_added_ids(cur)
        for user in users:
            if user.id in existing:
                continue
            _insert_user_added(cur, user)
            existing.add(user.id)
            inserted += 1
    return (len(users), inserted)


async def populate_users_once(writer: OffsetWriter, client: SlackClient) -> None:
    """One-shot startup users.list import (`user_added` events)."""
    try:
        total, inserted = await trio.to_thread.run_sync(
            lambda: _populate_users_once_sync(writer, client),
            limiter=writer.limiter,
        )
    except (httpx.HTTPError, SlackAPIError, ValueError):
        log.warning("users: startup populate failed", exc_info=True)
        return
    log.info("users: startup populate complete users=%d inserted=%d skipped=%d", total, inserted, total - inserted)


def _load_user_state(cur: Cursor[TupleRow], user_id: str) -> _UserState | None:
    cur.execute(
        "SELECT kind, payload FROM events "
        "WHERE stream = %s AND ("
        "  (kind = 'user_added' AND payload->>'id' = %s) OR "
        "  (kind IN ('user_renamed', 'user_profile_changed') AND payload->>'user_id' = %s)"
        ") "
        "ORDER BY offset_in_stream",
        (_USERS_STREAM, user_id, user_id),
    )
    display_name: str | None = None
    profile_fields: JsonObject | None = None
    for kind_raw, payload_raw in cur.fetchall():
        kind = str(kind_raw)
        if not isinstance(payload_raw, dict):
            continue
        payload = cast(dict[str, object], payload_raw)
        if kind == "user_added":
            try:
                member = SlackUser.model_validate(payload)
            except ValidationError:
                continue
            display_name = member.display()
            profile_fields = _profile_payload(member)
            continue
        if kind == "user_renamed":
            renamed = payload.get("new_display_name")
            if isinstance(renamed, str):
                display_name = renamed
            continue
        if kind == "user_profile_changed":
            updated = payload.get("profile_fields")
            if isinstance(updated, dict):
                profile_fields = cast(JsonObject, updated)
    if display_name is None or profile_fields is None:
        return None
    return _UserState(display_name=display_name, profile_fields=profile_fields)


def _apply_user_change_sync(writer: OffsetWriter, client: SlackClient, user_id: str) -> tuple[bool, bool, bool]:
    member = _fetch_user(client, user_id)
    new_display_name = member.display()
    new_profile_fields = _profile_payload(member)

    wrote_user_added = False
    wrote_renamed = False
    wrote_profile_changed = False
    with writer.conn.transaction(), writer.conn.cursor() as cur:
        _lock_users_stream(cur)
        previous = _load_user_state(cur, user_id)
        if previous is None:
            _insert_user_added(cur, member)
            wrote_user_added = True
        else:
            if previous.display_name != new_display_name:
                renamed_payload: JsonObject = {"user_id": user_id, "new_display_name": new_display_name}
                offset = assign_offset(cur, _USERS_STREAM)
                insert_event(
                    cur,
                    offset,
                    EventRecord(stream=_USERS_STREAM, kind="user_renamed", ts=None, payload=renamed_payload),
                )
                wrote_renamed = True
            if previous.profile_fields != new_profile_fields:
                profile_payload: JsonObject = {"user_id": user_id, "profile_fields": new_profile_fields}
                offset = assign_offset(cur, _USERS_STREAM)
                insert_event(
                    cur,
                    offset,
                    EventRecord(stream=_USERS_STREAM, kind="user_profile_changed", ts=None, payload=profile_payload),
                )
                wrote_profile_changed = True
    return (wrote_user_added, wrote_renamed, wrote_profile_changed)


async def apply_user_change_event(writer: OffsetWriter, client: SlackClient, event: SocketEventPayload) -> None:
    """Translate one `user_change` socket event to `users` stream writes."""
    if event.type != "user_change":
        return
    user_id = event.user
    if not user_id:
        log.debug("users: ignoring user_change without user id")
        return
    try:
        added, renamed, profile_changed = await trio.to_thread.run_sync(
            lambda: _apply_user_change_sync(writer, client, user_id),
            limiter=writer.limiter,
        )
    except (httpx.HTTPError, SlackAPIError, ValueError):
        log.warning("users: failed to apply user_change for %s", user_id, exc_info=True)
        return
    if added:
        log.info("users: user_change inserted missing user_added for %s", user_id)
        return
    if renamed or profile_changed:
        log.info(
            "users: user_change applied for %s (renamed=%s profile_changed=%s)",
            user_id,
            renamed,
            profile_changed,
        )


def _normalize_user_change_envelope(message: str | bytes) -> JsonObject | None:
    try:
        raw = json.loads(message)
    except json.JSONDecodeError:
        return None
    if not isinstance(raw, dict):
        return None
    envelope = cast(dict[str, object], raw)
    if envelope.get("type") != "events_api":
        return None
    payload_raw = envelope.get("payload")
    if not isinstance(payload_raw, dict):
        return None
    payload = cast(dict[str, object], payload_raw)
    event_raw = payload.get("event")
    if not isinstance(event_raw, dict):
        return None
    event = cast(dict[str, object], event_raw)
    if event.get("type") != "user_change":
        return None
    user_raw = event.get("user")
    if not isinstance(user_raw, dict):
        return None
    user = cast(dict[str, object], user_raw)
    user_id = user.get("id")
    if not isinstance(user_id, str) or not user_id:
        return None
    normalized_event: dict[str, object] = {**event, "user": user_id}
    normalized_payload: dict[str, object] = {**payload, "event": normalized_event}
    normalized: dict[str, object] = {**envelope, "payload": normalized_payload}
    return cast(JsonObject, normalized)


def _parse_envelope_allow_user_change(message: str | bytes) -> SocketEnvelope | None:
    try:
        return SocketEnvelope.model_validate_json(message)
    except ValidationError as exc:
        normalized = _normalize_user_change_envelope(message)
        if normalized is None:
            log.warning("socket-mode: envelope parse error: %s", exc)
            return None
        try:
            return SocketEnvelope.model_validate(normalized)
        except ValidationError as fallback_exc:
            log.warning("socket-mode: envelope parse error: %s", fallback_exc)
            return None


def _ack(envelope_id: str) -> str:
    return json.dumps({"envelope_id": envelope_id})


class UsersSocketModeRunner(SocketModeRunner):
    """Socket-mode runner that adds `user_change` handling to Sprint-1A logic."""

    def __init__(
        self,
        writer: OffsetWriter,
        health: HealthEmitter,
        client: SlackClient,
        app_token: str,
        *,
        options: SocketModeOptions | None = None,
    ) -> None:
        super().__init__(writer, health, client, app_token, options=options)
        self._users_writer = writer
        self._users_client = client

    async def _handle_event(self, event: SocketEventPayload) -> None:
        if event.type == "user_change":
            await apply_user_change_event(self._users_writer, self._users_client, event)
            return
        await super()._handle_event(event)

    async def _message_loop(self, ws: WebSocketConnection) -> bool:
        """Base loop with user-change envelope normalization."""
        try:
            while True:
                message = await ws.get_message()
                envelope = _parse_envelope_allow_user_change(message)
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


async def run_socket_mode_with_users(
    writer: OffsetWriter,
    health: HealthEmitter,
    client: SlackClient,
    app_token: str,
    *,
    options: SocketModeOptions | None = None,
) -> None:
    """Entry point: Socket Mode with message/channel + user-change writes."""
    await UsersSocketModeRunner(writer, health, client, app_token, options=options).run()
