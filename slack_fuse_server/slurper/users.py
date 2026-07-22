"""Users-stream ingestion for the slurper.

Closes the Sprint-1A deferral: the server now emits `users` stream events from
both a startup one-shot users.list pass and live `user_change` socket events.

- Startup: one `user_added` event per workspace user (idempotent on restart).
- Live join: `team_join` emits the same `user_added` event from Slack's full
  user payload (idempotent on user id).
- Live: `user_change` emits `user_renamed` and/or `user_profile_changed`.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import cast

import httpx
import trio
from psycopg import Connection, Cursor
from psycopg.rows import TupleRow
from pydantic import ValidationError

from slack_fuse.models import (
    SlackUser,
    SocketEnvelope,
    SocketEventPayload,
    UsersInfoResponse,
    UsersListResponse,
)
from slack_fuse_server._json import JsonObject
from slack_fuse_server.slurper.api import SlackAPIError, SlackClient, Validated
from slack_fuse_server.slurper.health import HealthEmitter
from slack_fuse_server.slurper.limiters import SlurperLimiters
from slack_fuse_server.slurper.offsets import (
    PG_TIMEOUT_EXCEPTIONS,
    EventRecord,
    OffsetWriter,
    assign_offset,
    insert_event,
)
from slack_fuse_server.slurper.socket import (
    SlackEventDispatcherProtocol,
    SocketModeOptions,
    SocketModeRunner,
)
from slack_fuse_server.slurper.supervisor import TaskSupervisor

log = logging.getLogger(__name__)

_USERS_STREAM = "users"
_USERS_LIST_URL = "https://slack.com/api/users.list"
_USERS_INFO_URL = "https://slack.com/api/users.info"


@dataclass(frozen=True, slots=True)
class _UserState:
    display_name: str
    profile_fields: JsonObject


def _fetch_workspace_users(client: SlackClient) -> list[Validated[SlackUser]]:
    """Iterate ``users.list`` paginated; pair each member dict (raw) with
    its validated model so the persistence site below writes raw."""
    users: list[Validated[SlackUser]] = []
    cursor = ""
    while True:
        params: dict[str, str] = {"limit": "200"}
        if cursor:
            params["cursor"] = cursor
        resp = client.http.get(_USERS_LIST_URL, params=params, timeout=30.0)
        resp.raise_for_status()
        raw_body = cast(JsonObject, json.loads(resp.content))
        parsed = UsersListResponse.model_validate(raw_body)
        if not parsed.ok:
            raise SlackAPIError(f"users.list failed: {parsed.error or 'unknown'}")
        raw_members = raw_body.get("members")
        raw_list: list[object] = list(raw_members) if isinstance(raw_members, list) else []
        for raw_member, model_member in zip(raw_list, parsed.members, strict=False):
            if isinstance(raw_member, dict):
                users.append(Validated(raw=cast(JsonObject, raw_member), model=model_member))
        cursor = parsed.response_metadata.next_cursor
        if not cursor:
            break
    return users


def _fetch_user(client: SlackClient, user_id: str) -> Validated[SlackUser]:
    resp = client.http.get(_USERS_INFO_URL, params={"user": user_id}, timeout=30.0)
    resp.raise_for_status()
    raw_body = cast(JsonObject, json.loads(resp.content))
    parsed = UsersInfoResponse.model_validate(raw_body)
    if not parsed.ok:
        raise SlackAPIError(f"users.info failed for {user_id}: {parsed.error or 'unknown'}")
    if parsed.user is None:
        raise SlackAPIError(f"users.info returned no user for {user_id}")
    raw_user = raw_body.get("user")
    if not isinstance(raw_user, dict):
        raise SlackAPIError(f"users.info returned non-dict user for {user_id}")
    return Validated(raw=cast(JsonObject, raw_user), model=parsed.user)


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


def _user_added_exists(cur: Cursor[TupleRow], user_id: str) -> bool:
    cur.execute(
        "SELECT 1 FROM events WHERE stream = %s AND kind = 'user_added' AND payload->>'id' = %s LIMIT 1",
        (_USERS_STREAM, user_id),
    )
    return cur.fetchone() is not None


def _insert_user_added(cur: Cursor[TupleRow], user_raw: JsonObject) -> int:
    """Persist the RAW user dict — same lossless contract as
    ``channels._insert_channel_added``."""
    offset = assign_offset(cur, _USERS_STREAM)
    record = EventRecord(stream=_USERS_STREAM, kind="user_added", ts=None, payload=user_raw)
    insert_event(cur, offset, record)
    return offset


def _apply_team_join_sync(conn: Connection[TupleRow], validated: Validated[SlackUser]) -> bool:
    user_id = validated.model.id
    with conn.cursor() as cur:
        _lock_users_stream(cur)
        if _user_added_exists(cur, user_id):
            return False
        _insert_user_added(cur, validated.raw)
    return True


def _populate_users_once_sync(
    conn: Connection[TupleRow],
    users: list[Validated[SlackUser]],
) -> tuple[int, int]:
    inserted = 0
    with conn.cursor() as cur:
        _lock_users_stream(cur)
        existing = _existing_user_added_ids(cur)
        for validated in users:
            user = validated.model
            if user.id in existing:
                continue
            _insert_user_added(cur, validated.raw)
            existing.add(user.id)
            inserted += 1
    return (len(users), inserted)


async def populate_users_once(
    writer: OffsetWriter,
    client: SlackClient,
    limiters: SlurperLimiters,
    supervisor: TaskSupervisor | None = None,
) -> None:
    """One-shot startup users.list import (`user_added` events)."""
    try:
        if supervisor is not None:
            supervisor.declare("populate-users", "listing_users", deadline_s=60)
        users = await trio.to_thread.run_sync(lambda: _fetch_workspace_users(client), limiter=limiters.slack_api)
        if supervisor is not None:
            supervisor.declare("populate-users", "writing_users", deadline_s=30)
        total, inserted = await writer.run_transaction(lambda conn: _populate_users_once_sync(conn, users))
    except (*PG_TIMEOUT_EXCEPTIONS, httpx.HTTPError, SlackAPIError, ValueError):
        log.warning("users: startup populate failed", exc_info=True)
        if supervisor is not None:
            supervisor.declare("populate-users", "failed", deadline_s=None)
        return
    log.info("users: startup populate complete users=%d inserted=%d skipped=%d", total, inserted, total - inserted)
    if supervisor is not None:
        supervisor.declare("populate-users", "complete", deadline_s=None)


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
            profile_fields = cast(JsonObject, member.profile.model_dump(mode="json"))
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


def _apply_user_change_sync(
    conn: Connection[TupleRow],
    validated: Validated[SlackUser],
    user_id: str,
) -> tuple[bool, bool, bool]:
    member = validated.model
    new_display_name = member.display()
    # ``profile_fields`` for diff stays model-derived: it's the in-process
    # comparison signal, not what we persist. The user_profile_changed
    # event's payload still includes it (callers may want the delta
    # explicitly even if they could re-derive from the new user_added).
    new_profile_fields = cast(JsonObject, member.profile.model_dump(mode="json"))

    wrote_user_added = False
    wrote_renamed = False
    wrote_profile_changed = False
    with conn.cursor() as cur:
        _lock_users_stream(cur)
        previous = _load_user_state(cur, user_id)
        if previous is None:
            _insert_user_added(cur, validated.raw)
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


async def apply_user_change_event(
    writer: OffsetWriter,
    client: SlackClient,
    event: SocketEventPayload,
    limiters: SlurperLimiters,
) -> None:
    """Translate one `user_change` socket event to `users` stream writes."""
    if event.type != "user_change":
        return
    user_id = event.user
    if not user_id:
        msg = "user_change missing user id"
        raise ValueError(msg)
    validated = await trio.to_thread.run_sync(lambda: _fetch_user(client, user_id), limiter=limiters.slack_api)
    added, renamed, profile_changed = await writer.run_transaction(
        lambda conn: _apply_user_change_sync(conn, validated, user_id)
    )
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


async def apply_team_join_event(
    writer: OffsetWriter,
    event: SocketEventPayload,
    raw_event: JsonObject,
) -> None:
    """Translate one `team_join` socket event to a `user_added` stream write."""
    if event.type != "team_join":
        return
    raw_user = raw_event.get("user")
    if not isinstance(raw_user, dict):
        msg = "team_join missing user payload"
        raise ValueError(msg)
    user_raw = cast(JsonObject, raw_user)
    user = SlackUser.model_validate(user_raw)
    inserted = await writer.run_transaction(lambda conn: _apply_team_join_sync(conn, Validated(user_raw, user)))
    if inserted:
        log.info("users: team_join inserted user_added for %s", user.id)


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
    if event.get("type") not in {"team_join", "user_change"}:
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


def parse_envelope_allow_user_change(
    message: str | bytes,
) -> tuple[SocketEnvelope, JsonObject] | None:
    """Validate + return paired (possibly normalized model, original raw dict).

    See base ``_parse_envelope`` for the rationale on returning the raw
    dict — message persistence is lossless against the wire shape.
    """
    try:
        raw = json.loads(message)
    except json.JSONDecodeError as exc:
        log.warning("socket-mode: envelope parse error: %s", exc)
        return None
    if not isinstance(raw, dict):
        return None
    raw_envelope = cast(JsonObject, raw)
    try:
        return SocketEnvelope.model_validate(raw_envelope), raw_envelope
    except ValidationError as exc:
        normalized = _normalize_user_change_envelope(message)
        if normalized is None:
            log.warning("socket-mode: envelope validation failed exception_type=%s", type(exc).__name__)
            return None
        try:
            return SocketEnvelope.model_validate(normalized), raw_envelope
        except ValidationError as fallback_exc:
            log.warning(
                "socket-mode: normalized envelope validation failed exception_type=%s",
                type(fallback_exc).__name__,
            )
            return None


# Compatibility alias for tests/callers predating common nested-user model
# normalization. New transport code uses SocketEventPayload directly.
_parse_envelope_allow_user_change = parse_envelope_allow_user_change


async def run_socket_mode_with_users(  # noqa: PLR0913 - public wrapper mirrors runner dependencies.
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
    """Entry point: Socket Mode with message/channel + user-change writes."""
    # Nested user objects are normalized by SocketEventPayload while the raw
    # event remains lossless, so user events now flow through the same shared
    # dispatcher as every other event family.
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
