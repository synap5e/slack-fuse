"""Build + persist one per-stream snapshot from the events log.

A snapshot is the *current materialised state* of a stream at a particular
offset — the fold of every event up to that offset. Cold consumers fetch it
instead of replaying from 0 (RFC §Snapshot vs event replay decision).

Three stream families project (RFC §Event kinds):

- `channel:<id>` — every undeleted message (top-level + thread replies), folded
  from `message` / `message_changed` / `message_deleted`. Each line matches the
  HTTP `SnapshotLine` DTO: `{"ts": <slack-ts>, "payload": <message object>}`,
  where the payload is byte-for-byte what a live `message` `EventFrame` carried.
- `users` — every `user_added`-and-current user, with `user_renamed` /
  `user_profile_changed` folded in. Each line is the current `SlackUser` object
  (== a `user_added` payload).
- `channel-list` — every known channel, with rename / archive / membership
  folded in. Each line is the current channel object (== a `channel_added`
  payload, plus the `is_archived` flag the `channel_archived` events carry).

Two correctness properties the tests pin:

- **Consistency vs the live log** (RFC acceptance): the read runs in a
  `REPEATABLE READ` transaction, so events committed *during* generation are
  invisible and land in the next snapshot, not this one. The snapshot row is
  inserted in the same transaction, so `at_offset` always matches the events
  the payload folded.
- **Determinism**: `project_stream` sorts deterministically (messages by `ts`,
  users/channels by id) and `canonical_json` sorts object keys, so regenerating
  at the same offset is byte-identical regardless of event-arrival order or
  Postgres' JSONB key ordering.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Literal, cast

from psycopg import Connection, IsolationLevel
from psycopg.rows import TupleRow
from psycopg.types.json import Jsonb

from slack_fuse_server._json import JsonObject, JsonValue

GenerationTrigger = Literal["event_count", "time", "manual"]

USERS_STREAM = "users"
CHANNEL_LIST_STREAM = "channel-list"
_CHANNEL_STREAM_PREFIX = "channel:"

# One (kind, payload) pair from the events log, in offset order.
type EventRow = tuple[str, JsonObject]


def is_projectable_stream(stream: str) -> bool:
    """Whether a snapshot can be materialised for `stream`.

    The `slurper-health` stream (and any future bookkeeping stream) is excluded:
    clients always replay it from 0 and it carries no fold-able state.
    """
    return stream in (USERS_STREAM, CHANNEL_LIST_STREAM) or stream.startswith(_CHANNEL_STREAM_PREFIX)


# === Serialisation ===


def canonical_json(value: JsonValue | Sequence[JsonObject]) -> str:
    """Deterministic JSON: sorted keys, compact separators.

    Used for both the stored payload's byte accounting and the JSONL the
    `/snapshot` endpoint streams, so the cost columns describe exactly the bytes
    a consumer would receive (pre-gzip). Key sorting makes regeneration
    byte-identical even though Postgres JSONB does not preserve insertion order.
    """
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def to_jsonl(lines: Sequence[JsonObject]) -> str:
    """The snapshot's wire form: one canonical-JSON object per line.

    `\\n`-joined with no trailing newline. Matches the JSONL the `/snapshot`
    endpoint streams (one current-state record per line, RFC §Snapshot delivery
    via HTTP).
    """
    return "\n".join(canonical_json(line) for line in lines)


# === Projection (pure) ===


def _ts_sort_key(ts: object) -> tuple[int, int]:
    """Sort key for a Slack `ts` ("seconds.microseconds").

    Slack timestamps always carry a 6-digit fractional part, so comparing the
    integer seconds then the integer fraction orders them chronologically.
    Malformed values sort first rather than raising.
    """
    if not isinstance(ts, str):
        return (0, 0)
    seconds, _, fraction = ts.partition(".")
    try:
        return (int(seconds), int(fraction) if fraction else 0)
    except ValueError:
        return (0, 0)


def _project_channel(events: Sequence[EventRow]) -> list[JsonObject]:
    """Fold a `channel:<id>` event log to its current undeleted messages.

    Returns `SnapshotLine`-shaped lines (`{"ts", "payload"}`) sorted by `ts`.
    """
    messages: dict[str, JsonObject] = {}
    for kind, payload in events:
        if kind == "message":
            ts = payload.get("ts")
            if isinstance(ts, str):
                messages[ts] = payload
        elif kind == "message_changed":
            new_msg = payload.get("message")
            if isinstance(new_msg, dict):
                msg = cast("JsonObject", new_msg)
                previous_ts = payload.get("previous_ts")
                new_ts = msg.get("ts")
                if isinstance(previous_ts, str) and previous_ts != new_ts:
                    messages.pop(previous_ts, None)
                if isinstance(new_ts, str):
                    messages[new_ts] = msg
        elif kind == "message_deleted":
            deleted_ts = payload.get("deleted_ts")
            if isinstance(deleted_ts, str):
                messages.pop(deleted_ts, None)
        # reaction_* and unknown kinds carry no message-set mutation here:
        # reactions ride on the message object's own `reactions` field, and the
        # v1 app config never subscribes to them (see slurper/socket.py).
    ordered = sorted(messages.values(), key=lambda m: _ts_sort_key(m.get("ts")))
    return [{"ts": m.get("ts"), "payload": m} for m in ordered]


def _project_users(events: Sequence[EventRow]) -> list[JsonObject]:
    """Fold the `users` event log to the current `SlackUser` object per user."""
    users: dict[str, JsonObject] = {}
    for kind, payload in events:
        if kind == "user_added":
            user_id = payload.get("id")
            if isinstance(user_id, str):
                users[user_id] = payload
            continue
        user_id = payload.get("user_id")
        if not isinstance(user_id, str) or user_id not in users:
            continue
        current = dict(users[user_id])
        if kind == "user_renamed":
            new_name = payload.get("new_display_name")
            if isinstance(new_name, str):
                profile_raw = current.get("profile")
                profile = dict(profile_raw) if isinstance(profile_raw, dict) else {}
                profile["display_name"] = new_name
                current["profile"] = cast("JsonValue", profile)
        elif kind == "user_profile_changed":
            fields = payload.get("profile_fields")
            if isinstance(fields, dict):
                current["profile"] = cast("JsonValue", fields)
        users[user_id] = current
    return [users[user_id] for user_id in sorted(users)]


def _apply_channel_change(current: JsonObject, kind: str, payload: JsonObject) -> JsonObject:
    """Apply one structural `channel-list` event to a channel object (copy)."""
    updated = dict(current)
    if kind == "channel_renamed":
        new_name = payload.get("new_name")
        if isinstance(new_name, str):
            updated["name"] = new_name
    elif kind == "channel_archived":
        updated["is_archived"] = True
    elif kind == "channel_unarchived":
        updated["is_archived"] = False
    elif kind == "channel_member_changed":
        is_member = payload.get("is_member")
        if isinstance(is_member, bool):
            updated["is_member"] = is_member
    return updated


def _project_channel_list(events: Sequence[EventRow]) -> list[JsonObject]:
    """Fold the `channel-list` event log to the current channel object per id."""
    channels: dict[str, JsonObject] = {}
    for kind, payload in events:
        if kind == "channel_added":
            channel_id = payload.get("id")
            if isinstance(channel_id, str):
                channels[channel_id] = payload
            continue
        channel_id = payload.get("channel_id")
        if isinstance(channel_id, str) and channel_id in channels:
            channels[channel_id] = _apply_channel_change(channels[channel_id], kind, payload)
    return [channels[channel_id] for channel_id in sorted(channels)]


def project_stream(stream: str, events: Sequence[EventRow]) -> list[JsonObject]:
    """Fold an ordered event list to the snapshot's current-state line objects.

    The returned list is exactly what the snapshot stores (a JSON array) and,
    serialised one element per line, the JSONL the `/snapshot` endpoint streams.
    """
    if stream.startswith(_CHANNEL_STREAM_PREFIX):
        return _project_channel(events)
    if stream == USERS_STREAM:
        return _project_users(events)
    if stream == CHANNEL_LIST_STREAM:
        return _project_channel_list(events)
    msg = f"non-projectable stream {stream!r}"
    raise ValueError(msg)


# === Generation (DB-facing) ===


@dataclass(frozen=True, slots=True)
class SnapshotResult:
    """The outcome of one `generate_snapshot` call (also the inserted row)."""

    stream: str
    at_offset: int
    lines: tuple[JsonObject, ...]
    payload_bytes: int
    events_covered: int
    generation_duration_ms: int
    generation_trigger: GenerationTrigger

    def jsonl(self) -> str:
        """The wire JSONL body for this snapshot (RFC §Snapshot delivery)."""
        return to_jsonl(self.lines)


def _as_event_row(row: TupleRow) -> EventRow:
    kind, payload = row
    obj = cast("JsonObject", payload) if isinstance(payload, dict) else cast("JsonObject", {})
    return (str(kind), obj)


def generate_snapshot(
    conn: Connection[TupleRow],
    stream: str,
    *,
    trigger: GenerationTrigger = "manual",
    clock: Callable[[], float] = time.perf_counter,
) -> SnapshotResult | None:
    """Materialise + persist one snapshot for `stream` at its current head.

    Returns the inserted `SnapshotResult`, or `None` when there is nothing to
    snapshot (no events yet, or the latest snapshot already covers the head).

    The read + insert run in one `REPEATABLE READ` transaction: the head offset,
    the folded events, and the inserted `at_offset` all describe one consistent
    point in the log. Events committed by other writers during generation are
    invisible to this snapshot (RFC acceptance: they land in the next one).

    `conn` must be in autocommit mode (the snapshot connection mirrors the
    `OffsetWriter` contract) so `conn.transaction()` opens a real transaction at
    the isolation level set below.
    """
    if not is_projectable_stream(stream):
        msg = f"non-projectable stream {stream!r}"
        raise ValueError(msg)

    # Applied when the next transaction() block begins; harmless to re-set.
    conn.isolation_level = IsolationLevel.REPEATABLE_READ

    start = clock()
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("SELECT max(offset_in_stream) FROM events WHERE stream = %s", (stream,))
        head_row = cur.fetchone()
        head = head_row[0] if head_row is not None else None
        if head is None:
            return None
        at_offset = int(head)

        cur.execute(
            "SELECT at_offset FROM snapshots WHERE stream = %s ORDER BY at_offset DESC LIMIT 1",
            (stream,),
        )
        prev_row = cur.fetchone()
        previous_offset = int(prev_row[0]) if prev_row is not None else 0
        if at_offset <= previous_offset:
            return None

        cur.execute(
            "SELECT kind, payload FROM events WHERE stream = %s AND offset_in_stream <= %s ORDER BY offset_in_stream",
            (stream, at_offset),
        )
        events = [_as_event_row(row) for row in cur.fetchall()]
        lines = project_stream(stream, events)

        payload_bytes = len(canonical_json(lines).encode("utf-8"))
        events_covered = at_offset - previous_offset
        duration_ms = max(0, round((clock() - start) * 1000))

        cur.execute(
            "INSERT INTO snapshots "
            "(stream, at_offset, payload, payload_bytes, events_covered, generation_duration_ms, generation_trigger) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (stream, at_offset, Jsonb(lines), payload_bytes, events_covered, duration_ms, trigger),
        )

    return SnapshotResult(
        stream=stream,
        at_offset=at_offset,
        lines=tuple(lines),
        payload_bytes=payload_bytes,
        events_covered=events_covered,
        generation_duration_ms=duration_ms,
        generation_trigger=trigger,
    )
