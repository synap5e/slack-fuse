"""Offset-assignment: the canonical event-write transaction.

Per RFC §Schemas → Offset assignment pattern. Concurrent writers to the same
stream serialize via the `stream_heads` row lock taken by the
`UPDATE ... RETURNING`; writers to different streams are independent. The
pattern survives parallelisation (one task per channel during backfill)
without code changes.

Two layers:

- Pure cursor-level helpers (`assign_offset`, `insert_event`) that compose
  inside a caller-managed transaction. `health.py` reuses them to write the
  event and its `health_log` mirror atomically.
- `write_event` / `OffsetWriter` — the sync transaction and its trio-friendly
  async wrapper. Sync psycopg is run via `trio.to_thread.run_sync` behind a
  shared `CapacityLimiter`, mirroring how the client serializes store/API work.

Message events are deduped on `(stream, kind, payload->>'ts')` via the
`events_message_dedup` partial unique index (RFC §Backfill). A duplicate
write is a no-op that consumes *no* offset — the transaction aborts before
committing the `stream_heads` bump, so offsets stay gap-free and re-running a
backfill is idempotent.

Backfill/catchup can opt into "corrective" duplicate handling with
`write_message_or_corrective()`: when the fresh historical payload differs from
the existing `message` row for that Slack ts, it appends a `message_changed`
event instead of widening or bypassing the `message` dedup invariant. Socket
Mode continues to use plain `write_event()`, so live duplicate `message` writes
stay no-ops.

Every successful insert also fires `NOTIFY new_event, '<stream-id>'` in the
same transaction (delivered on COMMIT). The WS server's tail loop LISTENs on
`new_event` to push real-time events to subscribers; a deduped no-op insert
fires no NOTIFY because its transaction never commits.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import cast

import psycopg
import trio
from psycopg import Connection, Cursor
from psycopg.rows import TupleRow
from psycopg.types.json import Jsonb

from slack_fuse_server._json import JsonObject

PG_TIMEOUT_EXCEPTIONS: tuple[type[BaseException], ...] = (
    psycopg.errors.LockNotAvailable,
    psycopg.errors.QueryCanceled,
)


@dataclass(frozen=True, slots=True)
class EventRecord:
    """One event ready to append to the `events` table.

    `dedup` is set for `message` events (the only kind the `events_message_dedup`
    partial unique index covers); a re-written message is then a no-op.
    """

    stream: str
    kind: str
    ts: str | None
    payload: JsonObject = field(default_factory=dict)
    dedup: bool = False


class _DuplicateSkip(Exception):
    """Internal: a deduped message event already exists; abort to roll back the
    offset bump so no gap is left in the stream."""


def assign_offset(cur: Cursor[TupleRow], stream: str) -> int:
    """Bump `stream_heads.next_offset` for `stream` and return the offset to use.

    Takes the stream's `stream_heads` row lock for the rest of the transaction,
    serializing concurrent same-stream writers. Creates the row on first use.
    """
    cur.execute(
        "INSERT INTO stream_heads (stream) VALUES (%s) ON CONFLICT (stream) DO NOTHING",
        (stream,),
    )
    cur.execute(
        "UPDATE stream_heads SET next_offset = next_offset + 1 WHERE stream = %s RETURNING next_offset - 1",
        (stream,),
    )
    row = cur.fetchone()
    if row is None:  # pragma: no cover — the upsert guarantees the row exists
        msg = f"stream_heads row vanished for {stream!r}"
        raise RuntimeError(msg)
    return int(row[0])


def insert_event(cur: Cursor[TupleRow], offset: int, record: EventRecord) -> bool:
    """Insert one event row at `offset`. Returns whether a row was written.

    When `record.dedup` is set the insert is `ON CONFLICT DO NOTHING` against the
    message-dedup index, so a re-written message returns False instead of
    raising. A real insert also fires `NOTIFY new_event, '<stream>'` so the WS
    server's tail loop wakes; a deduped no-op fires no NOTIFY.
    """
    values = (record.stream, offset, record.kind, record.ts, Jsonb(record.payload))
    if record.dedup:
        cur.execute(
            "INSERT INTO events (stream, offset_in_stream, kind, ts, payload) "
            "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING RETURNING offset_in_stream",
            values,
        )
        if cur.fetchone() is None:
            return False
    else:
        cur.execute(
            "INSERT INTO events (stream, offset_in_stream, kind, ts, payload) VALUES (%s, %s, %s, %s, %s)",
            values,
        )
    # Wake the WS server's LISTEN new_event tail loop. Payload = stream id;
    # delivered to listeners on COMMIT (no-op if the TX later aborts).
    cur.execute("SELECT pg_notify('new_event', %s)", (record.stream,))
    return True


def _fetch_message_payload(cur: Cursor[TupleRow], stream: str, ts: str) -> JsonObject | None:
    cur.execute(
        """
        SELECT payload
        FROM events
        WHERE stream = %s
          AND kind = 'message'
          AND payload->>'ts' = %s
        ORDER BY id
        LIMIT 1
        """,
        (stream, ts),
    )
    row = cur.fetchone()
    if row is None or not isinstance(row[0], dict):
        return None
    return cast(JsonObject, row[0])


def _has_matching_message_changed(cur: Cursor[TupleRow], stream: str, ts: str, payload: JsonObject) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM events
        WHERE stream = %s
          AND kind = 'message_changed'
          AND ts = %s
          AND payload->'message' = %s::jsonb
        LIMIT 1
        """,
        (stream, ts, Jsonb(payload)),
    )
    return cur.fetchone() is not None


def _corrective_record(record: EventRecord, ts: str) -> EventRecord:
    return EventRecord(
        stream=record.stream,
        kind="message_changed",
        ts=ts,
        payload={"message": record.payload, "previous_ts": ts},
    )


def write_event(conn: Connection[TupleRow], record: EventRecord) -> int | None:
    """Assign an offset and append one event, in a single transaction.

    Returns the assigned offset, or None when `record.dedup` is set and the
    message already exists (no offset consumed — the bump is rolled back).
    """
    try:
        with conn.transaction(), conn.cursor() as cur:
            offset = assign_offset(cur, record.stream)
            if not insert_event(cur, offset, record):
                raise _DuplicateSkip
    except _DuplicateSkip:
        return None
    return offset


def write_message_or_corrective(conn: Connection[TupleRow], record: EventRecord) -> int | None:
    """Append a backfill message, or a corrective edit when a message exists.

    Backfill and catchup use this instead of plain `write_event()` so re-running
    with a richer historical payload can repair an older lossy `message` row
    without deleting events or weakening the `message` dedup index. If the fresh
    payload is already represented by the original message or by an earlier
    corrective `message_changed`, this remains a no-op and consumes no offset.
    """
    if record.kind != "message" or not record.dedup:
        msg = "write_message_or_corrective requires a deduped message EventRecord"
        raise ValueError(msg)
    if record.ts is None:
        msg = "write_message_or_corrective requires record.ts"
        raise ValueError(msg)
    payload_ts = record.payload.get("ts")
    if payload_ts != record.ts:
        msg = "write_message_or_corrective requires record.payload['ts'] to match record.ts"
        raise ValueError(msg)

    try:
        with conn.transaction(), conn.cursor() as cur:
            offset = assign_offset(cur, record.stream)
            if insert_event(cur, offset, record):
                return offset

            existing = _fetch_message_payload(cur, record.stream, record.ts)
            if existing == record.payload or _has_matching_message_changed(
                cur, record.stream, record.ts, record.payload
            ):
                raise _DuplicateSkip

            corrective = _corrective_record(record, record.ts)
            if not insert_event(cur, offset, corrective):  # pragma: no cover - message_changed is not deduped
                msg = f"failed to insert corrective event for {record.stream} ts={record.ts}"
                raise RuntimeError(msg)
    except _DuplicateSkip:
        return None
    return offset


class OffsetWriter:
    """Trio-friendly wrapper around the sync `write_event` transaction.

    Holds one psycopg connection and runs every write on a worker thread behind
    a shared `CapacityLimiter`, so all DB writes for the process serialize
    through one connection (single-process slurper). The underlying SQL still
    survives true concurrency — see `tests/slurper/test_offsets.py`.

    The connection MUST be in autocommit mode: each `write_event` brackets its
    work in `with conn.transaction()`, which only durably commits when no outer
    transaction is open. A non-autocommit connection on which a bare read ran
    first would make these transactions savepoints that vanish on close.
    """

    def __init__(self, conn: Connection[TupleRow], limiter: trio.CapacityLimiter) -> None:
        # Fail fast rather than rely on callers remembering the docstring
        # contract. The bug this guards against is silent: writes appear to
        # succeed, then disappear when the connection closes because they
        # were nested savepoints inside an implicit outer transaction
        # opened by an earlier bare SELECT.
        if not conn.autocommit:
            msg = (
                "OffsetWriter requires conn.autocommit=True. "
                "Without it, write_event()'s `with conn.transaction()` becomes "
                "a savepoint inside an implicit outer transaction and rolls "
                "back when the connection closes. Set conn.autocommit=True "
                "BEFORE constructing the writer."
            )
            raise ValueError(msg)
        self._conn = conn
        self._limiter = limiter

    @property
    def conn(self) -> Connection[TupleRow]:
        """The underlying connection, for callers that compose their own TX
        (e.g. `health.py` writing the event + `health_log` mirror atomically)."""
        return self._conn

    @property
    def limiter(self) -> trio.CapacityLimiter:
        return self._limiter

    async def write_event(self, record: EventRecord) -> int | None:
        """Async: assign an offset and append one event. See `write_event`."""

        def _run() -> int | None:
            return write_event(self._conn, record)

        return await trio.to_thread.run_sync(_run, limiter=self._limiter)

    async def write_message_or_corrective(self, record: EventRecord) -> int | None:
        """Async: append a backfill message or corrective edit."""

        def _run() -> int | None:
            return write_message_or_corrective(self._conn, record)

        return await trio.to_thread.run_sync(_run, limiter=self._limiter)
