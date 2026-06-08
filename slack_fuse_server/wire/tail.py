"""Postgres event-log replay and live tailing for the WS wire server.

Trio-native: the whole mount loop runs under trio, and `psycopg.AsyncConnection`
is asyncio-only (it calls `asyncio.get_running_loop()` internally and raises
under trio). So every query here runs *sync* psycopg on a worker thread via
`trio.to_thread.run_sync`, mirroring how `OffsetWriter` serializes its writes.

Live wakeups use PostgreSQL `LISTEN new_event`. The slurper issues
`NOTIFY new_event, '<stream-id>'` after committing an event insert (see
`offsets.insert_event`). Empty payloads are also supported and mean "some
stream changed"; the server then checks every caught-up subscription on that
connection. The listen connection blocks on `notifies(timeout=...)` inside a
worker thread; the short poll interval bounds how long a closing connection
waits for the thread to return before cancellation is delivered.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Final, cast

import psycopg
import trio
from psycopg.rows import RowFactory, TupleRow, class_row
from pydantic import TypeAdapter, ValidationError

from slack_fuse_server._json import JsonObject
from slack_fuse_server.wire.frames import EventFrame

NEW_EVENT_CHANNEL: Final = "new_event"
DEFAULT_MAX_REPLAY_EVENTS: Final = 5_000
DEFAULT_EVENT_PAGE_SIZE: Final = 500

# How long the listen connection blocks for notifications per poll. Also the
# upper bound on how long a cancelled live-tail loop waits for its worker
# thread to return before the connection is torn down.
_NOTIFY_POLL_INTERVAL_S: Final = 1.0

_JSON_OBJECT_ADAPTER: TypeAdapter[JsonObject] = TypeAdapter(JsonObject)


class EventTailError(Exception):
    """Base class for event-log tail failures."""


class InvalidEventPayloadError(EventTailError):
    """A persisted event payload was not a JSON object."""


@dataclass(frozen=True, slots=True)
class _HeadRecord:
    head_offset: int


@dataclass(frozen=True, slots=True)
class _EventRecord:
    stream: str
    offset: int
    kind: str
    ts: str | None
    payload: object


class EventTailer:
    """Reads stream heads and event pages from the authoritative event log."""

    def __init__(
        self,
        database_url: str,
        *,
        max_replay_events: int = DEFAULT_MAX_REPLAY_EVENTS,
        event_page_size: int = DEFAULT_EVENT_PAGE_SIZE,
    ) -> None:
        self._database_url = database_url
        self.max_replay_events = max_replay_events
        self.event_page_size = event_page_size

    async def get_head_offset(self, stream: str) -> int | None:
        return await trio.to_thread.run_sync(self._get_head_offset_sync, stream)

    def replay_is_too_old(self, since: int, head_offset: int) -> bool:
        return head_offset - since > self.max_replay_events

    async def iter_events_after(
        self,
        stream: str,
        since: int,
        *,
        through: int | None = None,
    ) -> AsyncIterator[EventFrame]:
        cursor = since
        while True:
            page = await trio.to_thread.run_sync(self._fetch_event_page_sync, stream, cursor, through)
            if not page:
                return
            for event in page:
                cursor = event.offset
                yield event
            if len(page) < self.event_page_size:
                return

    async def listen(self) -> AsyncIterator[str | None]:
        conn = await trio.to_thread.run_sync(self._open_listen_conn)
        try:
            while True:
                payloads = await trio.to_thread.run_sync(self._poll_notifies, conn)
                for payload in payloads:
                    yield payload
        finally:
            await trio.to_thread.run_sync(conn.close)

    # === sync helpers (run on worker threads) ===

    def _get_head_offset_sync(self, stream: str) -> int | None:
        with (
            psycopg.Connection[_HeadRecord].connect(
                self._database_url,
                row_factory=cast(RowFactory[_HeadRecord], class_row(_HeadRecord)),
            ) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(
                """
                SELECT next_offset - 1 AS head_offset
                FROM stream_heads
                WHERE stream = %s
                """,
                (stream,),
            )
            row = cur.fetchone()
        return None if row is None else row.head_offset

    def _fetch_event_page_sync(self, stream: str, since: int, through: int | None) -> list[EventFrame]:
        with (
            psycopg.Connection[_EventRecord].connect(
                self._database_url,
                row_factory=cast(RowFactory[_EventRecord], class_row(_EventRecord)),
            ) as conn,
            conn.cursor() as cur,
        ):
            if through is None:
                cur.execute(
                    """
                    SELECT stream, offset_in_stream AS offset, kind, ts, payload
                    FROM events
                    WHERE stream = %s AND offset_in_stream > %s
                    ORDER BY offset_in_stream
                    LIMIT %s
                    """,
                    (stream, since, self.event_page_size),
                )
            else:
                cur.execute(
                    """
                    SELECT stream, offset_in_stream AS offset, kind, ts, payload
                    FROM events
                    WHERE stream = %s AND offset_in_stream > %s AND offset_in_stream <= %s
                    ORDER BY offset_in_stream
                    LIMIT %s
                    """,
                    (stream, since, through, self.event_page_size),
                )
            rows = cur.fetchall()
        return [_record_to_frame(row) for row in rows]

    def _open_listen_conn(self) -> psycopg.Connection[TupleRow]:
        conn: psycopg.Connection[TupleRow] = psycopg.connect(self._database_url, autocommit=True)
        conn.execute(f"LISTEN {NEW_EVENT_CHANNEL}")
        return conn

    def _poll_notifies(self, conn: psycopg.Connection[TupleRow]) -> list[str | None]:
        payloads: list[str | None] = []
        for notify in conn.notifies(timeout=_NOTIFY_POLL_INTERVAL_S):
            payload = notify.payload.strip()
            payloads.append(payload or None)
        return payloads


def _record_to_frame(record: _EventRecord) -> EventFrame:
    try:
        payload = _JSON_OBJECT_ADAPTER.validate_python(record.payload)
    except ValidationError as exc:
        raise InvalidEventPayloadError(f"invalid event payload for {record.stream}:{record.offset}") from exc
    return EventFrame(
        stream=record.stream,
        offset=record.offset,
        kind=record.kind,
        ts=record.ts,
        payload=payload,
    )
