"""Postgres event-log replay and live tailing for the WS wire server.

Live wakeups use PostgreSQL `LISTEN new_event`. The slurper should issue
`NOTIFY new_event, '<stream-id>'` after committing an event insert. Empty
payloads are also supported and mean "some stream changed"; the server then
checks every caught-up subscription on that connection.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Final, cast

import psycopg
from psycopg.rows import AsyncRowFactory, class_row
from pydantic import TypeAdapter, ValidationError

from slack_fuse_server._json import JsonObject
from slack_fuse_server.wire.frames import EventFrame

NEW_EVENT_CHANNEL: Final = "new_event"
DEFAULT_MAX_REPLAY_EVENTS: Final = 5_000
DEFAULT_EVENT_PAGE_SIZE: Final = 500

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
        async with (
            await psycopg.AsyncConnection[_HeadRecord].connect(
                self._database_url,
                row_factory=cast(AsyncRowFactory[_HeadRecord], class_row(_HeadRecord)),
            ) as conn,
            conn.cursor() as cur,
        ):
            await cur.execute(
                """
                SELECT next_offset - 1 AS head_offset
                FROM stream_heads
                WHERE stream = %s
                """,
                (stream,),
            )
            row = await cur.fetchone()
        return None if row is None else row.head_offset

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
            page = await self._fetch_event_page(stream, cursor, through=through)
            if not page:
                return
            for event in page:
                cursor = event.offset
                yield event
            if len(page) < self.event_page_size:
                return

    async def listen(self) -> AsyncIterator[str | None]:
        async with await psycopg.AsyncConnection.connect(self._database_url, autocommit=True) as conn:
            await conn.execute("LISTEN new_event")
            async for notify in conn.notifies():
                payload = notify.payload.strip()
                yield payload or None

    async def _fetch_event_page(self, stream: str, since: int, *, through: int | None) -> list[EventFrame]:
        async with (
            await psycopg.AsyncConnection[_EventRecord].connect(
                self._database_url,
                row_factory=cast(AsyncRowFactory[_EventRecord], class_row(_EventRecord)),
            ) as conn,
            conn.cursor() as cur,
        ):
            if through is None:
                await cur.execute(
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
                await cur.execute(
                    """
                    SELECT stream, offset_in_stream AS offset, kind, ts, payload
                    FROM events
                    WHERE stream = %s AND offset_in_stream > %s AND offset_in_stream <= %s
                    ORDER BY offset_in_stream
                    LIMIT %s
                    """,
                    (stream, since, through, self.event_page_size),
                )
            rows = await cur.fetchall()
        return [_record_to_frame(row) for row in rows]


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
