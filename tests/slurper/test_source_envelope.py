# pyright: reportPrivateUsage=false
"""The `events.source` envelope end-to-end: insert composition, span join,
socket-mode event timestamps, legacy-NULL compatibility, forensic queries."""

from __future__ import annotations

from typing import cast

import psycopg
import trio
from psycopg.rows import TupleRow

from slack_fuse.models import SocketEventPayload
from slack_fuse_server._json import JsonObject
from slack_fuse_server.slurper.ingestion import IngestionContext, ingesting, make_source
from slack_fuse_server.slurper.offsets import EventRecord, write_event, write_message_or_corrective
from slack_fuse_server.slurper.socket import translate_message_event
from slack_fuse_server.slurper.spans import SpanRecorder
from tests.conftest import make_test_writer


def _ctx(producer: str = "test-producer", commit: str | None = "abc123") -> IngestionContext:
    return IngestionContext(producer=producer, boot_id="boot-1", task_id="task-1", commit=commit)


def _sources(conn: psycopg.Connection[TupleRow], stream: str) -> list[JsonObject | None]:
    with conn.cursor() as cur:
        cur.execute("SELECT source FROM events WHERE stream = %s ORDER BY id", (stream,))
        rows = cur.fetchall()
    out: list[JsonObject | None] = []
    for (source,) in rows:
        assert source is None or isinstance(source, dict)
        out.append(cast(JsonObject | None, source))
    return out


def test_insert_composes_ambient_context_into_source(server_conn: psycopg.Connection[TupleRow]) -> None:
    record = EventRecord(
        stream="channel:CSRC", kind="message", ts="1700000000.000100", payload={"ts": "1700000000.000100"}
    )
    with ingesting(_ctx()):
        offset = write_event(server_conn, record)
    assert offset == 1
    (source,) = _sources(server_conn, "channel:CSRC")
    assert source == {"producer": "test-producer", "boot_id": "boot-1", "task_id": "task-1", "commit": "abc123"}


def test_insert_without_context_or_record_source_stays_null(server_conn: psycopg.Connection[TupleRow]) -> None:
    record = EventRecord(
        stream="channel:CNULL", kind="message", ts="1700000000.000100", payload={"ts": "1700000000.000100"}
    )
    assert write_event(server_conn, record) == 1
    assert _sources(server_conn, "channel:CNULL") == [None]


def test_record_fields_win_over_ambient(server_conn: psycopg.Connection[TupleRow]) -> None:
    record = EventRecord(
        stream="channel:COVR",
        kind="message",
        ts="1700000000.000100",
        payload={"ts": "1700000000.000100"},
        source=make_source(producer="backfill-history-page", slack_cursor="c2", page_index=0),
    )
    with ingesting(_ctx()):
        write_event(server_conn, record)
    (source,) = _sources(server_conn, "channel:COVR")
    assert source is not None
    assert source["producer"] == "backfill-history-page"
    assert source["slack_cursor"] == "c2"
    assert source["boot_id"] == "boot-1"


def test_corrective_message_changed_inherits_record_source(server_conn: psycopg.Connection[TupleRow]) -> None:
    stream = "channel:CCORRSRC"
    original: JsonObject = {"ts": "1700000000.000100", "text": "old"}
    richer: JsonObject = {"ts": "1700000000.000100", "text": "old", "attachments": [{"fallback": "x"}]}
    first = EventRecord(stream=stream, kind="message", ts="1700000000.000100", payload=original, dedup=True)
    assert write_event(server_conn, first) is not None
    with ingesting(_ctx()):
        second = EventRecord(
            stream=stream,
            kind="message",
            ts="1700000000.000100",
            payload=richer,
            dedup=True,
            source=make_source(producer="backfill-history-page", page_index=4),
        )
        assert write_message_or_corrective(server_conn, second) is not None
    sources = _sources(server_conn, stream)
    assert sources[0] is None
    corrective = sources[1]
    assert corrective is not None
    assert corrective["producer"] == "backfill-history-page"
    assert corrective["page_index"] == 4
    assert corrective["boot_id"] == "boot-1"


def test_offset_writer_stamps_span_id(server_conn: psycopg.Connection[TupleRow]) -> None:
    writer = make_test_writer(server_conn)
    recorder = SpanRecorder()
    record = EventRecord(
        stream="channel:CSPANID", kind="message", ts="1700000000.000100", payload={"ts": "1700000000.000100"}
    )

    async def go() -> None:
        with ingesting(_ctx()):
            offset = await writer.write_event(record, span=recorder)
        assert offset == 1

    trio.run(go)
    (source,) = _sources(server_conn, "channel:CSPANID")
    assert source is not None
    assert source["span_id"] == recorder.span_id


def test_forensic_queries_distinguish_commits_and_legacy_rows(server_conn: psycopg.Connection[TupleRow]) -> None:
    stream = "channel:CFORENSIC"
    # A legacy row (no scope), then one row per deployed commit.
    write_event(server_conn, EventRecord(stream=stream, kind="message", ts="1700000000.000100", payload={}))
    with ingesting(_ctx(commit="commit-a")):
        write_event(server_conn, EventRecord(stream=stream, kind="message", ts="1700000000.000200", payload={}))
    with ingesting(_ctx(commit="commit-b")):
        write_event(server_conn, EventRecord(stream=stream, kind="message", ts="1700000000.000300", payload={}))
    with server_conn.cursor() as cur:
        cur.execute("SELECT ts FROM events WHERE source->>'commit' = 'commit-a'")
        assert [r[0] for r in cur.fetchall()] == ["1700000000.000200"]
        cur.execute("SELECT count(*) FROM events WHERE stream = %s AND source IS NULL", (stream,))
        row = cur.fetchone()
        assert row is not None and row[0] == 1
        # Payload-only reads (every pre-envelope query shape) are unaffected.
        cur.execute("SELECT count(*) FROM events WHERE stream = %s AND kind = 'message'", (stream,))
        row = cur.fetchone()
        assert row is not None and row[0] == 3


def _message_event(raw_event: JsonObject) -> SocketEventPayload:
    return SocketEventPayload.model_validate(raw_event)


def test_socket_message_write_carries_event_ts() -> None:
    raw_event: JsonObject = {
        "type": "message",
        "channel": "C1",
        "ts": "1700000000.000100",
        "event_ts": "1700000000.000100",
        "user": "U1",
        "text": "hi",
    }
    record = translate_message_event(_message_event(raw_event), raw_event)
    assert record is not None
    assert record.source == {"slack_event_ts": "1700000000.000100"}


def test_socket_message_changed_event_ts_is_edit_time_not_message_ts() -> None:
    raw_event: JsonObject = {
        "type": "message",
        "subtype": "message_changed",
        "channel": "C1",
        "event_ts": "1700000500.000900",  # when the edit happened
        "message": {"ts": "1700000000.000100", "user": "U1", "text": "edited"},
    }
    record = translate_message_event(_message_event(raw_event), raw_event)
    assert record is not None
    assert record.kind == "message_changed"
    assert record.source == {"slack_event_ts": "1700000500.000900"}


def test_socket_message_deleted_carries_event_ts() -> None:
    raw_event: JsonObject = {
        "type": "message",
        "subtype": "message_deleted",
        "channel": "C1",
        "deleted_ts": "1700000000.000100",
        "event_ts": "1700000600.000300",
    }
    record = translate_message_event(_message_event(raw_event), raw_event)
    assert record is not None
    assert record.kind == "message_deleted"
    assert record.source == {"slack_event_ts": "1700000600.000300"}
