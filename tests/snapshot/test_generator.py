"""Database-backed snapshot generation: cost columns, determinism, isolation.

Covers acceptance criteria 3 (cost columns), 4 (deterministic payload), and 6
(REPEATABLE READ consistency vs the live log).
"""

from __future__ import annotations

from typing import cast

import psycopg
from psycopg import IsolationLevel
from psycopg.rows import TupleRow

from slack_fuse.models import Message
from slack_fuse_server._json import JsonObject
from slack_fuse_server.slurper.offsets import EventRecord, write_event
from slack_fuse_server.snapshot.generator import canonical_json, generate_snapshot
from tests.conftest import ServerConnFactory


def _write_message(conn: psycopg.Connection[TupleRow], stream: str, ts: str) -> int | None:
    payload: JsonObject = Message.model_validate({"ts": ts, "user": "U1", "text": f"msg {ts}"}).model_dump(mode="json")
    return write_event(conn, EventRecord(stream=stream, kind="message", ts=ts, payload=payload, dedup=True))


def _stored_payload(conn: psycopg.Connection[TupleRow], stream: str, at_offset: int) -> list[JsonObject]:
    with conn.cursor() as cur:
        cur.execute("SELECT payload FROM snapshots WHERE stream = %s AND at_offset = %s", (stream, at_offset))
        row = cur.fetchone()
    assert row is not None
    payload = row[0]
    assert isinstance(payload, list)
    return cast("list[JsonObject]", payload)


def test_generate_populates_cost_columns(server_conn: psycopg.Connection[TupleRow]) -> None:
    stream = "channel:C1"
    for i in range(4):
        _write_message(server_conn, stream, f"10{i}.000000")

    result = generate_snapshot(server_conn, stream, trigger="manual")

    assert result is not None
    assert result.at_offset == 4
    assert result.events_covered == 4
    assert result.generation_trigger == "manual"
    assert result.generation_duration_ms >= 0
    assert len(result.lines) == 4

    # payload_bytes is the canonical-JSON byte size of the stored payload.
    stored = _stored_payload(server_conn, stream, at_offset=4)
    assert result.payload_bytes == len(canonical_json(stored).encode("utf-8"))

    # The persisted row mirrors the returned result.
    with server_conn.cursor() as cur:
        cur.execute(
            "SELECT payload_bytes, events_covered, generation_duration_ms, generation_trigger "
            "FROM snapshots WHERE stream = %s AND at_offset = %s",
            (stream, 4),
        )
        db_row = cur.fetchone()
    assert db_row is not None
    assert db_row[0] == result.payload_bytes
    assert db_row[1] == 4
    assert db_row[3] == "manual"


def test_events_covered_is_delta_from_previous_snapshot(server_conn: psycopg.Connection[TupleRow]) -> None:
    stream = "channel:C1"
    for i in range(3):
        _write_message(server_conn, stream, f"20{i}.000000")
    first = generate_snapshot(server_conn, stream, trigger="manual")
    assert first is not None and first.at_offset == 3 and first.events_covered == 3

    # New events arriving after the first snapshot land in the next one.
    for i in range(2):
        _write_message(server_conn, stream, f"21{i}.000000")
    second = generate_snapshot(server_conn, stream, trigger="event_count")
    assert second is not None
    assert second.at_offset == 5
    assert second.events_covered == 2  # 5 - 3
    assert len(second.lines) == 5  # full state, not a delta


def test_regenerating_at_same_offset_is_byte_identical(server_conn: psycopg.Connection[TupleRow]) -> None:
    stream = "channel:C1"
    for i in range(5):
        _write_message(server_conn, stream, f"30{i}.000000")

    first = generate_snapshot(server_conn, stream, trigger="manual")
    assert first is not None
    # Drop the row so the same offset can be regenerated, then compare bytes.
    with server_conn.cursor() as cur:
        cur.execute("DELETE FROM snapshots WHERE stream = %s", (stream,))
    second = generate_snapshot(server_conn, stream, trigger="manual")
    assert second is not None

    assert second.at_offset == first.at_offset
    assert canonical_json(list(first.lines)) == canonical_json(list(second.lines))
    assert first.payload_bytes == second.payload_bytes


def test_generate_returns_none_when_no_events(server_conn: psycopg.Connection[TupleRow]) -> None:
    assert generate_snapshot(server_conn, "channel:empty", trigger="manual") is None


def test_generate_returns_none_when_no_new_events(server_conn: psycopg.Connection[TupleRow]) -> None:
    stream = "channel:C1"
    _write_message(server_conn, stream, "400.000000")
    assert generate_snapshot(server_conn, stream, trigger="manual") is not None
    # Nothing new since the last snapshot → no second row.
    assert generate_snapshot(server_conn, stream, trigger="time") is None


def test_repeatable_read_excludes_concurrent_writes(server_conn_factory: ServerConnFactory) -> None:
    """Criterion 6: events committed during generation are invisible to it.

    Exercises the exact isolation generate_snapshot depends on, using two
    backends in one schema: connection A opens a REPEATABLE READ transaction and
    takes its snapshot; connection B commits a new event; A still sees only the
    pre-existing events.
    """
    reader = server_conn_factory()
    writer = server_conn_factory()
    stream = "channel:C1"
    for i in range(3):
        _write_message(writer, stream, f"50{i}.000000")

    reader.isolation_level = IsolationLevel.REPEATABLE_READ
    with reader.transaction(), reader.cursor() as cur:
        # First query establishes the REPEATABLE READ snapshot.
        cur.execute("SELECT max(offset_in_stream) FROM events WHERE stream = %s", (stream,))
        head_row = cur.fetchone()
        assert head_row is not None and head_row[0] == 3

        # A concurrent writer commits a 4th event on another backend.
        assert _write_message(writer, stream, "503.000000") == 4

        # The reader's consistent snapshot still sees only the original 3.
        cur.execute("SELECT count(*) FROM events WHERE stream = %s", (stream,))
        count_row = cur.fetchone()
        assert count_row is not None and count_row[0] == 3
