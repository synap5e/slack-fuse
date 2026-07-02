"""Migration 0008: active message and thread-parent views."""

from __future__ import annotations

from typing import cast

import psycopg
from psycopg.rows import TupleRow

from slack_fuse_server._json import JsonObject
from slack_fuse_server.slurper.offsets import EventRecord, write_event


def _write(  # noqa: PLR0913 - tiny test insert helper keeps view cases readable.
    conn: psycopg.Connection[TupleRow],
    stream: str,
    kind: str,
    payload: JsonObject,
    *,
    ts: str | None = None,
    dedup: bool = False,
) -> int:
    offset = write_event(conn, EventRecord(stream=stream, kind=kind, ts=ts, payload=payload, dedup=dedup))
    assert offset is not None
    return offset


def _message(ts: str, text: str, **extra: object) -> JsonObject:
    payload: dict[str, object] = {"ts": ts, "user": "U1", "text": text}
    payload.update(extra)
    return cast(JsonObject, payload)


def _change(previous_ts: str, message: JsonObject) -> JsonObject:
    return cast(JsonObject, {"previous_ts": previous_ts, "message": message})


def _active_rows(conn: psycopg.Connection[TupleRow], stream: str) -> list[tuple[str, JsonObject, int]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ts, active_payload, offset_in_stream
            FROM active_messages
            WHERE stream = %s
            ORDER BY ts
            """,
            (stream,),
        )
        rows = cur.fetchall()
    out: list[tuple[str, JsonObject, int]] = []
    for ts, payload, offset in rows:
        assert isinstance(payload, dict)
        out.append((str(ts), cast(JsonObject, payload), int(offset)))
    return out


def test_is_valid_slack_ts_strict_shape_and_pg_attributes(server_conn: psycopg.Connection[TupleRow]) -> None:
    cases: list[tuple[str | None, bool | None]] = [
        ("1000000000.000000", True),
        ("1700000000.000100", True),
        ("1700000100.000200", True),
        ("1700000200.000300", True),
        ("1999999999.999999", True),
        ("9999999999.123456", True),
        ("1.0", False),
        ("0.000001", False),
        ("1e6", False),
        ("1", False),
        ("1700000000", False),
        ("1700000000.0001000", False),
        ("01700000000.000100", False),
        ("", False),
        ("abc", False),
        (None, None),
        ("99999999999.000001", False),
        ("999999999.999999", False),
        ("1000000000.00000", False),
        ("1000000000.000000 ", False),
        (" 1000000000.000000", False),
        ("-1700000000.000100", False),
        ("+1700000000.000100", False),
        ("1700000000.abcdef", False),
        ("1700000000,000100", False),
        ("0000000001.000000", False),
    ]
    with server_conn.cursor() as cur:
        for value, expected in cases:
            cur.execute("SELECT is_valid_slack_ts(%s)", (value,))
            row = cur.fetchone()
            assert row is not None
            assert row[0] is expected
        cur.execute(
            """
            SELECT p.provolatile, p.proparallel, p.proisstrict
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE p.proname = 'is_valid_slack_ts'
              AND n.nspname = current_schema()
            """
        )
        row = cur.fetchone()
    assert row == ("i", "s", True)


def test_active_messages_base_change_same_ts_latest_offset_wins(server_conn: psycopg.Connection[TupleRow]) -> None:
    stream = "channel:CLATEST"
    ts = "1700000000.000100"
    _write(server_conn, stream, "message_changed", _change(ts, _message(ts, "edited")), ts=ts)
    _write(server_conn, stream, "message", _message(ts, "base later"), ts=ts, dedup=True)

    rows = _active_rows(server_conn, stream)

    assert [(ts_value, payload["text"], offset) for ts_value, payload, offset in rows] == [
        (ts, "base later", 2)
    ]


def test_active_messages_edit_only_and_ts_changing_chain(server_conn: psycopg.Connection[TupleRow]) -> None:
    stream = "channel:CCHAIN"
    a = "1700000000.000100"
    b = "1700000100.000200"
    c = "1700000200.000300"
    _write(server_conn, stream, "message", _message(a, "A"), ts=a, dedup=True)
    _write(server_conn, stream, "message_changed", _change(a, _message(b, "B")), ts=b)
    _write(server_conn, stream, "message_changed", _change(b, _message(c, "C")), ts=c)
    _write(
        server_conn,
        stream,
        "message_changed",
        _change("1700000300.000400", _message("1700000400.000500", "edit only")),
        ts="1700000400.000500",
    )

    rows = _active_rows(server_conn, stream)

    assert [(ts_value, payload["text"]) for ts_value, payload, _offset in rows] == [
        (c, "C"),
        ("1700000400.000500", "edit only"),
    ]


def test_active_messages_terminal_deletes_and_malformed_timestamps(server_conn: psycopg.Connection[TupleRow]) -> None:
    stream = "channel:CDELETE"
    deleted = "1700000000.000100"
    changed_then_deleted = "1700000100.000200"
    stays = "1700000200.000300"
    malformed = "1.0"
    _write(server_conn, stream, "message", _message(deleted, "gone"), ts=deleted, dedup=True)
    _write(server_conn, stream, "message_deleted", {"deleted_ts": deleted, "previous_message": None}, ts=deleted)
    _write(server_conn, stream, "message_changed", _change(deleted, _message(deleted, "restore ignored")), ts=deleted)
    _write(server_conn, stream, "message", _message(changed_then_deleted, "old"), ts=changed_then_deleted, dedup=True)
    _write(
        server_conn,
        stream,
        "message_changed",
        _change(changed_then_deleted, _message(changed_then_deleted, "edited")),
        ts=changed_then_deleted,
    )
    _write(
        server_conn,
        stream,
        "message_deleted",
        {"deleted_ts": changed_then_deleted, "previous_message": None},
        ts=changed_then_deleted,
    )
    _write(server_conn, stream, "message", _message(stays, "kept"), ts=stays, dedup=True)
    _write(server_conn, stream, "message_deleted", {"deleted_ts": malformed, "previous_message": None}, ts=malformed)
    _write(server_conn, stream, "message", _message(malformed, "bad ts"), ts=malformed, dedup=True)

    rows = _active_rows(server_conn, stream)

    assert [(ts_value, payload["text"]) for ts_value, payload, _offset in rows] == [(stays, "kept")]


def test_active_thread_parents_latest_offset_stream_and_unknown_parent_rules(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    stream = "channel:CTHREAD"
    parent_ts = "1700000000.000100"
    stale_reply = "1700000100.000200"
    fresh_reply = "1700000200.000300"
    _write(
        server_conn,
        stream,
        "parent_replied",
        {"parent_ts": parent_ts, "reply_count": 3, "latest_reply": stale_reply},
        ts=parent_ts,
    )
    _write(
        server_conn,
        stream,
        "message",
        _message(parent_ts, "parent", thread_ts=parent_ts, reply_count=5, latest_reply=fresh_reply),
        ts=parent_ts,
        dedup=True,
    )
    _write(
        server_conn,
        "channel:CUNKNOWN",
        "parent_replied",
        {"parent_ts": "1700000300.000400", "reply_count": 1, "latest_reply": "1700000400.000500"},
        ts="1700000300.000400",
    )

    with server_conn.cursor() as cur:
        cur.execute(
            """
            SELECT stream, channel_id, parent_ts, reply_count, latest_reply, effective_offset
            FROM active_thread_parents
            WHERE stream = %s
            """,
            (stream,),
        )
        rows = cur.fetchall()
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = 'active_thread_parents'
            ORDER BY ordinal_position
            """
        )
        columns = [str(row[0]) for row in cur.fetchall()]
        try:
            cur.execute("SET enable_seqscan = off")
            cur.execute("EXPLAIN (COSTS OFF) SELECT * FROM active_thread_parents WHERE stream = %s", (stream,))
            plan = "\n".join(str(row[0]) for row in cur.fetchall())
        finally:
            cur.execute("RESET enable_seqscan")

    assert rows == [(stream, "CTHREAD", parent_ts, 5, fresh_reply, 2)]
    assert columns == ["stream", "channel_id", "parent_ts", "reply_count", "latest_reply", "effective_offset"]
    assert "Index" in plan
    assert _active_thread_parent_count(server_conn, "channel:CUNKNOWN") == 0


def _active_thread_parent_count(conn: psycopg.Connection[TupleRow], stream: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM active_thread_parents WHERE stream = %s", (stream,))
        row = cur.fetchone()
    assert row is not None
    return int(row[0])
