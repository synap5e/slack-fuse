"""Unit tests for `cursor.advance_cursor` / `cursor.read_cursor`.

Acceptance criterion: the projector's cursor cannot regress on replay. Replay
delivers events in increasing offset order from the resumption point, so a
naive `applied_offset = $offset` would still work — but a partial replay of an
older batch landing on a partially-advanced cursor must not move backwards.
The `GREATEST` clause guarantees that.
"""

from __future__ import annotations

import psycopg
from psycopg.rows import TupleRow

from slack_fuse.projector.cursor import advance_cursor, read_cursor


def _set(conn: psycopg.Connection[TupleRow], stream: str, offset: int) -> None:
    with conn.cursor() as cur:
        advance_cursor(cur, stream, offset)


def _read(conn: psycopg.Connection[TupleRow], stream: str) -> int:
    with conn.cursor() as cur:
        return read_cursor(cur, stream)


def test_advance_inserts_then_updates(client_conn: psycopg.Connection[TupleRow]) -> None:
    assert _read(client_conn, "channel:CC1") == 0
    _set(client_conn, "channel:CC1", 100)
    assert _read(client_conn, "channel:CC1") == 100
    _set(client_conn, "channel:CC1", 105)
    assert _read(client_conn, "channel:CC1") == 105


def test_advance_does_not_regress(client_conn: psycopg.Connection[TupleRow]) -> None:
    _set(client_conn, "channel:CC2", 500)
    _set(client_conn, "channel:CC2", 200)  # older — must NOT overwrite
    assert _read(client_conn, "channel:CC2") == 500


def test_read_missing_cursor_returns_zero(client_conn: psycopg.Connection[TupleRow]) -> None:
    assert _read(client_conn, "channel:NOEXIST") == 0
