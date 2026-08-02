# pyright: reportPrivateUsage=false
"""Batched active-reply predicate for thread-expansion skips."""

from __future__ import annotations

from typing import cast

import psycopg
from psycopg import Connection
from psycopg.rows import TupleRow

from slack_fuse_server.backfill.skip_predicate import ThreadParent, find_caught_up_threads
from slack_fuse_server.slurper.offsets import EventRecord, write_event

_CHANNEL = "CSKIP"
_STREAM = f"channel:{_CHANNEL}"
_PARENT = "1700000000.000100"


def _seed_reply(
    conn: psycopg.Connection[TupleRow],
    thread_ts: str,
    reply_ts: str,
) -> None:
    assert (
        write_event(
            conn,
            EventRecord(
                stream=_STREAM,
                kind="message",
                ts=reply_ts,
                payload={"ts": reply_ts, "thread_ts": thread_ts, "user": "U1", "text": reply_ts},
                dedup=True,
            ),
        )
        is not None
    )


def test_exact_active_count_and_max_is_caught_up(server_conn: psycopg.Connection[TupleRow]) -> None:
    replies = ["1700000100.000100", "1700000200.000100", "1700000300.000100"]
    for reply_ts in replies:
        _seed_reply(server_conn, _PARENT, reply_ts)

    caught_up = find_caught_up_threads(
        server_conn,
        _CHANNEL,
        [ThreadParent(_PARENT, reply_count=3, latest_reply=replies[-1])],
    )

    assert caught_up == {_PARENT}


def test_count_mismatch_fetches(server_conn: psycopg.Connection[TupleRow]) -> None:
    replies = ["1700000100.000100", "1700000200.000100"]
    for reply_ts in replies:
        _seed_reply(server_conn, _PARENT, reply_ts)

    caught_up = find_caught_up_threads(
        server_conn,
        _CHANNEL,
        [ThreadParent(_PARENT, reply_count=3, latest_reply=replies[-1])],
    )

    assert caught_up == set()


def test_max_mismatch_fetches(server_conn: psycopg.Connection[TupleRow]) -> None:
    replies = ["1700000100.000100", "1700000200.000100", "1700000300.000100"]
    for reply_ts in replies:
        _seed_reply(server_conn, _PARENT, reply_ts)

    caught_up = find_caught_up_threads(
        server_conn,
        _CHANNEL,
        [ThreadParent(_PARENT, reply_count=3, latest_reply="1700000400.000100")],
    )

    assert caught_up == set()


def test_missing_local_summary_fetches(server_conn: psycopg.Connection[TupleRow]) -> None:
    caught_up = find_caught_up_threads(
        server_conn,
        _CHANNEL,
        [ThreadParent(_PARENT, reply_count=1, latest_reply="1700000100.000100")],
    )

    assert caught_up == set()


def test_deleted_reply_is_excluded_by_active_fold(server_conn: psycopg.Connection[TupleRow]) -> None:
    replies = ["1700000100.000100", "1700000200.000100", "1700000300.000100"]
    for reply_ts in replies:
        _seed_reply(server_conn, _PARENT, reply_ts)
    assert (
        write_event(
            server_conn,
            EventRecord(
                stream=_STREAM,
                kind="message_deleted",
                ts=replies[1],
                payload={"deleted_ts": replies[1]},
            ),
        )
        is not None
    )

    caught_up = find_caught_up_threads(
        server_conn,
        _CHANNEL,
        [ThreadParent(_PARENT, reply_count=2, latest_reply=replies[-1])],
    )

    assert caught_up == {_PARENT}


class _FakeCursor:
    def __init__(self, rows: list[tuple[object, object, object]]) -> None:
        self.rows = rows
        self.execute_calls = 0

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, _query: object, _params: object) -> None:
        self.execute_calls += 1

    def fetchall(self) -> list[tuple[object, object, object]]:
        return self.rows


class _FakeConnection:
    def __init__(self, rows: list[tuple[object, object, object]]) -> None:
        self.fake_cursor = _FakeCursor(rows)

    def cursor(self) -> _FakeCursor:
        return self.fake_cursor


def test_five_parents_are_compared_by_one_query() -> None:
    parents = [
        ThreadParent("1700000000.000100", 1, "1700000100.000100"),
        ThreadParent("1700000000.000200", 2, "1700000200.000200"),
        ThreadParent("1700000000.000300", 3, "1700000300.000300"),
        ThreadParent("1700000000.000400", 4, "1700000400.000400"),
        ThreadParent("1700000000.000500", 5, "1700000500.000500"),
    ]
    conn = _FakeConnection([
        (parents[0].thread_ts, 1, parents[0].latest_reply),
        (parents[1].thread_ts, 1, parents[1].latest_reply),
        (parents[2].thread_ts, 3, parents[2].latest_reply),
        (parents[3].thread_ts, 4, "1700000350.000400"),
        (parents[4].thread_ts, 5, parents[4].latest_reply),
    ])

    caught_up = find_caught_up_threads(cast("Connection[TupleRow]", conn), _CHANNEL, parents)

    assert conn.fake_cursor.execute_calls == 1
    assert caught_up == {parents[0].thread_ts, parents[2].thread_ts, parents[4].thread_ts}
