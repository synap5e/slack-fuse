"""Derived backfill-run views."""

from __future__ import annotations

import psycopg
from psycopg.rows import TupleRow

from slack_fuse_server.slurper import catchup
from slack_fuse_server.slurper.offsets import EventRecord, write_event


def test_channel_backfill_state_folds_latest_run(server_conn: psycopg.Connection[TupleRow]) -> None:
    assert (
        write_event(
            server_conn,
            EventRecord(
                stream="backfill-run:CVIEW",
                kind="backfill_run_started",
                ts=None,
                payload={"run_id": "RUN1", "params": {}, "triggered_by": "startup"},
                dedup=True,
            ),
        )
        is not None
    )
    assert (
        write_event(
            server_conn,
            EventRecord(
                stream="backfill-run:CVIEW",
                kind="backfill_page_committed",
                ts=None,
                payload={
                    "run_id": "RUN1",
                    "page_index": 2,
                    "has_more": True,
                    "final_page": False,
                    "slack_cursor": "CUR2",
                    "messages_written": 10,
                    "kind": "history_page",
                },
                dedup=True,
            ),
        )
        is not None
    )

    with server_conn.cursor() as cur:
        cur.execute(
            """
            SELECT channel_id, last_run_id, last_run_outcome, last_run_finished_at,
                   latest_page_index, latest_has_more, latest_slack_cursor
            FROM channel_backfill_state
            WHERE channel_id = 'CVIEW'
            """
        )
        row = cur.fetchone()

    assert row == ("CVIEW", "RUN1", None, None, 2, True, "CUR2")


def test_channel_ingest_head_folds_message_ts(server_conn: psycopg.Connection[TupleRow]) -> None:
    for ts in ("1700000000.000001", "1700000100.000001"):
        assert (
            write_event(
                server_conn,
                EventRecord(
                    stream="channel:CVIEW",
                    kind="message",
                    ts=ts,
                    payload={"ts": ts},
                    dedup=True,
                ),
            )
            is not None
        )

    with server_conn.cursor() as cur:
        cur.execute("SELECT latest_ts FROM channel_ingest_head WHERE channel_id = 'CVIEW'")
        row = cur.fetchone()

    assert row == ("1700000100.000001",)


def test_catchup_ingest_head_matches_the_view_without_scanning_it(server_conn: psycopg.Connection[TupleRow]) -> None:
    # The view is the reference semantics; catchup must agree with it while using the index.
    fixtures = {
        "channel:CA": ["1700000000.000001", "1700000100.000001", "not-a-ts"],
        "channel:CB": ["1700000050.000001"],
        "channel:CC": ["bogus"],
        "backfill-run:CA": [],
    }
    for stream, stamps in fixtures.items():
        for ts in stamps:
            assert (
                write_event(
                    server_conn,
                    EventRecord(stream=stream, kind="message", ts=None, payload={"ts": ts}, dedup=True),
                )
                is not None
            )
    assert (
        write_event(
            server_conn,
            EventRecord(stream="channel:CD", kind="message_changed", ts=None, payload={"ts": "1800000000.000001"}),
        )
        is not None
    )

    with server_conn.cursor() as cur:
        cur.execute("SELECT channel_id, latest_ts::float FROM channel_ingest_head")
        from_view = {str(c): float(t) for c, t in cur.fetchall()}

    assert (
        catchup.latest_ingest_head_by_channel(server_conn)
        == from_view
        == {
            "CA": 1700000100.000001,
            "CB": 1700000050.000001,
        }
    ), "invalid ts ignored; channels with no valid ts and non-message kinds omitted"


def test_catchup_ingest_head_uses_the_index(server_conn: psycopg.Connection[TupleRow]) -> None:
    with server_conn.cursor() as cur:
        cur.execute("SET enable_seqscan = off")
        cur.execute("EXPLAIN " + catchup._INGEST_HEAD_SQL)  # pyright: ignore[reportPrivateUsage]
        plan = "\n".join(str(r[0]) for r in cur.fetchall())
        cur.execute("RESET enable_seqscan")
    assert "events_message_dedup" in plan, "a full scan of events is what OOMed postgres"
    assert "HashAggregate" not in plan and "GroupAggregate" not in plan
