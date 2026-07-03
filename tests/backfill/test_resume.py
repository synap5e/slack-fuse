# pyright: reportPrivateUsage=false
"""Restart-safe backfill resume via the `events.source` envelope.

Covers `find_resume_plan` (termination gate, cursor selection, thread
worklist) and drives `backfill_channel` end-to-end over a scripted fake Slack
transport to prove a crashed run resumes from its stored cursor — including
the prior review's A.3 stale-parent construction, which must terminate.
"""

from __future__ import annotations

import json
import logging
import time
from typing import cast

import httpx
import psycopg
import pytest
import trio
from psycopg import Cursor
from psycopg.abc import Params, QueryNoTemplate
from psycopg.rows import TupleRow

from slack_fuse_render import ChannelId
from slack_fuse_server._json import JsonObject
from slack_fuse_server.backfill.api import BackfillContext, SlackApiBackfiller, SleepBounds, backfill_channel
from slack_fuse_server.backfill.resume import (
    _KNOWN_PARENTS_SQL,
    ResumePlan,
    ThreadResume,
    find_resume_plan,
)
from slack_fuse_server.slurper.api import SlackClient
from slack_fuse_server.slurper.health import HealthEmitter
from slack_fuse_server.slurper.ingestion import make_source
from slack_fuse_server.slurper.offsets import EventRecord, OffsetWriter, write_event
from tests.conftest import make_test_limiters, make_test_writer

_NO_SLEEP = SleepBounds(page_min_s=0.0, page_max_s=0.0, thread_min_s=0.0, thread_max_s=0.0)

_CHANNEL = "CRESUME"
_STREAM = f"channel:{_CHANNEL}"


# === Seeding helpers ===


def _seed_message(
    conn: psycopg.Connection[TupleRow],
    ts: str,
    *,
    stream: str = _STREAM,
    payload_extra: JsonObject | None = None,
    source: JsonObject | None = None,
) -> None:
    payload: JsonObject = {"ts": ts, "user": "U1", "text": f"m-{ts}"}
    if payload_extra:
        payload.update(payload_extra)
    record = EventRecord(stream=stream, kind="message", ts=ts, payload=payload, dedup=True, source=source)
    assert write_event(conn, record) is not None


def _history_source(*, cursor: str, page_index: int, final: bool, oldest: str | None = None) -> JsonObject:
    return make_source(
        producer="backfill-history-page",
        slack_cursor=cursor,
        page_index=page_index,
        has_more=not final,
        final_page=final,
        oldest=oldest,
    )


def _replies_source(*, thread_ts: str, cursor: str, page_index: int, final: bool) -> JsonObject:
    return make_source(
        producer="backfill-replies-page",
        thread_ts=thread_ts,
        slack_cursor=cursor,
        page_index=page_index,
        has_more=not final,
        final_page=final,
    )


def _seed_terminal(conn: psycopg.Connection[TupleRow], kind: str, channel_id: str = _CHANNEL) -> None:
    record = EventRecord(stream="slurper-health", kind=kind, ts=None, payload={"channel_id": channel_id})
    assert write_event(conn, record) is not None


# === find_resume_plan ===


def test_no_source_rows_means_no_resume(server_conn: psycopg.Connection[TupleRow]) -> None:
    _seed_message(server_conn, "1700000000.000100")  # legacy row, source NULL
    assert find_resume_plan(server_conn, _CHANNEL) is None


def test_resume_from_latest_non_final_history_cursor(server_conn: psycopg.Connection[TupleRow]) -> None:
    _seed_message(server_conn, "1700000300.000100", source=_history_source(cursor="c1", page_index=0, final=False))
    _seed_message(server_conn, "1700000200.000100", source=_history_source(cursor="c2", page_index=1, final=False))
    plan = find_resume_plan(server_conn, _CHANNEL)
    assert plan is not None
    assert plan.history_done is False
    assert plan.history_cursor == "c2"


def test_final_history_page_means_history_done(server_conn: psycopg.Connection[TupleRow]) -> None:
    _seed_message(server_conn, "1700000300.000100", source=_history_source(cursor="c1", page_index=0, final=False))
    _seed_message(server_conn, "1700000200.000100", source=_history_source(cursor="", page_index=1, final=True))
    plan = find_resume_plan(server_conn, _CHANNEL)
    assert plan is not None
    assert plan.history_done is True
    assert plan.history_cursor == ""


def test_terminal_event_gates_out_completed_runs(server_conn: psycopg.Connection[TupleRow]) -> None:
    """A completed run's final_page rows must NOT make a later re-backfill a no-op."""
    _seed_message(server_conn, "1700000300.000100", source=_history_source(cursor="", page_index=0, final=True))
    _seed_terminal(server_conn, "backfill_completed")
    assert find_resume_plan(server_conn, _CHANNEL) is None


def test_terminal_abort_gates_out_aborted_runs(server_conn: psycopg.Connection[TupleRow]) -> None:
    """Resume must never dig past a deliberate size-cap abort."""
    _seed_message(server_conn, "1700000300.000100", source=_history_source(cursor="c9", page_index=0, final=False))
    _seed_terminal(server_conn, "backfill_aborted")
    assert find_resume_plan(server_conn, _CHANNEL) is None


def test_rows_after_terminal_are_resume_state(server_conn: psycopg.Connection[TupleRow]) -> None:
    _seed_message(server_conn, "1700000300.000100", source=_history_source(cursor="old", page_index=0, final=True))
    _seed_terminal(server_conn, "backfill_completed")
    _seed_message(server_conn, "1700000200.000100", source=_history_source(cursor="fresh", page_index=0, final=False))
    plan = find_resume_plan(server_conn, _CHANNEL)
    assert plan is not None
    assert plan.history_cursor == "fresh"


def test_since_run_rows_never_anchor_resume(server_conn: psycopg.Connection[TupleRow]) -> None:
    """`--since` pages walk a bounded window; their cursors are not full-history progress."""
    _seed_message(
        server_conn,
        "1700000300.000100",
        source=_history_source(cursor="bounded", page_index=0, final=False, oldest="1700000000.000000"),
    )
    assert find_resume_plan(server_conn, _CHANNEL) is None


def test_thread_worklist_excludes_final_threads_and_resumes_partial(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    done_parent = "1700000100.000100"
    partial_parent = "1700000200.000100"
    fresh_parent = "1700000300.000100"
    for parent_ts in (done_parent, partial_parent, fresh_parent):
        _seed_message(
            server_conn,
            parent_ts,
            payload_extra={"reply_count": 2, "thread_ts": parent_ts},
            source=_history_source(cursor="", page_index=0, final=True),
        )
    _seed_message(
        server_conn,
        "1700000110.000100",
        payload_extra={"thread_ts": done_parent},
        source=_replies_source(thread_ts=done_parent, cursor="", page_index=1, final=True),
    )
    _seed_message(
        server_conn,
        "1700000210.000100",
        payload_extra={"thread_ts": partial_parent},
        source=_replies_source(thread_ts=partial_parent, cursor="rc2", page_index=2, final=False),
    )
    plan = find_resume_plan(server_conn, _CHANNEL)
    assert plan is not None
    assert plan.history_done is True
    assert plan.done_thread_ts == {done_parent}
    assert plan.threads == (
        ThreadResume(thread_ts=partial_parent, cursor="rc2"),
        ThreadResume(thread_ts=fresh_parent, cursor=""),
    )


def test_replies_rows_without_history_rows_imply_history_done(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    parent_ts = "1700000100.000100"
    _seed_message(server_conn, parent_ts, payload_extra={"reply_count": 1, "thread_ts": parent_ts})
    _seed_message(
        server_conn,
        "1700000110.000100",
        payload_extra={"thread_ts": parent_ts},
        source=_replies_source(thread_ts=parent_ts, cursor="rc9", page_index=0, final=False),
    )
    plan = find_resume_plan(server_conn, _CHANNEL)
    assert plan is not None
    assert plan.history_done is True
    assert plan.threads == (ThreadResume(thread_ts=parent_ts, cursor="rc9"),)


# === Thread-worklist query (direct events query, not the view) ===


def test_worklist_excludes_deleted_parent(server_conn: psycopg.Connection[TupleRow]) -> None:
    """A tombstoned parent must never re-enter the worklist: fetching replies
    for a deleted thread is a Slack error, not an empty page."""
    deleted_parent = "1700000100.000100"
    live_parent = "1700000200.000100"
    for parent_ts in (deleted_parent, live_parent):
        _seed_message(
            server_conn,
            parent_ts,
            payload_extra={"reply_count": 1, "thread_ts": parent_ts},
            source=_history_source(cursor="", page_index=0, final=True),
        )
    record = EventRecord(
        stream=_STREAM,
        kind="message_deleted",
        ts=deleted_parent,
        payload={"deleted_ts": deleted_parent},
    )
    assert write_event(server_conn, record) is not None
    plan = find_resume_plan(server_conn, _CHANNEL)
    assert plan is not None
    assert plan.threads == (ThreadResume(thread_ts=live_parent, cursor=""),)


def test_worklist_includes_parent_known_only_via_message_changed(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    """A parent whose only reply_count evidence is a corrective/edit
    `message_changed` row (its base `message` row predates any replies) must
    stay in the worklist — parity with `active_thread_parents`."""
    parent_ts = "1700000100.000100"
    _seed_message(server_conn, parent_ts, source=_history_source(cursor="", page_index=0, final=True))
    record = EventRecord(
        stream=_STREAM,
        kind="message_changed",
        ts=parent_ts,
        payload={
            "previous_ts": parent_ts,
            "message": {"ts": parent_ts, "user": "U1", "text": "parent", "reply_count": 1, "thread_ts": parent_ts},
        },
    )
    assert write_event(server_conn, record) is not None
    plan = find_resume_plan(server_conn, _CHANNEL)
    assert plan is not None
    assert plan.threads == (ThreadResume(thread_ts=parent_ts, cursor=""),)


# === Timeout fallback ===


class _TimeoutInjectingCursor:
    """Delegates to a real cursor; the owning conn decides which execute dies."""

    def __init__(self, cur: Cursor[TupleRow], owner: _TimeoutInjectingConn) -> None:
        self._cur = cur
        self._owner = owner

    def __enter__(self) -> _TimeoutInjectingCursor:
        self._cur.__enter__()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self._cur.__exit__(*exc_info)  # pyright: ignore[reportArgumentType]

    def execute(self, query: QueryNoTemplate, params: Params | None = None) -> None:
        self._owner.executes += 1
        if self._owner.executes == self._owner.fail_on:
            raise self._owner.exc_type("canceling statement due to statement timeout")
        self._cur.execute(query, params)

    def fetchone(self) -> TupleRow | None:
        return self._cur.fetchone()

    def fetchall(self) -> list[TupleRow]:
        return self._cur.fetchall()


class _TimeoutInjectingConn:
    """Wraps a real connection so the Nth cursor.execute raises a PG timeout."""

    def __init__(
        self,
        conn: psycopg.Connection[TupleRow],
        fail_on: int,
        exc_type: type[Exception] = psycopg.errors.QueryCanceled,
    ) -> None:
        self._conn = conn
        self.fail_on = fail_on
        self.exc_type = exc_type
        self.executes = 0

    def cursor(self) -> _TimeoutInjectingCursor:
        return _TimeoutInjectingCursor(self._conn.cursor(), self)


def _seed_full_resume_state(conn: psycopg.Connection[TupleRow]) -> None:
    """Enough state that every find_resume_plan query genuinely runs."""
    parent_ts = "1700000200.000100"
    _seed_message(conn, "1700000300.000100", source=_history_source(cursor="c1", page_index=0, final=False))
    _seed_message(
        conn,
        parent_ts,
        payload_extra={"reply_count": 1, "thread_ts": parent_ts},
        source=_history_source(cursor="c1", page_index=0, final=False),
    )
    _seed_message(
        conn,
        "1700000210.000100",
        payload_extra={"thread_ts": parent_ts},
        source=_replies_source(thread_ts=parent_ts, cursor="rc1", page_index=1, final=False),
    )


# Execute order inside find_resume_plan: 1=terminal_watermark, 2=latest_history_page,
# 3=replies_progress, 4=known parents, 5=tombstones.
@pytest.mark.parametrize("fail_on", [1, 2, 3, 4, 5])
def test_pg_timeout_falls_back_to_fresh_plan(
    server_conn: psycopg.Connection[TupleRow],
    caplog: pytest.LogCaptureFixture,
    fail_on: int,
) -> None:
    """A statement timeout anywhere in the plan computation degrades to the
    first-boot answer (None) instead of propagating — the 2026-07-03
    CrashLoop. Atomic per plan: no partial worklists."""
    _seed_full_resume_state(server_conn)
    assert find_resume_plan(server_conn, _CHANNEL) is not None  # the seed is real resume state

    wrapped = cast("psycopg.Connection[TupleRow]", _TimeoutInjectingConn(server_conn, fail_on=fail_on))
    with caplog.at_level(logging.WARNING, logger="slack_fuse_server.backfill.resume"):
        assert find_resume_plan(wrapped, _CHANNEL) is None
    [record] = caplog.records
    assert _CHANNEL in record.getMessage()
    assert "timeout" in record.getMessage()


def test_lock_timeout_also_falls_back(
    server_conn: psycopg.Connection[TupleRow],
    caplog: pytest.LogCaptureFixture,
) -> None:
    _seed_full_resume_state(server_conn)
    wrapped = cast(
        "psycopg.Connection[TupleRow]",
        _TimeoutInjectingConn(server_conn, fail_on=1, exc_type=psycopg.errors.LockNotAvailable),
    )
    with caplog.at_level(logging.WARNING, logger="slack_fuse_server.backfill.resume"):
        assert find_resume_plan(wrapped, _CHANNEL) is None
    assert len(caplog.records) == 1


# === Worklist query scale (the 2026-07-03 CrashLoop regression) ===


def _seed_bulk_parents(conn: psycopg.Connection[TupleRow], n_parents: int, n_filler: int) -> None:
    """Bulk-seed via generate_series — write_event row-at-a-time is too slow here."""
    history_source = json.dumps({
        "producer": "backfill-history-page",
        "slack_cursor": "",
        "page_index": 0,
        "has_more": False,
        "final_page": True,
    })
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO events (stream, offset_in_stream, kind, ts, payload, source)
            SELECT %(stream)s, i, 'message', g.ts,
                   jsonb_build_object('ts', g.ts, 'user', 'U1', 'text', 'parent',
                                      'reply_count', 2, 'thread_ts', g.ts),
                   %(source)s::jsonb
            FROM (
                SELECT i, (1700000000 + i)::text || '.' || lpad((i %% 1000000)::text, 6, '0') AS ts
                FROM generate_series(1, %(n)s) AS i
            ) g
            """,
            {"stream": _STREAM, "n": n_parents, "source": history_source},
        )
        cur.execute(
            """
            INSERT INTO events (stream, offset_in_stream, kind, ts, payload)
            SELECT %(stream)s, %(base)s + i, 'message', g.ts,
                   jsonb_build_object('ts', g.ts, 'user', 'U1', 'text', 'plain')
            FROM (
                SELECT i, (1600000000 + i)::text || '.' || lpad((i %% 1000000)::text, 6, '0') AS ts
                FROM generate_series(1, %(n)s) AS i
            ) g
            """,
            {"stream": _STREAM, "base": n_parents, "n": n_filler},
        )
        cur.execute("ANALYZE events")


@pytest.mark.parametrize("n_parents", [1000, 5000])
def test_resume_plan_fast_at_scale(server_conn: psycopg.Connection[TupleRow], n_parents: int) -> None:
    """5000 thread parents among 20k filler rows must plan well under the
    budget — production hit the 30s statement_timeout via the view fold."""
    _seed_bulk_parents(server_conn, n_parents=n_parents, n_filler=20_000)
    start = time.perf_counter()
    plan = find_resume_plan(server_conn, _CHANNEL)
    elapsed = time.perf_counter() - start
    assert plan is not None
    assert plan.history_done is True
    assert len(plan.threads) == n_parents
    assert elapsed < 0.5, f"find_resume_plan took {elapsed:.3f}s at {n_parents} parents"


def test_worklist_query_uses_partial_indexes(server_conn: psycopg.Connection[TupleRow]) -> None:
    """The direct query must be served by the migration-0010 partial indexes,
    not a stream-wide scan — that's what keeps it fast cold-cache."""
    _seed_bulk_parents(server_conn, n_parents=1000, n_filler=20_000)
    with server_conn.cursor() as cur:
        cur.execute("EXPLAIN " + _KNOWN_PARENTS_SQL, {"stream": _STREAM})
        plan_text = "\n".join(str(row[0]) for row in cur.fetchall())
    assert "events_message_parent_hint_idx" in plan_text
    assert "events_changed_parent_hint_idx" in plan_text


# === End-to-end resume over a scripted fake Slack transport ===


def _history_body(messages: list[JsonObject], *, next_cursor: str) -> JsonObject:
    return cast(
        JsonObject,
        {
            "ok": True,
            "messages": messages,
            "has_more": bool(next_cursor),
            "response_metadata": {"next_cursor": next_cursor},
        },
    )


def _replies_body(messages: list[JsonObject]) -> JsonObject:
    return cast(
        JsonObject,
        {
            "ok": True,
            "messages": messages,
            "has_more": False,
            "response_metadata": {"next_cursor": ""},
        },
    )


class _ScriptedSlack:
    """Records every request; serves canned history/replies pages."""

    def __init__(self, history: dict[str, JsonObject], replies: dict[str, JsonObject]) -> None:
        self.history = history
        self.replies = replies
        self.history_cursors: list[str] = []
        self.replies_threads: list[str] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("conversations.history"):
            cursor = request.url.params.get("cursor") or ""
            self.history_cursors.append(cursor)
            body = self.history.get(cursor)
            assert body is not None, f"unexpected history cursor {cursor!r}"
            return httpx.Response(200, json=body, headers={"x-slack-req-id": f"req-{len(self.history_cursors)}"})
        if request.url.path.endswith("conversations.replies"):
            thread_ts = request.url.params.get("ts") or ""
            self.replies_threads.append(thread_ts)
            body = self.replies.get(thread_ts)
            assert body is not None, f"unexpected replies thread {thread_ts!r}"
            return httpx.Response(200, json=body)
        raise AssertionError(f"unexpected request {request.url.path}")

    def client(self) -> SlackClient:
        client = SlackClient("xoxp-test")
        client._http = httpx.Client(transport=httpx.MockTransport(self.handler))
        return client


def _resume_backfiller(
    slack: _ScriptedSlack,
    writer: OffsetWriter,
) -> SlackApiBackfiller:
    limiters = make_test_limiters()

    async def resume_plan(channel_id: str) -> ResumePlan | None:
        return await writer.run_read(
            lambda conn: find_resume_plan(conn, channel_id),
            limiter=limiters.admin_read,
        )

    return SlackApiBackfiller(slack.client(), trio.CapacityLimiter(1), _NO_SLEEP, resume_plan=resume_plan)


def _channel_rows(
    conn: psycopg.Connection[TupleRow], stream: str = _STREAM
) -> list[tuple[str, JsonObject, JsonObject | None]]:
    with conn.cursor() as cur:
        cur.execute("SELECT kind, payload, source FROM events WHERE stream = %s ORDER BY id", (stream,))
        rows = cur.fetchall()
    out: list[tuple[str, JsonObject, JsonObject | None]] = []
    for kind, payload, source in rows:
        assert isinstance(payload, dict)
        out.append((str(kind), cast(JsonObject, payload), cast("JsonObject | None", source)))
    return out


def test_backfill_resumes_from_stored_cursor(server_conn: psycopg.Connection[TupleRow]) -> None:
    """Crashed run: page 1 committed (cursor c2), no terminal event. The next
    run must fetch from c2, never from the start, and finish the thread walk
    with a worklist merged from the DB (pre-crash parents) and the freshly
    fetched pages (post-resume parents)."""
    pre_crash_parent = "1700000400.000100"
    _seed_message(server_conn, "1700000500.000100", source=_history_source(cursor="c2", page_index=0, final=False))
    _seed_message(
        server_conn,
        pre_crash_parent,
        payload_extra={"reply_count": 1, "thread_ts": pre_crash_parent},
        source=_history_source(cursor="c2", page_index=0, final=False),
    )

    new_parent = "1700000300.000100"
    page2 = [
        {"ts": new_parent, "user": "U1", "text": "p2 parent", "reply_count": 1, "thread_ts": new_parent},
        {"ts": "1700000250.000100", "user": "U1", "text": "plain"},
    ]
    slack = _ScriptedSlack(
        history={"c2": _history_body(cast("list[JsonObject]", page2), next_cursor="")},
        replies={
            pre_crash_parent: _replies_body([
                {
                    "ts": pre_crash_parent,
                    "user": "U1",
                    "text": "parent",
                    "reply_count": 1,
                    "thread_ts": pre_crash_parent,
                },
                {"ts": "1700000410.000100", "user": "U2", "text": "r", "thread_ts": pre_crash_parent},
            ]),
            new_parent: _replies_body([
                {"ts": new_parent, "user": "U1", "text": "p2 parent", "reply_count": 1, "thread_ts": new_parent},
                {"ts": "1700000310.000100", "user": "U2", "text": "r2", "thread_ts": new_parent},
            ]),
        },
    )
    writer = make_test_writer(server_conn)
    ctx = BackfillContext(
        writer=writer, health=HealthEmitter(writer), limiters=make_test_limiters(), warn_at=1000, abort_at=20000
    )

    result = trio.run(backfill_channel, _resume_backfiller(slack, writer), ChannelId(_CHANNEL), ctx)

    assert not result.aborted
    # Resume: only the stored cursor was fetched, never the first page.
    assert slack.history_cursors == ["c2"]
    # Thread worklist merged DB-known + freshly discovered parents.
    assert sorted(slack.replies_threads) == sorted([pre_crash_parent, new_parent])
    rows = _channel_rows(server_conn)
    written_ts = {str(payload.get("ts")) for kind, payload, _ in rows if kind == "message"}
    assert {"1700000250.000100", new_parent, "1700000310.000100", "1700000410.000100"} <= written_ts
    # After completion the terminal event gates resume off: next run is fresh.
    assert find_resume_plan(server_conn, _CHANNEL) is None


def test_backfill_skips_history_when_final_page_stored(server_conn: psycopg.Connection[TupleRow]) -> None:
    parent_ts = "1700000400.000100"
    _seed_message(
        server_conn,
        parent_ts,
        payload_extra={"reply_count": 1, "thread_ts": parent_ts},
        source=_history_source(cursor="", page_index=0, final=True),
    )
    slack = _ScriptedSlack(
        history={},  # any history fetch is a test failure
        replies={
            parent_ts: _replies_body([
                {"ts": parent_ts, "user": "U1", "text": "parent", "reply_count": 1, "thread_ts": parent_ts},
                {"ts": "1700000410.000100", "user": "U2", "text": "r", "thread_ts": parent_ts},
            ]),
        },
    )
    writer = make_test_writer(server_conn)
    ctx = BackfillContext(
        writer=writer, health=HealthEmitter(writer), limiters=make_test_limiters(), warn_at=1000, abort_at=20000
    )

    result = trio.run(backfill_channel, _resume_backfiller(slack, writer), ChannelId(_CHANNEL), ctx)

    assert not result.aborted
    assert slack.history_cursors == []
    assert slack.replies_threads == [parent_ts]


def test_a3_stale_parent_construction_terminates(server_conn: psycopg.Connection[TupleRow]) -> None:
    """The prior review's livelock: parent metadata stale-high (reply_count=2)
    after a real reply deletion. Under source annotation the replies fetch
    writes a corrective parent + a final_page marker, and the completion
    signal is Slack's own has_more — the thread can never be re-selected
    forever."""
    parent_ts = "1700000100.000100"
    live_reply = "1700000120.000100"
    deleted_reply = "1700000110.000100"
    # Local state: parent claims 2 replies; one was really deleted upstream.
    _seed_message(
        server_conn,
        parent_ts,
        payload_extra={"reply_count": 2, "thread_ts": parent_ts, "latest_reply": live_reply},
        source=_history_source(cursor="", page_index=0, final=True),
    )
    _seed_message(server_conn, deleted_reply, payload_extra={"thread_ts": parent_ts})
    _seed_message(server_conn, live_reply, payload_extra={"thread_ts": parent_ts})
    assert (
        write_event(
            server_conn,
            EventRecord(
                stream=_STREAM,
                kind="message_deleted",
                ts=deleted_reply,
                payload={"deleted_ts": deleted_reply},
            ),
        )
        is not None
    )

    fresh_parent: JsonObject = {
        "ts": parent_ts,
        "user": "U1",
        "text": "parent",
        "reply_count": 1,
        "latest_reply": live_reply,
        "thread_ts": parent_ts,
    }
    slack = _ScriptedSlack(
        history={},
        replies={
            parent_ts: _replies_body([
                fresh_parent,
                {"ts": live_reply, "user": "U2", "text": "r", "thread_ts": parent_ts},
            ]),
        },
    )
    writer = make_test_writer(server_conn)
    ctx = BackfillContext(
        writer=writer, health=HealthEmitter(writer), limiters=make_test_limiters(), warn_at=1000, abort_at=20000
    )

    result = trio.run(backfill_channel, _resume_backfiller(slack, writer), ChannelId(_CHANNEL), ctx)

    assert not result.aborted
    assert slack.replies_threads == [parent_ts]
    rows = _channel_rows(server_conn)
    correctives = [
        (payload, source)
        for kind, payload, source in rows
        if kind == "message_changed" and payload.get("previous_ts") == parent_ts
    ]
    assert len(correctives) == 1
    corrected_payload, corrective_source = correctives[0]
    corrected_message = corrected_payload.get("message")
    assert isinstance(corrected_message, dict) and corrected_message.get("reply_count") == 1
    assert corrective_source is not None
    assert corrective_source["producer"] == "backfill-corrective-parent"
    # A final replies-page marker landed for the thread, so a resumed run
    # excludes it — and the run itself terminated with a completion event.
    final_markers = [
        source
        for _kind, _payload, source in rows
        if source is not None
        and source.get("producer") == "backfill-replies-page"
        and source.get("thread_ts") == parent_ts
        and source.get("final_page") is True
    ]
    assert final_markers
    assert find_resume_plan(server_conn, _CHANNEL) is None


def test_live_history_pages_carry_full_source_envelope(server_conn: psycopg.Connection[TupleRow]) -> None:
    """Fresh two-page walk: every record carries the page's cursors, index,
    termination fact and API exchange metadata."""
    page1 = [{"ts": "1700000500.000100", "user": "U1", "text": "newest"}]
    page2 = [{"ts": "1700000400.000100", "user": "U1", "text": "older"}]
    slack = _ScriptedSlack(
        history={
            "": _history_body(cast("list[JsonObject]", page1), next_cursor="c2"),
            "c2": _history_body(cast("list[JsonObject]", page2), next_cursor=""),
        },
        replies={},
    )
    writer = make_test_writer(server_conn)
    ctx = BackfillContext(
        writer=writer, health=HealthEmitter(writer), limiters=make_test_limiters(), warn_at=1000, abort_at=20000
    )

    result = trio.run(backfill_channel, _resume_backfiller(slack, writer), ChannelId(_CHANNEL), ctx)

    assert result.events_written == 2
    rows = _channel_rows(server_conn)
    by_ts = {str(payload["ts"]): source for _kind, payload, source in rows}
    first = by_ts["1700000500.000100"]
    assert first is not None
    assert first["producer"] == "backfill-history-page"
    assert first["slack_cursor"] == "c2"
    assert first["page_index"] == 0
    assert first["has_more"] is True
    assert first["final_page"] is False
    assert "prior_cursor" not in first
    assert first["api_endpoint"] == "conversations.history"
    assert first["slack_request_id"] == "req-1"
    assert isinstance(first["api_latency_ms"], int)
    second = by_ts["1700000400.000100"]
    assert second is not None
    assert second["prior_cursor"] == "c2"
    assert second["page_index"] == 1
    assert second["final_page"] is True
    assert "oldest" not in second
