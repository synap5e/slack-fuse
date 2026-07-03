"""Restart-safe backfill resume, driven by the `events.source` envelope.

A crashed backfill (deploy mid-run, OOM, network loss) leaves committed
page-atomic batches whose `source` rows record the Slack cursor each page was
fetched with and whether Slack said the collection was exhausted
(`final_page`). `find_resume_plan()` reads those rows back so the next run
continues from the last committed page instead of re-walking the channel.

Termination gate: only rows *newer* (by `events.id`) than the channel's latest
`backfill_completed` / `backfill_aborted` health event count as resume state.
A completed run must not make a later operator re-backfill a silent no-op, and
an aborted run must not be dug past its size cap — only a crashed run (pages
written, no terminal event) leaves resume state. This is read-side policy
derivation, like Wave 1 D's skip-completed check; no progress facts are ever
*written* anywhere.

`--since` gap-fill runs write `source->>'oldest'` and are never used as resume
anchors: their history cursors walk a bounded window and their replies pages
persist only the post-`since` tail, so neither is evidence of full coverage.

Completion signal is Slack's own `has_more` (stored as `final_page`), never
local count arithmetic — the thread-predicate livelock class from the prior
derived-state designs cannot occur here.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

from psycopg import Connection
from psycopg.rows import TupleRow

from slack_fuse_server.slurper.offsets import PG_TIMEOUT_EXCEPTIONS

log = logging.getLogger(__name__)

# Mirrors the SQL `is_valid_slack_ts` from migration 0008.
_VALID_SLACK_TS = re.compile(r"^[1-9][0-9]{9}\.[0-9]{6}$")


@dataclass(frozen=True, slots=True)
class ThreadResume:
    """One thread the replies phase still needs. `cursor=""` = fetch from the start."""

    thread_ts: str
    cursor: str = ""


@dataclass(frozen=True, slots=True)
class ResumePlan:
    """Where a crashed full-history backfill left off for one channel.

    `history_cursor` is the cursor to hand `conversations.history` for the next
    page (`""` when no page committed). `threads` is the DB-known worklist with
    per-thread resume cursors; parents discovered by pages fetched after the
    resume merge in-memory on top. `done_thread_ts` are threads whose replies
    already reached a `final_page=true` row and must not be re-fetched.
    """

    history_done: bool
    history_cursor: str
    threads: tuple[ThreadResume, ...]
    done_thread_ts: frozenset[str]


def _terminal_watermark(conn: Connection[TupleRow], channel_id: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(MAX(id), 0)
            FROM events
            WHERE stream = 'slurper-health'
              AND kind IN ('backfill_completed', 'backfill_aborted')
              AND payload->>'channel_id' = %s
            """,
            (channel_id,),
        )
        row = cur.fetchone()
    return 0 if row is None else int(row[0])


def _latest_history_page(
    conn: Connection[TupleRow],
    stream: str,
    watermark: int,
) -> tuple[str, bool] | None:
    """(next_cursor, final_page) of the newest un-terminated full-run history page."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT source->>'slack_cursor', (source->>'final_page')::bool
            FROM events
            WHERE stream = %s
              AND source->>'producer' = 'backfill-history-page'
              AND NOT (source ? 'oldest')
              AND id > %s
            ORDER BY offset_in_stream DESC
            LIMIT 1
            """,
            (stream, watermark),
        )
        row = cur.fetchone()
    if row is None:
        return None
    cursor_raw, final_raw = row
    cursor = str(cursor_raw) if cursor_raw is not None else ""
    return (cursor, bool(final_raw))


def _replies_progress(
    conn: Connection[TupleRow],
    stream: str,
    watermark: int,
) -> dict[str, tuple[str, bool]]:
    """Per-thread latest replies-page (next_cursor, final_page) past the watermark."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (source->>'thread_ts')
                source->>'thread_ts',
                source->>'slack_cursor',
                (source->>'final_page')::bool
            FROM events
            WHERE stream = %s
              AND source->>'producer' = 'backfill-replies-page'
              AND NOT (source ? 'oldest')
              AND id > %s
            ORDER BY source->>'thread_ts', offset_in_stream DESC
            """,
            (stream, watermark),
        )
        rows = cur.fetchall()
    progress: dict[str, tuple[str, bool]] = {}
    for thread_ts_raw, cursor_raw, final_raw in rows:
        if thread_ts_raw is None:
            continue
        cursor = str(cursor_raw) if cursor_raw is not None else ""
        progress[str(thread_ts_raw)] = (cursor, bool(final_raw))
    return progress


# Matched by the migration-0010 partial indexes: the query predicates must
# repeat the index predicates verbatim (`kind = …` + `payload … ? 'reply_count'`)
# so the planner can use them; the `> 0` filter is applied on the index rows.
_KNOWN_PARENTS_SQL = """
    SELECT DISTINCT parent_ts FROM (
        SELECT payload->>'ts' AS parent_ts
        FROM events
        WHERE stream = %(stream)s
          AND kind = 'message'
          AND payload ? 'reply_count'
          AND (payload->>'reply_count')::int > 0
        UNION ALL
        SELECT (payload->'message')->>'ts' AS parent_ts
        FROM events
        WHERE stream = %(stream)s
          AND kind = 'message_changed'
          AND payload->'message' ? 'reply_count'
          AND ((payload->'message')->>'reply_count')::int > 0
    ) candidates
    WHERE parent_ts IS NOT NULL
"""

_TOMBSTONED_TS_SQL = """
    SELECT DISTINCT payload->>'deleted_ts'
    FROM events
    WHERE stream = %(stream)s
      AND kind = 'message_deleted'
"""


def _known_thread_parents(conn: Connection[TupleRow], stream: str) -> list[str]:
    """Every thread parent this stream has seen, minus deleted ones.

    Deliberately NOT a read of `active_thread_parents`: the view's
    latest-per-ts fold times out cold-cache at production scale (the
    2026-07-03 CrashLoop), and the worklist doesn't need the view's exactness.
    Including a parent whose *latest* state dropped to `reply_count = 0` (all
    replies deleted) only costs one no-op replies fetch that re-marks the
    thread `final_page`. Deleted parents must still be excluded —
    `conversations.replies` on a deleted thread is a Slack error, not an empty
    page — hence the tombstone subtraction (served by
    `events_message_deleted_target_idx`).
    """
    with conn.cursor() as cur:
        cur.execute(_KNOWN_PARENTS_SQL, {"stream": stream})
        parents = [str(row[0]) for row in cur.fetchall()]
        cur.execute(_TOMBSTONED_TS_SQL, {"stream": stream})
        deleted = {str(row[0]) for row in cur.fetchall() if row[0] is not None}
    return sorted(ts for ts in parents if ts not in deleted and _VALID_SLACK_TS.fullmatch(ts))


def find_resume_plan(conn: Connection[TupleRow], channel_id: str) -> ResumePlan | None:
    """Resume state for one channel, or None when there is no crashed run.

    Only meaningful for full-history runs (`since_ts is None`); callers must
    not consult it for `--since` gap-fills.

    A PostgreSQL timeout while computing the plan degrades to None — the
    first-boot answer (walk from Slack's newest, treat every thread as
    unfinished) — instead of killing the caller's nursery. The fallback is
    atomic per plan: partial results are worse than none, because a plan whose
    worklist query died mid-scan could present as falsely complete and
    livelock the channel. Cost is bounded at one redundant channel re-walk.
    """
    stream = f"channel:{channel_id}"
    query_kind = "terminal_watermark"
    try:
        watermark = _terminal_watermark(conn, channel_id)
        query_kind = "latest_history_page"
        history = _latest_history_page(conn, stream, watermark)
        query_kind = "replies_progress"
        replies = _replies_progress(conn, stream, watermark)
        if history is None and not replies:
            return None
        query_kind = "known_thread_parents"
        known_parents = _known_thread_parents(conn, stream)
    except PG_TIMEOUT_EXCEPTIONS:
        log.warning(
            "resume: PostgreSQL timeout computing resume plan for %s (query=%s); "
            "starting fresh as if no resume state existed",
            channel_id,
            query_kind,
            exc_info=True,
        )
        return None

    if history is not None:
        history_cursor, history_done = history
        if history_done:
            history_cursor = ""
    else:
        # Replies rows without history rows: the replies phase only starts
        # after history pagination exhausts, so history is done (its pages all
        # deduped against already-present rows and left no marker).
        history_cursor, history_done = "", True

    done = frozenset(thread_ts for thread_ts, (_cursor, final) in replies.items() if final)
    threads = tuple(
        ThreadResume(
            thread_ts=parent_ts,
            cursor=replies[parent_ts][0] if parent_ts in replies else "",
        )
        for parent_ts in known_parents
        if parent_ts not in done
    )
    return ResumePlan(
        history_done=history_done,
        history_cursor=history_cursor,
        threads=threads,
        done_thread_ts=done,
    )
