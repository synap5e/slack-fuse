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

from dataclasses import dataclass

from psycopg import Connection
from psycopg.rows import TupleRow


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


def _known_thread_parents(conn: Connection[TupleRow], stream: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT parent_ts FROM active_thread_parents WHERE stream = %s ORDER BY parent_ts",
            (stream,),
        )
        return [str(row[0]) for row in cur.fetchall() if row[0] is not None]


def find_resume_plan(conn: Connection[TupleRow], channel_id: str) -> ResumePlan | None:
    """Resume state for one channel, or None when there is no crashed run.

    Only meaningful for full-history runs (`since_ts is None`); callers must
    not consult it for `--since` gap-fills.
    """
    stream = f"channel:{channel_id}"
    watermark = _terminal_watermark(conn, channel_id)
    history = _latest_history_page(conn, stream, watermark)
    replies = _replies_progress(conn, stream, watermark)
    if history is None and not replies:
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
        for parent_ts in _known_thread_parents(conn, stream)
        if parent_ts not in done
    )
    return ResumePlan(
        history_done=history_done,
        history_cursor=history_cursor,
        threads=threads,
        done_thread_ts=done,
    )
