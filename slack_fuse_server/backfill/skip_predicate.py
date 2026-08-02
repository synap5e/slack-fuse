"""Batched local-state predicate for Slack thread expansion."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from psycopg import Connection
from psycopg.rows import TupleRow


@dataclass(frozen=True, slots=True)
class ThreadParent:
    """Slack's thread summary from a ``conversations.history`` parent."""

    thread_ts: str
    reply_count: int
    latest_reply: str | None


_ACTIVE_REPLY_SUMMARIES_SQL = """
    SELECT
        thread_ts,
        COUNT(DISTINCT ts) AS local_count,
        MAX(ts) AS local_max_reply
    FROM active_messages
    WHERE stream = %(stream)s
      AND thread_ts = ANY(%(thread_ts)s)
      AND ts <> thread_ts
    GROUP BY thread_ts
"""


def find_caught_up_threads(
    conn: Connection[TupleRow],
    channel_id: str,
    parents: Sequence[ThreadParent],
) -> set[str]:
    """Return parents whose active local replies exactly match Slack's summary.

    The one grouped read is deliberately against ``active_messages``: that
    view folds ``message_changed`` and ``message_deleted`` facts before replies
    are counted. Missing rows and null/mismatched latest timestamps are fetches.
    """
    by_ts = {parent.thread_ts: parent for parent in parents}
    if not by_ts:
        return set()

    with conn.cursor() as cur:
        cur.execute(
            _ACTIVE_REPLY_SUMMARIES_SQL,
            {"stream": f"channel:{channel_id}", "thread_ts": list(by_ts)},
        )
        rows = cur.fetchall()

    caught_up: set[str] = set()
    for thread_ts_raw, local_count_raw, local_max_raw in rows:
        thread_ts = str(thread_ts_raw)
        parent = by_ts.get(thread_ts)
        if parent is None or local_max_raw is None:
            continue
        local_count = int(local_count_raw)
        local_max = str(local_max_raw)
        if local_count == parent.reply_count and local_max == parent.latest_reply:
            caught_up.add(thread_ts)
    return caught_up
