"""Cursor advance for the client projector.

Per RFC §Schemas → Client: projections store. `cursors.applied_offset` tracks
how far each stream's applier has progressed; advanced in the same transaction
as the event apply so `(chunks, chunk_mentions, cursors)` stays mutually
consistent. On crash/restart, the WS client re-subscribes with `since =
applied_offset`; the server re-sends events from that point and any partial
batch is replayed harmlessly (every chunk write is `ON CONFLICT DO UPDATE`).
"""

from __future__ import annotations

from psycopg import Cursor
from psycopg.rows import TupleRow


def read_cursor(cur: Cursor[TupleRow], stream: str) -> int:
    """Return `applied_offset` for `stream`, or 0 if the cursor row is absent."""
    cur.execute("SELECT applied_offset FROM cursors WHERE stream = %s", (stream,))
    row = cur.fetchone()
    return 0 if row is None else int(row[0])


def advance_cursor(cur: Cursor[TupleRow], stream: str, offset: int) -> None:
    """Set `applied_offset` for `stream`. Idempotent; safe to call in replay.

    Uses `GREATEST` to refuse to move the cursor backwards — replays of older
    events on a partially-advanced cursor must not lose progress.
    """
    cur.execute(
        "INSERT INTO cursors (stream, applied_offset) VALUES (%s, %s) "
        "ON CONFLICT (stream) DO UPDATE SET "
        "  applied_offset = GREATEST(cursors.applied_offset, EXCLUDED.applied_offset), "
        "  updated_at = now()",
        (stream, offset),
    )
