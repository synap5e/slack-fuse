"""Mutable operator policy for server-side channel blocks.

Channel blocks are not Slack events. They are operator intent, stored in the
``blocked_channels`` table and queried by operational surfaces that need to
avoid expensive or unwanted channel work.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, TypedDict

if TYPE_CHECKING:
    import psycopg
    from psycopg.rows import TupleRow


class BlockedChannelRow(TypedDict):
    channel_id: str
    blocked_at: str
    reason: str | None


class BlockedChannelError(RuntimeError):
    """Raised when an operation is rejected by the operator block table."""

    def __init__(self, channel_id: str) -> None:
        super().__init__(f"channel {channel_id} is blocked")
        self.channel_id = channel_id


def _format_blocked_at(value: object) -> str:
    if isinstance(value, datetime):
        dt = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return str(value)


def _row(channel_id: object, blocked_at: object, reason: object) -> BlockedChannelRow:
    return {
        "channel_id": str(channel_id),
        "blocked_at": _format_blocked_at(blocked_at),
        "reason": reason if isinstance(reason, str) else None,
    }


def list_blocked_channels(conn: psycopg.Connection[TupleRow]) -> list[BlockedChannelRow]:
    """Return the block table ordered by channel id."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT channel_id, blocked_at, reason FROM blocked_channels ORDER BY channel_id"
        )
        return [_row(*row) for row in cur.fetchall()]


def blocked_channel_ids(conn: psycopg.Connection[TupleRow]) -> set[str]:
    """Return the current blocked channel ids."""
    with conn.cursor() as cur:
        cur.execute("SELECT channel_id FROM blocked_channels")
        return {str(row[0]) for row in cur.fetchall()}


def is_channel_blocked(conn: psycopg.Connection[TupleRow], channel_id: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM blocked_channels WHERE channel_id = %s", (channel_id,))
        return cur.fetchone() is not None


def get_blocked_channel(
    conn: psycopg.Connection[TupleRow],
    channel_id: str,
) -> BlockedChannelRow | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT channel_id, blocked_at, reason FROM blocked_channels WHERE channel_id = %s",
            (channel_id,),
        )
        row = cur.fetchone()
    return None if row is None else _row(*row)


def block_channel(
    conn: psycopg.Connection[TupleRow],
    channel_id: str,
    *,
    reason: str | None = None,
) -> BlockedChannelRow:
    """Insert a block row if absent and return the stored row.

    Existing blocks are deliberately left unchanged: POST is idempotent and does
    not rewrite ``blocked_at`` or the operator's original reason.
    """
    clean_reason = reason.strip() if isinstance(reason, str) and reason.strip() else None
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO blocked_channels (channel_id, reason)
            VALUES (%s, %s)
            ON CONFLICT (channel_id) DO NOTHING
            RETURNING channel_id, blocked_at, reason
            """,
            (channel_id, clean_reason),
        )
        inserted = cur.fetchone()
        if inserted is not None:
            return _row(*inserted)
        cur.execute(
            "SELECT channel_id, blocked_at, reason FROM blocked_channels WHERE channel_id = %s",
            (channel_id,),
        )
        existing = cur.fetchone()
    if existing is None:  # pragma: no cover - impossible unless the row vanished mid-TX.
        raise LookupError(channel_id)
    return _row(*existing)


def unblock_channel(conn: psycopg.Connection[TupleRow], channel_id: str) -> None:
    """Delete a block row. Missing rows are a no-op."""
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("DELETE FROM blocked_channels WHERE channel_id = %s", (channel_id,))


def raise_if_blocked(conn: psycopg.Connection[TupleRow], channel_id: str) -> None:
    if is_channel_blocked(conn, channel_id):
        raise BlockedChannelError(channel_id)


__all__ = [
    "BlockedChannelError",
    "BlockedChannelRow",
    "block_channel",
    "blocked_channel_ids",
    "get_blocked_channel",
    "is_channel_blocked",
    "list_blocked_channels",
    "raise_if_blocked",
    "unblock_channel",
]
