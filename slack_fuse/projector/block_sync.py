# pyright: reportPrivateUsage=false
"""Periodic client reconciliation from server-side blocked_channels SSOT."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

import httpx
import trio

from slack_fuse.projector.apply import (  # pyright: ignore[reportPrivateUsage]
    _default_tier,
    _force_blocked_manual,
)
from slack_fuse.projector.block_fetch import blocked_channel_ids_from_payload, get_blocked_channels

if TYPE_CHECKING:
    import psycopg
    from psycopg.rows import TupleRow

log = logging.getLogger(__name__)

DEFAULT_BLOCK_SYNC_INTERVAL_S = 30.0


def apply_blocked_channel_sync(conn: psycopg.Connection[TupleRow], blocked_ids: set[str]) -> None:
    """Apply one server block-list snapshot to the client ``channels`` table."""
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("SELECT channel_id FROM server_block_sync")
        previously_synced = {str(row[0]) for row in cur.fetchall()}

        for channel_id in sorted(blocked_ids):
            cur.execute(
                """
                INSERT INTO server_block_sync (channel_id, synced_at)
                VALUES (%s, now())
                ON CONFLICT (channel_id) DO UPDATE SET synced_at = EXCLUDED.synced_at
                """,
                (channel_id,),
            )
            _force_blocked_manual(cur, channel_id)

        for channel_id in sorted(previously_synced - blocked_ids):
            cur.execute(
                "SELECT is_im, is_mpim, is_member, is_archived, tier, tier_source "
                "FROM channels WHERE channel_id = %s",
                (channel_id,),
            )
            row = cur.fetchone()
            if row is not None and str(row[4]) == "blocked" and str(row[5]) == "manual":
                tier = _default_tier(
                    is_im=bool(row[0]),
                    is_mpim=bool(row[1]),
                    is_member=bool(row[2]),
                    is_archived=bool(row[3]),
                )
                cur.execute(
                    "UPDATE channels SET tier = %s, tier_source = 'auto', subscribed = %s, "
                    "updated_at = now() WHERE channel_id = %s",
                    (tier, tier != "blocked", channel_id),
                )
            cur.execute("DELETE FROM server_block_sync WHERE channel_id = %s", (channel_id,))


def sync_blocked_channels_once(
    http_client: httpx.Client,
    base_http_url: str,
    conn: psycopg.Connection[TupleRow],
    *,
    shared_secret: str | None = None,
) -> bool:
    """Fetch the server block list and reconcile local tiers.

    Returns True when a snapshot was applied, False when the server was
    unreachable or returned a non-200 response.
    """
    status, payload = get_blocked_channels(http_client, base_http_url, shared_secret=shared_secret)
    if status != 200:
        log.warning("block-sync: GET /blocked-channels returned %s", status)
        return False
    apply_blocked_channel_sync(conn, blocked_channel_ids_from_payload(payload))
    return True


async def sync_blocked_channels_periodically(  # noqa: PLR0913 - process wiring needs explicit factories/knobs.
    make_http_client: Callable[[], httpx.Client],
    base_http_url: str,
    open_conn: Callable[[], psycopg.Connection[TupleRow]],
    *,
    shared_secret: str | None = None,
    interval_s: float = DEFAULT_BLOCK_SYNC_INTERVAL_S,
    limiter: trio.CapacityLimiter | None = None,
) -> None:
    """Long-running trio task for split-mode mount processes."""
    http_client = make_http_client()
    conn = open_conn()
    try:
        while True:
            try:
                await trio.to_thread.run_sync(
                    lambda: sync_blocked_channels_once(
                        http_client,
                        base_http_url,
                        conn,
                        shared_secret=shared_secret,
                    ),
                    limiter=limiter,
                )
            except Exception:
                log.exception("block-sync: cycle failed")
            await trio.sleep(interval_s)
    finally:
        http_client.close()
        conn.close()


__all__ = [
    "DEFAULT_BLOCK_SYNC_INTERVAL_S",
    "apply_blocked_channel_sync",
    "sync_blocked_channels_once",
    "sync_blocked_channels_periodically",
]
