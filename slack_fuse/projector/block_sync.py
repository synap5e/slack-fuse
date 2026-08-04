# pyright: reportPrivateUsage=false
"""Periodic client reconciliation from server-side blocked_channels SSOT."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import httpx
import trio

from slack_fuse.projector.apply import (  # pyright: ignore[reportPrivateUsage]
    _default_tier,
    _force_blocked_manual,
)
from slack_fuse.projector.block_fetch import blocked_channel_ids_from_payload, get_blocked_channels
from slack_fuse.projector.projection_ledger import RENDERER_VERSION, bump_channel_visibility_targets
from slack_fuse.projector.reconnecting_conn import TupleConnection

log = logging.getLogger(__name__)

DEFAULT_BLOCK_SYNC_INTERVAL_S = 30.0


@dataclass(frozen=True, slots=True)
class VisibilityChanges:
    """Channel visibility transitions caused by one server block snapshot."""

    newly_subscribed: frozenset[str]
    newly_blocked: frozenset[str]


def desired_subscribed_channel_ids(conn: TupleConnection) -> frozenset[str]:
    """Return the full channel subscription postcondition from local state."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT channel_id FROM channels "
            "WHERE tier != 'blocked' AND subscribed = TRUE ORDER BY channel_id"
        )
        return frozenset(str(row[0]) for row in cur.fetchall())


def apply_blocked_channel_sync(conn: TupleConnection, blocked_ids: set[str]) -> VisibilityChanges:
    """Apply one server block-list snapshot to the client ``channels`` table.

    Returns both directions of visibility change. The WSClient consumes
    ``newly_subscribed`` so it can dynamically add appliers + send
    SubscribeFrame for newly-visible streams. The mount consumes
    ``newly_blocked`` to invalidate materialized FUSE inodes and projected
    files that could otherwise remain visible from cache.

    FINDING-14 (2026-07-17): preserve local tier state across a server
    block/unblock cycle. When a server block first applies (row not
    previously in ``server_block_sync``), snapshot the channel's current
    ``(tier, tier_source)`` into ``prior_tier`` / ``prior_tier_source``
    on the ``server_block_sync`` row. On unblock, restore that pair
    instead of resetting to auto/default. Otherwise:
      * an operator-pinned ``tier='hot'`` channel would come out ``auto``
        (pin lost);
      * a locally CLI-blocked channel that the server also blocks then
        unblocks would come out unblocked (local block silently removed).
    """
    newly_subscribed: set[str] = set()
    newly_blocked: set[str] = set()
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("SELECT channel_id FROM server_block_sync")
        previously_synced = {str(row[0]) for row in cur.fetchall()}

        for channel_id in sorted(blocked_ids):
            # A previously-synced channel can become visible again between
            # cycles (for example, a local channel-list mutation or operator
            # tier change). Inspect every row immediately before enforcing the
            # server policy so both first-time and re-applied transitions are
            # reported to cache invalidators.
            cur.execute(
                "SELECT tier FROM channels WHERE channel_id = %s",
                (channel_id,),
            )
            tier_row = cur.fetchone()
            if tier_row is not None and str(tier_row[0]) != "blocked":
                newly_blocked.add(channel_id)

            if channel_id in previously_synced:
                # Already server-blocked — refresh the synced_at heartbeat.
                cur.execute(
                    "UPDATE server_block_sync SET synced_at = now() WHERE channel_id = %s",
                    (channel_id,),
                )
            else:
                # First application of server block — snapshot the CURRENT
                # local (tier, tier_source) so the eventual unblock can
                # restore it. NULLs on brand-new channels the client has
                # never seen; the unblock branch then falls back to auto.
                cur.execute(
                    "SELECT tier, tier_source FROM channels WHERE channel_id = %s",
                    (channel_id,),
                )
                snapshot_row = cur.fetchone()
                prior_tier = str(snapshot_row[0]) if snapshot_row is not None else None
                prior_tier_source = str(snapshot_row[1]) if snapshot_row is not None else None
                cur.execute(
                    """
                    INSERT INTO server_block_sync (channel_id, synced_at, prior_tier, prior_tier_source)
                    VALUES (%s, now(), %s, %s)
                    ON CONFLICT (channel_id) DO UPDATE SET
                        synced_at = EXCLUDED.synced_at,
                        prior_tier = COALESCE(server_block_sync.prior_tier, EXCLUDED.prior_tier),
                        prior_tier_source = COALESCE(
                            server_block_sync.prior_tier_source, EXCLUDED.prior_tier_source
                        )
                    """,
                    (channel_id, prior_tier, prior_tier_source),
                )
            _force_blocked_manual(cur, channel_id)

        for channel_id in sorted(previously_synced - blocked_ids):
            cur.execute(
                "SELECT c.is_im, c.is_mpim, c.is_member, c.is_archived, c.tier, c.tier_source, "
                "  s.prior_tier, s.prior_tier_source "
                "FROM channels c LEFT JOIN server_block_sync s ON s.channel_id = c.channel_id "
                "WHERE c.channel_id = %s",
                (channel_id,),
            )
            row = cur.fetchone()
            if row is not None and str(row[4]) == "blocked" and str(row[5]) == "manual":
                prior_tier = str(row[6]) if row[6] is not None else None
                prior_tier_source = str(row[7]) if row[7] is not None else None
                # FINDING-14: if we recorded the pre-block state, restore it —
                # unless it was itself 'blocked' from before (e.g. locally-CLI-
                # blocked channel that the server ALSO blocked; the local
                # block was authoritative before, still is).
                if prior_tier is not None and prior_tier_source is not None:
                    tier = prior_tier
                    tier_source = prior_tier_source
                else:
                    tier = _default_tier(
                        is_im=bool(row[0]),
                        is_mpim=bool(row[1]),
                        is_member=bool(row[2]),
                        is_archived=bool(row[3]),
                    )
                    tier_source = "auto"
                cur.execute(
                    "UPDATE channels SET tier = %s, tier_source = %s, subscribed = %s, "
                    "updated_at = now() WHERE channel_id = %s",
                    (tier, tier_source, tier != "blocked", channel_id),
                )
                if tier != "blocked":
                    newly_subscribed.add(channel_id)
            cur.execute("DELETE FROM server_block_sync WHERE channel_id = %s", (channel_id,))

        # DUAL-WRITE: visibility mutations are cursor-neutral, so their durable
        # invalidation signal must commit with the channel row itself. The
        # existing DiskProjection dirty callback remains the active reader-side
        # correctness path until PR 3.
        for channel_id in sorted(newly_blocked | newly_subscribed):
            bump_channel_visibility_targets(cur, channel_id, RENDERER_VERSION)
    return VisibilityChanges(
        newly_subscribed=frozenset(newly_subscribed),
        newly_blocked=frozenset(newly_blocked),
    )


def sync_blocked_channels_once(
    http_client: httpx.Client,
    base_http_url: str,
    conn: TupleConnection,
    *,
    shared_secret: str | None = None,
) -> VisibilityChanges | None:
    """Fetch the server block list and reconcile local tiers.

    Returns visibility transitions from an applied snapshot; ``None`` when the
    server was unreachable or returned a non-200 response (so callers can
    distinguish "no changes this cycle" from "sync failed").
    """
    status, payload = get_blocked_channels(http_client, base_http_url, shared_secret=shared_secret)
    if status != 200:
        log.warning("block-sync: GET /blocked-channels returned %s", status)
        return None
    return apply_blocked_channel_sync(conn, blocked_channel_ids_from_payload(payload))


def _sync_cycle_with_desired_subscriptions(
    http_client: httpx.Client,
    base_http_url: str,
    conn: TupleConnection,
    shared_secret: str | None,
) -> tuple[VisibilityChanges | None, frozenset[str] | None]:
    changes = sync_blocked_channels_once(
        http_client,
        base_http_url,
        conn,
        shared_secret=shared_secret,
    )
    desired = desired_subscribed_channel_ids(conn) if changes is not None else None
    return changes, desired


async def _dispatch_cycle_callbacks(
    changes: VisibilityChanges | None,
    desired: frozenset[str] | None,
    on_newly_subscribed: Callable[[frozenset[str]], Awaitable[None]] | None,
    on_newly_blocked: Callable[[frozenset[str]], Awaitable[None]] | None,
    on_reconcile_subscriptions: Callable[[frozenset[str]], Awaitable[None]] | None,
) -> None:
    if changes is not None and changes.newly_subscribed and on_newly_subscribed is not None:
        try:
            await on_newly_subscribed(changes.newly_subscribed)
        except Exception:
            log.exception("block-sync: on_newly_subscribed callback failed")
    if changes is not None and changes.newly_blocked and on_newly_blocked is not None:
        try:
            await on_newly_blocked(changes.newly_blocked)
        except Exception:
            log.exception("block-sync: on_newly_blocked callback failed")
    if desired is not None and on_reconcile_subscriptions is not None:
        try:
            await on_reconcile_subscriptions(desired)
        except Exception:
            log.exception("block-sync: on_reconcile_subscriptions callback failed")


async def sync_blocked_channels_periodically(  # noqa: PLR0913 - process wiring needs explicit factories/knobs.
    make_http_client: Callable[[], httpx.Client],
    base_http_url: str,
    open_conn: Callable[[], TupleConnection],
    *,
    shared_secret: str | None = None,
    interval_s: float = DEFAULT_BLOCK_SYNC_INTERVAL_S,
    limiter: trio.CapacityLimiter | None = None,
    on_newly_subscribed: Callable[[frozenset[str]], Awaitable[None]] | None = None,
    on_newly_blocked: Callable[[frozenset[str]], Awaitable[None]] | None = None,
    on_reconcile_subscriptions: Callable[[frozenset[str]], Awaitable[None]] | None = None,
) -> None:
    """Long-running trio task for split-mode mount processes.

    ``on_newly_subscribed`` is invoked (in the trio event-loop task) with the
    set of channel_ids that transitioned blocked → subscribed in each cycle.
    Wired to ``WSClient.subscribe_channels`` so unblocking via
    ``_control/blocked_channels`` triggers WS subscribes without a mount
    restart. ``on_newly_blocked`` separately receives subscribed → blocked
    transitions so callers can invalidate visibility caches without changing
    the WSClient callback contract. After every successful cycle, including a
    no-op cycle, ``on_reconcile_subscriptions`` receives the complete desired
    channel set. That idempotent postcondition repairs an unblock whose COMMIT
    succeeded but whose acknowledgement was lost.
    """
    http_client = make_http_client()
    conn: TupleConnection | None = None
    try:
        while True:
            try:
                # Initial construction/connect belongs under this supervisor:
                # PG-down at task start must become a retryable cycle failure,
                # not an exception that tears down the process nursery.
                if conn is None:
                    conn = open_conn()
                cycle_conn = conn
                visibility_changes, desired_subscriptions = await trio.to_thread.run_sync(
                    _sync_cycle_with_desired_subscriptions,
                    http_client,
                    base_http_url,
                    cycle_conn,
                    shared_secret,
                    limiter=limiter,
                )
            except Exception:
                log.exception("block-sync: cycle failed")
                visibility_changes = None
                desired_subscriptions = None
            await _dispatch_cycle_callbacks(
                visibility_changes,
                desired_subscriptions,
                on_newly_subscribed,
                on_newly_blocked,
                on_reconcile_subscriptions,
            )
            await trio.sleep(interval_s)
    finally:
        http_client.close()
        if conn is not None:
            conn.close()


__all__ = [
    "DEFAULT_BLOCK_SYNC_INTERVAL_S",
    "VisibilityChanges",
    "apply_blocked_channel_sync",
    "desired_subscribed_channel_ids",
    "sync_blocked_channels_once",
    "sync_blocked_channels_periodically",
]
