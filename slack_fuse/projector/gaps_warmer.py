"""Background warmer for the gaps ghost-file caches.

The workspace gaps query takes ~2s server-side (single SQL scan, grouped
by stream+day across the whole workspace). The per-channel query is
faster but still hundreds of milliseconds for big channels. Either is
too slow to do synchronously inside a FUSE callback (the 1s per-callback
budget exists for the wedge-defence reasons documented in
``fuse_ops_v2._callback_guard``).

This module spawns a trio task that periodically fetches both endpoints
into the in-process cache on ``SlackFuseOpsV2``. The FUSE callbacks
themselves are pure cache lookups — they never block on HTTP. Cache
miss → file appears not to exist (ENOENT-like), retry after the next
warmer cycle returns content.

The warmer runs an initial warm at startup so the files become
available within seconds of mount-up, then refreshes every
``DEFAULT_INTERVAL_S``. A failed fetch logs a warning and the next cycle
tries again — the cache TTL (``600s`` on ``SlackFuseOpsV2``) covers
short transient failures.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING

import trio

if TYPE_CHECKING:
    from slack_fuse.fuse_ops_v2 import (
        ChannelGapsFetchFn,
        SlackFuseOpsV2,
        WorkspaceGapsFetchFn,
    )

log = logging.getLogger(__name__)

#: Refresh cadence. Cache TTL on ``SlackFuseOpsV2`` is 600s, so one missed
#: cycle still serves cached bytes; two missed cycles fall back to ENOENT.
DEFAULT_INTERVAL_S = 300.0


async def warm_gaps_periodically(
    ops: SlackFuseOpsV2,
    *,
    workspace_gaps_fetch: WorkspaceGapsFetchFn,
    channel_gaps_fetch: ChannelGapsFetchFn,
    list_channel_ids: Callable[[], Iterable[str]],
    interval_s: float = DEFAULT_INTERVAL_S,
) -> None:
    """Trio task that warms ``ops``'s gaps caches in a loop.

    ``list_channel_ids`` is called fresh every cycle so newly-added
    channels surface in the warmer's working set without restart.
    """
    while True:
        await _warm_once(
            ops,
            workspace_gaps_fetch=workspace_gaps_fetch,
            channel_gaps_fetch=channel_gaps_fetch,
            list_channel_ids=list_channel_ids,
        )
        await trio.sleep(interval_s)


async def _warm_once(
    ops: SlackFuseOpsV2,
    *,
    workspace_gaps_fetch: WorkspaceGapsFetchFn,
    channel_gaps_fetch: ChannelGapsFetchFn,
    list_channel_ids: Callable[[], Iterable[str]],
) -> None:
    """One full pass: workspace summary + every known channel.

    Workspace first so the broad summary populates within a few seconds
    of mount-up; per-channel iteration follows. A failure on any single
    target is isolated — we keep warming the rest.
    """
    try:
        body = await trio.to_thread.run_sync(workspace_gaps_fetch)
    except Exception as exc:  # noqa: BLE001 — warmer must outlive transient HTTP / server errors.
        log.warning("gaps warmer: workspace fetch failed: %s", type(exc).__name__)
    else:
        ops.put_workspace_gaps_cached(body)

    try:
        channel_ids = list(list_channel_ids())
    except Exception as exc:  # noqa: BLE001 — DB hiccup; skip this cycle's per-channel warm.
        log.warning("gaps warmer: list_channel_ids failed: %s", type(exc).__name__)
        return

    log.info("gaps warmer: refreshing %d channel(s)", len(channel_ids))
    warmed = 0
    failed = 0
    for channel_id in channel_ids:
        try:
            body = await trio.to_thread.run_sync(channel_gaps_fetch, channel_id)
        except Exception:  # noqa: BLE001 — single-channel failure is isolated.
            failed += 1
        else:
            ops.put_channel_gaps_cached(channel_id, body)
            warmed += 1
        # Yield between channels so we don't monopolise the event loop on
        # large workspaces (and so the cancellation point lands inside
        # the loop, not only between cycles).
        await trio.sleep(0)
    log.info("gaps warmer: warmed=%d failed=%d", warmed, failed)
