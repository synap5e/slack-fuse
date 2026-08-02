"""Background warmer for the workspace channel inventory cache."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import trio

if TYPE_CHECKING:
    from slack_fuse.fuse_ops_v2 import SlackFuseOpsV2, WorkspaceChannelsFetchFn

log = logging.getLogger(__name__)

DEFAULT_INTERVAL_S = 300.0


async def warm_channel_stats_periodically(
    ops: SlackFuseOpsV2,
    *,
    fetch: WorkspaceChannelsFetchFn,
    interval_s: float = DEFAULT_INTERVAL_S,
) -> None:
    """Warm immediately, then refresh the cached markdown every five minutes."""
    while True:
        await _warm_once(ops, fetch=fetch)
        await trio.sleep(interval_s)


async def _warm_once(ops: SlackFuseOpsV2, *, fetch: WorkspaceChannelsFetchFn) -> None:
    try:
        body = await trio.to_thread.run_sync(fetch)
    except Exception as exc:  # noqa: BLE001 - a transient server failure must not kill the mount.
        log.warning("channel-stats warmer: fetch failed: %s", type(exc).__name__)
        return
    ops.put_workspace_channels_cached(body)


__all__ = ["warm_channel_stats_periodically"]
