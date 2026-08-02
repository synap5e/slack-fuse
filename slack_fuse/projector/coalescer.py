"""Trio lifecycle task for periodically flushing the disk projection."""

from __future__ import annotations

import functools
import logging
from typing import Protocol

import trio
from psycopg import Connection
from psycopg.rows import TupleRow

from slack_fuse.projector.disk_projection import DiskProjection
from slack_fuse.projector.reconnecting_conn import ReconnectingConnection

log = logging.getLogger(__name__)


class ProjectionPathInvalidator(Protocol):
    """Kernel-cache invalidator addressed by FUSE path."""

    def path_changed(self, path: str) -> None: ...


async def run_coalescer(
    projection: DiskProjection,
    conn: Connection[TupleRow] | ReconnectingConnection,
    invalidator: ProjectionPathInvalidator,
    *,
    tick_s: float = 5.0,
    initial_flush_batch: int = 200,
) -> None:
    """Bootstrap and flush dirty paths in bounded batches every ``tick_s``."""
    if tick_s < 0:
        msg = "coalescer tick_s must be non-negative"
        raise ValueError(msg)
    if initial_flush_batch <= 0:
        msg = "coalescer initial_flush_batch must be positive"
        raise ValueError(msg)

    bootstrapped = False
    while True:
        try:
            if not bootstrapped:
                await trio.to_thread.run_sync(functools.partial(projection.bootstrap, conn))
                bootstrapped = True
            flushed = await trio.to_thread.run_sync(projection.flush_dirty, initial_flush_batch)
            if flushed:
                await trio.to_thread.run_sync(_invalidate_paths, invalidator, flushed)
        except Exception:
            log.warning("disk projection coalescer tick failed; retrying", exc_info=True)
        await trio.sleep(tick_s)


def _invalidate_paths(invalidator: ProjectionPathInvalidator, paths: list[str]) -> None:
    for path in paths:
        invalidator.path_changed(path)
