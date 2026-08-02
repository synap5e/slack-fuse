"""Cached-only FUSE semantics for ``/_workspace/channels.md``."""

from __future__ import annotations

from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import pytest
import trio

from slack_fuse.fuse_ops_v2 import SlackFuseOpsV2
from slack_fuse.fuse_v2_helpers import CHANNELS_MD, WORKSPACE_DIR
from slack_fuse.projector.channel_stats_warmer import _warm_once  # pyright: ignore[reportPrivateUsage]

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow


class _CountingFetcher:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self) -> bytes:
        self.calls += 1
        return b"# Workspace channels\n\n1 channel\n"


def _ops(conn: Connection[TupleRow], fetcher: _CountingFetcher) -> SlackFuseOpsV2:
    return SlackFuseOpsV2(
        conn=conn,
        local_tz=ZoneInfo("UTC"),
        limiter=trio.CapacityLimiter(1),
        workspace_channels_fetch=fetcher,
    )


def test_workspace_channels_file_is_discoverable_and_cache_only(
    client_conn: Connection[TupleRow],
) -> None:
    fetcher = _CountingFetcher()
    ops = _ops(client_conn, fetcher)
    assert (WORKSPACE_DIR, True) in ops.list_dir_for_test("/")
    assert ops.list_dir_for_test(f"/{WORKSPACE_DIR}") == [(CHANNELS_MD, False)]
    path = f"/{WORKSPACE_DIR}/{CHANNELS_MD}"

    assert ops.resolve_content_for_test(path) is None
    assert fetcher.calls == 0

    ops.put_workspace_channels_cached(b"# Workspace channels\n\nCached body\n")
    resolved = ops.resolve_content_for_test(path)
    assert resolved is not None
    assert b"Cached body" in resolved[0]
    assert fetcher.calls == 0
    assert ops.is_dir_for_test(path) is False


@pytest.mark.trio
async def test_channel_stats_warmer_populates_cache_off_callback(
    client_conn: Connection[TupleRow],
) -> None:
    fetcher = _CountingFetcher()
    ops = _ops(client_conn, fetcher)

    await _warm_once(ops, fetch=fetcher)

    assert fetcher.calls == 1
    resolved = ops.resolve_content_for_test(f"/{WORKSPACE_DIR}/{CHANNELS_MD}")
    assert resolved is not None
    assert b"1 channel" in resolved[0]
