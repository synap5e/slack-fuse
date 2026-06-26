"""``gaps_warmer`` background-task semantics.

The warmer is the only path that populates the gaps caches; FUSE
callbacks just read. These tests pin the cycle behaviour: one workspace
fetch + one fetch per known channel + isolation (one channel failing
doesn't poison the rest) + outliving exceptions.
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import pytest
import trio

from slack_fuse.fuse_ops_v2 import SlackFuseOpsV2
from slack_fuse.fuse_v2_helpers import GAPS_MD, WORKSPACE_DIR
from slack_fuse.projector.gaps_warmer import _warm_once  # pyright: ignore[reportPrivateUsage]

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow


class _FlexibleWorkspaceFetcher:
    """Workspace fetcher with configurable return / raise."""

    def __init__(self, body: bytes | None = b"# Workspace gaps\n\nNo gaps detected.\n") -> None:
        self._body = body
        self.calls = 0
        self._lock = threading.Lock()

    def __call__(self) -> bytes:
        with self._lock:
            self.calls += 1
        if self._body is None:
            msg = "boom"
            raise RuntimeError(msg)
        return self._body


class _FlexibleChannelFetcher:
    """Channel fetcher that can selectively raise per channel."""

    def __init__(self, *, raise_for: set[str] | None = None) -> None:
        self.raise_for = raise_for or set()
        self.calls: list[str] = []
        self._lock = threading.Lock()

    def __call__(self, channel_id: str) -> bytes:
        with self._lock:
            self.calls.append(channel_id)
        if channel_id in self.raise_for:
            msg = f"boom for {channel_id}"
            raise RuntimeError(msg)
        return f"# Gaps for {channel_id}\n\nplaceholder\n".encode()


def _make_ops_with_fetchers(
    conn: Connection[TupleRow],
    *,
    workspace_fetch: _FlexibleWorkspaceFetcher,
    channel_fetch: _FlexibleChannelFetcher,
) -> SlackFuseOpsV2:
    return SlackFuseOpsV2(
        conn=conn,
        local_tz=ZoneInfo("UTC"),
        limiter=trio.CapacityLimiter(1),
        channel_gaps_fetch=channel_fetch,
        workspace_gaps_fetch=workspace_fetch,
    )


@pytest.mark.trio
async def test_warm_once_populates_workspace_and_each_channel(
    client_conn: Connection[TupleRow],
) -> None:
    workspace_fetch = _FlexibleWorkspaceFetcher(b"# Workspace gaps\n\n2 channels\n")
    channel_fetch = _FlexibleChannelFetcher()
    ops = _make_ops_with_fetchers(client_conn, workspace_fetch=workspace_fetch, channel_fetch=channel_fetch)

    await _warm_once(
        ops,
        workspace_gaps_fetch=workspace_fetch,
        channel_gaps_fetch=channel_fetch,
        list_channel_ids=lambda: ["C1", "C2", "C3"],
    )

    # Workspace cache populated.
    workspace_path = f"/{WORKSPACE_DIR}/{GAPS_MD}"
    resolved_ws = ops.resolve_content_for_test(workspace_path)
    assert resolved_ws is not None
    assert b"2 channels" in resolved_ws[0]

    # Every channel was fetched exactly once.
    assert workspace_fetch.calls == 1
    assert sorted(channel_fetch.calls) == ["C1", "C2", "C3"]


@pytest.mark.trio
async def test_warm_once_workspace_failure_does_not_stop_per_channel_pass(
    client_conn: Connection[TupleRow],
) -> None:
    """A 5xx on the workspace fetch must NOT skip per-channel warming.
    Mirrors the real-world case where the heavy workspace query times out
    but the lighter per-channel queries are fine."""
    workspace_fetch = _FlexibleWorkspaceFetcher(body=None)  # raises
    channel_fetch = _FlexibleChannelFetcher()
    ops = _make_ops_with_fetchers(client_conn, workspace_fetch=workspace_fetch, channel_fetch=channel_fetch)

    await _warm_once(
        ops,
        workspace_gaps_fetch=workspace_fetch,
        channel_gaps_fetch=channel_fetch,
        list_channel_ids=lambda: ["C1", "C2"],
    )

    assert workspace_fetch.calls == 1
    # Channels were warmed despite the workspace failure.
    assert sorted(channel_fetch.calls) == ["C1", "C2"]


@pytest.mark.trio
async def test_warm_once_per_channel_failure_isolated(
    client_conn: Connection[TupleRow],
) -> None:
    """A single channel raising doesn't poison the rest of the cycle —
    the warmer keeps going. Future cycles will retry the failed one."""
    workspace_fetch = _FlexibleWorkspaceFetcher()
    channel_fetch = _FlexibleChannelFetcher(raise_for={"C_BAD"})
    ops = _make_ops_with_fetchers(client_conn, workspace_fetch=workspace_fetch, channel_fetch=channel_fetch)

    await _warm_once(
        ops,
        workspace_gaps_fetch=workspace_fetch,
        channel_gaps_fetch=channel_fetch,
        list_channel_ids=lambda: ["C_GOOD_1", "C_BAD", "C_GOOD_2"],
    )

    # All three were attempted.
    assert sorted(channel_fetch.calls) == ["C_BAD", "C_GOOD_1", "C_GOOD_2"]

    # Good channels are in the cache; the bad one is not.
    # Synthetic IDs don't have channel rows so resolve_content_for_test
    # would 404 on path-classify — inspect the cache directly instead.
    assert ops._channel_gaps_cache is not None  # pyright: ignore[reportPrivateUsage]
    assert ops._channel_gaps_cache.get("C_GOOD_1") == b"# Gaps for C_GOOD_1\n\nplaceholder\n"  # pyright: ignore[reportPrivateUsage]
    assert ops._channel_gaps_cache.get("C_GOOD_2") == b"# Gaps for C_GOOD_2\n\nplaceholder\n"  # pyright: ignore[reportPrivateUsage]
    assert ops._channel_gaps_cache.get("C_BAD") is None  # pyright: ignore[reportPrivateUsage]


@pytest.mark.trio
async def test_warm_once_with_no_channels_still_warms_workspace(
    client_conn: Connection[TupleRow],
) -> None:
    workspace_fetch = _FlexibleWorkspaceFetcher(b"# Workspace gaps\n\nNo gaps detected.\n")
    channel_fetch = _FlexibleChannelFetcher()
    ops = _make_ops_with_fetchers(client_conn, workspace_fetch=workspace_fetch, channel_fetch=channel_fetch)

    await _warm_once(
        ops,
        workspace_gaps_fetch=workspace_fetch,
        channel_gaps_fetch=channel_fetch,
        list_channel_ids=lambda: [],
    )

    assert workspace_fetch.calls == 1
    assert channel_fetch.calls == []


@pytest.mark.trio
async def test_warm_once_with_listed_id_failure_skips_cycle_safely(
    client_conn: Connection[TupleRow],
) -> None:
    """If ``list_channel_ids`` itself raises (DB hiccup), the cycle bails
    out gracefully. Workspace was already warmed before the listing call,
    so that part isn't lost."""
    workspace_fetch = _FlexibleWorkspaceFetcher(b"# Workspace gaps\n\nNo gaps detected.\n")
    channel_fetch = _FlexibleChannelFetcher()
    ops = _make_ops_with_fetchers(client_conn, workspace_fetch=workspace_fetch, channel_fetch=channel_fetch)

    def _angry_lister() -> list[str]:
        msg = "PG hiccup"
        raise RuntimeError(msg)

    # Must NOT raise out of the warmer.
    await _warm_once(
        ops,
        workspace_gaps_fetch=workspace_fetch,
        channel_gaps_fetch=channel_fetch,
        list_channel_ids=_angry_lister,
    )

    assert workspace_fetch.calls == 1
    assert channel_fetch.calls == []
