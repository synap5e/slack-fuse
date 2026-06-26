"""``gaps.md`` ghost-file + ``/_workspace/gaps.md`` semantics.

Same ghost-file pattern as ``channel.original.md``:

- ``readdir`` for a channel root does NOT list ``gaps.md`` — recursive
  ``rg`` / ``ls -la`` never trips the slow events-aggregation path.
- ``lookup`` for the path RESOLVES — direct ``cat`` works.
- ``/_workspace/`` IS listed at the root (it's a discoverable namespace),
  and its contents (``gaps.md`` for now) ARE listed (you navigated there
  deliberately).

Tests use injectable fetchers + the shared cache primitive so we can
assert behaviour without booting an HTTP server.
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import pytest
import trio

from slack_fuse.fuse_ops_v2 import (
    ChannelGapsFetchFn,
    SlackFuseOpsV2,
    WorkspaceGapsFetchFn,
)
from slack_fuse.fuse_v2_helpers import (
    CHANNEL_MD,
    CHANNEL_ORIGINAL_MD,
    CONV_ROOTS,
    GAPS_MD,
    WORKSPACE_DIR,
)
from tests.fuse_v2.conftest import (
    mark_stream_caught_up,
    seed_channel,
    set_connection_state,
)

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow


class _CountingChannelGapsFetcher:
    def __init__(self, response: bytes = b"# Gaps for general\n\n- 2026-06-05 (1 day)\n") -> None:
        self._response = response
        self._calls: list[str] = []
        self._lock = threading.Lock()

    def __call__(self, channel_id: str) -> bytes:
        with self._lock:
            self._calls.append(channel_id)
        return self._response

    @property
    def calls(self) -> list[str]:
        with self._lock:
            return list(self._calls)


class _CountingWorkspaceGapsFetcher:
    def __init__(self, response: bytes = b"# Workspace gaps\n\nNo gaps detected.\n") -> None:
        self._response = response
        self._calls = 0
        self._lock = threading.Lock()

    def __call__(self) -> bytes:
        with self._lock:
            self._calls += 1
        return self._response

    @property
    def calls(self) -> int:
        with self._lock:
            return self._calls


def _seed_channel_with_data(conn: Connection[TupleRow]) -> None:
    seed_channel(conn, "C1", "general", tier="hot")
    set_connection_state(conn, last_slurper_health="healthy", last_frame_at_offset_s=1.0)
    mark_stream_caught_up(conn, "channel:C1", at_offset=10)


def _make_ops(
    conn: Connection[TupleRow],
    *,
    channel_gaps_fetch: ChannelGapsFetchFn | None = None,
    workspace_gaps_fetch: WorkspaceGapsFetchFn | None = None,
) -> SlackFuseOpsV2:
    return SlackFuseOpsV2(
        conn=conn,
        local_tz=ZoneInfo("UTC"),
        limiter=trio.CapacityLimiter(1),
        channel_gaps_fetch=channel_gaps_fetch,
        workspace_gaps_fetch=workspace_gaps_fetch,
    )


_CHANNEL_ROOT = "/channels/general"
_CHANNEL_GAPS_PATH = f"{_CHANNEL_ROOT}/{GAPS_MD}"
_WORKSPACE_PATH = f"/{WORKSPACE_DIR}"
_WORKSPACE_GAPS_PATH = f"/{WORKSPACE_DIR}/{GAPS_MD}"


# ============================================================================
# Per-channel gaps.md: ghost (lookup only, not readdir)
# ============================================================================


def test_channel_root_readdir_omits_gaps_md(
    client_conn: Connection[TupleRow],
) -> None:
    _seed_channel_with_data(client_conn)
    ops = _make_ops(client_conn, channel_gaps_fetch=_CountingChannelGapsFetcher())
    entries = ops.list_dir_for_test(_CHANNEL_ROOT)
    names = [name for name, _is_dir in entries]
    assert CHANNEL_MD in names
    assert GAPS_MD not in names


def test_channel_root_lookup_includes_gaps_md_when_fetcher_wired(
    client_conn: Connection[TupleRow],
) -> None:
    _seed_channel_with_data(client_conn)
    ops = _make_ops(client_conn, channel_gaps_fetch=_CountingChannelGapsFetcher())
    lookup_entries = ops._list_dir(_CHANNEL_ROOT, for_lookup=True)  # pyright: ignore[reportPrivateUsage]
    names = [name for name, _is_dir in lookup_entries]
    assert GAPS_MD in names


def test_channel_root_lookup_omits_gaps_md_when_no_fetcher(
    client_conn: Connection[TupleRow],
) -> None:
    _seed_channel_with_data(client_conn)
    ops = _make_ops(client_conn, channel_gaps_fetch=None)
    lookup_entries = ops._list_dir(_CHANNEL_ROOT, for_lookup=True)  # pyright: ignore[reportPrivateUsage]
    names = [name for name, _is_dir in lookup_entries]
    assert GAPS_MD not in names


def test_channel_gaps_resolve_returns_warmed_cache_body(
    client_conn: Connection[TupleRow],
) -> None:
    """The FUSE callback never fetches — it serves the in-process cache
    populated by the background warmer. Pre-warm via the dedicated
    ``put_channel_gaps_cached`` mutator (the same entry point the warmer
    uses) and assert the next resolve returns the cached bytes."""
    _seed_channel_with_data(client_conn)
    fetcher = _CountingChannelGapsFetcher(b"# Gaps for general\n\nFour day hole here\n")
    ops = _make_ops(client_conn, channel_gaps_fetch=fetcher)
    ops.put_channel_gaps_cached("C1", b"# Gaps for general\n\nFour day hole here\n")
    resolved = ops.resolve_content_for_test(_CHANNEL_GAPS_PATH)
    assert resolved is not None
    content, _trailer, _fallback = resolved
    assert b"Four day hole here" in content
    # CRITICAL: the FUSE callback path MUST NOT have called the fetcher.
    # Any synchronous fetch inside a callback re-introduces the 1s budget
    # blow-up we shipped this background-warmer architecture to avoid.
    assert fetcher.calls == []


def test_channel_gaps_empty_body_resolves_to_none(
    client_conn: Connection[TupleRow],
) -> None:
    """Empty body from the server = ENOENT-like behaviour, not an empty file."""
    _seed_channel_with_data(client_conn)
    fetcher = _CountingChannelGapsFetcher(b"")
    ops = _make_ops(client_conn, channel_gaps_fetch=fetcher)
    assert ops.resolve_content_for_test(_CHANNEL_GAPS_PATH) is None


def test_channel_gaps_classified_as_file_not_dir(
    client_conn: Connection[TupleRow],
) -> None:
    _seed_channel_with_data(client_conn)
    ops = _make_ops(client_conn, channel_gaps_fetch=_CountingChannelGapsFetcher())
    assert ops.is_dir_for_test(_CHANNEL_GAPS_PATH) is False


# ============================================================================
# Per-channel cache: stat+read share one fetch, distinct channels don't
# ============================================================================


def test_channel_gaps_resolve_returns_none_without_warmed_cache(
    client_conn: Connection[TupleRow],
) -> None:
    """Cold cache → ENOENT-like ``None``. The FUSE side surfaces this as
    "file does not exist yet" to userspace; userspace retries after the
    next warmer cycle and sees content."""
    _seed_channel_with_data(client_conn)
    fetcher = _CountingChannelGapsFetcher()
    ops = _make_ops(client_conn, channel_gaps_fetch=fetcher)
    assert ops.resolve_content_for_test(_CHANNEL_GAPS_PATH) is None
    # No fetch from inside the callback path — the warmer is the only path.
    assert fetcher.calls == []


def test_channel_gaps_cache_isolates_per_channel(
    client_conn: Connection[TupleRow],
) -> None:
    """Two channels warmed separately don't share cache entries."""
    _seed_channel_with_data(client_conn)
    seed_channel(client_conn, "C2", "alpha", tier="hot")
    fetcher = _CountingChannelGapsFetcher()
    ops = _make_ops(client_conn, channel_gaps_fetch=fetcher)
    ops.put_channel_gaps_cached("C1", b"# Gaps for general\n\nA hole\n")
    ops.put_channel_gaps_cached("C2", b"# Gaps for alpha\n\nDifferent hole\n")

    resolved_c1 = ops.resolve_content_for_test(_CHANNEL_GAPS_PATH)
    resolved_c2 = ops.resolve_content_for_test(f"/channels/alpha/{GAPS_MD}")
    assert resolved_c1 is not None and b"general" in resolved_c1[0]
    assert resolved_c2 is not None and b"alpha" in resolved_c2[0]
    # Asserts content didn't cross-contaminate.
    assert b"alpha" not in resolved_c1[0]
    assert b"general" not in resolved_c2[0]


# ============================================================================
# /_workspace/ namespace + /_workspace/gaps.md
# ============================================================================


def test_workspace_dir_listed_at_root_when_fetcher_wired(
    client_conn: Connection[TupleRow],
) -> None:
    ops = _make_ops(client_conn, workspace_gaps_fetch=_CountingWorkspaceGapsFetcher())
    entries = ops.list_dir_for_test("/")
    names = [name for name, _is_dir in entries]
    # All conv-roots still listed.
    for root in CONV_ROOTS:
        assert root in names
    # And the workspace namespace.
    assert WORKSPACE_DIR in names


def test_workspace_dir_omitted_at_root_when_no_fetcher(
    client_conn: Connection[TupleRow],
) -> None:
    """No fetcher = no namespace surface. Avoids dangling-link UX where the
    dir exists but nothing inside can render."""
    ops = _make_ops(client_conn, workspace_gaps_fetch=None)
    entries = ops.list_dir_for_test("/")
    names = [name for name, _is_dir in entries]
    assert WORKSPACE_DIR not in names


def test_workspace_dir_is_classified_as_dir(
    client_conn: Connection[TupleRow],
) -> None:
    ops = _make_ops(client_conn, workspace_gaps_fetch=_CountingWorkspaceGapsFetcher())
    assert ops.is_dir_for_test(_WORKSPACE_PATH) is True


def test_workspace_dir_lists_gaps_md(
    client_conn: Connection[TupleRow],
) -> None:
    """``/_workspace/`` lists ``gaps.md`` even on the readdir path — you
    navigated into the namespace explicitly, the slow-path concern doesn't
    apply here."""
    ops = _make_ops(client_conn, workspace_gaps_fetch=_CountingWorkspaceGapsFetcher())
    entries = ops.list_dir_for_test(_WORKSPACE_PATH)
    names = [name for name, _is_dir in entries]
    assert names == [GAPS_MD]


def test_workspace_gaps_resolve_returns_warmed_cache_body(
    client_conn: Connection[TupleRow],
) -> None:
    """Pre-warm via ``put_workspace_gaps_cached``; the FUSE callback never
    fetches synchronously, just reads the cache."""
    fetcher = _CountingWorkspaceGapsFetcher(b"# Workspace gaps\n\n3 channel(s) with gaps.\n")
    ops = _make_ops(client_conn, workspace_gaps_fetch=fetcher)
    ops.put_workspace_gaps_cached(b"# Workspace gaps\n\n3 channel(s) with gaps.\n")
    resolved = ops.resolve_content_for_test(_WORKSPACE_GAPS_PATH)
    assert resolved is not None
    content, _trailer, _fallback = resolved
    assert b"3 channel(s) with gaps" in content
    # No synchronous fetch from inside the callback path.
    assert fetcher.calls == 0


def test_workspace_gaps_resolve_returns_none_without_warmed_cache(
    client_conn: Connection[TupleRow],
) -> None:
    fetcher = _CountingWorkspaceGapsFetcher()
    ops = _make_ops(client_conn, workspace_gaps_fetch=fetcher)
    assert ops.resolve_content_for_test(_WORKSPACE_GAPS_PATH) is None
    assert fetcher.calls == 0


def test_workspace_gaps_resolves_to_none_when_no_fetcher(
    client_conn: Connection[TupleRow],
) -> None:
    ops = _make_ops(client_conn, workspace_gaps_fetch=None)
    assert ops.resolve_content_for_test(_WORKSPACE_GAPS_PATH) is None


# ============================================================================
# Layout sanity: the two ghost-file patterns don't collide
# ============================================================================


@pytest.mark.parametrize(
    "ghost_name",
    [CHANNEL_ORIGINAL_MD, GAPS_MD],
)
def test_channel_root_readdir_never_lists_either_ghost(
    client_conn: Connection[TupleRow],
    ghost_name: str,
) -> None:
    """Per-channel ghost files (channel.original.md at day-level, gaps.md
    at channel-root) both stay off the readdir path. Recursive scans must
    never trip the slow path."""
    _seed_channel_with_data(client_conn)
    ops = _make_ops(
        client_conn,
        channel_gaps_fetch=_CountingChannelGapsFetcher(),
        workspace_gaps_fetch=_CountingWorkspaceGapsFetcher(),
    )
    # Walk every readdir-visible depth and assert no ghost appears.
    for path in ["/", "/channels", _CHANNEL_ROOT]:
        names = [name for name, _is_dir in ops.list_dir_for_test(path)]
        assert ghost_name not in names, f"{ghost_name} leaked into readdir at {path}"
