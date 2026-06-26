"""``channel.original.md`` ghost-file semantics.

The ghost-file pattern: ``readdir`` MUST NOT list ``channel.original.md`` (so
recursive scans like ``ls -la`` / ``find`` / ``rg`` never trip the slow
events-replay path), but ``lookup`` MUST resolve it (so a direct
``bat channel.original.md`` works).

The in-process TTL cache exists to keep ``stat`` + ``read`` on the same file
from each calling the fetcher: getattr renders to compute size, read renders
to return bytes. Without the cache that's two server replays per ``cat``.

Tests use the ``originals_fetch`` injection point — production wires a sync
httpx call, here we wire a counting in-memory stub so we can assert call
counts directly.
"""

from __future__ import annotations

import threading
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import pytest
import trio

from slack_fuse.fuse_ops_v2 import (
    OriginalsFetchFn,
    SlackFuseOpsV2,
    _OriginalsCache,  # pyright: ignore[reportPrivateUsage]
)
from slack_fuse.fuse_v2_helpers import CHANNEL_MD, CHANNEL_ORIGINAL_MD
from tests.fuse_v2.conftest import (
    mark_stream_caught_up,
    seed_channel,
    seed_chunk,
    set_connection_state,
)

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow


# A counting fetcher stub. Returns canned bytes; records every call so the
# cache test can assert "called once even when stat+read both happen".
class _CountingFetcher:
    def __init__(self, response: bytes = b"## 12:00 @U1\n\nseeded original\n") -> None:
        self._response = response
        self._calls: list[tuple[str, float, float]] = []
        self._lock = threading.Lock()

    def __call__(self, channel_id: str, from_epoch: float, to_epoch: float) -> bytes:
        with self._lock:
            self._calls.append((channel_id, from_epoch, to_epoch))
        return self._response

    @property
    def calls(self) -> list[tuple[str, float, float]]:
        with self._lock:
            return list(self._calls)


def _ts(dt: datetime) -> Decimal:
    return Decimal(str(dt.timestamp()))


def _seed_day(conn: Connection[TupleRow]) -> None:
    seed_channel(conn, "C1", "general", tier="hot")
    seed_chunk(
        conn,
        "C1",
        _ts(datetime(2026, 6, 8, 12, 0, tzinfo=UTC)),
        "## 12:00 <@U1>\n\nCurrent (post-edit) content\n",
    )
    set_connection_state(conn, last_slurper_health="healthy", last_frame_at_offset_s=1.0)
    mark_stream_caught_up(conn, "channel:C1", at_offset=10)


def _make_ops(
    conn: Connection[TupleRow],
    *,
    originals_fetch: OriginalsFetchFn | None,
) -> SlackFuseOpsV2:
    return SlackFuseOpsV2(
        conn=conn,
        local_tz=ZoneInfo("UTC"),
        limiter=trio.CapacityLimiter(1),
        originals_fetch=originals_fetch,
    )


_DAY_PATH = "/channels/general/2026-06/08"
_GHOST_PATH = f"{_DAY_PATH}/{CHANNEL_ORIGINAL_MD}"


# ============================================================================
# Listing semantics: readdir omits, lookup resolves
# ============================================================================


def test_readdir_omits_ghost_file_even_with_fetcher_wired(
    client_conn: Connection[TupleRow],
) -> None:
    _seed_day(client_conn)
    ops = _make_ops(client_conn, originals_fetch=_CountingFetcher())
    entries = ops.list_dir_for_test(_DAY_PATH)
    names = [name for name, _is_dir in entries]
    assert CHANNEL_MD in names
    assert CHANNEL_ORIGINAL_MD not in names


def test_lookup_path_includes_ghost_file_when_fetcher_wired(
    client_conn: Connection[TupleRow],
) -> None:
    _seed_day(client_conn)
    ops = _make_ops(client_conn, originals_fetch=_CountingFetcher())
    # for_lookup=True is the codepath ``lookup`` uses; ``readdir`` uses default.
    lookup_entries = ops._list_dir(_DAY_PATH, for_lookup=True)  # pyright: ignore[reportPrivateUsage]
    names = [name for name, _is_dir in lookup_entries]
    assert CHANNEL_ORIGINAL_MD in names


def test_lookup_path_omits_ghost_file_when_no_fetcher(
    client_conn: Connection[TupleRow],
) -> None:
    """Without a fetcher the file can't be rendered — it must not appear
    even on the lookup path, otherwise a ``cat`` would hit an unrenderable
    inode and we'd have to fabricate an EIO ourselves.
    """
    _seed_day(client_conn)
    ops = _make_ops(client_conn, originals_fetch=None)
    lookup_entries = ops._list_dir(_DAY_PATH, for_lookup=True)  # pyright: ignore[reportPrivateUsage]
    names = [name for name, _is_dir in lookup_entries]
    assert CHANNEL_ORIGINAL_MD not in names


# ============================================================================
# Read path: returns fetcher bytes + resolves mentions
# ============================================================================


def test_resolve_content_returns_fetcher_body_for_ghost_path(
    client_conn: Connection[TupleRow],
) -> None:
    _seed_day(client_conn)
    fetcher = _CountingFetcher(b"## 12:00 <@U1>\n\nThe original draft text\n")
    ops = _make_ops(client_conn, originals_fetch=fetcher)
    resolved = ops.resolve_content_for_test(_GHOST_PATH)
    assert resolved is not None
    content, _trailer, _fallback = resolved
    assert b"The original draft text" in content
    # The originals marker is in the frontmatter prefix.
    assert b"originals view" in content


def test_resolve_content_returns_none_for_empty_originals(
    client_conn: Connection[TupleRow],
) -> None:
    """An empty body from the server (no message events in the day) means
    the ghost file behaves like a missing file — a direct ``cat`` should
    surface ENOENT, not zero bytes."""
    _seed_day(client_conn)
    fetcher = _CountingFetcher(b"")
    ops = _make_ops(client_conn, originals_fetch=fetcher)
    resolved = ops.resolve_content_for_test(_GHOST_PATH)
    assert resolved is None


def test_resolve_content_returns_none_when_fetcher_not_wired(
    client_conn: Connection[TupleRow],
) -> None:
    _seed_day(client_conn)
    ops = _make_ops(client_conn, originals_fetch=None)
    resolved = ops.resolve_content_for_test(_GHOST_PATH)
    assert resolved is None


# ============================================================================
# In-process TTL cache — stat+read share one fetch
# ============================================================================


def test_cache_dedupes_repeated_resolves_within_ttl(
    client_conn: Connection[TupleRow],
) -> None:
    """The canonical stat-then-read flow: getattr renders to compute size,
    read renders to return bytes. The cache must produce one fetcher call,
    not two."""
    _seed_day(client_conn)
    fetcher = _CountingFetcher()
    ops = _make_ops(client_conn, originals_fetch=fetcher)
    # Simulate getattr -> read on the same file.
    _ = ops.resolve_content_for_test(_GHOST_PATH)
    _ = ops.resolve_content_for_test(_GHOST_PATH)
    assert len(fetcher.calls) == 1


def test_cache_passes_correct_day_range(
    client_conn: Connection[TupleRow],
) -> None:
    """The cache key is (channel, date); the fetcher is called with the
    matching UTC-epoch range. Off-by-one would either silently truncate
    today's messages or pull in yesterday's."""
    _seed_day(client_conn)
    fetcher = _CountingFetcher()
    ops = _make_ops(client_conn, originals_fetch=fetcher)
    _ = ops.resolve_content_for_test(_GHOST_PATH)
    assert len(fetcher.calls) == 1
    channel, from_epoch, to_epoch = fetcher.calls[0]
    assert channel == "C1"
    # ZoneInfo("UTC") + day 2026-06-08 → midnight to midnight UTC.
    expected_start = datetime(2026, 6, 8, tzinfo=UTC).timestamp()
    expected_end = datetime(2026, 6, 9, tzinfo=UTC).timestamp()
    assert abs(from_epoch - expected_start) < 0.01
    assert abs(to_epoch - expected_end) < 0.01


def test_cache_keys_distinguish_channels_and_days(
    client_conn: Connection[TupleRow],
) -> None:
    """Two different (channel, day) lookups must not share a cache entry."""
    _seed_day(client_conn)
    seed_channel(client_conn, "C2", "alpha", tier="hot")
    seed_chunk(
        client_conn,
        "C2",
        _ts(datetime(2026, 6, 8, 12, 0, tzinfo=UTC)),
        "## 12:00 <@U2>\n\nAlpha content\n",
    )
    seed_chunk(
        client_conn,
        "C1",
        _ts(datetime(2026, 6, 9, 12, 0, tzinfo=UTC)),
        "## 12:00 <@U1>\n\nNext day\n",
    )
    fetcher = _CountingFetcher()
    ops = _make_ops(client_conn, originals_fetch=fetcher)

    _ = ops.resolve_content_for_test(_GHOST_PATH)  # C1, 2026-06-08
    _ = ops.resolve_content_for_test(f"/channels/alpha/2026-06/08/{CHANNEL_ORIGINAL_MD}")  # C2
    _ = ops.resolve_content_for_test(f"/channels/general/2026-06/09/{CHANNEL_ORIGINAL_MD}")  # C1, day+1
    # Three distinct keys → three fetcher calls.
    assert len(fetcher.calls) == 3


def test_cache_eviction_when_max_entries_exceeded(
    client_conn: Connection[TupleRow],
) -> None:
    """Tight FIFO cap so a perverse recursive scan can't grow memory
    unbounded. Verify by overrunning the bound with a tiny cache."""
    cache = _OriginalsCache(max_entries=2, ttl_s=60.0)
    cache.put("C1", "2026-06-01", content=b"a")
    cache.put("C1", "2026-06-02", content=b"b")
    cache.put("C1", "2026-06-03", content=b"c")  # evicts 2026-06-01
    assert cache.get("C1", "2026-06-01") is None
    assert cache.get("C1", "2026-06-02") == b"b"
    assert cache.get("C1", "2026-06-03") == b"c"


def test_cache_ttl_expiry(
    client_conn: Connection[TupleRow],
) -> None:
    """A short TTL forces the cache to refetch after expiry."""

    cache = _OriginalsCache(max_entries=8, ttl_s=0.05)
    cache.put("C1", "2026-06-01", content=b"snapshot")
    assert cache.get("C1", "2026-06-01") == b"snapshot"
    time.sleep(0.1)
    assert cache.get("C1", "2026-06-01") is None


# ============================================================================
# Direct lookup behaves like a normal file
# ============================================================================


def test_ghost_file_is_classified_as_file_not_dir(
    client_conn: Connection[TupleRow],
) -> None:
    _seed_day(client_conn)
    ops = _make_ops(client_conn, originals_fetch=_CountingFetcher())
    assert ops.is_dir_for_test(_GHOST_PATH) is False


@pytest.mark.parametrize("name", [CHANNEL_ORIGINAL_MD])
def test_ghost_file_resolves_independent_of_chunks_state(
    client_conn: Connection[TupleRow],
    name: str,
) -> None:
    """The originals view is sourced from the server's events table, not the
    client's chunks. A channel with NO chunks but events on the server side
    must still render via the fetcher path."""
    seed_channel(client_conn, "C1", "general", tier="hot")
    set_connection_state(client_conn, last_slurper_health="healthy", last_frame_at_offset_s=1.0)
    mark_stream_caught_up(client_conn, "channel:C1", at_offset=10)
    fetcher = _CountingFetcher(b"## 09:00 <@U1>\n\nServer-side only content\n")
    ops = _make_ops(client_conn, originals_fetch=fetcher)
    resolved = ops.resolve_content_for_test(f"/channels/general/2026-06/08/{name}")
    assert resolved is not None
    content, _trailer, _fallback = resolved
    assert b"Server-side only content" in content
