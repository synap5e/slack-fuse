"""Diagnostic ``/debug/heap`` endpoint driven by ``tracemalloc``.

Purpose: identify residual memory growth after ``MALLOC_ARENA_MAX=2`` cut
glibc arena fragmentation but left a ~12 MB/h app-side climb (session log
2026-07-27). Enabled via env var so the ~10-30% tracemalloc overhead is
opt-in.

Auth: same shared-secret gate as ``/snapshot`` and the other mutating
routes. The heap traceback aggregates can leak object-level information
about what's in memory — do not expose unauthenticated.
"""

from __future__ import annotations

import tracemalloc
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class HeapEntry:
    """One aggregated allocation site."""

    traceback: tuple[str, ...]
    size_bytes: int
    count: int


@dataclass(frozen=True, slots=True)
class HeapSnapshotResponse:
    """Top-N heap allocation sites, aggregated by traceback."""

    tracemalloc_enabled: bool
    total_traced_bytes: int
    peak_traced_bytes: int
    entries: tuple[HeapEntry, ...]


@dataclass(frozen=True, slots=True)
class DebugHeapDeps:
    shared_secret: str | None


def capture_heap_snapshot(*, top_n: int = 40) -> HeapSnapshotResponse:
    """Take a live ``tracemalloc`` snapshot and return the top-N sites.

    Returns ``tracemalloc_enabled=False`` (empty entries) if tracemalloc
    was never started — caller can 503 that back to the client.
    """
    if not tracemalloc.is_tracing():
        return HeapSnapshotResponse(
            tracemalloc_enabled=False,
            total_traced_bytes=0,
            peak_traced_bytes=0,
            entries=(),
        )

    current_bytes, peak_bytes = tracemalloc.get_traced_memory()
    snapshot = tracemalloc.take_snapshot()
    top_stats = snapshot.statistics("traceback")[:top_n]
    entries = tuple(
        HeapEntry(
            traceback=tuple(stat.traceback.format()),
            size_bytes=stat.size,
            count=stat.count,
        )
        for stat in top_stats
    )
    return HeapSnapshotResponse(
        tracemalloc_enabled=True,
        total_traced_bytes=current_bytes,
        peak_traced_bytes=peak_bytes,
        entries=entries,
    )
