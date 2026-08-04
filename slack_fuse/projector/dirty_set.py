"""Thread-safe ordered scheduling set for the disk projection."""

from __future__ import annotations

import threading
from collections.abc import Hashable


class DirtySet[T: Hashable]:
    """Deduplicating FIFO set with bounded, linear-time drains.

    ``dict`` preserves insertion order and gives O(1) duplicate marks.  The
    lock makes individual ``mark``/``drain`` operations linearizable across
    the projector worker threads and the coalescer thread.
    """

    def __init__(self) -> None:
        self._items: dict[T, None] = {}
        self._lock = threading.Lock()

    def mark(self, item: T) -> None:
        """Mark ``item`` dirty, preserving its first position in the queue."""
        with self._lock:
            self._items.setdefault(item, None)

    def drain(self, limit: int) -> list[T]:
        """Remove and return at most ``limit`` items in mark order."""
        if limit < 0:
            msg = "dirty drain limit must be non-negative"
            raise ValueError(msg)
        drained: list[T] = []
        with self._lock:
            while self._items and len(drained) < limit:
                item = next(iter(self._items))
                del self._items[item]
                drained.append(item)
        return drained

    def is_marked(self, item: T) -> bool:
        """Return whether ``item`` is currently queued as dirty."""
        with self._lock:
            return item in self._items

    def discard(self, item: T) -> None:
        """Remove ``item`` from the queue when present."""
        with self._lock:
            self._items.pop(item, None)

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)
