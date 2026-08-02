"""Thread-safe ordered dirty-path set for the disk projection."""

from __future__ import annotations

import threading


class DirtySet:
    """Deduplicating FIFO set with bounded, linear-time drains.

    ``dict`` preserves insertion order and gives O(1) duplicate marks.  The
    lock makes individual ``mark``/``drain`` operations linearizable across
    the projector worker threads and the coalescer thread.
    """

    def __init__(self) -> None:
        self._paths: dict[str, None] = {}
        self._lock = threading.Lock()

    def mark(self, path: str) -> None:
        """Mark ``path`` dirty, preserving its first position in the queue."""
        with self._lock:
            self._paths.setdefault(path, None)

    def drain(self, limit: int) -> list[str]:
        """Remove and return at most ``limit`` paths in mark order."""
        if limit < 0:
            msg = "dirty drain limit must be non-negative"
            raise ValueError(msg)
        drained: list[str] = []
        with self._lock:
            while self._paths and len(drained) < limit:
                path = next(iter(self._paths))
                del self._paths[path]
                drained.append(path)
        return drained

    def is_marked(self, path: str) -> bool:
        """Return whether ``path`` is currently queued as dirty."""
        with self._lock:
            return path in self._paths

    def __len__(self) -> int:
        with self._lock:
            return len(self._paths)
