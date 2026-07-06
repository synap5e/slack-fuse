"""Tiny per-URL TTL cache for slow control-surface fetches.

FUSE amplifies a single ``cat`` into 5+ callbacks (getattr, lookup, open,
read, getattr). Each callback that hits a slow endpoint (e.g. the multi-
second ``/gap-candidates`` day-presence query) fires its own HTTP GET,
which stacks in flight and can starve ``/health``. This cache lets the
cascade share one response for a short window; only the first call in a
window pays for the query.

Only successful responses are cached — errors retry immediately.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass


@dataclass
class _Entry[T]:
    value: T
    expires_at_monotonic: float


class TTLCache[T]:
    """Thread-safe single-slot TTL cache for one fetch result.

    Not keyed by anything — meant to sit next to a single ``fetch_X`` helper
    whose URL is fixed by construction (e.g. ``GET /gap-candidates`` is a
    parameter-free endpoint). Use one instance per endpoint.
    """

    def __init__(self, ttl_s: float, *, monotonic: Callable[[], float] = time.monotonic) -> None:
        self._ttl_s = ttl_s
        self._monotonic = monotonic
        self._lock = threading.Lock()
        self._entry: _Entry[T] | None = None

    def get(self) -> T | None:
        with self._lock:
            entry = self._entry
            if entry is None:
                return None
            if self._monotonic() >= entry.expires_at_monotonic:
                self._entry = None
                return None
            return entry.value

    def set(self, value: T) -> None:
        with self._lock:
            self._entry = _Entry(value=value, expires_at_monotonic=self._monotonic() + self._ttl_s)

    def invalidate(self) -> None:
        with self._lock:
            self._entry = None


__all__ = ["TTLCache"]
