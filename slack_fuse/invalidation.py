"""Invalidation protocol: drop kernel page-cache pages on push events.

`SlackStore` carries an optional `InvalidationSink`; `fuse_ops` implements one
that calls `pyfuse3.invalidate_inode` on the affected file inodes. Without
this, `open()` sets `fi.keep_cache = True` and the kernel serves stale
buffered bytes even after our render-cache has been superseded by a
socket-mode event.

The sink is an interface (not a concrete class) so tests can substitute a
recorder and the store has no compile-time dependency on pyfuse3.
"""

from __future__ import annotations

from typing import Protocol


class InvalidationSink(Protocol):
    """Receives notifications that a given cache key has changed.

    Implementations translate the cache key into one or more FUSE paths,
    look up inodes, and ask the kernel to drop its page cache for them.
    Calls are sync and run on whatever thread the store runs on.
    """

    def day_changed(self, channel_id: str, date_str: str) -> None: ...

    def thread_changed(self, channel_id: str, thread_ts: str) -> None: ...

    def channel_list_changed(self) -> None: ...
