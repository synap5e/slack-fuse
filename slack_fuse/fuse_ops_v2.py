"""Sprint 3B FUSE adapter — reads exclusively from the local chunks tables.

Per RFC §FUSE read path, §Three-tier visibility model, §Offline behaviour.
This adapter replaces ``slack_fuse/fuse_ops.py`` once ``SLACK_FUSE_MODE=split``
is the default; until then the legacy adapter stays runnable behind a flag.

Key invariants enforced here (each backed by a test under ``tests/fuse_v2/``):

1. *Trailer / kernel-cache invariant*. If the rendered bytes include a
   staleness trailer, the read handler must NOT call ``notify_store``.
2. *Unresolved-fallback / kernel-cache invariant*. If ``resolve_mentions``
   falls back to a UID/CID literal for ANY mention, the read handler must
   NOT call ``notify_store``.
3. *Connection-state transitions invalidate primed inodes*. The set of
   inodes ever primed via ``notify_store`` is tracked in-memory; any
   transition of ``connection_state`` or insert into ``stream_caught_up``
   triggers a ``pyfuse3.invalidate_inode`` call on every primed inode.
4. *Inodes are persistent*. The ``inodes`` table provides stable inode
   numbers across mount restarts.
5. *Hidden tier* readdir filters by ``tier = 'hot'``; ``lookup`` allows
   ``tier IN ('hot', 'hidden')``; ``blocked`` is ENOENT in both directions.
"""

from __future__ import annotations

import errno
import logging
import os
import stat
import threading
import time
from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Final
from zoneinfo import ZoneInfo

import pyfuse3
import trio

from slack_fuse.fuse_v2_helpers import (
    CHANNEL_MD,
    CONV_ROOTS,
    THREAD_MD,
    ChannelRow,
    PersistentInodeMap,
    build_channel_slug,
    channel_meta_frontmatter,
    day_channel_frontmatter,
    dedup_thread_slug_map,
    derive_thread_slug,
    fetch_channel_by_slug,
    fetch_conv_root_rows,
    fetch_day_chunks,
    fetch_day_thread_parents,
    fetch_known_days,
    fetch_known_months,
    fetch_staleness_state,
    fetch_thread_chunks,
    fetch_users_for_dm_slugs,
    format_trailer,
    is_valid_day,
    is_valid_month,
    parse_day_date,
    parse_path,
    resolve_with_miss_tracking,
    sql_resolvers_for,
    staleness_reason,
    thread_frontmatter,
)

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow

log = logging.getLogger(__name__)


NotifyStoreFn = Callable[[int, int, bytes], None]
InvalidateInodeFn = Callable[[int], None]

ROOT_INODE: Final = 1


def _default_notify_store(inode: int, offset: int, data: bytes) -> None:
    """Wrap pyfuse3.notify_store; exists so tests can inject a fake."""
    try:
        pyfuse3.notify_store(inode, offset, data)  # pyright: ignore[reportArgumentType]
    except OSError as exc:
        log.debug("notify_store(%d, %d, %d bytes) failed: %s", inode, offset, len(data), exc)


def _default_invalidate_inode(inode: int) -> None:
    """Wrap pyfuse3.invalidate_inode; exists so tests can inject a fake."""
    try:
        pyfuse3.invalidate_inode(inode)  # pyright: ignore[reportArgumentType]
    except OSError as exc:
        log.debug("invalidate_inode(%d) failed: %s", inode, exc)


def _make_dir_attr(inode: int) -> pyfuse3.EntryAttributes:
    entry = pyfuse3.EntryAttributes()
    entry.st_ino = inode  # pyright: ignore[reportAttributeAccessIssue]
    entry.st_mode = stat.S_IFDIR | 0o555
    entry.st_nlink = 2
    entry.st_size = 0
    now_ns = int(time.time() * 1e9)
    entry.st_atime_ns = now_ns
    entry.st_mtime_ns = now_ns
    entry.st_ctime_ns = now_ns
    entry.st_uid = os.getuid()
    entry.st_gid = os.getgid()
    return entry


def _make_file_attr(inode: int, size: int) -> pyfuse3.EntryAttributes:
    entry = pyfuse3.EntryAttributes()
    entry.st_ino = inode  # pyright: ignore[reportAttributeAccessIssue]
    entry.st_mode = stat.S_IFREG | 0o444
    entry.st_nlink = 1
    entry.st_size = size
    now_ns = int(time.time() * 1e9)
    entry.st_atime_ns = now_ns
    entry.st_mtime_ns = now_ns
    entry.st_ctime_ns = now_ns
    entry.st_uid = os.getuid()
    entry.st_gid = os.getgid()
    return entry


# ============================================================================
# Read path — pure assembly given a connection
# ============================================================================


def _assemble_channel_day(
    conn: Connection[TupleRow],
    row: ChannelRow,
    day: date,
    tz: ZoneInfo,
) -> tuple[bytes, bool, bool] | None:
    """Assemble bytes for ``/<conv-root>/<slug>/<YYYY-MM>/<DD>/channel.md``.

    Returns ``(bytes, had_trailer, had_unresolved_fallback)``. ``None`` if the
    day has no chunks.
    """
    contents = fetch_day_chunks(conn, row.channel_id, day, tz)
    if not contents:
        return None
    body = "\n".join(contents)
    users, channels = sql_resolvers_for(conn)
    resolved, had_miss = resolve_with_miss_tracking(body, users, channels)
    frontmatter = day_channel_frontmatter(row, day)
    base = frontmatter + resolved
    stale = fetch_staleness_state(conn, f"channel:{row.channel_id}")
    reason = staleness_reason(stale)
    if reason is not None:
        base += format_trailer(reason, stale.last_frame_at)
        return base.encode(), True, had_miss
    return base.encode(), False, had_miss


def _assemble_thread(
    conn: Connection[TupleRow],
    row: ChannelRow,
    thread_ts: Decimal,
    tz: ZoneInfo,
) -> tuple[bytes, bool, bool] | None:
    """Assemble bytes for ``/.../<thread-slug>/thread.md``.

    Returns ``(bytes, had_trailer, had_unresolved_fallback)`` or ``None``.
    """
    contents, reply_count = fetch_thread_chunks(conn, row.channel_id, thread_ts)
    if not contents:
        return None
    body = "\n".join(contents)
    users, channels = sql_resolvers_for(conn)
    resolved, had_miss = resolve_with_miss_tracking(body, users, channels)
    frontmatter = thread_frontmatter(row, thread_ts, reply_count, tz)
    base = frontmatter + resolved
    stale = fetch_staleness_state(conn, f"channel:{row.channel_id}")
    reason = staleness_reason(stale)
    if reason is not None:
        base += format_trailer(reason, stale.last_frame_at)
        return base.encode(), True, had_miss
    return base.encode(), False, had_miss


# ============================================================================
# Operations
# ============================================================================


class SlackFuseOpsV2(pyfuse3.Operations):
    """Read-only FUSE Operations over the local chunks projection store.

    Construct with a Postgres connection (autocommit recommended), a
    ``ZoneInfo`` for the local mount timezone, a worker-thread limiter, and
    optionally injected ``notify_store`` / ``invalidate_inode`` callables for
    tests. All filesystem callbacks delegate sync DB work to a worker thread
    via ``trio.to_thread.run_sync`` under the supplied limiter, mirroring the
    legacy adapter's threading discipline.
    """

    def __init__(
        self,
        conn: Connection[TupleRow],
        local_tz: ZoneInfo,
        limiter: trio.CapacityLimiter,
        *,
        notify_store: NotifyStoreFn | None = None,
        invalidate_inode: InvalidateInodeFn | None = None,
    ) -> None:
        super().__init__()
        self._conn = conn
        self._tz = local_tz
        self._limiter = limiter
        self._notify_store: NotifyStoreFn = notify_store if notify_store is not None else _default_notify_store
        self._invalidate_inode: InvalidateInodeFn = (
            invalidate_inode if invalidate_inode is not None else _default_invalidate_inode
        )
        self._inodes = PersistentInodeMap(conn)
        self._primed_inodes: set[int] = set()
        self._primed_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public surface used by the health subscriber + tests
    # ------------------------------------------------------------------

    @property
    def inodes(self) -> PersistentInodeMap:
        return self._inodes

    @property
    def primed_inodes_snapshot(self) -> frozenset[int]:
        with self._primed_lock:
            return frozenset(self._primed_inodes)

    def resolve_content_for_test(self, path: str) -> tuple[bytes, bool, bool] | None:
        """Test-only accessor for the read-path assembly result."""
        return self._resolve_content(path)

    def list_dir_for_test(self, path: str) -> list[tuple[str, bool]]:
        """Test-only accessor for the readdir result."""
        return self._list_dir(path)

    def is_dir_for_test(self, path: str) -> bool:
        """Test-only accessor for the directory classifier."""
        return self._is_dir(path)

    def invalidate_all_primed(self) -> int:
        """Drop every primed inode from the kernel page cache.

        Called by the health subscriber on any ``connection_state`` field
        change and on every ``stream_caught_up`` insert. Returns the number
        of inodes invalidated (test introspection).
        """
        with self._primed_lock:
            snapshot = list(self._primed_inodes)
            self._primed_inodes.clear()
        for inode in snapshot:
            self._invalidate_inode(inode)
        return len(snapshot)

    def _track_primed(self, inode: int) -> None:
        with self._primed_lock:
            self._primed_inodes.add(inode)

    # ------------------------------------------------------------------
    # Path classification and dispatch
    # ------------------------------------------------------------------

    def _list_dir(self, path: str) -> list[tuple[str, bool]]:  # noqa: C901  (path-depth dispatch hub)
        parts = parse_path(path)
        depth = len(parts)
        if depth == 0:
            return [(d, True) for d in CONV_ROOTS]

        if parts[0] not in CONV_ROOTS:
            return []
        conv_root = parts[0]

        if depth == 1:
            rows = fetch_conv_root_rows(self._conn, conv_root, allow_hidden=False)
            users = fetch_users_for_dm_slugs(self._conn, rows)
            counts: dict[str, int] = {}
            return [(build_channel_slug(r, users, counts), True) for r in rows]

        row = fetch_channel_by_slug(self._conn, conv_root, parts[1], allow_hidden=True)
        if row is None:
            return []

        if depth == 2:
            months = fetch_known_months(self._conn, row.channel_id, self._tz)
            return [(CHANNEL_MD, False), *((m, True) for m in months)]

        if depth == 3:
            if not is_valid_month(parts[2]):
                return []
            days = fetch_known_days(self._conn, row.channel_id, parts[2], self._tz)
            return [(d, True) for d in days]

        if depth == 4:
            day = parse_day_date(parts[2], parts[3])
            if day is None:
                return []
            parents = fetch_day_thread_parents(self._conn, row.channel_id, day, self._tz)
            result: list[tuple[str, bool]] = [(CHANNEL_MD, False)]
            for slug in dedup_thread_slug_map(parents):
                result.append((slug, True))
            return result

        if depth == 5:
            return [(THREAD_MD, False)]

        return []

    def _is_dir(self, path: str) -> bool:
        parts = parse_path(path)
        depth = len(parts)
        if depth == 0:
            return True
        if depth == 1:
            return parts[0] in CONV_ROOTS
        if parts[0] not in CONV_ROOTS:
            return False
        if depth == 2:
            row = fetch_channel_by_slug(self._conn, parts[0], parts[1], allow_hidden=True)
            return row is not None
        if depth == 3:
            if parts[2] == CHANNEL_MD:
                return False
            return is_valid_month(parts[2])
        if depth == 4:
            return is_valid_day(parts[3])
        if depth == 5:
            return parts[4] != CHANNEL_MD
        if depth == 6:
            return parts[5] != THREAD_MD
        return False

    def _resolve_content(self, path: str) -> tuple[bytes, bool, bool] | None:
        """Return ``(bytes, had_trailer, had_unresolved_fallback)`` or ``None``.

        The two booleans together drive the ``notify_store`` gating in
        ``read()``. Per RFC: trailer-bearing OR fallback-bearing bytes must
        NEVER enter the kernel page cache via ``notify_store``.
        """
        parts = parse_path(path)
        depth = len(parts)
        if depth < 3 or parts[0] not in CONV_ROOTS:
            return None

        row = fetch_channel_by_slug(self._conn, parts[0], parts[1], allow_hidden=True)
        if row is None:
            return None

        # /<conv-root>/<slug>/channel.md — channel metadata
        if depth == 3 and parts[2] == CHANNEL_MD:
            return channel_meta_frontmatter(row), False, False

        # /<conv-root>/<slug>/<YYYY-MM>/<DD>/channel.md
        if depth == 5 and parts[4] == CHANNEL_MD:
            day = parse_day_date(parts[2], parts[3])
            if day is None:
                return None
            return _assemble_channel_day(self._conn, row, day, self._tz)

        # /<conv-root>/<slug>/<YYYY-MM>/<DD>/<thread-slug>/thread.md
        if depth == 6 and parts[5] == THREAD_MD:
            day = parse_day_date(parts[2], parts[3])
            if day is None:
                return None
            thread_ts = self._resolve_thread_ts(row.channel_id, day, parts[4])
            if thread_ts is None:
                return None
            return _assemble_thread(self._conn, row, thread_ts, self._tz)

        return None

    def _resolve_thread_ts(self, channel_id: str, day: date, thread_slug: str) -> Decimal | None:
        parents = fetch_day_thread_parents(self._conn, channel_id, day, self._tz)
        slug_map = dedup_thread_slug_map(parents)
        ts = slug_map.get(thread_slug)
        if ts is not None:
            return ts
        # Fall back: if the slug map missed (no thread by that name yet under
        # this day), no thread file exists.
        _ = derive_thread_slug  # kept imported for symmetry; not used here
        return None

    # ------------------------------------------------------------------
    # FUSE callbacks
    # ------------------------------------------------------------------

    async def getattr(
        self,
        inode: int,
        ctx: pyfuse3.RequestContext,
    ) -> pyfuse3.EntryAttributes:
        path = self._inodes.get_path(inode)
        if path is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        def _sync() -> pyfuse3.EntryAttributes | None:
            if self._is_dir(path):
                return _make_dir_attr(inode)
            resolved = self._resolve_content(path)
            if resolved is None:
                return None
            content, _trailer, _fallback = resolved
            return _make_file_attr(inode, len(content))

        result = await trio.to_thread.run_sync(_sync, limiter=self._limiter)
        if result is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        return result

    async def lookup(
        self,
        parent_inode: int,
        name: bytes,
        ctx: pyfuse3.RequestContext,
    ) -> pyfuse3.EntryAttributes:
        parent_path = self._inodes.get_path(parent_inode)
        if parent_path is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        child_name = name.decode("utf-8", errors="surrogateescape")
        child_path = f"/{child_name}" if parent_path == "/" else f"{parent_path}/{child_name}"

        def _sync() -> pyfuse3.EntryAttributes | None:
            entries = self._list_dir(parent_path)
            for entry_name, entry_is_dir in entries:
                if entry_name == child_name:
                    inode = self._inodes.get_or_create(child_path)
                    if entry_is_dir:
                        return _make_dir_attr(inode)
                    resolved = self._resolve_content(child_path)
                    if resolved is None:
                        return None
                    content, _trailer, _fallback = resolved
                    return _make_file_attr(inode, len(content))
            return None

        result = await trio.to_thread.run_sync(_sync, limiter=self._limiter)
        if result is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        return result

    async def opendir(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        inode: int,
        ctx: pyfuse3.RequestContext,
    ) -> int:
        if self._inodes.get_path(inode) is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        return inode

    async def readdir(
        self,
        fh: int,
        start_id: int,
        token: pyfuse3.ReaddirToken,
    ) -> None:
        path = self._inodes.get_path(fh)
        if path is None:
            return

        def _sync() -> list[tuple[str, pyfuse3.EntryAttributes, int]]:
            entries = self._list_dir(path)
            result: list[tuple[str, pyfuse3.EntryAttributes, int]] = []
            for idx, (name, is_dir) in enumerate(entries):
                if idx < start_id:
                    continue
                child_path = f"/{name}" if path == "/" else f"{path}/{name}"
                child_inode = self._inodes.get_or_create(child_path)
                if is_dir:
                    attr = _make_dir_attr(child_inode)
                else:
                    resolved = self._resolve_content(child_path)
                    size = len(resolved[0]) if resolved is not None else 0
                    attr = _make_file_attr(child_inode, size)
                result.append((name, attr, idx + 1))
            return result

        computed = await trio.to_thread.run_sync(_sync, limiter=self._limiter)
        for name, attr, next_id in computed:
            if not pyfuse3.readdir_reply(token, name.encode("utf-8"), attr, next_id):
                break

    async def open(
        self,
        inode: int,
        flags: int,
        ctx: pyfuse3.RequestContext,
    ) -> pyfuse3.FileInfo:
        if self._inodes.get_path(inode) is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        fi = pyfuse3.FileInfo()
        fi.fh = inode  # pyright: ignore[reportAttributeAccessIssue]
        # Kernel page-cache caching is gated by the trailer + fallback rules
        # in read(); see RFC §FUSE read path → Trailer / kernel-cache
        # invariant + §Unresolved-fallback / kernel-cache invariant.
        fi.keep_cache = True  # pyright: ignore[reportAttributeAccessIssue]
        return fi

    async def read(self, fh: int, off: int, size: int) -> bytes:
        path = self._inodes.get_path(fh)
        if path is None:
            raise pyfuse3.FUSEError(errno.EIO)

        def _sync() -> tuple[bytes, bool, bool, str] | None:
            resolved = self._resolve_content(path)
            if resolved is None:
                return None
            content, trailer, fallback = resolved
            return content, trailer, fallback, path

        result = await trio.to_thread.run_sync(_sync, limiter=self._limiter)
        if result is None:
            raise pyfuse3.FUSEError(errno.EIO)
        content, had_trailer, had_fallback, real_path = result

        # ----------------- HARD INVARIANT GATE -----------------
        # notify_store is the bytes-into-kernel-page-cache action. Two
        # invariants forbid it:
        #   1. Trailer present → kernel must NOT cache the warning bytes
        #      (RFC §FUSE read path → Trailer / kernel-cache invariant)
        #   2. Unresolved-fallback present → kernel must NOT cache
        #      UID/CID literals (RFC §FUSE read path → Unresolved-fallback
        #      / kernel-cache invariant)
        # Tier is the third gate: only ``hot`` files get primed at all
        # (RFC §Three-tier visibility model → "Kernel priming … fires only
        # on tier = 'hot' reads.").
        # -------------------------------------------------------
        if not had_trailer and not had_fallback and self._is_hot(real_path):
            self._notify_store(fh, 0, content)
            self._track_primed(fh)

        return content[off : off + size]

    async def statfs(
        self,
        ctx: pyfuse3.RequestContext,
    ) -> pyfuse3.StatvfsData:
        stat_info = pyfuse3.StatvfsData()
        stat_info.f_bsize = 4096
        stat_info.f_frsize = 4096
        stat_info.f_blocks = 0
        stat_info.f_bfree = 0
        stat_info.f_bavail = 0
        stat_info.f_files = 0
        stat_info.f_ffree = 0
        stat_info.f_favail = 0
        stat_info.f_namemax = 255
        return stat_info

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _is_hot(self, path: str) -> bool:
        """Tier gate for ``notify_store``: only ``hot`` files get primed."""
        parts = parse_path(path)
        if len(parts) < 2 or parts[0] not in CONV_ROOTS:
            return False
        row = fetch_channel_by_slug(self._conn, parts[0], parts[1], allow_hidden=True)
        if row is None:
            return False
        return row.tier == "hot"


# ============================================================================
# Synchronous APIs for non-pyfuse3 unit tests
# ============================================================================


def synchronous_read_for_test(
    ops: SlackFuseOpsV2,
    path: str,
) -> tuple[bytes, bool, bool] | None:
    """Public test hook: assemble a path's bytes without going through pyfuse3.

    Useful for invariant tests that need to assert on ``(had_trailer,
    had_fallback)`` directly rather than infer them from notify_store calls.
    """
    return ops.resolve_content_for_test(path)


__all__ = [
    "InvalidateInodeFn",
    "NotifyStoreFn",
    "SlackFuseOpsV2",
    "synchronous_read_for_test",
]
# Imports retained for re-export under tests / health subscriber.
_ = (UTC, datetime)  # noqa: PLW0127, RUF100
