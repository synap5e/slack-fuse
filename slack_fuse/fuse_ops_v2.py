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
from collections.abc import Callable, Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Final
from zoneinfo import ZoneInfo

import pyfuse3
import trio

from slack_fuse.fuse_v2_helpers import (
    CHANNEL_LIST_STREAM,
    CHANNEL_MD,
    CONV_ROOTS,
    THREAD_MD,
    ChannelRow,
    PersistentInodeMap,
    assign_conv_root_slugs,
    channel_meta_frontmatter,
    day_channel_frontmatter,
    dedup_thread_slug_map,
    fetch_channel_by_slug,
    fetch_day_chunks,
    fetch_day_thread_parents,
    fetch_known_days,
    fetch_known_months,
    fetch_staleness_state,
    fetch_thread_chunks,
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


# errnos that are benign during a teardown/unmount race: the inode the kernel
# is asking about (or that we're trying to drop) is simply gone. Anything else
# from notify_store / invalidate_inode is a real surprise and logged loudly
# (review P2-10 / GPT: debug-level logging is effectively silent in prod).
_BENIGN_KERNEL_CACHE_ERRNOS: Final = frozenset({errno.ENOENT, errno.EBADF})


def _log_kernel_cache_oserror(op: str, inode: int, exc: OSError) -> None:
    if exc.errno in _BENIGN_KERNEL_CACHE_ERRNOS:
        log.debug("%s(%d) failed (benign shutdown/unmount race, errno=%s): %s", op, inode, exc.errno, exc)
    else:
        log.warning("%s(%d) failed (errno=%s): %s", op, inode, exc.errno, exc)


def _default_notify_store(inode: int, offset: int, data: bytes) -> None:
    """Wrap pyfuse3.notify_store; exists so tests can inject a fake."""
    try:
        pyfuse3.notify_store(inode, offset, data)  # pyright: ignore[reportArgumentType]
    except OSError as exc:
        _log_kernel_cache_oserror("notify_store", inode, exc)


def _default_invalidate_inode(inode: int) -> None:
    """Wrap pyfuse3.invalidate_inode; exists so tests can inject a fake."""
    try:
        pyfuse3.invalidate_inode(inode)  # pyright: ignore[reportArgumentType]
    except OSError as exc:
        _log_kernel_cache_oserror("invalidate_inode", inode, exc)


# Attribute/entry caching timeouts (review P2-8 / Gemini Class 8). Without
# these the kernel re-issues getattr on every access, and getattr renders the
# whole file to compute its size — so `ls -l` on a day directory re-renders
# every file. Only locked-in (strictly-past-day) ``channel.md`` files, whose
# bytes can no longer grow, are cached; everything whose size can still change
# (today's day file, thread files, channel metadata) keeps a 0 timeout so the
# kernel always re-checks the live size. Directories also stay at 0: caching a
# directory's entry would let a freshly-``blocked`` channel stay traversable
# until the entry expired, undermining the tier-flip ENOENT path (P1-6); dir
# getattr renders nothing, so there's no perf benefit to caching it anyway.
_DIR_ATTR_TIMEOUT_S: Final = 0.0
_IMMUTABLE_FILE_TIMEOUT_S: Final = 3600.0
_MUTABLE_FILE_TIMEOUT_S: Final = 0.0


def _make_dir_attr(inode: int) -> pyfuse3.EntryAttributes:
    entry = pyfuse3.EntryAttributes()
    entry.st_ino = inode  # pyright: ignore[reportAttributeAccessIssue]
    entry.st_mode = stat.S_IFDIR | 0o555
    entry.st_nlink = 2
    entry.st_size = 0
    entry.st_atime_ns = entry.st_mtime_ns = entry.st_ctime_ns = int(time.time() * 1e9)
    entry.st_uid = os.getuid()
    entry.st_gid = os.getgid()
    entry.entry_timeout = _DIR_ATTR_TIMEOUT_S
    entry.attr_timeout = _DIR_ATTR_TIMEOUT_S
    return entry


def _make_file_attr(inode: int, size: int, *, timeout_s: float = _MUTABLE_FILE_TIMEOUT_S) -> pyfuse3.EntryAttributes:
    entry = pyfuse3.EntryAttributes()
    entry.st_ino = inode  # pyright: ignore[reportAttributeAccessIssue]
    entry.st_mode = stat.S_IFREG | 0o444
    entry.st_nlink = 1
    entry.st_size = size
    entry.st_atime_ns = entry.st_mtime_ns = entry.st_ctime_ns = int(time.time() * 1e9)
    entry.st_uid = os.getuid()
    entry.st_gid = os.getgid()
    entry.entry_timeout = timeout_s
    entry.attr_timeout = timeout_s
    return entry


def _file_attr_timeout(path: str, tz: ZoneInfo) -> float:
    """Pick an attr/entry timeout for a regular file at ``path``.

    Only a day-level ``channel.md`` for a date strictly before today (local tz)
    is treated as immutable — its content can no longer grow. Today's day file
    (new messages), thread files (replies can land on any day), and channel
    metadata can all still change size, so they stay uncached.
    """
    parts = parse_path(path)
    if len(parts) == 5 and parts[4] == CHANNEL_MD:
        day = parse_day_date(parts[2], parts[3])
        if day is not None and day < datetime.now(tz).date():
            return _IMMUTABLE_FILE_TIMEOUT_S
    return _MUTABLE_FILE_TIMEOUT_S


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


def _assemble_channel_meta(
    conn: Connection[TupleRow],
    row: ChannelRow,
) -> tuple[bytes, bool, bool]:
    """Assemble bytes for ``/<conv-root>/<slug>/channel.md`` — channel metadata.

    Channel metadata is local projected data too: it goes stale after a
    rename/archive/tier change or a channel-list catch-up gap. So it is subject
    to the same trailer + ``notify_store`` gate as day/thread files (review
    P1-5: both reviewers flagged that this file was kernel-primed unconditionally
    and would serve stale metadata forever while disconnected). The natural
    staleness stream is ``channel-list``.
    """
    base = channel_meta_frontmatter(row).decode()
    stale = fetch_staleness_state(conn, CHANNEL_LIST_STREAM)
    reason = staleness_reason(stale)
    if reason is not None:
        base += format_trailer(reason, stale.last_frame_at)
        return base.encode(), True, False
    return base.encode(), False, False


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
        # Per-opendir snapshots so readdir pagination tokens stay stable even
        # if the underlying channel/day set shifts between paginated calls
        # (review P2-9 / Gemini Class 5: array-index tokens skip/duplicate
        # entries when the row-set changes mid-iteration).
        self._dir_handles: dict[int, list[tuple[str, pyfuse3.EntryAttributes, int]]] = {}
        self._dir_handle_seq = 0
        self._dir_handle_lock = threading.Lock()

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

    def _list_dir(self, path: str, *, for_lookup: bool = False) -> list[tuple[str, bool]]:  # noqa: C901  (path-depth dispatch hub)
        parts = parse_path(path)
        depth = len(parts)
        if depth == 0:
            return [(d, True) for d in CONV_ROOTS]

        if parts[0] not in CONV_ROOTS:
            return []
        conv_root = parts[0]

        if depth == 1:
            # Slugs are assigned over the full hot+hidden set (so they agree
            # with the lookup path — review P0-4) then filtered to hot for the
            # readdir listing. ``lookup`` passes ``for_lookup=True`` so hidden
            # channels remain reachable by their (suffixed) slug even though
            # readdir does not list them: per the RFC three-tier model, hidden
            # is "not listed but reachable by known path" (review P0-2 /
            # GPT-5.5). Without this, ``cat /channels/<hidden-slug>/...`` would
            # ENOENT because the kernel's lookup of the slug found nothing in
            # the hot-only listing. The conv-root child level is the ONLY depth
            # where readdir and lookup diverge — every deeper level already
            # resolves with ``allow_hidden=True`` and encodes existence (months/
            # days/threads that actually have chunks), so scanning the listing
            # stays correct there.
            slugs = assign_conv_root_slugs(self._conn, conv_root)
            if for_lookup:
                return [(slug, True) for _r, slug in slugs]
            return [(slug, True) for r, slug in slugs if r.tier == "hot"]

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
        if parts[0] not in CONV_ROOTS:
            return False
        if depth == 1:
            return True
        # depth >= 2: the channel must still resolve (hot or hidden). A
        # blocked/deleted channel is ENOENT for its ENTIRE subtree, not just
        # its root — `_is_dir` is reached via `getattr` on persistent inodes
        # that may have been allocated while the channel was still hot, so a
        # purely syntactic depth-3+ check would keep the blocked subtree
        # traversable (review P1-6 / GPT).
        if fetch_channel_by_slug(self._conn, parts[0], parts[1], allow_hidden=True) is None:
            return False
        if depth == 2:
            return True
        if depth == 3:
            return parts[2] != CHANNEL_MD and is_valid_month(parts[2])
        if depth == 4:
            return is_valid_month(parts[2]) and is_valid_day(parts[3])
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

        # /<conv-root>/<slug>/channel.md — channel metadata. Subject to the
        # same staleness trailer + notify_store gate as day/thread files
        # (review P1-5).
        if depth == 3 and parts[2] == CHANNEL_MD:
            return _assemble_channel_meta(self._conn, row)

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
        # If the slug map missed (no thread by that name yet under this day),
        # no thread file exists.
        return slug_map.get(thread_slug)

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
            return _make_file_attr(inode, len(content), timeout_s=_file_attr_timeout(path, self._tz))

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
            # ``for_lookup=True`` so hidden conv-root channels resolve by their
            # known slug (RFC: hidden is reachable, just not listed). readdir
            # filters them out; lookup must not (review P0-2 / GPT-5.5).
            entries = self._list_dir(parent_path, for_lookup=True)
            for entry_name, entry_is_dir in entries:
                if entry_name == child_name:
                    inode = self._inodes.get_or_create(child_path)
                    if entry_is_dir:
                        return _make_dir_attr(inode)
                    resolved = self._resolve_content(child_path)
                    if resolved is None:
                        return None
                    content, _trailer, _fallback = resolved
                    return _make_file_attr(inode, len(content), timeout_s=_file_attr_timeout(child_path, self._tz))
            return None

        result = await trio.to_thread.run_sync(_sync, limiter=self._limiter)
        if result is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        return result

    def _snapshot_dir(self, path: str) -> list[tuple[str, pyfuse3.EntryAttributes, int]]:
        """Materialize a directory listing with stable pagination tokens.

        Called once per ``opendir`` so the (name, attr, next_token) triples are
        frozen for the lifetime of the dir handle; ``readdir`` then serves
        slices by token without re-querying, so a concurrent insert can't shift
        the array out from under an in-progress iteration.
        """
        result: list[tuple[str, pyfuse3.EntryAttributes, int]] = []
        for idx, (name, is_dir) in enumerate(self._list_dir(path)):
            child_path = f"/{name}" if path == "/" else f"{path}/{name}"
            child_inode = self._inodes.get_or_create(child_path)
            if is_dir:
                attr = _make_dir_attr(child_inode)
            else:
                resolved = self._resolve_content(child_path)
                size = len(resolved[0]) if resolved is not None else 0
                attr = _make_file_attr(child_inode, size, timeout_s=_file_attr_timeout(child_path, self._tz))
            result.append((name, attr, idx + 1))
        return result

    def _register_dir_handle(self, snapshot: list[tuple[str, pyfuse3.EntryAttributes, int]]) -> int:
        with self._dir_handle_lock:
            self._dir_handle_seq += 1
            fh = self._dir_handle_seq
            self._dir_handles[fh] = snapshot
        return fh

    async def opendir(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        inode: int,
        ctx: pyfuse3.RequestContext,
    ) -> int:
        path = self._inodes.get_path(inode)
        if path is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        snapshot = await trio.to_thread.run_sync(lambda: self._snapshot_dir(path), limiter=self._limiter)
        return self._register_dir_handle(snapshot)

    async def readdir(
        self,
        fh: int,
        start_id: int,
        token: pyfuse3.ReaddirToken,
    ) -> None:
        with self._dir_handle_lock:
            snapshot = self._dir_handles.get(fh)
        if snapshot is None:
            return
        for name, attr, next_id in snapshot:
            if next_id <= start_id:
                continue
            if not pyfuse3.readdir_reply(token, name.encode("utf-8"), attr, next_id):
                break

    async def releasedir(self, fh: int) -> None:
        with self._dir_handle_lock:
            _ = self._dir_handles.pop(fh, None)

    async def forget(self, inode_list: Sequence[tuple[int, int]]) -> None:
        for inode, _nlookup in inode_list:
            self._inodes.forget(inode)

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
