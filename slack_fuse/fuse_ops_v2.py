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

import contextlib
import errno
import logging
import os
import stat
import threading
import time
from collections.abc import Callable, Iterator, Sequence
from contextvars import ContextVar
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Final, TypeVar
from zoneinfo import ZoneInfo

import psycopg
import pyfuse3
import trio

from slack_fuse.control import ControlState, result_for_status
from slack_fuse.fuse_v2_helpers import (
    CHANNEL_LIST_STREAM,
    CHANNEL_MD,
    CHANNEL_ORIGINAL_MD,
    CONTROL_BACKFILL_CHANNEL,
    CONTROL_BLOCKED_CHANNELS,
    CONTROL_DIR,
    CONTROL_GAPS,
    CONTROL_PROBE_SWEEP,
    CONTROL_PROBE_SWEEP_JOB,
    CONTROL_PROBE_SWEEP_TARGET,
    CONTROL_PROBES,
    CONTROL_REFILL_GAP,
    CONTROL_REFRESH_CHANNEL,
    CONTROL_REFRESH_CHANNELS,
    CONTROL_RERENDER_CHANNEL,
    CONTROL_STATUS,
    CONTROL_WRITABLE,
    CONV_ROOTS,
    GAPS_MD,
    THREAD_MD,
    WORKSPACE_DIR,
    ChannelRow,
    PersistentInodeMap,
    assign_conv_root_slugs,
    borrowed_fuse_conn,
    channel_meta_frontmatter,
    conv_root_for,
    day_channel_frontmatter,
    dedup_thread_slug_map,
    fetch_channel_by_slug,
    fetch_day_chunks,
    fetch_day_thread_parents,
    fetch_known_days,
    fetch_known_months,
    fetch_staleness_state,
    fetch_thread_chunks,
    is_valid_day,
    is_valid_month,
    parse_day_date,
    parse_path,
    resolve_with_miss_tracking,
    sql_resolvers_for,
    thread_frontmatter,
    ts_to_local_date,
)
from slack_fuse.logctx import fuse_op, set_path
from slack_fuse.pg_health import NO_POSTGRES_INODE, NO_POSTGRES_NAME
from slack_fuse.projector.trailer import (
    STALE_AFTER_DISCONNECT_S,
    TrailerDecision,
    classify_trailer,
    render_trailer,
)

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow

    from slack_fuse.pg_health import PgHealth
    from slack_fuse.projector.apply import ChunkRef, ThreadChunkRef
    from slack_fuse.projector.pool import ConnectionPool
    from slack_fuse.projector.trailer_log import TrailerLog


#: Default per-callback timeout when running in pool mode. The contract is:
#: every FUSE callback either returns valid data or EIO within this window.
#: Nothing legitimately takes longer than a fraction of a second against a
#: healthy local Postgres on warm caches — chunk reads are indexed, render
#: is in-memory, inode lookups are dict-or-indexed. Tightening from the
#: previous 30s to 1s means a wedge or slow-query path that *would* have
#: queued every subsequent FUSE callback behind it now surfaces as EIO
#: almost immediately. If a future code path legitimately needs to fetch
#: cold data over WS (currently nothing does — the projector materialises
#: everything locally), that path will need its own longer budget or a
#: separate dispatcher.
DEFAULT_CALLBACK_TIMEOUT_S: Final = 1.0

#: Looser per-callback budget for ``_control/*`` paths. Operator triggers +
#: liveness reads have expensive backing queries (e.g. ``/gap-candidates``
#: runs a ~2s SQL at prod scale) — the normal 1s budget was tuned for the
#: hot browsing path, not the control surface. A cold ``cat _control/gaps``
#: cannot service the fetch under 1s, and clamping it EIOs the operator
#: for no operational reason. 15s comfortably fits the slowest known query
#: while still guarding against wedges.
CONTROL_CALLBACK_TIMEOUT_S: Final = 15.0

#: The budget currently in force for this callback. ``_callback_guard`` sets
#: this per-op based on the resolved path; ``_run_sync``'s inner fail_after
#: guards read it so they honour the same budget instead of the default. If
#: unset (e.g. tests or a direct call path that bypasses the guard), the
#: reader falls back to ``self._callback_timeout_s`` — same behaviour as
#: before this ContextVar existed.
_current_callback_budget: ContextVar[float | None] = ContextVar("current_callback_budget", default=None)

#: Generic for ``_run_sync``: the worker's return type flows through to the
#: caller so each callback gets the right narrowed type.
_TSync = TypeVar("_TSync")


def _utcnow() -> datetime:
    return datetime.now(UTC)


NowFn = Callable[[], datetime]

log = logging.getLogger(__name__)


NotifyStoreFn = Callable[[int, int, bytes], None]
InvalidateInodeFn = Callable[[int], None]
# ``OriginalsFetchFn(channel_id, from_epoch, to_epoch) -> response bytes``.
# Bytes are the server's placeholder-markdown body (same shape as
# ``chunks.content_md``); the FUSE side resolves mentions before serving.
# Production wires this to a sync httpx GET (see projector/originals_fetch.py);
# tests pass a fake.
OriginalsFetchFn = Callable[[str, float, float], bytes]
# ``ChannelGapsFetchFn(channel_id) -> markdown body``.
# Per-channel ``gaps.md`` ghost file fetcher.
ChannelGapsFetchFn = Callable[[str], bytes]
# ``WorkspaceGapsFetchFn() -> markdown body``.
# ``/_workspace/gaps.md`` workspace-wide summary fetcher.
WorkspaceGapsFetchFn = Callable[[], bytes]

# ``_control/`` refresh triggers. Both return an HTTP status code (or the ``0``
# transport-error sentinel). Production wires these to sync httpx POSTs against
# the slurper server (see projector/refresh_fetch.py); tests pass a fake.
ControlRefreshWorkspaceFn = Callable[[], int]
ControlRefreshChannelFn = Callable[[str], int]
ControlBlockedChannelsReadFn = Callable[[], bytes]
ControlBlockedChannelsListFn = Callable[[], set[str]]
ControlBlockChannelFn = Callable[[str, str | None], int]
ControlUnblockChannelFn = Callable[[str], int]
ControlBackfillChannelFn = Callable[[str], tuple[int, str | None]]
ControlProbeSweepFn = Callable[[str | None, str | None], tuple[int, str | None]]
ControlGapsReadFn = Callable[[], bytes]
ControlProbesReadFn = Callable[[], bytes]
ControlRefillGapFn = Callable[[str, float, float], str]
# ``_control/rerender_channel`` hands a resolved channel id off to a background
# consumer (the rerender is too heavy for the per-callback budget). Returns True
# if the request was accepted (queued), False if the queue is full / busy. Wired
# in ``__main__.cmd_mount_split`` to a trio memory-channel ``send_nowait``;
# called from the trio event loop in ``_fire_control`` (never a worker thread,
# so the non-thread-safe channel send is safe).
ControlRerenderChannelFn = Callable[[str], bool]

# Write-handle file numbers for ``_control/`` writes live in a high, disjoint
# range so they can never collide with a real persistent inode (those come from
# a Postgres sequence that will not reach 2**48). ``open`` allocates from this
# base; ``write`` / ``release`` look the handle up in ``_control_write_buffers``.
_CONTROL_FH_BASE: Final = 1 << 48
# Hard cap on a single control write — these carry a channel id or slug, never
# more than a few dozen bytes. The cap turns a runaway ``cat huge > ctl`` into
# EFBIG instead of unbounded memory growth.
_CONTROL_WRITE_MAX: Final = 65536

ROOT_INODE: Final = 1

# Bounded TTL cache for channel.original.md renders.
# - getattr → render-to-compute-size; read → render-to-return-bytes. Without
#   a cache the same (channel, day) would replay the server's events twice
#   for a single ``cat``.
# - TTL is short (10s) so a fresh edit/delete shows up quickly on the next
#   read but stat+read+rapid-follow-up all share one fetch.
# - Capacity is bounded so a recursive scan that DID find these files (e.g.
#   someone running ``rg`` with a pattern that matches the literal filename)
#   doesn't grow memory unbounded.
_ORIGINALS_CACHE_TTL_S: Final = 10.0
_ORIGINALS_CACHE_MAX_ENTRIES: Final = 64


@dataclass(slots=True)
class _CachedOriginal:
    content: bytes
    cached_at_monotonic: float


class _BytesCache:
    """Bounded TTL cache over byte payloads keyed by arbitrary hashable keys.

    Used by the originals view (keyed by ``(channel_id, day_iso)``), the
    per-channel gaps view (keyed by ``(channel_id,)``), and the workspace
    gaps view (single-cell, keyed by ``()``). Thread-safe: a stat+read
    pair from one process arrives on different worker threads, so the
    cache lookup needs a lock. The lock is held only across the dict ops,
    never across an HTTP fetch.
    """

    def __init__(
        self,
        *,
        max_entries: int = _ORIGINALS_CACHE_MAX_ENTRIES,
        ttl_s: float = _ORIGINALS_CACHE_TTL_S,
    ) -> None:
        self._max = max_entries
        self._ttl = ttl_s
        self._entries: dict[tuple[object, ...], _CachedOriginal] = {}
        self._lock = threading.Lock()

    def get(self, *key: object) -> bytes | None:
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            if time.monotonic() - entry.cached_at_monotonic > self._ttl:
                _ = self._entries.pop(key, None)
                return None
            return entry.content

    def put(self, *key: object, content: bytes) -> None:
        now = time.monotonic()
        with self._lock:
            if len(self._entries) >= self._max and key not in self._entries:
                # Cheap FIFO eviction (insertion order). True LRU isn't worth
                # the bookkeeping for this rarely-touched cache.
                first_key = next(iter(self._entries))
                _ = self._entries.pop(first_key, None)
            self._entries[key] = _CachedOriginal(content=content, cached_at_monotonic=now)


# Backwards-compat alias so the originals tests' import keeps working.
_OriginalsCache = _BytesCache


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


def _make_file_attr(
    inode: int,
    size: int,
    *,
    timeout_s: float = _MUTABLE_FILE_TIMEOUT_S,
    mode: int = stat.S_IFREG | 0o444,
) -> pyfuse3.EntryAttributes:
    entry = pyfuse3.EntryAttributes()
    entry.st_ino = inode  # pyright: ignore[reportAttributeAccessIssue]
    entry.st_mode = mode
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


@dataclass(frozen=True, slots=True)
class TrailerConfig:
    """The read-path knobs that drive the trailer decision (Sprint 3C wiring).

    Threaded from ``ClientConfig`` through ``SlackFuseOpsV2`` into the assembly
    helpers so the per-read classification matches the health subscriber.
    ``trailer_enabled=False`` is the bake-in comparison knob (RFC §Configuration
    → ``stale_trailer_enabled``): when off, staleness no longer appends a
    trailer nor gates ``notify_store`` — the decision is still classified and
    logged, so the false-positive rate stays measurable, but the bytes are
    served raw. The unresolved-mention fallback invariant is independent and
    stays on regardless.
    """

    now: datetime
    stale_after_s: float = STALE_AFTER_DISCONNECT_S
    trailer_enabled: bool = True


def _decide_and_apply(
    conn: Connection[TupleRow],
    base: str,
    stream: str,
    fallback_reasons: Sequence[str],
    cfg: TrailerConfig,
) -> tuple[bytes, bool, bool, TrailerDecision]:
    """Classify staleness + fallback for ``base`` and apply the trailer.

    Returns ``(bytes, had_trailer, had_unresolved_fallback, decision)``.
    ``had_trailer`` is the *effective* flag (a trailer was appended), which is
    always ``False`` when ``trailer_enabled`` is off even if the decision
    classifies as ``stale``. ``had_unresolved_fallback`` is independent of the
    trailer flag and of ``trailer_enabled``. ``decision`` carries the true
    classification for the JSONL log regardless of either flag.
    """
    stale = fetch_staleness_state(conn, stream)
    decision = classify_trailer(
        stale,
        stream=stream,
        now=cfg.now,
        stale_after_s=cfg.stale_after_s,
        fallback_reasons=fallback_reasons,
    )
    had_fallback = bool(fallback_reasons)
    trailer_text = render_trailer(decision) if cfg.trailer_enabled else None
    if trailer_text is not None:
        return (base + trailer_text).encode(), True, had_fallback, decision
    return base.encode(), False, had_fallback, decision


def _assemble_channel_day(
    conn: Connection[TupleRow],
    row: ChannelRow,
    day: date,
    tz: ZoneInfo,
    cfg: TrailerConfig,
) -> tuple[bytes, bool, bool, TrailerDecision] | None:
    """Assemble bytes for ``/<conv-root>/<slug>/<YYYY-MM>/<DD>/channel.md``.

    Returns ``(bytes, had_trailer, had_unresolved_fallback, decision)``.
    ``None`` if the day has no chunks.
    """
    contents = fetch_day_chunks(conn, row.channel_id, day, tz)
    if not contents:
        return None
    body = "\n".join(contents)
    users, channels = sql_resolvers_for(conn)
    resolved, fallback_reasons = resolve_with_miss_tracking(body, users, channels)
    base = day_channel_frontmatter(row, day) + resolved
    return _decide_and_apply(conn, base, f"channel:{row.channel_id}", fallback_reasons, cfg)


def _assemble_channel_meta(
    conn: Connection[TupleRow],
    row: ChannelRow,
    cfg: TrailerConfig,
) -> tuple[bytes, bool, bool, TrailerDecision]:
    """Assemble bytes for ``/<conv-root>/<slug>/channel.md`` — channel metadata.

    Channel metadata is local projected data too: it goes stale after a
    rename/archive/tier change or a channel-list catch-up gap. So it is subject
    to the same trailer + ``notify_store`` gate as day/thread files (review
    P1-5: both reviewers flagged that this file was kernel-primed unconditionally
    and would serve stale metadata forever while disconnected). The natural
    staleness stream is ``channel-list``.
    """
    base = channel_meta_frontmatter(row).decode()
    return _decide_and_apply(conn, base, CHANNEL_LIST_STREAM, (), cfg)


def _assemble_thread(
    conn: Connection[TupleRow],
    row: ChannelRow,
    thread_ts: Decimal,
    tz: ZoneInfo,
    cfg: TrailerConfig,
) -> tuple[bytes, bool, bool, TrailerDecision] | None:
    """Assemble bytes for ``/.../<thread-slug>/thread.md``.

    Returns ``(bytes, had_trailer, had_unresolved_fallback, decision)`` or
    ``None``.
    """
    contents, reply_count = fetch_thread_chunks(conn, row.channel_id, thread_ts)
    if not contents:
        return None
    body = "\n".join(contents)
    users, channels = sql_resolvers_for(conn)
    resolved, fallback_reasons = resolve_with_miss_tracking(body, users, channels)
    base = thread_frontmatter(row, thread_ts, reply_count, tz) + resolved
    return _decide_and_apply(conn, base, f"channel:{row.channel_id}", fallback_reasons, cfg)


# ============================================================================
# Control-surface helpers
# ============================================================================


@dataclass(slots=True)
class _ControlWrite:
    """Per-open accumulation buffer for a ``_control/`` write handle."""

    path: str
    buffer: bytearray


@dataclass(frozen=True, slots=True)
class _ControlResult:
    """Outcome of firing a control action — drives the ``status`` record."""

    result: str
    channel: str | None = None
    job_id: str | None = None
    target: str | None = None


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

    def __init__(  # noqa: PLR0913  (keyword-only config + test-injection knobs)
        self,
        conn: Connection[TupleRow],
        local_tz: ZoneInfo,
        limiter: trio.CapacityLimiter,
        *,
        pool: ConnectionPool | None = None,
        callback_timeout_s: float = DEFAULT_CALLBACK_TIMEOUT_S,
        pg_health: PgHealth | None = None,
        notify_store: NotifyStoreFn | None = None,
        invalidate_inode: InvalidateInodeFn | None = None,
        stale_after_s: float = STALE_AFTER_DISCONNECT_S,
        trailer_enabled: bool = True,
        trailer_log: TrailerLog | None = None,
        now_fn: NowFn = _utcnow,
        originals_fetch: OriginalsFetchFn | None = None,
        channel_gaps_fetch: ChannelGapsFetchFn | None = None,
        workspace_gaps_fetch: WorkspaceGapsFetchFn | None = None,
        control_state: ControlState | None = None,
        control_refresh_workspace: ControlRefreshWorkspaceFn | None = None,
        control_refresh_channel: ControlRefreshChannelFn | None = None,
        control_blocked_channels_read: ControlBlockedChannelsReadFn | None = None,
        control_blocked_channels_list: ControlBlockedChannelsListFn | None = None,
        control_block_channel: ControlBlockChannelFn | None = None,
        control_unblock_channel: ControlUnblockChannelFn | None = None,
        control_backfill_channel: ControlBackfillChannelFn | None = None,
        control_probe_sweep: ControlProbeSweepFn | None = None,
        control_gaps_read: ControlGapsReadFn | None = None,
        control_probes_read: ControlProbesReadFn | None = None,
        control_refill_gap: ControlRefillGapFn | None = None,
        control_rerender_channel: ControlRerenderChannelFn | None = None,
    ) -> None:
        super().__init__()
        # ``conn`` is the inode-map's dedicated connection. In pool mode (``pool``
        # set), per-callback FUSE work borrows a separate conn from the pool; in
        # legacy conn-only mode (no pool — test fixtures), every callback runs
        # serially against ``conn`` via ``self._limiter``. The ``self._conn``
        # property resolves to the right one per call.
        self._inode_conn = conn
        self._pool = pool
        self._callback_timeout_s = callback_timeout_s
        # When set, surfaces ``/NO_POSTGRES`` while PG is down and lets
        # ``_run_sync`` mark PG down on OperationalError so subsequent
        # callbacks fast-fail with EIO instead of crashing the process.
        self._pg_health = pg_health
        self._tz = local_tz
        self._limiter = limiter
        self._notify_store: NotifyStoreFn = notify_store if notify_store is not None else _default_notify_store
        self._invalidate_inode: InvalidateInodeFn = (
            invalidate_inode if invalidate_inode is not None else _default_invalidate_inode
        )
        # Trailer-decision wiring (Sprint 3C). ``stale_after_s`` /
        # ``trailer_enabled`` come from ``ClientConfig``; ``trailer_log``
        # (when set) records one decision per read for bake-in false-positive
        # analysis; ``now_fn`` is injectable so tests can cross the staleness
        # window without sleeping.
        self._stale_after_s = stale_after_s
        self._trailer_enabled = trailer_enabled
        self._trailer_log = trailer_log
        self._now_fn = now_fn
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
        # ``channel.original.md`` ghost-file plumbing. When ``originals_fetch``
        # is None (most tests, and any deployment where the feature is gated
        # off), the ghost file is invisible and any lookup of it returns
        # ENOENT — the dispatch is centralised in ``_resolve_content`` /
        # ``_list_dir(for_lookup=True)``.
        self._originals_fetch = originals_fetch
        self._originals_cache: _BytesCache | None = _BytesCache() if originals_fetch is not None else None
        # Gaps ghost-file plumbing. Two caches because the access patterns
        # differ — per-channel gaps mirror originals (many keys), workspace
        # gaps is a single-cell cache. Both gated on the corresponding
        # fetcher: a missing fetcher hides the file (mirrors originals).
        # Cache TTL for the gaps views is intentionally LONG (10min). The
        # background warmer refreshes every 5min; the longer TTL means a
        # missed warmer cycle still serves slightly-stale-but-correct data
        # rather than ENOENT. Mirrors the "forensic snapshot, not a live
        # view" framing.
        self._channel_gaps_fetch = channel_gaps_fetch
        self._channel_gaps_cache: _BytesCache | None = (
            _BytesCache(max_entries=512, ttl_s=600.0) if channel_gaps_fetch is not None else None
        )
        self._workspace_gaps_fetch = workspace_gaps_fetch
        self._workspace_gaps_cache: _BytesCache | None = (
            _BytesCache(max_entries=1, ttl_s=600.0) if workspace_gaps_fetch is not None else None
        )
        # ``_control/`` write surface. The directory is visible (and writes
        # accepted) only when ``control_state`` is wired — the two refresh
        # fetchers may be None in tests that exercise listing/attrs without
        # firing. Write handles accumulate in ``_control_write_buffers`` keyed
        # by the high-range fh ``open`` assigns; ``release`` drains + fires.
        self._control_state = control_state
        self._control_refresh_workspace = control_refresh_workspace
        self._control_refresh_channel = control_refresh_channel
        self._control_blocked_channels_read = control_blocked_channels_read
        self._control_blocked_channels_list = control_blocked_channels_list
        self._control_block_channel = control_block_channel
        self._control_unblock_channel = control_unblock_channel
        self._control_backfill_channel = control_backfill_channel
        self._control_probe_sweep = control_probe_sweep
        self._control_gaps_read = control_gaps_read
        self._control_probes_read = control_probes_read
        self._control_refill_gap = control_refill_gap
        self._control_rerender_channel = control_rerender_channel
        self._control_write_buffers: dict[int, _ControlWrite] = {}
        self._control_write_lock = threading.Lock()
        self._control_fh_seq = 0

    # ------------------------------------------------------------------
    # Public surface used by the health subscriber + tests
    # ------------------------------------------------------------------

    @property
    def _conn(self) -> Connection[TupleRow]:
        """Resolve the connection sync FUSE code should use for queries.

        Pool mode: the per-callback borrowed connection set by
        :meth:`_run_sync` lives in the ``borrowed_fuse_conn`` ContextVar
        (declared in ``fuse_v2_helpers`` so both classes can read it).
        Outside a callback (or in conn-only test mode), fall back to the
        inode connection — the legacy single-conn-with-limiter behaviour.
        """
        borrowed = borrowed_fuse_conn.get()
        if borrowed is not None:
            return borrowed
        return self._inode_conn

    async def _run_sync(self, sync_fn: Callable[[], _TSync]) -> _TSync:  # noqa: C901  (multi-branch error-handling hub; flattens cleaner than splitting)
        """Dispatch ``sync_fn`` to a worker thread under the right isolation.

        Pool mode (``self._pool`` set): borrow a connection from the pool,
        pin it to the ``_borrowed_fuse_conn`` ContextVar for the duration of
        the call so the sync body's ``self._conn`` resolves to it, run the
        body in a worker thread with two layers of timeout protection:

        1. *PG ``statement_timeout``* (set by the pool's factory in
           ``__main__``) catches slow SQL — the dominant risk under heavy
           projector contention. The statement aborts, exception propagates
           back through the thread normally, conn returns to the pool.
        2. *trio ``fail_after``* catches the remaining failure mode:
           pure-Python hangs (CPU loops, bad data triggering an infinite
           regex, etc.). Cancellation raises ``TooSlowError`` in this task;
           the worker thread is *abandoned* (it keeps running pure-Python
           work but any further DB op fails because we close its conn).
           Caller sees ``FUSEError(EIO)`` — no kernel page stays locked
           waiting for a never-completing upcall.

        Conn-only mode (no pool — tests, also the legacy v1 shape): serial
        execution under ``self._limiter`` against ``self._inode_conn``. One
        callback at a time; preserves the existing test contract. No
        per-callback timeout — tests are deterministic and would only see
        spurious cancellations under that policy.
        """
        # PG-health fast-fail. When the local Postgres goes down — most
        # commonly during a ``game-mode on`` cycle that stops
        # claude-hooks-postgres.service — there's no point acquiring a
        # pool conn that will immediately raise OperationalError. Surface
        # EIO directly; the ``/NO_POSTGRES`` file (handled in the
        # callback short-circuits below) tells the user what's wrong.
        if self._pg_health is not None and self._pg_health.is_down():
            raise pyfuse3.FUSEError(errno.EIO)

        if self._pool is None:
            try:
                return await trio.to_thread.run_sync(sync_fn, limiter=self._limiter)
            except pyfuse3.FUSEError:
                # Intentional FS-level error code from sync_fn — let through.
                raise
            except psycopg.OperationalError as exc:
                self._maybe_mark_pg_down(exc)
                raise pyfuse3.FUSEError(errno.EIO) from None
            except Exception:
                log.exception("FUSE sync body (conn-only) raised; returning EIO")
                raise pyfuse3.FUSEError(errno.EIO) from None
        budget = _current_callback_budget.get() or self._callback_timeout_s
        try:
            # IMPORTANT: pool acquire must also be inside a timeout. If all
            # pool slots are held by worker threads stuck in PG / kernel
            # (the host-level FUSE wedge documented in BACKLOG), an
            # unbounded acquire blocks every subsequent callback for as
            # long as the wedge persists. Found 2026-06-22 via the new
            # slow-op logging: an unprotected acquire let one read sit
            # 115s on a wedge before completing. Use the same per-callback
            # budget for the whole borrow → run → release cycle.
            with trio.fail_after(budget):
                conn = await self._pool.acquire()
        except trio.TooSlowError:
            log.warning("pool acquire timed out after %.1fs; returning EIO", budget)
            raise pyfuse3.FUSEError(errno.EIO) from None
        except psycopg.OperationalError as exc:
            # Pool factory couldn't open a fresh conn — PG socket is gone.
            self._maybe_mark_pg_down(exc)
            raise pyfuse3.FUSEError(errno.EIO) from None
        except Exception:
            log.exception("FUSE pool acquire raised; returning EIO")
            raise pyfuse3.FUSEError(errno.EIO) from None
        token = borrowed_fuse_conn.set(conn)
        released = False
        budget = _current_callback_budget.get() or self._callback_timeout_s
        try:
            with trio.fail_after(budget):
                return await trio.to_thread.run_sync(sync_fn, abandon_on_cancel=True)
        except trio.TooSlowError:
            # Close the conn so any SQL the abandoned thread is still running
            # aborts; the pool slot is freed for the next caller. The thread
            # itself keeps running pure-Python work but can't touch the DB.
            log.warning(
                "FUSE callback exceeded %.1fs timeout — returning EIO and discarding the borrowed conn",
                budget,
            )
            released = True
            with trio.CancelScope(shield=True):
                await self._pool.release(conn, discard=True)
            raise pyfuse3.FUSEError(errno.EIO) from None
        except pyfuse3.FUSEError:
            # Intentional FS-level error code from sync_fn — let through, but
            # the conn might still be valid so return it (don't discard).
            raise
        except psycopg.OperationalError as exc:
            # PG socket vanished mid-query. Mark down so subsequent callbacks
            # fast-fail; discard the bad conn (don't return it to the pool).
            self._maybe_mark_pg_down(exc)
            released = True
            with trio.CancelScope(shield=True):
                await self._pool.release(conn, discard=True)
            raise pyfuse3.FUSEError(errno.EIO) from None
        except Exception:
            # Catch-all: any unhandled exception in the FUSE callback should
            # become EIO, never propagate out of the process. The conn might
            # have been left in an inconsistent state (open cursor, half-
            # processed result) so discard rather than risk reusing it.
            log.exception("FUSE callback raised unexpected error; returning EIO")
            released = True
            with trio.CancelScope(shield=True):
                await self._pool.release(conn, discard=True)
            raise pyfuse3.FUSEError(errno.EIO) from None
        finally:
            borrowed_fuse_conn.reset(token)
            if not released:
                # SHIELD the release: an outer ``_callback_guard`` fail_after
                # (which always starts strictly before the inner fail_after so
                # its deadline fires first, review 2026-07-17) delivers
                # ``trio.Cancelled`` — not ``TooSlowError`` — at whatever await
                # follows. Without the shield, that Cancelled preempts
                # ``pool.release`` at its first checkpoint, the semaphore never
                # runs release(), and the slot is permanently leaked. Four
                # such leaks (max_size=4) wedge the whole mount with no log
                # line. Repro pinned by
                # ``test_run_sync_shields_release_from_outer_cancel``.
                with trio.CancelScope(shield=True):
                    await self._pool.release(conn)

    def _maybe_mark_pg_down(self, exc: BaseException) -> None:
        """Tell ``PgHealth`` PG is down (if wired)."""
        if self._pg_health is not None:
            reason = str(exc).splitlines()[0][:160]
            self._pg_health.mark_down(reason=reason)

    @contextlib.contextmanager
    def _callback_guard(
        self,
        op: str,
        *,
        inode: int | None = None,
        path: str | None = None,
    ) -> Iterator[None]:
        """Wrap a FUSE callback body so any uncaught exception becomes EIO,
        and the WHOLE body honours a single ``callback_timeout_s`` budget.

        Opens a :func:`fuse_op` logging scope at the same time so every
        log line inside this callback (including from worker threads,
        helpers, ``psycopg``) carries ``req_id``, ``op``, ``inode``, and
        ``path`` — making it trivial to grep one logical operation out
        of a busy journal and trace it end-to-end.

        The contract every callback honours after this:

        - Return valid data within ``callback_timeout_s``, or
        - Raise ``FUSEError(<errno>)`` (ENOENT, EIO, ENOTDIR, …) intentionally.

        Any other exception — psycopg failure, KeyError from a render edge
        case, AttributeError after a refactor regression, a slow stage,
        anything — gets logged with full traceback (including the FUSE scope
        context) and converted to ``EIO``. The process never dies from a
        single bad callback.

        The budget wraps the OUTER callback body (this was the 2026-06-24
        wedge: ``_run_sync`` had its own fail_after but the read method did
        work AFTER ``_run_sync`` returned — the ``notify_store`` call past
        that inner guard had unbounded time. One outer budget makes every
        callback either succeed or surface EIO within
        ``callback_timeout_s``, no matter which stage stalls).
        """
        resolved_path = path
        if resolved_path is None and inode is not None:
            # Callers usually pass just inode; look up the path to keep the
            # per-path budget check working. Cheap in-memory dict lookup.
            resolved_path = self._inodes.get_path(inode)
        budget = (
            CONTROL_CALLBACK_TIMEOUT_S
            if resolved_path is not None and resolved_path.startswith(f"/{CONTROL_DIR}/")
            else self._callback_timeout_s
        )
        # Publish so ``_run_sync``'s own inner ``trio.fail_after`` guards
        # inherit the same budget (otherwise the outer 15s is overridden by
        # the inner 1s ``self._callback_timeout_s`` and we still EIO fast).
        budget_token = _current_callback_budget.set(budget)
        with fuse_op(op, inode=inode, path=path):
            try:
                with trio.fail_after(budget):
                    yield
            except pyfuse3.FUSEError:
                raise
            except trio.TooSlowError:
                log.warning("callback exceeded %.1fs budget; returning EIO", budget)
                raise pyfuse3.FUSEError(errno.EIO) from None
            except psycopg.OperationalError as exc:
                # Pre-``_run_sync`` PG access (e.g. inode lookup on cache miss)
                # can still hit a dead socket. Mark down so subsequent callbacks
                # fast-fail; surface as EIO to this caller.
                self._maybe_mark_pg_down(exc)
                log.warning("hit PG OperationalError: %s", exc)
                raise pyfuse3.FUSEError(errno.EIO) from None
            except Exception:
                log.exception("unexpected error; returning EIO")
                raise pyfuse3.FUSEError(errno.EIO) from None
            finally:
                _current_callback_budget.reset(budget_token)

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

    def control_file_attr_for_test(self, path: str, inode: int) -> pyfuse3.EntryAttributes | None:
        """Test-only accessor for control-file attrs."""
        return self._control_file_attr(path, inode)

    def control_read_for_test(self, path: str) -> bytes | None:
        """Test-only accessor for the control read-path bytes."""
        return self._control_read_bytes(path)

    def control_write_buffer_count(self) -> int:
        """Test-only: number of live per-fh control write buffers (leak check)."""
        with self._control_write_lock:
            return len(self._control_write_buffers)

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
    # Control surface (`_control/`)
    # ------------------------------------------------------------------

    @property
    def _control_enabled(self) -> bool:
        return self._control_state is not None

    def _control_file_attr(self, path: str, inode: int) -> pyfuse3.EntryAttributes | None:
        """Attrs for a ``_control/<name>`` file, or ``None`` if not one.

        ``status`` is read-only JSON sized to its current body; the refresh
        triggers are write-to-fire (0o644) and read back empty (size 0,
        Plan-9 ctl style). Control attrs are never cached (timeout 0) so the
        kernel always re-checks ``status``'s changing size.
        """
        parts = parse_path(path)
        if len(parts) != 2 or parts[0] != CONTROL_DIR:
            return None
        name = parts[1]
        if name == CONTROL_STATUS:
            return _make_file_attr(inode, len(self._control_status_bytes()), mode=stat.S_IFREG | 0o444)
        if name == CONTROL_BLOCKED_CHANNELS:
            return _make_file_attr(
                inode,
                len(self._control_blocked_channels_bytes()),
                mode=stat.S_IFREG | 0o644,
            )
        if name == CONTROL_GAPS:
            # Size reported as 0 — st_size is a hint, and computing the real
            # size here would fetch /gap-candidates (a ~2s SQL query), which
            # busts the FUSE per-callback budget on every getattr/lookup and
            # DoSes the server via cascade amplification. `cat`/`grep`/`wc -c`
            # all read until EOF regardless of st_size; the fetch happens once
            # in the `read` callback.
            return _make_file_attr(inode, 0, mode=stat.S_IFREG | 0o444)
        if name == CONTROL_PROBES:
            # Same reasoning as CONTROL_GAPS above.
            return _make_file_attr(inode, 0, mode=stat.S_IFREG | 0o444)
        if name in CONTROL_WRITABLE:
            return _make_file_attr(inode, 0, mode=stat.S_IFREG | 0o644)
        return None

    def _control_status_bytes(self) -> bytes:
        return self._control_state.render() if self._control_state is not None else b""

    def _control_blocked_channels_bytes(self) -> bytes:
        if self._control_blocked_channels_read is None:
            return b'{"error":"server_unavailable"}\n'
        return self._control_blocked_channels_read()

    def _control_gaps_bytes(self) -> bytes:
        if self._control_gaps_read is None:
            return b"# error\tserver_unavailable\n"
        return self._control_gaps_read()

    def _control_probes_bytes(self) -> bytes:
        if self._control_probes_read is None:
            return b'{"error":"server_unavailable"}\n'
        return self._control_probes_read()

    def _control_read_bytes(self, path: str) -> bytes | None:
        """Read-side bytes for a ``_control/<name>`` file, or ``None``."""
        parts = parse_path(path)
        if len(parts) != 2 or parts[0] != CONTROL_DIR:
            return None
        name = parts[1]
        if name == CONTROL_STATUS:
            return self._control_status_bytes()
        if name == CONTROL_BLOCKED_CHANNELS:
            return self._control_blocked_channels_bytes()
        if name == CONTROL_GAPS:
            return self._control_gaps_bytes()
        if name == CONTROL_PROBES:
            return self._control_probes_bytes()
        if name in CONTROL_WRITABLE:
            return b""
        return None

    def _alloc_control_write(self, path: str) -> int:
        with self._control_write_lock:
            self._control_fh_seq += 1
            fh = _CONTROL_FH_BASE + self._control_fh_seq
            self._control_write_buffers[fh] = _ControlWrite(path=path, buffer=bytearray())
        return fh

    def _open_control(self, path: str, flags: int, inode: int) -> pyfuse3.FileInfo:
        """Open dispatch for a ``_control/<name>`` file.

        Write-mode opens of the two refresh triggers allocate a write handle;
        write-mode opens of ``status`` (or any other control name) are EROFS.
        Reads use ``direct_io`` so ``status`` is never served from a stale
        kernel page cache.
        """
        parts = parse_path(path)
        name = parts[1] if len(parts) == 2 else ""
        writing = (flags & os.O_ACCMODE) != os.O_RDONLY
        if writing:
            if name not in CONTROL_WRITABLE:
                raise pyfuse3.FUSEError(errno.EROFS)
            fh = self._alloc_control_write(path)
            fi = pyfuse3.FileInfo()
            fi.fh = fh  # pyright: ignore[reportAttributeAccessIssue]
            fi.direct_io = True  # pyright: ignore[reportAttributeAccessIssue]
            fi.keep_cache = False  # pyright: ignore[reportAttributeAccessIssue]
            return fi
        fi = pyfuse3.FileInfo()
        fi.fh = inode  # pyright: ignore[reportAttributeAccessIssue]
        fi.direct_io = True  # pyright: ignore[reportAttributeAccessIssue]
        fi.keep_cache = False  # pyright: ignore[reportAttributeAccessIssue]
        return fi

    async def _fire_control(self, entry: _ControlWrite) -> None:  # noqa: C901 - control-file dispatch hub.
        """Drain a finished control write and trigger the matching action.

        The action (slug resolution + HTTP POST) runs in a worker thread via
        ``_run_sync`` so the FUSE event loop stays responsive and the call is
        bounded by the per-callback budget. A timeout/EIO from ``_run_sync`` is
        recorded as ``server_unavailable`` — the write already succeeded at the
        kernel level, so we never propagate the failure back to ``release``.
        """
        if self._control_state is None:
            return
        name = parse_path(entry.path)[1] if len(parse_path(entry.path)) == 2 else ""
        data = bytes(entry.buffer)
        if name == CONTROL_REFRESH_CHANNELS:
            if not data.strip():
                return
            result = await self._control_action_or_unavailable(self._do_workspace_refresh)
            self._control_state.record_workspace(result.result)
        elif name == CONTROL_REFRESH_CHANNEL:
            await self._fire_refresh_channel(data)
        elif name == CONTROL_BLOCKED_CHANNELS:
            await self._fire_block_toggle(data)
        elif name == CONTROL_BACKFILL_CHANNEL:
            await self._fire_backfill(data)
        elif name == CONTROL_REFILL_GAP:
            await self._fire_refill_gap(data)
        elif name in {CONTROL_PROBE_SWEEP, CONTROL_PROBE_SWEEP_JOB, CONTROL_PROBE_SWEEP_TARGET}:
            await self._fire_probe_control(name, data)
        elif name == CONTROL_RERENDER_CHANNEL:
            token = data.decode("utf-8", errors="replace").strip()
            if not token:
                return
            result = await self._fire_rerender(token)
            self._control_state.record_rerender(result.channel or token, result.result)

    async def _fire_probe_control(self, name: str, data: bytes) -> None:
        if name == CONTROL_PROBE_SWEEP:
            await self._fire_probe_sweep(data)
        elif name == CONTROL_PROBE_SWEEP_JOB:
            await self._fire_probe_sweep_job(data)
        elif name == CONTROL_PROBE_SWEEP_TARGET:
            await self._fire_probe_sweep_target(data)

    async def _fire_refresh_channel(self, data: bytes) -> None:
        assert self._control_state is not None
        token = data.decode("utf-8", errors="replace").strip()
        if not token:
            return
        result = await self._control_action_or_unavailable(lambda: self._do_channel_refresh(token))
        self._control_state.record_channel(result.channel or token, result.result)

    async def _fire_block_toggle(self, data: bytes) -> None:
        assert self._control_state is not None
        token, reason = self._parse_control_channel_reason(data)
        if not token:
            return
        result = await self._control_action_or_unavailable(lambda: self._do_block_toggle(token, reason))
        if result.result == "unblocked":
            self._control_state.record_unblock(result.channel or token, result.result)
        else:
            self._control_state.record_block(result.channel or token, result.result)

    async def _fire_backfill(self, data: bytes) -> None:
        assert self._control_state is not None
        token = data.decode("utf-8", errors="replace").strip()
        if not token:
            return
        result = await self._control_action_or_unavailable(lambda: self._do_backfill(token))
        self._control_state.record_backfill(result.channel or token, result.result)

    async def _fire_refill_gap(self, data: bytes) -> None:
        assert self._control_state is not None
        text = data.decode("utf-8", errors="replace")
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            parsed = self._parse_refill_gap_line(line)
            if parsed is None:
                channel_hint = line.split(None, 1)[0] if line.split(None, 1) else line
                self._control_state.record_refill_gap(channel_hint, "bad_request")
                continue
            token, oldest, latest = parsed
            result = await self._control_action_or_unavailable(
                lambda token=token, oldest=oldest, latest=latest: self._do_refill_gap(token, oldest, latest)
            )
            self._control_state.record_refill_gap(
                result.channel or token,
                result.result,
                oldest_ts=oldest,
                latest_ts=latest,
            )

    async def _fire_probe_sweep(self, data: bytes) -> None:
        assert self._control_state is not None
        if not data.strip():
            return
        result = await self._control_action_or_unavailable(lambda: self._do_probe_sweep(None, None))
        self._control_state.record_probe_sweep(result.result, job_id=result.job_id, target=result.target)

    async def _fire_probe_sweep_job(self, data: bytes) -> None:
        assert self._control_state is not None
        text = data.decode("utf-8", errors="replace").strip()
        if not text:
            return
        parts = text.split()
        if len(parts) != 1:
            self._control_state.record_probe_sweep(result_for_status(400))
            return
        job_id = parts[0]
        result = await self._control_action_or_unavailable(lambda: self._do_probe_sweep(job_id, None))
        self._control_state.record_probe_sweep(result.result, job_id=result.job_id, target=result.target)

    async def _fire_probe_sweep_target(self, data: bytes) -> None:
        assert self._control_state is not None
        text = data.decode("utf-8", errors="replace").strip()
        if not text:
            return
        parts = text.split()
        if len(parts) != 2:
            self._control_state.record_probe_sweep(result_for_status(400))
            return
        job_id, target = parts
        result = await self._control_action_or_unavailable(lambda: self._do_probe_sweep(job_id, target))
        self._control_state.record_probe_sweep(result.result, job_id=result.job_id, target=result.target)

    async def _control_action_or_unavailable(self, fn: Callable[[], _ControlResult]) -> _ControlResult:
        try:
            return await self._run_sync(fn)
        except pyfuse3.FUSEError:
            return _ControlResult(result="server_unavailable")

    def _do_workspace_refresh(self) -> _ControlResult:
        if self._control_refresh_workspace is None:
            return _ControlResult(result="server_unavailable")
        return _ControlResult(result=result_for_status(self._control_refresh_workspace()))

    def _do_channel_refresh(self, token: str) -> _ControlResult:
        channel_id = self._resolve_control_channel(token)
        if channel_id is None:
            return _ControlResult(result="unknown_channel", channel=token)
        if self._control_refresh_channel is None:
            return _ControlResult(result="server_unavailable", channel=channel_id)
        return _ControlResult(result=result_for_status(self._control_refresh_channel(channel_id)), channel=channel_id)

    def _do_block_toggle(self, token: str, reason: str | None) -> _ControlResult:
        channel_id = self._resolve_control_channel_or_literal_id(token)
        if channel_id is None:
            return _ControlResult(result="unknown_channel", channel=token)
        if (
            self._control_blocked_channels_list is None
            or self._control_block_channel is None
            or self._control_unblock_channel is None
        ):
            return _ControlResult(result="server_unavailable", channel=channel_id)
        blocked = self._control_blocked_channels_list()
        if channel_id in blocked:
            code = self._control_unblock_channel(channel_id)
            return _ControlResult(
                result="unblocked" if code == 200 else result_for_status(code),
                channel=channel_id,
            )
        code = self._control_block_channel(channel_id, reason)
        return _ControlResult(result="blocked" if code == 200 else result_for_status(code), channel=channel_id)

    def _do_backfill(self, token: str) -> _ControlResult:
        channel_id = self._resolve_control_channel_or_literal_id(token)
        if channel_id is None:
            return _ControlResult(result="unknown_channel", channel=token)
        if self._control_backfill_channel is None:
            return _ControlResult(result="server_unavailable", channel=channel_id)
        code, message = self._control_backfill_channel(channel_id)
        if code == 202:
            return _ControlResult(result="queued", channel=channel_id)
        if code == 409 and message == "blocked":
            return _ControlResult(result="blocked", channel=channel_id)
        return _ControlResult(result=result_for_status(code), channel=channel_id)

    def _do_refill_gap(self, token: str, oldest: float, latest: float) -> _ControlResult:
        channel_id = self._resolve_control_channel(token)
        if channel_id is None:
            return _ControlResult(result="unknown_channel", channel=token)
        if self._control_refill_gap is None:
            return _ControlResult(result="server_unavailable", channel=channel_id)
        return _ControlResult(result=self._control_refill_gap(channel_id, oldest, latest), channel=channel_id)

    def _do_probe_sweep(self, job_id: str | None, target: str | None) -> _ControlResult:
        if self._control_probe_sweep is None:
            return _ControlResult(result="server_unavailable", job_id=job_id, target=target)
        code, message = self._control_probe_sweep(job_id, target)
        result = "unknown_job" if code == 400 and message == "unknown_job" else result_for_status(code)
        return _ControlResult(result=result, job_id=job_id, target=target)

    async def _fire_rerender(self, token: str) -> _ControlResult:
        """Resolve a rerender token and hand it to the background consumer.

        Resolution (a DB read) runs in a worker thread under the callback
        budget; the enqueue itself runs here on the trio event loop because the
        injected ``control_rerender_channel`` is a trio memory-channel send that
        is not thread-safe. The actual rerender is heavy (snapshot fetch + apply)
        and runs off-budget in the consumer, so this only ever records ``queued``
        / ``busy`` / ``unknown_channel`` — the consumer overwrites ``status``
        with the final verb when it finishes.
        """
        if self._control_rerender_channel is None:
            return _ControlResult(result="server_unavailable")
        try:
            channel_id = await self._run_sync(lambda: self._resolve_control_channel(token))
        except pyfuse3.FUSEError:
            return _ControlResult(result="server_unavailable")
        if channel_id is None:
            return _ControlResult(result="unknown_channel", channel=token)
        accepted = self._control_rerender_channel(channel_id)
        return _ControlResult(result="queued" if accepted else "busy", channel=channel_id)

    def _resolve_control_channel(self, token: str) -> str | None:
        """Resolve a written token (channel id or slug or name) to a channel id.

        A literal channel id wins (exact match in ``channels``). Otherwise the
        token is tried as a slug across every conv-root (hidden allowed, so a
        hidden channel is still resolvable by its known slug — ``blocked`` is
        excluded from slug assignment by design). Finally, as a fallback, the
        token is matched against ``channels.name`` across all tiers — so the
        operator can address a currently-blocked channel by its name (the
        motivating case: ``echo metrics > _control/blocked_channels`` to
        UNblock a channel whose slug was suppressed because it was blocked).
        """
        conn = self._conn
        with conn.cursor() as cur:
            _ = cur.execute("SELECT 1 FROM channels WHERE channel_id = %s", (token,))
            if cur.fetchone() is not None:
                return token
        for conv_root in CONV_ROOTS:
            row = fetch_channel_by_slug(conn, conv_root, token, allow_hidden=True)
            if row is not None:
                return row.channel_id
        # Name-match fallback (blocked-channel-friendly). Slug assignment
        # skips blocked rows so the loop above can't reach them, but the
        # underlying ``channels.name`` is still present. Unique-name
        # collisions here are ambiguous by construction — first-by-id wins,
        # matching the ordering the slug loop would use.
        with conn.cursor() as cur:
            _ = cur.execute("SELECT channel_id FROM channels WHERE name = %s ORDER BY channel_id LIMIT 1", (token,))
            row = cur.fetchone()
            if row is not None:
                return str(row[0])
        return None

    def _resolve_control_channel_or_literal_id(self, token: str) -> str | None:
        resolved = self._resolve_control_channel(token)
        if resolved is not None:
            return resolved
        clean = token.strip()
        if len(clean) >= 2 and clean[0] in {"C", "D", "G"} and clean.isalnum():
            return clean
        return None

    def _parse_control_channel_reason(self, data: bytes) -> tuple[str, str | None]:
        text = data.decode("utf-8", errors="replace").strip()
        if not text:
            return "", None
        parts = text.split(None, 1)
        reason = parts[1].strip() if len(parts) == 2 and parts[1].strip() else None
        return parts[0], reason

    def _parse_refill_gap_line(self, line: str) -> tuple[str, float, float] | None:
        parts = line.split()
        if len(parts) != 3:
            return None
        token, oldest_text, latest_text = parts
        try:
            oldest = float(oldest_text)
            latest = float(latest_text)
        except ValueError:
            return None
        if oldest < 0.0 or latest <= oldest:
            return None
        return token, oldest, latest

    # ------------------------------------------------------------------
    # Path classification and dispatch
    # ------------------------------------------------------------------

    def _list_dir(self, path: str, *, for_lookup: bool = False) -> list[tuple[str, bool]]:  # noqa: C901  (path-depth dispatch hub)
        parts = parse_path(path)
        depth = len(parts)
        if depth == 0:
            # ``_workspace/`` is a sibling of the conv-roots — a discoverable
            # namespace for read-only diagnostic surfaces. Listed on both the
            # readdir AND lookup paths so ``ls /views/slack-split`` shows it.
            roots: list[tuple[str, bool]] = [(d, True) for d in CONV_ROOTS]
            if self._workspace_gaps_fetch is not None:
                roots.append((WORKSPACE_DIR, True))
            if self._control_enabled:
                roots.append((CONTROL_DIR, True))
            return roots

        # Top-level ``_workspace/`` namespace. Currently contains only
        # ``gaps.md``; future read-only diagnostic ghost files land here too.
        if parts[0] == WORKSPACE_DIR:
            if depth == 1 and self._workspace_gaps_fetch is not None:
                return [(GAPS_MD, False)]
            return []

        # Top-level ``_control/`` write surface (Plan-9 ctl/status).
        if parts[0] == CONTROL_DIR:
            if depth == 1 and self._control_enabled:
                return [
                    (CONTROL_GAPS, False),
                    (CONTROL_PROBES, False),
                    (CONTROL_REFRESH_CHANNELS, False),
                    (CONTROL_REFRESH_CHANNEL, False),
                    (CONTROL_BLOCKED_CHANNELS, False),
                    (CONTROL_BACKFILL_CHANNEL, False),
                    (CONTROL_REFILL_GAP, False),
                    (CONTROL_PROBE_SWEEP, False),
                    (CONTROL_PROBE_SWEEP_JOB, False),
                    (CONTROL_PROBE_SWEEP_TARGET, False),
                    (CONTROL_RERENDER_CHANNEL, False),
                    (CONTROL_STATUS, False),
                ]
            return []

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
            result_root: list[tuple[str, bool]] = [(CHANNEL_MD, False)]
            # gaps.md is a ghost: lookup resolves it, readdir does not list
            # it (same pattern as channel.original.md — keep recursive ``rg``
            # off the slow path).
            if for_lookup and self._channel_gaps_fetch is not None:
                result_root.append((GAPS_MD, False))
            result_root.extend((m, True) for m in months)
            return result_root

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
            # Ghost file: surfaced ONLY on the lookup path. readdir omits it
            # (no recursive scan should hit the events-replay slow path), but
            # a direct ``cat /…/channel.original.md`` must succeed. Gated on
            # ``originals_fetch`` being wired — without a fetcher, the file
            # cannot render, so don't pretend it exists.
            if for_lookup and self._originals_fetch is not None:
                result.append((CHANNEL_ORIGINAL_MD, False))
            for slug in dedup_thread_slug_map(parents):
                result.append((slug, True))
            return result

        if depth == 5:
            return [(THREAD_MD, False)]

        return []

    def _is_dir(self, path: str) -> bool:  # noqa: C901  (path-depth dispatch hub)
        parts = parse_path(path)
        depth = len(parts)
        if depth == 0:
            return True
        if parts[0] == WORKSPACE_DIR:
            return depth == 1
        if parts[0] == CONTROL_DIR:
            return depth == 1 and self._control_enabled
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
            return parts[2] not in (CHANNEL_MD, GAPS_MD) and is_valid_month(parts[2])
        if depth == 4:
            return is_valid_month(parts[2]) and is_valid_day(parts[3])
        if depth == 5:
            return parts[4] not in (CHANNEL_MD, CHANNEL_ORIGINAL_MD)
        if depth == 6:
            return parts[5] != THREAD_MD
        return False

    def _resolve_content(self, path: str) -> tuple[bytes, bool, bool] | None:
        """Return ``(bytes, had_trailer, had_unresolved_fallback)`` or ``None``.

        The two booleans together drive the ``notify_store`` gating in
        ``read()``. Per RFC: trailer-bearing OR fallback-bearing bytes must
        NEVER enter the kernel page cache via ``notify_store``. This is the
        size/attr-path accessor (``getattr`` / ``lookup`` / ``opendir``); the
        read path uses :meth:`_resolve_decision` to also obtain the loggable
        decision record.
        """
        resolved = self._resolve_decision(path)
        if resolved is None:
            return None
        content, had_trailer, had_fallback, _decision = resolved
        return content, had_trailer, had_fallback

    def _resolve_decision(  # noqa: C901  (path-depth dispatch hub)
        self,
        path: str,
        now: datetime | None = None,
    ) -> tuple[bytes, bool, bool, TrailerDecision] | None:
        """Assemble bytes + the trailer decision for ``path``, or ``None``.

        ``now`` defaults to ``self._now_fn()`` so attr-path callers don't have
        to thread a clock; the read path passes its own ``now`` so the logged
        decision timestamp matches the served bytes.
        """
        parts = parse_path(path)
        depth = len(parts)

        # ``/_workspace/gaps.md`` — workspace-wide gaps summary, fetched
        # from the slurper-server's events table. Same cache-then-fetch
        # pattern as the channel-level gaps below. No mention resolution
        # needed — the server returns plain markdown.
        if (
            depth == 2
            and parts[0] == WORKSPACE_DIR
            and parts[1] == GAPS_MD
            and self._workspace_gaps_fetch is not None
            and self._workspace_gaps_cache is not None
        ):
            return self._assemble_workspace_gaps()

        if depth < 3 or parts[0] not in CONV_ROOTS:
            return None

        row = fetch_channel_by_slug(self._conn, parts[0], parts[1], allow_hidden=True)
        if row is None:
            return None

        cfg = TrailerConfig(
            now=now if now is not None else self._now_fn(),
            stale_after_s=self._stale_after_s,
            trailer_enabled=self._trailer_enabled,
        )

        # /<conv-root>/<slug>/channel.md — channel metadata. Subject to the
        # same staleness trailer + notify_store gate as day/thread files
        # (review P1-5).
        if depth == 3 and parts[2] == CHANNEL_MD:
            return _assemble_channel_meta(self._conn, row, cfg)

        # /<conv-root>/<slug>/gaps.md  (ghost diagnostic file)
        # Lists UTC days with no message events on this channel that are
        # bounded by days with events. Lookup-only (not in readdir) so a
        # recursive ``rg`` doesn't trigger the events aggregation on the
        # server. Empty body → ENOENT (no gaps to show).
        if (
            depth == 3
            and parts[2] == GAPS_MD
            and self._channel_gaps_fetch is not None
            and self._channel_gaps_cache is not None
        ):
            return self._assemble_channel_gaps(row.channel_id)

        # /<conv-root>/<slug>/<YYYY-MM>/<DD>/channel.md
        if depth == 5 and parts[4] == CHANNEL_MD:
            day = parse_day_date(parts[2], parts[3])
            if day is None:
                return None
            return _assemble_channel_day(self._conn, row, day, self._tz, cfg)

        # /<conv-root>/<slug>/<YYYY-MM>/<DD>/channel.original.md  (ghost file)
        # Reached only via direct lookup — readdir does not list it. Renders
        # by replaying the cluster's events table; cached in-process for the
        # duration of one stat+read pair so a single ``cat`` doesn't replay
        # the events twice.
        if (
            depth == 5
            and parts[4] == CHANNEL_ORIGINAL_MD
            and self._originals_fetch is not None
            and self._originals_cache is not None
        ):
            day = parse_day_date(parts[2], parts[3])
            if day is None:
                return None
            return self._assemble_channel_original_day(row, day, cfg)

        # /<conv-root>/<slug>/<YYYY-MM>/<DD>/<thread-slug>/thread.md
        if depth == 6 and parts[5] == THREAD_MD:
            day = parse_day_date(parts[2], parts[3])
            if day is None:
                return None
            thread_ts = self._resolve_thread_ts(row.channel_id, day, parts[4])
            if thread_ts is None:
                return None
            return _assemble_thread(self._conn, row, thread_ts, self._tz, cfg)

        return None

    def _resolve_thread_ts(self, channel_id: str, day: date, thread_slug: str) -> Decimal | None:
        parents = fetch_day_thread_parents(self._conn, channel_id, day, self._tz)
        slug_map = dedup_thread_slug_map(parents)
        # If the slug map missed (no thread by that name yet under this day),
        # no thread file exists.
        return slug_map.get(thread_slug)

    def _assemble_channel_original_day(
        self,
        row: ChannelRow,
        day: date,
        cfg: TrailerConfig,
    ) -> tuple[bytes, bool, bool, TrailerDecision] | None:
        """Assemble bytes for ``channel.original.md`` (events-replay view).

        Fetches the raw originals body from the slurper-server (cached for a
        short window so stat+read share one fetch), then runs the standard
        mention-resolver pipeline against the client's local users/channels
        tables so display names render as for ``channel.md``.

        Returns ``None`` when there's nothing to render (no message events in
        the day on the server side), making the file ``ENOENT``-like — a
        ``lookup`` of ``channel.original.md`` on an empty day succeeds for
        the *parent* but yields no resolved attrs, so ``lookup`` raises
        ``ENOENT`` to userspace. Same shape as the channel.md empty-day case.
        """
        assert self._originals_fetch is not None
        assert self._originals_cache is not None
        day_iso = day.isoformat()
        cached = self._originals_cache.get(row.channel_id, day_iso)
        if cached is None:
            day_start_local = datetime.combine(day, datetime.min.time()).replace(tzinfo=self._tz)
            day_end_local = day_start_local + timedelta(days=1)
            from_epoch = day_start_local.timestamp()
            to_epoch = day_end_local.timestamp()
            # Any exception (httpx network error, server 5xx, etc.) propagates
            # to ``_callback_guard``, which logs full traceback and surfaces
            # EIO — same path as any other callback-stage failure.
            raw = self._originals_fetch(row.channel_id, from_epoch, to_epoch)
            self._originals_cache.put(row.channel_id, day_iso, content=raw)
            cached = raw
        if not cached:
            return None
        users, channels = sql_resolvers_for(self._conn)
        resolved, fallback_reasons = resolve_with_miss_tracking(cached.decode("utf-8"), users, channels)
        # Same frontmatter as channel.md plus an originals marker line so a
        # human eyeballing the file immediately knows what they're reading.
        frontmatter = day_channel_frontmatter(row, day)
        marker = "<!-- originals view: replay of events log; edits/deletes annotated inline -->\n\n"
        base = frontmatter + marker + resolved
        return _decide_and_apply(self._conn, base, f"channel:{row.channel_id}", fallback_reasons, cfg)

    def _assemble_channel_gaps(
        self,
        channel_id: str,
    ) -> tuple[bytes, bool, bool, TrailerDecision] | None:
        """``/<conv>/<slug>/gaps.md`` — pure cache lookup, never fetches.

        The server-side workspace gap aggregation runs ~2s; doing the HTTP
        fetch synchronously inside the FUSE callback would blow the 1s
        per-callback budget AND tie up a pool slot while waiting, queueing
        every other callback behind us. The background warmer task
        (:func:`slack_fuse.projector.gaps_warmer.warm_gaps_periodically`)
        populates the cache; this method just reads from it.

        Cache miss → ``None`` (ENOENT-like) — userspace sees the file
        "not yet" and the next try after the warmer cycle returns content.
        """
        if self._channel_gaps_cache is None:
            return None
        cached = self._channel_gaps_cache.get(channel_id)
        if not cached:
            return None
        return cached, False, False, TrailerDecision(kind="clean", stream=f"channel:{channel_id}")

    def _assemble_workspace_gaps(self) -> tuple[bytes, bool, bool, TrailerDecision] | None:
        """``/_workspace/gaps.md`` — pure cache lookup, never fetches.
        See :meth:`_assemble_channel_gaps` for the rationale.
        """
        if self._workspace_gaps_cache is None:
            return None
        cached = self._workspace_gaps_cache.get()
        if not cached:
            return None
        return cached, False, False, TrailerDecision(kind="clean", stream="workspace-gaps")

    # ------------------------------------------------------------------
    # Cache mutators used by the background warmer (off the FUSE path).
    # ------------------------------------------------------------------

    def put_channel_gaps_cached(self, channel_id: str, content: bytes) -> None:
        """Background warmer entry point — populate the per-channel cache
        without going through the FUSE callback path."""
        if self._channel_gaps_cache is None:
            return
        self._channel_gaps_cache.put(channel_id, content=content)

    def put_workspace_gaps_cached(self, content: bytes) -> None:
        """Background warmer entry point — populate the workspace cache
        without going through the FUSE callback path."""
        if self._workspace_gaps_cache is None:
            return
        self._workspace_gaps_cache.put(content=content)

    # ------------------------------------------------------------------
    # FUSE callbacks
    # ------------------------------------------------------------------

    async def getattr(
        self,
        inode: int,
        ctx: pyfuse3.RequestContext,
    ) -> pyfuse3.EntryAttributes:
        with self._callback_guard("getattr", inode=inode):
            # The reserved NO_POSTGRES inode is handled entirely without DB
            # access — it has to be, because PG being down is the whole
            # reason this file exists.
            if inode == NO_POSTGRES_INODE:
                set_path(f"/{NO_POSTGRES_NAME}")
                return self._no_postgres_attr_or_enoent()
            path = self._inodes.get_path(inode)
            if path is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            set_path(path)

            def _sync() -> pyfuse3.EntryAttributes | None:
                if self._is_dir(path):
                    return _make_dir_attr(inode)
                if self._control_enabled:
                    ctl = self._control_file_attr(path, inode)
                    if ctl is not None:
                        return ctl
                resolved = self._resolve_content(path)
                if resolved is None:
                    return None
                content, _trailer, _fallback = resolved
                return _make_file_attr(inode, len(content), timeout_s=_file_attr_timeout(path, self._tz))

            result = await self._run_sync(_sync)
            if result is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            return result

    def _no_postgres_attr_or_enoent(self) -> pyfuse3.EntryAttributes:
        """Return attrs for ``/NO_POSTGRES`` while PG is down, ENOENT otherwise."""
        if self._pg_health is None or not self._pg_health.is_down():
            raise pyfuse3.FUSEError(errno.ENOENT)
        return _make_file_attr(
            NO_POSTGRES_INODE,
            len(self._pg_health.explanation_bytes),
            timeout_s=_MUTABLE_FILE_TIMEOUT_S,  # disappears the moment PG comes back
        )

    async def lookup(
        self,
        parent_inode: int,
        name: bytes,
        ctx: pyfuse3.RequestContext,
    ) -> pyfuse3.EntryAttributes:
        with self._callback_guard("lookup", inode=parent_inode):
            parent_path = self._inodes.get_path(parent_inode)
            if parent_path is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            child_name = name.decode("utf-8", errors="surrogateescape")
            # Special case: ``/NO_POSTGRES`` resolves entirely from the
            # in-process PgHealth flag, no DB hit. Pre-empts ``_list_dir``
            # which would otherwise need PG to enumerate the root.
            if parent_inode == ROOT_INODE and child_name == NO_POSTGRES_NAME:
                set_path(f"/{NO_POSTGRES_NAME}")
                return self._no_postgres_attr_or_enoent()
            child_path = f"/{child_name}" if parent_path == "/" else f"{parent_path}/{child_name}"
            set_path(child_path)

            def _sync() -> pyfuse3.EntryAttributes | None:
                # ``for_lookup=True`` so hidden conv-root channels resolve by their
                # known slug (RFC: hidden is reachable, just not listed). readdir
                # filters them out; lookup must not (review P0-2 / GPT-5.5).
                entries = self._list_dir(parent_path, for_lookup=True)
                for entry_name, entry_is_dir in entries:
                    if entry_name == child_name:
                        return self._child_entry_attr(child_path, is_dir=entry_is_dir)
                return None

            result = await self._run_sync(_sync)
            if result is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            return result

    def _child_entry_attr(self, child_path: str, *, is_dir: bool) -> pyfuse3.EntryAttributes | None:
        """Resolve attrs for a directory child (shared by lookup + opendir).

        Materializes the inode, then returns dir attrs, control-file attrs, or
        rendered-file attrs (sized to the assembled bytes) — or ``None`` when
        the path renders empty (ENOENT-like).
        """
        inode = self._inodes.get_or_create(child_path)
        if is_dir:
            return _make_dir_attr(inode)
        if self._control_enabled:
            ctl = self._control_file_attr(child_path, inode)
            if ctl is not None:
                return ctl
        resolved = self._resolve_content(child_path)
        if resolved is None:
            return None
        content, _trailer, _fallback = resolved
        return _make_file_attr(inode, len(content), timeout_s=_file_attr_timeout(child_path, self._tz))

    def _snapshot_dir(self, path: str) -> list[tuple[str, pyfuse3.EntryAttributes, int]]:
        """Materialize a directory listing with stable pagination tokens.

        Called once per ``opendir`` so the (name, attr, next_token) triples are
        frozen for the lifetime of the dir handle; ``readdir`` then serves
        slices by token without re-querying, so a concurrent insert can't shift
        the array out from under an in-progress iteration.
        """
        result: list[tuple[str, pyfuse3.EntryAttributes, int]] = []
        entries = list(self._list_dir(path))
        # Surface /NO_POSTGRES at the mount root while PG is down. Insert
        # after the normal entries so token IDs of stable entries don't
        # shift between mount lifetimes.
        if path == "/" and self._pg_health is not None and self._pg_health.is_down():
            entries.append((NO_POSTGRES_NAME, False))
        for idx, (name, is_dir) in enumerate(entries):
            child_path = f"/{name}" if path == "/" else f"{path}/{name}"
            if child_path == f"/{NO_POSTGRES_NAME}":
                # Reserved inode; no DB hit (mandatory — PG is the thing
                # that's broken).
                assert self._pg_health is not None
                attr = _make_file_attr(
                    NO_POSTGRES_INODE,
                    len(self._pg_health.explanation_bytes),
                    timeout_s=_MUTABLE_FILE_TIMEOUT_S,
                )
                result.append((name, attr, idx + 1))
                continue
            child_inode = self._inodes.get_or_create(child_path)
            if is_dir:
                attr = _make_dir_attr(child_inode)
            else:
                ctl = self._control_file_attr(child_path, child_inode) if self._control_enabled else None
                if ctl is not None:
                    attr = ctl
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
        with self._callback_guard("opendir", inode=inode):
            path = self._inodes.get_path(inode)
            if path is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            set_path(path)
            snapshot = await self._run_sync(lambda: self._snapshot_dir(path))
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
        with self._callback_guard("open", inode=inode):
            # NO_POSTGRES is a virtual file backed by in-process bytes; no inode
            # map lookup needed (and shouldn't be — PG is the thing that's down).
            if inode == NO_POSTGRES_INODE:
                set_path(f"/{NO_POSTGRES_NAME}")
                if self._pg_health is None or not self._pg_health.is_down():
                    raise pyfuse3.FUSEError(errno.ENOENT)
                fi = pyfuse3.FileInfo()
                fi.fh = inode  # pyright: ignore[reportAttributeAccessIssue]
                # Don't cache: the file disappears when PG comes back up.
                fi.keep_cache = False  # pyright: ignore[reportAttributeAccessIssue]
                return fi
            path = self._inodes.get_path(inode)
            if path is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            set_path(path)
            # ``_control/`` files have their own open semantics (write handles
            # for the refresh triggers, direct-io reads for status).
            if self._control_enabled and parse_path(path)[:1] == [CONTROL_DIR]:
                return self._open_control(path, flags, inode)
            # Everything else is read-only: reject any write-mode open with
            # EROFS so the daemon stays read-only now that the kernel no longer
            # blocks writes for us (the ``ro`` mount option is dropped to let
            # the control writes through). This is the single enforcement point
            # — ``echo > channel.md`` fails here before any write callback.
            if (flags & os.O_ACCMODE) != os.O_RDONLY:
                raise pyfuse3.FUSEError(errno.EROFS)
            fi = pyfuse3.FileInfo()
            fi.fh = inode  # pyright: ignore[reportAttributeAccessIssue]
            # Kernel page-cache caching is gated by the trailer + fallback rules
            # in read(); see RFC §FUSE read path → Trailer / kernel-cache
            # invariant + §Unresolved-fallback / kernel-cache invariant.
            fi.keep_cache = True  # pyright: ignore[reportAttributeAccessIssue]
            return fi

    async def read(self, fh: int, off: int, size: int) -> bytes:
        with self._callback_guard("read", inode=fh):
            # NO_POSTGRES virtual read: pure in-process bytes, no DB hit.
            if fh == NO_POSTGRES_INODE:
                set_path(f"/{NO_POSTGRES_NAME}")
                if self._pg_health is None or not self._pg_health.is_down():
                    raise pyfuse3.FUSEError(errno.EIO)
                data = self._pg_health.explanation_bytes
                return data[off : off + size]
            path = self._inodes.get_path(fh)
            if path is None:
                raise pyfuse3.FUSEError(errno.EIO)
            set_path(path)

            # ``_control/`` reads are pure in-memory (status JSON / empty ctl
            # files) or short HTTP fetches (blocked_channels) — no kernel priming.
            if self._control_enabled and parse_path(path)[:1] == [CONTROL_DIR]:
                return await self._read_control_file(path, off, size)

            def _sync() -> tuple[bytes, bool, bool, TrailerDecision, str] | None:
                resolved = self._resolve_decision(path)
                if resolved is None:
                    return None
                content, trailer, fallback, decision = resolved
                return content, trailer, fallback, decision, path

            result = await self._run_sync(_sync)
            if result is None:
                raise pyfuse3.FUSEError(errno.EIO)
            content, had_trailer, had_fallback, decision, real_path = result

            # One JSONL decision record per read (clean reads included) for bake-in
            # false-positive measurement. ``inode`` is stamped here, where it's
            # known; the writer is append-only and out of the page-cache path, so
            # this never blocks the read result.
            self._record_trailer_decision(decision, fh)

            # ----------------- HARD INVARIANT GATE -----------------
            # notify_store is the bytes-into-kernel-page-cache action. Two
            # invariants forbid it:
            #   1. Trailer present → kernel must NOT cache the warning bytes
            #      (RFC §FUSE read path → Trailer / kernel-cache invariant).
            #      ``had_trailer`` is the *effective* flag: with
            #      ``stale_trailer_enabled=False`` no trailer is appended, so
            #      staleness no longer gates priming (the bake-in comparison knob).
            #   2. Unresolved-fallback present → kernel must NOT cache
            #      UID/CID literals (RFC §FUSE read path → Unresolved-fallback
            #      / kernel-cache invariant). Independent of the trailer flag.
            # Tier is the third gate: only ``hot`` files get primed at all
            # (RFC §Three-tier visibility model → "Kernel priming … fires only
            # on tier = 'hot' reads.").
            # -------------------------------------------------------
            if not had_trailer and not had_fallback and self._is_hot(real_path):
                # 2026-06-24 wedge — notify_store call removed. The original
                # rationale was "prime the kernel page cache so subsequent
                # reads skip the daemon"; but ``fi.keep_cache=True`` (set in
                # open()) already makes the kernel cache the bytes from the
                # read response we're about to send. notify_store would do
                # the same thing redundantly — except it has to TAKE the
                # page lock the kernel holds for the in-flight read, and
                # that's a strict deadlock (kernel waits for read response;
                # notify_store waits for page lock; we haven't yet replied).
                # Dispatching to a worker thread only moved the deadlock off
                # the event loop without breaking it. Drop the call and let
                # keep_cache handle priming organically; live-tail updates
                # still drop the cache via the invalidator path (which runs
                # post-commit, after any in-flight reads have completed).
                self._track_primed(fh)

            return content[off : off + size]

    async def _read_control_file(self, path: str, off: int, size: int) -> bytes:
        parts = parse_path(path)
        if len(parts) == 2 and parts[1] in {CONTROL_BLOCKED_CHANNELS, CONTROL_GAPS, CONTROL_PROBES}:
            data = await self._run_sync(lambda: self._control_read_bytes(path))
        else:
            data = self._control_read_bytes(path)
        if data is None:
            raise pyfuse3.FUSEError(errno.EIO)
        return data[off : off + size]

    def _record_trailer_decision(self, decision: TrailerDecision, inode: int) -> None:
        """Append one trailer decision to the JSONL log, if logging is enabled.

        Best-effort: a log-write failure must never fail a read, so any OSError
        is swallowed with a warning (the decision log is observability, not a
        correctness surface).
        """
        if self._trailer_log is None:
            return
        try:
            self._trailer_log.write(replace(decision, inode=inode))
        except OSError as exc:  # pragma: no cover - log fd failures are rare
            log.warning("trailer_log write failed (inode=%d): %s", inode, exc)

    async def write(self, fh: int, off: int, buf: bytes) -> int:
        with self._callback_guard("write", inode=fh):
            with self._control_write_lock:
                entry = self._control_write_buffers.get(fh)
            if entry is None:
                # Only ``_control/`` write handles are writeable; anything else
                # never got past ``open`` (EROFS), so a write here is a bug or a
                # stale handle. Surface read-only either way.
                raise pyfuse3.FUSEError(errno.EROFS)
            end = off + len(buf)
            if end > _CONTROL_WRITE_MAX:
                raise pyfuse3.FUSEError(errno.EFBIG)
            with self._control_write_lock:
                if len(entry.buffer) < end:
                    entry.buffer.extend(b"\x00" * (end - len(entry.buffer)))
                entry.buffer[off:end] = buf
            return len(buf)

    async def setattr(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        inode: int,
        attr: pyfuse3.EntryAttributes,
        fields: pyfuse3.SetattrFields,
        fh: int | None,
        ctx: pyfuse3.RequestContext,
    ) -> pyfuse3.EntryAttributes:
        with self._callback_guard("setattr", inode=inode):
            path = self._inodes.get_path(inode)
            if path is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            set_path(path)
            # The only legal setattr is the implicit truncate-to-zero an
            # ``O_TRUNC`` open performs on a writeable control trigger (kernels
            # without atomic_o_trunc issue it as a separate SETATTR). Accept it
            # as a no-op and report the file's steady-state attrs. Everything
            # else (chmod/utimes/truncate on read-only files) is EROFS.
            parts = parse_path(path)
            if self._control_enabled and len(parts) == 2 and parts[0] == CONTROL_DIR and parts[1] in CONTROL_WRITABLE:
                return _make_file_attr(inode, 0, mode=stat.S_IFREG | 0o644)
            raise pyfuse3.FUSEError(errno.EROFS)

    async def flush(self, fh: int) -> None:
        # Read-only FS: flush is a no-op. The actual control trigger fires on
        # ``release`` (after the final write), not here — flush can be called
        # multiple times for one open.
        return None

    async def release(self, fh: int) -> None:
        with self._control_write_lock:
            entry = self._control_write_buffers.pop(fh, None)
        if entry is None:
            # A normal read-side release (fh == inode) — nothing to clean up.
            return
        # Fire the control action. Any failure is recorded as
        # ``server_unavailable`` inside ``_fire_control``; a release must never
        # raise (the write already succeeded at the kernel level).
        #
        # Wrap in ``_callback_guard`` so the ContextVar-published control
        # budget (15s) applies. Without this, the guard never runs for
        # release()'s fh — the inode-to-path map has no entry for control
        # write fhs (they live above ``_CONTROL_FH_BASE``) so ``_run_sync``'s
        # inner ``trio.fail_after`` fell back to the default 1s and killed
        # the mid-flight HTTP call, which then landed in
        # ``_control_action_or_unavailable`` as ``server_unavailable`` even
        # though the request succeeded server-side (200) moments later.
        try:
            with self._callback_guard("release", path=entry.path):
                await self._fire_control(entry)
        except pyfuse3.FUSEError:
            # ``_callback_guard`` converts unexpected exceptions to EIO for
            # the FUSE-level release. That's fine — the write already
            # returned success at the kernel level, and any operational
            # verb was recorded inside ``_fire_control`` before the guard's
            # timeout could fire (the control action stamps status per
            # sub-step). We swallow the EIO so pyfuse3 doesn't propagate it.
            log.debug("control release for %s hit callback guard", entry.path)
        except Exception:
            log.exception("control action failed on release for %s", entry.path)

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
# Cross-stream invalidation sink (Sprint 3E)
# ============================================================================


def _fetch_channel_row_by_id(conn: Connection[TupleRow], channel_id: str) -> ChannelRow | None:
    """SELECT a single channels row by id (the columns ``ChannelRow`` needs)."""
    with conn.cursor() as cur:
        _ = cur.execute(
            "SELECT channel_id, name, is_im, is_mpim, is_member, is_archived, im_user_id, tier "
            "FROM channels WHERE channel_id = %s",
            (channel_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return ChannelRow(
        channel_id=str(row[0]),
        name="" if row[1] is None else str(row[1]),
        is_im=bool(row[2]),
        is_mpim=bool(row[3]),
        is_member=bool(row[4]),
        is_archived=bool(row[5]),
        im_user_id=None if row[6] is None else str(row[6]),
        tier=str(row[7]),
    )


class V2InvalidationSink:
    """Projector ``InvalidationSink`` that drops V2 FUSE inodes' kernel cache.

    Structurally implements ``slack_fuse.projector.apply.InvalidationSink``
    (``chunk_changed`` / ``thread_chunk_changed`` / ``channel_list_changed``).
    It is the production wiring the RFC §FUSE read path → Unresolved-fallback /
    kernel-cache invariant relies on: the V2 projector's per-event mutations
    must reach the FUSE process's kernel page cache, or live chunk changes stay
    invisible behind ``fi.keep_cache=True`` until the polling-TTL floor.

    --------------------------------------------------------------------------
    Per-event-kind invalidation pattern (reader's guide — not runtime config)
    --------------------------------------------------------------------------
    The projector's ``apply_event`` performs a cross-stream ``chunk_mentions``
    lookup *inside the same TX* as the upsert for these kinds, then emits
    ``ChunkRef`` / ``ThreadChunkRef`` intents this sink turns into inode drops:

      * ``user_added`` / ``user_renamed`` — upsert ``users`` + lookup
        ``chunk_mentions WHERE mention_kind='user' AND mentioned_id=$uid``.
      * ``channel_added`` / ``channel_renamed`` — upsert ``channels`` + lookup
        ``chunk_mentions WHERE mention_kind='channel' AND mentioned_id=$cid``.

    The *same TX* is load-bearing: under ``READ COMMITTED`` a separate-TX lookup
    can miss a ``message`` whose write TX has not committed yet (the reviewer's
    adversarial race). The read-side unresolved-fallback invariant
    (``notify_store`` skipped while any mention is unresolved) is the backstop
    if the lookup still misses — see ``tests/projector/test_cross_stream_race.py``.

    These kinds need only the chunk-changed invalidation (the mutated chunk's
    own inode) — no cross-stream lookup:

      * ``message`` / ``message_changed`` / ``message_deleted``
      * ``channel_archived`` / ``channel_unarchived``

    --------------------------------------------------------------------------
    Threading
    --------------------------------------------------------------------------
    Called from a worker thread via ``trio.to_thread.run_sync`` from both the
    live apply path (``StreamApplier._fire_invalidations``) and the snapshot
    fetch path (``snapshot_fetch.fetch_and_apply_snapshot``) — never on the
    event loop. Running on the loop can deadlock against in-flight FUSE reads
    (``pyfuse3.invalidate_inode`` blocks on kernel writeback, kernel writeback
    is holding a lock the FUSE read is waiting on, the read needs the event
    loop — folio_wait_bit_common wedge, 2026-06-24). Both callers moved their
    dispatch off the loop; do not "helpfully" undo that from a caller you
    add. Owns a dedicated psycopg connection so its reads never race the
    FUSE callbacks' connection. ``invalidate_inode`` defaults to the same
    pyfuse3 wrapper ``SlackFuseOpsV2`` uses for the health-subscriber path.

    --------------------------------------------------------------------------
    Why materialized, not only notify_store-primed
    --------------------------------------------------------------------------
    We invalidate every *materialized* inode (one present in the ``inodes``
    table — looked-up or read at least once), not just the notify_store-primed
    set. With ``fi.keep_cache=True`` the kernel caches the bytes returned by a
    plain ``read()`` even when ``notify_store`` was skipped (e.g. an
    unresolved-``<@U…>`` fallback read). So a later ``user_added`` must drop that
    inode's cache regardless of whether ``notify_store`` ever fired — otherwise
    the UID-literal fallback would be served from the kernel cache forever.
    """

    def __init__(
        self,
        conn: Connection[TupleRow],
        local_tz: ZoneInfo,
        *,
        invalidate_inode: InvalidateInodeFn | None = None,
    ) -> None:
        self._conn = conn
        self._tz = local_tz
        self._inodes = PersistentInodeMap(conn)
        self._invalidate_inode: InvalidateInodeFn = (
            invalidate_inode if invalidate_inode is not None else _default_invalidate_inode
        )

    # -- InvalidationSink protocol --------------------------------------

    def chunk_changed(self, ref: ChunkRef) -> None:
        path = self._day_file_path(ref.channel_id, ref.message_ts)
        if path is not None:
            self._invalidate_path(path)

    def thread_chunk_changed(self, ref: ThreadChunkRef) -> None:
        path = self._thread_file_path(ref.channel_id, ref.thread_ts)
        if path is not None:
            self._invalidate_path(path)

    def channel_list_changed(self) -> None:
        # Channel-list churn (add/rename/archive/tier/membership) changes far
        # more than the conv-root listings and channel.md frontmatter:
        #
        #   * ``channel_renamed`` rewrites the ``channel:`` line in every
        #     ``thread.md`` frontmatter, not just ``channel.md``.
        #   * ``channel_archived`` / a tier flip to ``blocked`` makes the whole
        #     subtree ENOENT — every cached ``thread.md`` and month/day/thread
        #     directory under it must drop.
        #   * ``channel_member_changed`` moves the channel between ``channels/``
        #     and ``other-channels/``, changing every descendant path.
        #   * A DM user display change reslugs the DM directory.
        #
        # The mutated channel's slug/conv-root may already be unknown after the
        # mutation, so resolving "just the affected subtree" is unreliable. With
        # ``fi.keep_cache=True`` any *materialized* inode (looked up or read at
        # least once) can be serving stale kernel-cached bytes, so on any
        # channel-list change we invalidate every materialized inode — channel.md
        # AND thread.md AND every directory (review P1-F). Channel-list churn is
        # rare, so the broad sweep is acceptable for v1.
        for inode in self._all_materialized_inodes():
            self._invalidate_inode(inode)

    # -- resolution helpers ---------------------------------------------

    def _channel_location(self, channel_id: str) -> tuple[str, str] | None:
        """Return ``(conv_root, slug)`` for ``channel_id``, or ``None``.

        ``None`` when the channel is unknown or ``blocked`` (blocked channels
        are excluded from slug assignment and have no reachable subtree).
        """
        row = _fetch_channel_row_by_id(self._conn, channel_id)
        if row is None or row.tier == "blocked":
            return None
        conv_root = conv_root_for(row)
        for candidate, slug in assign_conv_root_slugs(self._conn, conv_root):
            if candidate.channel_id == channel_id:
                return conv_root, slug
        return None

    def _day_file_path(self, channel_id: str, message_ts: Decimal) -> str | None:
        location = self._channel_location(channel_id)
        if location is None:
            return None
        conv_root, slug = location
        day = ts_to_local_date(message_ts, self._tz)
        return f"/{conv_root}/{slug}/{day:%Y-%m}/{day:%d}/{CHANNEL_MD}"

    def _thread_file_path(self, channel_id: str, thread_ts: Decimal) -> str | None:
        location = self._channel_location(channel_id)
        if location is None:
            return None
        conv_root, slug = location
        day = ts_to_local_date(thread_ts, self._tz)
        thread_slug = self._thread_slug(channel_id, day, thread_ts)
        if thread_slug is None:
            return None
        return f"/{conv_root}/{slug}/{day:%Y-%m}/{day:%d}/{thread_slug}/{THREAD_MD}"

    def _thread_slug(self, channel_id: str, day: date, thread_ts: Decimal) -> str | None:
        parents = fetch_day_thread_parents(self._conn, channel_id, day, self._tz)
        for slug, ts in dedup_thread_slug_map(parents).items():
            if ts == thread_ts:
                return slug
        return None

    def _all_materialized_inodes(self) -> list[int]:
        """Every allocated inode (review P1-F).

        A channel-list change can invalidate any file or directory under a
        renamed/archived/re-tiered channel, and the affected slug/conv-root may
        be unknown post-mutation, so we sweep the whole ``inodes`` table rather
        than trying to scope to one subtree. Conv-root directory inodes are in
        the table too (materialized when root is first listed), so they are
        covered without a separate pass.
        """
        with self._conn.cursor() as cur:
            _ = cur.execute("SELECT inode FROM inodes")
            return [int(r[0]) for r in cur.fetchall()]

    def _invalidate_path(self, path: str) -> None:
        inode = self._inodes.get_inode(path)
        if inode is None:
            # Never materialized → the kernel holds nothing for this path.
            return
        self._invalidate_inode(inode)


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
    "V2InvalidationSink",
    "synchronous_read_for_test",
]
