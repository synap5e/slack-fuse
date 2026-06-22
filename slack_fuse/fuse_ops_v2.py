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
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Final, TypeVar
from zoneinfo import ZoneInfo

import psycopg
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

#: Generic for ``_run_sync``: the worker's return type flows through to the
#: caller so each callback gets the right narrowed type.
_TSync = TypeVar("_TSync")


def _utcnow() -> datetime:
    return datetime.now(UTC)


NowFn = Callable[[], datetime]

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
        try:
            # IMPORTANT: pool acquire must also be inside a timeout. If all
            # pool slots are held by worker threads stuck in PG / kernel
            # (the host-level FUSE wedge documented in BACKLOG), an
            # unbounded acquire blocks every subsequent callback for as
            # long as the wedge persists. Found 2026-06-22 via the new
            # slow-op logging: an unprotected acquire let one read sit
            # 115s on a wedge before completing. Use the same per-callback
            # budget for the whole borrow → run → release cycle.
            with trio.fail_after(self._callback_timeout_s):
                conn = await self._pool.acquire()
        except trio.TooSlowError:
            log.warning("pool acquire timed out after %.1fs; returning EIO", self._callback_timeout_s)
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
        try:
            with trio.fail_after(self._callback_timeout_s):
                return await trio.to_thread.run_sync(sync_fn, abandon_on_cancel=True)
        except trio.TooSlowError:
            # Close the conn so any SQL the abandoned thread is still running
            # aborts; the pool slot is freed for the next caller. The thread
            # itself keeps running pure-Python work but can't touch the DB.
            log.warning(
                "FUSE callback exceeded %.1fs timeout — returning EIO and "
                "discarding the borrowed conn",
                self._callback_timeout_s,
            )
            await self._pool.release(conn, discard=True)
            released = True
            raise pyfuse3.FUSEError(errno.EIO) from None
        except pyfuse3.FUSEError:
            # Intentional FS-level error code from sync_fn — let through, but
            # the conn might still be valid so return it (don't discard).
            raise
        except psycopg.OperationalError as exc:
            # PG socket vanished mid-query. Mark down so subsequent callbacks
            # fast-fail; discard the bad conn (don't return it to the pool).
            self._maybe_mark_pg_down(exc)
            await self._pool.release(conn, discard=True)
            released = True
            raise pyfuse3.FUSEError(errno.EIO) from None
        except Exception:
            # Catch-all: any unhandled exception in the FUSE callback should
            # become EIO, never propagate out of the process. The conn might
            # have been left in an inconsistent state (open cursor, half-
            # processed result) so discard rather than risk reusing it.
            log.exception("FUSE callback raised unexpected error; returning EIO")
            await self._pool.release(conn, discard=True)
            released = True
            raise pyfuse3.FUSEError(errno.EIO) from None
        finally:
            borrowed_fuse_conn.reset(token)
            if not released:
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
        """Wrap a FUSE callback body so any uncaught exception becomes EIO.

        Opens a :func:`fuse_op` logging scope at the same time so every
        log line inside this callback (including from worker threads,
        helpers, ``psycopg``) carries ``req_id``, ``op``, ``inode``, and
        ``path`` — making it trivial to grep one logical operation out
        of a busy journal and trace it end-to-end.

        The contract every callback honours after this:

        - Return valid data, or
        - Raise ``FUSEError(<errno>)`` (ENOENT, EIO, ENOTDIR, …) intentionally.

        Any other exception — psycopg failure, KeyError from a render
        edge case, AttributeError after a refactor regression, anything —
        gets logged with full traceback (including the FUSE scope
        context) and converted to ``EIO``. The process never dies from a
        single bad callback. Combined with the per-callback timeout in
        :meth:`_run_sync`, every callback either succeeds, returns a
        clean error code, or surfaces EIO within ``callback_timeout_s``.
        """
        with fuse_op(op, inode=inode, path=path):
            try:
                yield
            except pyfuse3.FUSEError:
                raise
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

    def _resolve_decision(
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

        # /<conv-root>/<slug>/<YYYY-MM>/<DD>/channel.md
        if depth == 5 and parts[4] == CHANNEL_MD:
            day = parse_day_date(parts[2], parts[3])
            if day is None:
                return None
            return _assemble_channel_day(self._conn, row, day, self._tz, cfg)

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
                        inode = self._inodes.get_or_create(child_path)
                        if entry_is_dir:
                            return _make_dir_attr(inode)
                        resolved = self._resolve_content(child_path)
                        if resolved is None:
                            return None
                        content, _trailer, _fallback = resolved
                        return _make_file_attr(inode, len(content), timeout_s=_file_attr_timeout(child_path, self._tz))
                return None

            result = await self._run_sync(_sync)
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
                self._notify_store(fh, 0, content)
                self._track_primed(fh)

            return content[off : off + size]

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
    Called synchronously from ``StreamApplier._fire_invalidations`` on the trio
    event-loop thread *after* the applier's TX commits. Owns a dedicated psycopg
    connection so its reads never race the FUSE callbacks' connection (which run
    on worker threads). ``invalidate_inode`` defaults to the same pyfuse3
    wrapper ``SlackFuseOpsV2`` uses for the health-subscriber path.

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
