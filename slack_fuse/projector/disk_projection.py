"""Ledger-backed on-disk mirror of hot-channel FUSE markdown files.

The chunks tables remain authoritative.  This module composes their current
base bytes into a disposable tree rooted at ``PROJECTION_ROOT``.  Dynamic
staleness trailers remain a read-path concern; structural rendering, mention
resolution, frontmatter, path slugs, and ordering match the current JIT path.

PostgreSQL ``projection_targets`` rows are authoritative for materialization
validity.  The in-process dirty and in-flight sets only schedule ledger work;
they never admit projected bytes on the read path.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections.abc import Callable, Iterable
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path, PurePosixPath
from typing import cast
from zoneinfo import ZoneInfo

from psycopg import Connection
from psycopg.rows import TupleRow

from slack_fuse.fuse_v2_helpers import (
    CHANNEL_MD,
    CONV_ROOTS,
    THREAD_MD,
    ChannelRow,
    assign_conv_root_slugs,
    channel_meta_frontmatter,
    day_channel_frontmatter,
    dedup_thread_slug_map,
    fetch_day_chunks,
    fetch_day_thread_parents,
    fetch_thread_chunks,
    sql_resolvers_for,
    thread_frontmatter,
)
from slack_fuse.projector.apply import ApplyResult
from slack_fuse.projector.dirty_set import DirtySet
from slack_fuse.projector.projection_ledger import (
    RENDERER_VERSION,
    TargetKey,
    bump_targets,
    clean_targets,
    ensure_targets_pending,
    layout_needs_reconciliation,
    mark_layout_reconciled,
    mark_target_rendered,
    path_for_target,
    pending_targets,
    reconcile_layout_generation,
    reconcile_renderer_epoch,
    target_generation_for_render,
    target_key_for_path,
    targets_for_apply_result,
)
from slack_fuse.projector.reconnecting_conn import ReconnectingConnection
from slack_fuse_render import resolve_mentions

PROJECTION_ROOT = Path.home() / ".cache" / "slack-fuse" / "projection"
log = logging.getLogger(__name__)


class DiskProjection:
    """Atomically materialize ledger-versioned projected markdown."""

    def __init__(
        self,
        conn: Connection[TupleRow] | ReconnectingConnection,
        local_tz: ZoneInfo,
        *,
        root: Path | None = None,
    ) -> None:
        self._conn = cast("Connection[TupleRow]", conn)
        self._tz = local_tz
        self._root = PROJECTION_ROOT if root is None else root
        self._dirty = DirtySet[TargetKey]()
        self._inflight: set[TargetKey] = set()
        # Filesystem mutations must be followed by a successful kernel cache
        # drop before their ledger generation can become clean. Remember
        # paths whose invalidator failed so deleted aliases are not forgotten.
        self._pending_invalidations: dict[TargetKey, set[str]] = {}
        self._state_lock = threading.RLock()
        # Production has one coalescer today. Keep that a code-level invariant:
        # a set-valued _inflight cannot represent two owners of one target.
        self._flush_lock = threading.Lock()
        # A psycopg connection is not safe for overlapping operations. Only
        # the coalescer and direct/manual scheduling helpers use this conn;
        # FUSE readers use their callback-pool connections instead.
        self._io_lock = threading.Lock()

    @property
    def pending_count(self) -> int:
        """Number of queued paths, excluding the bounded in-flight batch."""
        with self._state_lock:
            return len(self._dirty)

    def path_for(self, path: str) -> Path:
        """Map a FUSE path to its backing path beneath the projection root."""
        normalized = _normalize_path(path)
        return self._root.joinpath(*PurePosixPath(normalized).parts[1:])

    def read_bytes(self, path: str) -> bytes | None:
        """Return a projected file's bytes, or ``None`` when it is absent.

        Dirtiness is deliberately not checked here. The FUSE caller owns the
        clean/dirty tier decision and uses this accessor only after that gate.
        A missing file can still be observed after the clean check when an
        atomic replacement or deletion races the read, so absence is a normal
        fallback signal rather than an exception.
        """
        try:
            return self.path_for(path).read_bytes()
        except FileNotFoundError:
            return None

    def backing_matches_target(
        self,
        path: str,
        key: TargetKey,
        content: bytes | None = None,
    ) -> bool:
        """Return whether backing frontmatter claims the resolved identity."""
        if content is not None:
            return _target_key_from_content(content) == key
        return _target_key_from_backing(self.path_for(path)) == key

    def mark_dirty(self, path: str) -> None:
        """Durably invalidate a path resolved through the dedicated conn.

        Production writers bump the ledger in their source transaction and use
        :meth:`mark_apply_result` only as a scheduling hint. This compatibility
        surface remains for direct/manual invalidators and tests.
        """
        normalized = _normalize_path(path)
        with self._io_lock, self._conn.transaction(), self._conn.cursor() as cur:
            key = target_key_for_path(normalized, self._tz, cur)
            if key is None:
                return
            bump_targets(cur, (key,), RENDERER_VERSION)
        self.mark_target_dirty(key)

    def mark_target_dirty(self, key: TargetKey) -> None:
        """Queue one durable target in the process-local scheduling cache."""
        if key.target_kind == "layout":
            return
        with self._state_lock:
            self._dirty.mark(key)

    def mark_apply_result(self, result: ApplyResult) -> None:
        """Queue stable keys from one committed result without DB or path I/O."""
        keys = tuple(
            key
            for key in targets_for_apply_result(result, self._tz)
            if key.target_kind != "layout"
        )
        with self._state_lock:
            for key in keys:
                self._dirty.mark(key)

    def bootstrap(
        self,
        conn: Connection[TupleRow] | ReconnectingConnection | None = None,
        *,
        today: date | None = None,
    ) -> list[str]:
        """Durably mark today's channel/thread targets for hot channels.

        Retained as a direct/manual compatibility helper. The lifecycle
        coalescer uses :meth:`reconcile_startup` instead.
        """
        bootstrap_conn = self._conn if conn is None else cast("Connection[TupleRow]", conn)
        bootstrap_day = datetime.now(self._tz).date() if today is None else today
        targets: list[TargetKey] = []
        paths: list[str] = []
        with self._io_lock:
            for conv_root in CONV_ROOTS:
                for row, slug in assign_conv_root_slugs(bootstrap_conn, conv_root):
                    if row.tier != "hot":
                        continue
                    targets.append(TargetKey("channel-meta", row.channel_id, None, None))
                    paths.append(f"/{conv_root}/{slug}/{CHANNEL_MD}")
                    contents = fetch_day_chunks(bootstrap_conn, row.channel_id, bootstrap_day, self._tz)
                    if not contents:
                        continue
                    targets.append(TargetKey("day", row.channel_id, bootstrap_day, None))
                    day_root = f"/{conv_root}/{slug}/{bootstrap_day:%Y-%m}/{bootstrap_day:%d}"
                    paths.append(f"{day_root}/{CHANNEL_MD}")
                    parents = fetch_day_thread_parents(bootstrap_conn, row.channel_id, bootstrap_day, self._tz)
                    for thread_slug, thread_ts in dedup_thread_slug_map(parents, bootstrap_conn).items():
                        targets.append(TargetKey("thread", row.channel_id, bootstrap_day, thread_ts))
                        paths.append(f"{day_root}/{thread_slug}/{THREAD_MD}")
            with bootstrap_conn.transaction(), bootstrap_conn.cursor() as cur:
                bump_targets(cur, targets, RENDERER_VERSION)
        for key in targets:
            self.mark_target_dirty(key)
        return paths

    def reconcile_startup(
        self,
        conn: Connection[TupleRow] | ReconnectingConnection | None = None,
        invalidate_path: Callable[[str], None] | None = None,
    ) -> tuple[list[str], int, float]:
        """Recover renderer epochs and pre-ledger files once at startup.

        Returns ``(removed_stale_paths, recovered_target_count, duration_ms)``.
        Missing file identities are parsed from their stable frontmatter so
        renamed/blocked paths do not need to resolve through current slugs.
        """
        started = time.perf_counter()
        reconcile_conn = self._conn if conn is None else cast("Connection[TupleRow]", conn)
        with self._io_lock:
            files = tuple(sorted(self._root.rglob("*.md"))) if self._root.exists() else ()
            file_targets = tuple(
                key for backing in files if (key := _target_key_from_backing(backing)) is not None
            )
            with reconcile_conn.transaction(), reconcile_conn.cursor() as cur:
                epoch_targets = reconcile_renderer_epoch(cur, RENDERER_VERSION)
                inserted_targets = ensure_targets_pending(cur, file_targets, RENDERER_VERSION)
                existing_clean = clean_targets(cur, RENDERER_VERSION)
                expected_paths = self._expected_paths_for_targets(reconcile_conn, existing_clean)
                missing_targets = tuple(
                    key
                    for key, expected_path in expected_paths.items()
                    if expected_path is not None and not self.path_for(expected_path).is_file()
                )
                bump_targets(cur, missing_targets, RENDERER_VERSION)
            removed = self._remove_stale_backings(
                reconcile_conn,
                files,
                TargetKey("layout", None, None, None),
                invalidate_path,
            )
        recovered = tuple(dict.fromkeys((*epoch_targets, *inserted_targets, *missing_targets)))
        for key in recovered:
            self.mark_target_dirty(key)
        duration_ms = (time.perf_counter() - started) * 1000
        log.info(
            "projector-span op=projection.startup_reconciliation duration_ms=%.3f recovered_count=%d",
            duration_ms,
            len(recovered),
        )
        return removed, len(recovered), duration_ms

    def reconcile_layout(
        self,
        conn: Connection[TupleRow] | ReconnectingConnection | None = None,
        invalidate_path: Callable[[str], None] | None = None,
    ) -> list[str]:
        """Fan out one pending layout generation off the apply hot path."""
        reconcile_conn = self._conn if conn is None else cast("Connection[TupleRow]", conn)
        with self._io_lock:
            with reconcile_conn.cursor() as cur:
                if not layout_needs_reconciliation(cur, RENDERER_VERSION):
                    return []
            files = tuple(sorted(self._root.rglob("*.md"))) if self._root.exists() else ()
            file_targets = tuple(
                key for backing in files if (key := _target_key_from_backing(backing)) is not None
            )
            with reconcile_conn.transaction(), reconcile_conn.cursor() as cur:
                cur.execute("SELECT channel_id FROM channels WHERE tier = 'hot' ORDER BY channel_id")
                channel_meta_targets = tuple(
                    TargetKey("channel-meta", str(row[0]), None, None) for row in cur.fetchall()
                )
                bootstrap_day = datetime.now(self._tz).date()
                today_targets: list[TargetKey] = []
                for target in channel_meta_targets:
                    assert target.channel_id is not None
                    contents = fetch_day_chunks(reconcile_conn, target.channel_id, bootstrap_day, self._tz)
                    if not contents:
                        continue
                    today_targets.append(TargetKey("day", target.channel_id, bootstrap_day, None))
                    parents = fetch_day_thread_parents(
                        reconcile_conn,
                        target.channel_id,
                        bootstrap_day,
                        self._tz,
                    )
                    today_targets.extend(
                        TargetKey("thread", target.channel_id, bootstrap_day, thread_ts)
                        for thread_ts in dedup_thread_slug_map(parents, reconcile_conn).values()
                    )
                generation, affected = reconcile_layout_generation(
                    cur,
                    (*file_targets, *channel_meta_targets, *today_targets),
                    RENDERER_VERSION,
                )
            if generation is None:
                return []
            # Keep the singleton pending until every obsolete inode is gone
            # and its kernel cache entry has been dropped. A failure leaves
            # both the durable layout generation and the in-memory retry set
            # pending for the next tick.
            removed = self._remove_stale_backings(
                reconcile_conn,
                files,
                TargetKey("layout", None, None, None),
                invalidate_path,
            )
            with reconcile_conn.transaction(), reconcile_conn.cursor() as cur:
                _ = mark_layout_reconciled(cur, generation, RENDERER_VERSION)
        for key in affected:
            self.mark_target_dirty(key)
        return removed

    def discover_pending(
        self,
        limit: int,
        conn: Connection[TupleRow] | ReconnectingConnection | None = None,
    ) -> tuple[TargetKey, ...]:
        """Enqueue a bounded steady-state batch from the pending partial index."""
        discover_conn = self._conn if conn is None else cast("Connection[TupleRow]", conn)
        with self._io_lock, discover_conn.cursor() as cur:
            discovered = pending_targets(cur, RENDERER_VERSION, limit)
        for key in discovered:
            self.mark_target_dirty(key)
        return discovered

    def flush_dirty(
        self,
        limit: int,
        invalidate_path: Callable[[str], None] | None = None,
    ) -> list[str]:
        """Render and CAS at most ``limit`` scheduled target identities.

        The heap sets are scheduling only. A concurrent durable invalidation is
        detected by the ledger CAS even when no heap mark arrives in time.
        """
        with self._flush_lock:
            batch = self._claim_scheduled_batch(limit)
            flushed: list[str] = []
            for index, key in enumerate(batch):
                try:
                    outcome = self._try_flush_target(key, invalidate_path)
                except BaseException:
                    self._requeue_targets(batch[index:])
                    raise
                if outcome is None:
                    continue
                path, cas_applied = outcome
                self._finish_scheduled_target(key, cas_applied)
                if path is None:
                    continue
                flushed.append(path)
            return flushed

    def _claim_scheduled_batch(self, limit: int) -> list[TargetKey]:
        with self._state_lock:
            batch = self._dirty.drain(limit)
            self._inflight.update(batch)
            return batch

    def _try_flush_target(
        self,
        key: TargetKey,
        invalidate_path: Callable[[str], None] | None,
    ) -> tuple[str | None, bool] | None:
        try:
            return self._flush_target(key, invalidate_path)
        except Exception:
            self._requeue_targets((key,))
            log.warning("disk projection target flush failed; requeued key=%r", key, exc_info=True)
            return None

    def _finish_scheduled_target(self, key: TargetKey, cas_applied: bool) -> None:
        with self._state_lock:
            remarked = self._dirty.is_marked(key)
            if not cas_applied or remarked:
                self._dirty.mark(key)
            self._inflight.discard(key)

    def _requeue_targets(self, targets: tuple[TargetKey, ...] | list[TargetKey]) -> None:
        with self._state_lock:
            for target in targets:
                self._dirty.mark(target)
                self._inflight.discard(target)

    def _remember_pending_invalidation(
        self,
        key: TargetKey,
        path: str,
    ) -> None:
        with self._state_lock:
            self._pending_invalidations.setdefault(key, set()).add(path)

    def _forget_pending_invalidation(self, key: TargetKey, path: str) -> None:
        with self._state_lock:
            paths = self._pending_invalidations.get(key)
            if paths is None:
                return
            paths.discard(path)
            if not paths:
                del self._pending_invalidations[key]

    def _retry_pending_invalidations(
        self,
        key: TargetKey,
        invalidate_path: Callable[[str], None] | None,
    ) -> None:
        with self._state_lock:
            paths = tuple(sorted(self._pending_invalidations.get(key, ())))
        if not paths:
            return
        if invalidate_path is None:
            msg = f"kernel invalidation callback missing for pending paths: {paths!r}"
            raise RuntimeError(msg)
        first_error: Exception | None = None
        for path in paths:
            try:
                invalidate_path(path)
            except Exception as exc:
                if first_error is None:
                    first_error = exc
                log.warning(
                    "projection path invalidation failed; retained for retry path=%s",
                    path,
                    exc_info=True,
                )
            else:
                self._forget_pending_invalidation(key, path)
        if first_error is not None:
            msg = f"kernel invalidation failed for {key!r}"
            raise RuntimeError(msg) from first_error

    def _invalidate_changed_path(
        self,
        key: TargetKey,
        path: str,
        invalidate_path: Callable[[str], None] | None,
    ) -> None:
        if invalidate_path is None:
            return
        self._remember_pending_invalidation(key, path)
        try:
            invalidate_path(path)
        except Exception:
            log.warning(
                "projection path invalidation failed; retained for retry path=%s",
                path,
                exc_info=True,
            )
            raise
        self._forget_pending_invalidation(key, path)

    def _flush_target(
        self,
        key: TargetKey,
        invalidate_path: Callable[[str], None] | None,
    ) -> tuple[str | None, bool]:
        started = time.perf_counter()
        path: str | None = None
        cas_applied = False
        generation: int | None = None
        try:
            # A deleted alias whose previous invalidation failed cannot be
            # rediscovered from disk. Retry remembered cache drops before
            # producing or completing another generation for this target.
            self._retry_pending_invalidations(key, invalidate_path)
            with self._io_lock:
                with self._conn.cursor() as cur:
                    generation = target_generation_for_render(cur, key, RENDERER_VERSION)
                if generation is None:
                    return None, True
                # Render strictly from the stable identity. Resolve the
                # mutable slug/path only after rendering, immediately before
                # the atomic filesystem mutation.
                rendered = self._render_target(key)
                with self._conn.cursor() as cur:
                    path = path_for_target(cur, key, self._tz)

            if path is not None:
                backing = self.path_for(path)
                if rendered is None:
                    try:
                        backing.unlink()
                    except FileNotFoundError:
                        pass
                    else:
                        self._invalidate_changed_path(key, path, invalidate_path)
                else:
                    _atomic_write_bytes(backing, rendered)
                    # Cache invalidation precedes the ledger CAS. Even a CAS
                    # execute/COMMIT exception after os.replace therefore
                    # cannot expose changed bytes through a stale inode.
                    self._invalidate_changed_path(key, path, invalidate_path)

            self._remove_stale_target_aliases(key, path, invalidate_path)

            with self._io_lock, self._conn.transaction(), self._conn.cursor() as cur:
                cas_applied = mark_target_rendered(
                    cur,
                    key,
                    generation,
                    RENDERER_VERSION,
                )
            return path, cas_applied
        finally:
            duration_ms = (time.perf_counter() - started) * 1000
            log.info(
                "projector-span op=projection.flush target_kind=%s channel_id=%s "
                "duration_ms=%.3f cas_result=%s",
                key.target_kind,
                key.channel_id or "-",
                duration_ms,
                "applied" if cas_applied else "skipped",
            )

    def _render_target(self, key: TargetKey) -> bytes | None:
        """Render from stable identity, independent of mutable path ownership."""
        if key.channel_id is None:
            return None
        row = _fetch_channel_row_by_id(self._conn, key.channel_id)
        if row is None or row.tier != "hot":
            return None
        if key.target_kind == "channel-meta":
            return channel_meta_frontmatter(row)
        if key.target_kind == "day" and key.local_day is not None:
            return self._render_day(row, key.local_day)
        if key.target_kind == "thread" and key.thread_ts is not None:
            return self._render_thread(row, key.thread_ts)
        return None

    def _remove_stale_target_aliases(
        self,
        key: TargetKey,
        current_path: str | None,
        invalidate_path: Callable[[str], None] | None,
    ) -> None:
        """Delete obsolete filesystem names for one stable thread target.

        Parent edits can change a thread slug without changing its TargetKey
        or bumping the global layout singleton. Search the bounded current-day
        directory after each thread render; if the target no longer has a
        materializable path, fall back to a rare full sweep so its old alias
        cannot survive a successful CAS.
        """
        if key.target_kind != "thread":
            return
        current_backing = None if current_path is None else self.path_for(current_path)
        if current_backing is None:
            candidates = tuple(self._root.rglob(THREAD_MD)) if self._root.exists() else ()
        else:
            day_root = current_backing.parent.parent
            candidates = tuple(day_root.glob(f"*/{THREAD_MD}")) if day_root.exists() else ()
        for backing in candidates:
            if current_backing is not None and backing == current_backing:
                continue
            if _target_key_from_backing(backing) != key:
                continue
            stale_path = f"/{backing.relative_to(self._root).as_posix()}"
            try:
                backing.unlink()
            except FileNotFoundError:
                continue
            self._invalidate_changed_path(key, stale_path, invalidate_path)

    def _remove_stale_backings(
        self,
        conn: Connection[TupleRow],
        files: tuple[Path, ...],
        owner_key: TargetKey,
        invalidate_path: Callable[[str], None] | None,
    ) -> list[str]:
        self._retry_pending_invalidations(owner_key, invalidate_path)
        removed: list[str] = []
        discovered = tuple(
            (backing, key)
            for backing in files
            if (key := _target_key_from_backing(backing)) is not None
        )
        expected_paths = self._expected_paths_for_targets(conn, (key for _backing, key in discovered))
        for backing, key in discovered:
            fuse_path = f"/{backing.relative_to(self._root).as_posix()}"
            if expected_paths[key] == fuse_path:
                continue
            try:
                backing.unlink()
            except FileNotFoundError:
                continue
            removed.append(fuse_path)
            self._invalidate_changed_path(owner_key, fuse_path, invalidate_path)
        return removed

    def _expected_paths_for_targets(
        self,
        conn: Connection[TupleRow],
        targets: Iterable[TargetKey],
    ) -> dict[TargetKey, str | None]:
        """Resolve many paths with one slug scan and one query per thread day."""
        locations: dict[str, tuple[str, str]] = {}
        for conv_root in CONV_ROOTS:
            for row, slug in assign_conv_root_slugs(conn, conv_root):
                if row.tier == "hot":
                    locations[row.channel_id] = (conv_root, slug)
        thread_slugs: dict[tuple[str, date], dict[Decimal, str]] = {}
        return {
            key: self._path_for_cached_target(conn, key, locations, thread_slugs)
            for key in dict.fromkeys(targets)
        }

    def _path_for_cached_target(
        self,
        conn: Connection[TupleRow],
        key: TargetKey,
        locations: dict[str, tuple[str, str]],
        thread_slugs: dict[tuple[str, date], dict[Decimal, str]],
    ) -> str | None:
        location = None if key.channel_id is None else locations.get(key.channel_id)
        if location is None:
            return None
        conv_root, slug = location
        channel_root = f"/{conv_root}/{slug}"
        if key.target_kind == "channel-meta":
            return f"{channel_root}/{CHANNEL_MD}"
        if key.local_day is None:
            return None
        day_root = f"{channel_root}/{key.local_day:%Y-%m}/{key.local_day:%d}"
        if key.target_kind == "day":
            return f"{day_root}/{CHANNEL_MD}"
        if key.thread_ts is None or key.channel_id is None:
            return None
        cache_key = (key.channel_id, key.local_day)
        if cache_key not in thread_slugs:
            parents = fetch_day_thread_parents(conn, key.channel_id, key.local_day, self._tz)
            thread_slugs[cache_key] = {
                thread_ts: thread_slug
                for thread_slug, thread_ts in dedup_thread_slug_map(parents, conn).items()
            }
        thread_slug = thread_slugs[cache_key].get(key.thread_ts)
        return None if thread_slug is None else f"{day_root}/{thread_slug}/{THREAD_MD}"

    def _render_day(self, row: ChannelRow, day: date) -> bytes | None:
        contents = fetch_day_chunks(self._conn, row.channel_id, day, self._tz)
        if not contents:
            return None
        users, channels = sql_resolvers_for(self._conn)
        resolved = resolve_mentions("\n".join(contents), users, channels)
        return (day_channel_frontmatter(row, day) + resolved).encode()

    def _render_thread(self, row: ChannelRow, thread_ts: Decimal) -> bytes | None:
        contents, reply_count = fetch_thread_chunks(self._conn, row.channel_id, thread_ts)
        if not contents:
            return None
        users, channels = sql_resolvers_for(self._conn)
        resolved = resolve_mentions("\n".join(contents), users, channels)
        return (thread_frontmatter(row, thread_ts, reply_count, self._tz) + resolved).encode()


def _normalize_path(path: str) -> str:
    if not path or "\x00" in path:
        msg = "projection path must be a non-empty text path"
        raise ValueError(msg)
    candidate = PurePosixPath(f"/{path.lstrip('/')}")
    if any(part in ("", ".", "..") for part in candidate.parts[1:]):
        msg = f"unsafe projection path: {path!r}"
        raise ValueError(msg)
    return candidate.as_posix()


def _target_key_from_backing(path: Path) -> TargetKey | None:
    """Recover a stable target identity from projected YAML frontmatter."""
    try:
        return _target_key_from_content(path.read_bytes())
    except (FileNotFoundError, UnicodeDecodeError, ValueError):
        # Atomic projection replacement/removal can race this discovery pass.
        # Malformed disposable bytes fail closed in the reader and can be
        # repaired by a later ordinary target invalidation.
        return None


def _target_key_from_content(content: bytes) -> TargetKey | None:
    """Parse stable target identity from one complete backing-file image."""
    try:
        fields = _read_frontmatter_fields(content)
        if fields is None:
            return None
        channel_id = fields.get("channel_id")
        if not channel_id:
            return None
        if "date" not in fields:
            return TargetKey("channel-meta", channel_id, None, None)
        raw_day = fields.get("date")
        if raw_day is None:
            return None
        local_day = date.fromisoformat(raw_day)
        raw_thread_ts = fields.get("thread_ts")
        if raw_thread_ts is None:
            return TargetKey("day", channel_id, local_day, None)
        return TargetKey("thread", channel_id, local_day, Decimal(raw_thread_ts))
    except (UnicodeDecodeError, ValueError):
        return None


def _read_frontmatter_fields(content: bytes) -> dict[str, str] | None:
    lines = iter(content.splitlines())
    if next(lines, None) != b"---":
        return None
    fields: dict[str, str] = {}
    for _ in range(32):
        line = next(lines, None)
        if line is None:
            return None
        if line == b"---":
            return fields
        name, separator, value = line.decode().partition(":")
        if separator:
            fields[name.strip()] = value.strip().strip('"')
    return None


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    """Write bytes via the same sibling-temp + atomic-replace pattern as disk_cache."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


def _fetch_channel_row_by_id(conn: Connection[TupleRow], channel_id: str) -> ChannelRow | None:
    with conn.cursor() as cur:
        _ = cur.execute(
            "SELECT channel_id, name, is_im, is_mpim, is_member, is_archived, im_user_id, tier "
            "FROM channels WHERE channel_id = %s",
            (channel_id,),
        )
        raw = cur.fetchone()
    if raw is None:
        return None
    return ChannelRow(
        channel_id=str(raw[0]),
        name="" if raw[1] is None else str(raw[1]),
        is_im=bool(raw[2]),
        is_mpim=bool(raw[3]),
        is_member=bool(raw[4]),
        is_archived=bool(raw[5]),
        im_user_id=None if raw[6] is None else str(raw[6]),
        tier=str(raw[7]),
    )
