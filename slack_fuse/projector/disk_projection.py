"""Coalesced on-disk mirror of hot-channel FUSE markdown files.

The chunks tables remain authoritative.  This module composes their current
base bytes into a disposable tree rooted at ``PROJECTION_ROOT``.  Dynamic
staleness trailers remain a read-path concern; structural rendering, mention
resolution, frontmatter, path slugs, and ordering match the current JIT path.
"""

from __future__ import annotations

import os
import threading
from collections.abc import Iterator
from contextlib import contextmanager
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
    conv_root_for,
    day_channel_frontmatter,
    dedup_thread_slug_map,
    fetch_channel_by_slug,
    fetch_day_chunks,
    fetch_day_thread_parents,
    fetch_thread_chunks,
    parse_day_date,
    sql_resolvers_for,
    thread_frontmatter,
    ts_to_local_date,
)
from slack_fuse.projector.apply import ApplyResult, ChunkRef, ThreadChunkRef
from slack_fuse.projector.dirty_set import DirtySet
from slack_fuse.projector.reconnecting_conn import ReconnectingConnection
from slack_fuse_render import resolve_mentions

PROJECTION_ROOT = Path.home() / ".cache" / "slack-fuse" / "projection"

type OffsetSnapshot = tuple[tuple[str, int], ...]


class DiskProjection:
    """Manage dirty state and atomically materialize projected markdown."""

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
        self._dirty = DirtySet()
        self._inflight: set[str] = set()
        self._state_lock = threading.RLock()
        # Production has one coalescer today. Keep that a code-level invariant:
        # a set-valued _inflight cannot represent two owners of the same path.
        self._flush_lock = threading.Lock()
        # A psycopg connection is not safe for overlapping operations.  Path
        # resolution runs in applier worker threads while a coalescer render
        # may run in another, so serialize complete DB/render operations.
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

    def mark_dirty(self, path: str) -> None:
        """Mark a FUSE-relative file path dirty."""
        normalized = _normalize_path(path)
        with self._state_lock:
            self._dirty.mark(normalized)

    def mark_channel_paths_dirty(self, channel_ids: frozenset[str]) -> list[str]:
        """Queue existing projected files for visibility-changed channels.

        A newly blocked channel is deliberately absent from post-mutation slug
        assignment, so its old path cannot be reconstructed from the database.
        Projected markdown carries ``channel_id`` in its frontmatter; use that
        stable identity to find the old paths and let the normal coalescer
        delete them after the tier gate starts returning no content.
        """
        if not channel_ids:
            return []

        paths = [
            f"/{backing.relative_to(self._root).as_posix()}"
            for backing in sorted(self._root.rglob("*.md"))
            if _projected_channel_id(backing) in channel_ids
        ]
        with self._state_lock:
            for path in paths:
                self._dirty.mark(path)
        return paths

    @contextmanager
    def invalidation_barrier(self) -> Iterator[None]:
        """Linearize a DB commit + dirty marks against clean transitions.

        Appliers acquire this before committing bytes that JIT can observe and
        hold it until every affected path is dirty. Readers and the coalescer's
        final transition use the same lock, closing the commit-to-mark gap.
        """
        with self._state_lock:
            yield

    def is_clean(self, path: str) -> bool:
        """Return whether ``path`` has current, fully-written backing bytes."""
        normalized = _normalize_path(path)
        with self._state_lock:
            if normalized in self._inflight or self._dirty.is_marked(normalized):
                return False
            return self.path_for(normalized).is_file()

    def mark_apply_result(self, result: ApplyResult) -> None:
        """Translate one committed apply result into dirty FUSE paths."""
        # Lock order is state -> DB I/O everywhere that needs both. The caller
        # normally already holds the re-entrant barrier across its commit; the
        # outer acquisition also makes direct/snapshot callers atomic.
        with self._state_lock, self._io_lock:
            for ref in result.chunks:
                path = self._day_file_path(ref)
                if path is not None:
                    self.mark_dirty(path)
                # A top-level parent is part of both the day file and its
                # thread file.  This also catches the reply-before-parent
                # ordering: once the parent lands, the previously-unresolvable
                # thread path becomes dirty on this chunk result.
                thread_path = self._thread_file_path(
                    ThreadChunkRef(ref.channel_id, ref.message_ts, ref.message_ts)
                )
                if thread_path is not None:
                    self.mark_dirty(thread_path)
            for ref in result.thread_chunks:
                path = self._thread_file_path(ref)
                if path is not None:
                    self.mark_dirty(path)
            if result.channel_list_changed:
                self._bootstrap_locked(self._conn, datetime.now(self._tz).date())

    def bootstrap(
        self,
        conn: Connection[TupleRow] | ReconnectingConnection | None = None,
        *,
        today: date | None = None,
    ) -> list[str]:
        """Mark today's channel/thread files for every hot channel dirty.

        ``conn`` is accepted for the coalescer's public lifecycle API; direct
        callers may omit it to reuse the projection's dedicated connection.
        """
        bootstrap_conn = self._conn if conn is None else cast("Connection[TupleRow]", conn)
        bootstrap_day = datetime.now(self._tz).date() if today is None else today
        with self._state_lock, self._io_lock:
            return self._bootstrap_locked(bootstrap_conn, bootstrap_day)

    def flush_dirty(self, limit: int) -> list[str]:
        """Render and atomically replace at most ``limit`` dirty paths.

        Drained paths remain logically dirty in ``_inflight`` until the atomic
        replacement completes.  A concurrent mark during a write stays queued
        for the next pass.  On failure, the failed and not-yet-attempted paths
        are requeued before the exception escapes.
        """
        with self._flush_lock:
            with self._state_lock:
                batch = self._dirty.drain(limit)
                self._inflight.update(batch)

            flushed: list[str] = []
            for index, path in enumerate(batch):
                try:
                    # D3 flush ordering steps 1-4: while the path remains
                    # logically dirty/inflight, snapshot every stream its bytes
                    # depend on, render at that point, write the sibling temp,
                    # and atomically replace the visible backing inode.
                    with self._state_lock, self._io_lock:
                        at_offset = self._offset_snapshot(path)
                    with self._io_lock:
                        rendered = self._render_path(path)
                        backing = self.path_for(path)
                        if rendered is None:
                            backing.unlink(missing_ok=True)
                        else:
                            _atomic_write_bytes(backing, rendered)
                    # Step 5: one state-lock acquisition compares offset drift
                    # and queued marks, then performs the dirty->clean (or
                    # dirty->dirty) transition. Never clear inflight separately.
                    _ = self.check_and_mark_clean_if_no_drift(path, at_offset)
                except BaseException:
                    with self._state_lock:
                        for remaining in batch[index:]:
                            self._dirty.mark(remaining)
                            self._inflight.discard(remaining)
                    raise
                # The lifecycle coalescer invalidates every attempted path only
                # after this method returns. A drifted path therefore re-enters
                # through JIT immediately; its later clean retry invalidates too.
                flushed.append(path)
            return flushed

    def check_and_mark_clean_if_no_drift(self, path: str, at_offset: OffsetSnapshot) -> bool:
        """Finish a flush iff neither dependent offsets nor dirty state drifted.

        ``True`` means the atomic replacement is current and the clean marker
        stuck. ``False`` means an event/direct invalidation landed during the
        write; the stale-but-complete disk inode remains ineligible and the path
        is queued for another coalescer pass.
        """
        normalized = _normalize_path(path)
        with self._state_lock, self._io_lock:
            current_offset = self._offset_snapshot(normalized)
            drifted = current_offset != at_offset
            remarked = self._dirty.is_marked(normalized)
            was_inflight = normalized in self._inflight
            if drifted or remarked or not was_inflight:
                self._dirty.mark(normalized)
            self._inflight.discard(normalized)
            return not drifted and not remarked and was_inflight

    def _offset_snapshot(self, path: str) -> OffsetSnapshot:
        """Copy applied offsets that can change one projected file's bytes."""
        streams = {"channel-list", "users"}
        parts = PurePosixPath(path).parts[1:]
        if len(parts) >= 2 and parts[0] in CONV_ROOTS:
            row = fetch_channel_by_slug(self._conn, parts[0], parts[1], allow_hidden=True)
            if row is not None:
                streams.add(f"channel:{row.channel_id}")
        offsets = dict.fromkeys(streams, 0)
        with self._conn.cursor() as cur:
            _ = cur.execute(
                "SELECT stream, applied_offset FROM cursors WHERE stream = ANY(%s)",
                (list(streams),),
            )
            for stream, applied_offset in cur.fetchall():
                offsets[str(stream)] = int(applied_offset)
        return tuple(sorted(offsets.items()))

    def _bootstrap_locked(self, conn: Connection[TupleRow], today: date) -> list[str]:
        marked: list[str] = []
        for conv_root in CONV_ROOTS:
            for row, slug in assign_conv_root_slugs(conn, conv_root):
                if row.tier != "hot":
                    continue
                channel_root = f"/{conv_root}/{slug}"
                metadata_path = f"{channel_root}/{CHANNEL_MD}"
                self.mark_dirty(metadata_path)
                marked.append(metadata_path)

                contents = fetch_day_chunks(conn, row.channel_id, today, self._tz)
                if not contents:
                    continue
                day_root = f"{channel_root}/{today:%Y-%m}/{today:%d}"
                day_path = f"{day_root}/{CHANNEL_MD}"
                self.mark_dirty(day_path)
                marked.append(day_path)

                parents = fetch_day_thread_parents(conn, row.channel_id, today, self._tz)
                for thread_slug in dedup_thread_slug_map(parents, conn):
                    thread_path = f"{day_root}/{thread_slug}/{THREAD_MD}"
                    self.mark_dirty(thread_path)
                    marked.append(thread_path)
        return marked

    def _day_file_path(self, ref: ChunkRef) -> str | None:
        location = self._channel_location(ref.channel_id)
        if location is None:
            return None
        conv_root, slug = location
        day = ts_to_local_date(ref.message_ts, self._tz)
        return f"/{conv_root}/{slug}/{day:%Y-%m}/{day:%d}/{CHANNEL_MD}"

    def _thread_file_path(self, ref: ThreadChunkRef) -> str | None:
        location = self._channel_location(ref.channel_id)
        if location is None:
            return None
        conv_root, slug = location
        day = ts_to_local_date(ref.thread_ts, self._tz)
        parents = fetch_day_thread_parents(self._conn, ref.channel_id, day, self._tz)
        for thread_slug, thread_ts in dedup_thread_slug_map(parents, self._conn).items():
            if thread_ts == ref.thread_ts:
                return f"/{conv_root}/{slug}/{day:%Y-%m}/{day:%d}/{thread_slug}/{THREAD_MD}"
        return None

    def _channel_location(self, channel_id: str) -> tuple[str, str] | None:
        row = _fetch_channel_row_by_id(self._conn, channel_id)
        if row is None or row.tier != "hot":
            return None
        conv_root = conv_root_for(row)
        for candidate, slug in assign_conv_root_slugs(self._conn, conv_root):
            if candidate.channel_id == channel_id:
                return conv_root, slug
        return None

    def _render_path(self, path: str) -> bytes | None:
        parts = PurePosixPath(path).parts[1:]
        if len(parts) < 3 or parts[0] not in CONV_ROOTS or parts[-1] not in (CHANNEL_MD, THREAD_MD):
            return None
        conv_root, slug = parts[0], parts[1]
        row = fetch_channel_by_slug(self._conn, conv_root, slug, allow_hidden=False)
        if row is None or row.tier != "hot":
            return None
        if len(parts) == 3 and parts[2] == CHANNEL_MD:
            return channel_meta_frontmatter(row)
        if len(parts) == 5 and parts[4] == CHANNEL_MD:
            day = parse_day_date(parts[2], parts[3])
            return None if day is None else self._render_day(row, day)
        if len(parts) == 6 and parts[5] == THREAD_MD:
            day = parse_day_date(parts[2], parts[3])
            if day is None:
                return None
            thread_ts = _thread_ts_for_slug(self._conn, row.channel_id, day, self._tz, parts[4])
            return None if thread_ts is None else self._render_thread(row, thread_ts)
        return None

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


def _projected_channel_id(path: Path) -> str | None:
    """Read ``channel_id`` from a projected file's small YAML frontmatter."""
    try:
        with path.open("rb") as handle:
            if handle.readline().rstrip(b"\r\n") != b"---":
                return None
            for _ in range(32):
                line = handle.readline()
                if not line or line.rstrip(b"\r\n") == b"---":
                    return None
                if line.startswith(b"channel_id: "):
                    return line.removeprefix(b"channel_id: ").strip().decode()
    except (FileNotFoundError, UnicodeDecodeError):
        # Atomic projection replacement/removal can race this discovery pass.
        # A vanished path needs no cleanup; malformed disposable bytes will be
        # repaired by another ordinary dirty mark rather than blocking sync.
        return None
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


def _thread_ts_for_slug(
    conn: Connection[TupleRow],
    channel_id: str,
    day: date,
    tz: ZoneInfo,
    slug: str,
) -> Decimal | None:
    parents = fetch_day_thread_parents(conn, channel_id, day, tz)
    return dedup_thread_slug_map(parents, conn).get(slug)
