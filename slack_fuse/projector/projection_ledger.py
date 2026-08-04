"""Transactional validity ledger for projected materializations.

Writers bump stable target identities in their source-data transactions.
Readers validate one resolved identity through their callback connection, and
the coalescer conditionally advances the rendered generation after replacing
the corresponding backing file.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Final, Literal, cast
from zoneinfo import ZoneInfo

from psycopg import Connection, Cursor
from psycopg.rows import TupleRow

from slack_fuse.fuse_v2_helpers import (
    CHANNEL_MD,
    CONV_ROOTS,
    THREAD_MD,
    ChannelRow,
    assign_conv_root_slugs,
    conv_root_for,
    dedup_thread_slug_map,
    fetch_channel_by_slug,
    fetch_day_thread_parents,
    parse_day_date,
)

if TYPE_CHECKING:
    from slack_fuse.projector.apply import ApplyResult


type TargetKind = Literal["channel-meta", "day", "thread", "layout"]
type TargetCleanResult = Literal["clean", "dirty", "missing"]

#: Manual renderer/schema epoch. Bump this stable value whenever the bytes a
#: projection target produces change structurally.
RENDERER_VERSION: Final = "v1"


@dataclass(frozen=True, slots=True)
class TargetKey:
    """Stable identity of one projected materialization."""

    target_kind: TargetKind
    channel_id: str | None
    local_day: date | None
    thread_ts: Decimal | None


def target_key_for_path(path: str, tz: ZoneInfo, cur: Cursor[TupleRow]) -> TargetKey | None:
    """Resolve one projectable FUSE path to its stable ledger identity.

    Resolution deliberately mirrors :class:`DiskProjection` path rendering,
    but is read-only and uses the caller's cursor/connection. A stale path for
    a renamed, blocked, or deleted channel therefore resolves to ``None``.
    """
    parts = _path_parts(path)
    if len(parts) < 3 or parts[0] not in CONV_ROOTS:
        return None
    conn = cast("Connection[TupleRow]", cur.connection)
    row = fetch_channel_by_slug(conn, parts[0], parts[1], allow_hidden=True)
    if row is None or row.tier != "hot":
        return None

    if len(parts) == 3 and parts[2] == CHANNEL_MD:
        return TargetKey("channel-meta", row.channel_id, None, None)
    if len(parts) == 5 and parts[4] == CHANNEL_MD:
        local_day = parse_day_date(parts[2], parts[3])
        if local_day is None:
            return None
        return TargetKey("day", row.channel_id, local_day, None)
    if len(parts) == 6 and parts[5] == THREAD_MD:
        local_day = parse_day_date(parts[2], parts[3])
        if local_day is None:
            return None
        parents = fetch_day_thread_parents(conn, row.channel_id, local_day, tz)
        thread_ts = dedup_thread_slug_map(parents, conn).get(parts[4])
        if thread_ts is None:
            return None
        return TargetKey("thread", row.channel_id, local_day, thread_ts)
    return None


def path_for_target(cur: Cursor[TupleRow], key: TargetKey, tz: ZoneInfo) -> str | None:
    """Resolve a stable ledger identity to its current projectable FUSE path."""
    if key.target_kind == "layout" or key.channel_id is None:
        return None
    conn = cast("Connection[TupleRow]", cur.connection)
    row = _fetch_channel_row(cur, key.channel_id)
    if row is None or row.tier != "hot":
        return None
    conv_root = conv_root_for(row)
    slug = next(
        (
            candidate_slug
            for candidate, candidate_slug in assign_conv_root_slugs(conn, conv_root)
            if candidate.channel_id == key.channel_id
        ),
        None,
    )
    if slug is None:
        return None
    root = f"/{conv_root}/{slug}"
    if key.target_kind == "channel-meta":
        return f"{root}/{CHANNEL_MD}"
    if key.local_day is None:
        return None
    day_root = f"{root}/{key.local_day:%Y-%m}/{key.local_day:%d}"
    if key.target_kind == "day":
        return f"{day_root}/{CHANNEL_MD}"
    if key.thread_ts is None:
        return None
    parents = fetch_day_thread_parents(conn, key.channel_id, key.local_day, tz)
    thread_slug = next(
        (
            candidate_slug
            for candidate_slug, thread_ts in dedup_thread_slug_map(parents, conn).items()
            if thread_ts == key.thread_ts
        ),
        None,
    )
    return None if thread_slug is None else f"{day_root}/{thread_slug}/{THREAD_MD}"


def target_clean_result(
    cur: Cursor[TupleRow],
    key: TargetKey,
    expected_renderer_version: str,
) -> TargetCleanResult:
    """Return the fail-closed ledger decision for one target."""
    cur.execute(
        "SELECT target_generation, rendered_generation, renderer_version, "
        "  EXISTS ("
        "    SELECT 1 FROM projection_targets AS layout "
        "    WHERE layout.target_kind = 'layout' "
        "    AND layout.channel_id IS NULL AND layout.local_day IS NULL AND layout.thread_ts IS NULL "
        "    AND layout.renderer_version = %s "
        "    AND layout.rendered_generation >= layout.target_generation"
        "  ) AS layout_is_clean "
        "FROM projection_targets WHERE target_kind = %s "
        "AND channel_id IS NOT DISTINCT FROM %s "
        "AND local_day IS NOT DISTINCT FROM %s "
        "AND thread_ts IS NOT DISTINCT FROM %s",
        (expected_renderer_version, *_target_params(key)),
    )
    row = cur.fetchone()
    if row is None:
        return "missing"
    target_generation, rendered_generation, renderer_version, layout_is_clean = row
    if str(renderer_version) != expected_renderer_version:
        return "dirty"
    if int(rendered_generation) < int(target_generation):
        return "dirty"
    # Layout mutations can change slug ownership and channel frontmatter for
    # otherwise-clean targets. Keep all disk bytes ineligible until the
    # background reconciler has durably fanned that singleton generation out.
    if not bool(layout_is_clean):
        return "dirty"
    return "clean"


def is_target_clean(
    cur: Cursor[TupleRow],
    key: TargetKey,
    expected_renderer_version: str,
) -> bool:
    """Return whether one target and the layout singleton admit disk bytes.

    A target can be generation-clean while a committed layout mutation is
    still awaiting background fan-out. Requiring the singleton to be clean in
    the same query closes that slug-reassignment window without a Python lock.
    """
    return target_clean_result(cur, key, expected_renderer_version) == "clean"


def pending_targets(
    cur: Cursor[TupleRow],
    expected_renderer_version: str,
    limit: int,
) -> tuple[TargetKey, ...]:
    """Return a bounded steady-state batch via the generation partial index."""
    if limit <= 0:
        return ()
    cur.execute(
        "SELECT target_kind, channel_id, local_day, thread_ts "
        "FROM projection_targets "
        "WHERE rendered_generation < target_generation "
        "AND renderer_version = %s AND target_kind <> 'layout' "
        "ORDER BY updated_at LIMIT %s",
        (expected_renderer_version, limit),
    )
    return tuple(_target_key_from_row(row) for row in cur.fetchall())


def clean_targets(
    cur: Cursor[TupleRow],
    expected_renderer_version: str,
) -> tuple[TargetKey, ...]:
    """Return current non-layout identities for startup file repair."""
    cur.execute(
        "SELECT target_kind, channel_id, local_day, thread_ts "
        "FROM projection_targets WHERE rendered_generation >= target_generation "
        "AND renderer_version = %s AND target_kind <> 'layout'",
        (expected_renderer_version,),
    )
    return tuple(_target_key_from_row(row) for row in cur.fetchall())


def layout_needs_reconciliation(
    cur: Cursor[TupleRow],
    expected_renderer_version: str,
) -> bool:
    """Return whether the singleton needs its rare fan-out pass."""
    cur.execute(
        "SELECT 1 FROM projection_targets WHERE target_kind = 'layout' "
        "AND channel_id IS NULL AND local_day IS NULL AND thread_ts IS NULL "
        "AND renderer_version = %s AND rendered_generation >= target_generation",
        (expected_renderer_version,),
    )
    return cur.fetchone() is None


def target_generation_for_render(
    cur: Cursor[TupleRow],
    key: TargetKey,
    expected_renderer_version: str,
) -> int | None:
    """Snapshot the pending generation to render, without holding a row lock.

    The completion UPDATE is the actual compare-and-set. Holding a PostgreSQL
    row lock across rendering and filesystem I/O would only delay invalidation
    commits and is unnecessary for correctness.
    """
    cur.execute(
        "SELECT target_generation, rendered_generation, renderer_version "
        "FROM projection_targets WHERE target_kind = %s "
        "AND channel_id IS NOT DISTINCT FROM %s "
        "AND local_day IS NOT DISTINCT FROM %s "
        "AND thread_ts IS NOT DISTINCT FROM %s",
        _target_params(key),
    )
    row = cur.fetchone()
    if row is None or str(row[2]) != expected_renderer_version:
        return None
    target_generation = int(row[0])
    return target_generation if int(row[1]) < target_generation else None


def mark_target_rendered(
    cur: Cursor[TupleRow],
    key: TargetKey,
    rendered_generation: int,
    expected_renderer_version: str,
) -> bool:
    """CAS one target's rendered generation after its backing-file replace."""
    cur.execute(
        "UPDATE projection_targets SET rendered_generation = %s, "
        "renderer_version = %s, updated_at = now() "
        "WHERE target_kind = %s "
        "AND channel_id IS NOT DISTINCT FROM %s "
        "AND local_day IS NOT DISTINCT FROM %s "
        "AND thread_ts IS NOT DISTINCT FROM %s "
        "AND target_generation = %s AND renderer_version = %s",
        (
            rendered_generation,
            expected_renderer_version,
            *_target_params(key),
            rendered_generation,
            expected_renderer_version,
        ),
    )
    return cur.rowcount == 1


def ensure_targets_pending(
    cur: Cursor[TupleRow],
    targets: Iterable[TargetKey],
    renderer_version: str,
) -> tuple[TargetKey, ...]:
    """Insert missing identities conservatively pending at generation one."""
    inserted: list[TargetKey] = []
    for target in dict.fromkeys(targets):
        cur.execute(
            "INSERT INTO projection_targets ("
            "  target_kind, channel_id, local_day, thread_ts, "
            "  target_generation, rendered_generation, renderer_version"
            ") VALUES (%s, %s, %s, %s, 1, 0, %s) "
            "ON CONFLICT ON CONSTRAINT projection_targets_identity DO NOTHING "
            "RETURNING target_kind",
            (*_target_params(target), renderer_version),
        )
        if cur.fetchone() is not None:
            inserted.append(target)
    return tuple(inserted)


def reconcile_renderer_epoch(
    cur: Cursor[TupleRow],
    expected_renderer_version: str,
) -> tuple[TargetKey, ...]:
    """Make every old renderer epoch current-but-pending exactly once."""
    cur.execute(
        "UPDATE projection_targets SET "
        "target_generation = target_generation + 1, renderer_version = %s, updated_at = now() "
        "WHERE renderer_version <> %s "
        "RETURNING target_kind, channel_id, local_day, thread_ts",
        (expected_renderer_version, expected_renderer_version),
    )
    return tuple(_target_key_from_row(row) for row in cur.fetchall())


def reconcile_layout_generation(
    cur: Cursor[TupleRow],
    discovered_targets: Iterable[TargetKey],
    expected_renderer_version: str,
) -> tuple[int | None, tuple[TargetKey, ...]]:
    """Fan one pending layout generation out to every materialized target.

    The layout row is locked only for this short SQL transaction. The caller
    keeps it pending while filesystem cleanup and kernel invalidation run,
    then conditionally marks this captured generation reconciled.
    """
    cur.execute(
        "SELECT target_generation, rendered_generation, renderer_version "
        "FROM projection_targets WHERE target_kind = 'layout' "
        "AND channel_id IS NULL AND local_day IS NULL AND thread_ts IS NULL FOR UPDATE"
    )
    row = cur.fetchone()
    if row is None:
        ensure_targets_pending(
            cur,
            (TargetKey("layout", None, None, None),),
            expected_renderer_version,
        )
        target_generation = 1
        rendered_generation = 0
        renderer_version = expected_renderer_version
    else:
        target_generation = int(row[0])
        rendered_generation = int(row[1])
        renderer_version = str(row[2])
    if renderer_version != expected_renderer_version:
        # Normally handled by the startup epoch pass. Keeping this local guard
        # makes a direct lifecycle caller fail closed too.
        target_generation += 1
        cur.execute(
            "UPDATE projection_targets SET target_generation = %s, renderer_version = %s, "
            "updated_at = now() WHERE target_kind = 'layout'",
            (target_generation, expected_renderer_version),
        )
    if rendered_generation >= target_generation:
        return None, ()

    ensure_targets_pending(
        cur,
        (target for target in discovered_targets if target.target_kind != "layout"),
        expected_renderer_version,
    )
    cur.execute(
        "UPDATE projection_targets SET target_generation = target_generation + 1, "
        "renderer_version = %s, updated_at = now() WHERE target_kind <> 'layout' "
        "RETURNING target_kind, channel_id, local_day, thread_ts",
        (expected_renderer_version,),
    )
    affected = tuple(_target_key_from_row(target_row) for target_row in cur.fetchall())
    return target_generation, affected


def mark_layout_reconciled(
    cur: Cursor[TupleRow],
    target_generation: int,
    expected_renderer_version: str,
) -> bool:
    """CAS one layout generation clean after orphan cleanup and invalidation.

    Fan-out and cleanup deliberately happen first while the singleton remains
    pending. A concurrent layout bump makes this UPDATE affect zero rows and
    leaves the newer generation pending for another reconciliation pass.
    """
    cur.execute(
        "UPDATE projection_targets SET rendered_generation = %s, renderer_version = %s, "
        "updated_at = now() WHERE target_kind = 'layout' "
        "AND channel_id IS NULL AND local_day IS NULL AND thread_ts IS NULL "
        "AND target_generation = %s AND renderer_version = %s",
        (
            target_generation,
            expected_renderer_version,
            target_generation,
            expected_renderer_version,
        ),
    )
    return cur.rowcount == 1


def bump_targets(
    cur: Cursor[TupleRow],
    targets: Iterable[TargetKey],
    renderer_version: str,
) -> None:
    """Increment each distinct target generation, inserting absent rows.

    Generation 1 is the conservative pre-bump baseline. A target first seen
    because of a source-data mutation is therefore inserted at generation 2;
    subsequent mutations increment the stored generation.
    """
    for target in dict.fromkeys(targets):
        cur.execute(
            "INSERT INTO projection_targets ("
            "  target_kind, channel_id, local_day, thread_ts, target_generation, renderer_version"
            ") VALUES (%s, %s, %s, %s, 2, %s) "
            "ON CONFLICT ON CONSTRAINT projection_targets_identity DO UPDATE SET "
            "  target_generation = projection_targets.target_generation + 1, "
            "  renderer_version = EXCLUDED.renderer_version, "
            "  updated_at = now()",
            (
                target.target_kind,
                target.channel_id,
                target.local_day,
                target.thread_ts,
                renderer_version,
            ),
        )


def targets_for_apply_result(result: ApplyResult, tz: ZoneInfo) -> tuple[TargetKey, ...]:
    """Map an apply result to stable day, thread, and layout identities.

    Every top-level chunk owns both its day materialization and a potential
    thread materialization. The latter intentionally covers reply-before-parent:
    a reply can dirty the stable thread identity before its path is resolvable.
    """
    targets: list[TargetKey] = []
    for ref in result.chunks:
        local_day = _local_day_for_ts(ref.message_ts, tz)
        targets.append(TargetKey("day", ref.channel_id, local_day, None))
        targets.append(TargetKey("thread", ref.channel_id, local_day, ref.message_ts))
    for ref in result.thread_chunks:
        local_day = _local_day_for_ts(ref.thread_ts, tz)
        targets.append(TargetKey("thread", ref.channel_id, local_day, ref.thread_ts))
    if result.channel_list_changed:
        targets.append(TargetKey("layout", None, None, None))
    return tuple(dict.fromkeys(targets))


def bump_channel_meta_target(
    cur: Cursor[TupleRow],
    channel_id: str,
    renderer_version: str,
) -> None:
    """Bump one channel metadata target inside the caller's transaction."""
    bump_targets(
        cur,
        (TargetKey("channel-meta", channel_id, None, None),),
        renderer_version,
    )


def bump_channel_visibility_targets(
    cur: Cursor[TupleRow],
    channel_id: str,
    renderer_version: str,
) -> None:
    """Bump channel metadata and global layout for a visibility transition."""
    bump_targets(
        cur,
        (
            TargetKey("channel-meta", channel_id, None, None),
            TargetKey("layout", None, None, None),
        ),
        renderer_version,
    )


def _local_day_for_ts(ts: Decimal, tz: ZoneInfo) -> date:
    return datetime.fromtimestamp(float(ts), tz=UTC).astimezone(tz).date()


def _path_parts(path: str) -> tuple[str, ...]:
    if not path or "\x00" in path:
        return ()
    candidate = PurePosixPath(f"/{path.lstrip('/')}")
    if any(part in ("", ".", "..") for part in candidate.parts[1:]):
        return ()
    return candidate.parts[1:]


def _target_params(key: TargetKey) -> tuple[object, ...]:
    return (key.target_kind, key.channel_id, key.local_day, key.thread_ts)


def _target_key_from_row(row: tuple[object, ...]) -> TargetKey:
    return TargetKey(
        cast("TargetKind", str(row[0])),
        None if row[1] is None else str(row[1]),
        cast("date | None", row[2]),
        None if row[3] is None else Decimal(str(row[3])),
    )


def _fetch_channel_row(cur: Cursor[TupleRow], channel_id: str) -> ChannelRow | None:
    cur.execute(
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


__all__ = [
    "RENDERER_VERSION",
    "TargetCleanResult",
    "TargetKey",
    "bump_channel_meta_target",
    "bump_channel_visibility_targets",
    "bump_targets",
    "clean_targets",
    "ensure_targets_pending",
    "is_target_clean",
    "layout_needs_reconciliation",
    "mark_layout_reconciled",
    "mark_target_rendered",
    "path_for_target",
    "pending_targets",
    "reconcile_layout_generation",
    "reconcile_renderer_epoch",
    "target_clean_result",
    "target_generation_for_render",
    "target_key_for_path",
    "targets_for_apply_result",
]
