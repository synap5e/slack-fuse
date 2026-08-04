"""Transactional invalidation ledger for projected materializations.

This module only computes stable target identities and bumps their desired
generations. Readers and the coalescer deliberately do not consume the ledger
until the projection cutover in PR 3.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Final, Literal
from zoneinfo import ZoneInfo

from psycopg import Cursor
from psycopg.rows import TupleRow

if TYPE_CHECKING:
    from slack_fuse.projector.apply import ApplyResult


type TargetKind = Literal["channel-meta", "day", "thread", "layout"]

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


__all__ = [
    "RENDERER_VERSION",
    "TargetKey",
    "bump_channel_meta_target",
    "bump_channel_visibility_targets",
    "bump_targets",
    "targets_for_apply_result",
]
