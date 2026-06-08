"""Append-only JSONL log of per-read trailer decisions (Sprint 3C).

Minimal bake-in observability: one JSON object per FUSE markdown read,
recording the :class:`~slack_fuse.projector.trailer.TrailerDecision` so the
trailer false-positive rate can be measured offline. Clean reads are logged too
— the rate is ``stale-or-fallback / all`` reads.

Durability: writes go through a single fd opened ``O_APPEND | O_CLOEXEC`` and
each record is one ``os.write`` of a ``<json>\\n`` line. ``O_APPEND`` makes the
kernel position the write at end-of-file atomically, and a single ``write`` of
a line shorter than ``PIPE_BUF`` is atomic, so concurrent readers/writers never
interleave a partial line and a crash loses at most the in-flight record (never
a corrupt one). No userspace buffering, so nothing is lost on an unclean exit.

Rotation is intentionally NOT implemented here — the operator handles it via
logrotate / cron (a ``copytruncate`` or rename+reopen). This module only emits.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, cast

from slack_fuse.projector.trailer import TrailerDecision, TrailerKind

if TYPE_CHECKING:
    from pathlib import Path


def _dt_to_json(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt is not None else None


def _dt_from_json(raw: object) -> datetime | None:
    if raw is None:
        return None
    return datetime.fromisoformat(str(raw))


def _opt_int(raw: object) -> int | None:
    if raw is None:
        return None
    if isinstance(raw, (int, str)):
        return int(raw)
    msg = f"expected int-like, got {type(raw).__name__}"
    raise ValueError(msg)


def _opt_str(raw: object) -> str | None:
    return None if raw is None else str(raw)


def decision_to_json(decision: TrailerDecision) -> dict[str, object]:
    """Serialize a decision to a JSON-ready dict (ISO-8601 timestamps)."""
    return {
        "kind": decision.kind,
        "reasons": list(decision.reasons),
        "stream": decision.stream,
        "inode": decision.inode,
        "at": _dt_to_json(decision.at),
        "last_frame_at": _dt_to_json(decision.last_frame_at),
        "last_health": decision.last_health,
        "caught_up_offset": decision.caught_up_offset,
    }


def decision_from_json(obj: dict[str, object]) -> TrailerDecision:
    """Parse a decision back from a JSONL line's decoded object.

    Inverse of :func:`decision_to_json`; the round-trip is exercised in the
    tests so the on-disk format stays stable.
    """
    kind = str(obj["kind"])
    if kind not in ("clean", "stale", "fallback"):  # pragma: no cover - guards malformed input
        msg = f"unknown trailer decision kind {kind!r}"
        raise ValueError(msg)
    reasons_raw = obj.get("reasons")
    reasons = [str(r) for r in cast("list[object]", reasons_raw)] if isinstance(reasons_raw, list) else []
    kind_typed: TrailerKind = kind
    return TrailerDecision(
        kind=kind_typed,
        reasons=reasons,
        stream=str(obj.get("stream", "")),
        inode=_opt_int(obj.get("inode")),
        at=_dt_from_json(obj.get("at")),
        last_frame_at=_dt_from_json(obj.get("last_frame_at")),
        last_health=_opt_str(obj.get("last_health")),
        caught_up_offset=_opt_int(obj.get("caught_up_offset")),
    )


@dataclass(slots=True)
class TrailerLog:
    """An open append-only JSONL sink for trailer decisions.

    Construct via :meth:`open`; close via :meth:`close`. Safe to share across
    the FUSE worker threads — ``os.write`` to an ``O_APPEND`` fd is atomic per
    call for sub-``PIPE_BUF`` lines, so no userspace lock is needed.
    """

    _fd: int
    _closed: bool = False

    @classmethod
    def open(cls, path: Path) -> TrailerLog:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_CLOEXEC, 0o644)
        return cls(_fd=fd)

    def write(self, decision: TrailerDecision) -> None:
        if self._closed:
            return
        line = json.dumps(decision_to_json(decision), separators=(",", ":")) + "\n"
        _ = os.write(self._fd, line.encode("utf-8"))

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        os.close(self._fd)


__all__ = ["TrailerLog", "decision_from_json", "decision_to_json"]
