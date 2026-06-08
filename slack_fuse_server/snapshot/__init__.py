"""Snapshot generation: materialise per-stream current state from the events log.

Per RFC §Snapshot delivery via HTTP + §Snapshot vs event replay decision. Cold
consumers catch up by fetching a snapshot at offset `M` rather than replaying
every event from 0. The server periodically (Sprint 2D) materialises these
snapshots; the HTTP `/snapshot` endpoint (Sprint 3A) streams them as JSONL and
writes the `snapshot_uses` rows.

The public surface:

- `generate_snapshot` — build + persist one snapshot for a stream, reading the
  events log in a `REPEATABLE READ` transaction so concurrent writes land in
  the *next* snapshot, not this one.
- `project_stream` — the pure fold from an ordered event list to the
  current-state line objects the snapshot stores (one JSONL line per item).
- `canonical_json` / `to_jsonl` — the deterministic serialisation the cost
  columns and the `/snapshot` endpoint share.
- `SnapshotScheduler` — the periodic trio worker wired into the slurper nursery.
"""

from __future__ import annotations

from slack_fuse_server.snapshot.generator import (
    CHANNEL_LIST_STREAM,
    USERS_STREAM,
    GenerationTrigger,
    SnapshotResult,
    canonical_json,
    generate_snapshot,
    is_projectable_stream,
    project_stream,
    to_jsonl,
)
from slack_fuse_server.snapshot.scheduler import (
    SnapshotScheduler,
    decide_trigger,
)

__all__ = [
    "CHANNEL_LIST_STREAM",
    "USERS_STREAM",
    "GenerationTrigger",
    "SnapshotResult",
    "SnapshotScheduler",
    "canonical_json",
    "decide_trigger",
    "generate_snapshot",
    "is_projectable_stream",
    "project_stream",
    "to_jsonl",
]
