"""HTTP snapshot fetch client.

Per RFC §Wire protocol → Snapshot delivery via HTTP. The WS server emits
`snapshot_at { stream, at, url }` when `since` is too far behind for cheap
event replay; this module fetches the snapshot over HTTP (`GET
/streams/<stream-id>/snapshot?at=<offset>`), streams the JSONL response, and
applies every line as a synthetic `message` event in a **single postgres
transaction**.

Atomicity matters: the cursor advance to `at` is part of the same TX as the
chunk writes. If the fetch or apply fails partway, the TX rolls back and the
next subscribe re-tries the snapshot from the same cursor — no partial chunks,
no orphaned advance.

Concurrency: this runs out-of-band from the per-stream event applier (the
applier is suspended for the duration via the WS client's stream-state
bookkeeping). To keep the apply genuinely transactional we toggle the
connection to non-autocommit for the duration; restore autocommit on exit.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from typing import cast
from urllib.parse import urljoin

import httpx
import trio
from psycopg import Connection
from psycopg.rows import TupleRow

from slack_fuse.models import JsonObject
from slack_fuse.projector.apply import (
    ApplyResult,
    InvalidationSink,
    NullInvalidationSink,
    apply_snapshot_row,
    require_autocommit,
)
from slack_fuse.projector.cursor import advance_cursor

log = logging.getLogger(__name__)


class SnapshotFetchError(Exception):
    """The snapshot fetch or apply failed; caller should retry from the cursor."""


@dataclass(frozen=True, slots=True)
class SnapshotResult:
    """Telemetry for one snapshot apply."""

    stream: str
    at_offset: int
    records_applied: int


@dataclass(frozen=True, slots=True)
class SnapshotRedirect:
    """The `snapshot_at` frame's payload, in storage-friendly form."""

    stream: str
    at_offset: int
    url: str


async def fetch_and_apply_snapshot(
    http: httpx.AsyncClient,
    conn: Connection[TupleRow],
    redirect: SnapshotRedirect,
    *,
    base_url: str | None = None,
    sink: InvalidationSink | None = None,
) -> SnapshotResult:
    """Fetch a snapshot, apply every JSONL row, advance the cursor — one TX.

    `url` is whatever the server sent in the `snapshot_at` frame (typically a
    relative path like `/streams/channel%3AC.../snapshot?at=...`). `base_url`
    resolves relative URLs against the server's HTTP origin; pass `None` if
    `url` is already absolute.
    """
    require_autocommit(conn)
    target_url = redirect.url if base_url is None else urljoin(base_url, redirect.url)
    sink_or_default: InvalidationSink = sink if sink is not None else NullInvalidationSink()

    async with http.stream("GET", target_url) as response:
        response.raise_for_status()
        body_bytes = await response.aread()

    lines = [line for line in body_bytes.decode("utf-8").splitlines() if line.strip()]
    if not lines:
        # Empty snapshot still advances the cursor so we don't re-fetch on
        # reconnect; one tiny TX covers it.
        await trio.to_thread.run_sync(_apply_empty_snapshot, conn, redirect.stream, redirect.at_offset)
        return SnapshotResult(stream=redirect.stream, at_offset=redirect.at_offset, records_applied=0)

    invalidations = await trio.to_thread.run_sync(
        _apply_snapshot_sync, conn, redirect.stream, redirect.at_offset, tuple(lines)
    )
    _fire_invalidations(sink_or_default, invalidations)
    return SnapshotResult(stream=redirect.stream, at_offset=redirect.at_offset, records_applied=len(lines))


def _apply_empty_snapshot(conn: Connection[TupleRow], stream: str, at_offset: int) -> None:
    with conn.transaction(), conn.cursor() as cur:
        advance_cursor(cur, stream, at_offset)


def _apply_snapshot_sync(
    conn: Connection[TupleRow],
    stream: str,
    at_offset: int,
    lines: tuple[str, ...],
) -> list[ApplyResult]:
    """Apply every JSONL row in one TX. Returns post-commit invalidations.

    The shape of each row matches a Slack `Message` payload (per RFC: snapshot
    apply re-uses the live-event `message` projection code via
    `apply_snapshot_row`).
    """
    results: list[ApplyResult] = []
    with conn.transaction(), conn.cursor() as cur:
        for raw in lines:
            try:
                row = json.loads(raw)
            except json.JSONDecodeError as exc:
                msg = f"snapshot for {stream}: malformed JSONL"
                raise SnapshotFetchError(msg) from exc
            if not isinstance(row, dict):
                msg = f"snapshot for {stream}: row is not an object"
                raise SnapshotFetchError(msg)
            results.append(apply_snapshot_row(cur, stream, cast(JsonObject, row)))
        advance_cursor(cur, stream, at_offset)
    return results


def _fire_invalidations(sink: InvalidationSink, results: Iterable[ApplyResult]) -> None:
    for result in results:
        for ref in result.chunks:
            sink.chunk_changed(ref)
        for thread_ref in result.thread_chunks:
            sink.thread_chunk_changed(thread_ref)
        if result.channel_list_changed:
            sink.channel_list_changed()
