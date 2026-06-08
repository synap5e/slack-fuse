"""HTTP snapshot fetch client.

Per RFC §Wire protocol → Snapshot delivery via HTTP. The WS server emits
`snapshot_at { stream, at, url }` when `since` is too far behind for cheap
event replay; this module fetches the snapshot over HTTP (`GET
/streams/<stream-id>/snapshot?at=<offset>`), streams the JSONL response, and
applies it as a **full-state replacement** in a **single postgres
transaction**.

Full-state replacement (review P0-B). A snapshot is the *current materialised
state* of a stream at offset `at` — it is authoritative, not additive. Applying
it as upserts alone is wrong: a row present locally but absent from the snapshot
(e.g. a message deleted by an event the client never saw before disconnecting)
would survive forever once the cursor advanced past the delete. So before
upserting the snapshot rows we DELETE the channel's `chunks` / `thread_chunks`
whose keys are absent from the snapshot (their `chunk_mentions` cascade away via
FK). The delete + upserts + cursor advance are one TX, so a crash mid-apply
leaves the prior consistent state, not a half-replaced one.

This holds even for an *empty* channel snapshot (review hole 1): a zero-row body
is the authoritative "this channel now has no messages" state, so a channel
stream always runs the full-replacement path (deleting every local chunk) rather
than the cursor-only short-circuit — only singleton streams (`users` /
`channel-list`) take the empty short-circuit, since the server gate replays them
instead of redirecting to an empty snapshot.

The delete itself surfaces the removed keys (review hole 2): a snapshot that
clears the last chunk for a materialised file leaves no surviving upsert to
carry the invalidation, so deleted `ChunkRef` / `ThreadChunkRef`s are collected
via `RETURNING` and fired through the sink post-commit alongside the upsert refs.

Atomicity matters: the cursor advance to `at` is part of the same TX as the
chunk writes. If the fetch or apply fails partway, the TX rolls back and the
next subscribe re-tries the snapshot from the same cursor — no partial chunks,
no orphaned advance.

Singleton streams (review P0-C). Only `channel:<id>` snapshots are consumable
by this client. The server is responsible for never issuing `snapshot_at` for
the `users` / `channel-list` singleton streams (it replays them instead). If a
stale server does, `apply_snapshot_row` raises `ValueError`, which the WS client
catches and logs rather than tearing the connection down.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import cast
from urllib.parse import urljoin

import httpx
import trio
from psycopg import Connection, Cursor
from psycopg.rows import TupleRow

from slack_fuse.models import JsonObject
from slack_fuse.projector.apply import (
    ApplyResult,
    ChunkRef,
    InvalidationSink,
    NullInvalidationSink,
    ThreadChunkRef,
    apply_snapshot_row,
    require_autocommit,
)
from slack_fuse.projector.cursor import advance_cursor

log = logging.getLogger(__name__)

_CHANNEL_STREAM_PREFIX = "channel:"


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
    if not lines and not redirect.stream.startswith(_CHANNEL_STREAM_PREFIX):
        # Singleton streams (`users` / `channel-list`): the server gate never
        # redirects these to a snapshot for the empty case (it replays them
        # instead), so an empty body just advances the cursor. One tiny TX.
        await trio.to_thread.run_sync(_apply_empty_snapshot, conn, redirect.stream, redirect.at_offset)
        return SnapshotResult(stream=redirect.stream, at_offset=redirect.at_offset, records_applied=0)

    # Channel streams ALWAYS go through the full-replacement path, even when the
    # body is empty (review P0-B): an empty channel snapshot is the authoritative
    # "this channel now has zero messages" state, so it must DELETE every local
    # chunk/thread_chunk for the channel — not merely advance the cursor, which
    # would orphan stale rows the client never saw deleted.
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
    """Apply the snapshot as a full-state replacement in one TX.

    The shape of each row matches a Slack `Message` payload (per RFC: snapshot
    apply re-uses the live-event `message` projection code via
    `apply_snapshot_row`). Before upserting, stale local rows absent from the
    snapshot are deleted (review P0-B) so the projection matches the
    authoritative server state at `at_offset` rather than the union of old + new.
    """
    results: list[ApplyResult] = []
    with conn.transaction(), conn.cursor() as cur:
        rows = [_decode_row(stream, raw) for raw in lines]
        if stream.startswith(_CHANNEL_STREAM_PREFIX):
            channel_id = stream.removeprefix(_CHANNEL_STREAM_PREFIX)
            results.append(_delete_chunks_absent_from_snapshot(cur, channel_id, rows))
        for row in rows:
            results.append(apply_snapshot_row(cur, stream, row))
        advance_cursor(cur, stream, at_offset)
    return results


def _decode_row(stream: str, raw: str) -> JsonObject:
    try:
        row = json.loads(raw)
    except json.JSONDecodeError as exc:
        msg = f"snapshot for {stream}: malformed JSONL"
        raise SnapshotFetchError(msg) from exc
    if not isinstance(row, dict):
        msg = f"snapshot for {stream}: row is not an object"
        raise SnapshotFetchError(msg)
    return cast(JsonObject, row)


def _snapshot_ts(value: object) -> Decimal | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return Decimal(value)
    except (ValueError, ArithmeticError):
        return None


def _delete_chunks_absent_from_snapshot(
    cur: Cursor[TupleRow],
    channel_id: str,
    rows: Sequence[JsonObject],
) -> ApplyResult:
    """Delete the channel's chunks/thread_chunks whose keys are not in the snapshot.

    A top-level message (or thread parent: `thread_ts` unset or == `ts`) keeps a
    `chunks` row keyed by `message_ts`; a thread reply (`thread_ts` set and !=
    `ts`) keeps a `thread_chunks` row keyed by `(thread_ts, reply_ts)`. Empty
    keep-sets delete everything for the channel — exactly what a snapshot with
    no top-level messages / no replies should produce. `chunk_mentions` /
    `thread_chunk_mentions` cascade away via their FKs.

    Returns the deleted rows as an `ApplyResult` (review hole 2): a snapshot that
    removes the last chunk(s) for a materialised day/thread file has no surviving
    upsert to carry the invalidation, so the deleted keys are surfaced via
    `RETURNING` and fired through the sink post-commit. Without this the DB is
    correct but the kernel page cache (V2 `fi.keep_cache=True`) serves the stale
    pre-delete bytes until something else touches the path.
    """
    keep_top: list[Decimal] = []
    keep_thread_parent: list[Decimal] = []
    keep_thread_reply: list[Decimal] = []
    for row in rows:
        ts = _snapshot_ts(row.get("ts"))
        if ts is None:
            continue
        thread_ts = _snapshot_ts(row.get("thread_ts"))
        if thread_ts is not None and thread_ts != ts:
            keep_thread_parent.append(thread_ts)
            keep_thread_reply.append(ts)
        else:
            keep_top.append(ts)
    cur.execute(
        "DELETE FROM chunks WHERE channel_id = %s AND message_ts <> ALL(%s::numeric[]) RETURNING message_ts",
        (channel_id, keep_top),
    )
    deleted_top = tuple(ChunkRef(channel_id, cast(Decimal, row[0])) for row in cur.fetchall())
    cur.execute(
        "DELETE FROM thread_chunks WHERE channel_id = %s "
        "AND (thread_ts, reply_ts) NOT IN (SELECT * FROM unnest(%s::numeric[], %s::numeric[])) "
        "RETURNING thread_ts, reply_ts",
        (channel_id, keep_thread_parent, keep_thread_reply),
    )
    deleted_thread = tuple(
        ThreadChunkRef(channel_id, cast(Decimal, row[0]), cast(Decimal, row[1])) for row in cur.fetchall()
    )
    return ApplyResult(chunks=deleted_top, thread_chunks=deleted_thread)


def _fire_invalidations(sink: InvalidationSink, results: Iterable[ApplyResult]) -> None:
    for result in results:
        for ref in result.chunks:
            sink.chunk_changed(ref)
        for thread_ref in result.thread_chunks:
            sink.thread_chunk_changed(thread_ref)
        if result.channel_list_changed:
            sink.channel_list_changed()
