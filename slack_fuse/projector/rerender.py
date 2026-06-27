"""On-demand chunk rerender for one channel.

Re-derives a channel's ``chunks`` / ``thread_chunks`` from the server's latest
snapshot using the **current** renderer code. The use case: a renderer change
ships (e.g. attachment rendering landed 2026-06-27), so every *new* event
renders correctly going forward, but chunks already materialised with the old
renderer stay stuck. Rerender refreshes them.

Why the snapshot path
---------------------
The ``chunks`` table is client-side state; it stores only the rendered
``content_md``, not the raw ``Message`` payload, so we cannot re-render from
local state alone. The server's per-stream snapshot is the *fold* of every
undeleted message up to a snapshot offset, delivered as raw ``message``-shaped
JSONL rows — exactly the input ``render_message_structural`` consumes. Applying
each row through :func:`apply_snapshot_row` re-renders it with the renderer
compiled into *this* process. So a snapshot apply IS a rerender.

Upsert-only — NOT a full snapshot resync
-----------------------------------------
This deliberately does **not** reuse ``snapshot_fetch.fetch_and_apply_snapshot``.
That path is a full-state *resync*: it DELETEs local chunks absent from the
snapshot and advances the stream cursor to the snapshot offset. Both are wrong
for a rerender:

* The latest persisted snapshot sits at offset ``M`` which lags the live head
  ``H`` (snapshots are periodic — every ``snapshot_every_n_events`` or
  ``snapshot_max_age_hours``). Deleting chunks "absent from the snapshot" would
  wipe the ``(M, H]`` tail of recent messages — real data the live projector
  already applied — and nothing would restore them.
* Advancing the cursor would collide with the live projector's cursor for the
  same stream (it sits at ``H``); ``GREATEST`` would ignore the regress, but
  touching the cursor at all is needless coupling.

So rerender is purely additive: re-render every row the snapshot carries
(``ON CONFLICT DO UPDATE``), leave everything else — and the cursor — alone.
Consequence: messages newer than the latest snapshot (``(M, H]``) are not
touched here. That is fine for the renderer-change use case: those were applied
by the live projector *after* the new renderer was deployed, so they already
carry current output. Historical chunks (the stuck ones) are all ``<= M`` and
get refreshed.

Concurrency
-----------
Runs against its own autocommit connection. Live events keep flowing through the
projector's per-stream applier on a separate connection; both write ``chunks``
via ``ON CONFLICT DO UPDATE`` so there is no offset corruption and no lost
updates beyond a last-writer-wins race on a single message edited *during* the
sub-second apply (self-heals on the next edit). The cursor is never touched, so
the live applier's catch-up position is unaffected.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import cast
from urllib.parse import quote

import httpx
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
from slack_fuse.projector.cursor import read_cursor

log = logging.getLogger(__name__)

#: Snapshot fetch + apply runs in a background task / standalone CLI, not inside
#: the FUSE per-callback budget, so the timeout is generous (a large channel's
#: snapshot can be a few MB of gzipped JSONL).
DEFAULT_RERENDER_TIMEOUT_S = 30.0


@dataclass(frozen=True, slots=True)
class RerenderResult:
    """Outcome of one :func:`rerender_channel` call.

    ``status`` is a stable verb the control surface records verbatim in
    ``_control/status``:

    * ``rerendered`` — snapshot applied; ``chunks`` / ``thread_chunks`` count
      the re-rendered rows.
    * ``no_snapshot`` — the server has no snapshot at/below the channel's
      applied offset yet (channel too new, or never caught up). Nothing to do.
    * ``server_unavailable`` — transport error reaching the server.
    * ``malformed`` — the snapshot body was not valid JSONL (the apply TX rolled
      back; no partial rows).
    * ``http_<code>`` — any other non-200 response.
    """

    channel_id: str
    status: str
    chunks: int = 0
    thread_chunks: int = 0


class _MalformedSnapshotError(Exception):
    """A snapshot row was not decodable JSON; abort the apply TX."""


def _auth_headers(shared_secret: str | None) -> dict[str, str]:
    """Bearer header when a secret is configured; empty otherwise (matches
    ``refresh_fetch._auth_headers`` and the server's home-lab no-auth default)."""
    if not shared_secret:
        return {}
    return {"Authorization": f"Bearer {shared_secret}"}


def _snapshot_url(base_http_url: str, stream: str, at_offset: int) -> str:
    """``{base}/streams/<url-encoded stream>/snapshot?at=<offset>``.

    Mirrors the server's ``build_snapshot_url`` format (kept inline to avoid
    importing the server's HTTP package into the client read path)."""
    encoded = quote(stream, safe="")
    return f"{base_http_url.rstrip('/')}/streams/{encoded}/snapshot?at={at_offset}"


def rerender_channel(  # noqa: PLR0913 — sync HTTP call needs client + url + conn + channel + auth/sink/timeout.
    http_client: httpx.Client,
    base_http_url: str,
    conn: Connection[TupleRow],
    channel_id: str,
    *,
    shared_secret: str | None = None,
    sink: InvalidationSink | None = None,
    timeout_s: float = DEFAULT_RERENDER_TIMEOUT_S,
) -> RerenderResult:
    """Re-render ``channel_id``'s chunks from the server's latest snapshot.

    Synchronous (sync httpx + sync psycopg) so it runs unchanged from the CLI
    and from a FUSE background task dispatched via ``trio.to_thread.run_sync``.
    ``conn`` must be autocommit (see :func:`require_autocommit`); the apply runs
    in a single transaction on it. Returns a :class:`RerenderResult`; never
    raises for an unreachable server or a missing snapshot (those map to a
    status verb).
    """
    require_autocommit(conn)
    stream = f"channel:{channel_id}"
    sink_or_default: InvalidationSink = sink if sink is not None else NullInvalidationSink()

    # Request the latest snapshot at/below the channel's applied offset. When
    # the live projector is caught up this is the most recent snapshot; the
    # server returns the newest snapshot row with at_offset <= this value.
    with conn.cursor() as cur:
        at_offset = read_cursor(cur, stream)

    url = _snapshot_url(base_http_url, stream, at_offset)
    try:
        response = http_client.get(url, headers=_auth_headers(shared_secret), timeout=timeout_s)
    except httpx.HTTPError:
        log.warning("rerender %s: snapshot fetch transport error", channel_id)
        return RerenderResult(channel_id, status="server_unavailable")

    if response.status_code == 404:
        # No snapshot yet (channel too new / never caught up). Anything already
        # in chunks for this channel is post-snapshot and was rendered live.
        return RerenderResult(channel_id, status="no_snapshot")
    if response.status_code != 200:
        log.warning("rerender %s: snapshot fetch returned %d", channel_id, response.status_code)
        return RerenderResult(channel_id, status=f"http_{response.status_code}")

    lines = tuple(line for line in response.text.splitlines() if line.strip())
    try:
        results = _apply_rerender(conn, stream, lines)
    except _MalformedSnapshotError:
        log.warning("rerender %s: malformed snapshot body; apply rolled back", channel_id)
        return RerenderResult(channel_id, status="malformed")

    _fire_invalidations(sink_or_default, results)
    chunk_count = sum(len(r.chunks) for r in results)
    thread_count = sum(len(r.thread_chunks) for r in results)
    log.info(
        "rerender %s: re-rendered %d chunk(s) / %d thread-chunk(s) from snapshot at <= %d",
        channel_id,
        chunk_count,
        thread_count,
        at_offset,
    )
    return RerenderResult(channel_id, status="rerendered", chunks=chunk_count, thread_chunks=thread_count)


def _apply_rerender(conn: Connection[TupleRow], stream: str, lines: Sequence[str]) -> list[ApplyResult]:
    """Re-apply every snapshot row in ONE transaction — upsert-only.

    No delete-absent and no cursor advance (see module docstring). Each row is
    re-rendered through ``apply_snapshot_row`` → ``render_message_structural``
    with the current renderer, then upserted via ``ON CONFLICT DO UPDATE``.
    """
    results: list[ApplyResult] = []
    with conn.transaction(), conn.cursor() as cur:
        for raw in lines:
            results.append(apply_snapshot_row(cur, stream, _decode_row(raw)))
    return results


def _decode_row(raw: str) -> JsonObject:
    try:
        row = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise _MalformedSnapshotError from exc
    if not isinstance(row, dict):
        raise _MalformedSnapshotError
    return cast("JsonObject", row)


def _fire_invalidations(sink: InvalidationSink, results: Iterable[ApplyResult]) -> None:
    for result in results:
        for ref in result.chunks:
            sink.chunk_changed(ref)
        for thread_ref in result.thread_chunks:
            sink.thread_chunk_changed(thread_ref)


__all__ = ["DEFAULT_RERENDER_TIMEOUT_S", "RerenderResult", "rerender_channel"]
