"""Snapshot endpoint helpers: DB lookup, JSONL encoding, gzip payload.

Used by:

- HTTP `GET /streams/<id>/snapshot?at=<offset>[&since=<offset>]`
- WS subscribe fallback (`snapshot_at` redirects)
"""

from __future__ import annotations

import gzip
from dataclasses import dataclass
from typing import cast
from urllib.parse import quote, urlencode

import psycopg

from slack_fuse_server._json import JsonObject
from slack_fuse_server.snapshot.generator import canonical_json


class SnapshotNotFoundError(LookupError):
    """No snapshot row matched the requested stream/offset."""


@dataclass(frozen=True, slots=True)
class SnapshotPayload:
    """A ready-to-send gzip body from one persisted snapshot row."""

    stream: str
    at_offset: int
    body: bytes


def fetch_snapshot_payload(
    database_url: str,
    *,
    stream: str,
    requested_at: int,
    client_since_offset: int | None = None,
) -> SnapshotPayload:
    """Load the latest snapshot `<= requested_at`, record `snapshot_uses`, gzip JSONL."""
    _require_non_negative("requested_at", requested_at)
    if client_since_offset is not None:
        _require_non_negative("client_since_offset", client_since_offset)

    effective_since = requested_at if client_since_offset is None else client_since_offset
    with psycopg.connect(database_url) as conn, conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "SELECT at_offset, payload FROM snapshots "
            "WHERE stream = %s AND at_offset <= %s "
            "ORDER BY at_offset DESC LIMIT 1",
            (stream, requested_at),
        )
        row = cur.fetchone()
        if row is None:
            raise SnapshotNotFoundError(stream)

        at_offset = int(row[0])
        payload_lines = _payload_lines(row[1])
        wire_jsonl = _wire_jsonl(stream, payload_lines)
        gzip_body = gzip.compress(wire_jsonl)

        cur.execute(
            "INSERT INTO snapshot_uses "
            "(snapshot_stream, snapshot_at_offset, client_since_offset, events_skipped) "
            "VALUES (%s, %s, %s, %s)",
            (stream, at_offset, effective_since, at_offset - effective_since),
        )

    return SnapshotPayload(stream=stream, at_offset=at_offset, body=gzip_body)


def find_snapshot_at_or_after(database_url: str, stream: str, since: int, head_offset: int) -> int | None:
    """Latest snapshot offset in `[since, head_offset]`, or None."""
    _require_non_negative("since", since)
    _require_non_negative("head_offset", head_offset)
    if since > head_offset:
        return None

    with psycopg.connect(database_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT at_offset FROM snapshots "
            "WHERE stream = %s AND at_offset >= %s AND at_offset <= %s "
            "ORDER BY at_offset DESC LIMIT 1",
            (stream, since, head_offset),
        )
        row = cur.fetchone()
    return None if row is None else int(row[0])


def build_snapshot_url(stream: str, *, at_offset: int, client_since_offset: int | None = None) -> str:
    """URL for `snapshot_at` frames; includes optional `since` for use accounting."""
    _require_non_negative("at_offset", at_offset)
    if client_since_offset is not None:
        _require_non_negative("client_since_offset", client_since_offset)

    query_items: list[tuple[str, str]] = [("at", str(at_offset))]
    if client_since_offset is not None:
        query_items.append(("since", str(client_since_offset)))
    encoded_stream = quote(stream, safe="")
    return f"/streams/{encoded_stream}/snapshot?{urlencode(query_items)}"


def _payload_lines(raw_payload: object) -> tuple[JsonObject, ...]:
    if not isinstance(raw_payload, list):
        msg = "snapshot payload must be a JSON array"
        raise ValueError(msg)
    lines: list[JsonObject] = []
    for item in cast("list[object]", raw_payload):
        if not isinstance(item, dict):
            msg = "snapshot payload rows must be JSON objects"
            raise ValueError(msg)
        lines.append(cast("JsonObject", item))
    return tuple(lines)


def _wire_jsonl(stream: str, lines: tuple[JsonObject, ...]) -> bytes:
    rendered = [canonical_json(_line_for_wire(stream, line)).encode("utf-8") for line in lines]
    return b"\n".join(rendered)


def _line_for_wire(stream: str, line: JsonObject) -> JsonObject:
    # Sprint 2D persisted channel snapshots as {"ts", "payload"} rows.
    # Sprint 2E projector expects message-shaped rows; unwrap only channel lines.
    if stream.startswith("channel:"):
        ts = line.get("ts")
        payload = line.get("payload")
        if isinstance(ts, str) and isinstance(payload, dict):
            return cast("JsonObject", payload)
    return line


def _require_non_negative(name: str, value: int) -> None:
    if value < 0:
        raise ValueError(f"{name} must be >= 0")
