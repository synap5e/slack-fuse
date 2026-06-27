"""Chunk-rerender tests (handoff: per-channel rerender path).

Drives `rerender_channel` against an httpx mock transport returning a snapshot
JSONL body. Verifies the core contract:

* stale chunks are re-rendered with the CURRENT renderer (the attachment-render
  regression this feature enables);
* chunks for OTHER channels are untouched;
* chunks present locally but absent from the snapshot survive (upsert-only — no
  delete-absent), which is what keeps the live `(snapshot, head]` tail safe;
* the stream cursor is never moved (so a concurrent live applier's offset can't
  be corrupted);
* a 404 (no snapshot yet) maps to `no_snapshot`, a transport error to
  `server_unavailable`;
* invalidations fire for every re-rendered chunk.
"""

from __future__ import annotations

import json
from decimal import Decimal

import httpx
import psycopg
from psycopg.rows import TupleRow

from slack_fuse.projector.cursor import advance_cursor
from slack_fuse.projector.rerender import rerender_channel
from tests.projector.conftest import ClientConnFactory, RecordingSink


def _seed_chunk(conn: psycopg.Connection[TupleRow], channel_id: str, ts: str, content: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO chunks (channel_id, message_ts, content_md, reply_count) VALUES (%s, %s, %s, 0)",
            (channel_id, Decimal(ts), content),
        )


def _content(conn: psycopg.Connection[TupleRow], channel_id: str, ts: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT content_md FROM chunks WHERE channel_id = %s AND message_ts = %s",
            (channel_id, Decimal(ts)),
        )
        row = cur.fetchone()
    return None if row is None else str(row[0])


def _cursor(conn: psycopg.Connection[TupleRow], stream: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT applied_offset FROM cursors WHERE stream = %s", (stream,))
        row = cur.fetchone()
    return 0 if row is None else int(row[0])


def _mock_http(body: bytes, *, status: int = 200) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.startswith("/streams/")
        assert "snapshot" in request.url.path
        return httpx.Response(status, content=body)

    return httpx.Client(base_url="http://snap.test", transport=httpx.MockTransport(handler))


def _attachment_message(ts: str) -> bytes:
    """A bot message whose body lives entirely in `attachments` (the renderer
    regression: old code dropped these, current code renders them)."""
    payload = {
        "type": "message",
        "ts": ts,
        "user": "U0001",
        "text": "",
        "thread_ts": None,
        "attachments": [{"title": "FE-740 unfurl", "text": "the unfurled body", "fallback": "fb"}],
    }
    return json.dumps(payload).encode()


def test_rerender_refreshes_stale_chunk_with_current_renderer(client_conn_factory: ClientConnFactory) -> None:
    """A chunk rendered by the old code (empty body) is overwritten by the
    current renderer's attachment output."""
    channel = "CRER1"
    ts = "1700000000.000100"

    seed = client_conn_factory()
    # Simulate an old render that dropped the attachment — just the header.
    _seed_chunk(seed, channel, ts, "## 11:13 <@U0001>\n")
    assert "unfurled body" not in (_content(seed, channel, ts) or "")

    sink = RecordingSink()
    conn = client_conn_factory()
    with _mock_http(_attachment_message(ts)) as http:
        result = rerender_channel(http, "http://snap.test", conn, channel, sink=sink)

    assert result.status == "rerendered"
    assert result.chunks == 1
    verify = client_conn_factory()
    body = _content(verify, channel, ts) or ""
    assert "FE-740 unfurl" in body
    assert "the unfurled body" in body
    # The invalidation fired for the re-rendered chunk.
    assert any(ref.channel_id == channel and ref.message_ts == Decimal(ts) for ref in sink.chunks)


def test_rerender_leaves_other_channels_untouched(client_conn_factory: ClientConnFactory) -> None:
    channel = "CRER2"
    other = "COTHER"
    ts = "1700000000.000200"
    other_ts = "1700000000.000300"

    seed = client_conn_factory()
    _seed_chunk(seed, channel, ts, "stale")
    _seed_chunk(seed, other, other_ts, "untouched other-channel chunk")

    conn = client_conn_factory()
    with _mock_http(_attachment_message(ts)) as http:
        rerender_channel(http, "http://snap.test", conn, channel)

    verify = client_conn_factory()
    assert _content(verify, other, other_ts) == "untouched other-channel chunk"


def test_rerender_is_upsert_only_keeps_tail_and_does_not_move_cursor(
    client_conn_factory: ClientConnFactory,
) -> None:
    """Upsert-only: a chunk absent from the snapshot (the live tail) survives,
    and the stream cursor is not advanced (so a concurrent live applier's
    offset is never corrupted)."""
    channel = "CRER3"
    stream = f"channel:{channel}"
    snap_ts = "1700000000.000400"
    tail_ts = "1700000099.000000"  # newer than the snapshot — the live tail

    seed = client_conn_factory()
    _seed_chunk(seed, channel, snap_ts, "stale snapshot-era chunk")
    _seed_chunk(seed, channel, tail_ts, "live tail chunk applied after the snapshot")
    # Pretend the live applier has advanced well past the snapshot offset.
    with seed.cursor() as cur:
        advance_cursor(cur, stream, 9999)

    conn = client_conn_factory()
    with _mock_http(_attachment_message(snap_ts)) as http:
        result = rerender_channel(http, "http://snap.test", conn, channel)
    assert result.status == "rerendered"

    verify = client_conn_factory()
    # The snapshot chunk was re-rendered...
    assert "unfurled body" in (_content(verify, channel, snap_ts) or "")
    # ...but the live tail chunk (absent from the snapshot) was NOT deleted.
    assert _content(verify, channel, tail_ts) == "live tail chunk applied after the snapshot"
    # ...and the cursor is exactly where the live applier left it.
    assert _cursor(verify, stream) == 9999


def test_rerender_requests_snapshot_at_applied_offset(client_conn_factory: ClientConnFactory) -> None:
    """The snapshot is requested at the channel's current applied offset so the
    server returns the most recent snapshot at/below the live position."""
    channel = "CRER4"
    stream = f"channel:{channel}"
    seed = client_conn_factory()
    with seed.cursor() as cur:
        advance_cursor(cur, stream, 4242)

    seen_at: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_at.append(request.url.params.get("at", ""))
        return httpx.Response(200, content=b"")

    conn = client_conn_factory()
    with httpx.Client(base_url="http://snap.test", transport=httpx.MockTransport(handler)) as http:
        rerender_channel(http, "http://snap.test", conn, channel)

    assert seen_at == ["4242"]


def test_rerender_no_snapshot_maps_to_status(client_conn_factory: ClientConnFactory) -> None:
    conn = client_conn_factory()
    with _mock_http(b"", status=404) as http:
        result = rerender_channel(http, "http://snap.test", conn, "CRER5")
    assert result.status == "no_snapshot"
    assert result.chunks == 0


def test_rerender_transport_error_maps_to_server_unavailable(client_conn_factory: ClientConnFactory) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused")

    conn = client_conn_factory()
    with httpx.Client(base_url="http://snap.test", transport=httpx.MockTransport(handler)) as http:
        result = rerender_channel(http, "http://snap.test", conn, "CRER6")
    assert result.status == "server_unavailable"


def test_rerender_malformed_body_rolls_back(client_conn_factory: ClientConnFactory) -> None:
    channel = "CRER7"
    ts = "1700000000.000700"
    seed = client_conn_factory()
    _seed_chunk(seed, channel, ts, "stale")

    bad = _attachment_message(ts) + b"\nNOT_JSON\n"
    conn = client_conn_factory()
    with _mock_http(bad) as http:
        result = rerender_channel(http, "http://snap.test", conn, channel)

    assert result.status == "malformed"
    # The whole apply rolled back — the stale chunk is unchanged (no partial).
    verify = client_conn_factory()
    assert _content(verify, channel, ts) == "stale"
