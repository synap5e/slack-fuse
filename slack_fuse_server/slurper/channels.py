"""Channel-list startup populate for the slurper.

Mirrors `slack_fuse_server.slurper.users.populate_users_once`: the slurper emits
`channel-list` `channel_added` events from two sources.

- Startup: one `channel_added` event per conversation the user can see, from a
  one-shot `conversations.list` pass (this module). Idempotent on restart.
- Live: `channel_created` / `im_created` socket events translate to the same
  `channel_added` wire kind via `slack_fuse_server.slurper.socket`.

Why the startup pass matters: a split-mode client subscribes to per-channel
streams only for channels present in its local `channels` table, which it
populates from `channel_added` events. Without this pass a fresh client sees
empty channel listings until Slack happens to push a live channel-structure
event — which never fires for channels the user is already a member of. The
users-stream equivalent already shipped (Sprint 1E); this closes the same gap
for channels.

The emitted payload is `Channel.model_dump(mode="json")`, byte-identical to the
shape the live socket-mode path writes (`socket._channel_added_write`), so the
client projector's `apply_event` processes startup and live events identically.
"""

from __future__ import annotations

import logging

import httpx
import trio
from psycopg import Connection, Cursor
from psycopg.rows import TupleRow

from slack_fuse.models import Channel
from slack_fuse_server._json import JsonObject
from slack_fuse_server.slurper.api import SlackAPIError, SlackClient, Validated
from slack_fuse_server.slurper.limiters import SlurperLimiters
from slack_fuse_server.slurper.offsets import EventRecord, OffsetWriter, assign_offset, insert_event
from slack_fuse_server.slurper.supervisor import TaskSupervisor

log = logging.getLogger(__name__)

_CHANNEL_LIST_STREAM = "channel-list"


def _lock_channel_list_stream(cur: Cursor[TupleRow]) -> None:
    cur.execute(
        "INSERT INTO stream_heads (stream) VALUES (%s) ON CONFLICT (stream) DO NOTHING",
        (_CHANNEL_LIST_STREAM,),
    )
    cur.execute(
        "SELECT next_offset FROM stream_heads WHERE stream = %s FOR UPDATE",
        (_CHANNEL_LIST_STREAM,),
    )
    if cur.fetchone() is None:  # pragma: no cover - row is guaranteed by upsert above
        msg = f"stream_heads row vanished for {_CHANNEL_LIST_STREAM!r}"
        raise RuntimeError(msg)


def _existing_channel_added_ids(cur: Cursor[TupleRow]) -> set[str]:
    cur.execute(
        "SELECT payload->>'id' FROM events WHERE stream = %s AND kind = 'channel_added'",
        (_CHANNEL_LIST_STREAM,),
    )
    existing: set[str] = set()
    for row in cur.fetchall():
        raw_id = row[0]
        if isinstance(raw_id, str) and raw_id:
            existing.add(raw_id)
    return existing


def _channel_added_exists(cur: Cursor[TupleRow], channel_id: str) -> bool:
    cur.execute(
        "SELECT 1 FROM events WHERE stream = %s AND kind = 'channel_added' AND payload->>'id' = %s LIMIT 1",
        (_CHANNEL_LIST_STREAM, channel_id),
    )
    return cur.fetchone() is not None


def _insert_channel_added(cur: Cursor[TupleRow], channel_raw: JsonObject) -> int:
    """Persist the RAW channel dict as the payload, not ``model_dump`` output.

    Pydantic ``model_dump`` reshapes nested fields (our ``topic`` is the
    flat string lifted from ``topic: {value, creator, last_set}``) and
    silently drops fields the model doesn't declare. The events table is
    the lossless source of truth, so we store what Slack actually sent.
    """
    offset = assign_offset(cur, _CHANNEL_LIST_STREAM)
    record = EventRecord(stream=_CHANNEL_LIST_STREAM, kind="channel_added", ts=None, payload=channel_raw)
    insert_event(cur, offset, record)
    return offset


def _populate_channels_once_sync(
    conn: Connection[TupleRow],
    channels: list[Validated[Channel]],
) -> tuple[int, int]:
    inserted = 0
    with conn.cursor() as cur:
        _lock_channel_list_stream(cur)
        existing = _existing_channel_added_ids(cur)
        for validated in channels:
            channel = validated.model
            if channel.id in existing:
                continue
            _insert_channel_added(cur, validated.raw)
            existing.add(channel.id)
            inserted += 1
    return (len(channels), inserted)


async def populate_channels_once(
    writer: OffsetWriter,
    client: SlackClient,
    limiters: SlurperLimiters,
    supervisor: TaskSupervisor | None = None,
) -> None:
    """One-shot startup conversations.list import (`channel_added` events)."""
    try:
        if supervisor is not None:
            supervisor.declare("populate-channels", "listing_channels", deadline_s=60)
        channels = await trio.to_thread.run_sync(client.list_conversations, limiter=limiters.slack_api)
        if supervisor is not None:
            supervisor.declare("populate-channels", "writing_channels", deadline_s=30)
        total, inserted = await writer.run_transaction(lambda conn: _populate_channels_once_sync(conn, channels))
    except (httpx.HTTPError, SlackAPIError, ValueError):
        log.warning("channels: startup populate failed", exc_info=True)
        if supervisor is not None:
            supervisor.declare("populate-channels", "failed", deadline_s=None)
        return
    log.info(
        "channels: startup populate complete channels=%d inserted=%d skipped=%d",
        total,
        inserted,
        total - inserted,
    )
    if supervisor is not None:
        supervisor.declare("populate-channels", "complete", deadline_s=None)


def _channel_added_exists_sync(conn: Connection[TupleRow], channel_id: str) -> bool:
    with conn.cursor() as cur:
        return _channel_added_exists(cur, channel_id)


def _insert_channel_added_if_missing_sync(
    conn: Connection[TupleRow],
    channel_id: str,
    channel_raw: JsonObject,
) -> bool:
    """Transaction-only body of :func:`ensure_channel_added`.

    Returns True if a new event was inserted, False if one already existed
    (idempotent re-run). The caller fetches ``channel_raw`` before entering this
    transaction; this helper must stay DB-only.
    """
    with conn.cursor() as cur:
        _lock_channel_list_stream(cur)
        if _channel_added_exists(cur, channel_id):
            return False
        _ = _insert_channel_added(cur, channel_raw)
        return True


async def ensure_channel_added(
    writer: OffsetWriter,
    client: SlackClient,
    channel_id: str,
    limiters: SlurperLimiters,
) -> bool:
    """Guarantee that ``channel-list`` has a ``channel_added`` event for
    ``channel_id`` before any per-channel events are written.

    Used by the admin backfill path so that legacy-cache imports (or any other
    source that names a channel directly) bring the channel under the
    projector's normal subscription/tier model. Without this, backfilled events
    land on a ``channel:<id>`` stream the projector has no row for in its
    ``channels`` table, so it never subscribes and the events are orphaned on
    the server forever.

    Returns True if a new event was inserted, False if one already existed.
    """
    already_exists = await writer.run_read(
        lambda conn: _channel_added_exists_sync(conn, channel_id),
        limiter=limiters.admin_read,
    )
    if already_exists:
        return False

    # Fetch outside the channel-list transaction. If Slack rejects the channel,
    # no event is written, matching the old rollback-on-HTTP-failure surface
    # without holding the stream row lock during the network call.
    validated = await trio.to_thread.run_sync(
        lambda: client.get_channel_info(channel_id),
        limiter=limiters.slack_api,
    )
    return await writer.run_transaction(
        lambda conn: _insert_channel_added_if_missing_sync(conn, channel_id, validated.raw)
    )
