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
from typing import cast

import httpx
import trio
from psycopg import Cursor
from psycopg.rows import TupleRow

from slack_fuse.models import Channel
from slack_fuse_server._json import JsonObject
from slack_fuse_server.slurper.api import SlackAPIError, SlackClient
from slack_fuse_server.slurper.offsets import EventRecord, OffsetWriter, assign_offset, insert_event

log = logging.getLogger(__name__)

_CHANNEL_LIST_STREAM = "channel-list"


def _channel_payload(channel: Channel) -> JsonObject:
    return cast(JsonObject, channel.model_dump(mode="json"))


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


def _insert_channel_added(cur: Cursor[TupleRow], channel: Channel) -> int:
    offset = assign_offset(cur, _CHANNEL_LIST_STREAM)
    record = EventRecord(stream=_CHANNEL_LIST_STREAM, kind="channel_added", ts=None, payload=_channel_payload(channel))
    insert_event(cur, offset, record)
    return offset


def _populate_channels_once_sync(writer: OffsetWriter, client: SlackClient) -> tuple[int, int]:
    channels = client.list_conversations()
    inserted = 0
    with writer.conn.transaction(), writer.conn.cursor() as cur:
        _lock_channel_list_stream(cur)
        existing = _existing_channel_added_ids(cur)
        for channel in channels:
            if channel.id in existing:
                continue
            _insert_channel_added(cur, channel)
            existing.add(channel.id)
            inserted += 1
    return (len(channels), inserted)


async def populate_channels_once(writer: OffsetWriter, client: SlackClient) -> None:
    """One-shot startup conversations.list import (`channel_added` events)."""
    try:
        total, inserted = await trio.to_thread.run_sync(
            lambda: _populate_channels_once_sync(writer, client),
            limiter=writer.limiter,
        )
    except (httpx.HTTPError, SlackAPIError, ValueError):
        log.warning("channels: startup populate failed", exc_info=True)
        return
    log.info(
        "channels: startup populate complete channels=%d inserted=%d skipped=%d",
        total,
        inserted,
        total - inserted,
    )
