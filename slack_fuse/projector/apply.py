"""Event → chunk operations. The projector's transactional core.

Per RFC §Projection logic. Each `apply_event` call runs one TX that:

- mutates `chunks` / `thread_chunks` / `chunk_mentions` /
  `thread_chunk_mentions` / `channels` / `users` per the projection table
- advances `cursors.applied_offset` for the event's stream

Atomicity is the load-bearing property — on crash mid-apply the next subscribe
re-sends from `applied_offset` and the partial batch replays harmlessly
because every chunk write is `ON CONFLICT DO UPDATE` and every cursor write
uses `GREATEST`. Concurrent appliers (one per stream) own their own
connections; postgres handles disjoint-PK writes cheaply.

User/channel events are the cross-stream race the RFC discusses (a `message`
event referencing `<@U…>` can arrive before `user_added`). The renderer stores
unresolved `<@U…>` placeholders in `content_md` and records `chunk_mentions`
rows at write time so that a *later* `user_added` lookup
(`WHERE mention_kind='user' AND mentioned_id=$uid`) finds the already-written
chunk, the projector fires invalidation, and the kernel page cache drops the
UID-literal fallback. No chunk re-render is needed; the next read substitutes
the display name from the now-populated `users` table.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol, cast

from psycopg import Connection, Cursor
from psycopg.rows import TupleRow
from pydantic import ValidationError

from slack_fuse.models import JsonObject, Message, SlackUser, SlackUserProfile
from slack_fuse.projector.cursor import advance_cursor
from slack_fuse_render import (
    extract_mention_channel_ids,
    extract_mention_user_ids,
    render_message_structural,
)
from slack_fuse_server.wire.frames import EventFrame

log = logging.getLogger(__name__)


# === Public types ===


@dataclass(frozen=True, slots=True)
class ChunkRef:
    """Identifies one top-level chunk to invalidate after the TX commits."""

    channel_id: str
    message_ts: Decimal


@dataclass(frozen=True, slots=True)
class ThreadChunkRef:
    """Identifies one thread-chunk to invalidate after the TX commits."""

    channel_id: str
    thread_ts: Decimal
    reply_ts: Decimal


@dataclass(frozen=True, slots=True)
class ApplyResult:
    """What the applier should do once its TX commits.

    Post-commit invalidations let `InvalidationSink` translate the chunk
    identifiers into FUSE inode invalidations (the FUSE side composes paths
    from `channel_id` + the date-folder it derives at read time). Empty lists
    are the common case (most events just mutate chunks).
    """

    chunks: tuple[ChunkRef, ...] = ()
    thread_chunks: tuple[ThreadChunkRef, ...] = ()
    channel_list_changed: bool = False


class InvalidationSink(Protocol):
    """How the projector tells its host (FUSE / tests) which inodes to drop.

    Translates a chunk-level identifier to an inode invalidation against the
    kernel page cache. Mirrors the legacy `slack_fuse.invalidation.InvalidationSink`
    interface in spirit — same separation of "the projector knows what changed"
    from "the FUSE layer knows the inode."
    """

    def chunk_changed(self, ref: ChunkRef) -> None: ...
    def thread_chunk_changed(self, ref: ThreadChunkRef) -> None: ...
    def channel_list_changed(self) -> None: ...


class NullInvalidationSink:
    """No-op `InvalidationSink`. Default for tests and pre-FUSE wiring."""

    def chunk_changed(self, ref: ChunkRef) -> None:
        return None

    def thread_chunk_changed(self, ref: ThreadChunkRef) -> None:
        return None

    def channel_list_changed(self) -> None:
        return None


# === Connection contract guard (mirrors OffsetWriter) ===


def require_autocommit(conn: Connection[TupleRow]) -> None:
    """Fail-fast on a non-autocommit projector connection.

    Mirrors `slack_fuse_server.slurper.offsets.OffsetWriter`'s guard, with the
    same failure-mode language. Without autocommit, each `with conn.transaction()`
    becomes a savepoint inside an implicit outer transaction and rolls back when
    the connection closes — chunks appear to write, then vanish.
    """
    if not conn.autocommit:
        msg = (
            "Projector connection requires conn.autocommit=True. "
            "Without it, apply_event()'s `with conn.transaction()` becomes "
            "a savepoint inside an implicit outer transaction and rolls "
            "back when the connection closes. Set conn.autocommit=True "
            "BEFORE handing the connection to the applier."
        )
        raise ValueError(msg)


# === Apply entry points ===


def apply_event(
    conn: Connection[TupleRow],
    frame: EventFrame,
) -> ApplyResult:
    """Apply one event in a single transaction. Returns post-commit work.

    Note: the ``always_blocked_channel_ids`` config field is deprecated and
    inert — see FINDING-17 (2026-07-17 review). Block enforcement lives in
    ``block_sync.apply_blocked_channel_sync`` which is the sole caller of
    ``_force_blocked_manual`` now.
    """
    with conn.transaction(), conn.cursor() as cur:
        result = _dispatch(cur, frame)
        advance_cursor(cur, frame.stream, frame.offset)
    return result


def apply_snapshot_row(cur: Cursor[TupleRow], stream: str, payload: JsonObject) -> ApplyResult:
    """Apply one snapshot row inside the caller's transaction.

    Snapshot rows reuse the on-the-wire `message` shape — RFC §Snapshot
    delivery via HTTP: "the snapshot-apply path is 'apply each line as if it
    were a `message` event,' sharing the projection code with the live-event
    path." Used by `snapshot_fetch.py` so the snapshot module doesn't reach
    into `_dispatch` directly.
    """
    if not stream.startswith("channel:"):
        msg = f"snapshot for non-channel stream {stream!r} not supported"
        raise ValueError(msg)
    channel_id = stream.removeprefix("channel:")
    return _dispatch_channel_event(cur, channel_id, "message", payload)


def record_caught_up(conn: Connection[TupleRow], stream: str, at_offset: int) -> None:
    """Stamp `stream_caught_up` for `stream` at `at_offset`.

    The RFC: "The FUSE read layer uses this to drive the *initial catch-up
    incomplete* trailer condition per stream." Idempotent on replay.
    """
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "INSERT INTO stream_caught_up (stream, caught_up_at, at_offset) "
            "VALUES (%s, now(), %s) "
            "ON CONFLICT (stream) DO UPDATE SET "
            "  caught_up_at = EXCLUDED.caught_up_at, "
            "  at_offset = GREATEST(stream_caught_up.at_offset, EXCLUDED.at_offset)",
            (stream, at_offset),
        )


# === Event dispatch ===


def _dispatch(cur: Cursor[TupleRow], frame: EventFrame) -> ApplyResult:
    """Route an event to its kind-specific handler. Stream-prefix scopes:

    - `channel:<id>` → message/reaction/edit/delete on that channel
    - `channel-list` → workspace channel inventory
    - `users` → workspace user directory
    - `slurper-health` → drives `connection_state.last_slurper_health`
    """
    stream = frame.stream
    kind = frame.kind
    payload = frame.payload
    if stream.startswith("channel:"):
        channel_id = stream.removeprefix("channel:")
        return _dispatch_channel_event(cur, channel_id, kind, payload)
    if stream == "channel-list":
        return _dispatch_channel_list_event(cur, kind, payload)
    if stream == "users":
        return _dispatch_users_event(cur, kind, payload)
    if stream == "slurper-health":
        return _dispatch_health_event(cur, kind, payload)
    log.debug("apply: ignoring unknown stream %s (kind=%s)", stream, kind)
    return ApplyResult()


# === `channel:<id>` stream ===


def _dispatch_channel_event(cur: Cursor[TupleRow], channel_id: str, kind: str, payload: JsonObject) -> ApplyResult:
    if kind == "message":
        return _apply_message(cur, channel_id, payload)
    if kind == "message_changed":
        return _apply_message_changed(cur, channel_id, payload)
    if kind == "message_deleted":
        return _apply_message_deleted(cur, channel_id, payload)
    if kind in ("reaction_added", "reaction_removed"):
        # Slack reaction events carry only `{target_ts, user, emoji}` — no full
        # Message — so a faithful re-render needs server-side reaction state
        # the v1 slurper doesn't emit. Logged + no-op until the slurper grows
        # an aggregator (RFC §Event kinds names these but they're unused in v1).
        log.debug("apply: %s on %s — v1 no-op (no server-side reaction state)", kind, channel_id)
        return ApplyResult()
    log.warning("apply: unknown channel event kind %r on channel:%s", kind, channel_id)
    return ApplyResult()


def _parse_message(payload: JsonObject) -> Message | None:
    try:
        return Message.model_validate(payload)
    except ValidationError:
        log.warning("apply: rejecting malformed Message payload")
        return None


def _apply_message(cur: Cursor[TupleRow], channel_id: str, payload: JsonObject) -> ApplyResult:
    msg = _parse_message(payload)
    if msg is None:
        return ApplyResult()
    if _is_reply(msg):
        return _write_thread_chunk(cur, channel_id, msg)
    return _write_top_level_chunk(cur, channel_id, msg)


def _apply_message_changed(cur: Cursor[TupleRow], channel_id: str, payload: JsonObject) -> ApplyResult:
    """`message_changed` payload: `{message: <Message>, previous_ts: <ts>}`."""
    inner = payload.get("message")
    if not isinstance(inner, dict):
        log.warning("apply: message_changed without message payload on channel:%s", channel_id)
        return ApplyResult()
    msg = _parse_message(cast(JsonObject, inner))
    if msg is None:
        return ApplyResult()
    if _is_reply(msg):
        # Thread reply edit: UPDATE thread_chunks (re-render + refresh mentions).
        return _write_thread_chunk(cur, channel_id, msg)
    return _write_top_level_chunk(cur, channel_id, msg)


def _apply_message_deleted(cur: Cursor[TupleRow], channel_id: str, payload: JsonObject) -> ApplyResult:
    """`message_deleted` payload: `{deleted_ts: <ts>, previous_message: <Message|None>}`.

    First try the top-level path (most messages are top-level), then the
    thread-reply path. If `previous_message` is supplied with `thread_ts !=
    ts`, prefer the thread path directly (saves a SELECT).
    """
    deleted_ts_raw = payload.get("deleted_ts")
    if not isinstance(deleted_ts_raw, str):
        log.warning("apply: message_deleted without deleted_ts on channel:%s", channel_id)
        return ApplyResult()
    deleted_ts = _ts_to_decimal(deleted_ts_raw)
    if deleted_ts is None:
        return ApplyResult()

    previous = payload.get("previous_message")
    if isinstance(previous, dict):
        prev_msg = _parse_message(cast(JsonObject, previous))
        if prev_msg is not None and _is_reply(prev_msg):
            return _delete_thread_chunk(cur, channel_id, prev_msg)

    # Try top-level first (CASCADE removes the chunk_mentions rows via FK).
    cur.execute(
        "DELETE FROM chunks WHERE channel_id = %s AND message_ts = %s RETURNING message_ts",
        (channel_id, deleted_ts),
    )
    if cur.fetchone() is not None:
        return ApplyResult(chunks=(ChunkRef(channel_id, deleted_ts),))

    # Fall back: scan thread_chunks for this reply_ts; we don't know thread_ts
    # without it being supplied, so locate by reply_ts.
    cur.execute(
        "SELECT thread_ts FROM thread_chunks WHERE channel_id = %s AND reply_ts = %s AND role = 'reply'",
        (channel_id, deleted_ts),
    )
    row = cur.fetchone()
    if row is None:
        log.debug("apply: message_deleted ts=%s not found on channel:%s", deleted_ts_raw, channel_id)
        return ApplyResult()
    thread_ts = cast(Decimal, row[0])
    cur.execute(
        "DELETE FROM thread_chunks WHERE channel_id = %s AND thread_ts = %s AND reply_ts = %s",
        (channel_id, thread_ts, deleted_ts),
    )
    _refresh_parent_reply_count(cur, channel_id, thread_ts, allow_downgrade=True)
    return ApplyResult(
        chunks=(ChunkRef(channel_id, thread_ts),),
        thread_chunks=(ThreadChunkRef(channel_id, thread_ts, deleted_ts),),
    )


def _is_reply(msg: Message) -> bool:
    """A message is a thread reply when `thread_ts` is set and points elsewhere."""
    return msg.thread_ts is not None and msg.thread_ts != msg.ts


def _write_top_level_chunk(cur: Cursor[TupleRow], channel_id: str, msg: Message) -> ApplyResult:
    content_md = render_message_structural(msg)
    message_ts = _ts_to_decimal(msg.ts)
    if message_ts is None:
        return ApplyResult()
    cur.execute(
        "INSERT INTO chunks (channel_id, message_ts, content_md, reply_count) "
        "VALUES (%s, %s, %s, %s) "
        "ON CONFLICT (channel_id, message_ts) DO UPDATE SET "
        "  content_md = EXCLUDED.content_md, "
        "  reply_count = GREATEST(chunks.reply_count, EXCLUDED.reply_count)",
        (channel_id, message_ts, content_md, msg.reply_count),
    )
    cur.execute(
        "DELETE FROM chunk_mentions WHERE channel_id = %s AND message_ts = %s",
        (channel_id, message_ts),
    )
    _insert_chunk_mentions(cur, channel_id, message_ts, content_md)
    # If a reply already landed (re-orderable cross-stream), re-derive the
    # parent's indicator so a replayed top-level message doesn't roll back
    # the reply count baked into `content_md`.
    _refresh_parent_reply_count(cur, channel_id, message_ts)
    return ApplyResult(chunks=(ChunkRef(channel_id, message_ts),))


def _write_thread_chunk(cur: Cursor[TupleRow], channel_id: str, msg: Message) -> ApplyResult:
    content_md = render_message_structural(msg)
    if msg.thread_ts is None:  # pragma: no cover - _is_reply guards this
        return ApplyResult()
    thread_ts = _ts_to_decimal(msg.thread_ts)
    reply_ts = _ts_to_decimal(msg.ts)
    if thread_ts is None or reply_ts is None:
        return ApplyResult()
    cur.execute(
        "INSERT INTO thread_chunks (channel_id, thread_ts, reply_ts, role, content_md) "
        "VALUES (%s, %s, %s, 'reply', %s) "
        "ON CONFLICT (channel_id, thread_ts, reply_ts) DO UPDATE SET "
        "  content_md = EXCLUDED.content_md",
        (channel_id, thread_ts, reply_ts, content_md),
    )
    cur.execute(
        "DELETE FROM thread_chunk_mentions WHERE channel_id = %s AND thread_ts = %s AND reply_ts = %s",
        (channel_id, thread_ts, reply_ts),
    )
    _insert_thread_chunk_mentions(cur, channel_id, thread_ts, reply_ts, content_md)
    parent_changed = _refresh_parent_reply_count(cur, channel_id, thread_ts)
    chunks_invalidations: tuple[ChunkRef, ...] = (ChunkRef(channel_id, thread_ts),) if parent_changed else ()
    return ApplyResult(
        thread_chunks=(ThreadChunkRef(channel_id, thread_ts, reply_ts),),
        chunks=chunks_invalidations,
    )


def _delete_thread_chunk(cur: Cursor[TupleRow], channel_id: str, prev_msg: Message) -> ApplyResult:
    if prev_msg.thread_ts is None:  # pragma: no cover - caller guards via _is_reply
        return ApplyResult()
    thread_ts = _ts_to_decimal(prev_msg.thread_ts)
    reply_ts = _ts_to_decimal(prev_msg.ts)
    if thread_ts is None or reply_ts is None:
        return ApplyResult()
    cur.execute(
        "DELETE FROM thread_chunks WHERE channel_id = %s AND thread_ts = %s AND reply_ts = %s",
        (channel_id, thread_ts, reply_ts),
    )
    _refresh_parent_reply_count(cur, channel_id, thread_ts, allow_downgrade=True)
    return ApplyResult(
        thread_chunks=(ThreadChunkRef(channel_id, thread_ts, reply_ts),),
        chunks=(ChunkRef(channel_id, thread_ts),),
    )


def _refresh_parent_reply_count(
    cur: Cursor[TupleRow],
    channel_id: str,
    thread_ts: Decimal,
    *,
    allow_downgrade: bool = False,
) -> bool:
    """Recompute the parent chunk's `reply_count` from `thread_chunks`.

    Idempotent on replay: derived from `COUNT(*)`, not `+= 1`. The parent's
    rendered `> Thread: N replies` indicator in `content_md` is NOT
    re-rendered here — we don't have the parent `Message` to feed back into
    `render_message_structural`. As a pragmatic v1 step, we *patch* the
    indicator line via regex when present so the rendered count stays in sync
    with the column; absent the indicator (parent rendered with reply_count=0
    initially) we synthesize one. Returns whether the parent chunk row was
    modified.

    ``allow_downgrade`` (FINDING-15, 2026-07-17): default False protects an
    already-stored, Slack-authoritative ``reply_count`` from being clobbered
    by a lower local count. ``chunks.reply_count`` may have been written
    from Slack's ``reply_count`` on the parent Message (authoritative for
    how many replies exist); the local ``thread_chunks`` count only
    reflects replies we have *materialized*. On a partially-backfilled
    channel the local count is legitimately lower — GREATEST protects
    that. Callers on the DELETE path (``message_deleted``, thread reply
    deletion via ``message_changed``) pass ``allow_downgrade=True`` so a
    legitimate decrement (the only reply just got deleted) can walk the
    count back to zero.
    """
    cur.execute(
        "SELECT COUNT(*) FROM thread_chunks WHERE channel_id = %s AND thread_ts = %s AND role = 'reply'",
        (channel_id, thread_ts),
    )
    row = cur.fetchone()
    local_replies = int(row[0]) if row is not None else 0
    cur.execute(
        "SELECT content_md, reply_count FROM chunks WHERE channel_id = %s AND message_ts = %s",
        (channel_id, thread_ts),
    )
    parent = cur.fetchone()
    if parent is None:
        return False
    current_md = cast(str, parent[0])
    current_count = int(parent[1])
    new_count = local_replies if allow_downgrade else max(local_replies, current_count)
    new_md = _patch_thread_indicator(current_md, new_count)
    if new_md == current_md and new_count == current_count:
        return False
    cur.execute(
        "UPDATE chunks SET reply_count = %s, content_md = %s WHERE channel_id = %s AND message_ts = %s",
        (new_count, new_md, channel_id, thread_ts),
    )
    return True


_THREAD_INDICATOR_RE = re.compile(r"> Thread: \d+ repl(?:y|ies)")


def _patch_thread_indicator(content_md: str, new_count: int) -> str:
    if new_count <= 0:
        # Strip any existing indicator + the surrounding blank line(s).
        return _THREAD_INDICATOR_RE.sub("", content_md).rstrip() + "\n"
    indicator = f"> Thread: {new_count} {'reply' if new_count == 1 else 'replies'}"
    if _THREAD_INDICATOR_RE.search(content_md):
        return _THREAD_INDICATOR_RE.sub(indicator, content_md)
    # Append a new indicator block.
    return content_md.rstrip() + "\n\n" + indicator + "\n"


def _insert_chunk_mentions(cur: Cursor[TupleRow], channel_id: str, message_ts: Decimal, structural_md: str) -> None:
    rows: list[tuple[str, Decimal, str, str]] = []
    rows.extend((channel_id, message_ts, "user", uid.value) for uid in extract_mention_user_ids(structural_md))
    rows.extend((channel_id, message_ts, "channel", cid.value) for cid in extract_mention_channel_ids(structural_md))
    if not rows:
        return
    cur.executemany(
        "INSERT INTO chunk_mentions (channel_id, message_ts, mention_kind, mentioned_id) "
        "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
        rows,
    )


def _insert_thread_chunk_mentions(
    cur: Cursor[TupleRow],
    channel_id: str,
    thread_ts: Decimal,
    reply_ts: Decimal,
    structural_md: str,
) -> None:
    rows: list[tuple[str, Decimal, Decimal, str, str]] = []
    rows.extend((channel_id, thread_ts, reply_ts, "user", uid.value) for uid in extract_mention_user_ids(structural_md))
    rows.extend(
        (channel_id, thread_ts, reply_ts, "channel", cid.value) for cid in extract_mention_channel_ids(structural_md)
    )
    if not rows:
        return
    cur.executemany(
        "INSERT INTO thread_chunk_mentions (channel_id, thread_ts, reply_ts, mention_kind, mentioned_id) "
        "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
        rows,
    )


def _ts_to_decimal(ts: str) -> Decimal | None:
    try:
        return Decimal(ts)
    except (ValueError, ArithmeticError):
        log.warning("apply: malformed ts %r", ts)
        return None


# === `channel-list` stream ===


def _dispatch_channel_list_event(
    cur: Cursor[TupleRow],
    kind: str,
    payload: JsonObject,
) -> ApplyResult:
    if kind == "channel_added":
        return _apply_channel_added(cur, payload)
    if kind == "channel_info_refreshed":
        # Same shape as channel_added — a fresh ``conversations.info``
        # response carrying full channel metadata. The
        # ``ON CONFLICT DO UPDATE`` in ``_apply_channel_added`` handles
        # the upsert; auto-tier re-evaluates against current is_member /
        # is_archived so a "we just joined" refresh promotes hidden →
        # hot, and a "we left" refresh demotes hot → hidden.
        return _apply_channel_added(cur, payload)
    if kind == "channel_renamed":
        return _apply_channel_renamed(cur, payload)
    if kind == "channel_archived":
        return _apply_channel_archived(cur, payload)
    if kind == "channel_unarchived":
        return _apply_channel_unarchived(cur, payload)
    if kind == "channel_member_changed":
        return _apply_channel_member_changed(cur, payload)
    log.warning("apply: unknown channel-list kind %r", kind)
    return ApplyResult()


def _force_blocked_manual(cur: Cursor[TupleRow], channel_id: str) -> None:  # pyright: ignore[reportUnusedFunction]
    """Pin a channel to ``tier='blocked', tier_source='manual', subscribed=FALSE``
    unconditionally — wins over auto re-evaluation AND any prior CLI override.
    Used by ``block_sync.apply_blocked_channel_sync`` to enforce operator server-side block policy on every
    channel-list event that could re-tier the row.
    """
    cur.execute(
        "UPDATE channels SET tier = 'blocked', tier_source = 'manual', subscribed = FALSE, updated_at = now() "
        "WHERE channel_id = %s",
        (channel_id,),
    )


def _default_tier(*, is_archived: bool, is_im: bool, is_mpim: bool, is_member: bool) -> str:
    """Default tier for a new channel. Per RFC §Default tier assignment.

    The full algorithm consults backfill/chunk state for DMs; the v1
    simplification here is `is_im → 'hot'` (treat as live) and `public not
    joined → 'hidden'`. Manual tier override (`tier_source='manual'`) is
    respected by every update path; this function is only called on initial
    `channel_added` (or via :func:`_reevaluate_auto_tier` for an auto-source
    row after `channel_unarchived` / `channel_member_changed`).

    Archived channels default to `hidden` rather than `blocked`: archival is a
    Slack-side state, not a user signal of "don't want to see this," so the
    channel stays reachable by known path (and the projector subscribes to its
    stream so backfilled historical messages can flow). Operators who want
    archived channels visible in `readdir` can promote with
    `slack-fuse tier <slug> hot`; operators who want them entirely gone can
    `slack-fuse tier <slug> blocked` (sticky via `tier_source='manual'`).
    """
    if is_archived:
        return "hidden"
    if is_im or is_mpim:
        return "hot"
    if is_member:
        return "hot"
    return "hidden"


def _apply_channel_added(cur: Cursor[TupleRow], payload: JsonObject) -> ApplyResult:
    channel_id = payload.get("id")
    if not isinstance(channel_id, str):
        log.warning("apply: channel_added missing id")
        return ApplyResult()
    name = _str_field(payload.get("name"))
    is_im = bool(payload.get("is_im"))
    is_mpim = bool(payload.get("is_mpim"))
    is_member = bool(payload.get("is_member"))
    is_archived = bool(payload.get("is_archived"))
    im_user_id_raw = payload.get("im_user_id")
    im_user_id = im_user_id_raw if isinstance(im_user_id_raw, str) else None
    topic = _str_field(payload.get("topic"))
    purpose = _str_field(payload.get("purpose"))
    tier = _default_tier(is_archived=is_archived, is_im=is_im, is_mpim=is_mpim, is_member=is_member)
    cur.execute(
        "INSERT INTO channels (channel_id, name, is_im, is_mpim, is_member, is_archived, im_user_id, topic, purpose, "
        "  tier, tier_source, subscribed) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'auto', %s) "
        "ON CONFLICT (channel_id) DO UPDATE SET "
        "  name = EXCLUDED.name, "
        "  is_im = EXCLUDED.is_im, "
        "  is_mpim = EXCLUDED.is_mpim, "
        "  is_member = EXCLUDED.is_member, "
        "  is_archived = EXCLUDED.is_archived, "
        "  im_user_id = EXCLUDED.im_user_id, "
        "  topic = EXCLUDED.topic, "
        "  purpose = EXCLUDED.purpose, "
        "  tier = CASE WHEN channels.tier_source = 'auto' THEN EXCLUDED.tier ELSE channels.tier END, "
        "  subscribed = CASE WHEN channels.tier_source = 'auto' THEN EXCLUDED.subscribed ELSE channels.subscribed END, "
        "  updated_at = now()",
        (channel_id, name, is_im, is_mpim, is_member, is_archived, im_user_id, topic, purpose, tier, tier != "blocked"),
    )
    # Cross-stream race (same shape as user_added): a `message` referencing
    # `<#C…>` can arrive before this `channel_added`, leaving chunks rendered
    # with the CID-literal fallback. The lookup runs in this SAME TX as the
    # upsert above so a concurrently-committing message TX can't slip between a
    # separate lookup and the insert under READ COMMITTED (see
    # tests/projector/test_cross_stream_race.py). channel_list_changed alone
    # would also drop these inodes, but the explicit refs keep the invalidation
    # precise instead of relying on the broad channel.md sweep.
    refs = _collect_channel_mention_refs(cur, channel_id)
    thread_refs = _collect_thread_channel_mention_refs(cur, channel_id)
    return ApplyResult(chunks=refs, thread_chunks=thread_refs, channel_list_changed=True)


def _apply_channel_renamed(cur: Cursor[TupleRow], payload: JsonObject) -> ApplyResult:
    channel_id = payload.get("channel_id")
    new_name = payload.get("new_name")
    if not isinstance(channel_id, str) or not isinstance(new_name, str):
        log.warning("apply: channel_renamed missing fields")
        return ApplyResult()
    cur.execute(
        "UPDATE channels SET name = %s, updated_at = now() WHERE channel_id = %s",
        (new_name, channel_id),
    )
    # Channel-mention references are by id, not name — no chunk re-render is
    # needed; mention resolution picks up the new name on next read. But the
    # chunk_mentions side table maps `mention_kind='channel'` → mentioned_id;
    # we invalidate those inodes so the kernel re-reads with the new name.
    refs = _collect_channel_mention_refs(cur, channel_id)
    thread_refs = _collect_thread_channel_mention_refs(cur, channel_id)
    return ApplyResult(chunks=refs, thread_chunks=thread_refs, channel_list_changed=True)


def _apply_channel_archived(cur: Cursor[TupleRow], payload: JsonObject) -> ApplyResult:
    channel_id = payload.get("channel_id")
    if not isinstance(channel_id, str):
        return ApplyResult()
    # Auto-source rows: archived → `hidden`, NOT `blocked` (matches
    # ``_default_tier``'s archived branch). Hidden keeps the channel reachable
    # by known path and the stream subscribed so any in-flight events drain
    # cleanly. Manual-source rows keep whatever the operator set.
    cur.execute(
        "UPDATE channels SET is_archived = TRUE, "
        "  tier = CASE WHEN tier_source = 'auto' THEN 'hidden' ELSE tier END, "
        "  subscribed = CASE WHEN tier_source = 'auto' THEN TRUE ELSE subscribed END, "
        "  updated_at = now() "
        "WHERE channel_id = %s",
        (channel_id,),
    )
    return ApplyResult(channel_list_changed=True)


def _apply_channel_unarchived(cur: Cursor[TupleRow], payload: JsonObject) -> ApplyResult:
    channel_id = payload.get("channel_id")
    if not isinstance(channel_id, str):
        return ApplyResult()
    cur.execute(
        "UPDATE channels SET is_archived = FALSE, updated_at = now() WHERE channel_id = %s",
        (channel_id,),
    )
    _reevaluate_auto_tier(cur, channel_id)
    return ApplyResult(channel_list_changed=True)


def _apply_channel_member_changed(cur: Cursor[TupleRow], payload: JsonObject) -> ApplyResult:
    channel_id = payload.get("channel_id")
    is_member = payload.get("is_member")
    if not isinstance(channel_id, str) or not isinstance(is_member, bool):
        return ApplyResult()
    cur.execute(
        "UPDATE channels SET is_member = %s, updated_at = now() WHERE channel_id = %s",
        (is_member, channel_id),
    )
    _reevaluate_auto_tier(cur, channel_id)
    return ApplyResult(channel_list_changed=True)


def _reevaluate_auto_tier(cur: Cursor[TupleRow], channel_id: str) -> None:
    """For `tier_source='auto'` rows: recompute the default tier in-place."""
    cur.execute(
        "SELECT is_im, is_mpim, is_member, is_archived, tier_source FROM channels WHERE channel_id = %s",
        (channel_id,),
    )
    row = cur.fetchone()
    if row is None or str(row[4]) != "auto":
        return
    new_tier = _default_tier(
        is_archived=bool(row[3]),
        is_im=bool(row[0]),
        is_mpim=bool(row[1]),
        is_member=bool(row[2]),
    )
    cur.execute(
        "UPDATE channels SET tier = %s, subscribed = %s, updated_at = now() WHERE channel_id = %s",
        (new_tier, new_tier != "blocked", channel_id),
    )


def _collect_channel_mention_refs(cur: Cursor[TupleRow], mentioned_channel_id: str) -> tuple[ChunkRef, ...]:
    cur.execute(
        "SELECT channel_id, message_ts FROM chunk_mentions WHERE mention_kind = 'channel' AND mentioned_id = %s",
        (mentioned_channel_id,),
    )
    return tuple(ChunkRef(str(row[0]), cast(Decimal, row[1])) for row in cur.fetchall())


def _collect_thread_channel_mention_refs(
    cur: Cursor[TupleRow], mentioned_channel_id: str
) -> tuple[ThreadChunkRef, ...]:
    cur.execute(
        "SELECT channel_id, thread_ts, reply_ts FROM thread_chunk_mentions "
        "WHERE mention_kind = 'channel' AND mentioned_id = %s",
        (mentioned_channel_id,),
    )
    return tuple(ThreadChunkRef(str(row[0]), cast(Decimal, row[1]), cast(Decimal, row[2])) for row in cur.fetchall())


def _collect_user_mention_refs(cur: Cursor[TupleRow], user_id: str) -> tuple[ChunkRef, ...]:
    cur.execute(
        "SELECT channel_id, message_ts FROM chunk_mentions WHERE mention_kind = 'user' AND mentioned_id = %s",
        (user_id,),
    )
    return tuple(ChunkRef(str(row[0]), cast(Decimal, row[1])) for row in cur.fetchall())


def _collect_thread_user_mention_refs(cur: Cursor[TupleRow], user_id: str) -> tuple[ThreadChunkRef, ...]:
    cur.execute(
        "SELECT channel_id, thread_ts, reply_ts FROM thread_chunk_mentions "
        "WHERE mention_kind = 'user' AND mentioned_id = %s",
        (user_id,),
    )
    return tuple(ThreadChunkRef(str(row[0]), cast(Decimal, row[1]), cast(Decimal, row[2])) for row in cur.fetchall())


# === `users` stream ===


def _dispatch_users_event(cur: Cursor[TupleRow], kind: str, payload: JsonObject) -> ApplyResult:
    if kind == "user_added":
        return _apply_user_added(cur, payload)
    if kind == "user_renamed":
        return _apply_user_renamed(cur, payload)
    if kind == "user_profile_changed":
        return _apply_user_profile_changed(cur, payload)
    log.warning("apply: unknown users kind %r", kind)
    return ApplyResult()


def _apply_user_added(cur: Cursor[TupleRow], payload: JsonObject) -> ApplyResult:
    try:
        user = SlackUser.model_validate(payload)
    except ValidationError:
        log.warning("apply: rejecting malformed SlackUser payload")
        return ApplyResult()
    display = user.display()
    cur.execute(
        "INSERT INTO users (user_id, display_name) VALUES (%s, %s) "
        "ON CONFLICT (user_id) DO UPDATE SET display_name = EXCLUDED.display_name, updated_at = now()",
        (user.id, display),
    )
    # Same-TX cross-stream lookup: `cur` is the cursor inside apply_event's
    # `with conn.transaction()`, so the upsert above and this `chunk_mentions`
    # scan share one transaction. That ordering is what closes the reviewer's
    # adversarial race — a separate-TX lookup under READ COMMITTED could run
    # between a message's INSERT and COMMIT and miss the just-written chunk.
    refs = _collect_user_mention_refs(cur, user.id)
    thread_refs = _collect_thread_user_mention_refs(cur, user.id)
    return ApplyResult(chunks=refs, thread_chunks=thread_refs)


def _apply_user_renamed(cur: Cursor[TupleRow], payload: JsonObject) -> ApplyResult:
    user_id = payload.get("user_id")
    new_display_name = payload.get("new_display_name")
    if not isinstance(user_id, str) or not isinstance(new_display_name, str):
        log.warning("apply: user_renamed missing fields")
        return ApplyResult()
    cur.execute(
        "INSERT INTO users (user_id, display_name) VALUES (%s, %s) "
        "ON CONFLICT (user_id) DO UPDATE SET display_name = EXCLUDED.display_name, updated_at = now()",
        (user_id, new_display_name),
    )
    refs = _collect_user_mention_refs(cur, user_id)
    thread_refs = _collect_thread_user_mention_refs(cur, user_id)
    return ApplyResult(chunks=refs, thread_chunks=thread_refs)


def _apply_user_profile_changed(cur: Cursor[TupleRow], payload: JsonObject) -> ApplyResult:
    user_id = payload.get("user_id")
    profile_fields = payload.get("profile_fields")
    if not isinstance(user_id, str) or not isinstance(profile_fields, dict):
        log.warning("apply: user_profile_changed missing fields")
        return ApplyResult()
    try:
        profile = SlackUserProfile.model_validate(profile_fields)
    except ValidationError:
        log.warning("apply: rejecting malformed SlackUserProfile payload")
        return ApplyResult()
    display = profile.display_name or profile.real_name or user_id
    cur.execute(
        "INSERT INTO users (user_id, display_name) VALUES (%s, %s) "
        "ON CONFLICT (user_id) DO UPDATE SET display_name = EXCLUDED.display_name, updated_at = now()",
        (user_id, display),
    )
    refs = _collect_user_mention_refs(cur, user_id)
    thread_refs = _collect_thread_user_mention_refs(cur, user_id)
    return ApplyResult(chunks=refs, thread_chunks=thread_refs)


# === `slurper-health` stream ===


_HEALTH_KIND_TO_STATE: dict[str, str] = {
    "slack_healthy": "healthy",
    "slack_degraded": "degraded",
    "socket_mode_disconnected": "disconnected",
    "socket_mode_reconnected": "healthy",
    "auth_token_invalid": "auth_failed",
}


def _dispatch_health_event(cur: Cursor[TupleRow], kind: str, _payload: JsonObject) -> ApplyResult:
    """Update `connection_state.last_slurper_health` from the wire kind.

    `backfill_*` events are observability-only and don't change the trailer
    classification — they're skipped here.
    """
    state = _HEALTH_KIND_TO_STATE.get(kind)
    if state is None:
        return ApplyResult()
    cur.execute(
        "UPDATE connection_state "
        "SET last_slurper_health = %s, last_health_update_at = now(), last_frame_at = now() "
        "WHERE id = 1",
        (state,),
    )
    return ApplyResult()


def _str_field(value: object) -> str:
    return value if isinstance(value, str) else ""


# Re-export for the per_stream applier (also reachable via apply.NullInvalidationSink).
__all__ = [
    "ApplyResult",
    "ChunkRef",
    "InvalidationSink",
    "NullInvalidationSink",
    "ThreadChunkRef",
    "apply_event",
    "apply_snapshot_row",
    "record_caught_up",
    "require_autocommit",
]
