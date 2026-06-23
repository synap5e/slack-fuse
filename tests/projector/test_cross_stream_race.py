"""Cross-stream race test (Sprint 2E acceptance criterion 6).

A `message` event referencing `<@U123>` arrives BEFORE the `user_added` event
for U123. The projector-side preconditions must hold so when `user_added`
eventually lands its lookup finds the affected chunks:

- Chunk's `content_md` stores the unresolved `<@U123>` placeholder.
- `chunk_mentions` row exists with `mention_kind='user' AND mentioned_id='U123'`.
- `user_added` invalidation lookup returns that chunk (so a hypothetical FUSE
  invalidate-inode would fire, dropping the UID-literal-fallback render that
  the kernel cached before `user_added` arrived).

FUSE read path / `notify_store` skip-when-unresolved is Sprint 3B; here we
assert only the projector-side state and the InvalidationSink callback fan-out.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, cast
from zoneinfo import ZoneInfo

import psycopg
import pytest
import trio
from psycopg.rows import TupleRow

from slack_fuse.fuse_ops_v2 import SlackFuseOpsV2
from slack_fuse.models import JsonObject
from slack_fuse.projector.apply import ChunkRef, ThreadChunkRef, apply_event
from slack_fuse_server.wire.frames import EventFrame
from tests._synthetic_events import synthetic_ts
from tests.fuse_v2.conftest import (
    FakePyfuse3,
    mark_stream_caught_up,
    seed_channel,
    set_connection_state,
)

if TYPE_CHECKING:
    from tests.projector.conftest import ClientConnFactory


def _payload(**fields: object) -> JsonObject:
    return cast("JsonObject", dict(fields))


def _chunk_mentions_for_user(conn: psycopg.Connection[TupleRow], user_id: str) -> list[tuple[str, Decimal]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT channel_id, message_ts FROM chunk_mentions "
            "WHERE mention_kind = 'user' AND mentioned_id = %s "
            "ORDER BY channel_id, message_ts",
            (user_id,),
        )
        return [(str(r[0]), Decimal(r[1])) for r in cur.fetchall()]


def test_message_before_user_added_records_mention_with_unresolved_placeholder(
    client_conn: psycopg.Connection[TupleRow],
) -> None:
    """Stage 1: a `message` event referencing `<@URACE>` arrives on `channel:CR1`.

    Acceptance: the chunk stores the raw `<@URACE>` placeholder (no
    UID-literal substitution at write time), and chunk_mentions records the
    `(channel, ts, 'user', 'URACE')` tuple. The `users` table is still empty
    — `user_added` hasn't been applied yet.
    """
    ts = synthetic_ts(0)
    message_payload = _payload(
        type="message",
        ts=ts,
        user="U0",
        text="hello <@URACE> are you here?",
        thread_ts=None,
    )
    apply_event(
        client_conn,
        EventFrame(stream="channel:CR1", offset=1, kind="message", ts=ts, payload=message_payload),
    )

    # Chunk persisted with the unresolved placeholder.
    with client_conn.cursor() as cur:
        cur.execute("SELECT content_md FROM chunks WHERE channel_id = 'CR1'")
        row = cur.fetchone()
    assert row is not None
    assert "<@URACE>" in str(row[0])

    # chunk_mentions row exists despite the user being unknown.
    assert _chunk_mentions_for_user(client_conn, "URACE") == [("CR1", Decimal(ts))]

    # Users table is empty so any read at this point would render `@URACE`
    # (UID literal fallback). This is the projector-side precondition that
    # makes the FUSE-side notify_store-skip safe to enforce later.
    with client_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM users WHERE user_id = 'URACE'")
        row = cur.fetchone()
    assert row is not None
    assert int(row[0]) == 0


def test_late_user_added_invalidates_pre_existing_chunks(
    client_conn: psycopg.Connection[TupleRow],
) -> None:
    """Stage 2: when `user_added` for URACE arrives, the lookup finds the chunk
    written in stage 1; the projector reports it via `InvalidationSink.chunk_changed`.

    This is the linchpin: without `chunk_mentions` being populated at chunk
    write time the lookup would miss and the kernel cache would carry the
    UID-literal forever.
    """
    ts = synthetic_ts(0)
    # Stage 1: message arrives first.
    apply_event(
        client_conn,
        EventFrame(
            stream="channel:CR2",
            offset=1,
            kind="message",
            ts=ts,
            payload=_payload(
                type="message",
                ts=ts,
                user="U0",
                text="hi <@URACE2>",
                thread_ts=None,
            ),
        ),
    )

    # Stage 2: user_added arrives. apply_event surfaces invalidations in its result.
    result = apply_event(
        client_conn,
        EventFrame(
            stream="users",
            offset=1,
            kind="user_added",
            ts=None,
            payload=_payload(
                id="URACE2",
                name="race",
                profile=_payload(display_name="Race User", real_name="Race Real"),
            ),
        ),
    )

    # The chunk written in stage 1 must be in the invalidation set.
    assert ChunkRef("CR2", Decimal(ts)) in result.chunks
    # Users table now carries the display name.
    with client_conn.cursor() as cur:
        cur.execute("SELECT display_name FROM users WHERE user_id = 'URACE2'")
        row = cur.fetchone()
    assert row is not None and str(row[0]) == "Race User"


def test_late_user_added_invalidates_thread_chunks_too(
    client_conn: psycopg.Connection[TupleRow],
) -> None:
    """Thread replies are stored in `thread_chunks` / `thread_chunk_mentions`; the
    same cross-stream race applies to mentions inside replies.
    """
    parent_ts = synthetic_ts(0)
    reply_ts = synthetic_ts(1)
    apply_event(
        client_conn,
        EventFrame(
            stream="channel:CR3",
            offset=1,
            kind="message",
            ts=parent_ts,
            payload=_payload(type="message", ts=parent_ts, user="U0", text="p", thread_ts=parent_ts),
        ),
    )
    apply_event(
        client_conn,
        EventFrame(
            stream="channel:CR3",
            offset=2,
            kind="message",
            ts=reply_ts,
            payload=_payload(
                type="message",
                ts=reply_ts,
                user="U0",
                text="ping <@URACE3>",
                thread_ts=parent_ts,
            ),
        ),
    )

    result = apply_event(
        client_conn,
        EventFrame(
            stream="users",
            offset=1,
            kind="user_added",
            ts=None,
            payload=_payload(id="URACE3", name="n", profile=_payload(display_name="R3", real_name="R3")),
        ),
    )
    assert ThreadChunkRef("CR3", Decimal(parent_ts), Decimal(reply_ts)) in result.thread_chunks


# ---------------------------------------------------------------------------
# Sprint 3E: the reviewer's adversarial race — user_added's cross-stream lookup
# runs BEFORE the message TX commits, so the lookup misses. The read-side
# unresolved-fallback invariant is the backstop that prevents kernel poisoning.
# ---------------------------------------------------------------------------


def _ts_decimal(dt: datetime) -> Decimal:
    return Decimal(str(dt.timestamp()))


@pytest.mark.trio
async def test_adversarial_user_added_lookup_before_message_commit(
    client_conn_factory: ClientConnFactory,
) -> None:
    """``user_added``'s ``chunk_mentions`` lookup runs before the message TX
    commits, so it MISSES — no inode invalidation fires for the message chunk.

    The backstop is the read-side unresolved-fallback invariant: while ``UADV``
    is unresolved, a read of the (now-committed) message renders the UID-literal
    fallback and ``notify_store`` is SKIPPED, so the kernel page cache is never
    poisoned even though the cross-stream invalidation missed. Once ``UADV``
    becomes resolvable, the next read primes the correct bytes.

    NB: ``SlackUser.display()`` falls back to the user id, so the literal
    "user is in the table but still a fallback" state isn't reachable via the
    real apply path. We realise the race faithfully by keeping ``user_added``'s
    TX *uncommitted* at the fallback read — the reader genuinely cannot see the
    user yet, which is exactly the window the backstop must cover.
    """
    setup = client_conn_factory()
    seed_channel(setup, "CADV", "adversarial", tier="hot")
    set_connection_state(setup, last_slurper_health="healthy", last_frame_at_offset_s=1.0)
    mark_stream_caught_up(setup, "channel:CADV")

    ts = _ts_decimal(datetime(2026, 6, 8, 14, 30, tzinfo=UTC))

    # TX-B: the projector's user_added transaction, driven statement-by-statement
    # so we can interleave the message commit + a read between its cross-stream
    # lookup and its commit (apply_event would commit atomically). Mirrors
    # `_apply_user_added`: upsert users THEN scan chunk_mentions.
    user_conn = client_conn_factory()
    user_conn.autocommit = False
    with user_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO users (user_id, display_name) VALUES ('UADV', 'Adv User') "
            "ON CONFLICT (user_id) DO UPDATE SET display_name = EXCLUDED.display_name"
        )
        cur.execute(
            "SELECT channel_id, message_ts FROM chunk_mentions WHERE mention_kind = 'user' AND mentioned_id = 'UADV'"
        )
        missed = cur.fetchall()
    # The lookup found nothing: TX-A (below) has not committed yet under READ
    # COMMITTED, so the cross-stream invalidation has nothing to fire for.
    assert missed == []

    # TX-A: the message write, committing AFTER user_added's lookup already ran.
    msg_conn = client_conn_factory()
    msg_conn.autocommit = False
    with msg_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO chunks (channel_id, message_ts, content_md, reply_count) VALUES (%s, %s, %s, 0)",
            ("CADV", ts, "## 14:30 <@UADV>\n\nhi\n"),
        )
        cur.execute(
            "INSERT INTO chunk_mentions (channel_id, message_ts, mention_kind, mentioned_id) "
            "VALUES (%s, %s, 'user', 'UADV')",
            ("CADV", ts),
        )
    msg_conn.commit()

    # Backstop read: the chunk is now visible, but UADV is still unresolved
    # (TX-B uncommitted). notify_store MUST be skipped → kernel not poisoned.
    read_conn = client_conn_factory()
    fake = FakePyfuse3()
    ops = SlackFuseOpsV2(
        read_conn,
        ZoneInfo("UTC"),
        trio.CapacityLimiter(1),
        notify_store=fake.notify_store,
        invalidate_inode=fake.invalidate_inode,
    )
    day_path = "/channels/adversarial/2026-06/08/channel.md"
    inode = ops.inodes.get_or_create(day_path)
    content_fallback = await ops.read(inode, 0, 131072)
    assert b"@UADV" in content_fallback
    assert b"\xe2\x9a\xa0 Content may be stale" not in content_fallback
    assert fake.notify_calls == []
    assert ops.primed_inodes_snapshot == frozenset()

    # TX-B commits: UADV becomes resolvable workspace-wide.
    user_conn.commit()

    # The next read picks up the user and marks the inode primed.
    # 2026-06-24: notify_store was removed from the read path (deadlock fix);
    # the priming-decision is tracked via primed_inodes instead.
    content_resolved = await ops.read(inode, 0, 131072)
    assert b"@Adv User" in content_resolved
    assert b"@UADV" not in content_resolved
    assert fake.notify_calls == []
    assert ops.primed_inodes_snapshot == frozenset({inode})
