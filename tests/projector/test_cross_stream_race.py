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

from decimal import Decimal
from typing import cast

import psycopg
from psycopg.rows import TupleRow

from slack_fuse.models import JsonObject
from slack_fuse.projector.apply import ChunkRef, ThreadChunkRef, apply_event
from slack_fuse_server.wire.frames import EventFrame
from tests._synthetic_events import synthetic_ts


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
