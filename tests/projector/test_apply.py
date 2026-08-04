# pyright: reportPrivateUsage=false
"""Apply unit tests — every entry in the §Projection logic table.

These exercise `apply_event` against a real Postgres schema with the client
migrations applied. Each test owns a fresh schema (client_conn fixture); no
test depends on another's state.
"""

from __future__ import annotations

import functools
import re
import threading
from decimal import Decimal
from typing import cast
from zoneinfo import ZoneInfo

import psycopg
import pytest
from psycopg.rows import TupleRow

import slack_fuse.projector.apply as apply_module
from slack_fuse.models import JsonObject
from slack_fuse.projector.apply import (
    apply_event as _apply_event,
    record_caught_up,
    require_autocommit,
)
from slack_fuse_server.wire.frames import EventFrame
from tests._synthetic_events import (
    SyntheticEvent,
    channel_message_events,
    channel_reply_events,
    synthetic_ts,
)
from tests.projector.conftest import ClientConnFactory

apply_event = functools.partial(_apply_event, tz=ZoneInfo("UTC"))

# === helpers ===


def _payload(**fields: object) -> JsonObject:
    """Build a `JsonObject` from arbitrary kwargs. The pydantic frame validator
    accepts whatever shape the projector handles; this helper just shuts
    pyright's invariant-dict-value complaint up at test edge."""
    return cast("JsonObject", dict(fields))


def _chunks(conn: psycopg.Connection[TupleRow]) -> list[tuple[str, Decimal, str, int]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT channel_id, message_ts, content_md, reply_count FROM chunks ORDER BY channel_id, message_ts"
        )
        return [(str(r[0]), Decimal(r[1]), str(r[2]), int(r[3])) for r in cur.fetchall()]


def _thread_chunks(conn: psycopg.Connection[TupleRow]) -> list[tuple[str, Decimal, Decimal, str, str]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT channel_id, thread_ts, reply_ts, role, content_md "
            "FROM thread_chunks ORDER BY channel_id, thread_ts, reply_ts"
        )
        return [(str(r[0]), Decimal(r[1]), Decimal(r[2]), str(r[3]), str(r[4])) for r in cur.fetchall()]


def _chunk_mentions(conn: psycopg.Connection[TupleRow]) -> list[tuple[str, Decimal, str, str]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT channel_id, message_ts, mention_kind, mentioned_id "
            "FROM chunk_mentions ORDER BY channel_id, message_ts, mention_kind, mentioned_id"
        )
        return [(str(r[0]), Decimal(r[1]), str(r[2]), str(r[3])) for r in cur.fetchall()]


def _cursor(conn: psycopg.Connection[TupleRow], stream: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT applied_offset FROM cursors WHERE stream = %s", (stream,))
        row = cur.fetchone()
    return 0 if row is None else int(row[0])


def _channels(conn: psycopg.Connection[TupleRow]) -> list[tuple[str, str | None, str, str]]:
    with conn.cursor() as cur:
        cur.execute("SELECT channel_id, name, tier, tier_source FROM channels ORDER BY channel_id")
        return [(str(r[0]), None if r[1] is None else str(r[1]), str(r[2]), str(r[3])) for r in cur.fetchall()]


def _users(conn: psycopg.Connection[TupleRow]) -> list[tuple[str, str]]:
    with conn.cursor() as cur:
        cur.execute("SELECT user_id, display_name FROM users ORDER BY user_id")
        return [(str(r[0]), str(r[1])) for r in cur.fetchall()]


# === message: top-level (chunks) ===


def test_top_level_message_writes_chunk_and_mentions(client_conn: psycopg.Connection[TupleRow]) -> None:
    """message with thread_ts=None → INSERT chunks + chunk_mentions; advance cursor."""
    [event] = list(channel_message_events("CTOP", 1, start_offset=42, start_index=0))
    apply_event(client_conn, event.to_frame())

    rows = _chunks(client_conn)
    assert len(rows) == 1
    channel, ts, content_md, reply_count = rows[0]
    assert event.ts is not None
    assert (channel, ts, reply_count) == ("CTOP", Decimal(event.ts), 0)
    # The synthetic generator embeds "<@U0001>" in the text; structural pass
    # keeps the placeholder unresolved (so resolve_mentions can substitute at
    # read time).
    assert "<@U0001>" in content_md

    # chunk_mentions captures both the author (U0000) and the mentioned user.
    mentions = _chunk_mentions(client_conn)
    user_ids = sorted(m[3] for m in mentions if m[2] == "user")
    assert "U0001" in user_ids

    assert _cursor(client_conn, "channel:CTOP") == 42


def test_top_level_message_replay_is_idempotent(client_conn: psycopg.Connection[TupleRow]) -> None:
    """Applying the same event twice produces identical state (ON CONFLICT DO UPDATE)."""
    [event] = list(channel_message_events("CIDM", 1, start_offset=5))
    apply_event(client_conn, event.to_frame())
    chunks_first = _chunks(client_conn)
    mentions_first = _chunk_mentions(client_conn)

    apply_event(client_conn, event.to_frame())
    assert _chunks(client_conn) == chunks_first
    assert _chunk_mentions(client_conn) == mentions_first
    assert _cursor(client_conn, "channel:CIDM") == 5


def test_blocked_channel_events_dropped_by_applier(client_conn: psycopg.Connection[TupleRow]) -> None:
    with client_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO channels "
            "(channel_id, name, is_im, is_mpim, is_member, is_archived, tier, tier_source, subscribed) "
            "VALUES ('CBLOCKED', 'blocked', FALSE, FALSE, TRUE, FALSE, 'blocked', 'manual', FALSE)"
        )
    [event] = list(channel_message_events("CBLOCKED", 1, start_offset=9))

    apply_event(client_conn, event.to_frame())

    assert _chunks(client_conn) == []
    assert _cursor(client_conn, "channel:CBLOCKED") == 9


def test_apply_event_blocked_race_serializes_via_row_lock(
    client_conn_factory: ClientConnFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed_conn = client_conn_factory()
    with seed_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO channels "
            "(channel_id, name, is_im, is_mpim, is_member, is_archived, tier, tier_source, subscribed) "
            "VALUES ('CRACE', 'race', FALSE, FALSE, TRUE, FALSE, 'hot', 'auto', TRUE)"
        )
    [event] = list(channel_message_events("CRACE", 1, start_offset=1))
    apply_conn = client_conn_factory()
    block_conn = client_conn_factory()
    apply_has_lock = threading.Event()
    release_apply = threading.Event()
    block_started = threading.Event()
    block_done = threading.Event()
    errors: list[BaseException] = []
    real_apply_message = apply_module._apply_message

    def paused_apply_message(
        cur: psycopg.Cursor[TupleRow],
        channel_id: str,
        payload: JsonObject,
    ) -> apply_module.ApplyResult:
        apply_has_lock.set()
        if not release_apply.wait(timeout=5):
            msg = "timed out waiting to release paused channel apply"
            raise TimeoutError(msg)
        return real_apply_message(cur, channel_id, payload)

    monkeypatch.setattr(apply_module, "_apply_message", paused_apply_message)

    def run_apply() -> None:
        try:
            apply_event(apply_conn, event.to_frame())
        except BaseException as exc:  # pragma: no cover - assertion reports thread errors
            errors.append(exc)

    def run_block() -> None:
        try:
            block_started.set()
            with block_conn.transaction(), block_conn.cursor() as cur:
                cur.execute(
                    "UPDATE channels SET tier = 'blocked', tier_source = 'manual', subscribed = FALSE "
                    "WHERE channel_id = 'CRACE'"
                )
            block_done.set()
        except BaseException as exc:  # pragma: no cover - assertion reports thread errors
            errors.append(exc)

    apply_thread = threading.Thread(target=run_apply)
    block_thread = threading.Thread(target=run_block)
    apply_thread.start()
    assert apply_has_lock.wait(timeout=5)
    block_thread.start()
    assert block_started.wait(timeout=5)
    assert not block_done.wait(timeout=0.2), "block commit bypassed the event apply row lock"
    release_apply.set()
    apply_thread.join(timeout=5)
    block_thread.join(timeout=5)

    assert not apply_thread.is_alive()
    assert not block_thread.is_alive()
    assert errors == []
    assert len(_chunks(seed_conn)) == 1
    assert _cursor(seed_conn, "channel:CRACE") == 1
    assert _channels(seed_conn)[0][2] == "blocked"


# === message: thread reply (thread_chunks) ===


def test_reply_writes_thread_chunk_and_bumps_parent_count(client_conn: psycopg.Connection[TupleRow]) -> None:
    """Reply (thread_ts != ts) → INSERT thread_chunks; parent reply_count refreshed via COUNT(*)."""
    parent_ts = synthetic_ts(0)
    parent_event = SyntheticEvent(
        stream="channel:CTHR",
        offset=1,
        kind="message",
        ts=parent_ts,
        payload=_payload(
            type="message",
            ts=parent_ts,
            user="U0000",
            text="parent message",
            thread_ts=parent_ts,  # top-level
            reply_count=0,
        ),
    )
    apply_event(client_conn, parent_event.to_frame())

    replies = list(channel_reply_events("CTHR", parent_ts, 3, start_offset=2, start_index=10))
    for r in replies:
        apply_event(client_conn, r.to_frame())

    # All replies live in thread_chunks with role='reply'.
    threads = _thread_chunks(client_conn)
    assert len(threads) == 3
    assert all(t[3] == "reply" for t in threads)
    assert all(t[1] == Decimal(parent_ts) for t in threads)

    # Parent chunk's reply_count column == COUNT(replies).
    [(_, _, content_md, reply_count)] = _chunks(client_conn)
    assert reply_count == 3
    # Indicator patched into content_md (v1 regex patch keeps rendered text consistent).
    assert "> Thread: 3 replies" in content_md


def test_reply_count_is_idempotent_on_replay(client_conn: psycopg.Connection[TupleRow]) -> None:
    """Replaying parent + replies leaves reply_count unchanged (derived via COUNT, not +=1)."""
    parent_ts = synthetic_ts(0)
    parent_event = SyntheticEvent(
        stream="channel:CIDP",
        offset=1,
        kind="message",
        ts=parent_ts,
        payload=_payload(type="message", ts=parent_ts, user="U0", text="p", thread_ts=parent_ts),
    )
    replies = list(channel_reply_events("CIDP", parent_ts, 2, start_offset=2, start_index=20))

    apply_event(client_conn, parent_event.to_frame())
    for r in replies:
        apply_event(client_conn, r.to_frame())
    state_first = (_chunks(client_conn), _thread_chunks(client_conn))

    # Replay everything.
    apply_event(client_conn, parent_event.to_frame())
    for r in replies:
        apply_event(client_conn, r.to_frame())
    state_second = (_chunks(client_conn), _thread_chunks(client_conn))

    assert state_first == state_second
    assert state_first[0][0][3] == 2  # reply_count


# === message_changed ===


def test_message_changed_updates_chunk_content(client_conn: psycopg.Connection[TupleRow]) -> None:
    """message_changed payload `{message, previous_ts}` re-renders + refreshes mentions."""
    [event] = list(channel_message_events("CEDIT", 1, start_offset=1))
    apply_event(client_conn, event.to_frame())
    original_md = _chunks(client_conn)[0][2]

    edited_payload = cast(
        "JsonObject",
        {
            "message": {
                "type": "message",
                "ts": event.ts,
                "user": "U0000",
                "text": "EDITED CONTENT no mention here",
                "thread_ts": None,
                "edited": {"user": "U0000", "ts": event.ts},
            },
            "previous_ts": event.ts,
        },
    )
    edit_frame = EventFrame(
        stream="channel:CEDIT", offset=2, kind="message_changed", ts=event.ts, payload=edited_payload
    )
    apply_event(client_conn, edit_frame)

    new_md = _chunks(client_conn)[0][2]
    assert new_md != original_md
    assert "EDITED CONTENT" in new_md
    # Old mentions cleared (no <@U0001> in the new content).
    assert event.ts is not None
    mentions = [m for m in _chunk_mentions(client_conn) if m[1] == Decimal(event.ts)]
    assert "U0001" not in [m[3] for m in mentions if m[2] == "user"]


# === message_deleted ===


def test_message_deleted_removes_chunk(client_conn: psycopg.Connection[TupleRow]) -> None:
    [event] = list(channel_message_events("CDEL", 1, start_offset=1))
    apply_event(client_conn, event.to_frame())
    assert len(_chunks(client_conn)) == 1

    delete_frame = EventFrame(
        stream="channel:CDEL",
        offset=2,
        kind="message_deleted",
        ts=event.ts,
        payload=_payload(deleted_ts=event.ts, previous_message=None),
    )
    apply_event(client_conn, delete_frame)
    assert _chunks(client_conn) == []
    # chunk_mentions CASCADEs away with the chunk.
    assert _chunk_mentions(client_conn) == []


def test_message_deleted_reply_removes_thread_chunk_and_refreshes_parent(
    client_conn: psycopg.Connection[TupleRow],
) -> None:
    parent_ts = synthetic_ts(0)
    apply_event(
        client_conn,
        EventFrame(
            stream="channel:CDR",
            offset=1,
            kind="message",
            ts=parent_ts,
            payload=_payload(type="message", ts=parent_ts, user="U0", text="p", thread_ts=parent_ts),
        ),
    )
    [reply] = list(channel_reply_events("CDR", parent_ts, 1, start_offset=2, start_index=30))
    apply_event(client_conn, reply.to_frame())
    assert _chunks(client_conn)[0][3] == 1  # parent reply_count

    delete_payload = _payload(
        deleted_ts=reply.ts,
        previous_message=_payload(
            type="message",
            ts=reply.ts,
            user="U0030",
            text="synthetic reply 30",
            thread_ts=parent_ts,
        ),
    )
    apply_event(
        client_conn,
        EventFrame(stream="channel:CDR", offset=3, kind="message_deleted", ts=reply.ts, payload=delete_payload),
    )
    assert _thread_chunks(client_conn) == []
    # Parent reply_count walked back to 0.
    assert _chunks(client_conn)[0][3] == 0


# === channel-list events ===


def test_channel_info_refreshed_upserts_membership_and_promotes_tier(
    client_conn: psycopg.Connection[TupleRow],
) -> None:
    """The 2026-06-27 refresh sweep emits ``channel_info_refreshed`` events
    when the most recent payload differs from a fresh ``conversations.info``
    response. Pre-fix the projector treated it as an unknown kind and the
    local ``channels.is_member`` stayed stuck at whatever ``channel_added``
    set — concretely: a channel you joined still appeared with
    ``is_member=false`` and ``tier=hidden``, so it wasn't even in
    ``readdir`` until you manually patched the row.

    Now we dispatch ``channel_info_refreshed`` to the same handler as
    ``channel_added`` — same payload shape, ``ON CONFLICT DO UPDATE``
    handles the upsert. Auto-tier re-evaluates against the new is_member
    so a "we just joined" refresh promotes hidden → hot.
    """
    # Seed: original channel_added when we weren't a member yet.
    apply_event(
        client_conn,
        EventFrame(
            stream="channel-list",
            offset=1,
            kind="channel_added",
            ts=None,
            payload=_payload(id="C001", name="frontend-nightly", is_member=False),
        ),
    )
    rows_before = _channels(client_conn)
    assert rows_before == [("C001", "frontend-nightly", "hidden", "auto")]

    # Refresh sweep delivers the new state: is_member=true.
    apply_event(
        client_conn,
        EventFrame(
            stream="channel-list",
            offset=2,
            kind="channel_info_refreshed",
            ts=None,
            payload=_payload(id="C001", name="frontend-nightly", is_member=True),
        ),
    )
    rows_after = _channels(client_conn)
    assert rows_after == [("C001", "frontend-nightly", "hot", "auto")]


def test_channel_member_joined_and_left_are_no_op_but_not_unknown(
    client_conn: psycopg.Connection[TupleRow],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Server emits ``channel_member_joined`` / ``channel_member_left`` for
    every user's membership change on channels this token can see (post
    2026-07-22 self-join detection + FINDING-16). The server's channels view
    folds these for aggregate counts; the client's ``channels`` table only
    tracks THIS token's ``is_member`` (flipped via ``channel_added`` on
    self-join, ``channel_member_changed`` on self-leave), so other-user
    membership events are correctly a no-op for the client projection.

    Regression: pre-fix the client logged ``apply: unknown channel-list kind
    'channel_member_joined'`` per event (reported 2026-08-01 on
    flow-crastinator, ~many hits per minute during backfill of a 3.7-day gap).
    """
    # Seed a channel so the events have somewhere plausible to land.
    apply_event(
        client_conn,
        EventFrame(
            stream="channel-list",
            offset=1,
            kind="channel_added",
            ts=None,
            payload=_payload(id="C001", name="general", is_member=True),
        ),
    )
    rows_before = _channels(client_conn)

    caplog.clear()
    caplog.set_level("WARNING", logger="slack_fuse.projector.apply")
    for offset, kind in ((2, "channel_member_joined"), (3, "channel_member_left")):
        apply_event(
            client_conn,
            EventFrame(
                stream="channel-list",
                offset=offset,
                kind=kind,
                ts=None,
                payload={"channel_id": "C001", "user_id": "U_OTHER"},
            ),
        )

    # No projection change — client doesn't fold other-user membership.
    assert _channels(client_conn) == rows_before
    # And no "unknown channel-list kind" warning was emitted (that's the
    # visible symptom this fix stops).
    unknown_kind_warnings = [
        rec for rec in caplog.records if "unknown channel-list kind" in rec.getMessage()
    ]
    assert unknown_kind_warnings == [], (
        f"expected zero unknown-kind warnings; got: {[r.getMessage() for r in unknown_kind_warnings]}"
    )


def test_channel_message_count_probe_is_no_op_but_not_unknown(
    client_conn: psycopg.Connection[TupleRow],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level("WARNING", logger="slack_fuse.projector.apply")
    apply_event(
        client_conn,
        EventFrame(
            stream="channel-list",
            offset=1,
            kind="channel_message_count_probed",
            ts="2026-08-02T00:00:00.000000Z",
            payload={"channel_id": "C001", "message_total": 42, "approximate": False},
        ),
    )

    assert _cursor(client_conn, "channel-list") == 1
    assert not any("unknown channel-list kind" in record.getMessage() for record in caplog.records)


def test_channel_info_refreshed_preserves_manual_tier_override(
    client_conn: psycopg.Connection[TupleRow],
) -> None:
    """``tier_source='manual'`` survives across refreshes — an operator
    who explicitly hid a channel via ``slack-fuse tier <slug> hidden``
    shouldn't have it auto-promoted back to hot by the next refresh."""
    apply_event(
        client_conn,
        EventFrame(
            stream="channel-list",
            offset=1,
            kind="channel_added",
            ts=None,
            payload=_payload(id="C001", name="noisy", is_member=True),
        ),
    )
    # Operator pins it to hidden / manual.
    with client_conn.cursor() as cur:
        cur.execute(
            "UPDATE channels SET tier = 'hidden', tier_source = 'manual' "
            "WHERE channel_id = %s",
            ("C001",),
        )

    apply_event(
        client_conn,
        EventFrame(
            stream="channel-list",
            offset=2,
            kind="channel_info_refreshed",
            ts=None,
            payload=_payload(id="C001", name="noisy", is_member=True),
        ),
    )
    rows = _channels(client_conn)
    # Manual override holds — refresh DOES update name/membership but
    # NOT tier.
    assert rows == [("C001", "noisy", "hidden", "manual")]


def test_channel_added_with_member_uses_hot_tier(client_conn: psycopg.Connection[TupleRow]) -> None:
    apply_event(
        client_conn,
        EventFrame(
            stream="channel-list",
            offset=1,
            kind="channel_added",
            ts=None,
            payload=_payload(id="C001", name="general", is_member=True),
        ),
    )
    rows = _channels(client_conn)
    assert rows == [("C001", "general", "hot", "auto")]


def test_channel_added_archived_defaults_to_hidden(client_conn: psycopg.Connection[TupleRow]) -> None:
    """Archived channels default to `hidden`, not `blocked` — they stay reachable
    by known path (and the projector keeps subscribed=TRUE so any in-flight
    historical events drain). Operator can promote with `slack-fuse tier
    <slug> hot` if they want it listed, or bury with `... blocked`.
    """
    apply_event(
        client_conn,
        EventFrame(
            stream="channel-list",
            offset=1,
            kind="channel_added",
            ts=None,
            payload=_payload(id="C002", name="old-room", is_archived=True),
        ),
    )
    rows = _channels(client_conn)
    assert rows == [("C002", "old-room", "hidden", "auto")]


def test_channel_added_archived_stays_subscribed(client_conn: psycopg.Connection[TupleRow]) -> None:
    """Archived → hidden (not blocked), so `subscribed=TRUE` and the projector
    keeps consuming the per-channel stream — backfilled history flows in."""
    apply_event(
        client_conn,
        EventFrame(
            stream="channel-list",
            offset=1,
            kind="channel_added",
            ts=None,
            payload=_payload(id="C002", name="old-room", is_archived=True),
        ),
    )
    with client_conn.cursor() as cur:
        cur.execute("SELECT subscribed FROM channels WHERE channel_id = 'C002'")
        row = cur.fetchone()
    assert row is not None
    assert row[0] is True


def test_channel_renamed_updates_name(client_conn: psycopg.Connection[TupleRow]) -> None:
    apply_event(
        client_conn,
        EventFrame(
            stream="channel-list",
            offset=1,
            kind="channel_added",
            ts=None,
            payload=_payload(id="C003", name="old", is_member=True),
        ),
    )
    apply_event(
        client_conn,
        EventFrame(
            stream="channel-list",
            offset=2,
            kind="channel_renamed",
            ts=None,
            payload=_payload(channel_id="C003", new_name="new"),
        ),
    )
    rows = _channels(client_conn)
    assert rows[0][:2] == ("C003", "new")


def test_channel_archived_demotes_to_hidden_when_auto(client_conn: psycopg.Connection[TupleRow]) -> None:
    """A live (hot) member channel that Slack archives demotes to `hidden`, not
    `blocked` — reachable by known path, subscription stays open so any
    trailing messages still drain. Matches ``_default_tier``'s archived branch.
    """
    apply_event(
        client_conn,
        EventFrame(
            stream="channel-list",
            offset=1,
            kind="channel_added",
            ts=None,
            payload=_payload(id="C004", name="live", is_member=True),
        ),
    )
    apply_event(
        client_conn,
        EventFrame(
            stream="channel-list",
            offset=2,
            kind="channel_archived",
            ts=None,
            payload=_payload(channel_id="C004"),
        ),
    )
    rows = _channels(client_conn)
    assert rows[0][2] == "hidden"
    with client_conn.cursor() as cur:
        cur.execute("SELECT subscribed FROM channels WHERE channel_id = 'C004'")
        row = cur.fetchone()
    assert row is not None
    assert row[0] is True


def test_channel_archived_respects_manual_tier(client_conn: psycopg.Connection[TupleRow]) -> None:
    apply_event(
        client_conn,
        EventFrame(
            stream="channel-list",
            offset=1,
            kind="channel_added",
            ts=None,
            payload=_payload(id="C005", name="manual", is_member=True),
        ),
    )
    # Flip to manual hot.
    with client_conn.cursor() as cur:
        cur.execute("UPDATE channels SET tier = 'hot', tier_source = 'manual' WHERE channel_id = 'C005'")
    apply_event(
        client_conn,
        EventFrame(
            stream="channel-list",
            offset=2,
            kind="channel_archived",
            ts=None,
            payload=_payload(channel_id="C005"),
        ),
    )
    rows = _channels(client_conn)
    assert rows[0] == ("C005", "manual", "hot", "manual")


def test_channel_member_changed_revalues_tier(client_conn: psycopg.Connection[TupleRow]) -> None:
    apply_event(
        client_conn,
        EventFrame(
            stream="channel-list",
            offset=1,
            kind="channel_added",
            ts=None,
            payload=_payload(id="C006", name="joined", is_member=True),
        ),
    )
    apply_event(
        client_conn,
        EventFrame(
            stream="channel-list",
            offset=2,
            kind="channel_member_changed",
            ts=None,
            payload=_payload(channel_id="C006", is_member=False),
        ),
    )
    # Public-not-joined → hidden by default.
    assert _channels(client_conn)[0][2] == "hidden"


# The block of ``always_blocked`` regression tests that lived here was
# deleted 2026-07-17 alongside FINDING-17 — the parameter chain the tests
# exercised was never wired to production (WSClient constructor never got
# the frozenset), and the applier-side enforcement has been removed.
# Block enforcement lives entirely in ``block_sync.apply_blocked_channel_sync``
# now; see ``tests/projector/test_block_sync.py``.


# === users events ===


def test_user_added_writes_users_row_with_display(client_conn: psycopg.Connection[TupleRow]) -> None:
    payload = _payload(
        id="U123",
        name="alice-fallback",
        profile=_payload(display_name="Alice", real_name="Alice Doe"),
    )
    apply_event(
        client_conn,
        EventFrame(stream="users", offset=1, kind="user_added", ts=None, payload=payload),
    )
    assert _users(client_conn) == [("U123", "Alice")]


def test_user_renamed_updates_display(client_conn: psycopg.Connection[TupleRow]) -> None:
    apply_event(
        client_conn,
        EventFrame(
            stream="users",
            offset=1,
            kind="user_added",
            ts=None,
            payload=_payload(id="U200", name="u", profile=_payload(display_name="Old", real_name="U")),
        ),
    )
    apply_event(
        client_conn,
        EventFrame(
            stream="users",
            offset=2,
            kind="user_renamed",
            ts=None,
            payload=_payload(user_id="U200", new_display_name="New"),
        ),
    )
    assert _users(client_conn) == [("U200", "New")]


def test_user_profile_changed_updates_display(client_conn: psycopg.Connection[TupleRow]) -> None:
    apply_event(
        client_conn,
        EventFrame(
            stream="users",
            offset=1,
            kind="user_added",
            ts=None,
            payload=_payload(id="U300", name="u", profile=_payload(display_name="DN", real_name="RN")),
        ),
    )
    apply_event(
        client_conn,
        EventFrame(
            stream="users",
            offset=2,
            kind="user_profile_changed",
            ts=None,
            payload=_payload(
                user_id="U300",
                profile_fields=_payload(display_name="DN2", real_name="RN"),
            ),
        ),
    )
    assert _users(client_conn) == [("U300", "DN2")]


# === slurper-health ===


def test_slurper_health_updates_connection_state(client_conn: psycopg.Connection[TupleRow]) -> None:
    apply_event(
        client_conn,
        EventFrame(
            stream="slurper-health",
            offset=1,
            kind="slack_degraded",
            ts=None,
            payload=_payload(reason="rate_limited"),
        ),
    )
    with client_conn.cursor() as cur:
        cur.execute("SELECT last_slurper_health FROM connection_state WHERE id = 1")
        row = cur.fetchone()
    assert row is not None
    assert str(row[0]) == "degraded"


# === record_caught_up ===


def test_record_caught_up_inserts_then_updates_monotonic(client_conn: psycopg.Connection[TupleRow]) -> None:
    record_caught_up(client_conn, "channel:CCU", 100)
    record_caught_up(client_conn, "channel:CCU", 50)  # older — should NOT roll back
    record_caught_up(client_conn, "channel:CCU", 200)
    with client_conn.cursor() as cur:
        cur.execute("SELECT at_offset FROM stream_caught_up WHERE stream = 'channel:CCU'")
        row = cur.fetchone()
    assert row is not None
    assert int(row[0]) == 200


# === autocommit guard ===


def test_require_autocommit_raises_on_non_autocommit(client_conn_factory: ClientConnFactory) -> None:
    """Mirrors the OffsetWriter contract: bad connection ⇒ fail fast with a specific message."""
    conn = client_conn_factory()
    conn.autocommit = False
    try:
        with pytest.raises(ValueError, match=re.escape("conn.autocommit=True")):
            require_autocommit(conn)
    finally:
        conn.autocommit = True
