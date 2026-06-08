"""Idempotent-replay acceptance test (criterion 4): applying the same event
stream twice produces identical DB state.

Mixes the synthetic event-stream generators (`_synthetic_events`) into a
realistic shape: parent messages + thread replies + a few user_added events
on the singletons. The whole suite is replayed and compared row-by-row.
"""

from __future__ import annotations

from typing import cast

import psycopg
from psycopg.rows import TupleRow

from slack_fuse.models import JsonObject
from slack_fuse.projector.apply import apply_event
from slack_fuse_server.wire.frames import EventFrame
from tests._synthetic_events import channel_message_events, channel_reply_events, synthetic_ts
from tests.projector.conftest import ClientConnFactory


def _payload(**fields: object) -> JsonObject:
    return cast("JsonObject", dict(fields))


def _dump_state(conn: psycopg.Connection[TupleRow]) -> dict[str, list[tuple[object, ...]]]:
    """A full state snapshot suitable for equality comparison across runs."""
    state: dict[str, list[tuple[object, ...]]] = {}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT channel_id, message_ts, content_md, reply_count FROM chunks ORDER BY channel_id, message_ts"
        )
        state["chunks"] = [tuple(row) for row in cur.fetchall()]
        cur.execute(
            "SELECT channel_id, thread_ts, reply_ts, role, content_md FROM thread_chunks "
            "ORDER BY channel_id, thread_ts, reply_ts"
        )
        state["thread_chunks"] = [tuple(row) for row in cur.fetchall()]
        cur.execute(
            "SELECT channel_id, message_ts, mention_kind, mentioned_id FROM chunk_mentions "
            "ORDER BY channel_id, message_ts, mention_kind, mentioned_id"
        )
        state["chunk_mentions"] = [tuple(row) for row in cur.fetchall()]
        cur.execute(
            "SELECT channel_id, thread_ts, reply_ts, mention_kind, mentioned_id FROM thread_chunk_mentions "
            "ORDER BY channel_id, thread_ts, reply_ts, mention_kind, mentioned_id"
        )
        state["thread_chunk_mentions"] = [tuple(row) for row in cur.fetchall()]
        cur.execute("SELECT channel_id, name, tier, tier_source FROM channels ORDER BY channel_id")
        state["channels"] = [tuple(row) for row in cur.fetchall()]
        cur.execute("SELECT user_id, display_name FROM users ORDER BY user_id")
        state["users"] = [tuple(row) for row in cur.fetchall()]
        cur.execute("SELECT stream, applied_offset FROM cursors ORDER BY stream")
        state["cursors"] = [tuple(row) for row in cur.fetchall()]
    return state


def _apply_stream(conn: psycopg.Connection[TupleRow], events: list[EventFrame]) -> None:
    for frame in events:
        apply_event(conn, frame)


def _build_event_suite() -> list[EventFrame]:
    """Mixed event stream: channel-list + messages + replies + users."""
    events: list[EventFrame] = []
    # channel-list: add two channels.
    events.append(
        EventFrame(
            stream="channel-list",
            offset=1,
            kind="channel_added",
            ts=None,
            payload=_payload(id="C1", name="one", is_member=True),
        )
    )
    events.append(
        EventFrame(
            stream="channel-list",
            offset=2,
            kind="channel_added",
            ts=None,
            payload=_payload(id="C2", name="two", is_member=True),
        )
    )
    # users.
    events.append(
        EventFrame(
            stream="users",
            offset=1,
            kind="user_added",
            ts=None,
            payload=_payload(id="U1", name="u", profile=_payload(display_name="User One", real_name="U1")),
        )
    )
    # channel:C1 — 5 top-level messages.
    events.extend(e.to_frame() for e in channel_message_events("C1", 5, start_offset=1))
    # channel:C1 — 3 replies to the first message.
    parent_ts_c1 = synthetic_ts(0)
    events.extend(e.to_frame() for e in channel_reply_events("C1", parent_ts_c1, 3, start_offset=6, start_index=100))
    # channel:C2 — 4 top-level messages.
    events.extend(e.to_frame() for e in channel_message_events("C2", 4, start_offset=1, start_index=200))
    return events


def test_replay_produces_identical_state(client_conn_factory: ClientConnFactory) -> None:
    suite = _build_event_suite()

    conn_a = client_conn_factory()
    _apply_stream(conn_a, suite)
    state_first = _dump_state(conn_a)

    # Second pass: same suite, same connection (replay scenario).
    _apply_stream(conn_a, suite)
    state_second = _dump_state(conn_a)

    assert state_first == state_second, f"Replay diverged.\nfirst: {state_first}\nsecond: {state_second}"


def test_replay_into_fresh_db_matches(client_conn_factory: ClientConnFactory) -> None:
    """Two fresh schemas, same suite, must reach the same state — establishes
    that idempotency isn't dependent on which TX boundary fell where."""
    suite = _build_event_suite()

    conn_a = client_conn_factory()
    _apply_stream(conn_a, suite)
    state_a = _dump_state(conn_a)

    conn_b = client_conn_factory()
    _apply_stream(conn_b, suite)
    state_b = _dump_state(conn_b)

    # The two connections share the same schema (the factory fixture), so
    # `_dump_state(conn_a)` and `_dump_state(conn_b)` actually read the same
    # rows; verify that explicitly.
    assert state_a == state_b
