"""Migration runner: discovery (no DB) + apply/idempotency (needs Postgres)."""

from __future__ import annotations

from pathlib import Path

import psycopg
from psycopg.rows import TupleRow

import slack_fuse.migrations as client_migrations
import slack_fuse_server.migrations as server_migrations
from slack_fuse.migrations.runner import apply_migrations, discover_migrations

_CLIENT_DIR = Path(client_migrations.__file__).parent
_SERVER_DIR = Path(server_migrations.__file__).parent


def test_discover_client_migrations() -> None:
    found = discover_migrations(_CLIENT_DIR)
    assert [name for _, name, _ in found] == ["0001_init.sql", "0002_block_sync.sql"]
    assert found[0][0] == 1


def test_discover_server_migrations() -> None:
    found = discover_migrations(_SERVER_DIR)
    assert [name for _, name, _ in found] == [
        "0001_init.sql",
        "0002_users_dedup.sql",
        "0003_channels_dedup.sql",
        "0004_channels_view.sql",
        "0005_health_log_view.sql",
        "0006_blocked_channels.sql",
        "0007_socket_event_dedup.sql",
        "0008_active_messages_view.sql",
        "0009_events_source_column.sql",
        "0010_thread_parent_hint_idx.sql",
    ]


def _relkind(conn: psycopg.Connection[TupleRow], name: str) -> str | None:
    """Returns 'r' for ordinary table, 'v' for view, None if absent."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT relkind FROM pg_class c "
            "JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE c.relname=%s AND n.nspname=current_schema()",
            (name,),
        )
        row = cur.fetchone()
    return None if row is None else str(row[0])


def _table_exists(conn: psycopg.Connection[TupleRow], name: str) -> bool:
    return _relkind(conn, name) == "r"


def test_apply_server_migrations_idempotent(pg_conn: psycopg.Connection[TupleRow]) -> None:
    assert apply_migrations(pg_conn, _SERVER_DIR) == [
        "0001_init.sql",
        "0002_users_dedup.sql",
        "0003_channels_dedup.sql",
        "0004_channels_view.sql",
        "0005_health_log_view.sql",
        "0006_blocked_channels.sql",
        "0007_socket_event_dedup.sql",
        "0008_active_messages_view.sql",
        "0009_events_source_column.sql",
        "0010_thread_parent_hint_idx.sql",
    ]
    assert _table_exists(pg_conn, "events")
    assert _table_exists(pg_conn, "snapshots")
    assert _table_exists(pg_conn, "backfill_overrides")
    assert _table_exists(pg_conn, "blocked_channels")
    # 0004 / 0005 replaced the empty `channels` / `health_log` tables with
    # VIEWs over the events log (ES-clean: one source of truth, no dual write).
    assert _relkind(pg_conn, "channels") == "v"
    assert _relkind(pg_conn, "health_log") == "v"
    assert _relkind(pg_conn, "active_messages") == "v"
    assert _relkind(pg_conn, "active_thread_parents") == "v"
    # The partial dedup indexes exist.
    for index in (
        "events_message_dedup",
        "events_users_added_dedup",
        "events_channels_added_dedup",
        "events_parent_replied_dedup",
        "events_channel_id_changed_dedup",
        "events_channel_history_changed_dedup",
        "events_channel_member_user_dedup",
        "events_tokens_revoked_dedup",
        "events_message_changed_target_idx",
        "events_message_deleted_target_idx",
        "events_parent_replied_target_idx",
        "events_source_backfill_history_idx",
        "events_source_backfill_replies_idx",
        "events_source_commit_idx",
        "events_source_boot_idx",
        "events_source_span_idx",
        "events_message_parent_hint_idx",
        "events_changed_parent_hint_idx",
    ):
        with pg_conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (index,))
            row = cur.fetchone()
        assert row is not None and row[0] is not None
    # Second run applies nothing.
    assert apply_migrations(pg_conn, _SERVER_DIR) == []


def test_apply_client_migrations_idempotent(pg_conn: psycopg.Connection[TupleRow]) -> None:
    assert apply_migrations(pg_conn, _CLIENT_DIR) == ["0001_init.sql", "0002_block_sync.sql"]
    assert _table_exists(pg_conn, "chunks")
    assert _table_exists(pg_conn, "thread_chunks")
    assert _table_exists(pg_conn, "chunk_mentions")
    assert _table_exists(pg_conn, "server_block_sync")
    # connection_state is seeded with its single row.
    with pg_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM connection_state")
        row = cur.fetchone()
    assert row is not None and row[0] == 1
    # Second run applies nothing (and does not re-seed connection_state).
    assert apply_migrations(pg_conn, _CLIENT_DIR) == []
    with pg_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM connection_state")
        row = cur.fetchone()
    assert row is not None and row[0] == 1
