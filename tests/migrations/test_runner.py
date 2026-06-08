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
    assert [name for _, name, _ in found] == ["0001_init.sql"]
    assert found[0][0] == 1


def test_discover_server_migrations() -> None:
    found = discover_migrations(_SERVER_DIR)
    assert [name for _, name, _ in found] == ["0001_init.sql", "0002_users_dedup.sql"]


def _table_exists(conn: psycopg.Connection[TupleRow], name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (name,))
        row = cur.fetchone()
    return row is not None and row[0] is not None


def test_apply_server_migrations_idempotent(pg_conn: psycopg.Connection[TupleRow]) -> None:
    assert apply_migrations(pg_conn, _SERVER_DIR) == ["0001_init.sql", "0002_users_dedup.sql"]
    assert _table_exists(pg_conn, "events")
    assert _table_exists(pg_conn, "snapshots")
    assert _table_exists(pg_conn, "backfill_overrides")
    # The partial dedup indexes exist (both message and users-added).
    with pg_conn.cursor() as cur:
        cur.execute("SELECT to_regclass('events_message_dedup')")
        row = cur.fetchone()
    assert row is not None and row[0] is not None
    with pg_conn.cursor() as cur:
        cur.execute("SELECT to_regclass('events_users_added_dedup')")
        row = cur.fetchone()
    assert row is not None and row[0] is not None
    # Second run applies nothing.
    assert apply_migrations(pg_conn, _SERVER_DIR) == []


def test_apply_client_migrations_idempotent(pg_conn: psycopg.Connection[TupleRow]) -> None:
    assert apply_migrations(pg_conn, _CLIENT_DIR) == ["0001_init.sql"]
    assert _table_exists(pg_conn, "chunks")
    assert _table_exists(pg_conn, "thread_chunks")
    assert _table_exists(pg_conn, "chunk_mentions")
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
