"""Fixtures for projector tests.

Each test gets a fresh schema with the **client** migrations applied (the
client projections store: chunks, thread_chunks, chunk_mentions, channels,
users, cursors, connection_state, stream_caught_up). Tests can open multiple
connections into the same schema via `client_conn_factory` — needed for
concurrent applier tests where each stream's task owns its own connection (the
production shape).

Pattern mirrors `server_conn_factory` in the top-level conftest: one admin
connection creates/drops the schema; per-test connections set `search_path` and
flip `autocommit = True` (matching the projector's contract).
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterator
from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import TupleRow

import slack_fuse.migrations as client_migrations
from slack_fuse.migrations.runner import apply_migrations
from slack_fuse.projector.apply import ChunkRef, ThreadChunkRef

_CLIENT_MIGRATIONS_DIR = Path(client_migrations.__file__).parent


ClientConnFactory = Callable[[], psycopg.Connection[TupleRow]]


@pytest.fixture
def client_conn_factory(database_url: str) -> Iterator[ClientConnFactory]:
    """Yield a factory that opens autocommit connections in a migrated client schema.

    Uses the session-scoped `database_url` fixture from the top-level conftest:
    either a user-set `DATABASE_URL` or an auto-provisioned temporary cluster
    (Sprint 2F). Lets the projector tests actually exercise their DB-backed
    invariants in default CI runs instead of all silently skipping.
    """
    dsn = database_url

    schema = f"sf_client_{uuid.uuid4().hex}"
    opened: list[psycopg.Connection[TupleRow]] = []
    admin: psycopg.Connection[TupleRow] = psycopg.connect(dsn)
    with admin.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    admin.commit()

    def make() -> psycopg.Connection[TupleRow]:
        conn: psycopg.Connection[TupleRow] = psycopg.connect(dsn)
        with conn.cursor() as cur:
            cur.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
        conn.commit()
        conn.autocommit = True
        opened.append(conn)
        return conn

    setup = make()
    apply_migrations(setup, _CLIENT_MIGRATIONS_DIR)
    setup.commit()

    try:
        yield make
    finally:
        for conn in opened:
            conn.close()
        with admin.cursor() as cur:
            cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
        admin.commit()
        admin.close()


@pytest.fixture
def client_conn(client_conn_factory: ClientConnFactory) -> psycopg.Connection[TupleRow]:
    """A single autocommit connection in a fresh, migrated client schema."""
    return client_conn_factory()


class RecordingSink:
    """An `InvalidationSink` that records every callback in arrival order.

    Lives in conftest so multiple test files share one implementation. Mirrors
    the `apply.InvalidationSink` Protocol (`chunk_changed`,
    `thread_chunk_changed`, `channel_list_changed`).
    """

    def __init__(self) -> None:
        self.chunks: list[ChunkRef] = []
        self.thread_chunks: list[ThreadChunkRef] = []
        self.channel_list_changes: int = 0

    def chunk_changed(self, ref: ChunkRef) -> None:
        self.chunks.append(ref)

    def thread_chunk_changed(self, ref: ThreadChunkRef) -> None:
        self.thread_chunks.append(ref)

    def channel_list_changed(self) -> None:
        self.channel_list_changes += 1
