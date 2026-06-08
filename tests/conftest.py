"""Shared pytest fixtures for the slack-fuse test suite.

- `pg_conn`: a Postgres connection scoped to a fresh, uniquely-named schema per
  test (pg_temp-style isolation), torn down with `DROP SCHEMA ... CASCADE`. If
  `DATABASE_URL` is not set the fixture skips the test with a clear message, so
  the suite stays green on machines without Postgres.
- `fake_slack_transport` / `fake_slack_http`: the fixture-backed fake Slack Web
  API (see `tests/_fake_slack`).
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator

import httpx
import psycopg
import pytest
from psycopg import sql
from psycopg.rows import TupleRow

from tests._fake_slack import make_fake_slack_transport

_DATABASE_URL = os.environ.get("DATABASE_URL")


@pytest.fixture
def pg_conn() -> Iterator[psycopg.Connection[TupleRow]]:
    """A Postgres connection isolated to a per-test schema.

    Skipped when `DATABASE_URL` is unset. The connection's `search_path` is set
    to the fresh schema, so unqualified DDL (the migration files) lands there
    and is dropped wholesale at teardown.
    """
    if not _DATABASE_URL:
        pytest.skip("DATABASE_URL not set; skipping Postgres-backed test")

    schema = f"sf_test_{uuid.uuid4().hex}"
    conn: psycopg.Connection[TupleRow] = psycopg.connect(_DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            cur.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
        conn.commit()
        yield conn
    finally:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
        conn.commit()
        conn.close()


@pytest.fixture
def fake_slack_transport() -> httpx.MockTransport:
    return make_fake_slack_transport()


@pytest.fixture
def fake_slack_http(fake_slack_transport: httpx.MockTransport) -> Iterator[httpx.Client]:
    with httpx.Client(base_url="https://slack.com/api", transport=fake_slack_transport) as client:
        yield client
