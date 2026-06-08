"""Shared pytest fixtures for the slack-fuse test suite.

- `pg_conn`: a Postgres connection scoped to a fresh, uniquely-named schema per
  test (pg_temp-style isolation), torn down with `DROP SCHEMA ... CASCADE`. If
  `DATABASE_URL` is not set, the suite auto-provisions a temporary local
  Postgres for the session and only skips if startup is unavailable/fails.
- `fake_slack_transport` / `fake_slack_http`: the fixture-backed fake Slack Web
  API (see `tests/_fake_slack`).
"""

from __future__ import annotations

import contextlib
import os
import shutil
import socket
import subprocess
import tempfile
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path

import httpx
import psycopg
import pytest
from psycopg import sql
from psycopg.rows import TupleRow

import slack_fuse_server.migrations as server_migrations
from slack_fuse.migrations.runner import apply_migrations
from tests._fake_slack import make_fake_slack_transport

_SERVER_MIGRATIONS_DIR = Path(server_migrations.__file__).parent
_DISABLE_AUTO_POSTGRES_ENV = "SLACK_FUSE_TEST_DISABLE_AUTO_POSTGRES"

# A factory that opens fresh connections bound to one migrated server schema.
ServerConnFactory = Callable[[], psycopg.Connection[TupleRow]]


class _EphemeralPostgresUnavailable(RuntimeError):
    """Raised when a temporary Postgres cannot be started for the test session."""


def _pick_unused_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    return int(port)


def _run_checked(command: list[str], *, action: str, capture_output: bool = True) -> None:
    try:
        if capture_output:
            subprocess.run(command, check=True, capture_output=True, text=True)
        else:
            subprocess.run(command, check=True, text=True)
    except (OSError, subprocess.CalledProcessError) as exc:
        if isinstance(exc, subprocess.CalledProcessError):
            stderr = exc.stderr.strip() if isinstance(exc.stderr, str) else ""
            stdout = exc.stdout.strip() if isinstance(exc.stdout, str) else ""
            detail = stderr or stdout or str(exc)
        else:
            detail = str(exc)
        raise _EphemeralPostgresUnavailable(f"{action}: {detail}") from exc


@contextlib.contextmanager
def _temporary_postgres_dsn() -> Iterator[str]:
    initdb = shutil.which("initdb")
    pg_ctl = shutil.which("pg_ctl")
    if initdb is None or pg_ctl is None:
        raise _EphemeralPostgresUnavailable("initdb/pg_ctl not found on PATH")

    with tempfile.TemporaryDirectory(prefix="slack-fuse-pg-") as data_dir:
        _run_checked(
            [initdb, "-D", data_dir, "-A", "trust", "-U", "postgres", "--no-instructions"],
            action="initdb failed",
        )
        socket_dir = Path(data_dir) / "socket"
        socket_dir.mkdir(parents=True, exist_ok=True)
        log_file = Path(data_dir) / "postgres.log"
        port = _pick_unused_tcp_port()
        _run_checked(
            [
                pg_ctl,
                "-D",
                data_dir,
                "-l",
                str(log_file),
                "-w",
                "-o",
                f"-h 127.0.0.1 -p {port} -k {socket_dir}",
                "start",
            ],
            action="pg_ctl start failed",
            capture_output=False,
        )
        try:
            yield f"postgresql://postgres@127.0.0.1:{port}/postgres"
        finally:
            subprocess.run(
                [pg_ctl, "-D", data_dir, "-w", "-m", "fast", "stop"],
                check=False,
                capture_output=True,
                text=True,
            )


@pytest.fixture(scope="session")
def database_url() -> Iterator[str]:
    """Resolve a DSN for DB-backed tests.

    Resolution order:
    1. `DATABASE_URL` (explicit user/CI database)
    2. Auto-provisioned temporary local postgres (`initdb` + `pg_ctl`)
    3. Skip DB-backed tests when temporary postgres is unavailable/fails
    """
    configured = os.environ.get("DATABASE_URL")
    if configured:
        yield configured
        return

    if os.environ.get(_DISABLE_AUTO_POSTGRES_ENV) == "1":
        pytest.skip(
            "DATABASE_URL not set and temporary Postgres auto-provision disabled "
            f"({_DISABLE_AUTO_POSTGRES_ENV}=1)"
        )
    try:
        with _temporary_postgres_dsn() as dsn:
            yield dsn
    except _EphemeralPostgresUnavailable as exc:
        pytest.skip(f"DATABASE_URL not set and temporary Postgres unavailable: {exc}")


@pytest.fixture
def pg_conn(database_url: str) -> Iterator[psycopg.Connection[TupleRow]]:
    """A Postgres connection isolated to a per-test schema.

    The connection's `search_path` is set to a fresh schema, so unqualified DDL
    (the migration files) lands there and is dropped wholesale at teardown.
    """
    schema = f"sf_test_{uuid.uuid4().hex}"
    conn: psycopg.Connection[TupleRow] = psycopg.connect(database_url)
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


@pytest.fixture
def server_conn_factory(database_url: str) -> Iterator[ServerConnFactory]:
    """Yield a factory producing connections bound to one migrated server schema.

    Unlike `pg_conn`, this applies the server migrations and lets a test open
    *several* connections into the same schema — needed by the concurrent-writer
    offset test, which must exercise the real `stream_heads` row lock across
    distinct backends. Every opened connection is closed at teardown, then the
    schema is dropped.
    """
    dsn = database_url  # narrowed local so the nested factory sees a `str`

    schema = f"sf_server_{uuid.uuid4().hex}"
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
        # Autocommit mirrors production (see OffsetWriter): each transaction()
        # is a real BEGIN/COMMIT regardless of any bare read that ran first.
        conn.autocommit = True
        opened.append(conn)
        return conn

    setup = make()
    apply_migrations(setup, _SERVER_MIGRATIONS_DIR)
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
def server_conn(server_conn_factory: ServerConnFactory) -> psycopg.Connection[TupleRow]:
    """A single connection in a fresh, migrated server schema."""
    return server_conn_factory()
