"""Simple forward-only SQL migration runner.

Shared by the client and the server — each passes its own migrations
directory. Migration files are named `NNNN_<name>.sql` and applied in
ascending numeric order. Applied migrations are recorded in a
`_migrations_applied` table (in the connection's current schema), so re-runs
are idempotent. No down-migrations.

Each migration runs in its own transaction together with its bookkeeping
INSERT, so a failure leaves earlier migrations applied and recorded.
"""

from __future__ import annotations

import re
from pathlib import Path

import psycopg
from psycopg.rows import TupleRow

_MIGRATION_NAME_RE = re.compile(r"^(\d+)_.+\.sql$")

_ENSURE_TABLE = """
CREATE TABLE IF NOT EXISTS _migrations_applied (
    name TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


def discover_migrations(migrations_dir: Path) -> list[tuple[int, str, Path]]:
    """Return `(number, name, path)` for every `NNNN_<name>.sql` file, sorted
    by the numeric prefix."""
    found: list[tuple[int, str, Path]] = []
    for path in migrations_dir.iterdir():
        match = _MIGRATION_NAME_RE.match(path.name)
        if match is not None:
            found.append((int(match.group(1)), path.name, path))
    found.sort(key=lambda entry: entry[0])
    return found


def apply_migrations(conn: psycopg.Connection[TupleRow], migrations_dir: Path) -> list[str]:
    """Apply all unapplied migrations in `migrations_dir` to `conn`.

    Returns the names of the migrations applied by this call (empty if the
    database was already up to date). Idempotent: a second call with the same
    inputs applies nothing.
    """
    migrations = discover_migrations(migrations_dir)

    with conn.transaction(), conn.cursor() as cur:
        cur.execute(_ENSURE_TABLE)
        cur.execute("SELECT name FROM _migrations_applied")
        already_applied = {str(row[0]) for row in cur.fetchall()}

    applied: list[str] = []
    for _number, name, path in migrations:
        if name in already_applied:
            continue
        sql = path.read_text()
        with conn.transaction(), conn.cursor() as cur:
            # Trusted whole-file DDL, not a parameterised query; psycopg accepts
            # a runtime str at execution, but its stub requires a LiteralString.
            cur.execute(sql)  # pyright: ignore[reportArgumentType, reportCallIssue]
            cur.execute("INSERT INTO _migrations_applied (name) VALUES (%s)", (name,))
        applied.append(name)

    return applied
