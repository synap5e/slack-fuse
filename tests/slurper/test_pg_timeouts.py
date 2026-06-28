# pyright: reportPrivateUsage=false
"""PostgreSQL runtime timeouts for the slurper writer connection."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import psycopg
import pytest
from psycopg.rows import TupleRow

import slack_fuse_server.slurper.__main__ as slurper_main
from slack_fuse_server.config import ServerConfig


def _show_setting(conn: psycopg.Connection[TupleRow], name: Literal["lock_timeout", "statement_timeout"]) -> str:
    with conn.cursor() as cur:
        if name == "lock_timeout":
            cur.execute("SHOW lock_timeout")
        else:
            cur.execute("SHOW statement_timeout")
        row = cur.fetchone()
    assert row is not None
    return str(row[0])


def test_connect_and_migrate_sets_runtime_timeouts_after_migrations(
    database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen_during_migration: list[str] = []

    def _fake_apply_migrations(conn: psycopg.Connection[TupleRow], _migrations_dir: Path) -> list[str]:
        seen_during_migration.append(_show_setting(conn, "lock_timeout"))
        return []

    monkeypatch.setattr(slurper_main, "apply_migrations", _fake_apply_migrations)
    config = ServerConfig(
        slack_user_token="xoxp-test",
        slack_app_token="xapp-test",
        shared_secret="secret",
        database_url=database_url,
        slurper_lock_timeout_s=0.2,
        slurper_statement_timeout_s=0.35,
    )

    conn = slurper_main._connect_and_migrate(config)
    try:
        assert seen_during_migration == ["0"]
        assert _show_setting(conn, "lock_timeout") == "200ms"
        assert _show_setting(conn, "statement_timeout") == "350ms"
    finally:
        conn.close()
