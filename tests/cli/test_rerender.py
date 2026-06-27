"""`slack-fuse rerender` CLI tests.

Covers parser wiring and that the subcommand resolves a slug to a channel id
(via the same resolver `tier` uses) before handing it to `rerender_channel`.
HTTP is not exercised here — `rerender_channel` is patched to capture the
resolved id — so the test stays a pure CLI-wiring check against the real client
schema. Snapshot fetch + apply itself is covered in
`tests/projector/test_rerender.py`.
"""

from __future__ import annotations

import argparse
import uuid
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import TupleRow

import slack_fuse.cli.rerender as rerender_cli
import slack_fuse.migrations as client_migrations
from slack_fuse.__main__ import build_parser
from slack_fuse.migrations.runner import apply_migrations
from slack_fuse.projector.rerender import RerenderResult

_CLIENT_MIGRATIONS_DIR = Path(client_migrations.__file__).parent


@pytest.fixture
def client_database_url(database_url: str) -> Iterator[str]:
    schema = f"sf_rerender_cli_{uuid.uuid4().hex}"
    admin: psycopg.Connection[TupleRow] = psycopg.connect(database_url)
    with admin.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    admin.commit()

    scoped = f"{database_url}?options=-csearch_path%3D{schema}"
    setup: psycopg.Connection[TupleRow] = psycopg.connect(scoped)
    try:
        apply_migrations(setup, _CLIENT_MIGRATIONS_DIR)
        setup.commit()
    finally:
        setup.close()
    try:
        yield scoped
    finally:
        with admin.cursor() as cur:
            cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
        admin.commit()
        admin.close()


def _seed_channel(database_url: str, channel_id: str, name: str) -> None:
    conn: psycopg.Connection[TupleRow] = psycopg.connect(database_url)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO channels (channel_id, name, is_im, is_mpim, is_member, is_archived, "
                "  tier, tier_source, subscribed) "
                "VALUES (%s, %s, FALSE, FALSE, TRUE, FALSE, 'hot', 'auto', TRUE)",
                (channel_id, name),
            )
    finally:
        conn.close()


def test_rerender_subcommand_registered() -> None:
    parser = build_parser()
    args = parser.parse_args(["rerender", "proj-cloud"])
    assert args.command == "rerender"
    assert args.slug_or_channel_id == "proj-cloud"
    assert args.func is rerender_cli.cmd_rerender


def test_cmd_rerender_resolves_slug_to_channel_id(client_database_url: str, monkeypatch: pytest.MonkeyPatch) -> None:
    _seed_channel(client_database_url, "C0ALLT6Q3SQ", "proj-cloud")
    captured: dict[str, str] = {}

    def fake_config() -> SimpleNamespace:
        return SimpleNamespace(
            database_url=client_database_url,
            server_url="ws://localhost:8765",
            shared_secret="sek",
        )

    def fake_rerender(
        _http: object,
        base_http_url: str,
        _conn: object,
        channel_id: str,
        *,
        shared_secret: str | None = None,
    ) -> RerenderResult:
        captured["channel_id"] = channel_id
        captured["base_http_url"] = base_http_url
        captured["shared_secret"] = shared_secret or ""
        return RerenderResult(channel_id, status="rerendered", chunks=3, thread_chunks=1)

    monkeypatch.setattr("slack_fuse.config.load_client_config", fake_config)
    monkeypatch.setattr("slack_fuse.projector.rerender.rerender_channel", fake_rerender)

    rerender_cli.cmd_rerender(argparse.Namespace(slug_or_channel_id="proj-cloud"))

    # Slug resolved to the channel id; server origin derived ws -> http.
    assert captured["channel_id"] == "C0ALLT6Q3SQ"
    assert captured["base_http_url"] == "http://localhost:8765"
    assert captured["shared_secret"] == "sek"


def test_cmd_rerender_unknown_channel_exits_2(client_database_url: str, monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_config() -> SimpleNamespace:
        return SimpleNamespace(
            database_url=client_database_url,
            server_url="ws://localhost:8765",
            shared_secret="sek",
        )

    monkeypatch.setattr("slack_fuse.config.load_client_config", fake_config)

    with pytest.raises(SystemExit) as excinfo:
        rerender_cli.cmd_rerender(argparse.Namespace(slug_or_channel_id="does-not-exist"))
    assert excinfo.value.code == 2
