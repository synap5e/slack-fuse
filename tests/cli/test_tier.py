from __future__ import annotations

import argparse
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import TupleRow

import slack_fuse.migrations as client_migrations
from slack_fuse.cli.tier import TierCommandError, TierName, TierUpdateResult, cmd_tier, set_channel_tier
from slack_fuse.migrations.runner import apply_migrations

_CLIENT_MIGRATIONS_DIR = Path(client_migrations.__file__).parent


def _dsn_with_search_path(database_url: str, schema: str) -> str:
    parsed = urlsplit(database_url)
    query_parts = parse_qsl(parsed.query, keep_blank_values=True)
    query_parts.append(("options", f"-csearch_path={schema}"))
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query_parts), parsed.fragment))


def _connect(database_url: str) -> psycopg.Connection[TupleRow]:
    conn: psycopg.Connection[TupleRow] = psycopg.connect(database_url)
    conn.autocommit = True
    return conn


@dataclass(frozen=True)
class _ChannelSeed:
    channel_id: str
    slug: str
    tier: TierName = "hot"
    tier_source: str = "auto"
    subscribed: bool = True


def _insert_channel(
    conn: psycopg.Connection[TupleRow],
    seed: _ChannelSeed,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO channels (channel_id, name, slug, tier, tier_source, subscribed) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (
                seed.channel_id,
                seed.channel_id.lower(),
                seed.slug,
                seed.tier,
                seed.tier_source,
                seed.subscribed,
            ),
        )


def _channel_row(conn: psycopg.Connection[TupleRow], channel_id: str) -> tuple[str, str, bool, str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT tier, tier_source, subscribed, xmin::text FROM channels WHERE channel_id = %s",
            (channel_id,),
        )
        row = cur.fetchone()
    assert row is not None
    return (str(row[0]), str(row[1]), bool(row[2]), str(row[3]))


@pytest.fixture
def client_database_url(database_url: str) -> Iterator[str]:
    schema = f"sf_cli_{uuid.uuid4().hex}"
    admin: psycopg.Connection[TupleRow] = psycopg.connect(database_url)
    with admin.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    admin.commit()

    scoped_database_url = _dsn_with_search_path(database_url, schema)
    setup: psycopg.Connection[TupleRow] = psycopg.connect(scoped_database_url)
    try:
        apply_migrations(setup, _CLIENT_MIGRATIONS_DIR)
        with setup.cursor() as cur:
            cur.execute("ALTER TABLE channels ADD COLUMN slug TEXT")
        setup.commit()
    finally:
        setup.close()

    try:
        yield scoped_database_url
    finally:
        with admin.cursor() as cur:
            cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
        admin.commit()
        admin.close()


def test_set_channel_tier_resolves_slug_and_sets_manual_hot(client_database_url: str) -> None:
    conn = _connect(client_database_url)
    try:
        _insert_channel(
            conn,
            _ChannelSeed(channel_id="C123", slug="general", tier="hidden", tier_source="auto", subscribed=True),
        )

        result = set_channel_tier(
            database_url=client_database_url,
            slug_or_channel_id="general",
            desired_tier="hot",
        )

        assert result == TierUpdateResult(channel_id="C123", tier="hot", changed=True)
        assert _channel_row(conn, "C123")[:3] == ("hot", "manual", True)
    finally:
        conn.close()


@pytest.mark.parametrize(("desired_tier", "expected_subscribed"), [("hidden", True), ("blocked", False)])
def test_set_channel_tier_accepts_direct_channel_id(
    client_database_url: str,
    desired_tier: TierName,
    expected_subscribed: bool,
) -> None:
    conn = _connect(client_database_url)
    try:
        _insert_channel(conn, _ChannelSeed(channel_id="C200", slug="room", tier="hot", tier_source="auto"))

        result = set_channel_tier(
            database_url=client_database_url,
            slug_or_channel_id="C200",
            desired_tier=desired_tier,
        )

        assert result.changed is True
        assert result.channel_id == "C200"
        assert result.tier == desired_tier
        assert _channel_row(conn, "C200")[:3] == (desired_tier, "manual", expected_subscribed)
    finally:
        conn.close()


def test_set_channel_tier_unknown_slug_raises_not_found(client_database_url: str) -> None:
    with pytest.raises(TierCommandError, match="unknown channel slug or id: missing-room") as exc_info:
        set_channel_tier(
            database_url=client_database_url,
            slug_or_channel_id="missing-room",
            desired_tier="hot",
        )

    assert exc_info.value.exit_code == 2


def test_set_channel_tier_noop_when_already_manual(client_database_url: str) -> None:
    conn = _connect(client_database_url)
    try:
        _insert_channel(conn, _ChannelSeed(channel_id="C300", slug="ops", tier="hot", tier_source="manual"))
        before = _channel_row(conn, "C300")[3]

        result = set_channel_tier(
            database_url=client_database_url,
            slug_or_channel_id="ops",
            desired_tier="hot",
        )

        after_row = _channel_row(conn, "C300")
        assert result == TierUpdateResult(channel_id="C300", tier="hot", changed=False)
        assert after_row[:3] == ("hot", "manual", True)
        assert after_row[3] == before
    finally:
        conn.close()


def test_cmd_tier_unknown_slug_exits_non_zero(
    client_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("SLACK_FUSE_DATABASE_URL", client_database_url)
    args = argparse.Namespace(slug_or_channel_id="nope", tier="hot")

    with pytest.raises(SystemExit) as exc_info:
        cmd_tier(args)

    assert exc_info.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "Error: unknown channel slug or id: nope" in captured.err
