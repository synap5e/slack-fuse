"""`slack-fuse tier` CLI tests.

Review P1-G: slug resolution must use the SAME assignment as FUSE V2
(`assign_conv_root_slugs` / `fetch_channel_by_slug`) against the REAL production
schema. The earlier suite added a synthetic `channels.slug` column that
production does not have, masking the fact that the CLI's resolution diverged
from the filesystem's. These tests run against the unmodified client migration
schema and assert parity with FUSE V2's slug logic (including DM display-name
slugs and cross-conv-root collisions).
"""

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
from slack_fuse.fuse_v2_helpers import fetch_channel_by_slug
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
    name: str = ""
    tier: TierName = "hot"
    tier_source: str = "auto"
    subscribed: bool = True
    is_im: bool = False
    is_mpim: bool = False
    is_member: bool = True
    is_archived: bool = False
    im_user_id: str | None = None


def _insert_channel(conn: psycopg.Connection[TupleRow], seed: _ChannelSeed) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO channels (channel_id, name, is_im, is_mpim, is_member, is_archived, im_user_id, "
            "  tier, tier_source, subscribed) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                seed.channel_id,
                seed.name,
                seed.is_im,
                seed.is_mpim,
                seed.is_member,
                seed.is_archived,
                seed.im_user_id,
                seed.tier,
                seed.tier_source,
                seed.subscribed,
            ),
        )


def _insert_user(conn: psycopg.Connection[TupleRow], user_id: str, display_name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO users (user_id, display_name) VALUES (%s, %s)",
            (user_id, display_name),
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
        # Real production schema only — NO synthetic `channels.slug` column.
        apply_migrations(setup, _CLIENT_MIGRATIONS_DIR)
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
            _ChannelSeed(channel_id="C123", name="general", tier="hidden", tier_source="auto"),
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


def test_slug_resolves_to_same_channel_as_fuse_v2(client_database_url: str) -> None:
    """Review P1-G: a bare slug resolves to exactly the channel FUSE V2 lists at
    `/channels/<slug>`. Two same-named channels get hot-first suffixing; the CLI
    must agree with `fetch_channel_by_slug` on which id owns which slug."""
    conn = _connect(client_database_url)
    try:
        _insert_channel(conn, _ChannelSeed(channel_id="CHOT", name="dup", tier="hot"))
        _insert_channel(conn, _ChannelSeed(channel_id="CHID", name="dup", tier="hidden"))

        # FUSE V2: hot wins the unsuffixed slug, hidden gets `dup-2`.
        hot_row = fetch_channel_by_slug(conn, "channels", "dup", allow_hidden=True)
        hidden_row = fetch_channel_by_slug(conn, "channels", "dup-2", allow_hidden=True)
        assert hot_row is not None and hot_row.channel_id == "CHOT"
        assert hidden_row is not None and hidden_row.channel_id == "CHID"

        # The CLI resolves the same slugs to the same ids. Resolve to each
        # channel's current tier so the assignment doesn't shift between calls.
        hot_result = set_channel_tier(database_url=client_database_url, slug_or_channel_id="dup", desired_tier="hot")
        hidden_result = set_channel_tier(
            database_url=client_database_url, slug_or_channel_id="dup-2", desired_tier="hidden"
        )
        assert hot_result.channel_id == "CHOT"
        assert hidden_result.channel_id == "CHID"
    finally:
        conn.close()


def test_set_channel_tier_resolves_dm_slug_by_display_name(client_database_url: str) -> None:
    """Review P1-G: DM slugs derive from the partner's display name (via the
    local `users` table), exactly as FUSE V2 does — not from the channel name."""
    conn = _connect(client_database_url)
    try:
        _insert_user(conn, "U999", "Alice Smith")
        _insert_channel(
            conn,
            _ChannelSeed(channel_id="D1", name="", is_im=True, is_member=False, im_user_id="U999", tier="hot"),
        )

        # FUSE V2 would list this DM at /dms/alice-smith.
        fuse_row = fetch_channel_by_slug(conn, "dms", "alice-smith", allow_hidden=True)
        assert fuse_row is not None and fuse_row.channel_id == "D1"

        result = set_channel_tier(
            database_url=client_database_url,
            slug_or_channel_id="alice-smith",
            desired_tier="hidden",
        )
        assert result == TierUpdateResult(channel_id="D1", tier="hidden", changed=True)
    finally:
        conn.close()


def test_set_channel_tier_qualified_conv_root_slug(client_database_url: str) -> None:
    """A `<conv-root>/<slug>` target resolves within that conv root only."""
    conn = _connect(client_database_url)
    try:
        _insert_channel(conn, _ChannelSeed(channel_id="C1", name="ops", is_member=True, tier="hot"))
        _insert_channel(conn, _ChannelSeed(channel_id="C2", name="ops", is_member=False, tier="hot"))

        result = set_channel_tier(
            database_url=client_database_url,
            slug_or_channel_id="other-channels/ops",
            desired_tier="blocked",
        )
        assert result.channel_id == "C2"
    finally:
        conn.close()


def test_set_channel_tier_ambiguous_bare_slug_raises(client_database_url: str) -> None:
    """A bare slug colliding across conv roots is ambiguous → error (exit 2),
    so the operator qualifies it rather than re-tiering the wrong channel."""
    conn = _connect(client_database_url)
    try:
        _insert_channel(conn, _ChannelSeed(channel_id="C1", name="ops", is_member=True, tier="hot"))
        _insert_channel(conn, _ChannelSeed(channel_id="C2", name="ops", is_member=False, tier="hot"))

        with pytest.raises(TierCommandError, match="ambiguous") as exc_info:
            set_channel_tier(database_url=client_database_url, slug_or_channel_id="ops", desired_tier="blocked")
        assert exc_info.value.exit_code == 2
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
        _insert_channel(conn, _ChannelSeed(channel_id="C200", name="room", tier="hot", tier_source="auto"))

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
        _insert_channel(conn, _ChannelSeed(channel_id="C300", name="ops", tier="hot", tier_source="manual"))
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
