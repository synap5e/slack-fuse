"""Tests for permalink parsing and v2 projection-backed path resolution."""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import pytest

from slack_fuse.__main__ import build_parser
from slack_fuse.fuse_v2_helpers import derive_thread_slug
from slack_fuse.resolve import PermalinkResolutionError, parse_permalink, resolve_permalink

# Re-export the migrated-client fixtures so pytest makes them available to this
# top-level test module (their defining conftest lives under tests/projector/).
from tests.projector.conftest import client_conn as _client_conn, client_conn_factory as _client_conn_factory

client_conn = _client_conn
client_conn_factory = _client_conn_factory

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow


_LOCAL_TZ = ZoneInfo("Pacific/Auckland")
_MOUNTPOINT = "/mnt/slack"


@pytest.fixture(autouse=True)
def pin_local_timezone(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Make resolver dates match a deterministic non-UTC mount timezone."""
    original = os.environ.get("TZ")
    monkeypatch.setenv("TZ", _LOCAL_TZ.key)
    time.tzset()
    yield
    if original is None:
        monkeypatch.delenv("TZ", raising=False)
    else:
        monkeypatch.setenv("TZ", original)
    time.tzset()


def _local_ts(year: int, month: int, day: int, hour: int, minute: int = 0) -> Decimal:
    value = datetime(year, month, day, hour, minute, tzinfo=_LOCAL_TZ).timestamp()
    return Decimal(str(value)).quantize(Decimal("0.000001"))


def _permalink(channel_id: str, message_ts: Decimal | None = None, *, thread_ts: Decimal | None = None) -> str:
    url = f"https://workspace.slack.com/archives/{channel_id}"
    if message_ts is not None:
        url = f"{url}/p{format(message_ts, '.6f').replace('.', '')}"
    if thread_ts is not None:
        url = f"{url}?thread_ts={thread_ts:.6f}&cid={channel_id}"
    return url


def _seed_channel(
    conn: Connection[TupleRow],
    channel_id: str = "C1",
    name: str = "general",
    *,
    im_user_id: str | None = None,
) -> None:
    is_im = im_user_id is not None
    with conn.cursor() as cur:
        _ = cur.execute(
            "INSERT INTO channels "
            "(channel_id, name, is_im, is_mpim, is_member, is_archived, im_user_id, tier) "
            "VALUES (%s, %s, %s, %s, %s, FALSE, %s, 'hot')",
            (channel_id, name, is_im, False, True, im_user_id),
        )


def _seed_chunk(
    conn: Connection[TupleRow],
    message_ts: Decimal,
    content_md: str,
    *,
    channel_id: str = "C1",
    reply_count: int = 0,
) -> None:
    with conn.cursor() as cur:
        _ = cur.execute(
            "INSERT INTO chunks (channel_id, message_ts, content_md, reply_count) VALUES (%s, %s, %s, %s)",
            (channel_id, message_ts, content_md, reply_count),
        )


def _seed_user(conn: Connection[TupleRow], user_id: str, display_name: str) -> None:
    with conn.cursor() as cur:
        _ = cur.execute(
            "INSERT INTO users (user_id, display_name) VALUES (%s, %s)",
            (user_id, display_name),
        )


class TestParsePermalink:
    def test_channel_message(self) -> None:
        cid, ts, thread_ts = parse_permalink(
            "https://comfy-organization.slack.com/archives/C09LDUKDQ1K/p1775493247936389"
        )
        assert cid == "C09LDUKDQ1K"
        assert ts == "1775493247.936389"
        assert thread_ts is None

    def test_thread_reply(self) -> None:
        cid, ts, thread_ts = parse_permalink(
            "https://comfy-organization.slack.com/archives/C09LDUKDQ1K/p1775493247936389"
            "?thread_ts=1775490000.000000&cid=C09LDUKDQ1K"
        )
        assert cid == "C09LDUKDQ1K"
        assert ts == "1775493247.936389"
        assert thread_ts == "1775490000.000000"

    def test_short_microsecond_part(self) -> None:
        cid, ts, _ = parse_permalink("https://workspace.slack.com/archives/C123ABC/p1700000000000100")
        assert cid == "C123ABC"
        assert ts == "1700000000.000100"

    def test_channel_only_url(self) -> None:
        cid, ts, thread_ts = parse_permalink("https://comfy-organization.slack.com/archives/C0AMT1A1YBV")
        assert cid == "C0AMT1A1YBV"
        assert ts is None
        assert thread_ts is None

    def test_channel_only_url_trailing_slash(self) -> None:
        cid, ts, thread_ts = parse_permalink("https://workspace.slack.com/archives/C123/")
        assert cid == "C123"
        assert ts is None
        assert thread_ts is None

    def test_rejects_non_archives_path(self) -> None:
        with pytest.raises(ValueError, match="Not a Slack archives URL"):
            parse_permalink("https://workspace.slack.com/messages/C123")

    def test_rejects_missing_p_prefix(self) -> None:
        with pytest.raises(ValueError, match="Not a Slack message permalink"):
            parse_permalink("https://workspace.slack.com/archives/C123/1234567890123456")

    def test_rejects_non_numeric_timestamp(self) -> None:
        with pytest.raises(ValueError, match="Invalid timestamp"):
            parse_permalink("https://workspace.slack.com/archives/C123/pabcdefghijk")

    def test_rejects_short_timestamp(self) -> None:
        with pytest.raises(ValueError, match="Invalid timestamp"):
            parse_permalink("https://workspace.slack.com/archives/C123/p12345")


def test_resolve_cli_mountpoint_defaults_to_client_config() -> None:
    args = build_parser().parse_args(["resolve", _permalink("C1")])
    assert args.mountpoint is None


def test_resolve_cli_explicit_mountpoint_is_preserved() -> None:
    args = build_parser().parse_args(["resolve", _permalink("C1"), "--mountpoint", "/custom/mnt"])
    assert args.mountpoint == "/custom/mnt"


def test_channel_only_url_returns_channel_directory(client_conn: Connection[TupleRow]) -> None:
    _seed_channel(client_conn)

    path = resolve_permalink(_permalink("C1"), _MOUNTPOINT, client_conn)

    assert path == "/mnt/slack/channels/general"


def test_non_thread_message_returns_day_channel_file(client_conn: Connection[TupleRow]) -> None:
    _seed_channel(client_conn)
    message_ts = _local_ts(2026, 7, 31, 10)
    _seed_chunk(client_conn, message_ts, "## 10:00 <@U1>\n\nA plain message\n")

    path = resolve_permalink(_permalink("C1", message_ts), _MOUNTPOINT, client_conn)

    assert path == "/mnt/slack/channels/general/2026-07/31/channel.md"


def test_thread_parent_message_returns_derived_thread_file(client_conn: Connection[TupleRow]) -> None:
    _seed_channel(client_conn)
    parent_ts = _local_ts(2026, 7, 31, 10)
    content_md = "## 10:00 <@U1>\n\nThread topic\n\n> Thread: 1 replies\n"
    _seed_chunk(client_conn, parent_ts, content_md, reply_count=1)
    expected_slug = derive_thread_slug(content_md, parent_ts)

    path = resolve_permalink(_permalink("C1", parent_ts), _MOUNTPOINT, client_conn)

    assert path == f"/mnt/slack/channels/general/2026-07/31/{expected_slug}/thread.md"


def test_thread_reply_uses_parent_date_and_slug(client_conn: Connection[TupleRow]) -> None:
    _seed_channel(client_conn)
    parent_ts = _local_ts(2026, 7, 31, 23)
    reply_ts = _local_ts(2026, 8, 2, 9)
    _seed_chunk(
        client_conn,
        parent_ts,
        "## 23:00 <@U1>\n\nBug under discussion\n\n> Thread: 2 replies\n",
        reply_count=2,
    )

    path = resolve_permalink(_permalink("C1", reply_ts, thread_ts=parent_ts), _MOUNTPOINT, client_conn)

    assert path == "/mnt/slack/channels/general/2026-07/31/bug-under-discussion/thread.md"


def test_thread_reply_raises_when_parent_is_not_projected(client_conn: Connection[TupleRow]) -> None:
    _seed_channel(client_conn)
    parent_ts = _local_ts(2026, 7, 31, 23)
    reply_ts = _local_ts(2026, 8, 2, 9)

    with pytest.raises(PermalinkResolutionError, match=str(parent_ts)):
        _ = resolve_permalink(_permalink("C1", reply_ts, thread_ts=parent_ts), _MOUNTPOINT, client_conn)


def test_duplicate_thread_slug_gets_stable_numeric_suffix(client_conn: Connection[TupleRow]) -> None:
    _seed_channel(client_conn)
    first_ts = _local_ts(2026, 7, 31, 9)
    second_ts = _local_ts(2026, 7, 31, 10)
    content_md = "## 09:00 <@U1>\n\nDeploy update\n\n> Thread: 1 replies\n"
    _seed_chunk(client_conn, first_ts, content_md, reply_count=1)
    _seed_chunk(client_conn, second_ts, content_md, reply_count=1)

    path = resolve_permalink(_permalink("C1", second_ts), _MOUNTPOINT, client_conn)

    assert path == "/mnt/slack/channels/general/2026-07/31/deploy-update-2/thread.md"


def test_thread_slug_resolves_user_mention_from_users_table(client_conn: Connection[TupleRow]) -> None:
    _seed_channel(client_conn)
    _seed_user(client_conn, "UCLAUDE", "claude")
    parent_ts = _local_ts(2026, 7, 31, 10)
    content_md = "## 10:00 <@UCLAUDE>\n\n<@UCLAUDE> ptal at the migration\n\n> Thread: 1 replies\n"
    _seed_chunk(client_conn, parent_ts, content_md, reply_count=1)

    path = resolve_permalink(_permalink("C1", parent_ts), _MOUNTPOINT, client_conn)

    assert path == "/mnt/slack/channels/general/2026-07/31/claude-ptal-at-the-migration/thread.md"
    assert "uclaude" not in path


def test_dm_channel_uses_partner_display_name_slug(client_conn: Connection[TupleRow]) -> None:
    _seed_user(client_conn, "UALICE", "Alice Smith")
    _seed_channel(
        client_conn,
        "D1",
        "",
        im_user_id="UALICE",
    )

    path = resolve_permalink(_permalink("D1"), _MOUNTPOINT, client_conn)

    assert path == "/mnt/slack/dms/alice-smith"


def test_unknown_channel_raises_resolution_error(client_conn: Connection[TupleRow]) -> None:
    with pytest.raises(PermalinkResolutionError, match="CUNKNOWN"):
        _ = resolve_permalink(_permalink("CUNKNOWN"), _MOUNTPOINT, client_conn)
