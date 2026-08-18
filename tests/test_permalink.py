"""Tests for FUSE-path → Slack-permalink resolution (v2 projections store)."""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import pytest

from slack_fuse.fuse_v2_helpers import derive_thread_slug
from slack_fuse.permalink import PermalinkGenerationError, resolve_path_to_permalink

# Re-export the migrated-client fixtures so pytest picks them up here (they
# live under tests/projector/conftest.py for the projector suite).
from tests.projector.conftest import (
    client_conn as _client_conn,
    client_conn_factory as _client_conn_factory,
)

client_conn = _client_conn
client_conn_factory = _client_conn_factory

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow


_LOCAL_TZ = ZoneInfo("Pacific/Auckland")
_MOUNTPOINT = "/mnt/slack"
_WORKSPACE = "https://workspace.slack.com"


@pytest.fixture(autouse=True)
def pin_local_timezone(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Pin the resolver's date arithmetic to a deterministic non-UTC zone."""
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


def _seed_channel(
    conn: Connection[TupleRow],
    channel_id: str = "C1",
    name: str = "general",
    *,
    im_user_id: str | None = None,
    tier: str = "hot",
) -> None:
    is_im = im_user_id is not None
    with conn.cursor() as cur:
        _ = cur.execute(
            "INSERT INTO channels "
            "(channel_id, name, is_im, is_mpim, is_member, is_archived, im_user_id, tier) "
            "VALUES (%s, %s, %s, %s, %s, FALSE, %s, %s)",
            (channel_id, name, is_im, False, True, im_user_id, tier),
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


def _write_frontmatter(tmp_path: Path, relative: str, keys: dict[str, str]) -> Path:
    """Materialise a fake rendered file with just the frontmatter we care about."""
    file = tmp_path / relative
    file.parent.mkdir(parents=True, exist_ok=True)
    lines = ["---"]
    lines.extend(f"{k}: {v}" for k, v in keys.items())
    lines.append("---")
    lines.append("")  # empty body — the resolver only reads the header
    file.write_text("\n".join(lines))
    return file


# ---------------------------------------------------------------------------
# URL synthesis (no PG)
# ---------------------------------------------------------------------------


def test_channel_dir_returns_archives_url(client_conn: Connection[TupleRow], tmp_path: Path) -> None:
    _seed_channel(client_conn)
    _write_frontmatter(tmp_path, "channels/general/channel.md", {"channel_id": "C1"})

    url = resolve_path_to_permalink(
        f"{tmp_path}/channels/general",
        str(tmp_path),
        client_conn,
        _WORKSPACE,
    )

    assert url == f"{_WORKSPACE}/archives/C1"


def test_channel_file_returns_archives_url(client_conn: Connection[TupleRow], tmp_path: Path) -> None:
    _seed_channel(client_conn)
    _write_frontmatter(tmp_path, "channels/general/channel.md", {"channel_id": "C1"})

    url = resolve_path_to_permalink(
        f"{tmp_path}/channels/general/channel.md",
        str(tmp_path),
        client_conn,
        _WORKSPACE,
    )

    assert url == f"{_WORKSPACE}/archives/C1"


def test_day_file_requires_ts(client_conn: Connection[TupleRow], tmp_path: Path) -> None:
    _seed_channel(client_conn)
    _write_frontmatter(tmp_path, "channels/general/2026-07/31/channel.md", {"channel_id": "C1"})

    with pytest.raises(ValueError, match="day file is ambiguous"):
        _ = resolve_path_to_permalink(
            f"{tmp_path}/channels/general/2026-07/31/channel.md",
            str(tmp_path),
            client_conn,
            _WORKSPACE,
        )


def test_day_file_with_ts_returns_message_permalink(client_conn: Connection[TupleRow], tmp_path: Path) -> None:
    _seed_channel(client_conn)
    _write_frontmatter(tmp_path, "channels/general/2026-07/31/channel.md", {"channel_id": "C1"})

    url = resolve_path_to_permalink(
        f"{tmp_path}/channels/general/2026-07/31/channel.md",
        str(tmp_path),
        client_conn,
        _WORKSPACE,
        ts="1785661200.000000",
    )

    assert url == f"{_WORKSPACE}/archives/C1/p1785661200000000"


def test_thread_file_uses_frontmatter_thread_ts(client_conn: Connection[TupleRow], tmp_path: Path) -> None:
    _seed_channel(client_conn)
    _write_frontmatter(
        tmp_path,
        "channels/general/2026-07/31/topic/thread.md",
        {"channel_id": "C1", "thread_ts": "1785661200.000000"},
    )

    url = resolve_path_to_permalink(
        f"{tmp_path}/channels/general/2026-07/31/topic/thread.md",
        str(tmp_path),
        client_conn,
        _WORKSPACE,
    )

    assert url == f"{_WORKSPACE}/archives/C1/p1785661200000000"


def test_thread_reply_appends_thread_ts_query(client_conn: Connection[TupleRow], tmp_path: Path) -> None:
    _seed_channel(client_conn)
    _write_frontmatter(
        tmp_path,
        "channels/general/2026-07/31/topic/thread.md",
        {"channel_id": "C1", "thread_ts": "1785660000.000000"},
    )

    url = resolve_path_to_permalink(
        f"{tmp_path}/channels/general/2026-07/31/topic/thread.md",
        str(tmp_path),
        client_conn,
        _WORKSPACE,
        ts="1785661200.000000",
    )

    assert url == f"{_WORKSPACE}/archives/C1/p1785661200000000?thread_ts=1785660000.000000&cid=C1"


def test_workspace_url_trailing_slash_stripped(client_conn: Connection[TupleRow], tmp_path: Path) -> None:
    _seed_channel(client_conn)
    _write_frontmatter(tmp_path, "channels/general/channel.md", {"channel_id": "C1"})

    url = resolve_path_to_permalink(
        f"{tmp_path}/channels/general",
        str(tmp_path),
        client_conn,
        f"{_WORKSPACE}/",
    )

    assert url == f"{_WORKSPACE}/archives/C1"


# ---------------------------------------------------------------------------
# Slug → channel_id fallback (frontmatter missing or outdated)
# ---------------------------------------------------------------------------


def test_channel_id_falls_back_to_local_projection_when_frontmatter_missing(
    client_conn: Connection[TupleRow], tmp_path: Path
) -> None:
    _seed_channel(client_conn)
    # No frontmatter file on disk — resolver falls back to slug lookup.

    url = resolve_path_to_permalink(
        f"{tmp_path}/channels/general",
        str(tmp_path),
        client_conn,
        _WORKSPACE,
    )

    assert url == f"{_WORKSPACE}/archives/C1"


def test_unknown_channel_slug_raises_generation_error(client_conn: Connection[TupleRow], tmp_path: Path) -> None:
    with pytest.raises(PermalinkGenerationError, match="ghost-channel"):
        _ = resolve_path_to_permalink(
            f"{tmp_path}/channels/ghost-channel",
            str(tmp_path),
            client_conn,
            _WORKSPACE,
        )


def test_slug_belonging_to_wrong_root_raises(client_conn: Connection[TupleRow], tmp_path: Path) -> None:
    _seed_user(client_conn, "UALICE", "Alice")
    _seed_channel(client_conn, "D1", "", im_user_id="UALICE")
    # DM belongs under `dms/`; asking for it under `channels/` must be rejected
    # (fetch_channel_by_slug scopes by conv-root, so this surfaces as
    # "could not resolve" rather than a wrong-root diagnostic).

    with pytest.raises(PermalinkGenerationError, match="could not resolve channel slug 'alice'"):
        _ = resolve_path_to_permalink(
            f"{tmp_path}/channels/alice",
            str(tmp_path),
            client_conn,
            _WORKSPACE,
        )


# ---------------------------------------------------------------------------
# Thread slug → thread_ts reversal (v2 projection lookup)
# ---------------------------------------------------------------------------


def test_thread_slug_reverses_via_local_projection(client_conn: Connection[TupleRow], tmp_path: Path) -> None:
    _seed_channel(client_conn)
    parent_ts = _local_ts(2026, 7, 31, 10)
    content_md = "## 10:00 <@U1>\n\nDeploy discussion\n\n> Thread: 1 replies\n"
    _seed_chunk(client_conn, parent_ts, content_md, reply_count=1)
    expected_slug = derive_thread_slug(content_md, parent_ts)
    # No frontmatter on disk — resolver reverses slug via fetch_day_thread_parents.

    url = resolve_path_to_permalink(
        f"{tmp_path}/channels/general/2026-07/31/{expected_slug}/thread.md",
        str(tmp_path),
        client_conn,
        _WORKSPACE,
    )

    assert url == f"{_WORKSPACE}/archives/C1/p{format(parent_ts, '.6f').replace('.', '')}"


def test_thread_slug_reversal_fails_when_parent_missing(client_conn: Connection[TupleRow], tmp_path: Path) -> None:
    _seed_channel(client_conn)

    with pytest.raises(PermalinkGenerationError, match="could not resolve thread slug"):
        _ = resolve_path_to_permalink(
            f"{tmp_path}/channels/general/2026-07/31/deleted-thread/thread.md",
            str(tmp_path),
            client_conn,
            _WORKSPACE,
        )


def test_dm_channel_slug_resolves_to_dm_archive(client_conn: Connection[TupleRow], tmp_path: Path) -> None:
    _seed_user(client_conn, "UALICE", "Alice Smith")
    _seed_channel(client_conn, "D1", "", im_user_id="UALICE")

    url = resolve_path_to_permalink(
        f"{tmp_path}/dms/alice-smith",
        str(tmp_path),
        client_conn,
        _WORKSPACE,
    )

    assert url == f"{_WORKSPACE}/archives/D1"


# ---------------------------------------------------------------------------
# Path parsing edge cases
# ---------------------------------------------------------------------------


def test_rejects_path_outside_mountpoint(client_conn: Connection[TupleRow], tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="not under mountpoint"):
        _ = resolve_path_to_permalink("/not/under/mount", str(tmp_path), client_conn, _WORKSPACE)


def test_rejects_unknown_root(client_conn: Connection[TupleRow], tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="expected one of"):
        _ = resolve_path_to_permalink(f"{tmp_path}/nonsense/foo", str(tmp_path), client_conn, _WORKSPACE)


def test_rejects_bare_date_directory(client_conn: Connection[TupleRow], tmp_path: Path) -> None:
    _seed_channel(client_conn)

    with pytest.raises(ValueError, match="parsed date directory"):
        _ = resolve_path_to_permalink(
            f"{tmp_path}/channels/general/2026-07/31",
            str(tmp_path),
            client_conn,
            _WORKSPACE,
        )


def test_rejects_invalid_date_components(client_conn: Connection[TupleRow], tmp_path: Path) -> None:
    _seed_channel(client_conn)

    with pytest.raises(ValueError, match="expected valid"):
        _ = resolve_path_to_permalink(
            f"{tmp_path}/channels/general/13-99/xx/channel.md",
            str(tmp_path),
            client_conn,
            _WORKSPACE,
        )
