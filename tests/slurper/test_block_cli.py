# pyright: reportPrivateUsage=false
"""Server CLI-facing block/backfill behavior."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import trio
from psycopg.conninfo import make_conninfo

from slack_fuse_server.blocked_channels import BlockedChannelError, block_channel
from slack_fuse_server.config import ServerConfig
from slack_fuse_server.slurper.__main__ import _run_backfill

if TYPE_CHECKING:
    import psycopg
    from psycopg.rows import TupleRow

    from tests.conftest import ServerConnFactory


def _database_url_for_schema(conn: psycopg.Connection[TupleRow]) -> str:
    with conn.cursor() as cur:
        cur.execute("SELECT current_schema()")
        row = cur.fetchone()
    assert row is not None
    return make_conninfo(conn.info.dsn, options=f"-c search_path={row[0]}")


def _config(database_url: str) -> ServerConfig:
    return ServerConfig(
        slack_user_token="xoxp-test",
        slack_app_token="xapp-test",
        shared_secret="sek",
        database_url=database_url,
    )


def test_backfill_cli_rejects_blocked_channel(
    server_conn_factory: ServerConnFactory,
) -> None:
    conn = server_conn_factory()
    block_channel(conn, "CBLOCK", reason="noisy")
    config = _config(_database_url_for_schema(conn))

    async def _run() -> None:
        await _run_backfill(
            config,
            "CBLOCK",
            allow_large=False,
            max_messages=None,
            source="slack-api",
        )

    with pytest.raises(BlockedChannelError):
        trio.run(_run)

    with conn.cursor() as cur:
        cur.execute("SELECT kind, payload FROM health_log ORDER BY id")
        rows = cur.fetchall()
    assert [(str(kind), payload) for kind, payload in rows] == [
        ("backfill_skipped", {"channel_id": "CBLOCK", "reason": "operator_blocked"})
    ]
