"""`slack-fuse-projector` entry point.

Connects to the server WebSocket, applies events into the client projections
store, and keeps `connection_state` / `stream_caught_up` current so the FUSE
read layer's trailer logic has up-to-date staleness signals.

Run with `uv run slack-fuse-projector`. Config comes from the Sprint-0
`ClientConfig` loader (env vars prefixed `SLACK_FUSE_`, then
`~/.config/slack-fuse/config.toml`).

This is intentionally thin: all the interesting code lives in `ws_client.py`,
`per_stream.py`, `apply.py`, and `snapshot_fetch.py`. The entry point only
wires migrations + connections + the WS client into one trio nursery.
"""

from __future__ import annotations

import logging
from pathlib import Path

import psycopg
import trio
from psycopg.rows import TupleRow

import slack_fuse.migrations as client_migrations
from slack_fuse.config import ClientConfig, load_client_config
from slack_fuse.migrations.runner import apply_migrations
from slack_fuse.projector.ws_client import SINGLETON_STREAMS, WSClient, WSClientOptions

log = logging.getLogger(__name__)

_MIGRATIONS_DIR = Path(client_migrations.__file__).parent


def _open_state_conn(database_url: str) -> psycopg.Connection[TupleRow]:
    """A bookkeeping connection: cursors reads + `connection_state` bumps.

    Autocommit because the projector's TX shape relies on every
    `with conn.transaction()` being a real BEGIN/COMMIT (RFC §Flow control →
    Idempotent re-apply).
    """
    conn: psycopg.Connection[TupleRow] = psycopg.connect(database_url)
    conn.autocommit = True
    return conn


def _make_connection_factory(database_url: str):
    """Each applier task gets its own autocommit connection."""

    def factory() -> psycopg.Connection[TupleRow]:
        conn: psycopg.Connection[TupleRow] = psycopg.connect(database_url)
        conn.autocommit = True
        return conn

    return factory


async def _run(config: ClientConfig) -> None:
    setup = _open_state_conn(config.database_url)
    try:
        applied = apply_migrations(setup, _MIGRATIONS_DIR)
    finally:
        setup.close()
    if applied:
        log.info("applied client migrations: %s", ", ".join(applied))

    state_conn = _open_state_conn(config.database_url)
    factory = _make_connection_factory(config.database_url)
    options = WSClientOptions(server_url=config.server_url, shared_secret=config.shared_secret)
    client = WSClient(options, factory, state_conn)
    # Pull existing channel streams from the projections store so the projector
    # resumes them rather than waiting for `channel_added` re-emission.
    with state_conn.cursor() as cur:
        cur.execute("SELECT channel_id FROM channels WHERE subscribed = TRUE")
        per_channel_streams = [f"channel:{row[0]}" for row in cur.fetchall()]
    initial_streams = list(SINGLETON_STREAMS) + per_channel_streams
    try:
        await client.run(initial_streams=initial_streams)
    finally:
        state_conn.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    config = load_client_config()
    trio.run(_run, config)


if __name__ == "__main__":
    main()
