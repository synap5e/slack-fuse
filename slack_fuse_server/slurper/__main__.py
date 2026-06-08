"""`slack-fuse-server` entry point.

Two modes:

- no subcommand (or `serve`): run the slurper — connect to postgres, apply
  server migrations, then start a trio nursery with the Socket Mode ingestion
  task (and, when `SLACK_FUSE_SERVER_BACKFILL` is truthy, the automatic
  channel-backfill pass). The WS server (1B) and HTTP server (1C) tasks slot
  into the same nursery later.
- `backfill <channel-id>`: the admin recovery command (RFC §Backfill → Manual).
  Backfills one channel through the same offset-assignment write path, honouring
  the configured size thresholds. `--allow-large` / `--max-messages N` raise or
  lift the per-channel limit and persist the choice in `backfill_overrides`.

Config comes from the Sprint-0 `ServerConfig` loader (env vars prefixed
`SLACK_FUSE_SERVER_`, then `~/.config/slack-fuse-server/config.toml`). The
automatic-backfill gate is an env var rather than a config field so the frozen
Sprint-0 config contract is untouched.
"""

from __future__ import annotations

import argparse
import logging
import os
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import psycopg
import trio
from psycopg.rows import TupleRow

import slack_fuse_server.migrations as server_migrations
from slack_fuse.migrations.runner import apply_migrations
from slack_fuse_render import ChannelId
from slack_fuse_server.backfill.api import BackfillContext, SlackApiBackfiller, SleepBounds, backfill_channel
from slack_fuse_server.backfill.legacy import LegacyCacheBackfiller
from slack_fuse_server.backfill.types import Backfiller
from slack_fuse_server.config import ServerConfig, load_server_config
from slack_fuse_server.dispatch import serve_dispatch
from slack_fuse_server.http.metrics import MetricsAggregator, SubscriberSnapshot
from slack_fuse_server.slurper.api import SlackClient
from slack_fuse_server.slurper.health import HealthEmitter
from slack_fuse_server.slurper.offsets import OffsetWriter
from slack_fuse_server.slurper.socket import SocketModeOptions, SocketModeStatus
from slack_fuse_server.slurper.users import populate_users_once, run_socket_mode_with_users
from slack_fuse_server.snapshot import SnapshotScheduler
from slack_fuse_server.wire.server import WireServer

log = logging.getLogger(__name__)

_MIGRATIONS_DIR = Path(server_migrations.__file__).parent
_AUTO_BACKFILL_ENV = "SLACK_FUSE_SERVER_BACKFILL"
# Sleep between channels in the automatic backfill pass (RFC: yields between
# channels so live ingestion stays responsive).
_AUTO_BACKFILL_CHANNEL_GAP_S = 60.0
_BACKFILL_SOURCES = ("slack-api", "legacy-cache")
type BackfillSource = Literal["slack-api", "legacy-cache"]


def _connect_and_migrate(database_url: str) -> psycopg.Connection[TupleRow]:
    conn: psycopg.Connection[TupleRow] = psycopg.connect(database_url)
    # Autocommit so each `with conn.transaction()` is a real BEGIN/COMMIT. Without
    # it, a bare read (e.g. the backfill-override lookup) opens an implicit
    # transaction, turning every later transaction() into a savepoint that never
    # durably commits — and conn.close() then rolls the whole thing back.
    conn.autocommit = True
    applied = apply_migrations(conn, _MIGRATIONS_DIR)
    if applied:
        log.info("applied server migrations: %s", ", ".join(applied))
    return conn


def _connect_snapshot(database_url: str) -> psycopg.Connection[TupleRow]:
    """A second connection for the snapshot scheduler (migrations already applied).

    Autocommit so `generate_snapshot`'s `conn.transaction()` opens a real
    transaction at the REPEATABLE READ level it sets — mirroring the
    `OffsetWriter` connection contract.
    """
    conn: psycopg.Connection[TupleRow] = psycopg.connect(database_url)
    conn.autocommit = True
    return conn


def _make_api_backfiller(
    client: SlackClient,
    limiter: trio.CapacityLimiter,
    config: ServerConfig,
) -> SlackApiBackfiller:
    sleeps = SleepBounds(
        page_min_s=config.backfill_page_sleep_min_s,
        page_max_s=config.backfill_page_sleep_max_s,
        thread_min_s=config.backfill_thread_sleep_min_s,
        thread_max_s=config.backfill_thread_sleep_max_s,
    )
    return SlackApiBackfiller(client, limiter, sleeps)


def _make_backfiller(
    source: BackfillSource,
    *,
    client: SlackClient | None,
    limiter: trio.CapacityLimiter,
    config: ServerConfig,
) -> Backfiller:
    if source == "legacy-cache":
        return LegacyCacheBackfiller(limiter=limiter)
    if source == "slack-api":
        if client is None:  # pragma: no cover - guarded by _run_backfill source wiring
            msg = "slack-api backfill source requires a SlackClient"
            raise ValueError(msg)
        return _make_api_backfiller(client, limiter, config)
    msg = f"unsupported backfill source {source!r}"
    raise ValueError(msg)


# === Backfill-override persistence (RFC §Backfill → Per-channel size threshold) ===


def _get_override(conn: psycopg.Connection[TupleRow], channel_id: str) -> tuple[bool, int | None]:
    """Return (found, max_messages). `found=False` means no override row."""
    with conn.cursor() as cur:
        cur.execute("SELECT max_messages FROM backfill_overrides WHERE channel_id = %s", (channel_id,))
        row = cur.fetchone()
    if row is None:
        return (False, None)
    return (True, None if row[0] is None else int(row[0]))


def _set_override(conn: psycopg.Connection[TupleRow], channel_id: str, max_messages: int | None) -> None:
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "INSERT INTO backfill_overrides (channel_id, max_messages) VALUES (%s, %s) "
            "ON CONFLICT (channel_id) DO UPDATE SET max_messages = EXCLUDED.max_messages",
            (channel_id, max_messages),
        )


def _resolve_abort_at(
    conn: psycopg.Connection[TupleRow],
    channel_id: str,
    config: ServerConfig,
    *,
    allow_large: bool,
    max_messages: int | None,
) -> int | None:
    """Resolve the effective abort threshold, persisting any CLI override."""
    if allow_large:
        _set_override(conn, channel_id, None)
        return None
    if max_messages is not None:
        _set_override(conn, channel_id, max_messages)
        return max_messages
    found, stored = _get_override(conn, channel_id)
    if found:
        return stored
    return config.backfill_abort_at


# === Server (serve) mode ===


def _build_metrics_aggregator(
    config: ServerConfig,
    status: SocketModeStatus,
    wire_server: WireServer,
    started_at: datetime,
) -> MetricsAggregator:
    """Wire `/metrics` to live runtime state: socket-mode status + WS subscribers."""

    def _subscribers() -> Sequence[SubscriberSnapshot]:
        return [
            SubscriberSnapshot(
                client_id=info.client_id,
                connected_since=info.connected_since,
                subscriptions=info.subscriptions,
            )
            for info in wire_server.connection_infos()
        ]

    return MetricsAggregator(
        database_url=config.database_url,
        server_started_at=started_at,
        socket_mode_state=lambda: status.state,
        subscribers=_subscribers,
    )


async def _serve(config: ServerConfig) -> None:
    conn = _connect_and_migrate(config.database_url)
    client = SlackClient(config.slack_user_token)
    limiter = trio.CapacityLimiter(1)
    writer = OffsetWriter(conn, limiter)
    health = HealthEmitter(writer)

    # The snapshot scheduler runs on its own connection + limiter so generating
    # a large snapshot never blocks live event writes (its REPEATABLE READ reads
    # don't contend with the writer's autocommit inserts on a separate backend).
    snapshot_conn = _connect_snapshot(config.database_url)
    snapshot_scheduler = SnapshotScheduler(
        snapshot_conn,
        every_n_events=config.snapshot_every_n_events,
        max_age_seconds=config.snapshot_max_age_hours * 3600,
        limiter=trio.CapacityLimiter(1),
    )

    status = SocketModeStatus()
    wire_server = WireServer(config.database_url, shared_secret=config.shared_secret or None)
    metrics = _build_metrics_aggregator(config, status, wire_server, datetime.now(UTC))

    auto_backfill = os.environ.get(_AUTO_BACKFILL_ENV, "").lower() in ("1", "true", "yes")
    try:
        async with trio.open_nursery() as nursery:
            nursery.start_soon(_run_socket_mode_with_users_task, writer, health, client, config, status)
            nursery.start_soon(populate_users_once, writer, client)
            nursery.start_soon(_serve_dispatch_task, config.listen_addr, wire_server, metrics)
            nursery.start_soon(snapshot_scheduler.run)
            if auto_backfill:
                nursery.start_soon(_auto_backfill, config, writer, health, client, limiter)
            log.info("slack-fuse-server listening on %s (HTTP /health, /metrics + WS /ws)", config.listen_addr)
    finally:
        client.close()
        conn.close()
        snapshot_conn.close()


async def _run_socket_mode_with_users_task(
    writer: OffsetWriter,
    health: HealthEmitter,
    client: SlackClient,
    config: ServerConfig,
    status: SocketModeStatus,
) -> None:
    options = SocketModeOptions(
        degraded_min_duration_s=config.slack_degraded_min_duration_s,
        status=status,
    )
    await run_socket_mode_with_users(writer, health, client, config.slack_app_token, options=options)


async def _serve_dispatch_task(listen_addr: str, wire_server: WireServer, metrics: MetricsAggregator) -> None:
    await serve_dispatch(listen_addr=listen_addr, wire_server=wire_server, metrics_source=metrics)


async def _auto_backfill(
    config: ServerConfig,
    writer: OffsetWriter,
    health: HealthEmitter,
    client: SlackClient,
    limiter: trio.CapacityLimiter,
) -> None:
    """Automatic first-bootup pass: backfill every member channel, throttled."""
    await trio.sleep(30)  # let startup settle before hitting the API hard
    backfiller = _make_backfiller("slack-api", client=client, limiter=limiter, config=config)
    first = True
    async for channel_id in backfiller.channels_to_backfill():
        if not first:
            await trio.sleep(_AUTO_BACKFILL_CHANNEL_GAP_S)
        first = False
        log.info("auto-backfill: %s", channel_id.value)
        ctx = BackfillContext(
            writer=writer, health=health, warn_at=config.backfill_warn_at, abort_at=config.backfill_abort_at
        )
        await backfill_channel(backfiller, channel_id, ctx)
    log.info("auto-backfill: complete")


# === Backfill (admin) mode ===


async def _run_backfill(
    config: ServerConfig,
    channel_id: str,
    *,
    allow_large: bool,
    max_messages: int | None,
    source: BackfillSource,
) -> None:
    conn = _connect_and_migrate(config.database_url)
    limiter = trio.CapacityLimiter(1)
    writer = OffsetWriter(conn, limiter)
    health = HealthEmitter(writer)
    client: SlackClient | None = None
    if source == "slack-api":
        client = SlackClient(config.slack_user_token)
    backfiller = _make_backfiller(source, client=client, limiter=limiter, config=config)

    abort_at = _resolve_abort_at(conn, channel_id, config, allow_large=allow_large, max_messages=max_messages)
    ctx = BackfillContext(writer=writer, health=health, warn_at=config.backfill_warn_at, abort_at=abort_at)
    try:
        result = await backfill_channel(backfiller, ChannelId(channel_id), ctx)
    finally:
        if client is not None:
            client.close()
        conn.close()

    status = "ABORTED" if result.aborted else "completed"
    log.info(
        "backfill %s: channel=%s messages=%d events_written=%d elapsed=%.1fs",
        status,
        channel_id,
        result.messages,
        result.events_written,
        result.elapsed_s,
    )


# === CLI ===


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="slack-fuse-server", description="slack-fuse event-sourced backend")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("serve", help="run the slurper (default)")
    bf = sub.add_parser("backfill", help="backfill one channel's history")
    bf.add_argument("channel_id", help="Slack channel id, e.g. C0AKQ5DS0FQ")
    bf.add_argument("--allow-large", action="store_true", help="lift the per-channel size limit entirely")
    bf.add_argument("--max-messages", type=int, default=None, help="override the per-channel abort threshold")
    bf.add_argument(
        "--source",
        choices=_BACKFILL_SOURCES,
        default="slack-api",
        help="backfill source implementation to use",
    )
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = _build_parser().parse_args()
    config = load_server_config()

    if args.command == "backfill":
        channel_id: str = args.channel_id
        allow_large: bool = args.allow_large
        max_messages: int | None = args.max_messages
        source: BackfillSource = args.source

        async def _thunk() -> None:
            await _run_backfill(
                config,
                channel_id,
                allow_large=allow_large,
                max_messages=max_messages,
                source=source,
            )

        trio.run(_thunk)
        return
    trio.run(_serve, config)


if __name__ == "__main__":
    main()
