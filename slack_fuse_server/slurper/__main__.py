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
import json
import logging
import os
import sys
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import psycopg
import trio
from psycopg.rows import TupleRow

import slack_fuse_server.migrations as server_migrations
from slack_fuse.migrations.runner import apply_migrations
from slack_fuse.user_cache import UserCache
from slack_fuse_render import ChannelId
from slack_fuse_server.backfill.api import BackfillContext, SlackApiBackfiller, SleepBounds, backfill_channel
from slack_fuse_server.backfill.legacy import LegacyCacheBackfiller
from slack_fuse_server.backfill.types import BackfillAbortReason, Backfiller
from slack_fuse_server.blocked_channels import (
    BlockedChannelError,
    block_channel,
    blocked_channel_ids,
    is_channel_blocked,
    list_blocked_channels,
    unblock_channel,
)
from slack_fuse_server.config import ServerConfig, load_server_config
from slack_fuse_server.dispatch import serve_dispatch
from slack_fuse_server.http.handlers import (
    BackfillDeps,
    BlockedChannelsDeps,
    GapsDeps,
    LivezDeps,
    OriginalsDeps,
    ProbeDeps,
    RefreshDeps,
    ResolvePermalinkDeps,
    SnapshotDeps,
)
from slack_fuse_server.http.metrics import MetricsAggregator, SubscriberSnapshot
from slack_fuse_server.slurper.api import ChannelNotFoundError, SlackAPIError, SlackClient
from slack_fuse_server.slurper.backfill_state import async_find_last_backfill_completion
from slack_fuse_server.slurper.catchup import (
    CatchupConfig,
    CatchupDeps,
    CatchupTrigger,
    should_catchup,
)
from slack_fuse_server.slurper.channels import ensure_channel_added, populate_channels_once
from slack_fuse_server.slurper.health import HealthEmitter, HealthKind
from slack_fuse_server.slurper.limiters import SlurperLimiters
from slack_fuse_server.slurper.offsets import OffsetWriter
from slack_fuse_server.slurper.probes import ProbeTrigger, probe_sweep
from slack_fuse_server.slurper.refresh import RefreshTrigger, refresh_channels_periodically
from slack_fuse_server.slurper.socket import SocketModeOptions, SocketModeStatus
from slack_fuse_server.slurper.spans import configure_span_thresholds_from_config, span
from slack_fuse_server.slurper.supervisor import TaskSupervisor, phase
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


def _connect_and_migrate(config: ServerConfig) -> psycopg.Connection[TupleRow]:
    conn = _connect_server_connection(config)
    applied = apply_migrations(conn, _MIGRATIONS_DIR)
    if applied:
        log.info("applied server migrations: %s", ", ".join(applied))
    _set_runtime_timeouts(conn, config)
    return conn


def _connect_server_connection(config: ServerConfig) -> psycopg.Connection[TupleRow]:
    conn: psycopg.Connection[TupleRow] = psycopg.connect(config.database_url)
    # Autocommit so each `with conn.transaction()` is a real BEGIN/COMMIT. Without
    # it, a bare read (e.g. the backfill-override lookup) opens an implicit
    # transaction, turning every later transaction() into a savepoint that never
    # durably commits — and conn.close() then rolls the whole thing back.
    conn.autocommit = True
    return conn


def _set_runtime_timeouts(conn: psycopg.Connection[TupleRow], config: ServerConfig) -> None:
    lock_timeout_ms = int(config.slurper_lock_timeout_s * 1000)
    statement_timeout_ms = int(config.slurper_statement_timeout_s * 1000)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT set_config('lock_timeout', %s, false), set_config('statement_timeout', %s, false)",
            (f"{lock_timeout_ms}ms", f"{statement_timeout_ms}ms"),
        )


def _connect_writer_pool(config: ServerConfig) -> list[psycopg.Connection[TupleRow]]:
    conns = [_connect_and_migrate(config)]
    try:
        for _ in range(config.slurper_writer_pool_size - 1):
            conn = _connect_server_connection(config)
            _set_runtime_timeouts(conn, config)
            conns.append(conn)
    except Exception:
        for conn in conns:
            conn.close()
        raise
    return conns


def _make_limiters(config: ServerConfig) -> SlurperLimiters:
    slack_api_limiter = trio.CapacityLimiter(2)
    writer_limiter = trio.CapacityLimiter(config.slurper_writer_pool_size)
    snapshot_limiter = trio.CapacityLimiter(1)
    admin_read_limiter = trio.CapacityLimiter(4)
    return SlurperLimiters(
        slack_api=slack_api_limiter,
        writer=writer_limiter,
        snapshot=snapshot_limiter,
        admin_read=admin_read_limiter,
    )


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
    limiters: SlurperLimiters,
    config: ServerConfig,
    writer: OffsetWriter | None = None,
    task_name: str = "backfill",
) -> SlackApiBackfiller:
    sleeps = SleepBounds(
        page_min_s=config.backfill_page_sleep_min_s,
        page_max_s=config.backfill_page_sleep_max_s,
        thread_min_s=config.backfill_thread_sleep_min_s,
        thread_max_s=config.backfill_thread_sleep_max_s,
    )
    blocked = None if writer is None else lambda: writer.run_read(blocked_channel_ids, limiter=limiters.admin_read)
    return SlackApiBackfiller(client, limiters.slack_api, sleeps, blocked_channel_ids=blocked, task_name=task_name)


def _make_catchup_deps(
    client: SlackClient,
    writer: OffsetWriter,
    config: ServerConfig,
    limiters: SlurperLimiters,
) -> CatchupDeps:
    """Build the reconnect/restart catchup sweep's dependencies.

    Uses its own backfiller with tight sleep bounds (the gap-fill is bounded by
    ``oldest``, so pages are few; the 30-180s backfill page throttle would make
    a multi-page busy channel needlessly slow). Slack HTTP uses the slack_api
    gate; blocked-list and resume-point SQL use admin_read; event writes go
    through the writer pool.
    """
    sleeps = SleepBounds(
        page_min_s=config.catchup_page_sleep_min_s,
        page_max_s=config.catchup_page_sleep_max_s,
        thread_min_s=config.catchup_thread_sleep_min_s,
        thread_max_s=config.catchup_thread_sleep_max_s,
    )
    backfiller = SlackApiBackfiller(
        client,
        limiters.slack_api,
        sleeps,
        blocked_channel_ids=lambda: writer.run_read(blocked_channel_ids, limiter=limiters.admin_read),
        task_name="catchup",
    )
    catchup_config = CatchupConfig(
        gap_threshold_s=config.catchup_gap_threshold_s,
        max_lookback_s=config.catchup_max_lookback_s,
        channel_gap_s=config.catchup_channel_gap_s,
        startup_delay_s=config.catchup_startup_delay_s,
    )
    return CatchupDeps(writer=writer, backfiller=backfiller, config=catchup_config, limiters=limiters)


def _make_backfiller(  # noqa: PLR0913 - source wiring keeps dependencies explicit.
    source: BackfillSource,
    *,
    client: SlackClient | None,
    limiters: SlurperLimiters,
    config: ServerConfig,
    writer: OffsetWriter | None = None,
    task_name: str = "backfill",
) -> Backfiller:
    if source == "legacy-cache":
        return LegacyCacheBackfiller(limiter=limiters.writer)
    if source == "slack-api":
        if client is None:  # pragma: no cover - guarded by _run_backfill source wiring
            msg = "slack-api backfill source requires a SlackClient"
            raise ValueError(msg)
        return _make_api_backfiller(client, limiters, config, writer=writer, task_name=task_name)
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


def _log_slurper_started() -> None:
    """Emit the canonical startup line for restart counting."""
    log.info(
        "slurper-started image=%s commit=%s pid=%d",
        os.environ.get("SLACK_FUSE_SERVER_IMAGE", "unknown"),
        os.environ.get("GIT_COMMIT", "unknown"),
        os.getpid(),
    )


async def _serve(config: ServerConfig) -> None:
    slack_api_limiter = trio.CapacityLimiter(2)
    writer_limiter = trio.CapacityLimiter(config.slurper_writer_pool_size)
    snapshot_limiter = trio.CapacityLimiter(1)
    admin_read_limiter = trio.CapacityLimiter(4)
    limiters = SlurperLimiters(
        slack_api=slack_api_limiter,
        writer=writer_limiter,
        snapshot=snapshot_limiter,
        admin_read=admin_read_limiter,
    )
    supervisor = TaskSupervisor()
    writer_conns = _connect_writer_pool(config)
    writer = OffsetWriter(
        writer_conns,
        limiter=limiters.writer,
        acquire_timeout_s=config.slurper_writer_pool_acquire_timeout_s,
    )
    client = SlackClient(config.slack_user_token)
    users = UserCache(client.http)
    users.populate()
    resolve_permalink_deps = ResolvePermalinkDeps(
        client=client,
        users=users,
        workspace_url=os.environ.get("SLACK_WORKSPACE_URL"),
    )
    snapshot_deps = SnapshotDeps(database_url=config.database_url)
    originals_deps = OriginalsDeps(database_url=config.database_url)
    gaps_deps = GapsDeps(database_url=config.database_url)
    livez_deps = LivezDeps(supervisor=supervisor)
    # Trigger for ``POST /refresh-channels`` — request() rendezvous against
    # the consumer task spawned below. Auth lives at the HTTP layer (shared
    # secret); the trigger itself is just a one-in-flight dispatcher.
    refresh_trigger = RefreshTrigger()
    refresh_deps = RefreshDeps(
        shared_secret=config.shared_secret,
        trigger=refresh_trigger,
        database_url=config.database_url,
    )
    blocked_channels_deps = BlockedChannelsDeps(
        shared_secret=config.shared_secret,
        database_url=config.database_url,
    )
    backfill_trigger = ManualBackfillTrigger()
    backfill_deps = BackfillDeps(
        shared_secret=config.shared_secret,
        database_url=config.database_url,
        trigger=backfill_trigger,
    )
    probe_trigger = ProbeTrigger(max_buffer_size=1)
    probe_deps = ProbeDeps(
        shared_secret=config.shared_secret,
        trigger=probe_trigger,
    )
    health = HealthEmitter(writer)

    # Reconnect/restart catchup: a startup gap-fill plus an on-demand one fired
    # by the socket runner when a reconnect's downtime drained Slack's buffer.
    catchup_trigger = CatchupTrigger() if config.catchup_enabled else None
    catchup_deps = _make_catchup_deps(client, writer, config, limiters) if catchup_trigger is not None else None

    # The snapshot scheduler runs on its own connection + limiter so generating
    # a large snapshot never blocks live event writes (its REPEATABLE READ reads
    # don't contend with the writer's autocommit inserts on a separate backend).
    snapshot_conn = _connect_snapshot(config.database_url)
    snapshot_scheduler = SnapshotScheduler(
        snapshot_conn,
        every_n_events=config.snapshot_every_n_events,
        max_age_seconds=config.snapshot_max_age_hours * 3600,
        limiter=limiters.snapshot,
    )

    status = SocketModeStatus()
    wire_server = WireServer(config.database_url, shared_secret=config.shared_secret or None)
    metrics = _build_metrics_aggregator(config, status, wire_server, datetime.now(UTC))

    auto_backfill = os.environ.get(_AUTO_BACKFILL_ENV, "").lower() in ("1", "true", "yes")
    try:
        async with trio.open_nursery() as nursery:
            _log_slurper_started()
            nursery.start_soon(
                _run_socket_mode_with_users_task,
                writer,
                health,
                client,
                config,
                status,
                catchup_trigger,
                limiters,
                supervisor,
            )
            nursery.start_soon(populate_users_once, writer, client, limiters, supervisor)
            nursery.start_soon(populate_channels_once, writer, client, limiters, supervisor)
            nursery.start_soon(
                _serve_dispatch_task,
                config.listen_addr,
                wire_server,
                metrics,
                resolve_permalink_deps,
                snapshot_deps,
                originals_deps,
                gaps_deps,
                refresh_deps,
                blocked_channels_deps,
                backfill_deps,
                probe_deps,
                livez_deps,
            )
            nursery.start_soon(snapshot_scheduler.run, supervisor)
            # Periodic ``conversations.info`` refresh: backfills lossy
            # legacy channel_added payloads (pre raw-persistence) and
            # catches drift the webhook flow doesn't surface.
            nursery.start_soon(refresh_channels_periodically, writer, client, limiters, supervisor)
            # Long-lived consumer for HTTP-triggered refresh requests
            # (POST /refresh-channels). Same job as the periodic task,
            # fires only on demand. Rendezvous channel means a second
            # POST while one is running gets 409, not a queued cycle.
            nursery.start_soon(refresh_trigger.consume, writer, client, limiters, supervisor)
            nursery.start_soon(probe_sweep, writer, client, limiters, supervisor, config, probe_trigger)
            nursery.start_soon(backfill_trigger.consume, config, supervisor)
            # Reconnect/restart catchup consumer: runs one bounded gap-fill at
            # startup (the restart case) and one per gap-reconnect the socket
            # runner signals via catchup_trigger.
            if catchup_trigger is not None and catchup_deps is not None:
                nursery.start_soon(catchup_trigger.consume, catchup_deps, supervisor)
            if auto_backfill:
                nursery.start_soon(_auto_backfill, config, writer, health, client, limiters, supervisor)
            log.info("slack-fuse-server listening on %s (HTTP /health, /metrics + WS /ws)", config.listen_addr)
    finally:
        client.close()
        writer.close()
        snapshot_conn.close()


async def _run_socket_mode_with_users_task(  # noqa: PLR0913, PLR0917 - socket task needs its full dep set
    writer: OffsetWriter,
    health: HealthEmitter,
    client: SlackClient,
    config: ServerConfig,
    status: SocketModeStatus,
    catchup_trigger: CatchupTrigger | None,
    limiters: SlurperLimiters,
    supervisor: TaskSupervisor,
) -> None:
    options = SocketModeOptions(
        degraded_min_duration_s=config.slack_degraded_min_duration_s,
        status=status,
        on_reconnect=_make_on_reconnect(catchup_trigger, config.catchup_gap_threshold_s),
    )
    await run_socket_mode_with_users(
        writer,
        health,
        client,
        config.slack_app_token,
        limiters=limiters,
        options=options,
        supervisor=supervisor,
    )


def _make_on_reconnect(
    catchup_trigger: CatchupTrigger | None,
    gap_threshold_s: float,
) -> Callable[[float], None] | None:
    """Build the socket runner's reconnect hook: nudge the catchup trigger when
    the downtime exceeded the buffer-drain threshold. ``None`` when catchup is
    disabled, so the runner skips the call entirely."""
    if catchup_trigger is None:
        return None

    def _on_reconnect(gap_seconds: float) -> None:
        if not should_catchup(gap_seconds, threshold_s=gap_threshold_s):
            return
        if not catchup_trigger.request(gap_seconds):
            log.info("catchup: reconnect gap=%.0fs but a catchup is already queued; skipping", gap_seconds)

    return _on_reconnect


async def _serve_dispatch_task(  # noqa: PLR0913, PLR0917 - dispatch wiring needs explicit deps.
    listen_addr: str,
    wire_server: WireServer,
    metrics: MetricsAggregator,
    resolve_permalink_deps: ResolvePermalinkDeps,
    snapshot_deps: SnapshotDeps,
    originals_deps: OriginalsDeps,
    gaps_deps: GapsDeps,
    refresh_deps: RefreshDeps,
    blocked_channels_deps: BlockedChannelsDeps,
    backfill_deps: BackfillDeps,
    probe_deps: ProbeDeps,
    livez_deps: LivezDeps,
) -> None:
    await serve_dispatch(
        listen_addr=listen_addr,
        wire_server=wire_server,
        metrics_source=metrics,
        resolve_permalink_deps=resolve_permalink_deps,
        snapshot_deps=snapshot_deps,
        originals_deps=originals_deps,
        gaps_deps=gaps_deps,
        refresh_deps=refresh_deps,
        blocked_channels_deps=blocked_channels_deps,
        backfill_deps=backfill_deps,
        probe_deps=probe_deps,
        livez_deps=livez_deps,
    )


async def _auto_backfill(  # noqa: C901, PLR0913, PLR0917 - task dependencies and phase boundaries are explicit.
    config: ServerConfig,
    writer: OffsetWriter,
    health: HealthEmitter,
    client: SlackClient,
    limiters: SlurperLimiters,
    supervisor: TaskSupervisor | None = None,
) -> None:
    """Automatic first-bootup pass: backfill every member channel, throttled."""
    if supervisor is not None:
        supervisor.declare("auto-backfill", "startup_sleep", deadline_s=None)
    await trio.sleep(30)  # let startup settle before hitting the API hard
    backfiller = _make_backfiller(
        "slack-api",
        client=client,
        limiters=limiters,
        config=config,
        writer=writer,
        task_name="auto-backfill",
    )
    first_backfill = True
    if supervisor is not None:
        supervisor.declare("auto-backfill", "listing_channels", deadline_s=60)
    async for channel_id in backfiller.channels_to_backfill():
        if config.auto_backfill_skip_if_completed:
            if supervisor is None:
                completion = await async_find_last_backfill_completion(writer, channel_id.value, limiters)
            else:
                async with phase(
                    supervisor,
                    "auto-backfill",
                    "checking_skip",
                    details={"channel_id": channel_id.value},
                    deadline_s=5,
                ):
                    completion = await async_find_last_backfill_completion(writer, channel_id.value, limiters)
            if completion is not None:
                async with span(
                    op="slurper.auto_backfill.channel",
                    task="auto-backfill",
                    extra={"channel_id": channel_id.value},
                ) as channel_span:
                    channel_span.mark_skipped()
                    channel_span.set("completed_at", completion.at.isoformat())
                    channel_span.set("events_written", completion.events_written)
                log.info(
                    "auto-backfill: skipping %s — completed at %s, events_written=%d",
                    channel_id.value,
                    completion.at.isoformat(),
                    completion.events_written,
                )
                continue
        if not first_backfill:
            if supervisor is not None:
                supervisor.declare(
                    "auto-backfill",
                    "inter_channel_sleep",
                    details={"channel_id": channel_id.value},
                    deadline_s=None,
                )
            await trio.sleep(_AUTO_BACKFILL_CHANNEL_GAP_S)
        first_backfill = False
        async with span(
            op="slurper.auto_backfill.channel",
            task="auto-backfill",
            extra={"channel_id": channel_id.value},
        ) as channel_span:
            log.info("auto-backfill: %s", channel_id.value)
            ctx = BackfillContext(
                writer=writer,
                health=health,
                limiters=limiters,
                warn_at=config.backfill_warn_at,
                abort_at=config.backfill_abort_at,
                task_name="auto-backfill",
            )
            if supervisor is None:
                result = await backfill_channel(backfiller, channel_id, ctx)
            else:
                async with phase(
                    supervisor,
                    "auto-backfill",
                    "channel",
                    details={"channel_id": channel_id.value},
                    deadline_s=config.backfill_abort_at * 0.5,
                ):
                    result = await backfill_channel(backfiller, channel_id, ctx)
            channel_span.set("messages", result.messages)
            channel_span.set("events_written", result.events_written)
            channel_span.set("aborted", result.aborted)
            if result.abort_reason is not None:
                channel_span.set("abort_reason", str(result.abort_reason))
    if supervisor is not None:
        supervisor.declare("auto-backfill", "complete", deadline_s=None)
    log.info("auto-backfill: complete")


# === refresh-channels (admin one-shot) ===


async def _run_refresh_channels_once(config: ServerConfig) -> None:
    """One-shot CLI: run a single channel-metadata refresh cycle.

    Same job the in-process periodic task does — useful when an operator
    just joined a channel and wants the projector to see the new
    ``is_member`` state without waiting for the next scheduled cycle.
    """
    from slack_fuse_server.slurper.refresh import refresh_channels_once  # noqa: PLC0415

    limiters = _make_limiters(config)
    writer = OffsetWriter(
        _connect_writer_pool(config),
        limiter=limiters.writer,
        acquire_timeout_s=config.slurper_writer_pool_acquire_timeout_s,
    )
    client = SlackClient(config.slack_user_token)
    try:
        await refresh_channels_once(writer, client, limiters)
    finally:
        client.close()
        writer.close()


# === Backfill (admin) mode ===


async def _run_backfill(  # noqa: PLR0913 — thin CLI thunk; bundling into options dataclass adds more noise than it saves
    config: ServerConfig,
    channel_id: str,
    *,
    allow_large: bool,
    max_messages: int | None,
    source: BackfillSource,
    since_ts: float | None = None,
) -> None:
    limiters = _make_limiters(config)
    writer = OffsetWriter(
        _connect_writer_pool(config),
        limiter=limiters.writer,
        acquire_timeout_s=config.slurper_writer_pool_acquire_timeout_s,
    )
    health = HealthEmitter(writer)
    client: SlackClient | None = None
    try:
        if await writer.run_read(lambda conn: is_channel_blocked(conn, channel_id), limiter=limiters.admin_read):
            await health.emit(
                HealthKind.BACKFILL_SKIPPED,
                {"channel_id": channel_id, "reason": str(BackfillAbortReason.OPERATOR_BLOCKED)},
            )
            raise BlockedChannelError(channel_id)
        # The SlackClient is required for `slack-api` source (history fetch) AND for
        # every source so we can call `conversations.info` to emit a synthetic
        # `channel_added` event before any per-channel writes. Without this, a
        # backfill on a channel the slurper's startup populate never saw (e.g.
        # archived → excluded by populate; or "channel I joined while server was
        # down") writes events on a `channel:<id>` stream the client projector has
        # no row for in its `channels` table, never subscribes, and orphans them.
        client = SlackClient(config.slack_user_token)
        backfiller = _make_backfiller(source, client=client, limiters=limiters, config=config, writer=writer)

        abort_at = await writer.run_transaction(
            lambda conn: _resolve_abort_at(
                conn,
                channel_id,
                config,
                allow_large=allow_large,
                max_messages=max_messages,
            )
        )
        ctx = BackfillContext(
            writer=writer,
            health=health,
            limiters=limiters,
            warn_at=config.backfill_warn_at,
            abort_at=abort_at,
        )
        # Bring the channel under the projector's normal model BEFORE we write
        # any per-channel events. A channel the user token can't describe
        # (left/closed DMs, archived-then-purged) gets skipped cleanly so the
        # admin Job exits 0; any other API failure still fails loud because it
        # would orphan events otherwise.
        try:
            emitted = await ensure_channel_added(writer, client, channel_id, limiters)
        except ChannelNotFoundError:
            log.warning(
                "backfill: channel %s not accessible (channel_not_found); skipping cleanly. "
                "Channel cache may still hold legacy data but the user token can't describe it.",
                channel_id,
            )
            return
        except SlackAPIError as exc:
            log.error("backfill: cannot establish channel_added for %s: %s", channel_id, exc)
            raise
        if emitted:
            log.info("backfill: emitted synthetic channel_added for %s", channel_id)
        result = await backfill_channel(backfiller, ChannelId(channel_id), ctx, since_ts=since_ts)
    finally:
        if client is not None:
            client.close()
        writer.close()

    if result.abort_reason == BackfillAbortReason.OPERATOR_BLOCKED:
        raise BlockedChannelError(channel_id)

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


class ManualBackfillTrigger:
    """Rendezvous trigger for HTTP-requested manual channel backfills."""

    def __init__(self) -> None:
        self._send, self._recv = trio.open_memory_channel[str](max_buffer_size=0)

    def request_channel(self, channel_id: str) -> bool:
        try:
            self._send.send_nowait(channel_id)
        except trio.WouldBlock:
            return False
        return True

    async def consume(self, config: ServerConfig, supervisor: TaskSupervisor | None = None) -> None:
        while True:
            if supervisor is not None:
                supervisor.declare("backfill-trigger", "waiting_for_trigger", deadline_s=None)
            try:
                channel_id = await self._recv.receive()
            except trio.EndOfChannel:
                return
            try:
                if supervisor is None:
                    await _run_backfill(
                        config,
                        channel_id,
                        allow_large=False,
                        max_messages=None,
                        source="slack-api",
                    )
                else:
                    async with phase(
                        supervisor,
                        "backfill-trigger",
                        "running",
                        details={"channel_id": channel_id},
                        deadline_s=config.backfill_abort_at * 0.5,
                    ):
                        await _run_backfill(
                            config,
                            channel_id,
                            allow_large=False,
                            max_messages=None,
                            source="slack-api",
                        )
            except BlockedChannelError:
                log.info("backfill: HTTP-triggered run for %s rejected: blocked", channel_id)
            except Exception:
                log.exception("backfill: HTTP-triggered run for %s failed", channel_id)


def _run_block_command(config: ServerConfig, channel_id: str, reason: str | None) -> None:
    conn = _connect_and_migrate(config)
    try:
        row = block_channel(conn, channel_id, reason=reason)
    finally:
        conn.close()
    print(json.dumps(row, separators=(",", ":")))


def _run_unblock_command(config: ServerConfig, channel_id: str) -> None:
    conn = _connect_and_migrate(config)
    try:
        unblock_channel(conn, channel_id)
    finally:
        conn.close()
    print(json.dumps({"status": "unblocked", "channel_id": channel_id}, separators=(",", ":")))


def _run_list_blocked_command(config: ServerConfig) -> None:
    conn = _connect_and_migrate(config)
    try:
        rows = list_blocked_channels(conn)
    finally:
        conn.close()
    print(json.dumps({"blocked": rows}, separators=(",", ":")))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="slack-fuse-server", description="slack-fuse event-sourced backend")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("serve", help="run the slurper (default)")
    sub.add_parser(
        "refresh-channels",
        help="run one channel-metadata refresh cycle (diff-and-emit "
        "channel_info_refreshed events). Same job the periodic in-process "
        "task does — exposed as a one-shot so operators can trigger drift "
        "catchup on demand.",
    )
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
    bf.add_argument(
        "--since",
        type=float,
        default=None,
        metavar="EPOCH",
        help="only fetch messages with ts > EPOCH (Slack ts is float seconds since epoch). "
        "Bounds pagination at the source; combined with the events_message_dedup index "
        "this makes per-channel gap-fills cheap and idempotent.",
    )
    block = sub.add_parser("block", help="block a channel from refresh/backfill")
    block.add_argument("channel_id", help="Slack channel id, e.g. C0AKQ5DS0FQ")
    block.add_argument("--reason", default=None, help="optional operator reason")
    unblock = sub.add_parser("unblock", help="remove a channel block")
    unblock.add_argument("channel_id", help="Slack channel id, e.g. C0AKQ5DS0FQ")
    sub.add_parser("list-blocked", help="dump blocked_channels as JSON")
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = _build_parser().parse_args()
    config = load_server_config()
    configure_span_thresholds_from_config(config)

    if args.command == "refresh-channels":
        trio.run(_run_refresh_channels_once, config)
        return
    if args.command == "backfill":
        channel_id: str = args.channel_id
        allow_large: bool = args.allow_large
        max_messages: int | None = args.max_messages
        source: BackfillSource = args.source
        since_ts: float | None = args.since

        async def _thunk() -> None:
            await _run_backfill(
                config,
                channel_id,
                allow_large=allow_large,
                max_messages=max_messages,
                source=source,
                since_ts=since_ts,
            )

        try:
            trio.run(_thunk)
        except BlockedChannelError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            raise SystemExit(2) from exc
        return
    if args.command == "block":
        _run_block_command(config, args.channel_id, args.reason)
        return
    if args.command == "unblock":
        _run_unblock_command(config, args.channel_id)
        return
    if args.command == "list-blocked":
        _run_list_blocked_command(config)
        return
    trio.run(_serve, config)


if __name__ == "__main__":
    main()
