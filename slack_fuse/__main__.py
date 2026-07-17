"""CLI entry point for slack-fuse."""

from __future__ import annotations

import argparse
import contextlib
import logging
import os
import signal
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, cast
from urllib.parse import urlsplit, urlunsplit

import httpx

if TYPE_CHECKING:
    from zoneinfo import ZoneInfo


def _default_mountpoint() -> str:
    from .auth import load_mountpoint

    return load_mountpoint() or os.path.expanduser("~/views/slack")


_REFRESH_INTERVAL = 1800  # 30 minutes, matches _CHANNEL_LIST_TTL


def _env_bool(name: str, default: bool) -> bool:
    """Parse a boolean env var. Accepts 1/0, true/false, yes/no, on/off."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    val = raw.strip().lower()
    if val in ("1", "true", "yes", "on"):
        return True
    if val in ("0", "false", "no", "off", ""):
        return False
    msg = f"{name} must be a boolean (got {raw!r})"
    raise ValueError(msg)


def _http_base_from_server_url(server_url: str) -> str:
    """Convert ws:// or http:// server URL into an HTTP base URL."""
    parsed = urlsplit(server_url)
    if parsed.scheme not in ("ws", "wss", "http", "https"):
        msg = f"server URL must use ws/wss/http/https (got {parsed.scheme!r})"
        raise ValueError(msg)
    if not parsed.netloc:
        msg = f"server URL is missing host:port: {server_url!r}"
        raise ValueError(msg)

    scheme = "https" if parsed.scheme in ("wss", "https") else "http"
    path = parsed.path.rstrip("/")
    if path.endswith("/ws"):
        path = path[:-3]
    return urlunsplit((scheme, parsed.netloc, path, "", ""))


def _post_server_json(server_url: str, endpoint: str, payload: dict[str, str]) -> dict[str, object]:
    """POST JSON to an endpoint and return a parsed JSON object body."""
    url = f"{_http_base_from_server_url(server_url)}{endpoint}"
    response = httpx.post(url, json=payload, timeout=30.0)
    try:
        body_raw: object = response.json()
    except ValueError:
        body_raw = {}

    if response.status_code >= 400:
        error = "unknown_error"
        if isinstance(body_raw, dict):
            body_dict = cast("dict[str, object]", body_raw)
            error_raw = body_dict.get("error")
            if isinstance(error_raw, str):
                error = error_raw
        raise RuntimeError(error)

    if not isinstance(body_raw, dict):
        msg = f"server returned non-object JSON from {endpoint}"
        raise ValueError(msg)
    return cast("dict[str, object]", body_raw)


def _required_string(payload: dict[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str):
        msg = f"server response missing string field {key!r}"
        raise ValueError(msg)
    return value


def _absolute_path_from_mount_relative(mountpoint: str, relative_path: str) -> str:
    clean = relative_path.lstrip("/")
    return os.path.join(mountpoint, clean)


def _path_for_server(path: str, mountpoint: str) -> str:
    """Prefer a mount-relative path when possible; else keep caller input."""
    expanded_mount = os.path.abspath(os.path.expanduser(mountpoint))
    if not os.path.isabs(path):
        return path
    expanded_path = os.path.abspath(os.path.expanduser(path))
    relative = os.path.relpath(expanded_path, expanded_mount)
    if relative == ".." or relative.startswith(f"..{os.sep}"):
        return expanded_path
    return relative


def _resolve_local_zoneinfo() -> ZoneInfo:
    """Resolve the process's local IANA timezone as a ``ZoneInfo``.

    ``SlackFuseOpsV2`` needs a keyed ``ZoneInfo`` (it passes ``tz.key`` to
    Postgres ``AT TIME ZONE``), so a bare ``datetime.astimezone().tzinfo``
    (which has no ``.key``) won't do. Prefer ``$TZ``, then the
    ``/etc/localtime`` symlink target, then UTC.
    """
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

    tz_env = os.environ.get("TZ")
    if tz_env:
        try:
            return ZoneInfo(tz_env)
        except (ZoneInfoNotFoundError, ValueError):
            pass
    localtime = Path("/etc/localtime")
    if localtime.is_symlink():
        target = str(localtime.resolve())
        marker = "/zoneinfo/"
        if marker in target:
            try:
                return ZoneInfo(target.split(marker, 1)[1])
            except (ZoneInfoNotFoundError, ValueError):
                pass
    return ZoneInfo("UTC")


def _migrate_legacy_always_blocked(
    http_client: httpx.Client,
    base_http_url: str,
    channel_ids: frozenset[str],
    *,
    shared_secret: str | None,
    log: logging.Logger,
) -> None:
    """Deprecated. Log the state of the config-driven block list; do NOT re-push.

    Previous behaviour re-POSTed every config entry to the server on every
    startup. That silently reversed any operator DELETE via
    ``_control/blocked_channels`` — an ID unblocked at runtime got re-blocked
    the next time the mount started.

    New behaviour is inert with respect to server state: read the current
    server-side block list (SSOT), classify each config entry as either
    already-blocked (safe to drop from config) or orphan (server thinks it's
    unblocked — either the operator unblocked it deliberately, or it was
    never migrated), and log an actionable warning for each. The server-side
    ``_control/blocked_channels`` write side is the only path that mutates
    blocks now.
    """
    if not channel_ids:
        return
    from slack_fuse.projector.block_fetch import blocked_channel_ids_from_payload, get_blocked_channels

    status, body = get_blocked_channels(http_client, base_http_url, shared_secret=shared_secret)
    if status != 200:
        log.warning(
            "always_blocked_channel_ids: cannot classify config entries (server /blocked-channels returned %s); "
            "leaving them alone. Fix and re-check on next start.",
            status,
        )
        return
    # Reuse the canonical payload parser (FINDING-13, 2026-07-17 adversarial
    # review). The prior inline code read ``body.get("blocked_channels")``
    # while the server returns ``{"blocked": [...]}`` — server_blocked_ids
    # was always empty, so every config entry was classified "orphan" and
    # the "already blocked, safe to drop from config" branch never fired.
    # Bug landed in b0dcff2 with a fixture engineered to match it.
    server_blocked_ids = blocked_channel_ids_from_payload(body)
    already_server_blocked = sorted(channel_ids & server_blocked_ids)
    orphan_config_only = sorted(channel_ids - server_blocked_ids)
    if already_server_blocked:
        log.warning(
            "always_blocked_channel_ids is deprecated. These %d id(s) are already server-side blocked "
            "(SSOT) and can be removed from config.toml: %s",
            len(already_server_blocked),
            already_server_blocked,
        )
    if orphan_config_only:
        log.warning(
            "always_blocked_channel_ids is deprecated. These %d id(s) are in config.toml but NOT server-side "
            "blocked — likely you unblocked them via _control/blocked_channels. This code no longer re-adds "
            "them on startup; either drop them from config.toml, or re-block via `echo <id> > "
            "/views/slack-split/_control/blocked_channels`: %s",
            len(orphan_config_only),
            orphan_config_only,
        )


def _mount_mode(args: argparse.Namespace) -> str:
    """Mount mode: CLI ``--mode`` wins, else ``SLACK_FUSE_MODE`` env, else legacy."""
    cli = getattr(args, "mode", None)
    if cli:
        return str(cli)
    return os.environ.get("SLACK_FUSE_MODE", "legacy")


def cmd_mount(args: argparse.Namespace) -> None:
    """Mount the FUSE filesystem (legacy store-backed or split projections)."""
    if _mount_mode(args) == "split":
        cmd_mount_split(args)
        return
    import pyfuse3
    import trio

    from .api import SlackClient
    from .archive import archive_all
    from .auth import load_tokens
    from .backfill import backfill_all
    from .fuse_ops import InodeInvalidator, SlackFuseOps
    from .store import SlackStore
    from .user_cache import UserCache

    mountpoint = Path(args.mountpoint or _default_mountpoint())
    mountpoint.mkdir(parents=True, exist_ok=True)

    # Clean stale mount if present (e.g. after a crash)
    import subprocess as _sp

    _sp.run(["fusermount3", "-uz", str(mountpoint)], capture_output=True)

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # Quiet httpx request logging — our store already logs what matters
    logging.getLogger("httpx").setLevel(
        logging.DEBUG if args.debug else logging.WARNING,
    )

    tokens = load_tokens()
    client = SlackClient(tokens.user_token)
    users = UserCache(client.http)
    users.populate()

    store = SlackStore(client, users)

    # Preload channel list so first ls is instant
    store.list_channels(kind="channels")

    # Serializes all sync store/API work to a single worker thread so
    # the trio event loop stays responsive and shared state stays safe.
    store_limiter = trio.CapacityLimiter(1)
    ops = SlackFuseOps(store, store_limiter)

    # Wire the invalidation sink so events trigger kernel page-cache drops.
    # Has to happen after ops is constructed so we share its InodeMap.
    store.set_invalidator(InodeInvalidator(ops.inodes, store))

    fuse_options: set[str] = {"fsname=slack-fuse", "ro"}
    if args.debug:
        fuse_options.add("debug")

    def _handle_usr1(signum: int, frame: object) -> None:
        store.force_refresh()

    signal.signal(signal.SIGUSR1, _handle_usr1)

    pyfuse3.init(ops, str(mountpoint), fuse_options)

    backfill_enabled = _env_bool("SLACK_FUSE_BACKFILL", default=False)
    logging.getLogger(__name__).info(
        "Backfill: %s",
        "enabled" if backfill_enabled else "disabled",
    )

    async def _periodic_refresh() -> None:
        while True:
            await trio.sleep(_REFRESH_INTERVAL)
            await trio.to_thread.run_sync(
                lambda: store.list_channels(kind="channels"),
                limiter=store_limiter,
            )

    async def _run() -> None:
        async with trio.open_nursery() as nursery:
            nursery.start_soon(_periodic_refresh)
            nursery.start_soon(archive_all, store)
            if backfill_enabled:
                nursery.start_soon(backfill_all, client, store, store_limiter)
            if tokens.app_token:
                from .socket_mode import run_socket_mode

                nursery.start_soon(
                    run_socket_mode,
                    store,
                    tokens.app_token,
                    client.http,
                    store_limiter,
                )
            await pyfuse3.main()
            nursery.cancel_scope.cancel()

    try:
        trio.run(_run)
    except KeyboardInterrupt:
        pass
    finally:
        pyfuse3.close()


def cmd_mount_split(args: argparse.Namespace) -> None:  # noqa: C901  (process-wiring entrypoint: conns, ops, projector, subscriber)
    """Mount the Sprint-3B split adapter (``SlackFuseOpsV2``) over the local
    projections store, with the projector + health subscriber wired to the
    FUSE-side kernel-cache invalidators.

    This is the integrated process the RFC describes (FUSE mount + projector +
    health subscriber share inode state in one process). Two complementary
    invalidation paths run alongside ``pyfuse3.main()``:

    * The **projector** (``WSClient``) subscribes to the server, applies events
      into the local projections store, and fires a :class:`V2InvalidationSink`
      after each TX commits so live chunk mutations drop the matching primed
      inode (Sprint 3E). Without this the projector ran out-of-process and could
      not reach this process's kernel page cache, so live messages stayed
      invisible behind ``fi.keep_cache=True`` until the polling-TTL floor.
    * The **health subscriber** polls ``connection_state`` / ``stream_caught_up``
      and drops every primed inode on a staleness transition (review P0-2).

    Gated behind ``SLACK_FUSE_MODE=split`` / ``--mode split`` so the legacy
    adapter stays the default per the Phase-4 cutover safety plan.
    """
    import psycopg
    import pyfuse3
    import trio
    from psycopg.rows import TupleRow

    import slack_fuse.migrations as client_migrations
    from slack_fuse.config import load_client_config
    from slack_fuse.fuse_ops_v2 import SlackFuseOpsV2, V2InvalidationSink
    from slack_fuse.migrations.runner import apply_migrations
    from slack_fuse.pg_health import PgHealth
    from slack_fuse.projector.health_subscriber import watch_health
    from slack_fuse.projector.pool import ConnectionPool as ProjectorConnectionPool
    from slack_fuse.projector.trailer_log import TrailerLog
    from slack_fuse.projector.ws_client import SINGLETON_STREAMS, WSClient, WSClientOptions

    config = load_client_config()
    mountpoint = Path(args.mountpoint or config.mountpoint)
    mountpoint.mkdir(parents=True, exist_ok=True)

    import subprocess as _sp

    _sp.run(["fusermount3", "-uz", str(mountpoint)], capture_output=True)

    # Format includes the FUSE-request context fields (req_id, op, inode,
    # path) injected by ``FuseContextFilter``. Outside a callback scope
    # these read as ``-``. The filter is attached to the root handler so
    # every logger benefits — projector, ws_client, helpers, etc.
    from slack_fuse.logctx import FuseContextFilter

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format=(
            "%(asctime)s %(levelname)s %(name)s "
            "[%(req_id)s op=%(fuse_op)s ino=%(inode)s path=%(fuse_path)s] "
            "%(message)s"
        ),
    )
    for handler in logging.getLogger().handlers:
        handler.addFilter(FuseContextFilter())
    log = logging.getLogger(__name__)

    def _open_conn() -> psycopg.Connection[TupleRow]:
        conn: psycopg.Connection[TupleRow] = psycopg.connect(config.database_url)
        conn.autocommit = True
        return conn

    def _open_fuse_pool_conn() -> psycopg.Connection[TupleRow]:
        """Factory for FUSE pool connections.

        Per-conn ``statement_timeout`` caps any single query: a SELECT
        stalled behind WAL fsync contention from the projector aborts at
        postgres instead of holding the FUSE upcall open and queueing every
        subsequent FUSE callback behind it. See BACKLOG for the wedge
        scenario this fixes.
        """
        conn = _open_conn()
        with conn.cursor() as cur:
            _ = cur.execute("SET statement_timeout = '25s'")
        return conn

    # One-time migrations so the projections schema exists.
    migrate_conn = _open_conn()
    try:
        applied = apply_migrations(migrate_conn, Path(client_migrations.__file__).parent)
    finally:
        migrate_conn.close()
    if applied:
        log.info("applied client migrations: %s", ", ".join(applied))

    # One connection per concurrent consumer; sharing a psycopg connection
    # across the FUSE callbacks (worker threads), the health poll loop, the
    # projector bookkeeping, and the invalidation sink would race.
    # (health subscriber opens its own conn inside `_run_health_subscriber`
    # so we can reconnect cleanly after a closed-connection event.)
    fuse_conn = _open_conn()  # dedicated inode-map conn (fallback for non-pool callers)
    state_conn = _open_conn()
    sink_conn = _open_conn()

    # Bounded pool for the FUSE read path. Each callback borrows a conn,
    # runs its SQL under PG ``statement_timeout``, returns it. Replaces the
    # single-conn-with-CapacityLimiter(1) bottleneck that could wedge the
    # whole mount on any one slow callback. Pool size 4 fits comfortably
    # under the local Postgres connection budget (default max_connections=100
    # minus the projector pool of 8 + the four fixed conns above).
    fuse_pool = ProjectorConnectionPool(_open_fuse_pool_conn, max_size=4)

    tz = _resolve_local_zoneinfo()
    store_limiter = trio.CapacityLimiter(1)

    # PG-down tolerance: when the local Postgres vanishes (game-mode on
    # stops claude-hooks-postgres.service, manual restart, crash …) the
    # FUSE callbacks fast-fail with EIO instead of crashing the process,
    # and the mount root surfaces a `NO_POSTGRES` virtual file with the
    # recovery story. A background trio task probes PG to flip back to
    # up; the file disappears the moment PG returns.
    pg_health = PgHealth(_open_conn)

    # Optional per-read trailer-decision JSONL log (bake-in observability).
    trailer_log = TrailerLog.open(config.trailer_log_path) if config.trailer_log_path is not None else None
    if trailer_log is not None:
        log.info("trailer decision log: %s", config.trailer_log_path)

    # Sync httpx client for the ``channel.original.md`` ghost-file fetcher.
    # The fetcher runs in FUSE worker threads (dispatched by ``_run_sync``),
    # so a sync client fits cleanly — no trio context to thread through.
    # Long-lived: one process, connection-pooled.
    from slack_fuse.projector.gaps_fetch import fetch_channel_gaps, fetch_gaps_tsv_bytes, fetch_workspace_gaps
    from slack_fuse.projector.originals_fetch import fetch_originals
    from slack_fuse.projector.probes_fetch import fetch_probes_bytes
    from slack_fuse.projector.refill_fetch import trigger_refill
    from slack_fuse.projector.ws_client import derive_http_base

    # One shared sync httpx.Client for all the ghost-file fetchers (originals,
    # gaps). They run in FUSE worker threads, so a sync client fits.
    ghost_http_client = httpx.Client(timeout=httpx.Timeout(connect=2.0, read=5.0, write=2.0, pool=5.0))
    ghost_base_http_url = derive_http_base(config.server_url)

    def _originals_fetch_sync(channel_id: str, from_epoch: float, to_epoch: float) -> bytes:
        return fetch_originals(
            ghost_http_client,
            ghost_base_http_url,
            channel_id,
            from_epoch=from_epoch,
            to_epoch=to_epoch,
            shared_secret=config.shared_secret,
        )

    def _channel_gaps_fetch_sync(channel_id: str) -> bytes:
        return fetch_channel_gaps(ghost_http_client, ghost_base_http_url, channel_id)

    def _workspace_gaps_fetch_sync() -> bytes:
        return fetch_workspace_gaps(ghost_http_client, ghost_base_http_url)

    # ``_control/`` write surface: trigger server-side refreshes/backfills and
    # mutate server-side block policy over HTTP. Shares the ghost-file httpx
    # client + server origin. The state is in-process (resets on restart).
    from slack_fuse.control import ControlState
    from slack_fuse.projector.block_fetch import (
        blocked_channel_ids_from_payload,
        delete_block_channel,
        get_blocked_channels,
        get_blocked_channels_bytes,
        post_backfill_channel,
        post_block_channel,
    )
    from slack_fuse.projector.probe_fetch import post_probe_sweep
    from slack_fuse.projector.refresh_fetch import post_refresh_channel, post_refresh_channels

    control_state = ControlState()

    _migrate_legacy_always_blocked(
        ghost_http_client,
        ghost_base_http_url,
        frozenset(config.always_blocked_channel_ids),
        shared_secret=config.shared_secret,
        log=log,
    )

    def _control_refresh_workspace() -> int:
        return post_refresh_channels(ghost_http_client, ghost_base_http_url, shared_secret=config.shared_secret)

    def _control_refresh_channel(channel_id: str) -> int:
        return post_refresh_channel(
            ghost_http_client, ghost_base_http_url, channel_id, shared_secret=config.shared_secret
        )

    def _control_blocked_channels_read() -> bytes:
        return get_blocked_channels_bytes(
            ghost_http_client,
            ghost_base_http_url,
            shared_secret=config.shared_secret,
        )

    def _control_blocked_channels_list() -> set[str]:
        status, payload = get_blocked_channels(
            ghost_http_client,
            ghost_base_http_url,
            shared_secret=config.shared_secret,
        )
        if status != 200:
            msg = f"GET /blocked-channels returned HTTP {status}"
            raise RuntimeError(msg)
        return blocked_channel_ids_from_payload(payload)

    def _control_block_channel(channel_id: str, reason: str | None) -> int:
        return post_block_channel(
            ghost_http_client,
            ghost_base_http_url,
            channel_id,
            reason=reason,
            shared_secret=config.shared_secret,
        )

    def _control_unblock_channel(channel_id: str) -> int:
        return delete_block_channel(
            ghost_http_client,
            ghost_base_http_url,
            channel_id,
            shared_secret=config.shared_secret,
        )

    def _control_backfill_channel(channel_id: str) -> tuple[int, str | None]:
        return post_backfill_channel(
            ghost_http_client,
            ghost_base_http_url,
            channel_id,
            shared_secret=config.shared_secret,
        )

    def _control_probe_sweep(job_id: str | None, target: str | None) -> tuple[int, str | None]:
        return post_probe_sweep(
            ghost_http_client,
            ghost_base_http_url,
            job_id=job_id,
            target=target,
            shared_secret=config.shared_secret,
        )

    def _control_gaps_read() -> bytes:
        return fetch_gaps_tsv_bytes(ghost_http_client, ghost_base_http_url)

    def _control_probes_read() -> bytes:
        return fetch_probes_bytes(
            ghost_http_client,
            ghost_base_http_url,
            shared_secret=config.shared_secret,
        )

    def _control_refill_gap(channel_id: str, oldest: float, latest: float) -> str:
        return trigger_refill(
            ghost_http_client,
            ghost_base_http_url,
            channel_id,
            oldest,
            latest,
            shared_secret=config.shared_secret,
        ).result

    # ``_control/rerender_channel`` hands resolved channel ids to a background
    # consumer (``_run_rerender_consumer``) rather than running inline: a
    # snapshot fetch + re-apply is far heavier than the sub-second per-callback
    # budget. The bounded channel turns a flood of writes into ``busy`` rather
    # than unbounded memory growth. The enqueue runs on the trio event loop
    # (called from ``_fire_rerender``), so ``send_nowait`` on the non-thread-safe
    # channel is safe.
    rerender_send, rerender_recv = trio.open_memory_channel[str](64)

    def _control_rerender_channel(channel_id: str) -> bool:
        try:
            rerender_send.send_nowait(channel_id)
        except trio.WouldBlock:
            return False
        return True

    ops = SlackFuseOpsV2(
        fuse_conn,
        tz,
        store_limiter,
        pool=fuse_pool,
        pg_health=pg_health,
        stale_after_s=config.stale_after_disconnect_s,
        trailer_enabled=config.stale_trailer_enabled,
        trailer_log=trailer_log,
        originals_fetch=_originals_fetch_sync,
        channel_gaps_fetch=_channel_gaps_fetch_sync,
        workspace_gaps_fetch=_workspace_gaps_fetch_sync,
        control_state=control_state,
        control_refresh_workspace=_control_refresh_workspace,
        control_refresh_channel=_control_refresh_channel,
        control_blocked_channels_read=_control_blocked_channels_read,
        control_blocked_channels_list=_control_blocked_channels_list,
        control_block_channel=_control_block_channel,
        control_unblock_channel=_control_unblock_channel,
        control_backfill_channel=_control_backfill_channel,
        control_probe_sweep=_control_probe_sweep,
        control_gaps_read=_control_gaps_read,
        control_probes_read=_control_probes_read,
        control_refill_gap=_control_refill_gap,
        control_rerender_channel=_control_rerender_channel,
    )

    # The projector's post-commit sink: maps ChunkRef / ThreadChunkRef /
    # channel-list intents onto V2 inodes and drops their kernel page cache.
    sink = V2InvalidationSink(sink_conn, tz)
    ws_options = WSClientOptions(
        server_url=config.server_url,
        shared_secret=config.shared_secret,
        pool_size=config.projector_pool_size,
    )
    # NB: NOT mounted ``ro`` — the kernel would reject every write before it
    # reached the daemon, including the ``_control/`` triggers. Read-only is
    # enforced in-daemon instead: ``open`` returns EROFS for any write-mode open
    # outside the two ``_control`` trigger files (see ``SlackFuseOpsV2.open``).
    fuse_options: set[str] = {"fsname=slack-fuse"}
    if args.debug:
        fuse_options.add("debug")
    pyfuse3.init(ops, str(mountpoint), fuse_options)

    # Single-slot holder for the current WSClient so block-sync can call
    # ``subscribe_channels`` on it after a server unblock. ``_run_projector``
    # writes the slot before ``client.run`` and clears it on exit; block-sync
    # reads it and skips the notification if unset (mid-reconnect).
    current_ws_client: list[WSClient | None] = [None]

    async def _run_projector() -> None:
        """Supervise the WSClient: reconnect with backoff if it exits.

        Unlike the standalone ``slack-fuse-projector`` (which systemd restarts),
        the in-mount projector must survive transient server outages without
        taking the mount down, so we restart a fresh ``WSClient`` on each exit.
        A fresh client per attempt matters: ``WSClient`` closes its appliers on
        exit and cannot be re-run. While disconnected the read path degrades to
        the staleness trailer via the health subscriber.
        """
        backoff = 2.0
        max_backoff = 300.0
        while True:
            client = WSClient(ws_options, _open_conn, state_conn, sink=sink)
            current_ws_client[0] = client
            try:
                with state_conn.cursor() as cur:
                    cur.execute("SELECT channel_id FROM channels WHERE subscribed = TRUE")
                    per_channel = [f"channel:{row[0]}" for row in cur.fetchall()]
                initial_streams = list(SINGLETON_STREAMS) + per_channel
                await client.run(initial_streams=initial_streams)
            except Exception as exc:  # noqa: BLE001 - supervisor must outlive any WS error
                log.warning("projector exited (%s); reconnecting in %.0fs", exc, backoff)
                await trio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
            else:
                # Clean exit (connection closed): retry promptly, reset backoff.
                backoff = 2.0
                await trio.sleep(1.0)
            finally:
                current_ws_client[0] = None

    async def _run_health_subscriber() -> None:
        """Supervised wrapper around ``watch_health``.

        ``watch_health`` raises on a closed connection (the inner conn can't
        self-heal). On any exit we close the broken conn, sleep with backoff,
        reopen a fresh conn, and resume. Mirrors the ``_run_projector`` pattern.
        """
        backoff = 2.0
        max_backoff = 60.0
        while True:
            # IMPORTANT: connect is INSIDE the try/except so PG-down at
            # connect time (vs. during a query) doesn't escape and kill
            # the process. Caught 2026-06-22 by the break-test harness:
            # when local-postgres goes down before the supervisor's next
            # iteration, ``psycopg.connect`` raises OperationalError that
            # used to propagate to the nursery and take the daemon down.
            conn: psycopg.Connection[TupleRow] | None = None
            try:
                conn = _open_conn()
                pg_health.mark_up()
                await watch_health(
                    conn,
                    ops.invalidate_all_primed,
                    stale_after_s=config.stale_after_disconnect_s,
                )
            except psycopg.OperationalError as exc:
                pg_health.mark_down(reason=f"health_subscriber: {exc}")
                log.warning("health_subscriber connect/query failed (%s); reconnecting in %.0fs", exc, backoff)
                await trio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
            except Exception as exc:  # noqa: BLE001 — supervisor must outlive any DB blip
                log.warning("health_subscriber exited (%s); reconnecting in %.0fs", exc, backoff)
                await trio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
            else:
                # Clean exit (iterations cap hit during a test, etc.) — retry promptly.
                backoff = 2.0
                await trio.sleep(1.0)
            finally:
                if conn is not None:
                    with contextlib.suppress(Exception):
                        conn.close()

    # Background warmer for the gaps ghost-file caches. The FUSE callbacks
    # never block on HTTP (the workspace query alone runs ~2s server-side
    # and would blow the 1s per-callback budget); this task feeds the
    # in-process cache periodically so callbacks just read from it.
    from slack_fuse.projector.gaps_warmer import warm_gaps_periodically

    def _list_known_channel_ids() -> list[str]:
        # Fresh conn per call so we don't contend with FUSE pool conns.
        with _open_conn() as conn, conn.cursor() as cur:
            _ = cur.execute("SELECT channel_id FROM channels WHERE tier != 'blocked' ORDER BY channel_id")
            return [str(r[0]) for r in cur.fetchall()]

    async def _run_gaps_warmer() -> None:
        try:
            await warm_gaps_periodically(
                ops,
                workspace_gaps_fetch=_workspace_gaps_fetch_sync,
                channel_gaps_fetch=_channel_gaps_fetch_sync,
                list_channel_ids=_list_known_channel_ids,
            )
        except Exception as exc:  # noqa: BLE001 — supervisor must outlive any warmer error
            log.warning("gaps warmer exited (%s); not restarting (gaps will fall back to ENOENT)", exc)

    async def _run_rerender_consumer() -> None:
        """Drain ``_control/rerender_channel`` requests and re-render off-budget.

        One channel at a time on dedicated connections (own apply conn + own
        invalidation-sink conn) so the heavy snapshot apply never contends with
        the FUSE read pool, the projector's applier pool, or the live sink's
        connection. ``rerender_channel`` is sync, so it runs in a worker thread;
        the ``async for`` is serial, so only one rerender runs at a time.
        """
        from slack_fuse.projector.rerender import rerender_channel

        rerender_conn = _open_conn()
        rerender_sink_conn = _open_conn()
        rerender_sink = V2InvalidationSink(rerender_sink_conn, tz)

        def _rerender_one(channel_id: str) -> str:
            return rerender_channel(
                ghost_http_client,
                ghost_base_http_url,
                rerender_conn,
                channel_id,
                shared_secret=config.shared_secret,
                sink=rerender_sink,
            ).status

        try:
            async for channel_id in rerender_recv:
                control_state.record_rerender(channel_id, "in_progress")
                try:
                    status = await trio.to_thread.run_sync(_rerender_one, channel_id)
                except Exception as exc:  # noqa: BLE001 — consumer must outlive any one failed rerender
                    log.warning("rerender consumer: channel %s failed (%s)", channel_id, exc)
                    status = "failed"
                control_state.record_rerender(channel_id, status)
        finally:
            with contextlib.suppress(Exception):
                rerender_conn.close()
            with contextlib.suppress(Exception):
                rerender_sink_conn.close()

    async def _run_block_sync() -> None:
        from slack_fuse.projector.block_sync import sync_blocked_channels_periodically

        def _make_http_client() -> httpx.Client:
            return httpx.Client(timeout=httpx.Timeout(connect=2.0, read=5.0, write=2.0, pool=5.0))

        async def _on_newly_subscribed(ids: frozenset[str]) -> None:
            client = current_ws_client[0]
            if client is None:
                # Projector is between runs (reconnect backoff). The initial
                # streams query on the next ``client.run`` will include the
                # unblocked rows because block-sync has already updated the
                # channels table, so nothing is lost.
                return
            await client.subscribe_channels(ids)

        await sync_blocked_channels_periodically(
            _make_http_client,
            ghost_base_http_url,
            _open_conn,
            shared_secret=config.shared_secret,
            interval_s=config.block_sync_interval_s,
            limiter=store_limiter,
            on_newly_subscribed=_on_newly_subscribed,
        )

    async def _run() -> None:
        try:
            async with trio.open_nursery() as nursery:
                nursery.start_soon(_run_health_subscriber)
                nursery.start_soon(_run_projector)
                nursery.start_soon(pg_health.run)
                nursery.start_soon(_run_gaps_warmer)
                nursery.start_soon(_run_rerender_consumer)
                nursery.start_soon(_run_block_sync)
                await pyfuse3.main()
                nursery.cancel_scope.cancel()
        finally:
            # Close pooled FUSE conns inside the trio scope (``aclose`` is
            # async). Idle conns are closed; borrowed ones close as soon as
            # their callback releases them.
            with trio.CancelScope(shield=True):
                await fuse_pool.aclose()

    try:
        trio.run(_run)
    except KeyboardInterrupt:
        pass
    finally:
        pyfuse3.close()
        fuse_conn.close()
        state_conn.close()
        sink_conn.close()
        if trailer_log is not None:
            trailer_log.close()


def cmd_unmount(args: argparse.Namespace) -> None:
    """Unmount the FUSE filesystem."""
    result = subprocess.run(
        ["fusermount3", "-u", args.mountpoint],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"Failed to unmount: {result.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    print(f"Unmounted {args.mountpoint}")


def cmd_resolve(args: argparse.Namespace) -> None:
    """Resolve a Slack permalink to a FUSE path."""
    mountpoint = os.path.expanduser(args.mountpoint)
    if args.server_url:
        try:
            payload = _post_server_json(args.server_url, "/resolve", {"url": args.url})
            relative_path = _required_string(payload, "path")
            print(_absolute_path_from_mount_relative(mountpoint, relative_path))
        except RuntimeError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(2 if str(e) == "not_found" else 1)
        except (ValueError, httpx.HTTPError) as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        return

    from .api import SlackAPIError, SlackClient
    from .auth import load_tokens
    from .resolve import PermalinkResolutionError, resolve_permalink
    from .user_cache import UserCache

    tokens = load_tokens()
    client = SlackClient(tokens.user_token)
    users = UserCache(client.http)

    try:
        path = resolve_permalink(args.url, mountpoint, client, users)
        print(path)
    except PermalinkResolutionError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(2)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except SlackAPIError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        client.close()


def cmd_permalink(args: argparse.Namespace) -> None:
    """Resolve a FUSE path to a Slack permalink."""
    mountpoint = os.path.expanduser(args.mountpoint)
    if args.server_url:
        request_payload: dict[str, str] = {"path": _path_for_server(args.path, mountpoint)}
        if args.ts:
            request_payload["ts"] = args.ts
        try:
            payload = _post_server_json(args.server_url, "/permalink", request_payload)
            print(_required_string(payload, "url"))
        except RuntimeError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        except (ValueError, httpx.HTTPError) as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        return

    from .api import SlackAPIError, SlackClient
    from .auth import load_tokens
    from .permalink import resolve_path_to_permalink
    from .user_cache import UserCache

    tokens = load_tokens()
    client = SlackClient(tokens.user_token)
    users = UserCache(client.http)

    try:
        url = resolve_path_to_permalink(
            args.path,
            mountpoint,
            client,
            users,
            tokens.workspace_url,
            ts=args.ts,
        )
        print(url)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except SlackAPIError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        client.close()


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser. Exposed so tests can assert argument wiring (e.g.
    the split-mount mountpoint default) without invoking the command handlers."""
    from .cli import register_rerender_subcommand, register_tier_subcommand

    parser = argparse.ArgumentParser(
        prog="slack-fuse",
        description="FUSE filesystem for Slack",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    mount_parser = sub.add_parser("mount", help="Mount the filesystem")
    mount_parser.add_argument(
        "mountpoint",
        nargs="?",
        default=None,
        # Default resolved lazily in the command handler so that, in split mode,
        # an unspecified mountpoint falls back to ClientConfig.mountpoint rather
        # than always being overridden by the legacy default (review residual:
        # config.mountpoint was dead because the argparse default always won).
        help=f"Mount point (default: ClientConfig.mountpoint in split mode, else {_default_mountpoint()})",
    )
    mount_parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    mount_parser.add_argument(
        "--mode",
        choices=("legacy", "split"),
        default=None,
        help="Adapter to mount: 'legacy' (store-backed, default) or 'split' "
        "(Sprint-3B projections adapter). Falls back to SLACK_FUSE_MODE.",
    )
    mount_parser.set_defaults(func=cmd_mount)

    unmount_parser = sub.add_parser("unmount", help="Unmount the filesystem")
    unmount_parser.add_argument(
        "mountpoint",
        nargs="?",
        default=_default_mountpoint(),
        help=f"Mount point (default: {_default_mountpoint()})",
    )
    unmount_parser.set_defaults(func=cmd_unmount)

    resolve_parser = sub.add_parser("resolve", help="Resolve a Slack permalink to a FUSE path")
    resolve_parser.add_argument("url", help="Slack permalink URL")
    resolve_parser.add_argument(
        "--mountpoint",
        default=_default_mountpoint(),
        help=f"Mount point (default: {_default_mountpoint()})",
    )
    resolve_parser.add_argument(
        "--server-url",
        help="Proxy to server endpoint (ws://..., wss://..., http://..., or https://...)",
    )
    resolve_parser.set_defaults(func=cmd_resolve)

    permalink_parser = sub.add_parser("permalink", help="Resolve a FUSE path to a Slack permalink")
    permalink_parser.add_argument("path", help="FUSE path (or path under .cached-only/)")
    permalink_parser.add_argument("--ts", help="Specific message ts (required for day files; refines threads)")
    permalink_parser.add_argument(
        "--mountpoint",
        default=_default_mountpoint(),
        help=f"Mount point (default: {_default_mountpoint()})",
    )
    permalink_parser.add_argument(
        "--server-url",
        help="Proxy to server endpoint (ws://..., wss://..., http://..., or https://...)",
    )
    permalink_parser.set_defaults(func=cmd_permalink)
    register_tier_subcommand(sub)
    register_rerender_subcommand(sub)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
