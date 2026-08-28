# slack_fuse — client mount package

The FUSE surface, its path grammar, process wiring, and the small CLIs. Live-data machinery is one level down in `projector/` (see its own AGENTS.md).

## WHERE TO LOOK

| Task | Location |
|---|---|
| Add/alter a mount path | `fuse_ops_v2.py::_list_dir` / `_is_dir` / `_resolve_decision` (three C901 dispatch hubs) |
| Path → DB query | `fuse_v2_helpers.py` (`fetch_day_chunks`, `fetch_day_thread_parents`, `fetch_channel_by_slug`) |
| Directory/thread naming | `fuse_v2_helpers.py::assign_conv_root_slugs`, `derive_thread_slug`, `dedup_thread_slug_map` |
| New `_control/` verb | `fuse_ops_v2.py::_fire_control` + a `projector/*_fetch.py` client + `control.py::result_for_status` |
| Process startup / task wiring | `__main__.py::cmd_mount` |
| New config key | `config.py::ClientConfig` (pydantic-settings, `SLACK_FUSE_` prefix, TOML at `~/.config/slack-fuse/config.toml`) |
| Kernel cache misbehaviour | `fuse_ops_v2.py::V2InvalidationSink` (bottom of file) |
| Local PG down handling | `pg_health.py` → `NO_POSTGRES` ghost at mount root |

## CODE MAP

| Symbol | Location | Role |
|---|---|---|
| `SlackFuseOpsV2` | `fuse_ops_v2.py:570` (2623 LOC file) | Every FUSE callback, control writes, ghost files, disk/JIT gate, trailers |
| `V2InvalidationSink` | `fuse_ops_v2.py:2419` | Ledger/health refs → `pyfuse3.invalidate_inode`, worker-thread only |
| `PersistentInodeMap` | `fuse_v2_helpers.py` | Inode identity across restarts, backed by `inodes` table |
| `cmd_mount` | `__main__.py:186` | Opens durable conns + pool, starts every nursery task |
| `ControlState` | `control.py` | Latest-outcome-per-action, serialised at `_control/status` |
| `PgHealth` | `pg_health.py` | 5s-down / 60s-up probe, fast-fails callbacks with EIO |
| `FuseContextFilter` | `logctx.py` | ContextVar `req_id`/`op`/`inode`/`path` for `projector-span` log lines |

## Ghost files (lookup-only, warmer-backed)

`_workspace/channels.md`, `_gaps/*`, `_probes/*`, `channel.original.md`, `NO_POSTGRES`, `.ignore`. The `projector/*_warmer.py` tasks poll the server and fill an in-process cache; **callbacks read the cache or nothing**. A cold or expired cache is temporarily ENOENT-like — that is correct behaviour, not a bug to fix by adding a fetch.

`_workspace/channels.md` specifically: server refreshes source totals every 6h (3.5s between Tier-2 calls); the client warmer GETs `/channel-stats` every 5min into a 10-minute cache. Mode bits matter — writable `_control/*` are 0o644, read-only status surfaces 0o444.

`_control/rerender_channel` enqueues on a **bounded(64)** trio channel (full → `busy`) consumed by `_run_rerender_consumer` on connections dedicated off the FUSE and projector pools. There is no `_control/refresh_all`; force-refresh has no implementation in v2.

## ANTI-PATTERNS

- **`fi.keep_cache = True` without invalidation wiring.** `open()` sets it; drop the sink in `cmd_mount` and the kernel serves stale bytes forever.
- **`notify_store`.** It was removed because it deadlocked against the in-flight read even from a worker thread (`fuse_ops_v2.py:2218-2250`). Do not reintroduce, in Python or otherwise.
- **Re-adding the `ro` mount option.** The kernel would reject `_control/` writes before they reach the daemon. Read-only is enforced in-daemon: `open()` returns EROFS for write-mode opens outside the declared writable set.
- **Removing `# pyright: ignore[reportAttributeAccessIssue]` on `fi.*` / `entry.st_ino` assignments.** pyfuse3 3.4.2 ships incomplete stubs; these are load-bearing (~14 sites).
- **HTTP inside a FUSE callback.** Ghost files read the in-process cache only; warmers do the fetching (`__main__.py:636-645`).
- **Sync DB work on the trio loop.** Goes through `_run_sync` with both a PG statement timeout and a trio callback budget (`fuse_ops_v2.py:733-750`); pool release is shielded or four leaked slots wedge the mount.
- **Attribute caching on mutable paths.** Only immutable past-day files may cache; directories and today's files keep timeout 0 (`fuse_ops_v2.py:348-356`).
- **Business logic in `fuse_ops_v2.py`.** Path parsing, dispatch and the tier decision only — Slack data goes through the projector.

## Legacy / shared surfaces (do not treat as v2 client code)

`models.py`, `user_cache.py`, `mrkdwn.py` are kept alive for **`slack_fuse_server`**, not for the mount. `inode_map.py` / `invalidation.py` are stale protocol stubs — the live ones are `PersistentInodeMap` and `projector.apply.InvalidationSink`. `schema.sql` is an unreferenced snapshot; migrations run from `migrations/runner.py`. The active renderer is `slack_fuse_render.render_message_structural`, never `mrkdwn.convert`.

## Notes

- `cli/rerender.py` cannot invalidate a running mount's kernel cache — that path only exists in-mount via `_control/rerender_channel`.
- Cross-module private imports (`_resolve_channel_id`, `_resolve_local_zoneinfo`, `_default_tier`) are deliberate and pyright-suppressed at each site.
- No SIGUSR1 handler exists in v2, despite the comment in `slack-fuse.service`.
