# slack-fuse — Claude notes

Read-only FUSE filesystem exposing a Slack workspace as markdown. Python 3.12, trio + pyfuse3. Split architecture: a **server** ingests Slack and holds the event log; a **client mount** subscribes over a wire protocol and materialises a local projection to disk.

User-facing docs are in `README.md`. This file is for navigating the codebase.

## Architecture in one paragraph

Server pod (k8s): slurper ingests Slack (Socket Mode + Events API webhooks + on-demand backfill) into a PostgreSQL event log ordered by `(stream, offset_in_stream)` per stream key; a trio WS gateway (`slack_fuse_server/wire/`) replays and tails it, redirecting too-far-behind consumers to gzip JSONL snapshots (`slack_fuse_server/http/snapshot.py`). Client mount host (one trio process, `slack_fuse/__main__.py`): `WSClient` subscribes per stream with a capability-negotiated protocol (`Subscribe`/`Unsubscribe`/`Event`/`CaughtUp`/`SnapshotAt`/`ServerCapabilities`); `apply_event` folds events into local PG (`chunks`, `thread_chunks`, `chunk_mentions`, `channels`, `users`) and advances `cursors` in the same TX while bumping the `projection_targets` ledger; the coalescer renders ledger-pending targets to `~/.cache/slack-fuse/projection/`; `SlackFuseOpsV2` serves clean targets from disk with a per-file kernel-cache invalidation post-commit, else JIT from PG, with a staleness trailer classified from health signals.

## Module map

### CLI + process wiring

| File | Responsibility |
|---|---|
| `slack_fuse/__main__.py` | CLI entrypoint: `mount`, `unmount`, `resolve`, `permalink`. `cmd_mount` opens seven durable PG connections (`inode`, `projector_state`, `projector_sink`, `disk_projection`, `rerender_apply`, `rerender_sink`, `block_sync`), plus a bounded pool of four for FUSE reads; wires nursery tasks (WS client, health subscriber, coalescer, PgHealth, block-sync, rerender consumer, warmers). Cancels the scope on unmount. |
| `slack_fuse/cli/rerender.py`, `slack_fuse/cli/tier.py` | Standalone CLIs: rerender a channel out-of-mount (calls the same `snapshot_fetch` + `render_message_structural` path), or set a channel's local `tier`. |
| `slack_fuse/config.py` | `ClientConfig` via pydantic-settings — `SLACK_FUSE_` env prefix, TOML file at `~/.config/slack-fuse/config.toml`, extras ignored. Holds `server_url`, `shared_secret`, `database_url`, `mountpoint`, `projector_pool_size`, `stale_after_disconnect_s`, `stale_trailer_enabled`, `trailer_log_path`, `block_sync_interval_s`, `disk_projection_enabled`. |
| `slack_fuse/auth.py` | Thin: `load_mountpoint()` and `load_workspace_url()` from env → `.env` → `~/.config/slack-fuse/config.json`. The v2 client does NOT hold Slack tokens; only the shared secret to talk to its own server. `load_tokens()` retains for the legacy `slack-fuse permalink` synth path. |

### FUSE surface

| File | Responsibility |
|---|---|
| `slack_fuse/fuse_ops_v2.py` | `SlackFuseOpsV2(pyfuse3.Operations)`. Path-dispatch hubs (`_list_dir_impl`, `_resolve_content_impl`, `_is_dir_impl`) all `# noqa: C901` — inherently big. Ledger-gated read: `_read_from_disk_if_clean` checks `projection_targets` on the callback's borrowed pool conn, reads backing, verifies frontmatter identity, two ENOENTs → JIT. `_control/*` writes accumulate per-fh (fh from `_CONTROL_FH_BASE=1<<48`) and fire on `release` under the callback budget. `V2InvalidationSink` at the bottom translates ledger and health events into `pyfuse3.invalidate_inode` calls off the event loop. `_default_invalidate_inode` swallows benign ENOENT/EBADF but re-raises everything else so failed invalidations keep the target pending. |
| `slack_fuse/fuse_v2_helpers.py` | Path/slug/DB helpers: `assign_conv_root_slugs`, `fetch_channel_by_slug`, `fetch_day_chunks`, `fetch_day_thread_parents`, `dedup_thread_slug_map`, `derive_thread_slug`, `channel_meta_frontmatter`, `day_channel_frontmatter`, `thread_frontmatter`, `ts_to_local_date`, `PersistentInodeMap`. The `borrowed_fuse_conn` ContextVar gives the inode map the per-callback conn. |
| `slack_fuse/inode_map.py`, `slack_fuse/invalidation.py`, `slack_fuse/slug.py`, `slack_fuse/mrkdwn.py` | Small: `InodeMap` typed protocol; `InvalidationSink` protocol; ASCII slug helper; Slack mrkdwn → markdown (`<@U…>`/`<#C…>`/`<url\|label>`/`*bold*`/`_italic_`/`~strike~`/code/blockquotes). |
| `slack_fuse/models.py` | Pydantic frozen models — remaining Slack wire shapes used by the server's originals fetcher + a few JSON types (`JsonObject`, `JsonValue`). Domain: `Channel`, `Message`, `Reaction`, `FileAttachment`, `Edited`, `Thread`. Response wrappers under a `_SlackResponse` base. |
| `slack_fuse/user_cache.py` | `UserCache`: bulk-fetches workspace users (server-side); provides `get_display_name(user_id)`. Persists to disk. Kept alive on the client side because `slack_fuse_server/http/resolve.py` also imports it. |

### Projector (client-side subscriber + applier + coalescer)

| File | Responsibility |
|---|---|
| `slack_fuse/projector/ws_client.py` | `WSClient`: opens `wss://server/ws`, exchanges `Hello`/`Capabilities`, subscribes per stream, receives `Event`/`CaughtUp`/`SnapshotAt`. `SubscriptionState` per channel (`PENDING`/`ACTIVE`/`FAILED`), full-desired-set reconcile every block-sync cycle so subscribes survive ambiguous COMMIT. Snapshot redirects run in a nursery task, apply via `snapshot_fetch`, guarded by a per-subscription token so a mid-flight snapshot cannot resurrect a retired subscription. Wire capability handshake: sends `UnsubscribeFrame` only when the server advertises support, otherwise falls back to controlled reconnect on set-shrink. |
| `slack_fuse/projector/per_stream.py` | `StreamApplier`: per-stream unbounded queue, one-at-a-time apply loop. Apply failure poisons the stream (tear down, replay from durable cursor). |
| `slack_fuse/projector/apply.py` | `apply_event(conn, frame, projection, tz)` — dispatch per event kind (message/message_changed/message_deleted/channel_added/renamed/archived/unarchived/user_added/renamed/profile_changed/member_joined/left/etc.). Cross-stream mention resolution (`chunk_mentions`, `thread_chunk_mentions`) inside the same TX. Bumps `projection_targets` ledger from `ApplyResult` before commit. `SELECT tier FROM channels ... FOR UPDATE` at the top serialises with block/unblock. |
| `slack_fuse/projector/snapshot_fetch.py` | `fetch_and_apply_snapshot`: GET the server's JSONL snapshot for a stream at an offset, `SELECT applied_offset ... FOR UPDATE` on the cursor row, refuse if the DB has advanced past `at_offset` (stale snapshot), else DELETE absent + upsert present + advance cursor + bump targets — all in one TX. Guards against wiping the live `(snapshot, head]` tail. |
| `slack_fuse/projector/rerender.py` | Re-apply the server's latest snapshot for one channel via `apply_snapshot_row` → the current `render_message_structural`. Upsert-only (no delete-absent, no cursor advance) so live-apply and rerender can't race destructively. |
| `slack_fuse/projector/reconnecting_conn.py` | `ReconnectingConnection`: psycopg wrapper. Mid-transaction `OperationalError` propagates (no silent partial-commit); reconnect happens between transactions. Emits structured `reconnect_recorded` events with `failure_phase` (`execute`/`fetch`/`commit`/`outside-tx`), `commit_outcome=unknown` for COMMIT failures, `attempt_result` at the end of the whole retry. One event per whole operation. |
| `slack_fuse/projector/projection_ledger.py` | `projection_targets` schema helpers + `bump_targets`, `bump_channel_visibility_targets`, `is_target_clean`, `target_key_for_path`, `targets_for_apply_result`, `RENDERER_VERSION`. Schema: `(target_kind, channel_id, local_day, thread_ts)` with `NULLS NOT DISTINCT` uniqueness; kinds `channel-meta`, `day`, `thread`, `layout` (singleton). Reader gate: renderer match ∧ `rendered_generation ≥ target_generation` ∧ layout singleton clean. |
| `slack_fuse/projector/disk_projection.py` | Coalescer. Startup: `reconcile_startup` walks the projection root, bumps every stale-renderer row, ensures targets for on-disk identities. Tick loop: `reconcile_layout` → `discover_pending` → `flush_dirty` under `_flush_lock`. Per-target flush: read generation, render from stable `TargetKey`, resolve mutable path only now, `_atomic_write_bytes` (tmp + `os.replace`), sweep stale thread aliases by frontmatter identity, invalidate_path, CAS `mark_target_rendered ... WHERE target_generation = %s`. A bump during render makes the CAS miss, key requeued, intermediates never written. Non-benign invalidation failures keep the row pending; failed paths remembered per key so deleted aliases aren't forgotten. |
| `slack_fuse/projector/coalescer.py` | Trio task wrapper: 5-second tick, batch 200, calls `DiskProjection.reconcile_startup` / `reconcile_layout` / `flush_dirty`. |
| `slack_fuse/projector/dirty_set.py`, `slack_fuse/projector/_control_cache.py`, `slack_fuse/projector/cursor.py` | Scheduling caches + control-body cache + cursor advance helper (`GREATEST`-based). |
| `slack_fuse/projector/block_sync.py` | Every 30s (config): GET `/blocked-channels`, apply to local `channels` table with `_force_blocked_manual` (bumps `channel-meta` + `layout` targets in the same TX), returns `VisibilityChanges { newly_subscribed, newly_blocked }`. Fires `V2InvalidationSink` for the newly-blocked subtree. Also runs a full desired-set reconcile against `WSClient.subscribe_channels` every cycle for ambiguous-COMMIT resilience. |
| `slack_fuse/projector/health_subscriber.py` | Trio task, 1-second polling of `connection_state.last_slurper_health`, `stream_caught_up`, `connection_state.last_frame_at`. Signature = `(last_slurper_health, frame_stale, caught_up_count, max_offset)` — edge-triggered so raw timestamp jitter doesn't storm the kernel. On signature change, invalidates every materialised inode via `V2InvalidationSink`. |
| `slack_fuse/projector/trailer.py`, `trailer_log.py` | Staleness classifier: `auth_failed` / `disconnected` / `degraded` / workspace `last_frame_at` > 60s / per-stream not caught up. Renders the `\n---\n> ⚠ Content may be stale ...\n---\n` appended block; `st_size` includes it. Optional JSONL log of per-read trailer decisions for FP debugging. |
| `slack_fuse/projector/pool.py` | Bounded per-callback FUSE conn pool (default 4). Replaces the earlier `CapacityLimiter(1)` bottleneck. |
| `slack_fuse/projector/gaps_fetch.py` / `gaps_warmer.py` / `channel_stats_fetch.py` / `channel_stats_warmer.py` / `originals_fetch.py` / `refresh_fetch.py` / `refill_fetch.py` / `block_fetch.py` / `probes_fetch.py` / `probe_fetch.py` | Sync httpx clients + trio background warmers for the server's read-only ghost-file surfaces (`_workspace/channels.md`, `_gaps/*`, `_probes/*`, `channel.original.md`) and the mutating control endpoints (`/refresh-channels`, `/refresh-channel`, `/blocked-channels`, `/backfill-channel`, `/refill-gap`, `/probe-sweep`). Warmers keep results in the in-process `_BytesCache`; FUSE callbacks read cached bytes only, never perform HTTP. |

### Control surface + health

| File | Responsibility |
|---|---|
| `slack_fuse/control.py` | `ControlState`: thread-safe latest-outcome-per-action JSON blob. HTTP-status → verb map (`result_for_status`: 202→`queued`, 409→`busy`, 401/403→`unauthorised`, 503/0→`server_unavailable`, else `http_<code>`). |
| `slack_fuse/pg_health.py` | `PgHealth`: fast-fails FUSE callbacks with EIO when local PG is unreachable. Probes 5s down / 60s up. Materialises `NO_POSTGRES` at the mount root (with recovery text) whenever down. |
| `slack_fuse/logctx.py` | Request-scoped `req_id`/`op`/`inode`/`path` via ContextVar filter for `projector-span`-style logs. |
| `slack_fuse/channel_stats.py` | Pure markdown renderer for the sorted `_workspace/channels.md` inventory. |
| `slack_fuse/resolve.py` | Forward: `slack-fuse resolve <slack-url>` → FUSE path via local v2 projections. Pure local. |
| `slack_fuse/permalink.py` | Reverse: FUSE path → Slack permalink URL. v2 — synthesizes URLs locally from `SLACK_WORKSPACE_URL` (matching `chat.getPermalink`'s output shape); no Slack API call. Frontmatter `channel_id`/`thread_ts` preferred; falls back to slug reversal via `fetch_day_thread_parents` + `dedup_thread_slug_map`. |

### Migrations

`slack_fuse/migrations/`: `0001_init.sql` (channels, users, chunks, thread_chunks, chunk_mentions, thread_chunk_mentions, cursors, connection_state, stream_caught_up, inodes, active_messages view), `0002_block_sync.sql`, `0003_server_block_sync_prior_tier.sql`, `0004_projection_targets.sql` (the ledger).

`slack_fuse_server/`: separate namespace. Wire (`wire/frames.py`, `wire/server.py`, `wire/tail.py`, `wire/subscriptions.py`), HTTP (`http/snapshot.py`, `http/resolve.py`, `http/dto.py`, `http/server.py`), slurper (`slurper/`), backfill (`backfill/`), probes (`probes/`).

## Conventions

Follows `~/docs/dev/python/` (uv, basedpyright strict, ruff preview, Pydantic at I/O boundaries, frozen models, lazy CLI imports).

- **Python 3.12**, `from __future__ import annotations` everywhere.
- **basedpyright strict** with `reportUnusedCallResult`, `reportImplicitStringConcatenation`, `reportUnannotatedClassAttribute`, `reportUnknownLambdaType` off. Target: `0 errors, 0 warnings, 0 notes`. Use `# pyright: ignore[reportAttributeAccessIssue]` for the pyfuse3 attribute assignments that don't have stubs (already in `fuse_ops_v2.py`).
- **ruff** with `E,F,W,I,UP,B,SIM,RUF,BLE001,C901,PLR0913,PLR0916,PLR0917,PLR1702,E402,PLC0415`, `line-length=120`. `__main__.py` has a per-file `PLC0415` ignore because the CLI defers heavy imports. The three path-dispatch hubs in `fuse_ops_v2.py` carry `# noqa: C901`.
- **Pydantic at the I/O boundary** — every Slack response is `model_validate`d in `slack_fuse/models.py` (or in the server slurper). `dict[str, Any]` does not leak. Wire-format quirks use `AliasPath`, `BeforeValidator`, `populate_by_name` declaratively; small typed `model_validator(mode='before')`s only for truthy or cross-field cases.
- **`JsonObject`** (from `models.py`) is the recursive JSON type for opaque pass-through. Never `dict[str, Any]` or `dict[str, object]`.
- **Frozen models** for everything — no in-place mutation.
- **trio**, not asyncio.
- **`_api_call(callable)` on the server** — pass a method reference or lambda; the wrapper preserves the return type via `TypeVar`. No string dispatch.
- **No business logic in `fuse_ops_v2.py`** beyond path parsing, dispatch, and the tier-decision path — Slack data goes through the projector.
- **Renderers are pure**: they take models + a user resolver, return bytes. No I/O.
- **No external I/O inside `conn.transaction()` blocks** — no HTTP, network, or file reads while holding DB locks.
- **Slurper PG operations are bounded by session `lock_timeout` / `statement_timeout`.** Treat `psycopg.errors.LockNotAvailable` and `psycopg.errors.QueryCanceled` as recoverable: backfill/catchup retry once then propagate.

## Health concepts (distinct, do not conflate)

The codebase has several "health" signals. They are orthogonal; never collapse them into one boolean.

- `/health` HTTP endpoint on the server — proves only that the dispatch HTTP task can answer. Kubelet readiness probe; no Slack/DB ingestion claim.
- `slurper-health` event stream — Slack-side ingestion + backfill observations (`slack_healthy`, `slack_degraded`, `socket_mode_*`, `backfill_*`, `auth_failed`).
- `connection_state.last_slurper_health` — client projection of selected `slurper-health` kinds for trailer classification (`healthy`/`degraded`/`disconnected`/`auth_failed`).
- Slurper task liveness — `/livez` reads the in-memory task supervisor's latest phase + deadline per long-running task; models scheduler progress, not data flow.
- Slurper span logs — stdout `slurper-span op=... task=... result=... duration_ms=... limiter_wait_ms=... sync_ms=...`; operation-level evidence for slow/failing/wedged sync hops.
- Client trailer `staleness_reason` — from `connection_state.last_frame_at`, WS state, and per-stream `stream_caught_up`. Known quiet-stream false positives live in `BACKLOG.md`.
- Client `cursors.updated_at` / `applied_offset` — per-stream apply progress for reconnect resume.
- `PgHealth` / `NO_POSTGRES` — local projector Postgres reachability. Cluster server/slurper may be fine while FUSE reads fail fast.
- `reconnect_recorded` events on `ReconnectingConnection` — per-conn wedge/recovery with `failure_phase` + `commit_outcome`. A PG bounce fans out to up to 7 events, one per durable conn wrapper.

When asking "is the slurper healthy?", name the observable: serving HTTP, ingesting Slack, task scheduler not wedged, client data current, restart-looping, or local PG reachable.

## Events vs operator policy

The server `events` table is reserved for facts that happened upstream in Slack: messages, edits/deletes, channel metadata changes, membership, and health observations. Operator intent is mutable policy and does **not** belong in replayable Slack event streams. Channel blocks live in the server-side `blocked_channels` table (`channel_id`, `blocked_at`, `reason`) and are propagated to clients by periodic block sync, not by `channel_blocked` / `channel_unblocked` events.

Query-derived Slack facts form a third category: Slack told us the value when asked, but did not push an event. Per-channel `search.messages` counts live in the refreshed `channel_message_totals` table. Never add `channel_total_refreshed` events; failed refreshes retain the last known count and change only `refresh_status` / `refreshed_at`.

## Workspace inventory (`_workspace/channels.md`)

`_workspace/channels.md` is a read-only, total-descending inventory of every visible channel: Slack search total, locally ingested `message` event count, derived status, membership, and creation date. The server refreshes source totals every six hours at 3.5 seconds between Tier-2 calls; `slack-fuse-server refresh-channel-totals` runs the same sweep once. The client warmer fetches authenticated `GET /channel-stats`, renders markdown, and fills an in-process 10-minute cache every five minutes. FUSE lookup/getattr/read never call the endpoint; a cold or expired cache is temporarily ENOENT-like until the warmer succeeds.

## Events source envelope (`events.source`)

Every event row has a `source` jsonb column carrying **ambient facts about the ingestion transaction**: `producer`, `boot_id`, `task_id`, `run_id`, `slack_cursor`/`prior_cursor`/`page_index`/`has_more`/`final_page`, `commit`/`image_digest`, `api_endpoint`/`api_latency_ms`/`slack_request_id`, `span_id` (joins Loki `slurper-span` lines), `triggered_by` (`startup | scheduled | reconnect | control-surface | admin-cli`). Slack facts stay in `payload`; `source` is invisible to the wire protocol and clients.

**Guardrail**: `source` may NOT carry derived state — running counters, aggregate flags computed across events, or state that answers "what work remains". Ambient facts stay; derived state belongs in views or is computed on read.

## Dev commands

```bash
uv sync
uv run slack-fuse mount --debug
uv run ruff check .
uv run ruff format .
uv run basedpyright
uv run pytest
```

Use `uv add` / `uv remove` for deps — never `uv pip install` against this project's env.

## Runtime gotchas

- The systemd unit's `ExecStart` points at `.venv/bin/slack-fuse` (editable install). `uv sync` after `git pull` picks up the code; `systemctl --user restart slack-fuse` cycles it.
- `SLACK_WORKSPACE_URL` is needed for `slack-fuse permalink` URL synthesis. `.env` next to the checkout is read by `slack_fuse/auth.py::load_workspace_url()`.
- `fusermount3` must be on PATH. `__main__.py`'s `ExecStartPre` calls `fusermount3 -uz` on startup; a manual `fusermount3 -u ~/views/slack` is the escape hatch.
- **No SIGUSR1 handler in v2** (the v1 wiring went with the island delete `3c7ef8b`). Programmatic force-refresh is a follow-up via `_control/refresh_all` (not yet built).
- **No `.cached-only/` prefix in v2** — it was a v1 mechanism to suppress Slack API calls at read time. v2 has no direct-API path from the mount, so the concept is meaningless.

## Wire protocol (client ↔ server)

Frames (`slack_fuse_server/wire/frames.py`): `Hello`, `HelloAck`, `Subscribe{stream, since}`, `Unsubscribe`, `ServerCapabilities`, `Event{stream, offset, kind, ts, payload}`, `CaughtUp{stream, head_offset}`, `SnapshotAt{stream, at, url}`, `Error{code, stream, head_offset}`, `Ping`/`Pong`.

Capability handshake: server advertises supported optional frames (currently `unsubscribe`); client sends `UnsubscribeFrame` only when advertised, else falls back to controlled reconnect on desired-set shrink.

Server behaviour: replay ≤ 5,000 events inline, else `SnapshotAt` redirect (only for `channel:` streams). Per-connection nursery isolates any exception to one socket.

Client behaviour: per-stream unbounded queue, one-at-a-time apply loop; apply failure poisons the stream (tear down, replay from durable cursor). Reconnect backoff 2s → 5min.

## Control surface (`_control/`)

A Plan-9-style ctl/status surface at the mount root, wired in `cmd_mount` (`SlackFuseOpsV2`). Per-fh writes buffer (64 KiB cap → EFBIG), fire on `release` under the callback budget (15s for `_control/*`, 1s default). Never fails `release`. Verbs from `control.py::result_for_status`. Latest-outcome-per-action JSON at `_control/status` (0o444).

- `_control/refresh_channels` (0o644) — workspace sweep. Routed to server.
- `_control/refresh_channel` (0o644) — single-channel refresh. Slug→id resolved locally; hidden allowed.
- `_control/blocked_channels` (0o644) — read: fresh GET from server SSOT. Write: toggle server block policy.
- `_control/backfill_channel` (0o644) — POST `/backfill-channel/{id}`. Server-side block records `blocked` instead of queueing.
- `_control/refill_gap` (0o644) — write `<slug-or-id> <oldest_ts> <latest_ts>` per line; batches accepted.
- `_control/probe_sweep`, `_control/probe_sweep_job`, `_control/probe_sweep_target` (0o644) — trigger probe sweeps.
- `_control/rerender_channel` (0o644) — client-local rerender. Enqueued on a bounded(64) trio channel → `busy` when full; consumed by `_run_rerender_consumer` on dedicated conns off the FUSE/projector pools. Rerender mechanics in `slack_fuse/projector/rerender.py` — upsert-only (no delete-absent, no cursor advance).
- `_control/gaps` (0o444), `_control/probes` (0o444) — read-only status surfaces populated by warmers.
- `_control/status` (0o444) — read JSON of last outcomes.

## Things not to do

- Don't commit `.env`. Gitignored; double-check `git status` before every commit.
- Don't add asyncio. The mount loop is trio-native; mixing event loops will deadlock pyfuse3.
- Don't remove the `# pyright: ignore` comments on pyfuse3 attribute assignments — they're load-bearing because pyfuse3's stubs are incomplete.
- Don't drop the invalidation-sink wiring in `cmd_mount`. Without it, `fi.keep_cache=True` in `SlackFuseOpsV2.open()` lets the kernel serve stale buffered bytes after live events land, so new messages won't appear until a broader invalidation fires.
- Don't re-add the `ro` mount option to `cmd_mount` (it's intentionally only `{"fsname=slack-fuse"}`). `ro` makes the kernel reject every write before it reaches the daemon, killing the `_control/` triggers. Read-only is enforced in-daemon: `SlackFuseOpsV2.open` returns `EROFS` for any write-mode open outside the declared writable `_control/*` files.
- Don't call `pyfuse3.invalidate_inode` from the request-serving thread/loop. It can wait on kernel page locks and deadlock against in-flight FUSE reads (2026-06-24 `folio_wait_bit_common` scar). Invalidate only from worker threads via `V2InvalidationSink`.
- Don't add a Python `notify_store` path. Same wedge class.
- Don't skip `renderer_version` on ledger writes. It's part of the reader gate; a bump to `RENDERER_VERSION` in code invalidates every projected file until re-rendered.

## Related docs

- `README.md` — user-facing setup, configuration, filesystem layout.
- `BACKLOG.md` — active work items (Ratified / Agent-raised) + `Resolved` history with commit refs.
- `~/docs/slack-fuse.md` — operator notes for this machine specifically (systemd, paths, recovery commands).
