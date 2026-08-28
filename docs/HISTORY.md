<!--
Mined from Claude Code session 3fb23ba4-b24f-4a62-822c-9ec6b69c5f8c (6,439 turns, 2026-04-10 → 2026-08-28)
by an 11-way parallel digest + synthesis pass on 2026-08-28. It is the transcript's account,
not an audit of the code: treat turn cites as provenance, not proof. Where it disagrees
with the source tree, the source tree wins.
-->

# slack-fuse — project history mined from session 3fb23ba4

Source: one 6,439-turn Claude Code session, 2026-04-10 → 2026-08-28. Turn cites in `[n]`.

## Arc

- **v1 release-readiness hardening [1]–[88].** Four external adversarial reviews before public release; findings triaged into verified vs hallucinated, 14 issues fixed across four phases, eight commits, first push to `origin/main`. Big themes: task-local `cached_only_mode`, `CapacityLimiter(1)` around all sync work, persistent `httpx.Client`, atomic disk-cache writes.
- **Owner mode, forward `resolve`, activity-based thread freshness, memory crisis [95]–[223].** Binary parent-date thread caching replaced with activity TTL; RSS hit 1.08 GB then 857 MB/2 GB peak; fixed by LRU caps *and* per-day archive `.done` markers (allocator churn, not live objects).
- **Socket Mode push ingestion [240]–[362].** Outbound websocket, no public ingress. Event-sourced snapshot-plus-log merge instead of invalidate-and-refetch. First implementation looked correct but `fi.keep_cache=True` served stale bytes — birth of `InvalidationSink`/`InodeInvalidator`.
- **Reverse permalinks, `/views/` migration, dormant DMs [363]–[623].** `channel_id` frontmatter, `SLACK_WORKSPACE_URL`, `SLACK_FUSE_MOUNTPOINT`; empty-DM/`known_dates` pollution fixed.
- **v2 architecture RFC: server split [642]–[840].** Event-sourced server + CQRS client read models, PostgreSQL both sides, pre-rendered markdown chunks, late mention resolution, hot/hidden/blocked tiers, synchronous per-event apply with TCP backpressure. Sprint plan 0–4 on a fresh `server-split-rebuild` worktree, no pushes to origin.
- **Sprint 0–3 rebuild [841]–[1142].** Contract freeze → vertical slice → fan-out → FUSE convergence. Dual-vendor (GPT-5.5 xhigh + Gemini 3.1 Pro) critical review became a mandatory gate; first 3B FUSE adapter was rejected outright.
- **Bake-in, k8s deploy, bulk legacy-cache backfill [1143]–[1957].** systemd units replace tmux; server + Postgres deployed to k8s-homelab via GHCR digest pin and Flux; NFS-mounted legacy cache ingested by cluster Jobs; Slack Event Subscriptions + Delayed Events enabled; 156k+ messages loaded.
- **FUSE survivability + ghost files [1958]–[2809].** Health-subscriber stall, host-level FUSE wedge hunt, connection pool, `PgHealth`/`NO_POSTGRES`, structured `logctx` logging, destructive break-test harness; `channel.original.md` and `gaps.md` ghost files; discovery that Pydantic `model_dump` persistence was lossy.
- **Lossless raw event sourcing + observability program [2810]–[3459].** `Validated[T]`, `Attachment` rendering, corrective `message_changed` events, `_control/` Plan 9 surface, `blocked_channels` policy table, full wipe-and-reingest, split sync gates, `TaskSupervisor`/`/livez`, `slurper-span` + Loki.
- **Restart-safe backfill and probe sweeps [3460]–[3962].** Three adversarial review cycles kill a work-derivation spec; page-atomic writes, `active_messages`/`active_thread_parents` views, `events.source` envelope, then reversal to a dedicated `backfill-run:<channel>` stream. Probe sweeps + `refill-window` recover 51 gap windows.
- **Slack Events API webhook cutover [3963]–[4663].** Socket Mode dropped self-join deliveries; exclusive cutover to Tailscale-Funnel webhooks with an HMAC-verified durable `slack_event_inbox`. Adversarial "Fable" audit of all of v2 lands 11 of 18 findings.
- **Flow host, OOM hunt, legacy retirement, ratified plan [4664]–[5374].** Client deployed to `flow-crastinator` over Tailscale; h11 HEAD crash loop; `MALLOC_ARENA_MAX=2`; legacy polling mount retired and `/views/slack` unified onto v2; BACKLOG restructured into Ratified/Agent-raised/Resolved and executed as phased hard-gated handoffs.
- **Coalesced disk projection D1/D2/D3 + WTF audit + PG ledger [5375]–[6124].** Disk writer → read-side tier logic → deterministic race tests; the mandated WTF audit found four correctness bugs; in-memory dirty state replaced by the `projection_targets` PG materialization ledger across four reviewed PRs.
- **v1 island deletion, Rust engine co-design, host migration, final OOM outage [6125]–[6439].** 12 v1 modules + 9 tests deleted after `permalink` migrated to local URL synthesis; interface-first design of a generic projection→VFS engine in Rust; daily driver moved pro → flow; server pod OOMKilled 1,658 times in 10 days, mitigated to 3Gi.

## Design decisions that still bind

- **Event-sourced server, CQRS client.** Server PostgreSQL `events` is the append-only source of truth; clients hold projections (`chunks`, `thread_chunks`, `channels`, `users`) and `cursors`, never their own event log. Rejected: a local client event log. [690]–[692]
- **PostgreSQL both sides, not SQLite.** User rejected file-backed DBs ("just kind of annoying"), wanted containerised/configurable placement and one operational engine. [655]–[657]
- **Server holds the only Slack credentials.** Clients get events; there is no direct-API path from the mount. This is why v2 has no `.cached-only/` and no client token. [650], [692], [6194]
- **Pre-rendered per-message markdown chunks with late mention resolution.** New-message insert is the dominant case; edits update one row by PK. Stored chunks keep `<@U…>` IDs; display names resolve at presentation. Rejected: typed messages rendered on read. [655]–[657], [699]
- **Events are Slack facts; operator intent is policy.** Blocks live in a mutable server `blocked_channels(channel_id, blocked_at, reason)` table, synced periodically — not `channel_blocked`/`channel_unblocked` events. Query-derived facts (`search.messages` totals) are a third category, in `channel_message_totals`. [3060]–[3070], [3144]–[3155], [3566]
- **Raw persistence: `Validated[T]` (frozen `raw` + `model`).** Pydantic validates but does not get to destroy fields. Rationale, verbatim: "the point of event sourcing models is you can go back and say this piece of information that we weren't using, but we've been capturing since the beginning of times, now useful". [2810]–[2844]
- **`events.source` carries ambient ingestion facts only** — producer, boot/task/run/span IDs, cursor, page index, commit/image digest, `triggered_by`. Guardrail: no derived state, no counters, no "what work remains". [3618]–[3626]
- **`projection_targets` PG ledger is the materialization source of truth.** Keyed by stable data identity `(target_kind, channel_id, local_day, thread_ts)`, gated on renderer version + `rendered_generation ≥ target_generation` + layout singleton, with CAS on flush. `_dirty`/`_inflight` demoted to scheduling caches. Rejected: cursor sidecars (too coarse, missed cursor-neutral writes). [5906]–[5914], [6012]–[6016]
- **Disk bytes are trailer-free; FUSE composes the staleness trailer live.** `getattr` sizes include the composed trailer (canary: 122B disk + 91B trailer = 213B). [5492]–[5493]
- **Invalidation never runs on the trio/FUSE event loop.** Always via worker threads (`V2InvalidationSink`), because `invalidate_inode`/`notify_store` can block on kernel page locks. [2482]–[2488], [4287]–[4309]
- **FUSE callbacks are fast or EIO.** "all other cases should either EIO in under a second or work" — 1s default budget (later 0.5s), 15s for `_control/*`, propagated via `_current_callback_budget` ContextVar into nested pool/worker guards. [2253]–[2255], [4077]–[4087], [6330]
- **Health signals stay orthogonal.** `/health` (HTTP serving), `/livez` (task liveness), `slurper-health` (Slack ingestion), client trailer (projection freshness), `PgHealth` (local PG). Never one boolean. [3260]–[3267], [3326], [6210]
- **Hidden ≠ dotfile.** Hidden entries keep their names, are omitted from `readdir`, and remain reachable by `lookup`/`cat`/`resolve`. Archived channels default to `hidden`, not `blocked`. Rejected: a fourth `archived` tier. [661]–[663], [1450]–[1454]
- **Read-only is enforced in-daemon, not by the `ro` mount option** — `ro` would kill `_control/` writes before they reach the daemon. [4037]–[4059]
- **Plan 9 ctl/status control surface.** `_control/*` writes buffer per file handle and fire on `release()`, so `echo x > file` is one operation; latest-outcome-per-action JSON at `_control/status`. Rejected: sysfs/procfs framing. [2907]–[2911]
- **Trio everywhere, never asyncio.** pyfuse3 is trio-native; asyncio's executor would have the same thread/allocator pathology. Sync psycopg/httpx stays behind `trio.to_thread`. [4766]–[4769]
- **Slack push events are internal-stream-filtered.** `backfill-run:` streams are rejected with `STREAM_NOT_FOUND` at the wire server; clients never see lifecycle bookkeeping. [3955]
- **Custom WS protocol stays for mount clients.** NATS/JetStream output is additive interop only — KV/latest-records cannot express feed epochs, priority sync, or currentness semantics. [6054]–[6056], [6127]

## Reversals and rewrites

- **v1 monolith → v2 server/client split [642]–[1142].** v1 was one process holding Slack tokens, TTL polling, disk JSON cache, and in-memory Socket Mode event logs. Abandoned because: duplicated API traffic across devices, no durable replay, disconnects lost data, no cross-device coherence. Replaced by an event-sourced server + per-machine PostgreSQL projection. Cutover was env-gated coexistence (`SLACK_FUSE_MODE`) rather than the originally-planned Phase-4 deletion [697], [708]; the v1 island survived until [6194], kept alive first by `_slug_helpers.py`/`permalink`, then deleted (12 modules, 9 tests) once `permalink` became local URL synthesis.
- **Socket Mode → Slack Events API webhooks [4476]–[4620].** Socket Mode silently dropped a real `member_joined_channel` for `incident-85`. Slack will not deliver HTTP callbacks while Socket Mode is on, so parallel dual-run was impossible; an *exclusive* cutover shipped with a ~10s gap (deliberately unreconciled — late night, minimal traffic). Socket Mode retained as rollback code, server task disabled via `SLACK_FUSE_SERVER_SOCKET_MODE_ENABLED=false`.
- **Binary parent-date thread caching → activity-based TTL [135]–[141].** Threads locked forever the day after the parent posted, so a thread started Apr 7 froze on Apr 8 while replies continued. Replaced with TTL from `latest_reply`: <1h → 60s, <24h → 10min, <7d → 5% of age (30min floor), ≥7d → infinite.
- **LRU cache caps → archive `.done` markers [173]–[221].** Bounding `_day_cache`/`_thread_cache`/`_render_cache` did not stop RSS reaching 857 MB — `archive.py` reparsed ~353 MB of day JSON every ten minutes and pymalloc retained arenas. The fix was to stop the allocation, not cap the survivors. RSS → ~36 MB.
- **Telemetry-based lag detection → synchronous per-event apply [735]–[740].** `cursor_state`/`head_pulse`/heads-in-pong were challenged by the user as "healing a non-robust system". Replaced by apply-and-commit-before-next-receive with TCP backpressure, making steady-state projector lag structurally unreachable. `caught_up` demoted to informational.
- **`catchup_window_s` time-window trailer → boolean `stream_caught_up` [1244]–[1280].** `caught_up` is a one-time per-connection transition, not a heartbeat, so a time window marked every long-lived stream stale. Rejected alternative: periodic server `caught_up` frames (64–128 frames/sec for ~320 channels × 2 clients). Net −104 LOC.
- **`events.source` as backfill completion state → dedicated `backfill-run:<channel>` stream [3923]–[3955].** The source envelope's `final_page` quietly became a second logical stream in the same row ("event sourcing with two schemas in one row"). User verdict: "Pure ES is less chance of breaking things. A is 'clever' in the bad way." Replaced with `backfill_run_started` / page-commit / terminal lifecycle events, written in the same transaction as the data page.
- **Work-derivation-from-SQL spec → page-atomic writes [3502]–[3617].** The first spec (`MIN(payload->>'ts')` history anchor, count-based thread completion) got a Codex NO-GO with 13 data-loss findings; v2 got FIX-AND-GO; the third targeted the wrong functions (`_paginate_history`/`_expand_threads` are catchup helpers, backfill uses `_history_batches`/`_reply_batches`). What survived: page atomicity, `active_messages`, `active_thread_parents`, `is_valid_slack_ts`.
- **In-memory dirty set → PG materialization ledger [5906]–[6016].** Cursor sidecars were rejected by sol review as too coarse and blind to cursor-neutral writes (rerender, block, tier). Landed as four reviewed PRs: reconnect safety, ledger dual-write, reader cutover, compatibility cleanup.
- **FUSE passthrough → coalesced disk projection [3452]–[3459], [4999]–[5000].** `FOPEN_PASSTHROUGH` needs `CAP_SYS_ADMIN`, pyfuse3 3.4.2 has no binding, and passthrough would not remove `getattr`/`readdir` daemon cost anyway. Replaced by rendering to `~/.cache/slack-fuse/projection/` and serving clean targets from disk.
- **`notify_store` in the read path → removed entirely [2504]–[2524].** Moving it to a worker thread only relocated the deadlock; the kernel held the page lock for the in-flight read response. `keep_cache=True` already populates cache from the response.
- **`count(*) FROM active_messages` → raw lifetime event count [6094]–[6100].** The "improved" folded count made `/channel-stats` take 261,821 ms by EXPLAIN and starved the tier-2 pacer into a crashloop. Emergency revert `c87572e`.
- **Codex `/views/slack` hardcode → `SLACK_FUSE_MOUNTPOINT` [440]–[493].** Rejected as an OSS regression, then recognised as correct for the local migration; final form is env → `.env` → `~/.config/slack-fuse/config.json` → `~/views/slack`.

## Scrapped, deferred, and forgotten

### Scrapped (deliberately killed)

- zrok / public callback ingress for Slack events — Socket Mode is outbound-only. [253]–[254]
- `slack_sdk` — stayed on `trio-websocket` to avoid asyncio baggage. [251]
- `auth.test` + on-disk workspace-domain discovery — replaced by explicit `SLACK_WORKSPACE_URL`. [397]–[400]
- `docs/socket-mode-research.md` as durable docs — facts folded into `CLAUDE.md`, `docs/` gitignored. [329]–[332]
- `cold` tier in v1 visibility model — avoided a second lazy-read path; seams kept. [660]–[669]
- `request_backfill_window` client wire command — backfill is an admin/server operation. [711]
- Global head pulses / client cursor-state telemetry. [737]–[740]
- `caught_up_max_at` in the health signature — `CaughtUpFrame` restamped it per message, causing once-per-second invalidation storms. [2460]–[2469]
- Table-growth as a slurper liveness metric — "table growing is not a sane health metric"; quiet workspaces break it. [3260]–[3262]
- The claim of a periodic 30s Socket Mode heartbeat — code inspection found only connect/reconnect emission. [3263]–[3267]
- `thread_reply_count_probed` — subscribed `message.*` already covers old-thread replies. [3415]–[3419]
- Deleting the lossy Linear event to repair it — append-only requires a corrective. [3020]–[3025]
- `channel_blocked`/`channel_unblocked` events. [3060]–[3070]
- `backfill_cursor_advanced` / `backfill_thread_done` as Slack-facts events — "events are Slack facts, not slurper bookkeeping". [3563]–[3568]
- `search.messages` as an authoritative count — filters, proximity collapse, poor DM/private/MPIM coverage. [3513], [3528]
- Autonomous probe→refill consumer for Phase 2 — manual `_control/refill_gap` first. [3963]–[3965]
- Renaming `/gaps` and `/gaps/<id>` — live clients depend on them; new routes named `/gap-candidates`, `/probe-status`. [3980]–[3983]
- Re-POSTing deprecated `always_blocked_channel_ids` at startup (it silently re-blocked `metrics` after a manual unblock); the whole dead `always_blocked` apply-path was deleted, not rewired. [4169]–[4184], [4329]–[4345]
- A second Slack app to run webhook and socket in parallel — rejected as ceremony + duplicated token surface. [4503]
- `slack_fuse_poc_b/` renderer-split POC, its two worktrees, and the orphaned byte-equivalence test. [4981]–[5044]
- The dead `channel_message_count` probe — deleted rather than reconciled; registry kept with `register_fact_probes()` returning `()`. [5837]–[5847]
- Background `sleep` tasks as a waiting mechanism — killed at turn boundaries. [5382]–[5384]

### Deferred (explicitly postponed, still wanted)

- FUSE hardening backlog from the v1 review: `inode_map.forget()`/inode reclamation, readdir snapshotting per open handle, `fi.direct_io`, cache path-segment sanitization, restrictive cache permissions, nuanced HTTP 403 handling. [9], [16]
- Channel-list invalidation debouncing (membership events produced 4 refetches in ~100 ms). [339]
- Lazy-cold subscriptions + automatic tier transitions from access counters. [667]–[669]
- Multi-user/GDPR/data-deletion; Prometheus (`/metrics` is plain JSON); TLS in-server (terminate at Caddy/Tailscale/nebula). [717], [731]
- Huddles parity in split mode — either implement or document legacy-only. [1512]
- Server-side block filtering (blocks are still a client projection filter; blocked channels' events keep accumulating in cluster PG). [1893], [1715]
- Extra dedup indexes for `message_changed`, `message_deleted`, reactions, rename, membership after Delayed Events was enabled. [1756]–[1759]
- Event-sourced tier overrides via `manual_tier_set` on a `client-overrides` stream — `slack-fuse tier` still writes `channels` directly, which is replay-unsafe. [2005]–[2006], [2100]
- Full PG-restart supervision for `state_conn`/`sink_conn` (only `health_subscriber` was hardened at the time). [2205]–[2207]
- Destructive-harness scenarios `clean_sigterm` and `sigkill_daemon` — systemd state made them fragile. [2320]–[2322]
- Events API via Tailscale Funnel with delayed retries — backlogged `7c602b3` (later shipped). [3549]–[3551]
- Pre-2026-06-27 lossy `model_dump` payload repair — targeted rerender only if rendering problems surface. [3513], [3640]–[3643]
- `GIT_COMMIT` / `SLACK_FUSE_SERVER_IMAGE_DIGEST` wiring into the k8s manifest — source provenance rows have empty commit. [3657]
- Kubelet readiness probe migration `/health` → `/livez`. [3373], [3379], [3541]
- Native-async psycopg/httpx rewrite (~60 `run_transaction()` call sites). [4766]–[4769]
- `tracemalloc(nframes=5|15)` in production — 15 frames caused a liveness crash loop, 5 frames benchmarked at 10.2× p99. [4807]–[4843]
- Process-wide aggregate marker for the 7 per-connection `client_wedged`/`client_recovered` events. [5037], [6083]
- `~/bin/game-mode` ordering (stop `slack-fuse.service` before local PG) — outside the repo, explicitly non-autonomous. [5132], [5155], [6079]
- `fsync` of projection files and parent dirs — `_atomic_write_bytes` is process-crash safe, not host-crash safe. [6081]
- Malformed-frontmatter startup repair — currently fails closed and waits for a later invalidation, can strand a cold target on JIT forever. [6081], [6286]
- Real cancellation of in-flight snapshot HTTP/apply on unsubscribe (`trio.CancelScope`); only a tail check exists. [6081], [6286]
- Maintained `channel_message_counts` counter table to replace the emergency raw-count revert. [6099], [6120]
- pyfuse3 `interrupt()` support — 3.4.2 has no `Operations.interrupt`, no request unique, no `fuse_req_interrupt_func` bindings; needs upstream change or a fork. [6354]–[6357]
- Root-cause memory profiling for the server OOM cadence — 3Gi is mitigation only. [6408]–[6409], [6439]
- Rust-engine items: PyO3 adapter (declared seam only), NFS/9P presenter, engine-side pluggable name normalizer, hidden-tier `NodeFlags::listed` semantics, precise per-node bumps for `user_profile_changed`. [6215]–[6247]

### Trailed off (raised, never resolved, nobody closed the loop)

- **`mpim_joined`** was not found among Slack's subscribable events; no replacement was ever identified. Died at [257]–[258].
- **Whether server and client PostgreSQL should share one physical database or merely one engine** — left at "separate roles/URLs". Died at [650], [655].
- **Snapshot cadence tuning** (default every 5,000 events or daily) and **backfill abort threshold** (20,000, a "configurable guess") were both left as measurement tasks with instrumentation and never measured. Died at [734], [751], [798].
- **`previous_message` capture in `socket.py`** — discussed for edit history, then dropped as "existing event timeline is sufficient"; no change landed. Died at [1796]–[1809].
- **Automated tests for `message-history.sh`** — explicitly declined, only live smoke tests. Died at [1815].
- **A `coverage` subcommand for `message-history.sh`** — offered, never built. Died at [1823].
- **Cluster-side `channels` materialization** — operator reports still join against `channel_added` event payloads instead of a table. Raised [1869]–[1873]; a server-side VIEW landed at [2069]–[2100], but the operator tooling was never revisited.
- **Four inaccessible channels** (`conversations.info → channel_not_found` for cached channels the token lost access to). Proposed fix: synthesize minimal metadata from cache. Filed to BACKLOG at [1840]–[1856], never implemented.
- **Host-wide FUSE kernel wedge root cause** — the `fuse_dev_write → __filemap_get_folio_mpol.cold → folio_wait_bit_common` stack appeared on unrelated mounts (a `claude` process stuck 2 days, a `bat` process stuck hours). Kernel tracing/dmesg/upstream investigation proposed, never done. Died at [2222]–[2224]; partially reframed at [4999]–[5000] as host workflow ordering, but never proven.
- **Channel-create-to-first-event gap detection** was later partially solved by `Channel.created` [2836], but **mid-day gaps, thread-reply gaps, and days outside the rolling 30-day window remain undetected** by the day-presence probe. Flagged at [3765]–[3771], never closed.
- **Catchup mid-stream gap** — a post-reconnect message advances `MAX(ts)` past an outage window, so the next catchup query skips the interior forever. Identified at [3513], [3541], [3681]–[3683]; a rolling-window `conversations.history` catchup was *proposed* at [6439] but not built.
- **`message_replied` / `parent_replied` subtype** — zero observed rows; likely user-token Socket Mode cannot receive that subtype. "Bot-token or Events API work may be needed". Died at [3661]–[3662].
- **A `message_not_found` probe sample** was classified as a benign delete race, but *the missing `message_deleted` event itself* was flagged as a concern and never chased. Died at [3820]–[3827].
- **Stop-hook pending-agent-message delivery** — the hook repeatedly claimed pending messages but never injected their content; attributed to text-only turns failing to mark messages delivered. Investigated at [3908]–[3921], no fix.
- **v2 messaging sender attribution loss** — a Flow message arrived from `UNKNOWN`/`ffffffff-…`; noted, no ping sent, never chased. Died at [4748]–[4750].
- **Possible self-leave `channel_removed` event** — suggested before self-join shipped; the implementation instead writes `channel_member_left` + `channel_member_changed(is_member=false)`. Died at [4406]–[4440].
- **FUSE applier-stall root cause** — instrumentation (queue depth, `acquire_ms`/`sync_ms`, `seconds_since_last_apply`) shipped instead of a fix; suspects were `_tail_lock` contention, pool starvation, notification batching. Died at [4240]–[4252].
- **`/channel-stats` server query optimization** — only the client timeout hotfix (5s → 30s, `801af2a`) ever landed; the query later caused the production crashloop. [5245]–[5253], [6085].
- **Boot-time `QueryCanceled` in `catchup` and `channel_older_than_oldest`** — parked as watch-only task #16. [4838]–[4846], [5117].
- **C-extension memory leak (~12 MB/h after `MALLOC_ARENA_MAX=2`)** — candidates psycopg/libpq buffers, pydantic-core Rust allocation, zlib, long-lived connection state. Backlog task #17, never worked. [4862], [5117]. Almost certainly the same defect as the 1,658-restart OOM outage at [6408].
- **Trailer-decision JSONL writer stopped writing after 2026-08-03** even though the configured path was still logged at boot. Flagged at [6083], never diagnosed.
- **Trailer NULL-at-mount false positives** — `connection_state.last_frame_at` starts NULL and is treated as stale until the first frame. Known quiet-stream FPs also live in BACKLOG. [3184]–[3192], [6083].
- **Residual `/health` 1 ms ↔ 2 s alternation** after the crashloop fix — "another intermittent starvation/PG contention source", undiagnosed. [6114]–[6117].
- **`test_resume_plan_fast_at_scale[1000|5000]`** failed 5/5 at 0.542–0.630 s against a 0.5 s ceiling — reclassified from flake to real regression on pro-crastinator, then never resolved. [6085].
- **FUSE vs direct-projection `rg` throughput gap** — ~12–25 files/s through FUSE vs ~62,000 files/s reading the projection tree directly. Offered as a Ratified backlog entry against ADR expectations at [6155]; no resolution recorded.
- **The paper mount** proving `channels/<slug>/channel.md` against the six Rust traits was proposed at [6203]/[6206] and never recorded as done.
- **The final platform event-architecture RFC response** was drafted, previewed, and re-requested at [6124]; no acceptance or merge is recorded. [6050]–[6053], [6129]–[6135].

## Incident log

- **Trio loop freeze from sync I/O.** Synchronous `httpx` + `time.sleep` on the trio loop froze the whole filesystem during API pagination; all four external reviews found it independently. Fix: `trio.to_thread.run_sync` behind a shared `CapacityLimiter(1)`. [9], [27]–[28], [86]
- **False-complete `.threads.done`.** Archive thread phase slept after a 429, continued without retrying, then wrote the marker — permanently hiding missing replies. Fix: boolean completeness return from `_thread_backfill_channel`. [9], [57]–[60]
- **RSS 1.08 GB → 857 MB / 2 GB peak.** Unbounded caches, then archive reparse churn. Fix: LRU caps + per-day `.done` markers → ~36 MB. [171]–[221]
- **Kernel page cache served stale bytes.** Socket Mode merge was correct but `cat` never re-entered FUSE (`fi.keep_cache=True`). Fix: `InvalidationSink` + `pyfuse3.invalidate_inode`. [292]–[315]
- **"System down" after a mistaken revert.** Codex's `/views/slack` default was reverted, leaving the service on an empty `~/views/slack`; consumers reported outage. Fix: `SLACK_FUSE_MOUNTPOINT` precedence. [442], [475], [493]
- **Empty-DM ratchet.** `[]` API responses were persisted as 2-byte day files and added to `known_dates`, so today's transient date became permanent on read; dormant duplicate accounts squatted bare slugs (`jacob-segal` → dormant `D0AKQ5DS0FQ` instead of active `D0A9ER27T7Y`). Fix: `has_any_messages()`, `_is_dormant_dm()`, filter before slug dedup. Verified 28 → 27 channels. [558]–[623]
- **asyncio under trio.** Sprint 1B's WS tail used `psycopg.AsyncConnection`; connections failed immediately. Sprint 1F rewrote `tail.py` trio-native. [992]–[997]
- **Post-Sprint-3 P0 connection exhaustion.** One psycopg connection per stream would blow `max_connections=100` at 320+ channels. Fix: bounded pool. [1197], [1211]
- **Snapshot apply was upsert-only**, so deleted rows survived; a second review found empty snapshots skipped deletion entirely and snapshot deletions emitted no invalidation refs. [1197], [1219], [1226]
- **Event-sourcing cursor violation.** Applier exception handling logged and returned, advancing past a failed event. [1197], [1206]
- **`stream_not_found` flood.** Client subscribed to ~395 empty channel streams; server dropped every subscription, so future events would never arrive until reconnect. Fix: `since=0` live-only subscriptions emitting `CaughtUp(head=0)`. [1394]–[1410]
- **FUSE kernel deadlock, D-state PID 2614202.** Server dead + read in flight. Recovery: lazy unmount + abort FUSE connection 101, releasing five waiting requests. User: "DONT READ FROM IT you will freeze". [1377]–[1388]
- **Docker CrashLoopBackOff.** Non-root `slackfuse` could not create `/app/.cache/slack-fuse` for `UserCache`. Fix: Dockerfile pre-creates and chowns to UID 10001. [1595]–[1598]
- **Socket Mode delivered zero events** on both local and cluster servers — Slack Event Subscriptions were simply not enabled. After enable/reinstall, event 22353 arrived and projected E2E. [1745]–[1753]
- **NFS mount timeout** — first cluster Job stuck in ContainerCreating; UFW denied NFS. Opened TCP 2049 + TCP/UDP 111 from node `10.0.0.6` only. [1677]–[1690]
- **Health-subscriber hot-spin on a closed connection**, ~2 hours, projection silently stalled, ~32k events backlogged. Fix: `_run_health_subscriber()` owns its connection + reconnect; restart restored ~115 events/s. [1958]–[1987]
- **Host-level FUSE wedge.** Daemon blocked in `fuse_dev_write → __filemap_get_folio_mpol.cold → folio_wait_bit_common`; downstream `cat`/`head`/`bat`/`claude` waited. Three explanations retracted in turn (swap-in pressure — only 30 MB swapped; `vm.swappiness=150`/`dirty_ratio=0` — actual byte thresholds were normal; leaving `game-mode` stopping PostgreSQL under live FUSE consumers as the only defensible trigger). Root cause never found; a sysfs-abort watchdog was built instead. [2103]–[2240]
- **`NO_POSTGRES_INODE=2` collided** with the DB identity sequence starting at 2. Changed to `(1 << 53) - 1`. [2264]–[2266]
- **`psycopg.connect()` outside the exception handler** crashed the whole daemon when PG was down at connect time. [2312]–[2319]
- **`pool.acquire()` outside `trio.fail_after`** let four slots be held while later callbacks waited 115 seconds. [2299]–[2301]
- **`notify_store` circular wait** — ran before the read response was sent, taking a page lock the kernel still held. Removed from the read path. [2504]–[2524]
- **Gaps endpoint 503** — `dispatch.serve_connection` accepted `gaps_deps` but never forwarded it to `serve_http_connection`. The identical bug recurred for `refresh_deps`. [2733]–[2747], [2900]–[2905]
- **Missing Linear message.** Two independent defects: `attachments[]` content dropped by `model_dump` persistence, and the original `message` event absent entirely because slurper downtime exceeded Slack's Socket Mode buffer. Fixes: `Attachment` + `_render_attachment`; startup/reconnect `conversations.history?oldest=` catchup. [2931]–[2982]
- **Stale lossy duplicate won the projection.** Event `405897` (later, lossy `message`) overwrote richer `message_changed` data at the same ts because projection folds by offset, and dedup blocked ordinary re-backfill. Repaired append-only by corrective `message_changed` event `407010`. [3014]–[3048]
- **Destructive wipe rollback** — the client TRUNCATE batch named nonexistent `stream_subscriptions`, rolling back every preceding truncate. [3128]–[3131]
- **FUSE EIO after a PostgreSQL restart** (twice, on consecutive days). The projector pool handed out cached psycopg handles killed by the admin restart. Fix `c2254bd`: `SELECT 1` pre-ping + discard/recreate dead handles in the same slot. [3170]–[3178], [3425]–[3443]
- **Silent slurper wedge, 11h46m.** All event production stopped at ~00:32 UTC while `/health` stayed green. Attributed first to an `OffsetWriter` limiter, then narrowed by Codex to the process-wide shared sync gate serializing Slack HTTP, SQL, file reads and event writes. Previous-container logs were missing, so the exact blocked task was never proven. Fix: split sync gates + pooled/per-task `OffsetWriter`. [3243]–[3278], [3352]–[3359]
- **`dust` reported 0B** — correct `st_size` but `st_blocks=0`. Fix: `st_blocks = (size + 511) // 512` (`83e79a1`). [3193]–[3201], [4973]–[4980]
- **`proj-cloud` triple backfill.** Repeated killed runs left ~19,773 messages with no `backfill_completed` marker, so later runs re-walked; 232 `write_message` spans in 5 minutes produced zero new rows. [3491]–[3501]
- **Server crash from an HTTP client disconnect** — `trio.BrokenResourceError` in `_send_response` killed the whole process; 43 restarts over 32 hours. Fix `91047b1`. [3736]–[3752]
- **`find_resume_plan()` crashed the server** — `_known_thread_parents()` hit the 30 s statement timeout against `active_thread_parents`. Fix `dc45399`: direct event queries + cast-free partial indexes. [3704]–[3729]
- **Node `talos-b0d-k40` DiskPressure** evicted ~248 slack-fuse-server pods → 503s. Escalated to the homelab owner. [3673]–[3680]
- **In-pod refill OOM** — CLI refills competed with the slurper under a 512 MiB PG limit; raised to 1Gi, and the 51 refills were ultimately run locally against port-forwarded PG (recovered 364 top-level + 541 replies). [3802]–[3813]
- **`ChannelNotFoundError` crash loop** — auto-backfill hitting a channel the token lost access to propagated through the nursery. Compounded because `SLACK_FUSE_SERVER_BACKFILL=true` had drifted into meaning "rolling deploy-loss protection". [3834]–[3871]
- **`_control/gaps` request storm.** FUSE's `getattr`/`lookup`/`open`/`read` cascade issued 5+ `/gap-candidates` requests per `cat`, each a ~2 s SQL query, starving `/health` into Traefik 503s. Fixes: `st_size=0` control files, TTL cache, per-path callback budgets. [4018]–[4087]
- **Overnight mount wedge invisible to the watchdog** — 7 queued kernel FUSE requests while the daemon sat in `epoll_wait`/S-state. First-week watchdog evidence: 13 lines, all "service not running", zero detections. Fix: second oracle `/sys/fs/fuse/connections/<id>/waiting`. [4098]–[4119]
- **`metrics` channel invisible** — server `blocked_channels` held `C09HV6S5KUH`; unblocking didn't help because the WSClient's desired stream set was frozen at startup; restarting re-blocked it via the deprecated `always_blocked_channel_ids` migration POST; and a failed `ls` left a kernel negative dentry. Four stacked failures. [4161]–[4205]
- **Auth rollout broke the client** — `snapshot_fetch.py`/`originals_fetch.py` omitted `x-slack-fuse-secret`, causing 401s and projector reconnect backoff until `a78fa60`. [4365]–[4373]
- **h11 HEAD crash loop.** HEAD requests made `_send_response` send a body h11 rejects; the exception escaped the trio nursery and kubelet restarted the pod (≥3 times). Fixes: per-request containment `3eb2aa2`, then root fix `e1d784e` (omit `h11.Data` for HEAD, pass `request_method` through the 400 path). Misdiagnosed first as a Tailscale reachability failure — Flow's probe had used HEAD while 8,844 GETs succeeded. [4701]–[4750]
- **Daily server OOMKill at 1 GiB.** Diagnosed as glibc arena fragmentation (multiple 30–65 MB anonymous arenas, 16 MB Python heap). `MALLOC_ARENA_MAX=2` cut growth 34 → 12 MB/h — explicitly shipped as a diagnostic pin, not a fix. Production `tracemalloc.start(15)` then caused a 5–10 min liveness crash loop; a 6-hour traced run went 465 → 749 MB with tracked Python bytes *shrinking*, proving tracemalloc's own overhead polluted the experiment. [4753]–[4862]
- **Flow silent wedge, 3.7 days.** systemd active, process alive, projection dead: `block_sync.py:169` kept using a connection closed by a NixOS-rebuild PG bounce, logging 165 reconnect failures. Fix: `ReconnectingConnection` across five (later seven) fixed connections, `client_wedged`/`client_recovered` events, real fault injection. [4930]–[5044]
- **`/channel-stats` crashloop, 304 → 385 restarts over ~28 h.** The `count(*) FROM active_messages` change made a 147 KB response take ~99.6 s in-pod (261,821 ms by EXPLAIN); `refresh_channel` waited 91.7 s on `SlackTierPacer`, `/health` shared the loop, kubelet SIGKILLed. The client warmer called it every 5 min with a 30 s timeout while server-side work continued for minutes. Fix `c87572e` + k8s `47e356c` (readiness 15 s, liveness 30 s). [6085]–[6116]
- **Final outage: 1,658 OOMKills over 10 days, exit 137, 1 Gi limit.** Initially misread as dead Tailscale ingress (`100.85.173.107` port-unreachable, 100% ICMP loss) — but the proxy path was alive and only the backend absent. Mitigated to 3Gi (`6e15f32`). Recovery exposed a genuinely missing message in `simon-yells-at-bots/2026-08/27`; channel-specific backfill returned `busy`, then 28 events, then the message. Non-message metadata events had advanced the channel cursor without creating a day chunk, masking the gap. [6399]–[6439]

## Invariants and hazards learned the hard way

- **Never call `pyfuse3.invalidate_inode` or `notify_store` from the request-serving loop.** It waits on kernel page locks and deadlocks against in-flight reads. Taught by the 2026-06-24 `folio_wait_bit_common` wedge [2482]–[2524], re-taught when snapshot-fetch invalidation reintroduced it synchronously [4287]–[4309].
- **Never mount with `ro`.** The kernel rejects writes before they reach the daemon, killing `_control/`. Read-only is enforced by `SlackFuseOpsV2.open` returning `EROFS`. [4037]–[4059]
- **Kernel page cache can serve stale bytes indefinitely** with `keep_cache=True`; the invalidation-sink wiring in `cmd_mount` is load-bearing. Taught by the first Socket Mode "working" test that was actually reading cache. [292]–[315]
- **Validation-time testing can be fooled by the thing you're testing.** Restarts, `SIGUSR1`, TTL expiry and `posix_fadvise` all drained or bypassed the state under test. Validate page-cache behaviour with an inotify/stat canary, plain reads, and a cache-bypass differential — inside the TTL window, no restarts. [292]–[315]
- **`caught_up` is a one-time per-connection transition, not a heartbeat.** Any time-window interpretation marks every long-lived stream stale. [1251]–[1255]
- **Never dual-write derived state at ingest.** "poking an update into the table on recieve feels like violating ES" — server `channels` and `health_log` became VIEWs over `events`. Projections in the same process/DB are fine; the same *table* is not. [1992]–[2006], [2447]–[2451]
- **Append-only means append-only.** Repair a lossy row with a corrective `message_changed`, never a DELETE or UPDATE. Taught by the Linear repair. [3020]–[3048]
- **Pydantic validates; it must not destroy.** Persist `Validated[T].raw`, not `model_dump()`. Taught by Linear's body living in `attachments[]`. [2810]–[2844], [2938]–[2940]
- **Pages must commit atomically.** Reversed oldest-first pages plus a mid-page crash advances a timestamp anchor past unwritten messages. Taught by the NO-GO review of the work-derivation spec. [3513], [3572]–[3574]
- **A completion marker's absence must be cheap to recover from.** Killed backfill runs left partial data with no `backfill_completed`, causing full re-walks. [3496]–[3501]
- **Health is never one boolean.** A single channel's `backfill_large` once made the whole workspace `degraded`; hence `BACKFILL_WARN_LARGE`. Health-transition events are edge-triggered, so a stale `degraded` marker survives until the next transition. [2019]–[2024], [1753]–[1755]
- **FUSE metadata callbacks must never do network or slow I/O.** `getattr`/`lookup` fire on every path component; `_control/gaps` turned one `cat` into 5+ two-second SQL queries. Expensive views get warmers + `st_size=0`. [2765]–[2776], [4018]–[4038]
- **Expensive views must never be hidden behind `readdir`.** `channel.original.md`, `gaps.md` and friends are direct-lookup-only so a recursive walk cannot detonate them. [2551]–[2560]
- **Fixed long-lived psycopg connections must survive a PG bounce.** Pre-ping cached pool handles; wrap every durable connection in `ReconnectingConnection`; propagate mid-transaction `OperationalError` rather than retrying inside an open transaction (silent partial commit). [3436]–[3443], [5030]–[5039], [5914], [5973]
- **"systemd active" is not "working".** A silent wedge with a green `/health` is worse than a crash, because supervision stays satisfied while users read stale data. [4931]–[4934]
- **D-state is not the only wedge shape.** A daemon in `epoll_wait` with queued kernel requests looks perfectly healthy; watch `/sys/fs/fuse/connections/<id>/waiting` too. [4098]–[4119]
- **Don't ship allocator tuning as a root fix.** "if we Changing the arena it means you've done something absolutely terrible, but sure, ship your fix and let's see how it breaks things." `MALLOC_ARENA_MAX=2` was kept only as an empirically load-bearing diagnostic. [4770]–[4780], [4861]–[4862]
- **Don't profile with tracemalloc in production.** 15 frames = liveness crash loop; even at low frames its own bookkeeping dominates the signal you're measuring. [4807]–[4862]
- **Aggregate views over full event history are a latency landmine.** `count(*) FROM active_messages` folded all history per request and starved the shared tier-2 pacer into a crashloop. [6085]–[6099]
- **A permalink resolving is not proof the projection exists.** The resolver is schematic and local. Taught by the final outage's missing message. [6386]–[6390]
- **Non-message metadata events advance a channel cursor without creating a day chunk** — cursor progress does not prove content materialised. [6433]–[6436]
- **Never `tmux kill-server`.** It kills every session on the host; kill individually with ~15 s between kills. [6021]–[6025]
- **Don't read a wedged mount.** "DO NOT TOUCH THE POISON PILL DIRECTLY." [2051]–[2057], [1377]

## Vocabulary

- **`active_messages` / `active_thread_parents`** — SQL views folding base `message`, corrective `message_changed`, and `message_deleted` tombstones into current state, with validated timestamps. Expensive to aggregate over. [3572]–[3574]
- **Ambient facts** — the only thing `events.source` may carry: producer, boot/task/run/span IDs, cursor, page index, commit, `triggered_by`. Contrast *derived state*. [3620]–[3626]
- **`backfill-run:<channel>`** — internal event stream (`backfill_run_started`, page-commit, terminal) recording backfill lifecycle; never exposed to clients. [3944]–[3955]
- **`blocked_channels`** — server-side mutable operator-policy table `(channel_id, blocked_at, reason)`. Not events. [3068]–[3081]
- **`borrowed_fuse_conn`** — ContextVar carrying the per-callback pool-borrowed psycopg connection into helpers and the inode map. [2125]–[2148]
- **Coalesced disk projection** — background-rendered mirror at `~/.cache/slack-fuse/projection/`; clean targets are served from disk, dirty ones JIT. [5145]–[5152]
- **Corrective event** — append-only richer `message_changed` repairing a dedup-blocked lossy `message`. Written by `write_message_or_corrective`. [3020]–[3048]
- **`_current_callback_budget`** — ContextVar propagating the outer FUSE callback budget into nested `_run_sync`/pool guards. [4077]–[4087]
- **`events_message_dedup`** — partial-uniqueness index making repeated `message` ingestion idempotent. [1756], [2374]
- **Feed epoch** — projection-generation boundary across which old cursors cannot safely read; the reason JetStream KV cannot replace the FUSE feed. [6050]–[6062], [6127]
- **`final_page`** — Slack-derived pagination termination fact; briefly load-bearing in `source`, then moved to run events. [3620], [3925]–[3931]
- **`.ignore` ghost** — always-present root virtual file keeping `rg` out of `_control/`; commit `24de34c`. [6357]–[6365]
- **Layout singleton** — the ledger's `layout` target representing path/slug visibility; bumped atomically with `channel-meta` on block/tier change. [5947]–[5951]
- **Live-only subscription** — `since=0` subscription to an empty stream; emits `CaughtUp(head=0)` and waits for future events. [1398]–[1410]
- **`MALLOC_ARENA_MAX=2`** — glibc arena cap, diagnostic pin for thread-offload fragmentation. [4765]–[4774]
- **`NO_POSTGRES`** — mount-root virtual file with reserved inode `(1<<53)-1`, materialised while local PG is down. [2241]–[2274]
- **Page-atomic write** — one PG transaction per Slack history/replies page; all or nothing. [3572]–[3574]
- **Paper mount** — a no-production-code proof that `channels/<slug>/channel.md` fits the six proposed Rust traits. [6203]
- **`PgHealth`** — local PG reachability state machine, 5 s down / 60 s up probes, fast-fails callbacks with EIO. [2241]–[2274]
- **Plan 9 ctl/status** — `_control/` semantics: writing performs an RPC-like action, reading `status` shows latest outcomes. [2907]–[2911]
- **Probe sweep** — scheduled/manual raw Slack API captures (`conversations_history_sampled`, `conversations_list_sampled`, `users_list_sampled`, day-presence) for reconciliation and gap detection. Names are raw captures, not interpretations. [3531]–[3557]
- **`projection_targets`** — the PG materialization ledger: `(target_kind, channel_id, local_day, thread_ts)` with `NULLS NOT DISTINCT`, kinds `channel-meta`/`day`/`thread`/`layout`, gated on `renderer_version` + generation CAS. [5906]–[5914]
- **`ReconnectingConnection`** — psycopg wrapper: lazy replacement, one supervised retry, mid-transaction `OperationalError` propagates, structured `reconnect_recorded` events with `failure_phase`/`commit_outcome`. Seven named instances. [5032]–[5039], [6083]
- **`refill-window`** — bounded operator CLI fetching history + replies between explicit `oldest`/`latest`. [3779]–[3799]
- **Rolling deploy-loss protection** — the drifted meaning of `SLACK_FUSE_SERVER_BACKFILL=true`. [3860]–[3871]
- **Skip-thread-expansion** — backfill optimization skipping `conversations.replies` when local active-fold count *and* `MAX(reply_ts)` both match Slack's `reply_count`/`latest_reply`. "Skipped" means "not needed", not "gap accepted". [5137]–[5141]
- **`SlackTierPacer`** — process-wide lock-backed pacing preventing channel totals and probes from independently overrunning the shared Tier-2 budget. Starving it caused the crashloop. [5289]–[5292], [6091]
- **`slack_event_inbox`** — durable Postgres webhook inbox: HMAC-verified raw envelope inserted before ACK, async consumer, `attempt_count`/`next_attempt_at`/dead-letter after 12. [4507]–[4527]
- **`slurper-span`** — structured op log: `op`, `task`, `result`, `duration_ms`, `limiter_wait_ms`, `sync_ms`. Shipped to Loki via Grafana Alloy. [3374]–[3411]
- **Split sync gates** — separate limiters for Slack API, event writes, snapshots and admin/read work, replacing the process-wide `CapacityLimiter(1)` chokepoint. [3278], [3352]–[3359]
- **Staleness trailer** — `⚠ Content may be stale` block composed at FUSE read time over trailer-free disk bytes; `st_size` includes it. [699], [5492]–[5493]
- **Structural fault boundary** — per-channel exception boundary emitting a fatal outcome without killing the catchup sweep. [3952]–[3955]
- **`TaskSupervisor` / `TaskPhase` / `/livez`** — in-memory task registry exposing declared phase + deadline; models scheduler progress, not data flow. [3362]–[3369]
- **Tiers: hot / hidden / blocked** — listed+maintained / directly addressable but omitted from `readdir` / inaccessible. `tier_source` `auto` vs sticky `manual`. [667]–[669], [1454]
- **`Validated[T]`** — frozen generic pairing `raw: JsonObject` with `model: M` at every I/O boundary. [2810]–[2844]
- **`V2InvalidationSink`** — translates ledger and health events into `pyfuse3.invalidate_inode` calls, off the event loop; non-benign failures keep the target pending. [1164]–[1179], [6257]
- **Waiting oracle** — `/sys/fs/fuse/connections/<id>/waiting`, the watchdog's second wedge signal beside daemon D-state. [4105]–[4119]
- **WTF audit** — the mandated final full-system review that had to lead with "Top X things that would make the human say 'wtf'". [5378], [5790]–[5793]

## Open threads as of the end of the transcript

**Explicitly asked for, never landed:**
- **Rolling-window `conversations.history` catchup** as defense against outage-era gaps — proposed at the very end after the OOM outage proved gaps survive apparently-continuous ingestion. Not built. [6439]
- **Server memory/OOM root cause.** 1,658 restarts in 10 days; 3Gi is mitigation. Almost certainly backlog task #17's C-extension leak (psycopg/libpq, pydantic-core, zlib, connection state). [4862], [6408]–[6439]
- **Maintained counter table for `/channel-stats`** — the raw-count revert is an emergency measure. [6099], [6120]
- **The platform event-architecture RFC response** — re-requested at [6124], no acceptance recorded. Draft at `/tmp/claude/platform-event-arch-slack-fuse-response.md`. [6125]–[6135]
- **`~/bin/game-mode` ordering** — must stop `slack-fuse.service` before local PG. Requires Simon's host-level edit. [5132], [6079]

**Open engineering items:**
- 9 server gap/day-presence tests fail under `America/Los_Angeles`; SQL uses `to_timestamp(...)::date`/`date_trunc(...)` without explicit UTC. Passes under `TZ=UTC`. [6357], [6365]
- Snapshot DELETE path lacks `_refresh_parent_reply_count` in the same transaction. [6081], [6286]
- Retired snapshot work is not truly cancelled (tail check only, no `trio.CancelScope`). [6081], [6286]
- `_atomic_write_bytes` needs file + parent-dir `fsync` for host-crash safety. [6081], [6286]
- Malformed frontmatter returns `None` and can strand a cold target permanently on JIT. [6081], [6286]
- Old-server round-trip compatibility test may not meet the intended bar. [6081], [6286]
- Residual `/health` 1 ms ↔ 2 s alternation, undiagnosed. [6114]–[6117]
- `test_resume_plan_fast_at_scale[1000|5000]` fails 5/5 (0.542–0.630 s vs 0.5 s). [6085]
- Trailer-decision JSONL writer stopped writing after 2026-08-03. [6083]
- Trailer NULL-at-mount false positives. [6083]
- `rg` throughput through FUSE (~12–25 files/s) vs direct projection (~62,000 files/s), unresolved against ADR expectations. [6155]
- pyfuse3 `interrupt()` — 0.5 s budget, 8-slot pool and `.ignore` are mitigations only. [6356], [6365]

**Rust projection→VFS engine (in-flight co-design, nothing built):**
- Requirements locked through checkpoint 2; trait critique in `eae6cc9`; additive ADR batches accepted. Build order: wire → engine → FUSE presenter + no-kernel harness → first migration → write-backs. [6227], [6236], [6254]
- Six seams: `Source`, `Render`, `Layout`, `Trailer`, `Control`, `Ghost`. [6206]
- Slack producer constraints not implemented: emit one synthetic root parenting `channels`/`dms`/`group-dms`/`other-channels`/`_workspace`; enforce 255-byte UTF-8 `Name::Title`; emoji/variation-selector normalization. Current `derive_thread_slug` can produce oversized names — latent `Name::Final` Nack. [6260]–[6266]
- Paper mount proof not done. [6203]
- Open: cold-start graph indexing at 1M nodes vs the 30 s threshold; `HandleKind::Passthrough` `OwnedFd` lifetime docs; `Fact::Epoch` coalescing docs; `_control/status` → `/.status` migration mapping; keep the PyO3 seam declared. [6221]–[6254]

**Process state:** the old 14-task session plan was deliberately deleted in full at [6287]–[6290] ("close entire plan, we can reevalute once runing on flow"); `git` history and `BACKLOG.md` are the durable record. Daily driver is `flow-crastinator` at `e4726bb`+; pro-crastinator's runtime, local DB `slack_fuse_split`, cache, venv, units, watchdog and mountpoint are removed, checkout and `.env` retained. [6279]–[6311]

## User's working preferences

- **Evidence or an admitted gap, never a plausible story.** "I expect either 'here is why, supported by X evidence' or 'here is the observability gap that means we dont know, and here is how I could fix it'" [3243]. He repeatedly rejected unsupported causal claims about D-state [2110]–[2180] and corrected a false assertion about the Socket Mode heartbeat by demanding code inspection [3263]–[3267]. "no, jsut show me" [3815].
- **BACKLOG.md is his, not a scratchpad.** "BACKLOG.md is when I tell you to park something. Not a scratchpad for 'idk why this broke'" [3221]. Structure: H1 `Ratified` (his), H1 `Agent raised` (needs review), H4 `Resolved` with commit refs. "backlog should be updated as you go with what you did and what commits solved it" [5118]. Prefers archiving and reconstructing a stale backlog over endlessly mutating it [6072]–[6078].
- **Handoff → review → bounce-or-merge.** Codex GPT-5.5/5.6-sol xhigh with `--worktree`; reviewers from a different model family than the writer; never skip review; escalate after 3 bounces, on any unratified design decision, on scope expansion, on >2× estimate, or on unrelated test regressions. [814]–[822], [3289], [5156]–[5164]
- **Decisions elicited and recorded.** "walk through the decisions needed using AskUserQuestion and record the results" [5133]. "Need more context ... Reorentate me with clear concise but technical-reader skilled engineer/architect level orentation on each question" [5137].
- **Worktrees for anything parallel.** "huh? are we not using worktrees?" [3099]. Same-worktree edits to the same files must be serialized [102]–[103].
- **Logically separate commits**, split by concern, not by file [151]–[157], [539]. Skeptical of ceremony: "why are we PRing anyway, could have been commit to main" for personal-repo docs [3221]; but used the owner-agent/PR workflow for k8s-homelab [4146]. "pr body does not need your own line wrapping" [5577].
- **No pushes until it works.** Standing "no push" through the entire v2 rebuild; explicit approval required to change it. [826], [1204], [1551], [6026] ("yes+deploy").
- **Pre-v2 standing authority to break things.** "you are slack fuse owner, and we are still pre v2 'prod', so you have standing authority to edit and push slack fuse" [2120]; "you have full standing authority to break all access in this testing in order to find things" [2275].
- **Data loss is the top risk.** "Each row of this table is CRITICAL to get right or we have data loss" [3505]. Accepted extra API cost to detect it: a few dozen daily sweep events was "100% worth it" [3506]. Wanted targeted refills before an expensive full re-backfill: "dont want to pay expensive backfill on a system we find is broken in a week" [3779].
- **Pure ES over cleverness.** "events are Slack facts, not slurper bookkeeping" [3566]; "poking an update into the table on recieve feels like violating ES" [1992]; "projection in same process/db is fine. just not same table" [2447]; "Pure ES is less chance of breaking things. A is 'clever' in the bad way" [3929].
- **Deep skepticism of magic fixes.** "if we Changing the arena it means you've done something absolutely terrible, but sure, ship your fix and let's see how it breaks things" [4770].
- **Latency budget he cares about.** "all other cases should either EIO in under a second or work" [2253]. Rejected legacy polling after it took "10s of seconds" [4876].
- **TDD selectively** ("if/when appropriate"), not universally [10]. Strict basedpyright/ruff/trio/frozen-Pydantic conventions are non-negotiable; handoffs reported 955 → 1,189 passing tests as they landed.
- **Don't restart dependency services you don't own**; escalate instead [2912]. K8s node/cluster problems go to the homelab owner [3677].
- **Trim dead code rather than explain it** [6164]. Inline small well-contextualised fixes: "inline too. you have the context" [3753].
- **Effort is measured in his review capacity**, not agent coding time [3644]–[3646].
- **Operational hygiene:** "sleep 15s between kills or we risk crashing tmux"; "kill server? wtf?" [6021]–[6025]. Handoffs should auto-attach, not hide in background mode [1158]–[1160]; "handoff should not require this [trust click]" [6345]. "when i say ping ... say pong with no thought or other action" [3687].
