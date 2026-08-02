# Backlog

Structure (Simon's convention, 2026-08-02):

- **Ratified** — items Simon has asked for or blessed. Highest confidence.
- **Agent-raised (needs human review)** — items an agent (usually Claude here) surfaced and validated, but Simon hasn't triaged yet.
- **Agent-raised (unconfirmed origin)** — items another agent proposed that may not trace back to a human ask, or that couldn't be confirmed. Weakest signal; treat as suggestions.
- **Resolved** — done items with the commits that closed them, most recent first. Not deleted, so the history reads back.

When an item ships, move it into **Resolved** with the commit hashes and date. When a new item surfaces, add it to the appropriate section — never auto-append to Ratified without explicit user blessing.

---

# Ratified

Ordered by priority (highest first). Each entry annotates **Effort**
(order-of-magnitude estimate for a single-agent handoff) and **Autonomous**
(whether I can drive it end-to-end without Simon's intervention — Yes /
Yes after decisions / No).

## FUSE passthrough + coalesced disk projection

**Effort**: 5–8 eng-days. **Autonomous**: Yes after decisions — five
open questions in the ADR (see below) need answers; some (projection
location, hidden channels, path semantics) are ergonomic; one
(`CAP_SYS_ADMIN` acceptance) is genuinely yours to call.

**Discovered**: 2026-06-29 while benchmarking ripgrep throughput. Live
mount serves ~18 files/sec (every file goes through FUSE round-trip);
the archive on disk serves ~135,000 files/sec. ~7,500× gap.

**ADR (2026-08-02, `/tmp/claude/adr-fuse-passthrough.md`)**:
recommendation **O2** — **reject** privileged FUSE passthrough (requires
`CAP_SYS_ADMIN`, no pyfuse3 binding, wouldn't fix the current
attribute-path bottleneck). Instead **build a direct coalesced on-disk
projection**: extend the archive concept into a searchable mirror,
eager/coalesced for hot channels and background-filled for cold ones.
FUSE stays as the exact-fresh interactive view.

**Decisions ratified 2026-08-02**:
1. **Serve at the same `/views/slack` path via tier logic** — not a
   separate `/views/slack-fast` mount. Consumers see one namespace;
   the daemon arbitrates per-read between fresh-FUSE-render (dirty
   paths) and disk-serve (clean paths). Implementation cost: per-read
   dirty-check + strict invalidation ordering (disk write must land
   before flip dirty→clean) so a clean read is provably byte-equal to
   what JIT would produce.
2. **5s lag budget for today's files.** Coalescer wakes every ~5s or
   on batched WS frames. Interactive tail-following sees visible lag
   on chatty channels; accepted for the cheaper coalescer cadence.
3. **Hot channels only.** Hidden channels (`tier != 'hot'`) stay
   FUSE-only; users needing them use `rg /views/slack/.hidden/` on
   the live JIT path. Saves ~2–3× disk.
4. **Projection lives at `~/.cache/slack-fuse/projection/`** —
   alongside the existing archive, XDG cache convention, rebuildable.
5. **`CAP_SYS_ADMIN` question is moot** since we're not doing
   passthrough. Recorded here for the ADR trail.

**Related design note (Simon, 2026-08-02, consider-only)**: sub-5s
use cases (live tail, per-channel notify) belong in dedicated CLI
tools, not in the mount. The coalesced projection intentionally
optimizes for `rg`-style broad reads at ~5s lag; the mount stays
exact-fresh for interactive `cat`; a future `slack-fuse notify
<channel>` or `slack-fuse tail <channel>` (both direct WS
subscribers) is the right home for the "surface every new message
within 1s" workload. Not building either now — noted so the 5s
projection lag doesn't get argued down for a workload it wasn't
sized for.

---

## FUSE mount wedge — host-level prevention (game-mode ordering)

**Effort**: 15–30 min. **Autonomous**: No — the change lives in
`/home/simon/bin/game-mode` (your personal operator script, not
slack-fuse). I can propose the diff but shouldn't push it unilaterally.

**Status**: architectural fix landed (`87487d0`). Per-callback connection
pool + 30s trio timeout + 25s PG ``statement_timeout``. Concurrent
callbacks no longer serialize behind one limiter slot; a slow SQL aborts
at the PG layer and surfaces as ``FUSEError(EIO)``; a pure-Python hang
times out at the trio layer with the same result. 4 regression tests
pin the new behaviour. Recovery watchdog (`scripts/watchdog/`) shipped
and live-verified against a 6h53m wedge on 2026-06-21: full recovery in
under 5s, projection state preserved.

**ADR (2026-08-02, `/tmp/claude/adr-fuse-mount-wedge.md`)**: keep landed
fixes; retain the watchdog; add clean `game-mode` stop/start ordering.
No kernel/zram tuning absent new evidence — the historical
`folio_wait_bit_common` specimen was the now-fixed
`FUSE_NOTIFY_STORE`-inside-`read()` deadlock, not folio pressure.

**Prevention still outstanding** — add `slack-fuse.service` to
`game-mode`'s `GAME_MODE_STOP_SERVICES` so it gets cleanly stopped
before `claude-hooks-postgres.service` is torn down, then restarted in
`cmd_off`. Priority is low because the watchdog already recovers in
<5s; prevention just avoids the transient EIO during game-mode
transitions.

---

# Agent-raised (needs human review)

## Trailer FP: NULL-at-mount is a separate diagnosis

**Raised**: 2026-08-02 during trailer-FP review. The defensive fix in
`9fa4b60` (workspace_last_frame_at classifier semantics + regression
tests) does NOT change production behaviour today — both
`last_frame_at` and `workspace_last_frame_at` are populated from the
same `connection_state.last_frame_at` singleton in `fetch_staleness_state`
and only diverge in pure tests. If the original 2026-06-27 21ms
`/general/channel.md` "server unreachable — last sync never" symptom was
caused by `connection_state.last_frame_at` being `NULL` at read time
(row exists as `INSERT id=1` with NULL until the first WS frame or
first `apply.py` health update), the fix does not cure it. Both fields
would still be `None` and the classifier still returns "server unreachable".

**To investigate**: re-open with a fresh symptom capture. Candidate
follow-ups if it recurs:
1. Populate `connection_state.last_frame_at` at mount start (treat startup
   as an implicit heartbeat) so the singleton is never NULL past first-boot.
2. Change NULL semantics: treat `NULL last_frame_at` at mount startup as
   "wait, not disconnected" for the first N seconds.
3. Add a real per-stream `last_frame_at` column to `stream_caught_up` so
   the two `StalenessState` fields actually diverge in production.

---

## `client_wedged` fan-out to per-conn events

**Raised**: 2026-08-02 during reconnect impl review. The
`ReconnectingConnection` wrapper emits `client_wedged`/`client_recovered`
per-wrapper. A single PG bounce produces up to 5 event pairs (one per
fixed conn: `inode`, `projector_state`, `projector_sink`, `block_sync`,
`rerender`, `rerender_sink`). Also 6 if rerender was active.

Operator surface for this fan-out isn't documented. Options:
1. Document in operator notes: "a PG bounce produces N wedge/recovery
   pairs named per-conn; N=5 total (6 if rerender is active)."
2. Aggregate into a process-wide episode marker in `_control/status`
   (single `client_bounce_episode` count instead of many named events).
3. Leave as-is — the per-conn granularity is valuable for debugging
   partial recoveries.

Recommendation: (1) first; (2) only if operators find the per-conn
noise confusing in practice.

---

## Migrate `slack-fuse permalink` off v1 island

**Raised**: 2026-08-02 after `resolve` was migrated (commit `057883c`).
`permalink` is the sole remaining consumer of `_slug_helpers.py`, which
in turn holds the entire v1 module island alive:

- `store.py`, `api.py`, `user_cache.py`, `disk_cache.py`, `renderer.py`,
  `fuse_ops.py`, `backfill.py`, `archive.py`, `socket_mode.py`

Migrating `permalink` unblocks deleting all nine modules (~several
thousand LOC). Same shape as `resolve` migration:

- Parse FUSE path → extract channel slug, date, thread slug
- Reverse slug → channel_id via the local `channels` table
  (assign_conv_root_slugs)
- For a thread slug on a date, reuse `fetch_day_thread_parents` +
  `dedup_thread_slug_map` to reverse
- Then call the server's `chat.getPermalink` endpoint (still Slack API,
  via the shared secret) for message-level permalinks
- Channel-root permalinks still require `SLACK_WORKSPACE_URL`

Estimated scope similar to resolve: ~150 LoC + ~250 LoC of tests
against the migrated PG fixture. Handoff-shaped.

---

## 26 pre-existing repo-wide `ruff format` differences

**Raised**: 2026-08-02 by the impl-resolve-migrate-v2 codex handoff
during its audit pass. Files outside anything today's work touched
would reformat under `ruff format .`. Not blocking anything; a
one-off `ruff format .` sweep + single commit would clear it.

---

## `/channel-stats` endpoint latency — server-side query optimisation

**Raised**: 2026-08-02 while verifying the workspace-channels deploy.
Endpoint takes 5–10s from LAN and 8–12s over Tailscale (135KB payload
across 664 channels; likely a per-channel JOIN cost). Client fetcher
timeout bumped 5s→30s inline (commit pending) to stop warmer
ReadTimeouts, but the endpoint itself needs optimisation.

Candidates:
1. Materialise the join into a view refreshed by the channel-totals
   sweep (once per 6h), read it as a single SELECT.
2. Precompute + cache the JSON body server-side; invalidate on refresh
   task tick.
3. Paginate the endpoint — probably unnecessary given the cache-warmer
   pattern.

Not blocking anything now that timeout is 30s; the warmer runs every
5 min so a 10s call is fine budget-wise. But 30s is a big client
budget for what should be a snappy read.

---

## Two flaky perf tests in `tests/backfill/test_resume.py`

**Raised**: 2026-08-02 by multiple handoffs today. `test_resume_plan_fast_at_scale`
uses a fixed `elapsed < 0.5s` wall-clock threshold and comes in at
0.70–1.03s on the current dev host under load. Not a bug in the code
under test — a load-based flake. Either:
1. Bump the threshold and add a note that it's a smoke test, not a perf
   test.
2. Move to a proper micro-benchmark with `pytest-benchmark`.
3. Skip in CI when the host is under load.

---

# Agent-raised (unconfirmed origin)

_None currently._

---

# Resolved

## 2026-08-02

- **Coalesced disk projection Stage D3 (invalidation ordering + adversarial tests)** —
  `abd3fbb` + `a1a415d`. Enforces race-safe ordering across D1/D2 with:
  new `check_and_mark_clean_if_no_drift` atomic primitive in
  `DiskProjection`, shared commit/clean barrier in `apply.py`,
  offset-drift snapshots in the applier, commit-gap fixes in
  `snapshot_fetch.py` and `rerender.py`, multi-writer guard in the
  coalescer. 11 new deterministic race tests via
  `test_projection_race_safety.py` — covers event-during-write with
  offset drift, commit-to-dirty reader gap, old-fd/new-inode atomic
  replace, double-ENOENT to JIT, 100 dirty flaps + convergence, 10
  readers during flush, dirty→clean byte-equality, pyfuse3
  invalidate LAST, feature-disabled no disk access, concurrent
  flush single-writer guard. D1/D2 were already correct for atomic
  bytes / bounded ENOENT / feature gate / invalidate shape; D3
  locks those in with adversarial coverage. 378 projector+fuse_v2
  tests pass; 1200 full-suite pass (2 authorized backfill timing
  cases deselected); basedpyright 0/0/0; ruff clean.
- **Coalesced disk projection Stage D2 (read-side tier logic)** —
  `8186d71` + `111968c`. `fuse_ops_v2.py` clean-tier reads/getattr/
  lookup/readdir now serve from `DiskProjection` when the flag is on
  and the path is clean; disk structural bytes go through the same
  live trailer composition and conservative fallback-cache gate as
  JIT. Uses `DiskProjection.is_clean()` atomically (dirty+inflight+
  backing-exists in one lock); no new dirty-set primitive needed.
  `read_bytes()` has no dirtiness logic; missing-after-clean retries
  exactly once before warned JIT fallback. Kernel invariants 15/15
  with flag both false AND true (regression guard). Timing 1.5KB
  warm read: disk median 0.665ms / p95 1.336ms vs JIT median 0.727ms
  / p95 0.957ms (under 1ms constraint; the live trailer/tier DB
  lookups keep it above the ideal 100us — flagged for future). Ghost
  files + `_control/` + NO_POSTGRES bypass the tier gate entirely;
  projection gate scoped to normal channel.md/thread.md only.
  D3 (adversarial invalidation ordering + race-injection tests) is
  the last stage.
- **Coalesced disk projection Stage D1 (writer + coalescer)** —
  `98f9657` + `8fea4a2`. Ships DARK behind `SLACK_FUSE_DISK_PROJECTION_ENABLED`
  (default false). New `slack_fuse/projector/disk_projection.py`,
  `coalescer.py`, `dirty_set.py`. Writes to `~/.cache/slack-fuse/projection/`
  (atomic temp+rename). Coalescer wakes every 5s, initial bootstrap
  paginates 200 paths/tick to keep the trio loop responsive during
  the first ~600 hot-channel bootstrap. Render composes exactly the
  same `fetch_day_chunks`/`fetch_thread_chunks` + `sql_resolvers_for`
  + `resolve_mentions` + frontmatter helpers as JIT — byte-equality
  test proves projection bytes match `synchronous_read_for_test`.
  Adds public `path_changed` invalidation adapter to `SlackFuseOpsV2`
  for D2 to consume. 17 focused tests + 1180 pass. D2 (read-side
  tier logic) and D3 (adversarial invalidation ordering + race tests)
  follow.
- **Probe-event pattern (framework + first fact probe)** — `a3be0ba`
  + `a0b7193`. First pass built a parallel `slack_fuse_server/probes/*`
  package; reconciliation collapsed it into the existing
  `slack_fuse_server/slurper/probes.py` scheduler with a `ProbeSink`
  Protocol and two implementations — `SlurperHealthSink` (raw
  detection, writes to `slurper-health`) and `EventFactsSink`
  (interpreted facts, atomic append to `events`). One unified sweep,
  one registry, two sinks. First fact probe is
  `channel_message_count_probed` (6h cadence, feeds backfill % calcs).
  Extensibility hard gate: `test_second_probe_is_registry_entry_only_no_framework_change`
  proves a new stub probe fires beside the default with no
  sweep/framework code changes. New `SlackTierPacer` primitive shared
  by `channel_totals.py` + fact probes for the search.messages
  Tier-2 budget (raw probes stay per-method as before). Migration
  0015 adds a partial index `events_probe_fact_latest_idx` on
  `(kind, ts DESC NULLS LAST) WHERE kind IN ('channel_message_count_probed')`
  so the MAX(ts) cadence lookup is O(1) — adding a new fact kind
  requires extending the WHERE clause via a follow-up migration.
  Client applier: explicit no-op branch for the new kind (real
  projection is a follow-up). 1163 pass, basedpyright 0/0/0, ruff
  clean (including the pre-existing en-dashes I introduced in
  `801af2a`, fixed in the merge commit).
- **Workspace channel inventory view (`_workspace/channels.md`)** —
  `34f7f4e`. New server-side sweep `slurper/channel_totals.py` (6h
  cadence, Tier-2 paced 3.5s between calls) + `search_messages.py`
  user-token-only wrapper (approximate-flag on 10K+ paging drift) +
  `channel_message_totals` table (migration 0014, upsert-preserves-
  last-known on error) + `channel_stats.py` server projection joining
  metadata/totals/blocks/ingest counts + `/channel-stats` authenticated
  endpoint + client `channel_stats_fetch.py`/`channel_stats_warmer.py`
  (5-minute background warmer, callbacks read cache only) + pure
  markdown renderer + FUSE ghost-file wiring in `fuse_ops_v2.py`.
  CLI: `slack-fuse-server refresh-channel-totals`. 12 focused new
  tests + full-suite 1140 pass.
- **Skip thread-expansion when local thread is already caught up** —
  `5d46c10`. New `slack_fuse_server/backfill/skip_predicate.py` reads
  the `active_messages` view (migration 0008 — edit/delete-folded)
  grouped by `thread_ts` for the current channel's parent worklist,
  returning `set[thread_ts]` where both local count and MAX(reply_ts)
  match Slack's `reply_count` and `latest_reply` from Phase 1's history.
  Skip emits a `slurper.backfill.thread_skip` span (no event row);
  partially-resumed threads (`cursor != ""`) always fetch. Callback
  injected into `SlackApiBackfiller` via `caught_up_threads`; slurper
  wires it through `writer.run_read` under `limiters.admin_read`.
  35 focused tests; 6 predicate + 6 API/integration.
- **Clean up repo — sprint/feat worktrees** — inline sweep in Ratified
  orchestration Phase 1. Removed 33 stale worktrees (~30
  `.wt/synap5e/feat/*` + 10 `.wt/handoff/*` + `.wt/server-split-rebuild`);
  only the two in-flight handoff worktrees remain. Deleted 7 branches
  with `ahead=0` vs main. Kept 25+ branch refs (squash-merged; cheap to
  preserve as history addressability). `.wt/` down to 6.5M.
- **Migrate `slack-fuse resolve` to v2 projections store** — `057883c` +
  docs `23d080d`. Fully local PG, no Slack API dependency. Composes
  existing v2 helpers (`assign_conv_root_slugs`, `fetch_day_thread_parents`,
  `dedup_thread_slug_map`). 20 focused tests via real migrated PG schema.
- **Collapse `--mode split` into the sole mount mode** — `3688c92`. V1
  `cmd_mount` dispatcher deleted; `cmd_mount_split` renamed to `cmd_mount`;
  `--mode`, `SLACK_FUSE_MODE`, `_env_bool`, `signal` import all removed.
  Pro's `slack-fuse-split.service` renamed to `slack-fuse.service` mounting
  `/views/slack` (host-side changes: retire legacy `slack-fuse.service`,
  retire dead `slack-fuse-server.service` bake-in, update watchdog
  script/timer to new unit name). Flow updated + restarted.
- **Client reconnect-after-PG-bounce (FINDING-06 shape)** — `80dc661…a6a0143`
  (5 commits). Introduces `ReconnectingConnection` wrapper, migrates the 5
  fixed conns (`_inode_conn`, `state_conn`, `sink_conn`, `block_sync`,
  `rerender` apply + sink), moves initial connects inside the supervisor
  try, emits `slurper-health.client_wedged`/`client_recovered` per wrapper.
  Fault-injection test via `_BreakOnExecuteCursor` on a real migrated PG
  schema. 468-line wrapper + 303 unit tests + 160 fault-injection tests.
- **Trailer FP defensive fix on quiet streams (partial)** — `9fa4b60` +
  BACKLOG note `7935199`. Classifier now uses `workspace_last_frame_at`
  with regression tests pinning correct behaviour when per-stream and
  workspace timestamps diverge. Caveat: NULL-at-mount is a separate
  diagnosis (see Agent-raised section above).
- **FUSE getattr `st_blocks=0`** — `83e79a1`. Both v1 and v2 helpers now
  set `st_blocks = ceil(size / 512)` so `du`/`dust` report real usage
  instead of 0B on every file. Parameterized boundary tests
  (`tests/fuse_v2/test_stat_blocks.py`, 15 assertions).
- **Drop shipped POC-B renderer-split scratch + POC worktrees** —
  `438402d`. `slack_fuse_poc_b/` deleted (shipped as `slack_fuse_render/`),
  `.wt/synap5e/poc/a-events-to-postgres` worktree + branch removed (0
  commits ahead of main), `.wt/synap5e/poc/b-renderer-split` worktree
  removed (branch retained — 1 historical commit).
- **Drop dead `tests/test_equivalence.py`** — `e26e9a0`. Tested POC-B
  vs. production single-pass; POC-B was deleted so the test file was
  broken imports (23 basedpyright errors). Also file-level pyright-ignore
  on `tests/fuse_v2/test_stat_blocks.py` for the intentional private
  `_make_file_attr` import.

## 2026-06-27+ (pre-restructure)

Prior resolved items were deleted per the previous convention ("Closed
items are removed"). Future closures accumulate here.
