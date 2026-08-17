# Backlog

Structure (Simon's convention, 2026-08-02):

- **Ratified** — items Simon has asked for or blessed. Highest confidence.
- **Agent-raised (needs human review)** — items an agent (usually Claude here) surfaced and validated with evidence, but Simon hasn't triaged yet.
- **Agent-raised (unconfirmed origin)** — items another agent proposed that may not trace back to a human ask, or that couldn't be confirmed. Weakest signal; treat as suggestions.
- **Resolved** — done items with the commits that closed them, most recent first. Not deleted, so the history reads back.

When an item ships, move it into **Resolved** with the commit hashes and date. When a new item surfaces, add it to the appropriate section — never auto-append to Ratified without explicit user blessing.

_Previous backlog archived to `BACKLOG.archive-2026-08-17.md` on 2026-08-17. This is a re-do: shipped items moved to Resolved with commits; agent-raised items were re-investigated by parallel forks before re-inclusion (many turned out already-resolved or not-a-real-issue post the 2026-08-17 architecture PRs)._

---

# Ratified

Ordered by priority (highest first). Each entry annotates **Effort** (order-of-magnitude estimate for a single-agent handoff) and **Autonomous** (whether I can drive it end-to-end without Simon's intervention — Yes / Yes after decisions / No).

## Proper fix for `/channel-stats` fold-count starvation

**Effort**: 1-2 eng-days. **Autonomous**: Yes.

**Context**: production incident 2026-08-17 (`c87572e`). The 2026-08-03 `185fde4` change made `/channel-stats` query `count(active_messages) GROUP BY channel_id` — the `active_messages` view folds edits+deletes across the entire event stream on every query, EXPLAIN-ANALYZE-measured at **261 seconds** against 828k events. Client warmer polls /channel-stats every 5min; the 261s query starved trio + PG pool and killed `/health` probes → CrashLoopBackOff (304 restarts / 28h). Reverted to raw `kind='message'` count as an emergency bandage. Metric is now honestly "lifetime ingested" not post-fold.

The proper fix is a maintained per-channel counter. Two candidates:

1. **Counter table maintained by the applier**: `channel_active_message_counts(channel_id, count, updated_at)`, incremented on `message` apply, decremented on `message_deleted` apply, no-op on `message_changed`. Trigger-free (applier code owns it). O(1) lookup.
2. **Materialised view** refreshed on the channel-totals sweep tick (every 6h). Slower to freshness but zero applier changes.

Recommend #1: freshness matches the applier's own commit cadence, exactly the property the reverted metric loses. #2 would give a stale count between sweeps.

## Primitives library extraction (slack-fuse-owned)

**Effort**: **L** (1-2 months). **Autonomous**: No — needs Simon's decisions per session task #18 (repo location, versioning cadence, notion-fuse migration coord, handoff-to-platform criteria).

**Context**: from the 2026-08-17 platform RFC exchange. Simon decided slack-fuse owns the primitives library initially (see `/tmp/claude/platform-event-arch-slack-fuse-response.md` §"Feed epochs & shared primitives"). Blocks any second FUSE service that would otherwise reimplement the same 15 commits' worth of reliability primitives.

Fork categorization of the six candidate primitives (agent-raised, size estimates from grep):

| # | Primitive | Location | Category |
|---|---|---|---|
| 1 | `ReconnectingConnection` | `slack_fuse/projector/reconnecting_conn.py` (616 LoC) | (a) extract as-is; zero slack imports |
| 2 | `projection_targets` ledger | `projection_ledger.py` + `disk_projection.py` (1293 LoC total) | (b) split generic ledger from slack target-key mapper |
| 3 | Snapshot-redirect protocol | `snapshot_fetch.py` (362 LoC) | (b) extract after ledger is generic |
| 4 | `SubscriptionState` + capability handshake | `ws_client.py` + `wire/frames.py` + `wire/subscriptions.py` (~791 LoC) | (c) split protocol shape from frame types |
| 5 | `projector-span` log conventions | Emitters in fuse_ops_v2 + disk_projection | (a) ~50 LoC helper |
| 6 | `make_source` / `insert_event` / `ingesting` scope | `slurper/ingestion.py` + `slurper/offsets.py` (~745 LoC) | (b) extract propagation, leave row shape |

## `/channel-stats` warmer stability watch

**Effort**: 15 min. **Autonomous**: Yes.

**Context**: post-incident diagnostic. Even after the 2026-08-17 revert, in-pod /health calls alternate 1ms → 2s over 5 samples — something is occasionally competing on the event loop or PG pool. Would have killed the pod at the old 5s probe timeout. Doesn't kill it at the new 15s/30s widened probes, but the underlying starvation source (whatever it is) is still there and worth understanding.

Cheapest first check: capture 100 sequential `/health` timings from the pod + correlate with `projector-span op=slurper.*` log lines to see what's on the loop during the slow samples. May or may not turn up a real bug.

## FUSE mount wedge — game-mode ordering

**Effort**: S (15-30 min). **Autonomous**: No — lives in Simon's personal `~/bin/game-mode` script outside the slack-fuse repo. Watchdog already recovers <5s so priority is low; this is transient-EIO avoidance, not correctness.

**Verified 2026-08-17**: `~/bin/game-mode` exists, has `GAME_MODE_STOP_SERVICES: list[tuple[str, str, list[str]]]` at line 121 containing `claude-hooks-postgres.service` at line 156. `slack-fuse.service` is NOT in the stop list. Add it so it's stopped cleanly before PG teardown and restarted in `cmd_off`.

---

# Agent-raised (needs human review)

## `/channel-stats` `/health` 1ms→2s alternation

**Effort**: 30-60 min investigation, then depends on cause. **Autonomous**: Yes for the investigation.

**Context**: post-`c87572e` deploy, in-pod /health calls alternate 1ms/2s across 5 sequential samples. Bad enough to have killed the pod at old 5s probe timeout (which is what caused the 2026-08-17 CrashLoopBackOff pre-fix). Doesn't kill it at widened 15s/30s timeouts. Suggests something OTHER than the channel-stats query is occasionally starving trio for ~2s. Candidates: channel-totals sweep, some other slow slurper task, or a PG conn pool contention artifact from PR 3 ledger writes.

Investigation approach: 100 sequential /health timings correlated with `projector-span op=slurper.*` logs to identify what's on the loop during slow samples.

## Migrate `slack-fuse permalink` off v1 island

**Effort**: 1-2 eng-days. **Autonomous**: Yes.

**Verified 2026-08-17**: grep confirms `permalink.py` is the sole live consumer of `_slug_helpers.py`, which keeps the entire v1 module island alive:

- `store.py`, `api.py`, `user_cache.py`, `disk_cache.py`, `renderer.py`, `fuse_ops.py`, `backfill.py`, `archive.py`, `socket_mode.py`

Zero live-production callers of any of these outside the v1 island itself. Migrating `permalink` unblocks deleting all nine modules + their tests (~several thousand LoC). Migration same shape as `resolve` (commit `057883c`): parse FUSE path → reverse slug via `channels` table + `assign_conv_root_slugs`, use `fetch_day_thread_parents` + `dedup_thread_slug_map` for thread slug reversal, call server's `chat.getPermalink` for message URLs, retain `SLACK_WORKSPACE_URL` requirement for channel-root URLs.

## Snapshot DELETE leaves parent `reply_count` stale

**Effort**: 1-2h. **Autonomous**: Yes.

**Verified 2026-08-17**: `snapshot_fetch.py:303-346` does `DELETE FROM thread_chunks ... RETURNING` + ApplyResult + post-commit sink, but never calls `_refresh_parent_reply_count`. That helper exists at `apply.py:424` with an `allow_downgrade=True` mode (FINDING-15, 2026-07-17) already wired into `apply_event`'s delete path. Call-site fix, no schema change: after the RETURNING, iterate distinct `thread_ts` values in `deleted_thread` and call `_refresh_parent_reply_count(cur, channel_id, thread_ts, allow_downgrade=True)` in the same TX.

Surfaced by sol during PR 2 review; pre-existing (not introduced by any recent architecture PR).

## README rewrite around v2

**Effort**: 4-6h. **Autonomous**: Yes after Simon confirms he wants v1 removed entirely rather than dual-documented.

**Verified 2026-08-17**: every v1 marker is still present in `README.md`:

- `SLACK_USER_TOKEN` listed as required (line 43)
- `SLACK_APP_TOKEN` (line 44)
- `SLACK_FUSE_BACKFILL` (line 47, plus §"Background backfill" 124-131)
- `feed.md` (lines 93, 96)
- `.cached-only/` (line 106, §"Offline mode" 116-122)
- Socket Mode described v1-style (line 141)
- TTL caching table (lines 163-170)
- SIGUSR1 (lines 175-177)

The "Filesystem layout", "Caching", "Live updates" sections all need rewriting around v2 (server URL + shared secret + local PG + `_control/` surface + workspace channels view + projection ledger). Half of DESIGN-2 (config drift) shipped in `9956f3f`; this is the README half.

## Trailer FP NULL-at-mount

**Effort**: 30 min for option 1 (populate `last_frame_at` at mount start as implicit heartbeat). **Autonomous**: Yes.

**Verified 2026-08-17**: `connection_state` is still seeded as `INSERT id=1` with `last_frame_at=NULL`. Writers (`ws_client.py:205`, `apply.py:887-888`) both wait for a first frame. `health_subscriber.py:125` treats `NULL last_frame_at` as `frame_stale=True` → "server unreachable" trailer branch. PR 1-4 didn't touch this path.

Post-deploy evidence: no journal repro since 2026-08-16 restart, but the historical log ran 2026-06-29 through 2026-07-21 with hundreds of "server unreachable" entries. Pattern is sparse (bursts, not steady). Recommended fix: option 1 from archived entry — populate `last_frame_at` at mount start as implicit heartbeat.

**Adjacent flag**: trailer-decision JSONL log stopped writing 2026-08-03 despite `--trailer-log-path` still configured (INFO line at boot shows the path). Something after PR 1 quietly killed the writer. Separate line item — worth 15 min to verify.

## Two flaky perf tests in `tests/backfill/test_resume.py`

**Effort**: 15-30 min. **Autonomous**: Yes.

**Verified 2026-08-17**: 5 back-to-back runs show **100% failure rate** on this host, both `[1000]` and `[5000]` consistently over the `<0.5s` threshold (median ~0.57s, range 0.542-0.630s). Not a flake — a genuine threshold regression. Options:
1. Widen to `<1.0s` + document as smoke-not-perf.
2. Migrate to `pytest-benchmark` for percentile-based assertion.
3. Add `benchmark` marker + exclude from default runs.

## Repo-wide `ruff format` sweep

**Effort**: 15 min. **Autonomous**: Yes.

**Verified 2026-08-17**: `uv run ruff format --check .` reports **36 files** would be reformatted (up from 26 on 2026-08-02). Purely cosmetic; one-off sweep + single commit.

## `_atomic_write_bytes` host-crash safety

**Effort**: 30 min - 1h + a note in the contract doc. **Autonomous**: Yes.

**Verified 2026-08-17**: `slack_fuse/projector/disk_projection.py:717-722` is literally `tmp.write_bytes(data); os.replace(tmp, path)` with no fsync of file or parent dir. Process-crash safe, host-crash unsafe. Sol flagged during PR 3 review; explicitly OK'd as out-of-scope for PR 3's process-crash-only durability contract, but worth hardening.

## Malformed-frontmatter startup repair

**Effort**: 2-4h. **Autonomous**: Yes.

**Verified 2026-08-17**: `_target_key_from_backing` catches `FileNotFoundError, UnicodeDecodeError, ValueError` and returns `None` with comment "Malformed disposable bytes fail closed in the reader and can be repaired by a later ordinary target invalidation." The design intent is real for the hot-channel case, but for cold/blocked channels a permanent JIT can persist forever. Enqueue for repair rather than silent-skip when startup discovery encounters malformed frontmatter.

## PR 1 sub-followups: cancel retired snapshot work + old-server round-trip test

**Effort**: 3-5h for snapshot cancellation (belt-and-suspenders); may already be done for the compat test. **Autonomous**: Yes.

**Verified 2026-08-17**:

- **Snapshot cancel** — `ws_client.py:347-391` `_handle_snapshot` runs to completion, then post-fetch re-checks `_is_desired_stream`, token match, and `SubscriptionState.PENDING` before re-subscribing. HTTP fetch + apply are not cancelled — wasted work but no correctness bug given the check. Effort: 3-5h to add real cancellation via `trio.CancelScope` per snapshot task.
- **Old-server round-trip test** — `test_ws_client_recovery.py` already has `test_client_falls_back_to_controlled_reconnect_when_server_does_not_advertise_unsubscribe` (line 218) and `test_old_server_new_client_does_not_break_on_shrink` (line 244, using `ws://old-server.invalid`). Sol's ask may already be met; audit these test bodies vs the true-round-trip bar before scheduling more work.

## `client_wedged` fan-out documentation

**Effort**: 15 min (documentation only). **Autonomous**: Yes.

**Verified 2026-08-17**: today's actual fan-out is **7** durable conn wrappers (`inode`, `projector_state`, `projector_sink`, `disk_projection` (added PR 3), `rerender_apply`, `rerender_sink`, `block_sync`) — one PG bounce fans out to up to 7 wedge/recovery event pairs (archived count of 5-6 was slightly stale). `ControlState._record_health` already keeps `_client_wedged`/`_client_recovered` as singleton latest-outcome overwrites — every fan-out event clobbers the last one at the control surface. PR 1 followups added `connection_name` to `reconnect_recorded` so operators can attribute each of the 7 events. Recommendation: **option 1 (document)**. No code change needed. Add operator note: "a PG bounce produces up to 7 `reconnect_recorded` events named per-conn — `inode`, `projector_state`, `projector_sink`, `disk_projection`, `rerender_apply`, `rerender_sink`, `block_sync`. `_control/status` shows only the latest wedge/recovery outcome."

---

# Agent-raised (unconfirmed origin)

_None currently._

---

# Resolved

## 2026-08-17

### Coalesced disk projection — full ledger-based architecture landed

**Original ratified 2026-08-02.** The full "FUSE passthrough + coalesced disk projection" ADR (see archive) was implemented over 4 sequenced PRs plus follow-ups. WTF-audit findings CORRECTNESS-1, CORRECTNESS-2, CORRECTNESS-3, OPS-1 were all fixed as part of this work.

- `80f2b97` `fix(block-sync): invalidate FUSE and projection on block` — CORRECTNESS-3 fix, added `VisibilityChanges` type with both `newly_subscribed` and `newly_blocked` transitions, wired FUSE inode invalidation + targeted disk-projection cleanup, added live-cache regression coverage.
- `ea0bee3` `fix(reconnecting-conn): propagate mid-transaction OperationalError` — CORRECTNESS-2 fix, deleted `_ReconnectingTransaction._restart` silent-restart, propagates original `OperationalError` inside active TX, block-sync now reconciles full desired-subscribed set every cycle (survives ambiguous COMMIT), snapshot pool conn releases with `discard=True` on `OperationalError`.
- `1f0caa0` + `0b827ec` `feat(projection): dual-write projection_targets ledger` + `fix(projection): persist visibility layout invalidations` — the transactionally-persistent per-materialization invalidation primitive. New table `projection_targets(target_kind, channel_id, local_day, thread_ts, target_generation, rendered_generation, renderer_version)` with `NULLS NOT DISTINCT` uniqueness for the singleton layout row. Dual-write from applier / snapshot / rerender / block-sync / tier CLI, all in the same source-data transaction. Snapshot replacement grew a stale-cursor guard (`SELECT ... FOR UPDATE`) that refuses to replace content when the DB cursor has moved past `at_offset`.
- `b1ac741` + `41c79f9` + `0f5adae` + `7087e8f` (PR 1 follow-ups) — `SubscriptionState` (PENDING/ACTIVE/FAILED) with `UnsubscribeFrame` + capability handshake for old-server compatibility (Option A negotiated, Option B controlled-reconnect fallback), structured `reconnect_recorded` observability with `failure_phase` and `commit_outcome=unknown`, one whole-op result per reconnect, applier `SELECT tier FOR UPDATE` serializes with block/unblock, snapshot re-subscribe race closed via desired-set token check.
- `69cb3cf` + `59b7bba` `feat(projection): consume ledger in readers and coalescer` + `fix(projection): enable ledger reader in production` — OPS-1 fix (global `_state_lock` removed from apply / fuse_ops_v2 / coalescer paths). Reader gates on target + layout singleton dual-clean via the callback pool connection. Coalescer flow is TargetKey-based with startup epoch reconciliation, layout fanout + orphan sweep, stable-key rendering, per-file kernel invalidation before CAS. Production wiring passes `disk_projection_enabled` from ClientConfig; non-benign `OSError`s propagate from `_default_invalidate_inode` so failed invalidations keep the target pending. Latency evidence: sustained-events p50=1.544ms / p95=2.016ms / p99=2.350ms (comparable to JIT baseline); under layout churn p99=2.153ms (better than JIT).
- `f6b4558` `refactor(projection): remove compatibility no-ops after ledger cutover` — cleanup of `invalidation_barrier`, `mark_channel_paths_dirty`, `is_clean` no-op compat surfaces.

Deployed 2026-08-16: pushed 15 commits to origin/main, restarted `slack-fuse.service` on pro-crastinator + flow-crastinator (editable installs), bumped k8s image pin to `sha-f6b4558@sha256:e76694d2…`, resumed suspended `apps` Flux kustomization, verified `slack-fuse-server` pod `1/1 Running`.

### Simple/medium WTF-audit fixes

- `c2b43e5` `refactor(probes): delete dead channel_message_count fact probe` — DESIGN-1 fix. Duplicate `search.messages` sweep with no consumer removed.
- `185fde4` `fix(channel-stats): count post-fold + honest refresh-coverage aggregation` — DESIGN-3 fix. Counts folded `active_messages`; reports oldest/newest refresh + refreshed_ok/refreshable coverage instead of misleading `MAX(refreshed_at)`.
- `9956f3f` `refactor(config): move disk_projection_enabled into ClientConfig` — DESIGN-2 partial fix (config-drift half). Flag now lives in typed ClientConfig, participates in env + TOML precedence, injected as constructor kwarg.
- `c86833e` `chore(config): drop stale slack_fuse_poc_b from basedpyright include` — stale include path caused basedpyright to exit 3 in isolated worktree environments even with 0 diagnostics.
- `4dbac3c` `fix(watchdog): target /views/slack (not deleted /views/slack-split)` — CORRECTNESS-4 fix (already noted resolved in prior backlog).

### `/channel-stats` fold-count starvation → prod CrashLoopBackOff

`c87572e` `fix(channel-stats): revert active_messages fold-count to lifetime-ingested`. The 2026-08-03 `185fde4` change made `/channel-stats` query `count(active_messages)` (post-fold view over 828k events, EXPLAIN-ANALYZE 261s wall clock). Client warmer polls every 5min; the query starved trio + PG pool and killed `/health` probes. Server pod flipped between healthy → 5s /health timeout → 90s of trio silence → liveness kill → 304 restarts / 28h before diagnosis. Reverted to raw `kind='message'` count (metric now honestly "lifetime ingested"). Also widened k8s /health readiness/liveness probes from 5s → 15s/30s in `k8s-homelab@47e356c` as a defense against future occasional trio starvation. Deployed 2026-08-17: image `sha-c87572e@sha256:b743fad1…`, pod up 1/1 Running with 0 restarts, in-pod `/channel-stats` down from ~100s to 14.6s. Proper counter fix now on Ratified.

## Pre-2026-08-17

_See `BACKLOG.archive-2026-08-17.md` "Resolved" section for prior history._
