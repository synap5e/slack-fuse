# Sprint status (live)

Lives on `server-split-rebuild`. Updated as the owner loop ticks.
Most-recent first.

## 2026-06-08 — Sprint 0 + POC B merged

### Sprint 0 — MERGED

- **Branch**: `synap5e/feat/sprint0-interface-freeze` (2 commits:
  worker's interface freeze `8e4f197` + owner fixup `8046a54`)
- **Worker**: claude (opus) — completed cleanly
- **Reviewer**: cursor (gpt-5.3-codex-xhigh) — REQUEST CHANGES with
  3 polish findings + 2 design findings
- **Resolution**:
  - Owner fixed the 3 polish findings inline (snapshot JSONL line
    DTO added; round-trip tests upgraded to byte-level; fake-Slack
    fixture coverage extended to all 9 fixtures)
  - Owner folded the 2 design findings into the RFC as
    clarifications:
    - Backfill idempotency under edits → accepted v1 limitation,
      remediation path via future admin `--refresh` command
    - Cross-stream race → unresolved-fallback / kernel-cache
      invariant added to RFC §FUSE read path; concurrency test
      required as Sprint 2E acceptance criterion
- **Verification**: ruff + basedpyright + pytest (344 passed, 2
  skipped) all green
- **Risk analyses from worker (both validated)**:
  - Backfill idempotency: `message_changed`-event refresh path
    works for live events; bootstrap edits are the accepted v1
    limitation
  - Cross-stream race: `READ COMMITTED` analysis correct for the
    initial scenario, but reviewer found a second race
    (user_added's lookup-before-message-commit) — covered by the
    new RFC invariant

### POC B — MERGED (earlier)

See previous status. RFC absent-user-fallback note added.

### POC A — RUNNING

- Worker reported partial/blocked: zero `events_api` envelopes in
  30-minute sample window. Confirmed against production
  slack-fuse (also zero) — workspace genuinely quiet (Sunday night
  US time). Worker released; slurper continues in tmux session
  `poc-a-slurper` for 48-hour observation.
- Report at `docs/plans/poc-reports/poc-a.md`
- Action for owner: collect overnight data tomorrow

### Integration target

Worktree `.wt/server-split-rebuild/`, branch `server-split-rebuild`.
Currently at `<after-this-commit>` with Sprint 0 + POC B merged.

### Sprint-boundary check-in points (user-in-loop)

- ~~After Sprint 0 lands + POC reports in~~ **Sprint 0 done; POC A
  data collection deferred to tomorrow (overnight observation)**
- After Sprint 1 server soak (~1 week observation) **← next user touchpoint**
- After Sprint 2 fan-out (6-ish tracks merged)
- After Sprint 3 convergence (pre-cutover gate)

### Next owner action

~~Spawn Sprint 1 tracks~~ — **spawned**.

| Track | Model | tmux | Branch | Status |
|---|---|---|---|---|
| 1A slurper | claude (opus) | (killed) | `synap5e/feat/sprint1a-slurper` | **MERGED** at `aa7855c`. 363 → 364 tests. Live-smoked against real Slack workspace + scratch postgres. NOTIFY new_event landed in offsets.insert_event. Bug found & fixed: shared psycopg connection must be `autocommit=True` or backfill silently rolls back into savepoints. Verified 1B/1C unaffected (read-only). Deferred users-stream emission → handed off as 1E. |
| 1E users-stream emitter | cursor (gpt-5.3-codex-xhigh) | (killed) | `synap5e/feat/sprint1e-users-stream` | **MERGED** at `98a4f8d`. Runtime dedup (SELECT-before-INSERT) rather than schema constraint — flagged for reviewer. 365 tests, Postgres-backed users tests pass. Live-tested via fake Slack httpx transport. |
| Post-Sprint-1 critical review | cursor (gpt-5.5-extra-high) | (killed) | n/a | **REQUEST CHANGES**. 5 findings (2 blockers + 3 high). Triage: findings 3+5 owner-fixed inline (autocommit guard + schema users dedup); findings 1+2 handed to 1F (wire binary + Upgrade dispatch); finding 4 handed to 1G (Socket Mode payload conformance). Report at `~/.agent-handoff/2026-06-08/review-sprint1/report.md`. |
| 1F wire binary + same-port dispatch | claude (opus) | (killed) | `synap5e/feat/sprint1f-wire-binary` | **MERGED**. Discovered + fixed pre-existing severe bug: 1B's `wire/tail.py` used `psycopg.AsyncConnection` (asyncio) — every WS connect crashed under trio with real DB; reviewer missed because their env had no DATABASE_URL. Trio-native rewrite (sync psycopg + to_thread + notifies-poller thread + memory channel bridge) landed in scope-extended 1F. Also: new `dispatch.py` for same-port Upgrade routing, full binary wiring, slack_degraded debouncing, backfill_progress emission. 393 tests with DATABASE_URL set. |
| **Sprint 1 smoke test** | n/a (owner) | `slack-fuse-smoke` | n/a | **PASS**. Integrated binary runs end-to-end against real Slack + local Postgres: `/health` 200, `/metrics` returns coherent JSON, `/ws` subscribe to `slurper-health` delivers event + caught_up. 490 user_added events on populate. **7-day soak is now live** in tmux session `slack-fuse-smoke`. Sprint 1 acceptance criterion in progress. |

## Sprint 2 — fan-out (batch 1)

| Track | Model | tmux | Branch | Status |
|---|---|---|---|---|
| 2A LegacyCacheBackfiller | cursor (gpt-5.3-codex-xhigh) | (killed) | `synap5e/feat/2a-legacy-backfill` | **MERGED** at `e722e79`. Reads `~/.cache/slack-fuse/messages/` cache; idempotent via events_message_dedup. Admin CLI gained `--source {slack-api,legacy-cache}` flag. Live smoke against real cache pulled valid payloads from C046S4RH6GG. |
| 2B Renderer library | claude (opus) | (killed) | `synap5e/feat/2b-renderer-library` | **MERGED** at `549ae15`. Ported POC B impl. 30 new render tests + 45 POC equivalence tests all pass. Author-header rendered as `<@U…>` placeholder so renames invalidate the chunk (late-resolution-aligned). Legacy renderer shim bonus deferred (file in not-allowed list). |
| 2F Test infra polish | cursor (gpt-5.3-codex-xhigh) | (killed) | `synap5e/feat/2f-test-infra` | **MERGED** at `5efe1db`. 405 → 439 tests; **0 skipped** (was 36). Manual initdb/pg_ctl postgres auto-provision (no pytest-postgresql dep). Synthetic generators cover all 9 event kinds. FUSE harness extended with lookup/getattr/read + tier_aware_channels_factory. The DB-bug-masking gap that the post-Sprint-1 review missed is fully closed. |
| 2C HTTP /resolve + /permalink | cursor (gpt-5.3-codex-xhigh) | `sprint2c-resolve` | `synap5e/feat/2c-http-resolve-permalink` | in flight (lift logic from legacy resolve/permalink modules) |
| 2D Snapshot generator | claude (opus) | (killed) | `synap5e/feat/2d-snapshots` | **MERGED** at `631bb05`. Periodic scheduler wired into slurper nursery on dedicated DB conn. Cost columns (`payload_bytes`, `events_covered`, `generation_duration_ms`, `generation_trigger`) populate; deterministic (canonical_json + sorted projection); REPEATABLE READ isolation for atomic-vs-events-log. Note: `SnapshotLine` DTO is message-shaped only — 3A will need a looser line shape for users/channel-list snapshots. |
| 2E Client projector | claude (opus[1m]) | (killed) | `synap5e/feat/2e-client-projector` | **MERGED** at `ce126fd`. 34 projector tests, all hard invariants verified. Owner-fixed conftest to use auto-postgres `database_url` fixture (worker's conftest still gated on direct env-var read, would have skipped 34 tests in default CI). **515 tests / 0 skipped — Sprint 2 complete.** Live-soak smoke deferred (workspace quiet). |

## Sprint 3 — convergence (starting now)

Sprint 2 didn't have its own review gate per the plan — straight to Sprint 3.

Dependencies:
- 3A snapshot HTTP endpoint: depends on 2D ✅
- 3B FUSE adapter: depends on 2E + 2B ✅ (Pre-3B critical review gate; double-review)
- 3C trailer logic: depends on 2E + 3B
- 3D tier CLI: independent (client schema only) ✅
- 3E cross-stream invalidation: depends on 2E + 3B

| Track | Model | tmux | Branch | Status |
|---|---|---|---|---|
| 3A snapshot HTTP endpoint | cursor (gpt-5.3-codex-xhigh) | (killed) | `synap5e/feat/3a-snapshot-http` | **MERGED** at `d4f83a6`. End-to-end test: generator → HTTP → 2E projector fetch — pass. WS now emits `snapshot_at` redirect (replaces `snapshot_required` error from 1B). Minor extension: URL includes optional `since=<offset>` for accurate `events_skipped` recording. |
| 3B FUSE adapter | claude (opus[1m]) | (killed) | `synap5e/feat/3b-fuse-adapter` | Worker reported SHIPPED; 583+68 tests. **Pre-3B dual review REJECTED**. |
| Pre-3B review (gpt-5.5 xhigh) | cursor | (killed) | n/a | **REQUEST CHANGES**. P0×2 + P1×3 + P2×1. Report at `~/.agent-handoff/2026-06-08/review-3b-gpt/report.md`. |
| Pre-3B review (gemini 3.1 pro) | cursor | (killed) | n/a | **REJECT**. 5 critical findings (3 overlap with gpt's P0/P1, 2 unique: slug collision + last_frame_at thrashing). Report at `~/.agent-handoff/2026-06-08/review-3b-gemini/report.md`. |
| 3B-fix (4 P0 + 3 P1 + 3 P2) | claude (opus[1m]) | (killed) | `synap5e/feat/3b-fuse-fixes` | **MERGED** into server-split-rebuild at `863b0fe`. Worker chose P0-1 option (a) time-aware subscriber, dropped raw last_frame_at from HealthSignature, slug assigned over hot+hidden hot-first, added `--mode split` wiring watch_health into FUSE nursery. 602 tests. |
| Pre-3B-fix re-review (gpt-5.5 xhigh) | cursor | (killed) | n/a | **REQUEST CHANGES**. Original findings closed; 2 new findings: hidden lookup broken in real ops.lookup (scope: 3B), V2 projector sink unwired (scope: 3E). Report at `~/.agent-handoff/2026-06-09/review-3bfix-gpt/report.md`. |
| Pre-3B-fix re-review (gemini 3.1 pro) | cursor | (killed) | n/a | **APPROVE**. All 5 original findings verified CLOSED; two non-blocking observations. Report at `~/.agent-handoff/2026-06-09/review-3bfix-gemini/report.md`. |
| 3B-fix-2 (hidden lookup regression) | claude (opus[1m]) | (killed) | `synap5e/feat/3b-fuse-fixes` (chained) | **MERGED** at `d5aafe6`. `ops.lookup()` no longer scans readdir output; conv-root children resolved via `fetch_channel_by_slug(allow_hidden=True)`. 4 new real-ops tests; 606 pre-rebase / 612 post-rebase tests. |
| 3D tier CLI | cursor (gpt-5.3-codex-xhigh) | (killed) | `synap5e/feat/3d-tier-cli` | **MERGED** at `c9b5835`. |
| 3E cross-stream invalidation + V2 sink | claude (opus[1m]) | (killed) | `synap5e/feat/3e-cross-stream-invalidation` | **MERGED** at `67fe7d6`. V2InvalidationSink maps ChunkRef/ThreadChunkRef/channel-list intents to V2 inodes; `cmd_mount_split` now runs the projector (WSClient) in-process with the sink + a reconnect supervisor + the health subscriber on separate psycopg connections. Added missing same-TX `chunk_mentions` lookup for `channel_added` (the `<#C…>-before-channel_added` gap; user_added/user_renamed/channel_renamed already had it from 2E). 621 tests (10 new: 8 sink units + 1 original race + 1 adversarial race). Worker deviations all justified in code: sink invalidates materialized inodes (not just notify_store-primed) because `keep_cache=True` caches even when notify_store was skipped; projector lives in-process (separate process can't invalidate this process's kernel cache); adversarial test guards the unresolved-fallback backstop (orthogonal to the cross-stream lookup by design). |

**Sprint 3 functionally complete.** All hard-gate tracks merged: 3A snapshot
HTTP, 3B FUSE adapter (+ fix + fix-2), 3D tier CLI, 3E cross-stream
invalidation. 3C (trailer JSONL log + module extraction) deferred to
Sprint 4 hygiene — pure observability, 3B already implements the trailer
correctness.

### Pre-cutover review

| Track | Model | tmux | Branch | Status |
|---|---|---|---|---|
| Post-Sprint-3 review | cursor (gpt-5.5-extra-high) | (killed) | n/a | **REQUEST CHANGES**. Verdict: legacy-default merge is safe, but split-mode opt-in bake-in needs fixes. Verifications all clean (ruff/pyright 0/0/0; pytest 621 passed, 0 skipped WITHOUT DATABASE_URL set — 2F fixture survived; smoke /health 200 + /metrics coherent). **6 substantive findings** (P0×2 + P1×4): (1) projector opens one psycopg conn per subscribed stream → 323+ conns for a 320-channel workspace, exceeds default `max_connections`; (2) snapshot apply is upsert-only, NOT full-state replacement → deleted rows survive cursor advance; (3) applier `except Exception: log; return` lets a later successful event advance cursor past a failed offset; (4) WS `_receive_loop` blocks on a full per-stream queue (HoL — module doc claims otherwise); (5) `V2InvalidationSink.channel_list_changed()` invalidates only `channel.md` + conv-root dirs, not thread.md or subtree dirs; (6) `slack-fuse tier <slug>` lookup doesn't match FUSE V2's slug logic — tests use synthetic `channels.slug` column that production schema doesn't have. Plus minor wiring gaps (split mount ignores `ClientConfig.mountpoint`; `stale_after_disconnect_s` parsed but unused). Report at `~/.agent-handoff/2026-06-09/post-sprint3-review/report.md`. |

### Post-Sprint-3 fixes

Owner decisions (from user-in-loop checkpoint):
- Merge server-split-rebuild → main immediately (legacy-default safe per reviewer).
- Single comprehensive fix track on Opus 1M for all 6 findings.

Server-split-rebuild merged to main at `99f991b` (62 commits ahead of
origin/main; not pushed per standing directive).

| Track | Model | tmux | Branch | Status |
|---|---|---|---|---|
| Post-Sprint-3 fixes | claude (opus[1m]) | (killed) | `synap5e/feat/post-sprint3-fixes` | Worker SHIPPED at `e53c6a5`. 636 tests / 0 skipped without DATABASE_URL; ruff/pyright 0/0/0. New: ConnectionPool (bounded, default 8, configurable; appliers borrow-per-event); snapshot DELETE-then-upsert all-one-TX for full-state semantics; singleton snapshot redirects gated to channel streams only; applier exceptions raise → WSClient teardown → reconnect from durable cursor; unbounded per-stream queues + send_nowait removes WS HoL; channel_list_changed invalidates ALL materialized inodes; tier CLI uses real V2 slug logic against production schema (synthetic `channels.slug` column REMOVED from tests). Worker verified each new regression fails on pre-fix behavior via temporary revert. Wired residuals: `ClientConfig.mountpoint` honored by split mount; `projector_pool_size` config. Deferred: `stale_after_disconnect_s` / `stale_trailer_enabled` / `catchup_window_s` config wiring; 3C trailer-decision JSONL. |
| Re-review post-Sprint-3 fixes | cursor (gpt-5.5-extra-high) | (killed) | n/a | **REQUEST CHANGES**. 6/7 closed; P0-B partially closed with 2 scoped holes: empty channel snapshot bypasses full-state replacement; snapshot-delete invalidations don't reach sink. Report at `~/.agent-handoff/2026-06-09/review-post-sprint3-fixes/report.md`. |
| Post-Sprint-3 fixes-2 (P0-B holes) | claude (opus[1m]) | (killed) | `synap5e/feat/post-sprint3-fixes` (chained) | **MERGED** at `da72097`. Empty channel bodies now route through full-state replacement (singleton streams keep cursor-only shortcut since server-side gate prevents singleton snapshot redirects). `_delete_chunks_absent_from_snapshot` returns deleted refs via `DELETE ... RETURNING`; refs reach the sink post-commit alongside upsert refs. 639 tests. New regressions verified failing on pre-fix tree. Design deviation: no direct end-to-end thread.md test (any surviving reply re-upserts and fires invalidation for the same inode; deleted-last-reply drops slug resolvability) — composition of projector test (deleted ThreadChunkRef reaches sink) + existing FUSE test (ref → inode) accepted by reviewer. |
| Re-re-review post-Sprint-3 fixes-2 | cursor (gpt-5.5-extra-high) | (killed) | n/a | **APPROVE**. Both P0-B holes closed; composition argument accepted. Verifications green (ruff/pyright/pytest 639/0-skipped). Report at `~/.agent-handoff/2026-06-09/review-post-sprint3-fixes-2/report.md`. |

**Pre-cutover gate cleared.** All post-Sprint-3 findings closed. Branch
merged to main at `da72097`. `SLACK_FUSE_MODE=split` is ready for opt-in
bake-in.

### Next owner action

**User-in-loop touchpoint.** Pre-cutover review approved. Decide:
- Start the 4-week `SLACK_FUSE_MODE=split` opt-in bake-in clock now.
- Land deferred 3C trailer-decision JSONL log + module extraction before
  the bake-in for forensic observability.
- Land deferred config wiring (`stale_after_disconnect_s` /
  `stale_trailer_enabled` / `catchup_window_s`) — reviewer flagged
  non-blocking.
- Push `main` to `origin/main` (held off per standing "no push" directive).
| 2C HTTP /resolve + /permalink | cursor (gpt-5.3-codex-xhigh) | (killed) | `synap5e/feat/2c-http-resolve-permalink` | **MERGED** at `e2d8a59`. Lifting strategy: option 2 (copy bodies into server modules), keeps legacy independent. CLI gained `--server-url` proxy mode. |
| (owner inline) flake fix | n/a | n/a | n/a | Bumped WS test timeouts (1.0→5.0s default, 0.5→3.0s explicit) — was flaking under full-suite + cold-Pg load after 2F auto-provision landed. |
| 1G Socket Mode payload conformance | cursor (gpt-5.3-codex-xhigh) | (killed) | `synap5e/feat/sprint1g-message-payload-conformance` | **MERGED** at `06bdad6`. Live `message` events now Message.model_validate(...).model_dump('json'), byte-equivalent to backfill. Conformance test asserts the equivalence against a conversations.history-derived envelope with reactions+files+edited+reply metadata. |
| 1B WS server | cursor (gpt-5.5-extra-high) | (killed) | `synap5e/feat/sprint1b-ws-server` | **MERGED** at `<after-this>`. Added SNAPSHOT_REQUIRED to ErrorCode enum (sanctioned per prompt). LISTEN protocol: `NOTIFY new_event, '<stream-id>'` (or empty payload for wake-all fallback) — 1A coordinates by emitting these in the offset-assignment TX. |
| 1C HTTP /health + /metrics | cursor (gpt-5.3-codex-xhigh) | (killed) | `synap5e/feat/sprint1c-http-health-metrics` | **MERGED**. Custom trio+h11 server, no new dep. `serve_http_on_listeners` exposed for 1B WS to compose (Upgrade-header path landed in 1B's PR). |
| 1D debug subscribe CLI | cursor (gpt-5.3-codex-xhigh) | (killed) | `synap5e/feat/sprint1d-debug-subscribe-cli` | **MERGED**. `tools/debug_subscribe.py` + 7 unit tests. Auto-responds to ping with pong. Manual smoke deferred until 1A's slurper is running. |

Cross-track coordination resolved: 1A → 1B `NOTIFY new_event, '<stream-id>'`
protocol documented in 1B's commit. Owner relays to 1A worker (still
in flight) when checking status.
