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
| 1F wire binary + same-port dispatch | claude (opus) | `sprint1f-wire` | `synap5e/feat/sprint1f-wire-binary` | in flight |
| 1G Socket Mode payload conformance | cursor (gpt-5.3-codex-xhigh) | `sprint1g-payload` | `synap5e/feat/sprint1g-message-payload-conformance` | in flight |
| 1B WS server | cursor (gpt-5.5-extra-high) | (killed) | `synap5e/feat/sprint1b-ws-server` | **MERGED** at `<after-this>`. Added SNAPSHOT_REQUIRED to ErrorCode enum (sanctioned per prompt). LISTEN protocol: `NOTIFY new_event, '<stream-id>'` (or empty payload for wake-all fallback) — 1A coordinates by emitting these in the offset-assignment TX. |
| 1C HTTP /health + /metrics | cursor (gpt-5.3-codex-xhigh) | (killed) | `synap5e/feat/sprint1c-http-health-metrics` | **MERGED**. Custom trio+h11 server, no new dep. `serve_http_on_listeners` exposed for 1B WS to compose (Upgrade-header path landed in 1B's PR). |
| 1D debug subscribe CLI | cursor (gpt-5.3-codex-xhigh) | (killed) | `synap5e/feat/sprint1d-debug-subscribe-cli` | **MERGED**. `tools/debug_subscribe.py` + 7 unit tests. Auto-responds to ping with pong. Manual smoke deferred until 1A's slurper is running. |

Cross-track coordination resolved: 1A → 1B `NOTIFY new_event, '<stream-id>'`
protocol documented in 1B's commit. Owner relays to 1A worker (still
in flight) when checking status.
