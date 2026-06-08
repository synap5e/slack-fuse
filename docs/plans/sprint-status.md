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
| 1A slurper | claude (opus) | `sprint1a-slurper` | `synap5e/feat/sprint1a-slurper` | in flight |
| 1B WS server | cursor (gpt-5.5-extra-high) | `sprint1b-ws-server` | `synap5e/feat/sprint1b-ws-server` | in flight |
| 1C HTTP /health + /metrics | cursor (gpt-5.3-codex-xhigh) | `sprint1c-http` | `synap5e/feat/sprint1c-http-health-metrics` | in flight |
| 1D debug subscribe CLI | Codex | TBD | TBD | queued (spawn when one of 1A/1B/1C completes) |

Per the cap-of-3 decision, 1D queued until a slot frees.

Cross-track coordination needed: 1A's slurper must `NOTIFY new_event`
after each INSERT into the events table; 1B's tail loop `LISTEN`s.
1B was told to document the protocol; owner monitors for either
worker raising it.
