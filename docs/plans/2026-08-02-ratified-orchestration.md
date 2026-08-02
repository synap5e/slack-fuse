# Ratified backlog orchestration (2026-08-02)

Plan for driving the six Ratified BACKLOG items through implementation.
Ratified list at time of writing (post-decision-recording in `a5fff71`):

| # | Item | Effort | Autonomous |
|---|---|---|---|
| 1 | Skip thread-expansion | 2–3 days | Yes |
| 2 | Workspace channels view | 1–2 days | Yes |
| 3 | Coalesced disk projection | 5–8 days | Yes |
| 4 | Probe-event pattern | 3–5 days | Yes |
| 5 | Repo cleanup — worktrees | 1–2 hrs | Yes |
| 6 | game-mode ordering | 15–30 min | No (needs `~/bin/game-mode` edit) |

Item 6 is out of scope for this plan (operator script, not slack-fuse).

## Shape

Three phases, not five items in parallel. Review capacity is the
bottleneck — two live handoffs is comfortable; three starts blurring
diffs. Each handoff runs codex 5.6-sol xhigh with `--worktree` for
isolation (proven pattern from 2026-08-02 morning work).

### Phase 1 — Parallel small wins (~1 day)

| Track | Owner | Notes |
|---|---|---|
| Repo cleanup | **Inline (me)** | Mechanical `git worktree remove` per `.wt/synap5e/feat/*` and `.wt/handoff/*`. Handoff overhead for a task about removing worktrees is absurd. 1–2 hrs. |
| Workspace channels view | **Handoff A** | Prompt already drafted at `/home/simon/.agent-handoff/2026-06-27/workspace-channels-view/prompt.md`. Small (~250 LoC), low bounce risk. Soft gate. |
| Skip thread-expansion | **Handoff B** | Server-only (`slack_fuse_server/backfill/`). No file overlap with A. Hard gate — correctness-sensitive predicate. |

### Phase 2 — Probe events framework (~2 days)

| Track | Owner | Notes |
|---|---|---|
| Probe events | **Handoff C** | Server-side sweep task + registry + first probe. Client-side minimal (new event kinds in `slack_fuse/projector/apply.py`). Solo phase because probe events touches `apply.py` which will overlap with Phase 3. Hard gate — framework design matters (registry extensibility). |

### Phase 3 — Coalesced projection (~1 week, staged)

5–8 eng-days as one lump. Break into three serial sub-handoffs, hard
gate after each — compressing into a single 8-day handoff makes
bounces expensive.

| # | Sub-handoff | Focus |
|---|---|---|
| D1 | Disk projection writer + coalescer | Write-side only. Renders to `~/.cache/slack-fuse/projection/`, 5s coalescer, restart-safe. No FUSE changes. |
| D2 | Tier logic in `fuse_ops_v2.py` | Read-side. Per-read dirty-check, serve-from-disk when clean, JIT when dirty. |
| D3 | Invalidation ordering + adversarial tests | Prove disk write lands before dirty→clean flip; race-injection tests. |

## Review gates (bounce until good)

- **A (workspace channels)** — soft. Verify Tier 2 budget respected;
  verify ghost-file contract matches existing `_workspace/gaps.md`
  warmer; verify no bot-token misuse.
- **B (skip-thread-expansion)** — hard. Verify test coverage matches
  the three ratified decisions (no age gate, no expansion marker,
  spans-only); verify no regression to existing backfill test path;
  verify the SQL predicate uses active-fold rows not raw event rows.
- **C (probe events)** — hard. Verify single-sweep-with-registry
  landed (not per-probe tasks); verify a stub second probe can be
  added by dropping in a registry entry; verify tier budget accounting.
- **D1** — hard. Coalescer must not measurably impact live FUSE read
  latency; disk writes must be all-or-nothing (temp+rename).
- **D2** — hard. Tier-logic per-read decision must add <1ms to
  hot-path FUSE reads; disk-serve path must be provably byte-equal
  to JIT for clean files.
- **D3** — hard. Race-injection tests must not produce a stale read
  that isn't reproduced by a follow-up read.

## Abort conditions

**Universal** (escalate to Simon, don't burn more tokens):

1. Same issue bounces 3+ times → spec is wrong, not impl. Escalate.
2. Handoff needs a design decision the ratified answers don't cover.
   Escalate.
3. Handoff needs to touch files outside the item's stated scope.
   Escalate.
4. Cost/time projection exceeds initial estimate by >2×. Escalate.
5. Full test suite regresses outside the item's area. Abort, revert,
   escalate.

**Per-item specific** (in addition to universal):

- **B (skip-thread-exp)**: any regression in existing backfill tests;
  any hint the predicate misreads deleted or edited rows.
- **A (workspace channels)**: `search.messages` bursts exceed Tier 2
  ceiling under load; server 5xx cascade from the new endpoint.
- **C (probe events)**: framework can't cleanly accommodate a stub
  second probe; first probe (`channel_message_count_probed`) doesn't
  feed the % calc correctly.
- **D1**: coalescer measurably impacts live FUSE read latency; disk
  writes fail atomically.
- **D2**: tier-logic per-read decision adds >1ms to hot-path FUSE
  reads; any test showing the disk-serve path returns bytes different
  from JIT for a clean file.
- **D3**: any race-injection test can produce a stale read that isn't
  reproduced by a follow-up read.

## Deploy story per item

- **Skip-thread-exp, workspace channels, probe events**: all need a
  server image rollout after merge. Standard `slack-fuse-server` roll.
- **Coalesced projection**: client-only, but touches enough of the
  mount that I'd want a canary on pro before flow. Flow gets updated
  via `home-manager` switch after pro proves stable for >6h.

## Cost note

5 handoffs (A, B, C, D1, D2, D3) at codex 5.6-sol xhigh, probably
2–3 bounce cycles each. That's ~15–20 codex sessions total. Non-trivial
API spend — worth flagging before starting.

## Success criteria

Plan is complete when:
- Items 1, 2, 3, 4, 5 in BACKLOG are moved to Resolved with commit refs.
- Both pro and flow run the merged code without regression for >24h.
- No new Agent-raised items were promoted to Ratified without Simon's
  explicit blessing during execution.
