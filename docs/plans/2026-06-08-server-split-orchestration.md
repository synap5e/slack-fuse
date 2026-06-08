# Build orchestration plan: server-split

**Status**: Operational
**Author**: Simon Pinfold (with Claude as long-running owner)
**Date**: 2026-06-08
**RFC**: [`../rfcs/2026-05-server-split.md`](../rfcs/2026-05-server-split.md)

## Purpose

How the server-split rebuild gets built. The RFC describes the
architecture; this doc describes the construction sequence, the
parallelism strategy, the agent-orchestration patterns, and the
runbook the owner agent follows day-to-day.

The owner agent is a long-running Claude Code session (`/owner` skill).
Workers are short-lived `agent-handoff` or `specced-handoff` agents,
several in flight at a time. Critical-review agents fire at gates.
Conflicts get rebased by the loser of the race, not resolved by the
owner.

## Decisions (recorded 2026-06-08)

These answers came from the user at orchestration kickoff. Recorded
here so future owner sessions don't re-ask.

| Question | Answer |
|---|---|
| Deployment target | k8s eventually, **but local Python service for now** (the rebuild ships against `uv run slack-fuse-server` first; manifests + systemd come after Sprint 3) |
| Client postgres | One per machine, local during dev |
| Sprint 2 concurrency cap | 2–3 concurrent worker tracks |
| POC A Slack target | Reuse the real Slack workspace (user token, read-only via Socket Mode + history) |
| Push to `origin` during build | **No.** Everything stays local until cutover. No remote backup, but no risk of exposing in-flight architecture |
| Integration target | **Fresh-slate worktree at `.wt/server-split-rebuild/`** branched from `main`. All worker branches branch from `server-split-rebuild` and merge back there. `main` stays untouched until cutover replaces v1 wholesale |
| Owner-loop cadence | `/wait-for 15min` pattern: spawn workers → wait up to 15m → check stalls → bump or resolve → repeat |
| Cost ceiling | None. Use the best model per the cheatsheet |

## Workflow setup

Before any Sprint 0 work starts, the owner executes once:

```bash
cd /home/simon/agentic/slack-fuse
git wt server-split-rebuild      # creates .wt/server-split-rebuild/, branch off main
```

Owner work happens in that worktree from then on. Worker handoffs
launch with `--cwd <their own worktree path>` and branch from
`server-split-rebuild`. Owner merges worker branches into
`server-split-rebuild`. `main` is never touched during the rebuild.

When Sprint 4 cutover completes:

```bash
# In the main checkout, NOT the rebuild worktree
git checkout main
git merge --squash server-split-rebuild
git commit -m "feat: server-split rebuild (replaces v1)"
git tag main-pre-split <old-main-sha>   # preserve v1 history
```

## Owner loop pattern

Realised via `ScheduleWakeup` at 15-minute intervals when workers
are in flight. Each wake-up:

1. Read incoming agent-messages (push-delivered to next-turn context).
2. For workers reporting done: review their PR (`git pr` workflow),
   run any required gate-review agent, merge to `server-split-rebuild`
   if green or message the worker if fix needed.
3. For workers reporting stuck: triage per *Edge case handling*.
   Mechanical clarifications resolved inline; ambiguous/cross-cutting
   escalated to the user.
4. For workers that haven't reported in this cycle: `agent-message
   <session> "status?"` to bump.
5. If anything is still in flight or just bumped, schedule the next
   wake-up. If everything done and no fresh work to spawn, surface
   the sprint-boundary check-in to the user.

## Repo and branch strategy

**Single repo, long-lived integration branch, worktrees per task.**

- All rebuild work lives in the existing `slack-fuse` repo. No new
  repo. The server package lives at `slack_fuse_server/` alongside
  the existing `slack_fuse/` (client).
- A long-lived `server-split` integration branch is the target for
  all rebuild PRs. `main` continues to receive bugfixes to the
  current single-process implementation; `server-split` rebases off
  `main` weekly.
- Worktrees live at `.wt/<branch>/` (deterministic per the existing
  `git wt` convention). Each worker gets its own worktree against its
  own branch, branched off `server-split`.
- Branches follow `synap5e/<type>/<slug>` (e.g.
  `synap5e/feat/sprint0-schemas`, `synap5e/feat/2a-legacy-backfill`).

When v1 is bake-in-complete (Sprint 4), `server-split` squash-merges
into `main` as a single commit; `main`'s history is preserved on a
`main-pre-split` tag.

## Owner ↔ worker communication

Owner uses `agent-handoff` to spawn workers. Each handoff prompt
includes the owner's session id + tmux session as the reply target,
so workers can:

```bash
~/bin/agent-message --session <owner-session-id> "..."
```

Owner runs each substantial worker via `specced-handoff` so the spec
captures acceptance criteria up-front and a quality gate evaluates
the work before the owner has to context-switch into it. For small,
narrow tasks (one-shot edits, mechanical refactors), plain
`agent-handoff` is fine.

Owner runs critical-review agents at gate points (see *Review gates*
below). Their reports come back as `agent-message` to the owner.

## Sprint sequence

| Sprint | Name | Wall-clock | Parallelism |
|---|---|---|---|
| 0 | Interface freeze | 1–2 weeks | Single PR, single agent (or owner directly) |
| 0' | POCs A and B | parallel with Sprint 0 | 2 agents |
| 1 | Server vertical slice | 2–3 weeks | 1–2 agents |
| 2 | Parallel implementation tracks | 2–4 weeks | 4–6 agents at once |
| 3 | Convergence | 2 weeks | 2–3 agents |
| 4 | Cutover + bake-in | 1 week active + 4 weeks bake-in | 1 agent + owner |

Sprints 0/0' are the only ones where wall-clock matters because
downstream tracks are blocked. Sprint 2 is where parallelism pays
off — half a dozen agents working independently.

## Sprint 0: Interface freeze

**One PR. No runtime behaviour. Pyright passes; Pydantic round-trip
tests pass. This is the contract.**

### What ships

- `slack_fuse_server/schema.sql` — full server schema per RFC §Schemas
- `slack_fuse/schema.sql` — full client schema per RFC §Schemas
- `slack_fuse/migrations/runner.py` — simple sequential SQL migration runner
- `slack_fuse_server/wire/frames.py` — Pydantic models for every WS frame
  (`Subscribe`, `Event`, `CaughtUp`, `SnapshotAt`, `Error`, `Ping`,
  `Pong`)
- `slack_fuse_server/http/dto.py` — Pydantic models for `/resolve`,
  `/permalink`, `/snapshot`, `/metrics` requests + responses
- `slack_fuse_server/backfill/types.py` — `Backfiller` Protocol + return
  dataclasses
- `slack_fuse_render/__init__.py` — public API: `render_message_structural`,
  `resolve_mentions`, `extract_mention_user_ids`,
  `channel_md_frontmatter`, `thread_md_frontmatter`. All bodies raise
  `NotImplementedError`.
- `slack_fuse_render/resolvers.py` — `UserResolver` / `ChannelResolver`
  Protocols + frozen dataclass types (`UserId`, `ChannelId`,
  `UserView`, `ChannelView`)
- `slack_fuse_server/config.py` + `slack_fuse/config.py` — Pydantic
  config loaders
- `tests/wire/test_frame_roundtrip.py` — golden tests proving every
  frame Pydantic model round-trips
- `tests/http/test_dto_roundtrip.py` — same for HTTP DTOs
- `tests/conftest.py` — postgres fixture (`pg_temp` per test),
  fake-Slack httpx mock transport, synthetic-event generator stub,
  in-memory FUSE harness skeleton

### Worker assignment

Specced-handoff to a single agent. **Worker model**: Claude Opus
(interfaces require care; small enough to fit comfortably in
standard context). **Reviewer model**: GPT-5.5 xhigh, with the
explicit charter of "do the stubs match the RFC exactly?" — see
post-Sprint-0 gate. One worktree, one PR. Owner does final review
against the RFC before merge.

### Acceptance

- `uv run ruff check . && uv run basedpyright && uv run pytest -q` all green
- Every interface mentioned in the RFC has a stub here
- Owner runs an RFC-divergence review agent (see *Review gates*)
  before merging — the gate is "interfaces match the RFC exactly"

### Risks the Sprint 0 worker should also surface

Two risks that emerged during orchestration planning. These don't
have to be RESOLVED in Sprint 0, but the worker should think about
them and flag in their report if either looks like an RFC gap:

1. **Backfiller idempotency under edits.** Legacy cache may have
   message version A (pre-edit); SlackApiBackfiller, run second, may
   see version B (post-edit). The RFC's `(stream, slack_ts)` dedup
   constraint keeps the first-written version and drops B —
   meaning legacy keeps an outdated version. Three possible
   resolutions: (a) API backfill runs FIRST for editable history,
   legacy fills only what API didn't reach; (b) constraint becomes
   `ON CONFLICT DO UPDATE` keyed by payload version; (c) we accept
   this as a known limitation and address with a periodic
   "refresh-from-API" sweep. Worker should pick a default and flag.

2. **Cross-stream race + per-stream queues.** Per-stream queues
   apply events on independent applier tasks. A `message` event on
   `channel:CX` mentioning U999 might apply concurrently with a
   `user_added` event for U999 on the `users` stream. The
   `chunk_mentions` invalidation logic could miss the just-written
   row if the user-added applier reads `chunk_mentions` before the
   message applier has committed. Worker should either (a) require
   both applies to coordinate via a shared lock / advisory lock,
   (b) prove the race is benign (e.g. invalidation always sees a
   stable snapshot due to MVCC), or (c) flag as needing RFC
   spec-work in Sprint 2E.

## Sprint 0': POCs (parallel with Sprint 0)

These don't need Sprint 0's interfaces. They de-risk the riskiest
assumptions before downstream commits.

### POC A: events-to-postgres

- Spec: lift Socket Mode loop from current `slack_fuse/socket_mode.py`,
  add a `psycopg` write to a hand-coded `events_poc(stream, ts, kind, payload)` table
- Run for 48 hours against the real workspace
- Report: event volume per hour, payload size distribution per kind,
  any unexpected event shapes

### POC B: renderer-split byte-equivalence

- Spec: split existing `slack_fuse/mrkdwn.py:convert` into
  `convert_structural` (bold/italic/links/code/blockquotes) +
  `resolve_mentions` (`<@U…>`, `<#C…>`)
- Golden test: for every input the current single-pass renderer
  produces output X, the new two-pass pipeline (with a populated
  resolver) must also produce X byte-for-byte
- Use the existing renderer test corpus + a few hand-written edge
  cases (cached-name normalisation, nested mentions, channel
  mentions inside code spans)
- Report: tests pass; any byte-mismatches identified and either
  fixed in the structural pass or escalated as RFC clarifications

### Worker assignment

Two separate agent-handoff workers, run in parallel during Sprint 0.
**POC A model**: Codex (mechanical lift of Socket Mode + write loop).
**POC B model**: Claude Opus (renderer-split correctness reasoning).
Both write reports to `docs/plans/poc-reports/`. Owner reads
reports, decides any RFC adjustments before Sprint 1. No formal
reviewer — owner reads the reports directly.

## Sprint 1: Server vertical slice

Goal: working slurper + minimal WS protocol, observed for a week
against the real workspace. No client work yet.

### Tracks

| Track | Files | Writer model | Spec sketch |
|---|---|---|---|
| 1A | `slack_fuse_server/slurper/{api,socket,offsets,health}.py` | Claude Opus (Socket Mode handler rewrite is subtle) | Lift SlackClient, rewrite Socket Mode handler to write events via the Sprint-0 offset-assignment pattern, emit slurper-health events |
| 1B | `slack_fuse_server/wire/server.py` | GPT-5.5 xhigh (concurrent WS connection handling + framing) | WS server, accepts subscribe, emits event/caught_up/error/ping/pong. No snapshot_at yet (no snapshots exist) |
| 1C | `slack_fuse_server/http/server.py` | Codex (mechanical HTTP handlers) | `/health` + `/metrics` only |
| 1D | `tools/debug_subscribe.py` | Codex (small CLI) | CLI: open WS, subscribe to <stream>, print events |

Sprint-1 reviewer: opposite-family of the dominant writer. Since 1A
is Opus and is the biggest of these, default reviewer is GPT-5.5
xhigh. If 1A/1B reveal anything subtle during their build, run an
additional Gemini 3.1 Pro wildcard review.

Tracks can run in parallel as soon as Sprint 0 is merged. 1A/1B/1C
all live in `slack_fuse_server/` so worktree boundaries matter — see
*File ownership* below.

**Deployment shape**: local Python process run as `uv run
slack-fuse-server`. No systemd unit, no Docker image, no k8s
manifest in v1. Those land in a separate post-Sprint-3 phase once
the architecture is proven against the user's real workspace.

### Acceptance

- Server runs against the real workspace for 7 consecutive days
  without crash
- `tools/debug_subscribe.py channel:CXXX` shows live activity
- `/metrics` returns coherent JSON with `slurper.last_event_at`
  advancing
- `/health` returns 200
- Owner runs an RFC-divergence review on the merged result

## Sprint 2: Parallel implementation tracks

This is the wide-fan-out sprint. Tracks below are independent given
Sprint 0's contracts; they CAN run concurrently as separate worktrees.

### Track 2A — LegacyCacheBackfiller

- **Worker model**: Codex (coding-optimized; mechanical lift from existing JSON-cache shape)
- **Reviewer model**: Claude Opus (opposite family)
- **Branch**: `synap5e/feat/2a-legacy-backfill`
- **Files owned exclusively**:
  - `slack_fuse_server/backfill/legacy.py` (new)
  - `tests/backfill/test_legacy.py` (new)
- **Files touched read-only**: `slack_fuse_server/backfill/types.py` (uses Protocol from Sprint 0), `slack_fuse_server/db.py`
- **Acceptance**:
  - Reads an actual `~/.cache/slack-fuse/` directory and produces a
    stream of `message` items matching the wire `Event` payload shape
  - Idempotent: running it twice against the same target events table
    inserts zero rows on the second run (relies on the
    `events_message_dedup` partial unique index from Sprint 0)
  - Reports per-channel message count, total events written, time
    elapsed
- **Spec gate**: byte-equivalence between legacy-import-then-render
  and current-codebase-render for a 10-channel sample

### Track 2B — Renderer library

- **Worker model**: Claude Opus (format reasoning; the two-pass split is subtle)
- **Reviewer model**: GPT-5.5 xhigh (opposite family)
- **Branch**: `synap5e/feat/2b-renderer-library`
- **Files owned exclusively**:
  - `slack_fuse_render/render.py` (new)
  - `slack_fuse_render/mrkdwn.py` (new — the split convert pass)
  - `tests/render/` (new tree)
- **Files touched read-only**: Sprint-0 stubs in `slack_fuse_render/__init__.py`, `slack_fuse_render/resolvers.py`
- **Acceptance**:
  - All Sprint-0 NotImplementedError stubs replaced with working impls
  - POC B's golden test corpus + ~50 additional edge-case messages
    pass byte-equivalence
  - Existing `slack_fuse/renderer.py` and `slack_fuse/mrkdwn.py` are
    replaced with thin re-export shims that import from
    `slack_fuse_render`, so current single-process slack-fuse keeps
    working

### Track 2C — HTTP /resolve + /permalink

- **Worker model**: Codex (lift the existing URL parsing from `slack_fuse/resolve.py`; coding-shape work)
- **Reviewer model**: Claude Opus (opposite family)
- **Branch**: `synap5e/feat/2c-http-resolve-permalink`
- **Files owned exclusively**:
  - `slack_fuse_server/http/resolve.py` (new)
  - `slack_fuse_server/http/permalink.py` (new)
  - `tests/http/test_resolve.py`, `tests/http/test_permalink.py` (new)
- **Files touched read-only**: existing `slack_fuse/resolve.py`,
  `slack_fuse/permalink.py` (lift their logic, don't modify them yet)
- **Acceptance**:
  - Endpoints accept the Sprint-0 DTOs, return correct paths/URLs
    for a curated test corpus
  - The existing `slack_fuse/__main__.py` CLI commands gain a
    `--server-url` flag that, when set, proxies through the server
    instead of importing local logic

### Track 2D — Snapshot generator

- **Worker model**: Claude Opus (transactional correctness + cost-metrics columns)
- **Reviewer model**: GPT-5.5 xhigh (opposite family)
- **Branch**: `synap5e/feat/2d-snapshots`
- **Files owned exclusively**:
  - `slack_fuse_server/snapshot/{generator,scheduler}.py` (new)
  - `tests/snapshot/` (new tree)
- **Files touched read-only**: server schema, wire frames
- **Acceptance**:
  - Periodic worker generates snapshots per the
    `every 5000 events or 24h` cadence
  - Cost columns (`payload_bytes`, `events_covered`,
    `generation_duration_ms`, `generation_trigger`) populate
    correctly
  - Snapshot output is deterministic and reproducible for a given
    `(stream, at_offset)`

### Track 2E — Client projector

- **Worker model**: Claude Opus (biggest single track; per-stream queue concurrency + chunk write logic + cursor management; use the 1M-context variant if the codebase grows past 200k tokens)
- **Reviewer model**: GPT-5.5 xhigh PLUS Gemini 3.1 Pro wildcard (two independent reviews; this track's scope and correctness surface justifies it)
- **Branch**: `synap5e/feat/2e-client-projector`
- **Files owned exclusively**:
  - `slack_fuse/projector/{__init__,ws_client,per_stream,apply}.py`
  - `slack_fuse/projector/snapshot_fetch.py` — HTTP snapshot client
  - `tests/projector/` (new tree)
- **Files touched read-only**: client schema, wire frames, renderer
  Protocol stubs (the actual renderer impl from 2B may or may not be
  done yet — use a stub UserResolver/ChannelResolver in tests)
- **Acceptance**:
  - Against a synthetic event-stream fixture (from Sprint 0), chunks
    + thread_chunks + chunk_mentions populate correctly
  - Per-stream queues prevent HoL: a synthetic test sends 1000
    events to stream A with a deliberately-slow apply hook, then
    100 events to stream B; B's chunks appear before A's complete
  - Idempotent replay: applying the same event stream twice
    produces identical DB state
  - Cursor advances correctly; `caught_up` frame triggers
    `stream_caught_up` insert
  - **Concurrency invariants enforced** (per the post-Sprint-0 review
    of the cross-stream race; see RFC §FUSE read path → Unresolved-fallback
    / kernel-cache invariant):
    - (i) chunk + chunk_mentions row written in a SINGLE postgres TX
    - (ii) user_added / channel_added / user_renamed / channel_renamed
      events: UPSERT + chunk_mentions-SELECT + invalidate_inode calls
      all in a SINGLE postgres TX
    - **Required concurrency test** (Sprint 2E gate): synthetic
      reproduction of the cross-stream race: user_added's lookup
      runs before the matching message's TX commits → user_added
      commits → message commits → FUSE read renders with fallback
      literal → assert that notify_store is SKIPPED (per the
      unresolved-fallback rule) and the subsequent read re-renders
      correctly. If notify_store is called on a fallback-bearing
      read, the test fails.

### Track 2F — Test infra polish

- **Worker model**: Codex (mechanical fixtures + harnesses)
- **Reviewer model**: skip formal review; owner reads the PR — these are test fixtures, no product semantics
- **Branch**: `synap5e/feat/2f-test-infra`
- **Files owned exclusively**:
  - `tests/_fake_slack/` (new) — full httpx mock transport for every Slack endpoint we call
  - `tests/_synthetic_events/` (new) — generators for plausible event streams
  - `tests/_fuse_harness/` (new) — in-memory pyfuse3 op invocation
- **Acceptance**:
  - Every other Sprint-2 track's tests can import from these
  - Documented in `tests/README.md`

### File ownership matrix (Sprint 2)

To make worktree conflicts predictable, every file is owned by at
most one track. The owner allocates files BEFORE spawning tracks.

```
slack_fuse_server/backfill/legacy.py     → 2A
slack_fuse_server/backfill/api.py        → 1A (already shipped Sprint 1)
slack_fuse_server/wire/server.py         → 1B (already shipped Sprint 1)
slack_fuse_server/wire/frames.py         → Sprint 0 (frozen; PRs that need changes go through the owner)
slack_fuse_server/http/server.py         → 1C (Sprint 1)
slack_fuse_server/http/resolve.py        → 2C
slack_fuse_server/http/permalink.py      → 2C
slack_fuse_server/http/snapshot.py       → Sprint 3 (depends on 2D snapshots)
slack_fuse_server/snapshot/*.py          → 2D
slack_fuse_server/slurper/*.py           → 1A (Sprint 1; no Sprint 2 work)
slack_fuse_render/*.py                   → 2B
slack_fuse/projector/*.py                → 2E
slack_fuse/fuse_ops_v2.py                → Sprint 3
slack_fuse/schema.sql                    → Sprint 0 (frozen)
slack_fuse_server/schema.sql             → Sprint 0 (frozen)
tests/_fake_slack/*                       → 2F
tests/_synthetic_events/*                 → 2F
tests/_fuse_harness/*                     → 2F
tests/<track>/*                           → that track
```

Anything not listed: PR must go through the owner. The Sprint 0
contracts (schemas, frame models, DTOs, Protocols) are frozen.
Modifications require an explicit "RFC clarification needed" gate
escalation.

## Sprint 3: Convergence

Things that need Sprint 2 substantially done.

### Track 3A — HTTP /snapshot endpoint

- **Depends on**: 2D snapshot generator
- **Files**: `slack_fuse_server/http/snapshot.py`, `tests/http/test_snapshot.py`
- **Writer model**: Codex (mechanical streaming endpoint)
- **Reviewer model**: Claude Opus (opposite family)
- **Acceptance**: Streams JSONL response, gzip-encoded, against a
  populated snapshots table

### Track 3B — FUSE adapter

- **Depends on**: 2E projector (chunks populate), 2B renderer
- **Files**: `slack_fuse/fuse_ops_v2.py` (parallel to existing
  `fuse_ops.py`; legacy stays for cutover safety)
- **Writer model**: Claude Opus (kernel-cache invariants + tier-aware
  dispatch are the riskiest correctness surface in the rebuild;
  pyfuse3 attribute-stub awkwardness needs careful navigation)
- **Reviewer model**: GPT-5.5 xhigh + Gemini 3.1 Pro wildcard (the
  Pre-3B-merge gate; two independent reviews because the failure
  mode is "stale bytes served forever from kernel cache" which is
  hard to notice)
- **Acceptance**:
  - Tier-aware readdir / lookup
  - inodes table populates
  - Day-range bound computation correct across timezones
  - notify_store + invalidate coordination respects the trailer rule

### Track 3C — Trailer logic

- **Depends on**: 2E projector + 3B FUSE adapter
- **Files**: `slack_fuse/projector/trailer.py`,
  `slack_fuse/fuse_ops_v2.py` (small additions)
- **Writer model**: Claude Opus (cross-cuts FUSE read + projector
  health-event handling)
- **Reviewer model**: GPT-5.5 xhigh
- **Acceptance**: Three trailer conditions per RFC fire correctly;
  JSONL log writes; transition invalidation pings all primed inodes

### Track 3D — Tier CLI

- **Depends on**: client schema only (Sprint 0)
- **Files**: `slack_fuse/cli/tier.py`
- **Can start anytime**: but only useful once 3B FUSE adapter is in
- **Writer model**: Codex (small CLI)
- **Reviewer model**: Claude Opus (opposite family)
- **Acceptance**: `slack-fuse tier <slug> <hot|hidden|blocked>` writes
  channels.tier correctly; subsequent readdir reflects it

### Track 3E — Cross-stream invalidation (`user_added` race)

- **Depends on**: 2E projector + 3B FUSE adapter
- **Files**: `slack_fuse/projector/apply.py` (additions),
  `slack_fuse/fuse_ops_v2.py` (additions)
- **Writer model**: Claude Opus (the race itself is what made the
  reviewer flag it; the fix needs care)
- **Reviewer model**: GPT-5.5 xhigh
- **Acceptance**:
  - The two TX invariants from 2E are present and enforced (NOT
    spread across separate transactions)
  - Two synthetic race tests pass:
    1. Original race: message before user_added → expects
       chunk_mentions invalidation lookup to find the matching row
       and call invalidate_inode
    2. Reviewer's adversarial race (from post-Sprint-0 review): user_added's lookup runs BEFORE
       message TX commits → falls back at FUSE-read time → assert
       notify_store SKIPPED → next read re-renders correctly. This
       enforces the unresolved-fallback / kernel-cache invariant
       from RFC §FUSE read path
  - Per-event-kind documentation of which TX-level invariants apply
    (mainly: user_added, channel_added, user_renamed, channel_renamed
    all need the UPSERT+lookup+invalidate-in-one-TX pattern from 2E)

## Sprint 4: Cutover

- `SLACK_FUSE_MODE=legacy|split` env-var dispatch in
  `slack_fuse/__main__.py`
- Phase 4a: ship with `legacy` default; users opt in to `split` for
  validation
- Owner monitors for issues for 4 weeks (bake-in)
- Phase 4b: flip default to `split`
- Phase 4c (separate PR, weeks later): delete legacy code

## Owner runbook

This is what I do day-to-day as the owner agent during the rebuild.

### Morning sweep

1. `git -C ~/agentic/slack-fuse fetch --all --prune`
2. `git wt list` — see which worktrees exist, which agents are still
   active in them
3. For each active worker, agent-message a brief status check:
   `~/bin/agent-message --tmux <session> "Status?"`
4. Read responses as they arrive (push-based, so they appear in my
   next-turn context).
5. Triage:
   - **Worker reports done with passing acceptance**: review their
     PR (`git pr` workflow), run a critical-review agent if the
     scope justifies it, merge to `server-split`.
   - **Worker reports stuck on something the RFC didn't cover**:
     escalate to user OR resolve myself if simple (per *Edge case
     handling* below).
   - **Worker reports done with failing acceptance**: agent-message
     them with the specific failure and what to try.

### Merging a worker's PR

```bash
# In the worker's worktree, presumably they've made a PR or pushed
# their branch to origin.
git -C ~/agentic/slack-fuse checkout server-split
git -C ~/agentic/slack-fuse pull --ff-only origin server-split
git -C ~/agentic/slack-fuse merge --ff-only <worker-branch>
# If --ff-only fails, the worker needs to rebase. See Conflict
# resolution below.
git -C ~/agentic/slack-fuse push origin server-split
```

Linear history on `server-split` — no merge commits. If a worker
can't fast-forward, they rebase.

### Conflict resolution: loser-rebases

When two workers in independent worktrees touch the same files (this
should be rare given the File ownership matrix, but happens when an
unanticipated cross-cutting change emerges), whoever lands SECOND
rebases. The owner does NOT manually resolve conflicts.

```bash
# Owner detects conflict trying to merge worker B's branch:
git -C ~/agentic/slack-fuse merge --ff-only <branch-B>
# error: Not possible to fast-forward, aborting.

# Owner messages worker B (the loser):
~/bin/agent-message --tmux <B-session> "$(cat <<'EOF'
Your branch can't fast-forward to server-split because <A-branch>
landed first and modified <file list of overlap>. Please:

  git fetch origin server-split
  git rebase origin/server-split

Resolve conflicts in files you own per the File ownership matrix.
If conflicts appear in files outside your scope, escalate back to
me — that's a sign of an ownership-matrix violation that needs
review. Push and tell me when done.
EOF
)"
```

If the loser's worker session has exited, the owner respawns it with
a continuation-of-prior-task spec referencing the earlier handoff
directory.

### Spawning a worker

Each handoff prompt template includes:

```md
> **Parent:** Claude Code (owner) | **Child:** <agent>
> **CWD:** <worktree path> (use `git -C` or cd-into)
> **Branch:** <branch-name>
> **Worktree:** <branch-name>  # use `git wt <branch>` to materialize

## Files you own
[exact paths from the File ownership matrix]

## Files you may read but NOT modify
[adjacent paths the task touches]

## Acceptance criteria
[concrete, executable]

## RFC reference
docs/rfcs/2026-05-server-split.md sections X, Y, Z

## On completion
Push to origin and `agent-message --session <owner-id> "..."` with:
- "Done; ready for merge" + branch name + acceptance evidence
OR
- "Stuck on <thing>" + specific question

## If you discover an edge case the RFC doesn't cover
agent-message me FIRST with the edge case and a proposed resolution.
Do not make unilateral RFC-divergent decisions.

## If you find your work conflicts with main on rebase
Resolve conflicts in files you own. If conflicts appear in files
outside your scope, agent-message me — that's a scope violation that
needs review.
```

### Review gates

Critical-review agents run at specific points. The review's job is
adversarial: find what's wrong with the implementation, not validate
that it's done.

**Reviewer selection rule**: use a model from a different family than
the one that wrote the majority of the work being reviewed. The point
is variance — same-family reviewers share the same training-induced
blind spots as the author. Gemini 3.1 Pro is a wildcard for any gate
when we want a third vendor entirely.

| Gate | When | Reviewer | What it checks |
|---|---|---|---|
| Post-Sprint-0 | After interface freeze PR | Opposite-family of writer (if Opus wrote → codex-xhigh; if codex wrote → opus) | Interfaces match RFC exactly; no surprise additions; types check |
| Post-Sprint-1 | After server vertical slice merges to server-split | Opposite-family of dominant Sprint-1 writer | Slurper writes events conformant to wire frames; offsets advance correctly; slurper-health emits per spec |
| Pre-3B-merge | Before FUSE adapter merges | Opposite-family of 3B writer, PLUS gemini-3.1-pro as a second independent review | Kernel-cache invariants hold; notify_store and invalidate_inode coordination per RFC; trailer rule honoured. Two reviews because this is the riskiest correctness boundary in the rebuild |
| Post-Sprint-3 | After convergence | Two independent reviews: opposite-family of dominant Sprint-3 writer + gemini-3.1-pro | End-to-end behaviour matches RFC; resolved-questions table entries still hold |
| Pre-cutover | Before `SLACK_FUSE_MODE=split` becomes default | Opposite-family of dominant rebuild writer + gemini-3.1-pro | Bake-in evidence; performance acceptable; rollback path tested |

Gate failures cause the owner to spawn fix-tracks rather than merge.

### Edge case handling

When a worker reports "I discovered the RFC doesn't account for X",
the owner triages:

| Type | Owner action |
|---|---|
| Mechanical clarification (e.g. "RFC says column type TEXT, schema needs NUMERIC for ordering") | Owner makes the call, updates RFC, tells worker to proceed |
| Scope expansion (e.g. "RFC said one Backfiller; I need a Backfiller subprotocol for thread replies vs top-level") | Owner makes the call if obvious; escalates to user if not |
| Cross-cutting design change (e.g. "the per-stream queue assumption breaks for some channel-list events") | Always escalate to user |
| Genuinely ambiguous corner of the spec | Owner picks a sane default, documents it as an "RFC clarification" addendum in `docs/plans/rfc-clarifications.md`, tells worker to proceed |

The owner's `docs/plans/rfc-clarifications.md` accumulates the small
clarifying decisions; at the end of each sprint, the owner folds
them back into the RFC as an explicit revision commit so the RFC
stays the source of truth.

## What I (owner) escalate to the user

- Any RFC-divergent design change beyond mechanical clarification
- Two workers' specs are in contradiction and I can't reconcile them
- A critical-review agent reports a severe issue and I'm not
  confident in the fix direction
- Cumulative wall-clock has drifted significantly from sprint plan
  (e.g. Sprint 2 still not done after 6 weeks) — surface the
  bottleneck
- Anything that touches `main` (the non-rebuild branch)

## What I don't escalate

- Mechanical worker failures (CI red, lint, types) — agent-message
  the worker
- Schema additions that fit the RFC's "if scaling demands it" lines
  (e.g. adding an index on a column the RFC mentioned as a future
  concern) — document, proceed
- Test-infra additions that don't change product semantics
- Naming, formatting, code-organisation choices inside a single
  track's owned files

## RFC clarifications log

`docs/plans/rfc-clarifications.md` is a running list of small
clarifications the owner makes during the build. Each entry:

```md
## 2026-06-12: chunks.message_ts is NUMERIC(20, 6), not NUMERIC(15, 6)

**Context**: Sprint 0 worker found that some Slack timestamps from
huddle-related events have higher precision than the chunks PK can
hold.
**Decision**: bump precision to (20, 6).
**RFC update**: filed as next-revision change.
```

At end of each sprint, owner folds clarifications back into the RFC
in a single revision commit so the RFC stays canonical.

## Worker model selection

### Available models for this project

- **Claude Opus** — `agent-handoff --agent claude --model opus` (alias for `claude-opus-4-7`)
- **Codex** (coding-optimized) — `agent-handoff --agent cursor --model gpt-5.3-codex-xhigh` for the high-effort variant, or `gpt-5.3-codex` mid-tier
- **GPT-5.5 xhigh** (frontier) — `agent-handoff --agent cursor --model gpt-5.5-extra-high`
- **Gemini 3.1 Pro** (wildcard third vendor) — `agent-handoff --agent cursor --model gemini-3.1-pro`

`sonnet-thinking` and the smaller Claude sibling models are off-menu
for this project — the rebuild benefits from frontier-tier reasoning
even on tracks that look mechanical, because the seams between tracks
are subtle.

### Picking a writer

| Task shape | Default |
|---|---|
| Mechanical lift from current codebase | Codex (mid-tier, coding-optimized) |
| Mechanical refactor with tests | Codex |
| Spec-driven implementation, well-bounded | Opus or GPT-5.5 xhigh (pick by vendor diversity across concurrent tracks) |
| Implementation with subtle correctness concerns (cache invariants, concurrency, ordering) | Opus or GPT-5.5 xhigh, never Codex |
| Long contiguous track touching many files (e.g. 2E client projector) | Opus (1m context variant if needed) |

When spawning multiple concurrent tracks in Sprint 2, deliberately
spread vendors: 50/50 Opus and codex/GPT roughly, so each track has
a natural opposite-family reviewer available.

### Picking a reviewer (the variance rule)

Use a model from a different family than the writer of the work
being reviewed.

| Writer family | Default reviewer |
|---|---|
| Claude Opus | Codex or GPT-5.5 xhigh |
| Codex or GPT-5.5 xhigh | Claude Opus |
| Mixed-family tracks (large convergence sprint) | Two independent reviews: one opposite-family-of-dominant-writer, one Gemini 3.1 Pro wildcard |

Gemini 3.1 Pro is the wildcard third vendor — pull it in when:
- The review is high-stakes (FUSE adapter, pre-cutover sweep) and we
  want a third independent perspective beyond the writer/reviewer pair
- The owner suspects the writer + opposite-family-reviewer pair
  share a blind spot (e.g. both have leaned heavily on the same
  pattern across multiple tracks)
- A reviewer's findings need adjudication and we want a tiebreaker

Default to the cheapest model that can plausibly do the task. The
owner re-reviews everything before merge anyway.

## State the owner maintains

- `docs/plans/2026-06-08-server-split-orchestration.md` (this file) — updated as sprints complete
- `docs/plans/rfc-clarifications.md` — running log of mid-build decisions
- `docs/plans/poc-reports/` — POC outputs
- `docs/plans/sprint-status.md` — short living "who's working on what, when it's expected, what's blocked"
- `git wt list` output — implicit state of active worktrees
- `git log server-split..main --oneline` — what main has that we haven't rebased in yet

## Done = ?

v1 is done when:

1. All RFC §Success criteria pass against the running server-split build
2. Bake-in period (4 weeks of `split` default with no rollback) completes
3. Legacy code deletion PR lands on `main`
4. RFC marked Status: Implemented + linked to commits
5. This doc marked Status: Complete + folded into project history

## Out of scope for this doc

- The architectural design itself — see RFC
- v2 work (passthrough, auto tier transitions, last-N-days retention) — separate plan when v1 is bake-in-complete
