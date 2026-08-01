# Backlog

Structure (Simon's convention, 2026-08-02):

- **Ratified** — items Simon has asked for or blessed. Highest confidence.
- **Agent-raised (needs human review)** — items an agent (usually Claude here) surfaced and validated, but Simon hasn't triaged yet.
- **Agent-raised (unconfirmed origin)** — items another agent proposed that may not trace back to a human ask, or that couldn't be confirmed. Weakest signal; treat as suggestions.
- **Resolved** — done items with the commits that closed them, most recent first. Not deleted, so the history reads back.

When an item ships, move it into **Resolved** with the commit hashes and date. When a new item surfaces, add it to the appropriate section — never auto-append to Ratified without explicit user blessing.

---

# Ratified

## FUSE mount wedge — host-level condition

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

**Prevention not yet implemented** — add `slack-fuse.service` to
`game-mode`'s `GAME_MODE_STOP_SERVICES` so it gets cleanly stopped
before `claude-hooks-postgres.service` is torn down, then restarted in
`cmd_off`. Operator-side (`/home/simon/bin/game-mode`), not a slack-fuse
code change.

---

## Skip thread-expansion when local thread is already caught up

**Discovered**: 2026-06-30 watching proj-cloud's backfill spend ~9 hours
in the thread-expansion phase writing rows that all dedup'd to no-ops.
proj-cloud's history pagination finished by 17:57; the next 8+ hours
were `conversations.replies` calls per thread parent, paying the 2-8s
throttle per call, hitting the dedup index, inserting zero new rows.
Socket-mode events had already filled the threads in.

**ADR (2026-08-02, `/tmp/claude/adr-skip-thread-expansion.md`)**:
recommendation **O1** — server-side batched preflight per channel.
Before each `conversations.replies` call, `COUNT(DISTINCT reply_ts)`
plus `MAX(reply_ts)` from active (edit/delete-folded) message facts;
skip only when both match the parent's `reply_count` and `latest_reply`.
Nulls or mismatch fetch. Reclaims the 4,500-thread / 8+ hour no-op tail
per channel. Estimated 2–3 engineer-days.

**Open questions from the ADR**:
1. Should the first release require a minimum parent age (e.g. 60s) to
   reduce fresh-message race, or rely on mismatch + live delivery?
2. Is the missed-delete-plus-missed-add set-substitution risk
   acceptable, or should we also require a prior successful full-expansion
   marker?
3. Should skips become lifecycle events for crash-resume accounting, or
   remain spans only and be cheaply re-evaluated after a crash?

**Implementation**: not started. Well-scoped handoff candidate once the
open questions land.

---

## FUSE passthrough + coalesced disk projection

**Discovered**: 2026-06-29 while benchmarking ripgrep throughput. Live
mount serves ~18 files/sec (every file goes through FUSE round-trip);
the archive on disk serves ~135,000 files/sec. ~7,500× gap.

**ADR (2026-08-02, `/tmp/claude/adr-fuse-passthrough.md`)**:
recommendation **O2** — **reject** privileged FUSE passthrough (requires
`CAP_SYS_ADMIN`, no pyfuse3 binding, wouldn't fix the current
attribute-path bottleneck). Instead **build a direct coalesced on-disk
projection**: extend the archive concept into a searchable mirror,
eager/coalesced for hot channels and background-filled for cold ones.
FUSE stays as the exact-fresh interactive view. Estimated 5–8
engineer-days.

**Open questions from the ADR**:
- Must the fast path retain the exact `/views/slack` pathname?
- What maximum lag is acceptable for today's files (suggest 1s)?
- Should hidden channels be materialized, or only hot channels?
- Where should the 1–1.5 GB projection live on each host?
- Would Simon ever accept `CAP_SYS_ADMIN` for the client daemon?

**Implementation**: not started. Depends on the open questions.

---

## Workspace channel inventory view (`_workspace/channels.md`)

**Discovered**: 2026-06-27 during the dump-and-reingest while wanting
a real-time progress denominator. Slack's `search.messages` API exposes
a per-channel total message count (with `query=in:#<name>`, `count=1`,
read `messages.total`), giving authoritative size data we don't have
elsewhere.

**Symptom / motivation**: backfill progress, channel sizing for
manual-backfill decisions, block-list candidates, workspace overview
— all rely on knowing "how many messages does this channel have?"
Currently the only path is ad-hoc SQL + a one-off `search.messages`
sweep, which:

- requires kubectl exec into the slurper pod
- has no UI surface
- has no caching — every check pays Tier 2 rate budget
- doesn't expose non-joined channels' sizes (which we'd want before
  deciding whether to manually backfill them)

**Proposed shape**: `_workspace/channels.md` ghost file rendering a
per-channel inventory table:

| Name | Messages | Ingested | Status | Member | Created |

Status column maps `done` / `in_progress` / `blocked` / `not_started` /
`not_joined` / `unavailable`. Sorted by total messages desc.

**Server side**:
- New `channel_message_totals` table (channel_id PK, total BIGINT,
  refreshed_at TIMESTAMPTZ, refresh_status TEXT)
- Periodic refresh task (6h cadence) — Tier 2 throttle, 3.5s between
  calls, ~24 min per cycle for ~418 visible channels
- HTTP `GET /channel-stats` joining the totals + blocked_channels +
  latest channel-list payload + live events count
- CLI `slack-fuse-server refresh-channel-totals` for one-shot

**Client side**:
- `_workspace/channels.md` ghost file
- Background-warmed cache (same shape as `_workspace/gaps.md` warmer)
  so FUSE callbacks never block on server fetch
- Markdown renderer

**Architectural note**: the search-derived count is a fact about Slack
but it's *query-derived* (we asked, Slack told us), not pushed via
the events stream. It belongs in a refreshed table, not an event kind.
Same shape as `backfill_overrides` and `blocked_channels` — distinct
from both the events log (immutable upstream facts) and operator-policy
tables (mutable operator intent).

**Pitfalls** (for the eventual implementor):
- Search API requires user token, not bot token
- `is_im` channels can't be queried via `in:#<name>` — handle/skip
- Slack's total has approximation caveats above ~10K (mark
  `refresh_status='approximate'`)
- Don't truncate the totals table on refresh — preserve last-known
  on error so the view stays useful

**Estimated scope**: ~200-250 LoC + tests. Self-contained handoff;
prompt already drafted at
`/home/simon/.agent-handoff/2026-06-27/workspace-channels-view/prompt.md`
(queued for after the current backfill cycle settles).

**Impact**: changes the operational story from "ad-hoc SQL via kubectl"
to "cat the file". Reusable for every future "how big is X / how
complete are we" question.

---

## Probe-event pattern — channel message counts + wider pattern

**Discovered**: 2026-06-28, post Wave 2 deploy. Triggered by the
question "what's the % progress of the backfill?" — we have no
authoritative denominator until a channel is fully backfilled.

**Specific item**: add a `channel_message_count_probed` event kind.
Slurper periodically calls `search.messages?query=in:<channel>` and
emits one event per channel per period with the total count from the
API. Tier 2 (`search.messages`: 60/min). Lets `/livez` (or a new
endpoint) compute "% complete" as `sum(events_written from
backfill_completed) / sum(latest probed count)`. Cheap to implement
once the pattern shape is decided.

**Wider pattern to think through** before building the specific item.
Today our event kinds split cleanly into two shapes:

- **Push-driven** (Socket Mode): `message`, `channel_added`,
  `user_added`, `member_joined_channel`, `reaction_added`, etc.
- **Diff-driven refreshes** (`channel_info_refreshed`): fire ONLY when
  a periodic `conversations.info` sweep detects payload drift.

A **probe event** is a third shape: slurper-initiated, periodic,
captures authoritative Slack API state regardless of drift, immutable
in the events log. The latest probe wins; older ones are history.

Candidate probes — picked specifically because Slack EITHER lacks a
push event for them OR we don't subscribe today. (Thread replies are
covered by existing `message.*` subscriptions regardless of the
parent's age, so they don't fit this pattern.)

1. **`channel_message_count_probed`** — the asked-for one. Backfill %
   visibility. Tier 2.
2. **`channel_pin_count_probed`** — `pin_added`/`pin_removed` socket
   events exist but we don't subscribe. `pins.list` is cheap.
3. **`workspace_emoji_probed`** — `emoji.list` for custom emoji.
   `emoji_changed` socket event exists but we don't subscribe.
   Useful for rendering markdown output.
4. **`channel_bookmark_probed`** — no socket event exists. Some teams
   use bookmarks as canvas pointers.

Design points to settle BEFORE writing any of them:

- **One probe-sweep task or per-probe tasks?** One sweep is simpler
  (one supervisor entry, one limiter; the sweep walks a registry of
  probe kinds with their own intervals). Per-probe scales the nursery
  + supervisor surface unnecessarily.
- **TTL + cadence per kind.** `channel_message_count` could refresh
  every 6h; `workspace_emoji` daily; `pins` weekly. Make this part of
  ServerConfig.
- **Tier budget accounting.** `search.messages` (Tier 2, 60/min) for
  N channels at interval T must respect the ceiling. Bake into the
  sweep.
- **Failure handling.** API failure = no event written. Last probe
  stays as truth. Consumers shouldn't assume any cadence.
- **Spans wrap probes.** Each probe emits `slurper.probe.<kind>`
  spans for cost visibility — natural follow-on from Wave 2.C.
- **Distinct from refreshes.** `channel_info_refreshed` fires on
  diff; probes fire on period regardless. Two different consumers;
  don't piggyback.

**Recommendation**: spec the probe-event shape (one sweep task,
registry of probe kinds, per-kind TTL via config) as one design pass,
then implement the first probe (`channel_message_count_probed`) as
the proof. Other probes drop in cheaply afterward.

---

## Clean up repo — sprint/feat worktrees

**Discovered**: 2026-06-28. Partially done 2026-08-02.

Still to sweep:
- ~30 `synap5e/feat/sprint*` and `synap5e/feat/*` worktrees under
  `.wt/synap5e/feat/` (sprint0…sprint3, 2a-2f, 3a-3e,
  post-sprint3-fixes, slurper-channels-populate). Most shipped;
  needs one-by-one check.
- `.wt/handoff/*` — a dozen completed handoff worktrees
  (`self-join-detection`, `finding-16-refresh-discovery`,
  `v2-adversarial-review`, wave1/wave2 review, spec-review branches,
  etc.).
- `.wt/server-split-rebuild` — likely shipped as the split server.

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
