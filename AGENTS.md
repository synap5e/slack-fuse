# slack-fuse — project knowledge base

**Generated:** 2026-08-28 · **Commit:** `42b7a85` · **Branch:** `main`

Read-only FUSE filesystem exposing a Slack workspace as browsable, grep-able markdown. Python 3.12, trio + pyfuse3 + PostgreSQL, split into a token-holding **server** and a projection-only **client mount**. User-facing setup is in `README.md`; this file is for navigating and changing the code.

## Architecture in one paragraph

Server pod (k8s) ingests Slack — transport choices are Events API webhook, Socket Mode, and the medina NATS shim, with webhook/NATS sharing the durable `slack_event_inbox`, plus on-demand backfill — into an append-only PostgreSQL `events` log ordered by `(stream, offset_in_stream)`. A trio WS gateway replays and tails that log, redirecting too-far-behind consumers to gzip JSONL snapshots. Each mount host runs one trio process: `WSClient` subscribes per stream with a capability-negotiated protocol; `apply_event` folds events into local PG (`chunks`, `thread_chunks`, `chunk_mentions`, `channels`, `users`), advances `cursors` and bumps the `projection_targets` ledger **in one transaction**; a coalescer renders pending targets to `~/.cache/slack-fuse/projection/`; `SlackFuseOpsV2` serves ledger-clean targets from disk with per-file kernel-cache invalidation, else JIT-renders from PG, appending a staleness trailer classified from health signals.

## Structure

```
slack_fuse/            client mount — FUSE surface, path grammar, process wiring   → AGENTS.md
  projector/           subscriber, applier, ledger, coalescer (the correctness)    → AGENTS.md
  cli/                 out-of-mount `rerender` and `tier` subcommands
  migrations/          client projection schema, 0001-0004
slack_fuse_server/     ingestion, event log, wire gateway, HTTP surfaces           → AGENTS.md
  slurper/             the Slack-facing runtime + server CLI                       → AGENTS.md
  wire/ http/ backfill/ probes/ snapshot/ slack_events/
  migrations/          server schema, 0001-0015
slack_fuse_render/     pure shared renderer, imported by BOTH sides                → AGENTS.md
tests/                 1,107 tests, real Postgres, faked Slack                     → AGENTS.md
scripts/               operator tooling, some destructive                          → AGENTS.md
docs/                  RFCs, plans, probe queries, HISTORY.md (mostly gitignored)
.wt/                   git worktrees (33, all prunable) — use `git wt <branch>`
```

## WHERE TO LOOK

| Task | Go to |
|---|---|
| A message renders wrong | `slack_fuse_render/render.py`, then `projector/apply.py::_dispatch` |
| A file is stale / never updates | `projector/` — the ledger, not the FUSE layer |
| A path is wrong or missing | `slack_fuse/fuse_ops_v2.py` dispatch hubs + `fuse_v2_helpers.py` slugs |
| Events aren't arriving at all | `slack_fuse_server/slurper/` + `wire/`, then `_control/status` client-side |
| Data is missing for a date range | `slack_fuse_server/backfill/` + `gaps.py`; `_control/refill_gap` |
| Something is slow or wedged | health taxonomy below, then `slurper-span` logs |
| Why is it built this way | `docs/HISTORY.md`, `docs/rfcs/2026-05-server-split.md` |
| What's outstanding | `BACKLOG.md` (Ratified = Simon's; Agent-raised = untriaged) |

## Health signals (orthogonal — never collapse into one boolean)

| Signal | Proves |
|---|---|
| `/health` | the server's HTTP task can answer. Nothing about Slack or the DB. |
| `/livez` | the slurper task supervisor's phases are advancing. Scheduler progress, not data flow. |
| `slurper-health` events | Slack-side ingestion + backfill observations |
| `connection_state.last_slurper_health` | the client's projection of the above, for trailer classification |
| `slurper-span` stdout logs | per-operation timing evidence — the "why was it slow" surface |
| client trailer `staleness_reason` | WS state + `last_frame_at` + per-stream `stream_caught_up` |
| `cursors.applied_offset` | per-stream apply progress |
| `PgHealth` / `NO_POSTGRES` | local projector PG reachability only |
| `reconnect_recorded` | per-connection wedge/recovery, named per conn. Seven names exist (`inode`, `projector_state`, `projector_sink`, `disk_projection`, `rerender_apply`, `rerender_sink`, `block_sync`) but only four are open at steady state — the rerender pair is opened per rerender and `block_sync`'s per cycle. A routine PG bounce fans out to 4, not 7. |

When someone asks "is it healthy?", name the observable. "systemd active" has meant "silently projecting nothing for 3.7 days" here.

## Events vs operator policy

`events` holds facts that happened upstream in Slack. Operator intent is mutable policy and does not belong in a replayable stream — channel blocks live in the server's `blocked_channels` table and reach clients by periodic block sync. Query-derived facts (`search.messages` totals) are a third category in `channel_message_totals`. Detail and rationale: `slack_fuse_server/AGENTS.md`, `docs/HISTORY.md`.

## Wire protocol

Frames: `Hello`, `HelloAck`, `Subscribe{stream, since}`, `Unsubscribe`, `ServerCapabilities`, `Event{stream, offset, kind, ts, payload}`, `CaughtUp{stream, head_offset}`, `SnapshotAt{stream, at, url}`, `Error{code, stream, head_offset}`, `Ping`/`Pong`.

Server advertises optional frames (currently `unsubscribe`); the client sends `UnsubscribeFrame` only when advertised, else falls back to controlled reconnect on desired-set shrink. Replay ≤5,000 events inline, else `SnapshotAt` redirect — channel streams only. Client reconnect backoff 2s → 5min.

## Control surface (`_control/`)

Plan 9 ctl/status at the mount root. Writes buffer per file handle (64 KiB → EFBIG) and fire on `release`, so `echo x > file` is one action. Budget 15s for `_control/*`, 0.5s elsewhere. `release` never fails; verbs come from `control.py::result_for_status` (202→`queued`, 409→`busy`, 401/403→`unauthorised`, 503/0→`server_unavailable`).

Writable: `refresh_channels`, `refresh_channel`, `blocked_channels`, `backfill_channel`, `refill_gap`, `probe_sweep{,_job,_target}`, `rerender_channel`. Read-only: `gaps`, `probes`, `status`.

## Conventions

Python 3.12, `from __future__ import annotations` everywhere, **trio never asyncio**, uv for deps (`uv add`/`uv remove`, never `uv pip install` against this env).

- **basedpyright strict.** Disabled exactly four checks: `reportUnusedCallResult`, `reportImplicitStringConcatenation`, `reportUnannotatedClassAttribute`, `reportUnknownLambdaType`. Target is `0 errors, 0 warnings, 0 notes`.
- **ruff**, `line-length=120`, `preview=true`, select `E,F,W,I,UP,B,SIM,RUF,BLE001,C901,E402,PLC0415,PLR0913,PLR0916,PLR0917,PLR1702`, ignore `SIM108`. Per-file: `__init__.py`→F401, `tests/*`→BLE001, `slack_fuse/__main__.py` + `cli/rerender.py`→PLC0415.
- **Pydantic at every I/O boundary**, frozen models, no in-place mutation. Wire quirks go in `AliasPath` / `BeforeValidator` / `populate_by_name` declaratively. `JsonObject` from `models.py` for opaque pass-through — never `dict[str, Any]` or `dict[str, object]`.
- **Renderers are pure**: models + resolver in, bytes out, no I/O.
- **No external I/O inside `conn.transaction()`** — no HTTP, network or file reads while holding DB locks.
- Server Slack calls go through `_api_call(callable)`; the `TypeVar` preserves the return type. No string dispatch.

## Commands

```bash
uv sync
uv run ruff check . && uv run ruff format .
uv run basedpyright
uv run pytest                       # auto-provisions a temp PG cluster if DATABASE_URL is unset
uv run slack-fuse mount --debug
systemctl --user restart slack-fuse # after `uv sync`; the unit runs the editable install
```

**CI runs none of these.** `.github/workflows/docker.yml` is the only workflow and it builds/pushes the server image. Local gates are the only gates — run them before you claim done.

## ANTI-PATTERNS

Cross-cutting ones. Module-specific hazards live in the child AGENTS.md files, and every `# noqa` / `# pyright: ignore` in this repo is load-bearing until proven otherwise (pyfuse3 3.4.2 ships incomplete stubs).

- **Never suppress a type error** with `as any` / `# type: ignore` to make basedpyright pass.
- **Never call `pyfuse3.invalidate_inode` or `notify_store` from the request-serving loop.** Worker threads only. This deadlocked the host twice (`folio_wait_bit_common`).
- **Never re-add the `ro` mount option.** The kernel would reject `_control/` writes before the daemon sees them.
- **Never do network or slow I/O in a FUSE metadata callback.** `getattr`/`lookup` fire per path component; one `cat` of `_control/gaps` once issued five 2-second SQL queries and starved `/health` into 503s.
- **Never hide an expensive view behind `readdir`** — ghost files are lookup-only so a recursive walk can't detonate them.
- **Never dual-write derived state at ingest.** Server `channels` and `health_log` are VIEWs over `events`. A projection in the same DB is fine; the same *table* is not.
- **Append-only means append-only.** Repair a lossy row with a corrective `message_changed`, never UPDATE or DELETE.
- **Never aggregate over full event history in a served request.** `count(*) FROM active_messages` took 261s and CrashLoopBackOffed production for 28 hours.
- **Never ship allocator or timeout tuning as a root-cause fix**, and say so when it's a mitigation.
- **Never delete a failing test to go green**, and never commit unless explicitly asked.
- **Never restart a dependency service you don't own** — escalate.

## Runtime gotchas

- The systemd unit has **no `ExecStartPre`**; `cmd_mount` runs `fusermount3 -uz` itself. Manual escape hatch: `fusermount3 -u ~/views/slack`. `fusermount3` must be on PATH.
- The unit's header comment advertises `SIGUSR1` force-refresh. **There is no SIGUSR1 handler in v2** — that went with the v1 island deletion (`3c7ef8b`).
- `.env.example` is **stale**: it lists v1's `SLACK_USER_TOKEN` / `SLACK_FUSE_BACKFILL`. Real client config is `SLACK_FUSE_*` env or `~/.config/slack-fuse/config.toml`; the server uses `SLACK_FUSE_SERVER_*`. The mount holds no Slack token, only a shared secret.
- No `.cached-only/` prefix in v2 — it was a v1 mechanism to suppress read-time Slack calls, and v2 has no direct-API path from the mount.
- For wide greps, read `~/.cache/slack-fuse/projection/` directly (~62,000 files/s) rather than the mount (~15-25 files/s). A `.ignore` ghost at the mount root keeps `rg`/`fd` out of `_control/`.
- `git wt <branch>` for worktrees; don't `git switch` (the agent guard blocks it).

## Notes

- **`docs/HISTORY.md`** is the mined account of how this got built: design reversals, the incident log, invariants-learned-the-hard-way, and ~25 ideas that were raised and never closed. Read it before proposing an architecture change — most obvious ideas have already been tried here.
- The next architecture step is an in-flight co-design of a generic projection→VFS engine in Rust (six seams: `Source`, `Render`, `Layout`, `Trailer`, `Control`, `Ghost`). Requirements only; no code, and the paper mount proof is not done.
- The largest live problem is server memory: 1,658 OOMKills in 10 days at 1Gi, mitigated to 3Gi, root cause unfound.
- Host-specific operator notes (systemd, paths, recovery commands for *this* machine) are in `~/docs/slack-fuse.md`, outside the repo.
