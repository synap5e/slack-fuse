# Sprint status (live)

Updated as the owner loop ticks. Most-recent first.

## 2026-06-08 — Sprint 0 + POCs kickoff

**State**: Sprint 0 + POC A + POC B all in flight.

| Worker | Model | tmux | Branch | Handoff dir |
|---|---|---|---|---|
| Sprint 0 interface freeze | claude (opus) | `sprint0-interface-freeze` | `synap5e/feat/sprint0-interface-freeze` | `~/.agent-handoff/2026-06-08/sprint0-interface-freeze/` |
| POC A events→postgres | cursor (gpt-5.3-codex-xhigh) | `poc-a-events` | `synap5e/poc/a-events-to-postgres` | `~/.agent-handoff/2026-06-08/poc-a-events-to-postgres/` |
| POC B renderer-split | claude (opus) | `poc-b-renderer` | `synap5e/poc/b-renderer-split` | `~/.agent-handoff/2026-06-08/poc-b-renderer-split/` |

Integration target: worktree at `.wt/server-split-rebuild/`, branch
`server-split-rebuild` (created from `main` at `fee797f`).

**Next owner action**: 15-minute wake-up to check progress, bump
stalls, process any agent-messages received.

**Sprint-boundary check-in points** (where user is in the loop):
- After Sprint 0 lands + POC reports in
- After Sprint 1 server soak (~1 week observation)
- After Sprint 2 fan-out (6-ish tracks merged)
- After Sprint 3 convergence (pre-cutover gate)
