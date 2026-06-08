# Sprint status (live)

Lives on `server-split-rebuild`. Updated as the owner loop ticks.
Most-recent first.

## 2026-06-08 — Sprint 0 + POCs kickoff

### Workers

| Worker | Model | tmux | Branch | Status |
|---|---|---|---|---|
| Sprint 0 interface freeze | claude (opus) | `sprint0-interface-freeze` | `synap5e/feat/sprint0-interface-freeze` | in flight |
| POC A events→postgres | cursor (gpt-5.3-codex-xhigh) | `poc-a-events` | `synap5e/poc/a-events-to-postgres` | in flight |
| POC B renderer-split | claude (opus) | `poc-b-renderer` | `synap5e/poc/b-renderer-split` | **MERGED** at `7e5752a`. 45/45 tests, 0 structural bugs found. Design verified SAFE for Sprint 2B. RFC updated with absent-user-fallback tradeoff note. Real discovery: current `slack_fuse/mrkdwn.py` docstring/CLAUDE.md claim code+blockquote handling but the code doesn't — no code-span protection in v1. Doc'd in POC report. |

### Integration target

Worktree `.wt/server-split-rebuild/`, branch `server-split-rebuild`.
Created from `main` at `fee797f`. Currently has POC B merged.

### Sprint-boundary check-in points (user-in-loop)

- After Sprint 0 lands + POC reports in **← next user touchpoint**
- After Sprint 1 server soak (~1 week observation)
- After Sprint 2 fan-out (6-ish tracks merged)
- After Sprint 3 convergence (pre-cutover gate)
