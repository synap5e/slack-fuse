"""The slurper: the long-running server process that owns the Slack token.

Runs Socket Mode, makes every Slack API call, and persists events into the
append-only `events` table via the offset-assignment pattern (RFC §Schemas →
Offset assignment pattern). See the sibling modules:

- `api` — the lifted `SlackClient` (typed Slack Web API surface).
- `offsets` — the canonical offset-assigning write transaction.
- `health` — the `slurper-health` stream emitter (events + `health_log`).
- `socket` — the Socket Mode connection loop, writing events to postgres.
- `__main__` — the `slack-fuse-server` entry point + `backfill` admin command.
"""

from __future__ import annotations
