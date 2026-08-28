# slack-fuse

The project knowledge base lives in **[`AGENTS.md`](AGENTS.md)** — same content, under a name every agent harness reads.

It is hierarchical. The root file carries architecture, health taxonomy, wire protocol, conventions and cross-cutting anti-patterns; module detail lives beside the code it describes:

- [`slack_fuse/AGENTS.md`](slack_fuse/AGENTS.md) — client mount, FUSE surface, path grammar, ghost files
- [`slack_fuse/projector/AGENTS.md`](slack_fuse/projector/AGENTS.md) — subscriber, applier, ledger, coalescer
- [`slack_fuse_server/AGENTS.md`](slack_fuse_server/AGENTS.md) — ingestion, event log, wire gateway, HTTP
- [`slack_fuse_server/slurper/AGENTS.md`](slack_fuse_server/slurper/AGENTS.md) — the Slack-facing runtime
- [`slack_fuse_render/AGENTS.md`](slack_fuse_render/AGENTS.md) — the shared pure renderer
- [`tests/AGENTS.md`](tests/AGENTS.md) — fixtures, Postgres strategy, known failures
- [`scripts/AGENTS.md`](scripts/AGENTS.md) — operator tooling, including the destructive harness

Also: [`README.md`](README.md) for setup and usage, [`BACKLOG.md`](BACKLOG.md) for what's outstanding, [`docs/HISTORY.md`](docs/HISTORY.md) for how it got this way.
