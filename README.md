# slack-fuse

Read-only FUSE filesystem exposing a Slack workspace as browsable, grep-able markdown — channels, DMs, group DMs, threads — all addressable as files under one mount point.

```
~/views/slack/channels/general/2026-04/09/standup-update/thread.md
~/views/slack/dms/alice/2026-03/15/channel.md
```

Built for using Slack data with shell tools (`rg`, `bat`, `fd`) and as a stable filesystem surface for AI agents that prefer files over APIs.

## Architecture

slack-fuse runs as two processes with a wire protocol between them:

- **Server** (`slack-fuse-server`, typically in a container) holds the Slack tokens, ingests events from Slack (Socket Mode + Events API webhooks), runs backfill, and persists the event log to PostgreSQL. Exposes a WebSocket for clients + HTTP snapshot redirects.
- **Client mount** (`slack-fuse mount`) subscribes over the WebSocket, applies events into a local PostgreSQL, and materialises rendered markdown to a disk projection that FUSE serves. Client holds only a shared secret to talk to its server; no Slack tokens on the mount host.

Splitting means multiple mount hosts can share one authoritative server and one Slack token, and the mount survives PG bounces, Slack outages, and server redeploys without kernel-space wedges.

## Why

Slack's UI is fine for live use but bad for retrospection: search is mediocre, threads are hard to navigate, and there's no way to grep across everything you can read. slack-fuse mirrors what your user token can see into a file tree, materialises it to a local disk projection, and gives you the full power of Unix tools over your workspace history.

## Requirements

- Linux with `fusermount3` (libfuse3)
- Python 3.12+
- [`uv`](https://github.com/astral-sh/uv) for dependency management
- PostgreSQL on the mount host (local projection store; ~150-500MB for a typical workspace)
- A running `slack-fuse-server` reachable over the network (see below)

## Install (client mount)

```bash
git clone https://github.com/synap5e/slack-fuse.git
cd slack-fuse
uv sync
```

## Configuration

Client config lives in `~/.config/slack-fuse/config.toml` and/or `SLACK_FUSE_*` env vars (env wins; TOML falls through).

```toml
# ~/.config/slack-fuse/config.toml
server_url = "wss://slack-fuse.example.com/ws"
shared_secret = "..."
database_url = "postgresql:///slack_fuse?host=/run/user/1000/local-postgres&port=5433"
mountpoint = "/views/slack"
disk_projection_enabled = true
```

| Setting | Env var | Required | Default | Purpose |
|---|---|---|---|---|
| `server_url` | `SLACK_FUSE_SERVER_URL` | yes | — | WebSocket URL of the server (`ws://` or `wss://`). |
| `shared_secret` | `SLACK_FUSE_SHARED_SECRET` | yes | — | Must match the server's. |
| `database_url` | `SLACK_FUSE_DATABASE_URL` | yes | `postgresql:///slack_fuse` | Local PG for projections. |
| `mountpoint` | `SLACK_FUSE_MOUNTPOINT` | no | `~/views/slack` | Absolute path. |
| `disk_projection_enabled` | `SLACK_FUSE_DISK_PROJECTION_ENABLED` | no | `false` | Serve clean paths from disk (fast) vs JIT (slower). Enable in production. |

`SLACK_WORKSPACE_URL` may additionally be set (env or `.env`) if you want `slack-fuse permalink` to synthesize archive URLs without a server round-trip.

## Run

### One-shot CLI

```bash
uv run slack-fuse mount              # mounts at the configured mountpoint
uv run slack-fuse mount /tmp/slack   # override mountpoint
uv run slack-fuse mount --debug      # verbose + FUSE debug
uv run slack-fuse unmount            # fusermount3 -u
```

The mount command auto-runs `fusermount3 -uz` first, so a stale mount from a crash gets cleaned up before re-mounting.

### Systemd user service

A `slack-fuse.service` unit ships in the repo:

```bash
cp slack-fuse.service ~/.config/systemd/user/slack-fuse.service
systemctl --user daemon-reload
systemctl --user enable --now slack-fuse
```

Edit `ExecStart` and the `EnvironmentFile` path to match your checkout. Restarts on failure with a 10s delay; unmounts cleanly on stop.

```bash
systemctl --user status slack-fuse
systemctl --user restart slack-fuse
journalctl --user -u slack-fuse -n 30 --no-pager
```

## Filesystem layout

```
~/views/slack/
├── channels/<slug>/                # Channels you're in
│   ├── channel.md                  # Topic, purpose, member count
│   └── <YYYY-MM>/<DD>/
│       ├── channel.md              # Day's messages
│       └── <thread-slug>/
│           └── thread.md           # Thread snapshot
├── dms/<username>/                 # Direct messages
├── group-dms/<participants>/       # Group DMs
├── other-channels/<slug>/          # Public channels you haven't joined
├── _workspace/channels.md          # Inventory: sizes, ingest status, membership
├── _control/                       # Plan-9-style operator surface (writable)
│   ├── refresh_channels
│   ├── refresh_channel
│   ├── blocked_channels
│   ├── backfill_channel
│   ├── refill_gap
│   ├── probe_sweep{,_job,_target}
│   ├── rerender_channel
│   ├── gaps                        # (read-only)
│   ├── probes                      # (read-only)
│   └── status                      # (read-only) — latest outcomes per action
└── NO_POSTGRES                     # (appears only when local PG is unreachable)
```

Channel directory names are slugified. Thread slugs come from the first message (with user mentions resolved into names) so a plain `ls` is often enough to find what you want.

### Related CLIs

- `uv run slack-fuse resolve <slack-url>` → FUSE path for a Slack message permalink (fully local, no Slack API).
- `uv run slack-fuse permalink <fuse-path>` → Slack permalink URL (requires `SLACK_WORKSPACE_URL`). Pass `--ts <message_ts>` to permalink a specific message in a day file.
- `uv run slack-fuse-server refresh-channel-totals` (server-side) — one-shot totals sweep.

## Freshness model

Every rendered file has a durable identity in a per-mount PostgreSQL **projection ledger** (`projection_targets`). Each row carries `target_generation`, `rendered_generation`, and `renderer_version`. Reads admit disk only when both the specific target row and the singleton layout row are clean at the current renderer version, else JIT-render from PG.

Live events land as facts on the wire, apply into local PG and bump ledger targets in the same transaction; the coalescer atomically replaces the backing file and CAS-marks the target clean. Kernel-cache invalidation happens **before** the CAS so a mid-read that starts on old bytes can never observe them as clean afterward.

Staleness is surfaced as an appended trailer inside file bytes (never as an out-of-band silent stale), classified from `slurper-health` (server-side), the WS connection state (client-side), and per-stream catch-up progress. Historical days are effectively immutable; today's channel/thread files rewrite on every affecting event.

## Control surface

Writes to `_control/*` fire an action on `release` (fh close). Bounded budget per action (default 15s). Verbs are stable — read them back via `_control/status`.

```bash
# refresh workspace totals now
echo now > /views/slack/_control/refresh_channels

# block a channel
echo '#sensitive-channel reason: leaking' > /views/slack/_control/blocked_channels

# re-render one channel's rendered files with the current renderer code
echo general > /views/slack/_control/rerender_channel

# read the latest outcomes
cat /views/slack/_control/status
```

## Searching

```bash
rg keyword /views/slack/channels/                    # everything, via FUSE
rg keyword ~/.cache/slack-fuse/projection/channels/  # direct disk (much faster)
```

`rg` via the FUSE mount pays a per-file syscall cost — ~15-25 files/sec typical. `rg` directly against the disk projection cache at `~/.cache/slack-fuse/projection/` is ~62,000 files/sec (the same rendered bytes, just skipping the FUSE layer). Prefer the cache path for wide grep sweeps.

## Limitations

- Read-only via the mount (except `_control/`). You can't post, react, or edit through the filesystem.
- Reflects what the server's Slack token can see — private channels the token isn't in won't appear.
- Linux only (depends on libfuse3).
- Not all Slack message subtypes are rendered specially; exotic blocks may degrade to plain text.
- New client on old server (or vice versa) mostly works via wire capability negotiation, but you'll want both current for the full feature set (unsubscribe frame, subscription-state tracking).

## Development

```bash
uv run ruff check .
uv run ruff format .
uv run basedpyright            # strict type checking
uv run pytest                  # ~1100 tests; PG-backed, auto-provisioned temp cluster
```

Strict basedpyright, ruff preview, frozen Pydantic at I/O boundaries, trio async everywhere (never asyncio). See `CLAUDE.md` for a module map, health taxonomy, and things-not-to-do.

## License

[AGPL-3.0-or-later](LICENSE). If you run a modified version of this on a server that other people interact with — including over a network — you have to make your modifications available to those users under the same license. See the LICENSE file for the full text.

### Commercial / alternative licensing

If AGPL doesn't work for you, a copy under a different license is negotiable. The price is somewhere between **\$1 and \$1,000,000**, depending on how the negotiation goes. Open an issue or email the author.
