# slack-fuse

Read-only FUSE filesystem that exposes a Slack workspace as browsable, grep-able markdown — channels, DMs, group DMs, threads, huddle notes, and huddle transcripts, all addressable as files under one mount point.

```
~/views/slack/channels/general/2026-04/09/standup-update/thread.md
~/views/slack/huddles/2026-04/09/design-review/notes.md
~/views/slack/dms/alice/2026-03/15/channel.md
```

Built for using Slack data with shell tools (`rg`, `bat`, `fd`) and as a stable filesystem surface for AI agents that prefer files over APIs.

## Why

Slack's UI is fine for live use but bad for retrospection: search is mediocre, threads are hard to navigate, and there's no way to grep across everything you can read. slack-fuse mirrors what your user token can see into a file tree, caches it on disk, and gives you the full power of Unix tools over your workspace history.

## Requirements

- Linux with `fusermount3` (libfuse3)
- Python 3.12+
- [`uv`](https://github.com/astral-sh/uv) for dependency management
- A Slack user token (`xoxc-…` or `xoxp-…`) — the mount sees what you see

## Install

```bash
git clone https://github.com/synap5e/slack-fuse.git
cd slack-fuse
uv sync
```

## Configuration

Copy `.env.example` to `.env` and fill in your token:

```bash
cp .env.example .env
$EDITOR .env
```

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `SLACK_USER_TOKEN` | yes | — | User token (`xoxc-`/`xoxp-`). Mount reads what you can read. |
| `SLACK_APP_TOKEN` | no | — | App-level token (`xapp-…`, scope `connections:write`). Enables push liveness via Socket Mode — see "Live updates" below. Absent → mount runs polling-only. |
| `SLACK_BOT_TOKEN` | no | — | Bot token (`xoxb-…`). Slack requires an app to have a bot user for Socket Mode, but every event slack-fuse subscribes to is user-scope, so the bot token's scopes don't matter. Any scope is fine. |
| `SLACK_FUSE_BACKFILL` | no | `false` | Enable the background history backfill task. Accepts `true`/`false`, `1`/`0`, `yes`/`no`, `on`/`off`. |

`SLACK_USER_TOKEN` may also be supplied via `~/.config/slack-fuse/config.json` if you'd rather keep it out of `.env`.

`.env` is gitignored. Don't commit it.

## Run

### One-shot CLI

```bash
uv run slack-fuse mount              # mounts at ~/views/slack
uv run slack-fuse mount /tmp/slack   # custom mountpoint
uv run slack-fuse mount --debug      # verbose logging + FUSE debug
uv run slack-fuse unmount            # fusermount3 -u
```

The mount command auto-runs `fusermount3 -uz` against the mountpoint first, so a stale mount left over from a crash gets cleaned up before re-mounting.

### Systemd user service

A `slack-fuse.service` unit ships in the repo:

```bash
cp slack-fuse.service ~/.config/systemd/user/slack-fuse.service
systemctl --user daemon-reload
systemctl --user enable --now slack-fuse
```

Edit the `WorkingDirectory`, `EnvironmentFile`, and `ExecStart` paths in the service file to match where you cloned the repo. The defaults assume `~/agentic/slack-fuse`. The service restarts on failure with a 10s delay and unmounts cleanly on stop.

```bash
systemctl --user status slack-fuse
systemctl --user restart slack-fuse
journalctl --user -u slack-fuse -n 30 --no-pager
```

## Filesystem layout

```
~/views/slack/
├── channels/<slug>/                # Channels you've joined
│   ├── channel.md                  # Topic, purpose, member count
│   └── <YYYY-MM>/<DD>/
│       ├── channel.md              # Day's messages (snapshot)
│       ├── feed.md                 # Day's messages (append-only timeline)
│       └── <thread-slug>/
│           ├── thread.md           # Thread snapshot
│           ├── feed.md             # Thread feed
│           └── huddles/<slug>/
│               ├── notes.md        # AI huddle notes (canvas)
│               ├── transcript.md   # Speaker-attributed transcript
│               └── index           # symlink → /huddles/<YYYY-MM>/<DD>/<slug>
├── dms/<username>/                 # Direct messages
├── group-dms/<participants>/       # Group DMs
├── other-channels/<name>/          # Public channels you haven't joined
├── huddles/<YYYY-MM>/<DD>/<slug>/  # Top-level index of all huddles
└── .cached-only/                   # Mirror of the whole tree, no API fetches
```

Channel directory names are slugified. Thread slugs come from the first message (with user mentions resolved into names) so a plain `ls` is often enough to find what you want.

## `.cached-only/` — offline mode

`~/views/slack/.cached-only/` mirrors the entire tree but suppresses every Slack API call: listings and reads only return content already on disk. Useful for grepping the cache without triggering fetches, or for working when Slack is rate-limiting you. Empty listings just mean "not cached yet", not "doesn't exist".

```bash
rg keyword ~/views/slack/.cached-only/channels/
```

## Background backfill

Disabled by default. Set `SLACK_FUSE_BACKFILL=true` to enable a background task that slowly walks every member channel and pulls its full history into the disk cache. Per channel, two phases:

1. **Day backfill** — paginate `conversations.history`, write each day's messages to `~/.cache/slack-fuse/messages/<channel_id>/<YYYY-MM-DD>.json`. Long random sleeps (30-180s) between pages and channels. Marker: `~/.cache/slack-fuse/backfill/<channel_id>.done`.
2. **Thread backfill** — walk those cached day files, find every message with `reply_count > 0`, and fetch the replies via `conversations.replies`. Shorter sleeps (2-8s) since the calls are cheap. Skips threads already on disk so an interrupted run resumes cleanly. Marker: `~/.cache/slack-fuse/backfill/<channel_id>.threads.done`.

After both phases run for a channel, `rg`-ing its `feed.md` and `thread.md` files becomes a pure disk walk.

Other behavior:

- Skips channels whose name contains `notification`, `alert`, or `prod-alerts`.
- Rate-limit responses trigger a wait + jitter and the page is retried.
- Re-day-backfill a channel by deleting its `.done` marker; re-thread-backfill by deleting its `.threads.done` marker.

## Live updates (Socket Mode, optional)

If `SLACK_APP_TOKEN` is set, slack-fuse opens a Slack Socket Mode websocket and merges push events into the in-memory caches on the fly — new messages, edits, deletes, threaded replies, and channel-list changes reflect in `cat` output within ~2 seconds of the post, no polling wait. Polling TTLs stay in place as the correctness floor.

One-time app config (in the Slack app admin UI at `https://api.slack.com/apps`):

1. **Socket Mode → Enable Socket Mode**.
2. **Event Subscriptions → Enable Events**, then under **Subscribe to events on behalf of users** add:
   - `message.channels`, `message.groups`, `message.im`, `message.mpim`
   - `channel_created`, `channel_rename`, `channel_archive`, `channel_unarchive`
   - `member_joined_channel`, `member_left_channel`
   - `group_archive`, `group_unarchive`, `group_rename`, `group_deleted`
   - `im_created`
3. Generate an app-level token with `connections:write` and put it in `.env` as `SLACK_APP_TOKEN`.
4. Required user-token scopes (`channels:history` / `groups:history` / `im:history` / `mpim:history` plus the matching `*:read` for structural events) are already standard `xoxp-…` scopes — if your user token can list channels and read history, you're done.

**Coverage.** Public channels you've joined, private channels, DMs, group DMs. `other-channels/` (public channels you're *not* in) stays on TTL polling — Slack doesn't deliver events for channels the user isn't a member of.

**Without the token.** If `SLACK_APP_TOKEN` is unset, the socket task doesn't start and the mount behaves exactly like a polling-only build: today's messages refresh every 5 minutes, channel list every 30.

## Caching

Disk cache lives at `~/.cache/slack-fuse/` and survives restarts. Channel list, huddle index, day messages, threads, known dates per channel, and backfill markers all persist there.

In-memory TTLs (these are the polling floor; push events via Socket Mode beat them when configured):

| Data | TTL |
|---|---|
| Channel list | 30 min |
| Huddle index | 30 min |
| Today's messages (system local date) | 5 min |
| Active threads | 1 min → 10 min → proportional, based on last-reply age |
| Any earlier local date | indefinite |

Force refresh:

```bash
systemctl --user kill -s USR1 slack-fuse   # service mode
kill -USR1 $(pgrep -f 'slack-fuse mount')  # CLI mode
```

## Searching

```bash
rg keyword ~/views/slack/channels/        # all channels
rg keyword ~/views/slack/huddles/         # all huddles (notes + transcripts)
rg keyword ~/views/slack/.cached-only/    # offline grep, no API calls
```

## Limitations

- Read-only. You can't post, react, or edit through the mount.
- Reflects what the user token can see — private channels you're not in won't appear.
- Initial fetches of large channels can be slow if not yet backfilled.
- Linux only (depends on libfuse3).
- Not all Slack message subtypes are rendered specially; exotic blocks may degrade to plain text.

## Development

```bash
uv run ruff check .
uv run ruff format .
uv run basedpyright            # strict type checking
uv run pytest
```

The project uses strict basedpyright, ruff (preview, with `E,F,W,I,UP,B,SIM,RUF` enabled), frozen dataclasses for domain models, and trio for async I/O. See `CLAUDE.md` for a module map.

## License

[AGPL-3.0-or-later](LICENSE). If you run a modified version of this on a server that other people interact with — including over a network — you have to make your modifications available to those users under the same license. See the LICENSE file for the full text.

### Commercial / alternative licensing

If AGPL doesn't work for you, a copy under a different license is negotiable. The price is somewhere between **\$1 and \$1,000,000**, depending on how the negotiation goes. Open an issue or email the author.
