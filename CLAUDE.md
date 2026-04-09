# slack-fuse — Claude notes

Read-only FUSE filesystem exposing a Slack workspace as markdown. Python 3.12, trio + pyfuse3, httpx for the Slack REST API, frozen dataclasses for the domain model.

User-facing docs are in `README.md`. This file is for navigating the codebase.

## Module map

| File | Responsibility |
|---|---|
| `slack_fuse/__main__.py` | CLI entrypoint. `mount` and `unmount` subcommands, env-var parsing (`SLACK_FUSE_BACKFILL`), trio nursery wiring (`pyfuse3.main`, periodic channel-list refresh, optional backfill). Auto-cleans stale mounts on startup with `fusermount3 -uz`. |
| `slack_fuse/auth.py` | `load_tokens()` — reads `SLACK_USER_TOKEN`/`SLACK_APP_TOKEN` from env, falls back to `~/.config/slack-fuse/config.json`. |
| `slack_fuse/api.py` | `SlackClient`: synchronous httpx wrapper around Slack REST. Methods for listing conversations, fetching channel history (paginated), thread replies, file metadata, file downloads. Errors: `RateLimitedError` (429, with `retry_after`) and `FatalAPIError` (401/403). 0.1s delay between paginated requests. |
| `slack_fuse/models.py` | Frozen dataclasses for the domain: `Channel`, `Message`, `Thread`, `Reaction`, `FileAttachment`, `HuddleInfo`. Helpers: `message_to_dict`/`message_from_dict` and `channel_to_dict`/`channel_from_dict` for disk-cache round-tripping (must handle nested tuples). |
| `slack_fuse/store.py` | The brain. `SlackStore` owns: in-memory caches (channel list, huddle index, day messages, thread slugs, threads), TTL bookkeeping, exponential backoff (`_BackoffState`), and the `cached_only_mode()` context manager. Fuse ops call into `list_channels`, `get_channel_by_slug`, `get_known_dates`, `get_thread_slugs`, `get_day_messages`, `get_huddles_for_thread`, `get_huddle_index`, `get_huddle_by_canvas_id`, `find_huddle_index_entry_by_canvas`, `merge_known_dates`, `force_refresh`, plus `render_*` methods that delegate to `renderer.py`. TTLs: `_CHANNEL_LIST_TTL=1800`, `_RECENT_MSG_TTL=300`, `_OLD_MSG_TTL=inf`, `_OLD_THRESHOLD_DAYS=7`, `_HUDDLE_INDEX_TTL=1800`. |
| `slack_fuse/disk_cache.py` | Pure functions over `~/.cache/slack-fuse/`. JSON files keyed by channel/date/thread_ts/canvas_file_id. Old day messages (>7 days) are effectively immutable so they're cached indefinitely. |
| `slack_fuse/backfill.py` | `backfill_all`: trio task that paginates every member channel's full history into the disk cache. Long random sleeps (30–180s), per-channel `.done` markers in `~/.cache/slack-fuse/backfill/`, skips channels matching `notification`/`alert`/`prod-alerts`. Gated by `SLACK_FUSE_BACKFILL` (default true). |
| `slack_fuse/fuse_ops.py` | `SlackFuseOps(pyfuse3.Operations)`. Path-string-driven dispatch via `_list_dir_impl`, `_resolve_content_impl`, `_is_dir_impl`. Handles the `.cached-only/` prefix via `_strip_cached_prefix` + `cached_only_mode()`. Generates an `index` symlink inside in-thread huddle dirs that points back to `/huddles/<month>/<day>/<slug>` (see `_is_index_backlink` and `readlink`). |
| `slack_fuse/inode_map.py` | `InodeMap`: stable path↔inode mapping. `get_path`, `get_or_create`, `count`. |
| `slack_fuse/renderer.py` | YAML-frontmatter markdown rendering. `render_channel_metadata`, `render_day_snapshot`, `render_day_feed`, `render_thread_snapshot`, `render_thread_feed`. Uses `mrkdwn.convert()` for message bodies and `UserCache` for `<@U…>` resolution. |
| `slack_fuse/mrkdwn.py` | Slack mrkdwn → standard markdown. Handles `<@U…>`, `<#C…>`, `<url\|label>`, `*bold*`, `_italic_`, `~strike~`, code, blockquotes. |
| `slack_fuse/canvas.py` | `fetch_canvas_markdown`: pulls a Slack canvas's HTML via `files.info` + private URL download, then regex-converts HTML → markdown. Used for huddle notes (`notes.md`). Resolves `@U…` mentions via the same `_UserResolver` protocol as `transcript.py`. |
| `slack_fuse/transcript.py` | `fetch_transcript_markdown`: hits `files.info?include_transcription=true` to pull huddle transcripts as Slack Blocks JSON, then renders to markdown. Returns `None` on failure (logged) so a missing transcript never breaks a directory listing. |
| `slack_fuse/user_cache.py` | `UserCache`: bulk-fetches workspace users at startup (`populate()`), provides `get_display_name(user_id)` used by renderer/canvas/transcript. Persists to disk so restarts are cheap. |
| `slack_fuse/slug.py` | `slugify(text)` — lowercase, ASCII, dashes for everything else. |
| `slack_fuse/adapters/` | Currently empty (just `__init__.py`). Reserved namespace. |
| `tests/` | Currently empty. `pytest`/`pytest-trio` are configured but no tests written yet. |

## Conventions

- **Python 3.12**, `from __future__ import annotations` everywhere.
- **basedpyright strict** — every public function should be fully typed. Use `pyright: ignore[reportAttributeAccessIssue]` for the few pyfuse3 attribute assignments that don't have stubs (already in `fuse_ops.py`).
- **ruff** with `E,F,W,I,UP,B,SIM,RUF` and preview rules enabled.
- **Frozen dataclasses** for all domain models — no in-place mutation, makes the cache round-trip and concurrent reads safe.
- **trio**, not asyncio. The whole async surface is trio-native (pyfuse3 supports trio directly).
- **Errors**: `RateLimitedError` and `FatalAPIError` from `api.py` are the two retry-relevant types. Backoff state lives in `store.py`.
- **No business logic in `fuse_ops.py`** beyond path parsing and dispatch — anything that touches Slack data goes through `SlackStore`.
- **Renderers are pure**: `renderer.py` takes models + a user resolver, returns bytes. No I/O.

## Dev commands

```bash
uv sync                            # install + lockfile
uv run slack-fuse mount --debug    # foreground, verbose
uv run ruff check .
uv run ruff format .
uv run basedpyright
uv run pytest
```

Use `uv add` / `uv remove` to mutate dependencies — never `uv pip install` against this project's environment.

## Runtime gotchas

- The systemd unit's `EnvironmentFile=` is `~/agentic/slack-fuse/.env`. If `SLACK_USER_TOKEN` isn't there (or in `~/.config/slack-fuse/config.json`), `load_tokens()` raises and the service crashloops.
- `fusermount3` must be on PATH. If a previous mount left things in a bad state, `__main__.py` calls `fusermount3 -uz` on startup, but a manual `fusermount3 -u ~/views/slack` is the escape hatch.
- Force refresh by sending `SIGUSR1` to the process. The handler calls `store.force_refresh()`.
- The `.cached-only/` prefix is implemented via `store.cached_only_mode()` (a contextmanager that flips a thread-local-ish flag on the store). Anything new in `fuse_ops` that fetches data must go through `_strip_cached_prefix` + `cached_only_mode()`, otherwise `.cached-only/` will silently start hitting the API.

## Things not to do

- Don't commit `.env`. It's gitignored, but double-check `git status` before any commit.
- Don't add asyncio. The mount loop is trio-native; mixing event loops will deadlock pyfuse3.
- Don't introduce mutable dataclasses for the domain models — `disk_cache` round-tripping and the renderer assume frozen tuples.
- Don't widen `cached_only_mode()` to a global flag. It's scoped per-call so concurrent FUSE callbacks don't trample each other.
- Don't remove the `pyright: ignore` comments on pyfuse3 attribute assignments — they're load-bearing because pyfuse3's stubs are incomplete.

## Related docs

- `README.md` — user-facing setup, configuration, filesystem layout.
- `~/docs/slack-fuse.md` — operator notes for this machine specifically (systemd, paths, recovery commands).
