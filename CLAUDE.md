# slack-fuse — Claude notes

Read-only FUSE filesystem exposing a Slack workspace as markdown. Python 3.12, trio + pyfuse3, httpx for the Slack REST API, frozen dataclasses for the domain model.

User-facing docs are in `README.md`. This file is for navigating the codebase.

## Module map

| File | Responsibility |
|---|---|
| `slack_fuse/__main__.py` | CLI entrypoint. `mount` and `unmount` subcommands, env-var parsing (`SLACK_FUSE_BACKFILL`), trio nursery wiring (`pyfuse3.main`, periodic channel-list refresh, archive, optional backfill). Creates the shared `CapacityLimiter(1)` that serializes all sync store/API work. Cancels the nursery scope on unmount so background tasks exit cleanly. Auto-cleans stale mounts on startup with `fusermount3 -uz`. |
| `slack_fuse/auth.py` | `load_tokens()` — reads `SLACK_USER_TOKEN`/`SLACK_APP_TOKEN` from env, falls back to `~/.config/slack-fuse/config.json`. |
| `slack_fuse/api.py` | `SlackClient`: synchronous httpx wrapper around Slack REST with a persistent `httpx.Client` for connection pooling. Generic typed `_get(method, params, response_type)` validates every response into a Pydantic model — no `Any` leaks out. Methods return typed values: `list_conversations -> list[Channel]`, `get_history -> list[Message]`, `get_replies -> Thread`, `get_file_info -> SlackFile \| None`, `search_huddle_canvases -> list[SearchFile]`, `get_history_page -> ConversationsHistoryResponse`. Exception hierarchy: `SlackAPIError` base (raised on all `ok=false`), with `RateLimitedError` (429, has `retry_after`) and `FatalAPIError` (401/403 + body errors like `token_revoked`). Exposes `.http` property so canvas/transcript/user_cache share the client. 0.1s delay between paginated requests. |
| `slack_fuse/models.py` | Pydantic frozen models for both wire and domain (boundary-validated, then carried through). Domain types: `Channel`, `Message`, `Reaction`, `FileAttachment`, `Edited`, `Thread`, `HuddleInfo`. Wire types: `SlackFile`, `FileShares`/`FileShare`, `HuddleTranscription` + `RichTextSection`/`RichTextElement`/`TextStyle`, `SlackUser`/`SlackUserProfile`/`BotInfo`, `SearchFile`/`SearchFilesData`. Response wrappers all inherit a `_SlackResponse` base (`ok`, `error`): `ConversationsListResponse`, `ConversationsHistoryResponse`, `ConversationsRepliesResponse`, `FilesInfoResponse`, `SearchFilesResponse`, `UsersListResponse`, `UsersInfoResponse`, `BotsInfoResponse`. Internal: `HuddleIndexEntry`. Type aliases: `JsonValue`/`JsonObject` (recursive JSON pass-through). Wire-format quirks: `Channel.topic`/`purpose` use `AliasPath` + a `BeforeValidator` that coerces `null` → `""` (declarative). The truthy/cross-field cases (`Channel.name` falling back to `id` on empty, IM `user` → `im_user_id` only when `is_im=True`, `Message.user` falling through to `bot_id` on `None`/empty) live in small typed `model_validator(mode='before')`s — they need either a truthy check or sibling-field access that aliases can't express. `populate_by_name=True` on `_FrozenModel` lets disk-cache dumps round-trip via canonical field names. |
| `slack_fuse/store.py` | The brain. `SlackStore` owns: in-memory caches (channel list, huddle index, day messages, thread slugs, threads), TTL bookkeeping, exponential backoff (`_BackoffState`), LRU-bounded render cache, and `cached_only_mode()` (a `ContextVar[int]` context manager, ref-counted and per-trio-task so archive's flag doesn't leak to FUSE callbacks). `_api_call(fn: Callable[[], T]) -> T \| None` is a typed wrapper that records backoff on failure — pass it a method reference or a lambda, never a string. Catches `SlackAPIError` (non-fatal ok=false) as a recoverable failure alongside rate-limit and network errors. Fuse ops call into `list_channels`, `get_channel_by_slug`, `get_known_dates`, `get_thread_slugs`, `get_day_messages`, `get_huddles_for_thread`, `get_huddle_index`, `get_huddle_by_canvas_id`, `find_huddle_index_entry_by_canvas`, `merge_known_dates`, `force_refresh`, plus `render_*` methods. TTLs: `_CHANNEL_LIST_TTL=1800`, `_HUDDLE_INDEX_TTL=1800`. Day-message TTL is dynamic in `_date_ttl` (today → 300s, earlier → inf). Thread TTL follows the same today-vs-not-today policy. |
| `slack_fuse/disk_cache.py` | Pure functions over `~/.cache/slack-fuse/`. JSON files keyed by channel/date/thread_ts/canvas_file_id. All writes use atomic temp+rename via `_atomic_write_text`. Signatures use `JsonObject` (recursive JSON type from `models`) — typed validation happens in `store.py`. |
| `slack_fuse/backfill.py` | `backfill_all`: trio task that paginates every member channel's full history into the disk cache. Two phases per channel: (1) day backfill via `conversations.history` with 30-180s sleeps and `<channel_id>.done` markers; (2) thread backfill with 2-8s sleeps and `<channel_id>.threads.done` markers. Thread phase tracks skipped threads and only marks done when all succeed. API calls run through `trio.to_thread.run_sync` with the shared limiter. Gated by `SLACK_FUSE_BACKFILL` (default false). |
| `slack_fuse/archive.py` | `archive_all`: background trio task that pre-renders locked-in markdown (dates before today) to `~/.cache/slack-fuse/archive/` for fast grep. Runs every 10 minutes in `cached_only_mode()`, idempotent, skips files already on disk. Layout mirrors the FUSE mount. |
| `slack_fuse/fuse_ops.py` | `SlackFuseOps(pyfuse3.Operations)`. Path-string-driven dispatch via `_list_dir_impl`, `_resolve_content_impl`, `_is_dir_impl`. All FUSE callbacks delegate sync work to a worker thread via `trio.to_thread.run_sync` with a shared `CapacityLimiter(1)` so the event loop stays responsive. Handles the `.cached-only/` prefix via `_strip_cached_prefix` + `cached_only_mode()`. Generates an `index` symlink inside in-thread huddle dirs that points back to `/huddles/<month>/<day>/<slug>`. |
| `slack_fuse/inode_map.py` | `InodeMap`: stable path↔inode mapping. `get_path`, `get_or_create`, `count`. |
| `slack_fuse/renderer.py` | YAML-frontmatter markdown rendering. `render_channel_metadata`, `render_day_snapshot`, `render_day_feed`, `render_thread_snapshot`, `render_thread_feed`. Uses `mrkdwn.convert()` for message bodies and `UserCache` for `<@U…>` resolution. |
| `slack_fuse/mrkdwn.py` | Slack mrkdwn → standard markdown. Handles `<@U…>`, `<#C…>`, `<url\|label>`, `*bold*`, `_italic_`, `~strike~`, code, blockquotes. |
| `slack_fuse/canvas.py` | `fetch_canvas_markdown`: pulls a Slack canvas's HTML via `files.info` + private URL download, then regex-converts HTML → markdown. Used for huddle notes (`notes.md`). Validates the file_info response through `FilesInfoResponse` (typed `SlackFile`) but keeps raw httpx so a canvas fetch failure can't trip the store's backoff. Resolves `@U…` mentions via the same `_UserResolver` protocol as `transcript.py`. |
| `slack_fuse/transcript.py` | `fetch_transcript_markdown`: hits `files.info?include_transcription=true` to pull huddle transcripts as Slack Blocks JSON, validates into `HuddleTranscription` + nested rich-text models, then renders to markdown. Returns `None` on failure (logged) so a missing transcript never breaks a directory listing. |
| `slack_fuse/user_cache.py` | `UserCache`: bulk-fetches workspace users at startup (`populate()`), provides `get_display_name(user_id)` used by renderer/canvas/transcript. Validates `users.list`/`users.info`/`bots.info` responses via Pydantic at the boundary. Persists to disk so restarts are cheap. |
| `slack_fuse/slug.py` | `slugify(text)` — lowercase, ASCII, dashes for everything else. |
| `slack_fuse/adapters/` | Currently empty (just `__init__.py`). Reserved namespace. |
| `tests/` | `pytest`/`pytest-trio` configured. Tests cover: `_api_call` backoff state machine, `cached_only_mode` (incl. nesting), `_date_ttl`/`_thread_ttl` boundaries, path parsing in `fuse_ops`, `_collect_thread_parents`, disk cache round-tripping, model parsing. |

## Conventions

Follows `~/docs/dev/python/` (uv, basedpyright strict, ruff preview, Pydantic at I/O boundaries, frozen models, lazy CLI imports).

- **Python 3.12**, `from __future__ import annotations` everywhere.
- **basedpyright strict** with the standard noise-reducers (`reportUnusedCallResult`, `reportImplicitStringConcatenation`, `reportUnannotatedClassAttribute`, `reportUnknownLambdaType` all off). Target: `0 errors, 0 warnings, 0 notes`. Use `pyright: ignore[reportAttributeAccessIssue]` for the few pyfuse3 attribute assignments that don't have stubs (already in `fuse_ops.py`).
- **ruff** with the full convention rule set (`E,F,W,I,UP,B,SIM,RUF,BLE001,C901,PLR0913,PLR0916,PLR0917,PLR1702,E402,PLC0415`) plus `line-length=120`. `__main__.py` has a per-file ignore for `PLC0415` because the CLI defers heavy imports. The three path-dispatch hubs in `fuse_ops.py` carry `# noqa: C901` — they're inherently dispatch hubs.
- **Pydantic at the I/O boundary** — every Slack response is `model_validate`d into a typed model in `api.py` (or in `canvas.py`/`transcript.py`/`user_cache.py` for their direct httpx calls). `dict[str, Any]` does not leak past these boundaries. The same Pydantic models double as internal frozen domain types. Wire-format quirks use Pydantic's declarative primitives (`AliasPath`, `BeforeValidator`, `populate_by_name`) where possible; small typed `model_validator(mode='before')`s only where truthy checks or cross-field rules force it.
- **`JsonObject`** (from `models.py`) is the recursive JSON type for opaque pass-through (e.g. `disk_cache.py` signatures). Never use `dict[str, Any]` or `dict[str, object]`.
- **Frozen models** for everything — no in-place mutation, safe to share across the trio nursery, free disk-cache round-tripping.
- **trio**, not asyncio. The whole async surface is trio-native (pyfuse3 supports trio directly).
- **Errors**: `SlackAPIError` is the base class in `api.py`; `RateLimitedError` and `FatalAPIError` inherit from it. Backoff state lives in `store.py` and is updated by `_api_call`.
- **`_api_call(callable)`**, not `_api_call("method_name")`. Pass a method reference or lambda; the wrapper preserves the return type via `TypeVar`. No string-based dispatch, no `# type: ignore[assignment]`.
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
- The `.cached-only/` prefix is implemented via `store.cached_only_mode()` (a `ContextVar[int]` context manager, ref-counted and per-trio-task). Anything new in `fuse_ops` that fetches data must go through `_strip_cached_prefix` + `cached_only_mode()`, otherwise `.cached-only/` will silently start hitting the API.

## Things not to do

- Don't commit `.env`. It's gitignored, but double-check `git status` before any commit.
- Don't add asyncio. The mount loop is trio-native; mixing event loops will deadlock pyfuse3.
- Don't introduce mutable dataclasses for the domain models — `disk_cache` round-tripping and the renderer assume frozen tuples.
- Don't replace `cached_only_mode()`'s ContextVar with a plain instance attribute. The ContextVar ensures per-task isolation so archive's long-running pass doesn't leak to FUSE callbacks.
- Don't remove the `pyright: ignore` comments on pyfuse3 attribute assignments — they're load-bearing because pyfuse3's stubs are incomplete.

## Related docs

- `README.md` — user-facing setup, configuration, filesystem layout.
- `~/docs/slack-fuse.md` — operator notes for this machine specifically (systemd, paths, recovery commands).
