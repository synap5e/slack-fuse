# slack_fuse_server/slurper — the Slack-facing runtime

The server process itself: CLI, task supervision, Slack Web API client, and every long-running ingestion loop. 16 modules, ~7k LOC, of which `__main__.py` is 1,573 and `probes.py` is 1,068.

## Entry point

`slack-fuse-server` → `__main__.py::main`. Subcommands:

| Command | Effect |
|---|---|
| `serve` (default) | Full runtime: socket mode, webhook listener, dispatch server, periodic tasks |
| `refresh-channels` | One-shot channel metadata sweep |
| `refresh-channel-totals` | One-shot `search.messages` totals sweep (the 6h periodic one, run once) |
| `initial-ingest --channel ID \| --all` | First population |
| `backfill CHANNEL_ID [--allow-large] [--max-messages N] [--source slack-api\|legacy-cache] [--since EPOCH]` | Historical import |
| `refill-window CHANNEL_ID --oldest --latest` | Targeted gap refill |
| `block` / `unblock` / `list-blocked` | Operator policy (writes `blocked_channels`, not events) |

## Task inventory

| Module | Loop |
|---|---|
| `socket.py` | Socket Mode connection lifecycle + event translation (754 LOC) |
| `users.py` | Users stream: startup population + live `user_change` |
| `channels.py` | Channel-list population, `ensure_channel_added` |
| `catchup.py` | Reconnect/startup gap fill via `conversations.history` |
| `refresh.py` | Periodic channel metadata refresh |
| `channel_totals.py` | 6-hourly `search.messages` totals, 3.5s between Tier-2 calls |
| `probes.py` | Scheduled + manual probe sweeps against the registry |
| `health.py` | Emits the `slurper-health` stream |

## Cross-cutting machinery

- **`offsets.py`** — the only writer into `events`. See `../AGENTS.md` for its invariants.
- **`ingestion.py`** — `process_boot` / `ingesting` / `make_source`: builds the ambient `events.source` envelope on every row. Fields: `producer`, `boot_id`, `task_id`, `run_id`, `slack_cursor` / `prior_cursor` / `page_index` / `has_more` / `final_page`, `commit` / `image_digest`, `api_endpoint` / `api_latency_ms` / `slack_request_id`, `span_id`, `triggered_by` (`startup | scheduled | reconnect | control-surface | admin-cli`). Slack facts stay in `payload`; `source` is invisible to the wire protocol. Add ambient facts here — **never** derived state, running counters, or anything answering "what work remains".
- **`limiters.py`** — `SlackTierPacer` + `SlurperLimiters`. Slack tier pacing is a limiter, not a sleep sprinkled at the call site.
- **`spans.py`** — `slurper-span op=… task=… result=… duration_ms=… limiter_wait_ms=… sync_ms=…` on stdout. This is the operation-level evidence for "why was it slow"; `span_id` joins to `events.source`.
- **`supervisor.py`** — `TaskSupervisor.phase`: in-memory latest phase + deadline per task, read by `/livez`. Models scheduler progress, not data flow.
- **`api.py`** — typed `SlackClient`. Every response is `model_validate`d; `dict[str, Any]` does not leave this module.

## Conventions

- Wrap Slack calls as `_api_call(callable)` — pass a method reference or lambda, the `TypeVar` preserves the return type. No string dispatch.
- PG operations are bounded by session `lock_timeout` / `statement_timeout`. Treat `psycopg.errors.LockNotAvailable` and `QueryCanceled` as **recoverable**: backfill/catchup retry once, then propagate.
- Deferred imports (`# noqa: PLC0415`) at `__main__.py:449,858,892`, `socket.py:473`, and ten sites in `../http/handlers.py` keep the cold-start import graph light. Leave them deferred.
- Constructor `# noqa: PLR0913` sites are explicit dependency seams for tests, not accidents.

## ANTI-PATTERNS

- Emitting a new event kind without a dedup index. Redelivery is normal; migrations 0002/0003/0007/0012 exist because it wasn't handled (`socket.py:384-391`).
- Slack network I/O inside a transaction holding a stream lock (`channels.py:232-234`).
- Blocking the socket loop. Nudges are non-blocking (`socket.py:577`).
- Conflating the health signals. "Healthy" must name the observable: serving HTTP (`/health`), scheduler not wedged (`/livez`), Slack ingestion (`slurper-health` events), or client data currency (client-side trailer).
- Letting one probe target's failure abort the sweep — target-level error isolation is deliberate (`probes.py:416`).

## Known open items

Pod memory is the live problem: 1658 OOMKills in 10 days at the 1Gi limit, mitigated to 3Gi, root cause unfound. Suspects are the `chunk_mentions` lookup buffer, the totals sweep working set, backfill page cache, or a task holding state. See BACKLOG.md before you go hunting.
