# slack_fuse_server — ingestion, event log, wire gateway

Holds the Slack tokens. Ingests Slack into an append-only `events` table ordered by `(stream, offset_in_stream)`, then serves that log to mount clients over WS + HTTP. ~16k LOC, larger than the client. Slurper internals have their own AGENTS.md.

## Ingestion paths (three, one writer)

```
Socket Mode   socket.SocketModeRunner._handle_event ─┐
Events API    slack_webhook → inbox.enqueue          ├→ slack_events.dispatcher.SlackEventDispatcher.dispatch
                             → inbox.consume         │     → slurper.offsets.OffsetWriter.write_event
backfill/refresh/probes ──────────────────────────────┘         → assign_offset → insert_event
                                                                → INSERT events + pg_notify('new_event', stream)
```

Everything writes through `offsets.write_event`. There is no second insert path.

## Serving

```
dispatch.serve_dispatch      one port, peeks the first bytes, routes HTTP vs WS upgrade
  ├ http/server.route_request   → http/handlers.handle_*
  └ wire/server.WireServer      → _handle_subscribe → EventTailer.iter_events_after → EventFrame/CaughtUpFrame
                                   live tail woken by PostgreSQL LISTEN new_event
```

Replay ≤5,000 events inline; beyond that, `SnapshotAtFrame` redirect to `GET /streams/<stream>/snapshot?at=<offset>` — **channel streams only**, singleton streams must be replayed (`wire/server.py:36-50`).

## WHERE TO LOOK

| Task | Location |
|---|---|
| New event kind reaches the log | `slack_events/dispatcher.py::_dispatch_event` + `slurper/socket.py::translate_message_event` |
| Offsets / dedup / NOTIFY | `slurper/offsets.py` |
| New HTTP endpoint | `http/server.py::route_request` → `http/handlers.py` → DTO in `http/dto.py` |
| WS protocol change | `wire/frames.py` (+ capability advertisement in `wire/server.py`) |
| Webhook delivery reliability | `slack_events/inbox.py` (durable queue, retry, retention) |
| Historical import | `backfill/api.py` (Slack API), `backfill/legacy.py` (v1 cache), `backfill/resume.py` (restart safety) |
| Schema change | `migrations/00NN_*.sql`, forward-only; `schema.sql` is the read-only reference |
| Gap / probe diagnostics | `gaps.py`, `gap_detection.py`, `probes/registry.py` |

## Invariants

- **`stream_heads` row lock serialises same-stream offsets** (`offsets.py:1-35,102-106`); dedup no-ops consume no offset; NOTIFY is emitted in the same transaction and delivered only on COMMIT.
- **Every `OffsetWriter` connection must be autocommit** (`offsets.py:265-272`) — there is a constructor guard because non-autocommit turned writes into disposable savepoints.
- **Inbox commits before ACK** (`slack_events/inbox.py:1-12`). The consumer's dedicated connection intentionally holds a row lock across dispatch; `FOR UPDATE SKIP LOCKED` claims, caller owns COMMIT/ROLLBACK. NOTIFY payloads never drive correctness — polling is the fallback.
- **Subscription generations are reserved before async handlers start** (`wire/server.py:242-247,270-380`); every step re-checks, so a retired subscribe cannot resurrect a stream.
- **A per-connection nursery failure closes one socket, never the server** (`wire/server.py:214-224`).
- **Slack fetches happen before entering a stream-lock transaction** (`slurper/channels.py:232-234`). No network I/O while holding a lock.
- **Snapshot generation needs one REPEATABLE READ transaction** (head read + fold + insert) on its own connection and limiter, so it cannot block live writes (`snapshot/generator.py:23-30`, `snapshot/scheduler.py:1-17`).
- **Only committed full-history pages are backfill resume anchors** (`backfill/resume.py:3-22`); bounded `--since` walks and aborted runs are not.
- **Structural socket events must dedup** (`slurper/socket.py:384-391`) or redelivery causes a UniqueViolation crash loop.

## What belongs in `events` (and what does not)

`events` holds facts Slack told us: messages, edits/deletes, channel metadata, membership, health observations. **Operator intent is not an event** — channel blocks live in the mutable `blocked_channels` table and reach clients via periodic block sync, never as `channel_blocked` events. Query-derived facts (per-channel `search.messages` totals) live in `channel_message_totals`; a failed refresh keeps the last count and changes only `refresh_status`/`refreshed_at`.

`events.source` (migration 0009) carries **ambient** ingestion facts only — producer, boot/task/run ids, Slack cursor/page, api latency, `span_id`, `triggered_by`. Never derived state, running counters, or "what work remains".

## ANTI-PATTERNS

- Unauthenticated `/debug/heap` — the tracemalloc payload is sensitive (`http/debug.py:1-10`).
- Adding a second event-insert path that bypasses `OffsetWriter`.
- Expensive per-request aggregation on a served endpoint. `/channel-stats` once counted the `active_messages` fold (261s over 828k events) and CrashLoopBackOffed production for 28h — see BACKLOG.md.
- Treating `/health` as proof of ingestion. It proves the HTTP task can answer, nothing more; `/livez` reads the task supervisor, `slurper-health` events describe Slack-side reality.
