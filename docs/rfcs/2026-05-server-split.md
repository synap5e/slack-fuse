# RFC: Server-split slack-fuse with event-sourced backend

**Status**: Draft
**Author**: Simon Pinfold
**Date**: 2026-05-25
**Last revised**: 2026-05-26

## Summary

Split slack-fuse into two cooperating processes:

- A long-lived **server** that holds the Slack token, owns the Socket Mode
  connection, makes every API call, and persists events into an append-only
  log in Postgres.
- A **client** (the FUSE mount itself) that runs its own Postgres instance,
  subscribes to event streams from the server, and projects them into
  pre-rendered markdown chunks. FUSE reads concat chunks; the userspace
  renderer never runs at read time.

The current single-process model couples Slack API I/O, in-memory state, FUSE
handlers, and the disk cache into one Python program that runs per machine.
Each mounted machine re-fetches the same data, rate-limits independently, and
loses live-event continuity across reconnects. The proposed split fixes all
three by funnelling Slack contact through one place and letting readers be
thin replicas of an authoritative log.

This RFC covers the v1 design. Several extensions (cold-lazy tier, auto
tier transitions, multi-tenant deployments, server-side rendering) are
called out explicitly as deferred and the v1 design preserves the seams
needed to add them without rewrite.

## Motivation

### Concrete problems with the current architecture

1. **Stale threads (Bug 3 from the 2026-05-22 peer report).** Threads older
   than 7 days get `_OLD_MSG_TTL = inf` in `_thread_ttl`, and the parent's
   day cache is also `inf` because it's locked-in. If new replies arrived
   before Socket Mode came up, or during a disconnect, there's no path to
   refresh — TTLs say "never refetch" and Socket Mode missed the event.
   The user reads stale `thread.md` until they manually `SIGUSR1` the
   process.

2. **Duplicate Slack API traffic on multi-device mounts.** Every machine
   running slack-fuse re-fetches `conversations.list`, `users.list`,
   per-channel history. With a 320-channel workspace and three devices
   (laptop, desktop, homelab box), startup hits Slack three times for the
   same data. Token-scoped rate limits don't dedupe.

3. **Socket Mode gaps cause cache flushes.** On unclean disconnect,
   `flush_event_logs` drops every in-memory event log so reads fall back
   to the polling TTL. There's no durable record of what we saw between
   the gap. Anything still in the gap is invisible until the next API
   refresh (which, per bug 3, may be never for old threads).

4. **Cold-start time is dominated by population.** `UserCache.populate()`,
   `list_channels`, and the channel-list refresh take seconds even when
   the data is already cached on the local box, because validation +
   slug-dedup + tier filtering all run synchronously at startup.

5. **Cross-device incoherence.** Today the laptop and the desktop have
   independent caches. They drift. A read on one device doesn't warm the
   other.

### Why event sourcing specifically

The current store already models live changes as typed events
(`DayAppend`, `DayReplace`, `DayDelete`, `DayBumpParent`, `ThreadAppend`,
`ThreadReplace`, `ThreadDelete` in `events.py`) and merges them on top of
snapshots inside `merge_day` / `merge_thread`. This is event sourcing
done in-memory, in one process, with no durability beyond the in-memory
log and the periodic API refetch.

The proposed change is to **make the existing event-sourced model
authoritative and durable**. The events table on the server is the only
source of truth. Snapshots are derived. Everything else — local
projections, rendered chunks, FUSE state — is a cache that can be
rebuilt by replaying events from any offset.

Bug 3 becomes a non-issue under this model: "is my thread current?"
becomes "have I applied events up to offset N on stream `channel:<id>`?".
The local projector knows its applied offset; the server has the actual
offset; resume protocol catches up the gap. No TTL guessing.

## Architecture

### Two processes, both Postgres-backed

```
                                Slack API + Socket Mode
                                          │
                                          ▼
                              ┌──────────────────────┐
                              │   slack-fuse-server  │
                              │  ──────────────────  │
                              │  Slack token         │
                              │  Socket Mode driver  │
                              │  API call gateway    │
                              │  Backfill task       │
                              │                      │
                              │  Postgres (events)   │
                              │   ▸ events           │
                              │   ▸ snapshots        │
                              │   ▸ channels         │
                              │   ▸ users            │
                              └──────────┬───────────┘
                                         │
                              WebSocket (subscribe / snapshot / event)
                                         │
                                         ▼
                              ┌──────────────────────┐
                              │   slack-fuse client  │
                              │  ──────────────────  │
                              │  Projector loop      │
                              │  Renderer-as-lib     │
                              │  FUSE handlers       │
                              │  notify_store sink   │
                              │                      │
                              │  Postgres (local)    │
                              │   ▸ chunks           │
                              │   ▸ thread_chunks    │
                              │   ▸ channels         │
                              │   ▸ users            │
                              │   ▸ cursors          │
                              └──────────────────────┘
```

### Deployment shape

Single-user, multi-device homelab. One server instance runs on a
long-lived box (homelab server, NUC, whatever). One or more client
instances run on user-facing machines (laptop, desktop, the server
itself). The server runs Postgres for the event store; each client runs
its own Postgres for its projections. A `DATABASE_URL` env var decides
where each side's Postgres lives (local socket, container, network).

Multi-tenant is **not** in scope. The protocol can carry a tenant id
later if needed; v1 assumes one workspace per server.

### Server-side HTTP surface

Alongside the WebSocket server, the server exposes a small HTTP
surface for one-shot RPCs and operator visibility. Same process, same
port — Caddy/Tailscale-friendly if remote access is wired up later.

| Path | Method | Purpose |
|---|---|---|
| `/ws` | GET (upgrade) | WebSocket — the event stream described in *Wire protocol* |
| `/resolve` | POST | Resolve a Slack permalink → FUSE path. Body: `{url}`. Response: `{path}` |
| `/permalink` | POST | Resolve a FUSE path → Slack permalink. Body: `{path, ts?}`. Response: `{url}` |
| `/metrics` | GET | JSON snapshot of slurper state (see below) |
| `/health` | GET | Liveness probe. Returns `200 {ok: true}` if the process is alive |

`/metrics` is a single JSON document, not Prometheus text. Easy to
`curl | jq` and easy to scrape later if anyone wants to wrap it. v1
shape:

```jsonc
{
  "server_started_at": "2026-06-01T08:00:00Z",
  "slack": {
    "socket_mode_state": "connected",
    "last_event_at": "2026-06-07T12:34:56Z",
    "rate_limit_budget": { "remaining_pct": 87 },
    "last_health_kind": "slack_healthy"
  },
  "streams": [
    { "stream": "users",          "head_offset": 1240,    "events_per_min": 0 },
    { "stream": "channel-list",   "head_offset": 89,      "events_per_min": 0 },
    { "stream": "slurper-health", "head_offset": 312,     "events_per_min": 0 },
    { "stream": "channel:C0...",  "head_offset": 184600,  "events_per_min": 12 }
    // ...one per known stream
  ],
  "backfill": {
    "in_progress": [{ "channel_id": "C09...", "messages_so_far": 4200 }],
    "completed_count": 287,
    "aborted_count": 3
  },
  "subscribers": {
    "active_ws_connections": 2,
    "by_client": [
      { "client_id": "laptop", "connected_since": "...", "subscriptions": 320 }
    ]
  }
}
```

No Prometheus text export in v1. The shape is amenable to a future
adapter when someone wants to scrape into Grafana etc.

### Bulk-data endpoint format

WebSocket frames are JSON (one frame per WS message). HTTP responses
that return bulk data — historical event dumps, large search results,
snapshot exports — use **JSONL**: one JSON object per line, streamable
without loading the whole response into memory client-side. v1 doesn't
have a bulk-dump endpoint exposed, but the convention is established
so that when one is added (likely the `slack-fuse-server dump-stream`
admin tool exposing over HTTP, or a future search endpoint) it's
predictable.

### What lives where

| Concern | Server | Client |
|---|---|---|
| Slack token | yes | no (the `resolve`/`permalink` CLIs proxy through server) |
| Socket Mode connection | yes (single) | no |
| `httpx.Client` to slack.com | yes | no |
| Event log | authoritative | not stored |
| Snapshots | yes | not stored (re-derived from events on demand) |
| Rendered markdown chunks | no | yes (the projection) |
| User cache | source-of-truth in `users` table | local copy in its own `users` table |
| Channel list | source-of-truth in `channels` table | local copy |
| Tier metadata | (mirrored) | authoritative (per-client preferences) |
| FUSE mount | no | yes |
| Backfill task | yes | no |
| Renderer | optional (for cold-fetch RPC, deferred) | yes (default) |

Note that **tier is per-client**, not per-server. Different machines may
hide / hot different channels. This falls out for free because the
client's `channels` table is its own; the server's is just an inventory.

## Configuration

Both processes read config from env vars first, then a TOML file at a
conventional path, then built-in defaults. Mirrors the current
slack-fuse `load_tokens` / `load_mountpoint` precedence so a `.env`
next to the binary works for ad-hoc shells that don't inherit the
user-systemd environment.

### Server (`slack-fuse-server`)

Path: `~/.config/slack-fuse-server/config.toml` (or `$XDG_CONFIG_HOME`
equivalent). Env vars take precedence; env var name is the uppercase
key with `SLACK_FUSE_SERVER_` prefix.

```toml
# Slack credentials.
slack_user_token = "xoxp-..."
slack_app_token = "xapp-..."
slack_bot_token = "xoxb-..."

# Postgres.
database_url = "postgresql:///slack_fuse_server"

# WebSocket server.
listen_addr = "127.0.0.1:8765"
shared_secret = "..."   # required; clients send it as a header

# Snapshot cadence.
snapshot_every_n_events = 5000
snapshot_max_age_hours = 24

# Backfill thresholds.
backfill_warn_at = 5000
backfill_abort_at = 20000
backfill_page_sleep_min_s = 30.0
backfill_page_sleep_max_s = 180.0
backfill_thread_sleep_min_s = 2.0
backfill_thread_sleep_max_s = 8.0

# Health-stream debouncing.
slack_degraded_min_duration_s = 30.0
```

### Client (`slack-fuse`)

Path: `~/.config/slack-fuse/config.toml`. Env vars: `SLACK_FUSE_`
prefix.

```toml
# Server endpoint.
server_url = "ws://localhost:8765"
shared_secret = "..."

# Postgres.
database_url = "postgresql:///slack_fuse"

# Mountpoint (overridden by SLACK_FUSE_MOUNTPOINT env var per current
# behaviour).
mountpoint = "/views/slack"

# Staleness trailer behaviour.
stale_trailer_enabled = true
stale_after_disconnect_s = 60.0
catchup_window_s = 10.0
```

Every magic number in the RFC body that names a default (snapshot
cadence, backfill thresholds, heartbeat intervals, paginated-snapshot
size cap, etc.) corresponds to a config key here. The body keeps the
numbers as docs-of-defaults; the config keys are the authoritative
overrides.

## Wire protocol

WebSocket transport for the event stream. JSON-encoded frames, one
frame per WebSocket message. One connection per client. Multiple
stream subscriptions multiplexed on that connection.

HTTP endpoints (see *Server-side HTTP surface* above) use plain JSON
for request bodies and small responses. Any endpoint that returns
bulk data (event-log dumps, large search results, snapshot exports)
uses **JSONL** — one JSON object per line, no enclosing array — so
clients can stream-parse without loading the whole response into
memory. No bulk-data endpoint is exposed in v1, but the convention
is established for when one is added.

### Stream identifiers

Coarse-grained. One stream per top-level concept:

- `channel-list` — singleton. Workspace channel inventory (add/remove/rename).
- `users` — singleton. Workspace user directory.
- `channel:<channel_id>` — one per channel. Carries all activity within
  that channel: top-level messages, edits, deletes, reactions, thread
  replies. Subscribers receive every event for the channel, regardless
  of which day/thread the event belongs to.
- `slurper-health` — singleton. Server-self-reported health of the
  Slack-side ingestion pipeline. See *Slurper health stream* below.

Total stream count for a typical workspace ≈ `N_channels + 3`. The
client maintains an open subscription per visible channel plus the
three singletons. WebSocket multiplexes these onto one connection.

Fine-grained streams (per-day, per-thread) are **not** in v1. They'd
shrink wasted bandwidth for clients that only read a few days of a busy
channel, but at the cost of much more protocol surface area. Defer.

### Frame types

```jsonc
// Client → server: open a subscription, or resume an existing one.
{
  "type": "subscribe",
  "stream": "channel:C0AKQ5DS0FQ",
  "since": 184523   // last applied offset; 0 = from beginning
}

// Server → client: catch-up snapshot. Used when `since` is too far
// behind for cheap replay. The payload is a full materialization of
// stream state at offset `at`. After sending, the server streams
// events with offset > at. For streams whose snapshot exceeds the
// configured single-frame size cap, see "Paginated snapshots" below.
{
  "type": "snapshot",
  "stream": "channel:C0AKQ5DS0FQ",
  "at": 184500,
  "payload": { /* opaque, stream-kind-specific */ }
}

// Server → client: an individual event. Always strictly increasing
// offset within a stream.
{
  "type": "event",
  "stream": "channel:C0AKQ5DS0FQ",
  "offset": 184524,
  "kind": "message",
  "ts": "1779000000.000100",
  "payload": { /* event-kind-specific */ }
}

// Server → client: catch-up boundary marker. After this frame the
// client has seen every event up to head_offset; subsequent `event`
// frames on this stream are live.
//
// Informational only — the projector applies every event the same
// way (sync, one TX) per *Flow control* above. The trailer logic
// uses this frame to clear the "catching up after reconnect"
// degradation reason for this stream.
{
  "type": "caught_up",
  "stream": "channel:C0AKQ5DS0FQ",
  "head_offset": 184600
}

// Bidirectional heartbeat. Client must ping every 30s; server treats
// a client as dead after 90s with no frame received. Server pings on
// the same cadence; client treats the connection as dead and triggers
// reconnect after the same timeout.
{ "type": "ping" }
{ "type": "pong" }

// Server → client: stream/connection-level errors.
{ "type": "error", "code": "stream_not_found", "stream": "..." }
{ "type": "error", "code": "since_too_high", "stream": "...", "head_offset": 184523 }
{ "type": "error", "code": "auth_failed" }
```

### Subscribe response semantics

When the server receives `subscribe { stream, since }`:

1. **Unknown stream** (e.g. `channel:CDELETED`) → emit `error { code:
   stream_not_found }` and close the subscription. Client should drop the
   stream from its `cursors` table.
2. **`since` > current head** → emit `error { code: since_too_high,
   head_offset }`. Indicates client state is corrupt; client should reset
   the cursor and resubscribe with `since: 0`.
3. **`since` very old, snapshot covers `M ≥ since`** → emit `snapshot`
   frame at offset `M`, then stream events `(M, head]`, then emit
   `caught_up { head_offset: head }`.
4. **`since` recent enough to replay** → stream events `(since, head]`,
   then emit `caught_up`.
5. **`since` equal to head** → emit `caught_up` immediately. No
   snapshot or events.
6. After `caught_up`, live events from Socket Mode arrive as `event`
   frames in real time.

The `caught_up` frame is informational — the projector applies every
event the same way (per-event sync apply, see *Flow control*). The
trailer logic uses the frame to clear the "catching up after
reconnect" degradation reason for the stream.

### Paginated snapshots

For streams whose snapshot at offset `M` exceeds 4 MB (a conservative
WS-frame cap), the server emits a sequence:

```jsonc
{ "type": "snapshot_begin", "stream": "...", "at": 184500, "total_parts": 7 }
{ "type": "snapshot_part",  "stream": "...", "at": 184500, "part": 1, "payload": { ... } }
// ... part 2..6 ...
{ "type": "snapshot_end",   "stream": "...", "at": 184500 }
```

The client buffers parts in memory, applies them as one transaction on
`snapshot_end`, then expects events `> at`. Paginated and unpaginated
snapshots are interchangeable from the client's POV after assembly.

Snapshot payload format (for `channel:<id>`) is a flat list of the
current state of every undeleted top-level message + their thread
replies. Generation strategy: full-state rebuilds, taken on a cadence
of every 5000 events per stream or daily, whichever comes first.
Delta snapshots are deferred.

### Ordering guarantees

- **Per-stream**: events for a single stream arrive in strictly
  increasing offset order. Resume from a cursor always plays each event
  exactly once.
- **Cross-stream**: ordering is **undefined**. A `user_renamed` event
  on `users` and a `message` event on `channel:CX` referencing that
  user may arrive in either order. The projector handles this
  gracefully because mention substitution happens at FUSE-read time
  (see *Renderer* below), not at chunk-write time. Chunks store
  unresolved `<@U…>` placeholders.
- **Cross-server-restart**: offsets are persistent and monotonic
  across server restarts (sourced from `stream_heads.next_offset`).

### Flow control

**There is no application-level flow control.** TCP backpressure does
the job, and the projector contract makes it work.

The projector applies each event synchronously: receive an `event`
frame, run one postgres transaction (chunk write + `chunk_mentions`
update + `cursors.applied_offset` advance), THEN read the next WS
frame. No in-memory event queue beyond what the OS socket buffer
holds.

When the projector is slow (DB write contention, renderer CPU,
postgres on a slow disk), the WS receive halts; the kernel's receive
window shrinks; the server's WS send blocks. The server learns the
client can't keep up because its send buffer is full. No protocol
frames need to carry this information — the layers underneath already
do.

This means the system **cannot reach a "client is silently lagging"
state**. Either:

- The projector applies at its sustainable rate and the slurper is
  implicitly throttled to that, or
- The connection drops and the trailer fires for the actual reason
  (socket closed), or
- The initial catch-up never completes because the projector is
  catastrophically slow (e.g. disk failure) — in which case the
  trailer fires with `catching up after reconnect` and the user goes
  to investigate the actual problem.

**Bulk catch-up via snapshots.** A snapshot frame applies as one
postgres transaction (paginated snapshots buffer the parts and apply
on `snapshot_end`). This means catch-up speed is bounded by DB write
throughput for *one big write*, not per-event. The per-event sync
apply only governs the post-snapshot tail.

**Idempotent re-apply.** Every chunk write is `INSERT … ON CONFLICT
DO UPDATE`. Every event's effect on the projection is deterministic
from `(offset, payload)`. If a connection drops mid-apply, the next
subscribe re-sends from `cursors.applied_offset` and the projector
re-applies the partial batch harmlessly. There is no protocol-level
"please ack" frame needed because the worst case of re-delivery is a
no-op.

### Slurper health stream

A singleton `slurper-health` stream the server self-publishes to so
clients can detect "the pipeline upstream of me is broken" without
out-of-band signalling. Every client subscribes from offset 0 at
startup.

| Wire kind | Payload | Meaning |
|---|---|---|
| `slack_healthy` | `{}` | Socket Mode connected, API calls succeeding |
| `slack_degraded` | `{reason: "rate_limited" \| "api_5xx" \| ...}` | Slack-side issues, ingestion partially working |
| `socket_mode_disconnected` | `{}` | WS to Slack dropped, attempting reconnect |
| `socket_mode_reconnected` | `{gap_seconds}` | Reconnected after a disconnect; gap window logged |
| `auth_token_invalid` | `{}` | Slack rejected our token; ingestion stopped |
| `backfill_started` | `{channel_id}` | Synthetic-event backfill running for this channel |
| `backfill_completed` | `{channel_id, events_written}` | |
| `backfill_aborted` | `{channel_id, reason}` | Hit per-channel size limit (see *Backfill* below) |

The client uses these to drive the *Offline behaviour* stale markers
(see below). The events also write to a `health_log` table on the
server for operator inspection.

### Snapshot vs event replay decision

When the server receives `subscribe { since: N }`:

- If there's a snapshot for the stream at offset `M ≥ N`, ship that
  snapshot then tail events from `M + 1`.
- If no snapshot covers `N` (e.g. `N` is very recent and we haven't
  snapshotted since), replay events from `N + 1`.
- If `N == 0` and no snapshot exists yet (fresh stream), tail events
  from `1`.

The server periodically materializes snapshots per stream (cadence
TBD; suggested: every 1000 events or daily, whichever comes first).
Snapshot generation is the server's job and is invisible to the client
beyond making catch-up cheap.

### Event kinds

The wire vocabulary is designed fresh against the new model — it is
**not** a translation of the current `events.py` types. Those types
(`DayAppend`, `DayBumpParent`, `ThreadAppend`, …) are bookkeeping for
the day-keyed in-memory event log in the current store and are deleted
along with the rest of that store in Phase 4.

Two principles for the wire format:

1. **Slack-native naming.** Where a Slack Events API event already
   exists, use its name (`message`, `message_changed`,
   `reaction_added`). Minimal translation work on the server side and
   familiar vocabulary for anyone reading the protocol against Slack's
   own docs.
2. **No projector-internal events on the wire.** Things like
   "parent's reply count changed" are projector-side derivations from
   a `message` event whose `thread_ts != ts`. They never appear as
   wire events.

`channel:<channel_id>` stream:

| Wire kind | Payload | Source |
|---|---|---|
| `message` | full message object including `thread_ts` | Slack `message`, history backfill |
| `message_changed` | new message object + previous `ts` | Slack `message.message_changed` |
| `message_deleted` | `deleted_ts`, optional `previous_message` | Slack `message.message_deleted` |
| `reaction_added` | `target_ts`, `user`, `emoji` | Slack `reaction_added` |
| `reaction_removed` | `target_ts`, `user`, `emoji` | Slack `reaction_removed` |

(`pin_added` / `pin_removed` deferred unless we end up rendering pins.)

`channel-list` stream:

| Wire kind | Payload |
|---|---|
| `channel_added` | full channel object |
| `channel_renamed` | `channel_id`, `new_name` |
| `channel_archived` | `channel_id` |
| `channel_unarchived` | `channel_id` |
| `channel_member_changed` | `channel_id`, `is_member` |

`users` stream:

| Wire kind | Payload |
|---|---|
| `user_added` | full user object |
| `user_renamed` | `user_id`, `new_display_name` |
| `user_profile_changed` | `user_id`, `profile_fields` |

Threads are not their own streams. A `message` event with `thread_ts !=
ts` is a thread reply; the projector handles parent-row updates as a
side effect when applying it. This keeps the wire format flat and
avoids the projector having to merge ordering across two streams.

### Auth

v1: shared-secret in a header. Server config sets a token; client config
matches it. Single-user homelab posture. **Slack tokens never leave the
server.**

The `resolve` and `permalink` CLIs that today read tokens locally will
be reworked to proxy through the server via a small RPC layer (same
WebSocket connection, different message types). This means the client
machine doesn't need Slack credentials at all — just the shared-secret
to talk to its own server.

Multi-tenant, mTLS, OIDC — all deferred.

## Schemas

### Server: events store

```sql
-- The append-only event log.
CREATE TABLE events (
    id BIGSERIAL PRIMARY KEY,
    stream TEXT NOT NULL,
    offset_in_stream BIGINT NOT NULL,
    kind TEXT NOT NULL,
    ts TEXT,                          -- Slack message ts when applicable
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (stream, offset_in_stream)
);
CREATE INDEX events_stream_offset_idx ON events (stream, offset_in_stream);

-- Periodic snapshots so cold consumers don't replay from offset 0.
-- The cost columns are first-party instrumentation for the
-- still-open snapshot-cadence question — they let us measure whether
-- snapshots are paying for themselves before tuning cadence.
CREATE TABLE snapshots (
    stream TEXT NOT NULL,
    at_offset BIGINT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload_bytes BIGINT NOT NULL,
    events_covered BIGINT NOT NULL,
    generation_duration_ms INT NOT NULL,
    generation_trigger TEXT NOT NULL
        CHECK (generation_trigger IN ('event_count', 'time', 'manual')),
    PRIMARY KEY (stream, at_offset)
);

-- One row per time a snapshot was used to catch a client up. Lets us
-- measure cache-hit-rate per snapshot: if `events_skipped` is small,
-- the snapshot wasn't worth generating.
CREATE TABLE snapshot_uses (
    snapshot_stream TEXT NOT NULL,
    snapshot_at_offset BIGINT NOT NULL,
    used_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    client_since_offset BIGINT NOT NULL,
    events_skipped BIGINT NOT NULL,
    FOREIGN KEY (snapshot_stream, snapshot_at_offset)
        REFERENCES snapshots (stream, at_offset)
);
CREATE INDEX snapshot_uses_lookup_idx
    ON snapshot_uses (snapshot_stream, snapshot_at_offset);

-- Workspace inventory. Mirrored from Slack via events into a queryable
-- materialization for fast channel-list answers (so subscribe to
-- channel-list isn't required for cold metadata reads).
CREATE TABLE channels (
    channel_id TEXT PRIMARY KEY,
    name TEXT,
    is_im BOOLEAN,
    is_mpim BOOLEAN,
    is_member BOOLEAN,
    is_archived BOOLEAN,
    im_user_id TEXT,
    topic TEXT,
    purpose TEXT,
    num_members INT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE users (
    user_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Live cursor across all streams (the current write head). Used by the
-- server to assign monotonically increasing offsets within a stream
-- under concurrent writes.
CREATE TABLE stream_heads (
    stream TEXT PRIMARY KEY,
    next_offset BIGINT NOT NULL DEFAULT 1
);

-- Append-only log of slurper health transitions. Mirrors what the
-- server publishes on the slurper-health stream; here so an operator
-- can SELECT directly without parsing the event log.
CREATE TABLE health_log (
    id BIGSERIAL PRIMARY KEY,
    kind TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

**Offset assignment pattern.** Concurrent writers to the same stream
must serialize via the `stream_heads` row lock. Concurrent writers to
different streams are independent. Canonical write transaction:

```sql
BEGIN;
INSERT INTO stream_heads (stream) VALUES ($1)
  ON CONFLICT (stream) DO NOTHING;
UPDATE stream_heads
   SET next_offset = next_offset + 1
 WHERE stream = $1
RETURNING next_offset - 1 AS my_offset;
INSERT INTO events (stream, offset_in_stream, kind, ts, payload)
VALUES ($1, $my_offset, $2, $3, $4);
COMMIT;
```

The `UPDATE ... RETURNING` row-locks the stream's `stream_heads` row
for the duration of the transaction. v1 has a single-process slurper
so contention is theoretical, but the pattern survives parallelisation
(e.g. one task per channel during backfill) without code changes.

### Client: projections store

```sql
-- One pre-rendered markdown block per top-level message in a day.
-- Composing channel.md = ORDER BY message_ts and concat content_md.
CREATE TABLE chunks (
    channel_id TEXT NOT NULL,
    date TEXT NOT NULL,                -- 'YYYY-MM-DD' local-tz date
    message_ts TEXT NOT NULL,          -- Slack ts
    content_md TEXT NOT NULL,          -- output of render_message(...)
    reply_count INT NOT NULL DEFAULT 0,
    accessed_at TIMESTAMPTZ,           -- unused in v1; v2 LRU eviction
    PRIMARY KEY (channel_id, date, message_ts)
);
CREATE INDEX chunks_lookup_idx ON chunks (channel_id, date, message_ts);

-- One pre-rendered block per message in a thread (parent + replies).
CREATE TABLE thread_chunks (
    channel_id TEXT NOT NULL,
    thread_ts TEXT NOT NULL,
    reply_ts TEXT NOT NULL,            -- equals thread_ts for the parent row
    role TEXT NOT NULL CHECK (role IN ('parent', 'reply')),
    content_md TEXT NOT NULL,
    accessed_at TIMESTAMPTZ,
    PRIMARY KEY (channel_id, thread_ts, reply_ts)
);
CREATE INDEX thread_chunks_lookup_idx ON thread_chunks (channel_id, thread_ts, reply_ts);

-- Mirrored channel inventory + per-client tier preferences.
CREATE TABLE channels (
    channel_id TEXT PRIMARY KEY,
    name TEXT,
    is_im BOOLEAN,
    is_mpim BOOLEAN,
    is_member BOOLEAN,
    is_archived BOOLEAN,
    im_user_id TEXT,
    topic TEXT,
    purpose TEXT,
    tier TEXT NOT NULL DEFAULT 'hot'
        CHECK (tier IN ('hot', 'hidden', 'blocked')),
    tier_source TEXT NOT NULL DEFAULT 'auto'
        CHECK (tier_source IN ('auto', 'manual')),
    subscribed BOOLEAN NOT NULL DEFAULT TRUE,
    last_accessed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Local user cache. Mirrored from server.
CREATE TABLE users (
    user_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Per-stream applied offset. The projector advances these.
CREATE TABLE cursors (
    stream TEXT PRIMARY KEY,
    applied_offset BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Side table: which user/channel IDs are mentioned inside which
-- chunks. Lets a `user_renamed` or `channel_renamed` event invalidate
-- the affected inodes in O(N) instead of `WHERE content_md LIKE …`
-- over the whole chunks table. Populated as chunks are written.
CREATE TABLE chunk_mentions (
    channel_id TEXT NOT NULL,
    date TEXT NOT NULL,
    message_ts TEXT NOT NULL,
    mention_kind TEXT NOT NULL CHECK (mention_kind IN ('user', 'channel')),
    mentioned_id TEXT NOT NULL,
    PRIMARY KEY (channel_id, date, message_ts, mention_kind, mentioned_id),
    FOREIGN KEY (channel_id, date, message_ts)
        REFERENCES chunks (channel_id, date, message_ts)
        ON DELETE CASCADE
);
CREATE INDEX chunk_mentions_lookup_idx
    ON chunk_mentions (mention_kind, mentioned_id);

CREATE TABLE thread_chunk_mentions (
    channel_id TEXT NOT NULL,
    thread_ts TEXT NOT NULL,
    reply_ts TEXT NOT NULL,
    mention_kind TEXT NOT NULL CHECK (mention_kind IN ('user', 'channel')),
    mentioned_id TEXT NOT NULL,
    PRIMARY KEY (channel_id, thread_ts, reply_ts, mention_kind, mentioned_id),
    FOREIGN KEY (channel_id, thread_ts, reply_ts)
        REFERENCES thread_chunks (channel_id, thread_ts, reply_ts)
        ON DELETE CASCADE
);
CREATE INDEX thread_chunk_mentions_lookup_idx
    ON thread_chunk_mentions (mention_kind, mentioned_id);

-- Persistent FUSE inode mapping. Allocated on first lookup; never
-- recycled. Survives mount restarts so `find` outputs, fd-based
-- watching, and tools that cache inodes don't break across restarts.
CREATE TABLE inodes (
    path TEXT PRIMARY KEY,
    inode BIGINT NOT NULL UNIQUE GENERATED ALWAYS AS IDENTITY (START WITH 2)
);
-- Inode 1 is reserved for the filesystem root.

-- Tracks last successful contact with the server (any frame received).
-- Used by the FUSE read layer to decide whether to append a "content
-- may be stale" trailer. Updated on every frame from the WS connection.
CREATE TABLE connection_state (
    id INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    last_frame_at TIMESTAMPTZ,
    last_slurper_health TEXT NOT NULL DEFAULT 'unknown'
        CHECK (last_slurper_health IN
            ('unknown', 'healthy', 'degraded', 'disconnected', 'auth_failed')),
    last_health_update_at TIMESTAMPTZ
);
INSERT INTO connection_state (id) VALUES (1);

-- Per-stream catch-up state. Set when a `caught_up` frame arrives
-- for the stream; cleared when the WS reconnects. The FUSE read
-- layer uses this to drive the "initial catch-up incomplete"
-- trailer condition per stream.
CREATE TABLE stream_caught_up (
    stream TEXT PRIMARY KEY,
    caught_up_at TIMESTAMPTZ NOT NULL,
    at_offset BIGINT NOT NULL
);

-- Log of every trailer-append-or-suppress decision. Lets us measure
-- the false-positive rate of the single-row `connection_state`
-- granularity, so the still-open "connection-state granularity"
-- question is data-driven when we revisit. Rotate weekly to bound
-- disk.
CREATE TABLE trailer_decisions (
    decided_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    inode BIGINT NOT NULL,
    stream TEXT NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('appended', 'suppressed')),
    reason TEXT,
    connection_state_last_frame_at TIMESTAMPTZ,
    connection_state_last_health TEXT,
    stream_applied_offset BIGINT,
    -- v1 doesn't know the server's current head per stream; NULL here
    -- until we add per-stream head tracking. The column is here so
    -- post-hoc analysis can backfill it from server logs if needed.
    stream_head_offset_at_decision BIGINT
);
CREATE INDEX trailer_decisions_time_idx ON trailer_decisions (decided_at);
```

### Notes on schema design

- **`tier` is TEXT + CHECK**, not a PG enum. Enums are painful to extend
  (`ALTER TYPE` semantics, migration ordering). Adding `'cold'` later
  is `DROP CONSTRAINT; ADD CONSTRAINT`.

- **`subscribed` is a separate column** even though in v1 it's always
  derived from tier (`subscribed = tier != 'blocked'`). The projector
  loops over `WHERE subscribed = TRUE`. When cold-lazy tier is added,
  `subscribed` can be set independently per row without changing the
  projector. The derivation rule is enforced by the tier-write code
  path, not by a generated column.

- **`accessed_at` exists from day one** on `chunks` and `thread_chunks`.
  Unused in v1 (no eviction). The v2 LRU sweeper has the column waiting.

- **Date strings, not dates.** `chunks.date` is `TEXT` because all the
  ts→date conversion is already in local-tz and stored as
  `'YYYY-MM-DD'`. PG `DATE` would force timezone reasoning at boundary
  layers. Keep it pure-string.

## Projection logic (event → chunk operations)

The projector applies each event in a single transaction. The rules
are deterministic and pure (no Slack API calls); the projector branches
on the event payload (e.g. `thread_ts` presence) rather than on
separate top-level-vs-reply event kinds:

| Wire event | DB operation |
|---|---|
| `message` with `thread_ts is None` or `thread_ts == ts` | `INSERT INTO chunks` |
| `message` with `thread_ts != ts` (reply) | `INSERT INTO thread_chunks`; also `UPDATE chunks` for the parent row to refresh its `> Thread: N replies` indicator |
| `message_changed` on a top-level message | re-render, `UPDATE chunks` |
| `message_changed` on a reply | re-render, `UPDATE thread_chunks` |
| `message_deleted` on a top-level message | `DELETE FROM chunks` |
| `message_deleted` on a reply | `DELETE FROM thread_chunks`; also `UPDATE chunks` for the parent's indicator |
| `reaction_added` / `reaction_removed` on a top-level message | re-render, `UPDATE chunks` |
| `reaction_added` / `reaction_removed` on a reply | re-render, `UPDATE thread_chunks` |
| `channel_added` | `INSERT INTO channels` with default tier |
| `channel_renamed` | `UPDATE channels` |
| `channel_archived` | `UPDATE channels SET tier = 'blocked'` if `tier_source = 'auto'` |
| `channel_unarchived` | `UPDATE channels` to re-evaluate default tier if `tier_source = 'auto'` |
| `channel_member_changed` | `UPDATE channels`; re-evaluate default tier if `tier_source = 'auto'` |
| `user_added` / `user_renamed` / `user_profile_changed` | `UPSERT INTO users`. For `user_renamed`: query `chunk_mentions` + `thread_chunk_mentions` with `mention_kind = 'user' AND mentioned_id = $uid` for affected `(channel_id, date, message_ts)` / `(channel_id, thread_ts, reply_ts)` tuples; `invalidate_inode` on each affected file. **No chunk rewrites required** — chunks store unresolved `<@U…>` placeholders, the next read picks up the new display name from `users`. Same pattern for `channel_renamed` with `mention_kind = 'channel'`. |

Common case: top-level message arrives. One `INSERT` into `chunks`. Done.

User-rename handling in v1: **accept stale `<@U…>` mentions in stored
chunks**. The renderer ran against the user cache at insert time;
re-render only when the user is `@`-mentioned by a fresh message. A
future "rebuild chunks for affected user" command can do the
`WHERE content_md LIKE '%<@U%'` scan if it becomes annoying.

## FUSE read path

A read of `channels/<slug>/<YYYY-MM>/<DD>/channel.md` does:

1. Resolve `slug → channel_id` via `channels` table.
2. `SELECT content_md FROM chunks WHERE channel_id = ? AND date = ? ORDER BY message_ts`.
3. Compose structural body: `'\n'.join(rows)`.
4. Run `resolve_mentions(body, users_resolver, channels_resolver)`.
5. Prepend frontmatter and append the optional staleness trailer
   (see *Offline behaviour* below).
6. Return bytes.
7. (Hot only) `pyfuse3.notify_store(inode, 0, bytes)` so subsequent
   reads come from the kernel page cache.

The structural renderer runs at **event-application** time, not at
read time. The mention-resolution step is the only string work at read
time and runs in microseconds per file. Frontmatter and concat live in
the FUSE read path because they're cheap and stream-local.

Thread reads are symmetric: `SELECT content_md FROM thread_chunks WHERE
channel_id = ? AND thread_ts = ? ORDER BY reply_ts`, then resolve
mentions, prepend thread frontmatter, append staleness trailer.

Feed-style files (`feed.md`) are out of v1 scope. They're rarely read
and the chunk-store model handles them awkwardly because feed is
append-only while channel.md is in-place. Defer.

## Offline behaviour

The system is **offline-readable** by design. When the server is
unreachable, the slurper is unhealthy, or the auth token is invalid,
local chunks are still served. Reads work; they just don't reflect
recent activity. To make this visible to the user, **every read
appends a staleness trailer when the local view is known to be
behind.**

### Staleness conditions

Three fundamental-degradation conditions. The trailer is appended
when **any** of these is true at read time:

1. **WS disconnected.** WS to the server is currently disconnected
   and the client has been reconnecting unsuccessfully for at least
   60 s (short blips don't trigger; recover quietly).
2. **Slurper upstream unhealthy.** The most recent `slurper-health`
   event is one of `socket_mode_disconnected`, `slack_degraded`, or
   `auth_token_invalid`, and we haven't seen a subsequent
   `slack_healthy` / `socket_mode_reconnected` event.
3. **Initial catch-up incomplete for the stream this file belongs
   to.** Since the current connection opened, we haven't received a
   `caught_up` frame for this file's stream. Means we know we're
   behind by however much arrived between our `cursors.applied_offset`
   and the server's head at connect time.

The trailer is **not** appended when:

- The client is connected, slurper is healthy, and the channel just
  has no recent activity (which is the common case for idle DMs and
  archive-style channels). **Idle ≠ stale.**
- The projector is slow applying live events. This case **cannot
  exist** — per *Wire protocol → Flow control*, per-event sync apply
  + TCP backpressure means the projector either applies at its
  sustainable rate (and the server is naturally throttled) or the
  connection drops (condition 1 fires). There's no third state.

### Trailer format

Appended as a fenced separator below the content body:

```markdown
[...regular content...]

---

> ⚠ Content may be stale. Last successful sync: 2026-05-26 09:42:11 UTC (3 hours ago). Reason: server unreachable.
```

The reason string is one of: `server unreachable`,
`slack ingestion unhealthy (rate_limited)`, `slack ingestion unhealthy (api_5xx)`,
`socket-mode disconnected`, `auth token invalid`,
`catching up after reconnect`.

### Source of truth for staleness

The `connection_state` table is updated by the projector on:

- Every frame received from the server (`last_frame_at = now()`).
- Every `slurper-health` event applied (`last_slurper_health`,
  `last_health_update_at`).

The FUSE read layer SELECTs from `connection_state` once per read.
This is one row, primary-keyed; the lookup is negligible.

### Invariants

- **Reads never fail because the server is unreachable.** Local
  chunks always serve.
- **Reads never block on network.** All staleness signals are in the
  local DB.
- **The trailer never lies.** If it isn't there, the local view is
  current as of the last `caught_up` frame.
- **The trailer always tells you how old.** Specific timestamp and
  reason, not just "may be stale."

## Backfill

The slurper owns historical-data ingestion. Live Socket Mode events
only cover "from when the server started"; everything older needs to be
fetched explicitly via `conversations.history` /
`conversations.replies` and written into the events log as `message`
events that look identical to live ones (modulo offset position).

### Two backfill paths

**(a) Automatic, on-channel-first-seen.** When `channel_added` arrives
on the `channel-list` stream and we have no prior history for the
channel, the slurper queues a backfill task. Throttled — backfill
runs in a single worker, sleeps between pages, takes hours-to-days
total on first server bootup. Yields between channels so live ingestion
stays responsive.

**(b) Manual, via the `slack-fuse-server backfill` admin command.**
For recovery (e.g. a channel that was thought-empty but you've now
joined and want history). Same throttling, same event-writing path.

### Per-channel size threshold

Backfill writes events for each historical message. For very large
channels (busy bots, high-traffic ops channels, multi-year #general)
this can mean millions of events and many GB of payload — neither
of which is useful in slack-fuse's intended use case.

Default behaviour (every threshold is configurable via env var or
config file):

- **`BACKFILL_WARN_AT` (default 5,000 messages)**: emit
  `slack_degraded { reason: "backfill_large", channel_id }` on the
  `slurper-health` stream as a warning. Keep going.
- **`BACKFILL_ABORT_AT` (default 20,000 messages)**: **abort backfill
  for this channel** and emit `backfill_aborted { channel_id, reason:
  "exceeded_default_limit", message_count }`. The channel's events
  table contains only the truncated head; live events continue to be
  written as Socket Mode delivers them.
- **Override per channel**: `slack-fuse-server backfill <channel-id>
  --allow-large` (or `--max-messages N`) lifts/raises the limit for a
  specific channel. Persists in a small `backfill_overrides` table so
  re-runs honour the override.

Defaults are chosen conservatively — they should be re-tuned after
profiling a real workspace's message-count distribution. The override
mechanism handles legitimate outliers (e.g. busy #engineering
channels with years of activity).

The aborted-backfill event drives a v2 UX where the client suggests
`tier = 'blocked'` for the channel, since it's identified itself as
likely noise.

```sql
CREATE TABLE backfill_overrides (
    channel_id TEXT PRIMARY KEY,
    max_messages BIGINT,  -- NULL = no limit
    set_by TEXT NOT NULL DEFAULT 'admin',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### Throttling parameters

- Sleep 30–180 s (uniform random) between `conversations.history`
  pages for the same channel.
- Sleep 2–8 s between thread expansion calls.
- One channel at a time. No parallelism — the wall-clock cost is the
  trade for not tripping rate limits or contending with live events.
- Pause backfill entirely when the slurper is in `slack_degraded`
  state (rate-limited or 5xx).

This is the existing `backfill.py` behavior with the size-threshold
addition; lift it largely intact in Phase 1.

### Migration from existing disk cache

Open question #16 resolved: **blow away `~/.cache/slack-fuse/` on
first new-server bootup; take the backfill hit.** A first run may take
a full day to complete; live ingestion via Socket Mode is unaffected
during backfill.

The backfill itself doubles as a noisy-channel detector: anything that
trips the abort threshold is exactly the kind of channel a user would
want hidden or blocked anyway.

## Renderer-as-library

The current `slack_fuse/renderer.py` is close to the right shape (pure
functions over typed inputs) but needs two changes for v1:

1. **No `dict[str, str]` of display names crossing the API boundary.**
   The library operates on user IDs and channel IDs as typed values
   right up until presentation. Display-name resolution is a separate,
   late-bound step.
2. **No `UserCache` object dependency.** The library is pure; it does
   no file I/O and holds no state. Callers pass in typed lookup
   protocols.

### Two-pass rendering

Slack's mrkdwn → markdown conversion is split into two phases:

- **Structural pass** (`convert_structural`): bold/italic/links/code/
  blockquotes/lists. Pure string-to-string. No external dependencies.
  Runs at chunk-write time and the result is stored.
- **Mention-resolution pass** (`resolve_mentions`): substitutes
  `<@U…>` and `<#C…>` placeholders only. Takes typed resolvers. Runs
  at FUSE-read time during chunk concat.

This means a `user_renamed` event invalidates the affected inodes
(found via `chunk_mentions`) so the kernel page cache drops them, but
**does not require re-rendering any chunks**. The next read substitutes
with the new display name from the updated `users` table. Same for
channel renames.

It also means Slack's `<@U123|cached-name>` form is normalised to
`<@U123>` at chunk-write time — the cached display name is discarded;
our `users` table is always the source of truth at read time.

### Frozen-dataclass types

```python
# slack_fuse_render/types.py

@dataclass(frozen=True, slots=True)
class UserId:
    value: str

@dataclass(frozen=True, slots=True)
class ChannelId:
    value: str

@dataclass(frozen=True, slots=True)
class UserView:
    """What the renderer needs to present a user. Per-client; the
    display_name reflects the consumer's current users-table row."""
    user_id: UserId
    display_name: str

@dataclass(frozen=True, slots=True)
class ChannelView:
    channel_id: ChannelId
    name: str            # rendered name (with #, no leading)
    is_im: bool
    is_mpim: bool
```

### Resolver protocols

```python
# slack_fuse_render/resolvers.py

class UserResolver(Protocol):
    def resolve(self, user_id: UserId) -> UserView | None: ...

class ChannelResolver(Protocol):
    def resolve(self, channel_id: ChannelId) -> ChannelView | None: ...
```

Concrete implementations on the client back these by SELECTing from
the local `users` and `channels` tables. Tests inject in-memory
implementations.

### Public API

```python
# slack_fuse_render/__init__.py

def render_message_structural(msg: Message) -> str:
    """Render a single message to markdown with unresolved
    <@U…>/<#C…> placeholders. Output is stored in `chunks.content_md`.
    Pure; no resolvers needed."""

def resolve_mentions(
    md: str,
    users: UserResolver,
    channels: ChannelResolver,
) -> str:
    """Substitute <@U…> and <#C…> placeholders. Called by the FUSE
    read layer during chunk concat."""

def channel_md_frontmatter(channel: ChannelView, date: str) -> str: ...
def thread_md_frontmatter(channel: ChannelView, parent: Message) -> str: ...

def extract_mention_user_ids(structural_md: str) -> set[UserId]:
    """Returns the set of UserIds referenced by <@U…> placeholders
    in a structural chunk. Used to populate chunk_mentions when the
    projector writes a chunk."""
```

### Rendering decisions baked in

- The decision of "render on client vs server" becomes a deployment
  toggle. v1 ships with client-side rendering only (each device runs
  the renderer against its own users-table snapshot, which is a
  per-client read model). Server-side rendering remains importable
  without refactor.
- Display-name localisation is naturally per-client because mention
  resolution happens at read time against the local `users` table.
- The structural-only chunk content is stable across user-cache state,
  so the chunk store and the user store evolve independently.

### Why late mention resolution (full justification)

This is a meaningful divergence from the current code. Worth being
explicit about the alternatives and why this won.

**The three options:**

| Option | Where mentions resolve | User-rename behaviour |
|---|---|---|
| **(A) Render-on-write, eager** | At chunk-write time. Chunks store `@displayname`. | Find every affected chunk (`WHERE content_md LIKE '%@oldname%'` — wrong-on-collisions) or every chunk for that user (via `chunk_mentions`). Re-render each, `UPDATE chunks`. |
| **(B) Render-on-read, no chunks** | At read time. Store typed messages, not markdown. | Trivial: user table updates, next read renders fresh. |
| **(C) Two-pass, our pick** | Structural at write, mention substitution at read. Chunks store `<@U…>` placeholders. | Trivial: user table updates, next read substitutes fresh. `chunk_mentions` is only used to invalidate the kernel cache so the next read happens. |

Option A's costs are concrete and bad:

- Every user rename triggers N chunk re-renders. For Comfy-Org-shape
  workspaces (many users with frequent display-name fiddling), that's
  a real ongoing cost.
- The re-render itself depends on the renderer being stable across
  time — any renderer bugfix or formatting change means re-rendering
  all chunks that hit the buggy path, which is hard to identify.
- `@oldname` text-search to find affected chunks is fragile: it
  collides with literal text containing `@oldname` and misses
  display-name collisions. So you actually do need `chunk_mentions`
  in option A too — it doesn't save you from the side table.

Option B's costs are concrete and bad:

- Read latency is dominated by Python rendering for every read. Even
  with `notify_store` amortising, the first read of any file is
  expensive, and there are a lot of "first reads" in a tool people
  use for `rg` across the tree.
- Throws away the chunk store entirely, removing the
  "kernel-page-cache-friendly" property that makes hot reads cheap.

Option C costs:

- Read path does an extra regex pass over the assembled markdown plus
  N dict lookups (N = number of mentions in the file). For a typical
  channel.md (50–500 messages × 0–3 mentions each = 0–1500 lookups),
  this is microseconds. Cheap.
- Chunk content carries `<@U…>` instead of `@displayname`, slightly
  longer per mention (≈ 15 bytes vs ≈ 20 bytes). Negligible.
- Reads block on the local `users` table being populated. New-client
  startup must wait for the `users` stream's `caught_up` frame before
  serving FUSE reads — otherwise the first reads show raw UIDs. This
  is a startup-ordering concern, not a steady-state one.

The deciding factor is rename robustness: option A makes a real
user-cache mutation cascade into work proportional to history size.
Option C makes it free. For a workspace where display names get
fiddled with regularly, that compounds.

### Mention substitution algorithm

Slack's mention placeholders have three forms:

```
<@U02ABCD>            # user mention, raw
<@U02ABCD|display>   # user mention, with cached display
<#C0XYZ|channel>     # channel mention
<#C0XYZ>             # channel mention (rare; Slack usually fills name)
```

At **chunk-write time** (in the projector's `INSERT INTO chunks`
path):

1. Run `convert_structural(text)` — handles bold/italic/links/
   blockquotes/code, leaves mentions untouched.
2. **Normalise mention placeholders**: strip the `|cached-display`
   suffix from every `<@U…|…>` and `<#C…|…>`. The cached form is
   Slack's; we want a single canonical shape that always resolves
   against our local tables.
3. **Extract mention sets**: walk the result, pull out the set of
   `UserId`s from `<@U…>` and `ChannelId`s from `<#C…>`. Insert
   one row per mention into `chunk_mentions` with `mention_kind`
   set to `'user'` or `'channel'`.
4. `INSERT INTO chunks (...)`.

At **read time** (FUSE concat path):

1. `SELECT content_md FROM chunks WHERE … ORDER BY message_ts`,
   concat.
2. `resolve_mentions(text, users_resolver, channels_resolver)` — a
   single regex pass over the concatenated body:
   - `<@U[A-Z0-9]+>` → `@{users_resolver.resolve(uid).display_name}`
     (falls back to the UID literal if not in the local table)
   - `<#C[A-Z0-9]+>` → `#{channels_resolver.resolve(cid).name}`
3. Prepend frontmatter, append staleness trailer.

The fallback "show the UID literal" matters for the startup window
described above and for the rare case where a user is referenced
before our `users` stream has caught up to their `user_added` event.
It's a graceful degradation, not a guarantee.

### chunk_mentions lifecycle

Populated by the projector at chunk-write time. Updated by:

- **`message`** (new): INSERT chunk; parse mentions; INSERT chunk_mentions rows.
- **`message_changed`**: re-render structurally; UPDATE chunk; DELETE old chunk_mentions rows for this PK; INSERT new ones from the updated content.
- **`message_deleted`**: DELETE chunk; CASCADE deletes chunk_mentions rows via the FK.
- **`user_renamed`**: SELECT distinct `(channel_id, date, message_ts)` from `chunk_mentions WHERE mention_kind = 'user' AND mentioned_id = ?`. For each, `invalidate_inode` so the kernel page cache drops. **No chunk rewrites.** Next read substitutes the new name.
- **`channel_renamed`**: same as `user_renamed` but `mention_kind = 'channel'`.

The `chunk_mentions_lookup_idx` on `(mention_kind, mentioned_id)`
makes rename invalidation cheap: `SELECT` over a small index, not a
`LIKE` scan over content.

**Cost during backfill** (raised as still-open question #5 in the
2026-05-26 revision): each backfilled chunk does an INSERT + N
chunk_mentions INSERTs. For a 5,000-message channel where messages
average 1 mention, that's 5,000 chunk inserts + 5,000 mention
inserts. Postgres on local hardware does this in single-digit
seconds. The throttled backfill cadence (30–180 s sleep between
pages) dwarfs the per-chunk DB cost, so this is not actually a
bottleneck. Mark this open question resolved.

### `<@U…|cached-name>` normalisation timing

Decided: **server-side at slurper time**, in the event payload before
writing to `events`. The wire format and the events log both carry
the canonical `<@U…>` form. Reasoning:

- One normalisation site for all clients.
- The local-display-name layer is the only place display names need
  to exist; baking them into events would duplicate per-client state
  in shared infrastructure.
- Replay determinism: replaying events from offset 0 doesn't depend
  on the slurper's user-cache state at the time the event was
  originally written.

The slurper has the message text from `conversations.history` or
Socket Mode; it runs the normalisation transform before the
`INSERT INTO events`. Cheap.

## Three-tier visibility model

(Carried over from earlier design discussion.)

| Tier | `readdir` | `lookup` | Subscribed | Kernel-primed |
|---|---|---|---|---|
| **blocked** | skip | ENOENT | no | no |
| **hidden** | skip | works | yes | no |
| **hot** | include | works | yes | yes |

- `readdir` filters by `WHERE tier IN ('hot')` for `channels/`, `dms/`,
  `group-dms/`, `other-channels/`.
- `lookup` allows everything in `('hot', 'hidden')`. `blocked` returns
  ENOENT.
- The projector subscribes to streams `WHERE subscribed = TRUE` (in v1,
  derived from `tier != 'blocked'`).
- Kernel priming (`notify_store`) fires only on `tier = 'hot'` reads.

The hidden tier preserves discoverability via known paths (`slack-fuse
resolve <url>` output, hard-coded scripts, shell aliases) while keeping
`ls` and `rg` clean.

### Default tier assignment

When a new channel appears (`channel_added` event), the projector
assigns a default tier based on the channel's properties:

```python
def default_tier(ch: Channel) -> str:
    if ch.is_archived:
        return 'blocked'
    if ch.is_im:
        # Replaces the existing _is_dormant_dm filter in store.py.
        if has_any_chunks(ch.channel_id):
            return 'hot'
        if backfill_done(ch.channel_id):
            return 'hidden'
        return 'hot'  # newly-discovered, not yet evaluated
    if ch.is_mpim:
        return 'hot'
    if ch.is_member:
        return 'hot'
    # public, not joined
    return 'hidden'
```

**Behavior change to call out**: `other-channels/` (public channels you
haven't joined) is `hidden` by default rather than visible. Today they
show up in `ls ~/views/slack/other-channels/` as a wall of channels you
don't follow. Proposed: that directory is empty by default; specific
channels are made visible via `slack-fuse tier <slug> hot`. This is the
biggest user-visible behavioral change in v1.

### Manual override CLI

```bash
slack-fuse tier <slug-or-channel-id> <hot|hidden|blocked>
```

Writes `tier` and sets `tier_source = 'manual'` so subsequent auto
re-evaluation (when channel membership changes, archive state flips,
etc.) doesn't clobber the user's choice.

## Walk-back seams

Constraints baked into v1 so cold-lazy and other tier modes are a code
branch rather than a rewrite:

1. **One subscribe RPC.** No bespoke "give me a snapshot, I'm reading
   right now" RPC. Cold-lazy would reuse subscribe with an end-marker
   flag.

2. **Single read function** with future tier-aware branching at the
   top:

   ```python
   async def read_channel_md(channel_id, date) -> bytes:
       tier = await get_tier(channel_id)
       # FUTURE: if tier == 'cold-lazy':
       #     await projector.ensure_caught_up(f'channel:{channel_id}', ttl=300)
       return await assemble_chunks_md(channel_id, date)
   ```

3. **`tier TEXT CHECK** (not enum). Easy to add `'cold-lazy'`.

4. **`subscribed BOOLEAN` already present.** Projector reads
   `WHERE subscribed = TRUE`; nothing to change when the column is no
   longer pure-derived from tier.

5. **`accessed_at` on chunks** from day one. Eviction sweeper plugs in
   when needed.

6. **No archive task.** Chunks are the archive — `rg` works directly
   against the chunks table or the FUSE tree. The pre-render-to-disk
   loop in today's `archive.py` goes away.

## Kernel pre-push

Two mechanisms, both useful, both deferrable:

### `FUSE_NOTIFY_STORE` (v1 hot tier)

pyfuse3 exposes `notify_store(inode, offset, data)`. On the first read
of a hot file:

```python
async def read(self, fh, off, size, ctx):
    bytes_ = await assemble_chunks_md(...)
    await pyfuse3.notify_store(inode, 0, bytes_)
    return bytes_[off:off+size]
```

Subsequent reads are served from the kernel page cache without
touching userspace. On any event that mutates the file (chunk insert,
update, delete), the projector calls `notify_invalidate_inode` then
re-stores. We already do the invalidate half today via
`InodeInvalidator`; v1 adds the symmetric re-store.

Supported on kernel ≥ 3.6. Effectively universal.

### FUSE passthrough (v2)

Kernel 6.9+ (2024). Lets a FUSE inode be backed by a real file
descriptor; reads route to that fd directly, never through FUSE
userspace. For our case, the backing file is a tmpfs file containing
the rendered markdown. Mutations: rewrite the tmpfs file.

Cheaper than `notify_store` for large files where the kernel doesn't
want to keep the whole thing in the page cache. v1 ships `notify_store`
only; passthrough is a v2 graduation for hot files once the rest of the
design is stable.

## Phasing

Sequenced so each phase ships independently and the system stays
runnable throughout.

### Phase 1: Server skeleton

- New `slack_fuse_server/` package.
- Postgres schema for events, snapshots, channels, users, stream_heads.
- Slack API gateway (lift `SlackClient` from current codebase, no
  changes).
- Socket Mode driver (lift `socket_mode.py` from current codebase).
- Backfill task (lift `backfill.py`).
- WebSocket server with subscribe / snapshot / event frames.
- Single-binary deployment + systemd unit.

Acceptance: server runs against a live Slack workspace, events land in
postgres, a CLI client can subscribe and print events to stdout.

### Phase 2: Renderer-as-library

- Extract the public renderer API into `slack_fuse/renderer/__init__.py`
  with the stable surface described in the renderer section above.
- No behavior change. Existing FUSE mount keeps working.

Acceptance: `from slack_fuse.renderer import render_message` works
from both an external library and the current FUSE process.

### Phase 3: Local projector

- New `slack_fuse_projector/` package.
- Local Postgres schema (chunks, thread_chunks, channels, users,
  cursors).
- Subscribe loop: open WS to server, subscribe to all known channels,
  apply incoming events to chunks tables.
- Snapshot consumer for catch-up.
- Resume protocol: read `cursors`, send `subscribe { since: applied_offset }`.

Acceptance: projector runs alongside the current FUSE mount, builds
chunks for the same data, but FUSE handlers still read the old
in-process store. Diff the rendered output of both for verification.

### Phase 4: FUSE adapter (cutover with fallback)

**Risk callout**: this is the highest-risk phase. The in-process
store goes away and FUSE reads come from a new code path. To bound
the risk, **the old implementation stays runnable behind an env-var
gate** until Phase 5 settles:

```bash
SLACK_FUSE_MODE=legacy   # current in-process store (default during Phase 4)
SLACK_FUSE_MODE=split    # new client (default once Phase 5 ships)
```

The flag is read once at startup. Both code paths live in the repo
simultaneously. Phase 4 ships with `legacy` as the default; Phase 5
flips it to `split`; the legacy path is deleted in a cleanup commit
after a documented bake-in period (suggestion: 4 weeks of `split`
default with no rollbacks).

**Concrete rewrites in `slack_fuse/fuse_ops.py`:**

- `_list_dir_impl`: replace the in-memory channel iteration with
  `SELECT slug FROM channels WHERE tier = 'hot'` at the conv-root
  level (filters hidden + blocked from readdir).
- `_resolve_content_impl`: tier-aware path resolution.
  `SELECT * FROM channels WHERE slug = ? AND tier IN ('hot', 'hidden')`
  for channel-level lookups; ENOENT on blocked or unknown.
- `_is_dir_impl`: same tier check.
- `read`: replace the `store.render_*` calls with the chunks-table
  query path described in *FUSE read path*.
- `open`: `notify_store` on first read for `tier = 'hot'` files;
  no-op for hidden.
- `lookup`: persistent inode lookup via the `inodes` table.

**Renderer + projector coordination**: v1 runs the projector and the
FUSE mount in the same Python process. Chunk mutations and the
subsequent `invalidate_inode` + `notify_store` calls happen in the
same code path against shared inode state. (Splitting them later
becomes a PG `LISTEN/NOTIFY` problem; documented as a v2 option in
*Walk-back seams*.)

- The `resolve` and `permalink` CLIs proxy through the server's HTTP
  endpoints (token stays server-side).

Acceptance: FUSE reads come exclusively from local chunks tables when
`SLACK_FUSE_MODE=split`. Legacy mode continues to work. Old
`store.py`, `backfill.py`, `archive.py`, `socket_mode.py` are NOT
deleted yet — they're deleted in the post-Phase-5 cleanup.

### Phase 5: Tier system + CLI

- Add `tier` and `tier_source` to local `channels` schema (Phase 3
  already includes them).
- Default tier assignment logic.
- `slack-fuse tier <slug> <hot|hidden|blocked>` CLI command.
- Verify `readdir` skip + `lookup` permissive behavior end-to-end.

Acceptance: hidden channels don't appear in `ls` but `cat` by path
works. `rg ~/views/slack/` doesn't descend into hidden channels.

### Phase 6: FUSE passthrough (v2)

- Detect kernel ≥ 6.9 at mount time.
- For hot tier, back inodes with tmpfs files.
- Fall back to `notify_store` on older kernels.

### Phase 7: Auto tier transitions (v2)

- Access-count tracking on `chunks.accessed_at`.
- Periodic sweeper that promotes/demotes channels based on
  configurable thresholds.
- Respects `tier_source = 'manual'`.

## Testing

Each phase ships with tests. Both unit and integration. Tests are part
of the phase's acceptance criteria, not a follow-up.

### Per-phase test surface

**Phase 1 (Server skeleton)**
- Unit: event-frame serialization round-trips. Offset-assignment
  transaction correctness under concurrent writers to the same stream
  (use pg_temp schemas; spawn 4 trio tasks; assert no offset gaps or
  duplicates).
- Integration: stand up Postgres + the slurper against a **fake
  Slack API** (httpx mock transport — same pattern as current
  `tests/test_backfill.py` uses for the Slack client). Drive a
  full backfill + a Socket Mode event stream; assert events land in
  postgres with expected kinds, ordering, payload shapes.
- Integration: WebSocket protocol. Open a connection, subscribe to
  each stream kind, assert the `caught_up` boundary, error frames
  for bogus subscribes, paginated-snapshot assembly.
- HTTP: `/metrics`, `/health`, `/resolve`, `/permalink` smoke tests.

**Phase 2 (Renderer-as-library)**
- Unit: `convert_structural` round-trip across the existing mrkdwn
  corpus. `resolve_mentions` with a stubbed `UserResolver` /
  `ChannelResolver`. `extract_mention_user_ids` for placeholder
  extraction. Normalisation of `<@U…|cached>` → `<@U…>`.
- Property-style: structural pass is idempotent (running it twice
  gives the same output).
- Backward-compat: golden-file tests against rendered output from the
  current renderer for a representative set of message types
  (bold/italic/links/code/files/reactions/threads). The new
  structural+resolve pipeline must produce byte-identical output for
  the same inputs given a populated resolver.

**Phase 3 (Local projector)**
- Unit: each event-kind → chunk-operation mapping. Use a synthetic
  event stream; assert chunks-table state after applying.
- Integration: full client against the Phase-1 server. Start, catch
  up from offset 0, observe live events, assert local chunks match
  expected. Verify `chunk_mentions` index correctness for various
  message shapes.
- Stress: replay a 50,000-event synthetic stream and assert
  per-event-application latency stays bounded. Catches O(N²)
  regressions in chunk handlers.

**Phase 4 (FUSE adapter)**
- Unit: tier-aware `_list_dir_impl`, `_resolve_content_impl`,
  `_is_dir_impl` against synthetic channel rows of every tier.
- Unit: `inodes` table persistence; allocate, restart, re-lookup
  same path returns same inode.
- Integration: end-to-end mount against a running Phase-3 client.
  `ls`, `cat`, `find`, `rg` over a synthetic workspace; assert
  expected outputs. **Crucial:** assert that `readdir` skips hidden
  channels but `lookup`-by-known-name still finds them (regression
  guard for the dotfile-pattern correction).
- Integration: `notify_store` correctness. Read once, observe kernel
  cache hit; project a mutation event; observe kernel cache
  invalidation; next read returns updated bytes.
- End-to-end: both modes (`SLACK_FUSE_MODE=legacy` and `=split`)
  pass the same FUSE-surface integration tests. Same assertions,
  different backend.
- Offline-trailer: disconnect from server mid-test; assert trailer
  appears with correct reason and timestamp. Reconnect; assert
  trailer disappears.

**Phase 5 (Tier system + CLI)**
- Unit: default-tier assignment for every Slack channel-shape
  combination (joined, archived, dormant DM, public-not-joined,
  MPIM, etc.).
- Integration: `slack-fuse tier <slug> <hot|hidden|blocked>` CLI;
  assert `channels` table update and visibility change in `readdir`.
- Regression: `tier_source = 'manual'` survives auto-evaluation
  triggered by channel-state events.

**Phase 6 (FUSE passthrough)**
- Conditional: skip if kernel < 6.9. On a supported kernel, verify
  that read syscalls on hot files bypass the FUSE userspace handler
  (count handler invocations; assert zero after first prime).
- Fallback: assert that on an unsupported kernel the implementation
  falls back to `notify_store` automatically.

**Phase 7 (Auto tier transitions)**
- Unit: promotion/demotion thresholds; respect for `tier_source =
  'manual'`.
- Integration: simulate access patterns over a synthetic workspace;
  assert tier transitions land where expected.

### Cross-cutting test infrastructure

- **Fake Slack API.** A pytest fixture that stands up an `httpx`
  mock transport with deterministic responses for
  `conversations.list`, `conversations.history`,
  `conversations.replies`, `users.list`, `users.info`,
  `chat.getPermalink`. Same fixture is used by Phase 1, 4, and any
  test that needs to drive Slack-shaped data.
- **Postgres test fixtures.** Per-test schemas via `pg_temp`. No
  shared state between tests. Reuses a single running postgres for
  the test session (CI: postgres-in-docker; local: assume a running
  instance).
- **Synthetic event-stream generator.** Helper that produces
  `(stream, offset, kind, payload)` tuples matching the wire
  format. Used to drive projector tests without spinning up the
  full slurper.
- **In-memory FUSE harness.** pyfuse3 has a low-level test helper
  that lets us invoke handlers directly without mounting a real
  filesystem. Used for unit tests of `fuse_ops.py` rewrites.

### CI shape

GitHub Actions (matches the existing slack-fuse repo): one job per
phase package on every push to a non-`main` branch; full suite on
`main`. Postgres provided by `services: postgres:` in the workflow.
No code merges to `main` without all phases' suites green.

## Deferred / not in scope

| Item | Reason |
|---|---|
| Cold-lazy tier | v1 ships hot+hidden; cold can be added as a tier value + read-path branch later without rewriting (walk-back seams cover it) |
| Auto tier transitions | needs usage data; let users live with defaults + manual overrides first |
| Multi-tenant deployment | single-user posture is simpler and serves the homelab case; protocol is tenant-id-agnostic but not tenant-id-bearing in v1 |
| Server-side rendering | the renderer library is importable on the server but v1 only runs it on clients |
| `feed.md` views | rarely-read; awkward to fit into the in-place chunk model |
| TLS / WSS | This is a single-person tool. Encryption is a deployment-layer concern — run the server behind Caddy/Tailscale/nebula for off-LAN access and let those terminate TLS. Application doesn't speak TLS itself. |
| Search backend | search is "rg + the local FUSE tree" — no separate index in v1 |
| Huddle index | needs its own stream type; not part of v1 critical path |
| Canvas / transcript ingestion | server fetches them on demand; lift logic from current `canvas.py` / `transcript.py` unchanged |
| Last-N-days-only retention per channel | v2/v3: per-channel TTL on chunks so blocked-noisy channels stop after N days; relates to chunk-size telemetry from #4 |
| Wire-level write commands (`send_message`, `add_reaction`, `mark_read`) | v1 is read-only over the wire; writes are a separate command-RPC layer added when needed |
| LISTEN/NOTIFY between projector and FUSE | v1 keeps them in one process; the door to split is via PG pub/sub |
| Delta snapshots | v1 ships full-state snapshots; if any single stream exceeds 50k events per snapshot frame, revisit |
| Read-state stream | needed if `mark_read` ever lands; v1 doesn't track read state |
| Backpressure / flow control on subscription | event rates are low enough that TCP-level backpressure suffices; revisit if any stream produces >1k events/s sustained |
| Multi-user / GDPR / data-deletion pipeline | This is a single-person tool. Not in scope, not a thing. |

## Open questions

The following questions were closed by the 2026-05-26 revision and
are kept here as a record:

| Original question | Resolution |
|---|---|
| Inode stability across restarts | Persistent `inodes(path PK, inode UNIQUE GENERATED)` table on the client. Never recycled |
| `~/views/slack/.cached-only/` | Removed in v1. The projector is always offline-capable; the prefix has no remaining meaning |
| Migrating existing disk caches | Blow away on first server bootup. Backfill rebuilds. Backfill may take a full day; that's acceptable. Migration script not built |
| Subscribe response semantics | Server streams events from `since`; emits `caught_up { head_offset }` at the catch-up boundary. `error { stream_not_found \| since_too_high \| auth_failed }` on protocol problems |
| Resume staleness on the client | Server retains snapshots for at least 30 days. Older clients receive a fresh snapshot at the current head; their `applied_offset` is reset |
| Backfill ordering vs live events | Non-issue: backfill writes current-state `message` events, live writes delta events. Same-message ordering guaranteed by Socket Mode within a stream; backfill never overlaps live on the same message |
| Detecting noisy channels for blocking | Backfill aborts at 20k messages by default (configurable) and emits `backfill_aborted` on `slurper-health`; chunk-size telemetry surfaces ongoing growth (v2) |
| What "caught up" means in steady state (silent-lag) | **Dissolved by Flow Control design.** Per-event sync apply + TCP backpressure means the projector can't silently fall behind. Either it applies at sustainable rate (server is naturally throttled), or the connection drops (trailer fires for the actual reason). No third state. `caught_up` frame stays as informational marker for the initial-catch-up condition in the trailer logic |
| `chunk_mentions` population during backfill | Throttled backfill cadence (30–180 s sleep between pages) dwarfs the per-chunk DB cost. Inline `chunk_mentions` write is not a bottleneck |

Still open:

1. **Snapshot cadence trade-offs.** "Every 5000 events or daily" is a
   starting point. Whether `users` and `channel-list` need their own
   cadences (these streams are slow-moving; daily is enough) vs busy
   `channel:<id>` streams (which might want more frequent snapshots to
   limit catch-up replay) isn't measured. Land the defaults; tune
   after observing real cadence. **First-party instrumentation**
   (`snapshots.payload_bytes`, `snapshots.events_covered`,
   `snapshots.generation_duration_ms`, `snapshot_uses` table) ships
   in v1 so the tuning decision is data-driven when we get there.

2. **Connection-state granularity.** The trailer is a single
   per-process state ("server reachable, slurper healthy, all
   streams caught up"). If a single specific stream is far behind but
   everything else is current, do we mark only that stream's files
   stale? Per-stream granularity is more accurate but requires
   `connection_state` per stream; v1 uses one row. **Reversible
   punt**: the `trailer_decisions` log table ships in v1 capturing
   per-decision context, so the false-positive rate is measurable.
   Revisit when the data shows the punt was wrong.

3. **Backfill abort threshold default.** 20,000 messages is a guess.
   It may be too low for some legitimate channels (#engineering with
   years of activity). Pick after profiling a real workspace; the
   override mechanism handles outliers.

## Alternatives considered

### Keep single-process, add durable event log locally

Just write events to a local sqlite/postgres in the same process.
Doesn't solve cross-device coherence or rate-limit dedup. Punts bug 3
partially (we'd have the log but Socket Mode gaps still create
phantom-state windows). Less work but doesn't pay back the architectural
debt.

### Use NATS JetStream or Kafka as the event store

Both are great event-streaming substrates. Both add operational
overhead the single-user homelab case doesn't justify. Postgres `events`
+ `LISTEN/NOTIFY` for push gives us most of the win at a fraction of
the operational cost, and we already run Postgres for other FUSE
projects in this environment.

### Use SQLite locally instead of Postgres

Tempting for the "no second daemon to install" angle. Rejected
because:

- Forces the DB file to live on local disk; no homelab Postgres
  redirection.
- CoW filesystem interaction (btrfs/zfs) is bad for sqlite
  performance.
- Asymmetric operational story with the server's Postgres (different
  backup procedures, different connection libs, different schema
  migration tools).
- Container deployment is awkward (the DB file has to be a volume
  mount).

Postgres on both sides gives one operational story.

### Server-side rendering only

Server pre-renders chunks and ships rendered markdown to clients.
Removes the renderer from the client. Rejected for v1 because:

- User cache is per-client (different machines may have different
  display-name snapshots).
- Renderer upgrades become a server deployment instead of a client
  package bump.
- Bandwidth per event grows (full markdown instead of typed event).

The renderer-as-library approach keeps the door open to revisit this
later without a redesign.

## Success criteria

This RFC succeeds if v1 ships and:

1. Bug 3 is gone: thread.md reflects new replies within seconds
   regardless of how old the thread is, without manual refresh.
2. Two machines mounting slack-fuse against the same server make zero
   redundant Slack API calls during steady-state operation.
3. FUSE read latency for hot paths is dominated by kernel page-cache
   lookup (microseconds), not userspace rendering (milliseconds).
4. A 1-hour disconnect from the server reconverges to identical state
   on reconnect, with no manual intervention.
5. `rg` over the visible FUSE tree completes in seconds for typical
   queries, because chunks are pre-rendered.
6. The existing dormant-DM filter (`_is_dormant_dm`) and accumulated
   empty-cache-files behavior are subsumed by the `hidden` tier.
