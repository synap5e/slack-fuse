-- slack-fuse-server schema (event store).
--
-- Authoritative reference copy of RFC §Schemas → Server: events store.
-- The migration runner applies migrations/0001_init.sql (identical content);
-- this file exists so the schema is reviewable as a single document.

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

-- Backfill dedup: same Slack ts = same message. Keyed by (stream, ts) and
-- scoped to message events only, so re-running either backfiller is a no-op
-- while non-message event kinds may legitimately repeat. (RFC §Backfill →
-- Both writes are idempotent.)
CREATE UNIQUE INDEX events_message_dedup
    ON events (stream, kind, (payload->>'ts'))
    WHERE kind = 'message';

-- Users-stream dedup: one `user_added` per workspace user. Sprint 1E
-- emits these at slurper startup; the partial unique index makes the
-- "first writer wins" invariant a hard constraint instead of a runtime
-- SELECT-then-INSERT check that any future writer could bypass.
CREATE UNIQUE INDEX events_users_added_dedup
    ON events (stream, kind, (payload ->> 'id'))
    WHERE stream = 'users' AND kind = 'user_added';

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

-- Workspace inventory: a VIEW (not a table) derived from `channel_added`
-- events on the channel-list stream. Operators can SELECT it like a table.
-- See migrations/0004_channels_view.sql for the definition + rationale.
-- (Previously a table with no writer; now ES-clean — one source of truth.)
CREATE VIEW channels AS
SELECT DISTINCT ON (payload->>'id')
    payload->>'id'                       AS channel_id,
    payload->>'name'                     AS name,
    (payload->>'is_im')::boolean         AS is_im,
    (payload->>'is_mpim')::boolean       AS is_mpim,
    (payload->>'is_member')::boolean     AS is_member,
    (payload->>'is_archived')::boolean   AS is_archived,
    payload->>'im_user_id'               AS im_user_id,
    payload->>'topic'                    AS topic,
    payload->>'purpose'                  AS purpose,
    (payload->>'num_members')::int       AS num_members
FROM events
WHERE stream = 'channel-list'
  AND kind = 'channel_added'
  AND payload ? 'id'
ORDER BY payload->>'id', id DESC;

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

-- Append-only log of slurper health transitions: a VIEW over the
-- slurper-health stream in `events`. Operator-convenience SELECT shape,
-- no dual-write — the event log is the only writer.
-- See migrations/0005_health_log_view.sql.
CREATE VIEW health_log AS
SELECT id, kind, payload, created_at
FROM events
WHERE stream = 'slurper-health';

-- Per-channel backfill size overrides. Persists so re-runs honour the
-- operator's --allow-large / --max-messages decision. (RFC §Backfill →
-- Per-channel size threshold.)
CREATE TABLE backfill_overrides (
    channel_id TEXT PRIMARY KEY,
    max_messages BIGINT,  -- NULL = no limit
    set_by TEXT NOT NULL DEFAULT 'admin',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Mutable operator policy. These rows are NOT Slack facts and do not belong in
-- `events`; they are the current operator-maintained block list used by
-- refresh/backfill/clients.
CREATE TABLE blocked_channels (
    channel_id TEXT PRIMARY KEY,
    blocked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    reason TEXT
);

-- Offset-assignment pattern (RFC §Schemas → Offset assignment pattern).
-- Concurrent writers to the same stream serialize via the stream_heads row
-- lock; writers to different streams are independent. Canonical write TX:
--
--   BEGIN;
--   INSERT INTO stream_heads (stream) VALUES ($1)
--     ON CONFLICT (stream) DO NOTHING;
--   UPDATE stream_heads
--      SET next_offset = next_offset + 1
--    WHERE stream = $1
--   RETURNING next_offset - 1 AS my_offset;
--   INSERT INTO events (stream, offset_in_stream, kind, ts, payload)
--   VALUES ($1, $my_offset, $2, $3, $4);
--   COMMIT;
--
-- The UPDATE ... RETURNING row-locks the stream's stream_heads row for the
-- duration of the transaction. The pattern survives parallelisation (one
-- task per channel during backfill) without code changes.
