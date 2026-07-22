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
    source JSONB,                     -- ambient ingestion envelope (0009; NULL pre-migration)
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (stream, offset_in_stream)
);
CREATE INDEX events_stream_offset_idx ON events (stream, offset_in_stream);

-- Source-envelope indexes (0009). Slack facts live in `payload`; `source`
-- carries ambient facts about the ingestion transaction (producer, boot/task/
-- run ids, Slack cursors, commit, span id — see slurper/ingestion.py). The
-- backfill partial indexes drive restart-resume; the expression indexes drive
-- forensic correlation with deploys / boots / Loki spans.
CREATE INDEX events_source_backfill_history_idx
    ON events (stream, offset_in_stream DESC)
    WHERE source->>'producer' = 'backfill-history-page';
CREATE INDEX events_source_backfill_replies_idx
    ON events (stream, (source->>'thread_ts'), offset_in_stream DESC)
    WHERE source->>'producer' = 'backfill-replies-page';
CREATE INDEX events_source_commit_idx ON events ((source->>'commit')) WHERE source IS NOT NULL;
CREATE INDEX events_source_boot_idx ON events ((source->>'boot_id')) WHERE source IS NOT NULL;
CREATE INDEX events_source_span_idx ON events ((source->>'span_id')) WHERE source IS NOT NULL;

-- Universal Slack Events API delivery dedup (0012). Every row emitted inside
-- one dispatcher call inherits the event id via the ingestion ContextVar.
CREATE UNIQUE INDEX events_slack_event_id_dedup
    ON events (stream, kind, (source->>'slack_event_id'))
    WHERE source ? 'slack_event_id';

-- Durable HTTPS Events API inbox (0012). Callback envelopes are committed
-- here before Slack receives a 200 acknowledgement.
CREATE TABLE slack_event_inbox (
    event_id         TEXT        PRIMARY KEY,
    envelope         JSONB       NOT NULL,
    received_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at     TIMESTAMPTZ,
    attempt_count    INTEGER     NOT NULL DEFAULT 0,
    last_attempt_at  TIMESTAMPTZ,
    next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    dispatch_error   TEXT,
    dead_lettered_at TIMESTAMPTZ
);
CREATE INDEX slack_event_inbox_pending
    ON slack_event_inbox (next_attempt_at, received_at)
    WHERE processed_at IS NULL AND dead_lettered_at IS NULL;

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

-- Socket-mode replay dedup for additive event kinds introduced after initial
-- backfill. Each index is scoped to one logical event family so existing edit /
-- delete and non-message append semantics remain unchanged.
CREATE UNIQUE INDEX events_parent_replied_dedup
    ON events (stream, kind, (payload ->> 'parent_ts'), (payload ->> 'reply_count'))
    WHERE kind = 'parent_replied';
CREATE UNIQUE INDEX events_channel_id_changed_dedup
    ON events (stream, kind, (payload ->> 'old_channel_id'), (payload ->> 'new_channel_id'), (payload ->> 'event_ts'))
    WHERE stream = 'channel-list' AND kind = 'channel_id_changed';
CREATE UNIQUE INDEX events_channel_history_changed_dedup
    ON events (stream, kind, (payload ->> 'channel_id'), (payload ->> 'latest'), (payload ->> 'ts'), (payload ->> 'event_ts'))
    WHERE stream = 'channel-list' AND kind = 'channel_history_changed';
CREATE UNIQUE INDEX events_channel_member_user_dedup
    ON events (stream, kind, (payload ->> 'channel_id'), (payload ->> 'user_id'), (payload ->> 'event_ts'))
    WHERE stream = 'channel-list' AND kind IN ('channel_member_joined', 'channel_member_left');
CREATE UNIQUE INDEX events_tokens_revoked_dedup
    ON events (stream, kind, (payload -> 'tokens'))
    WHERE stream = 'slurper-health' AND kind = 'tokens_revoked';

-- Slack-shaped timestamp predicate used by normalized active-message views.
CREATE OR REPLACE FUNCTION is_valid_slack_ts(ts text) RETURNS bool
  LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT
  AS $$ SELECT ts ~ '^[1-9][0-9]{9}\.[0-9]{6}$' $$;

CREATE INDEX events_message_changed_target_idx
    ON events (stream, (payload->'message'->>'ts'), offset_in_stream DESC)
    WHERE kind = 'message_changed';
CREATE INDEX events_message_deleted_target_idx
    ON events (stream, (payload->>'deleted_ts'))
    WHERE kind = 'message_deleted';
CREATE INDEX events_parent_replied_target_idx
    ON events (stream, (payload->>'parent_ts'), offset_in_stream DESC)
    WHERE kind = 'parent_replied';

-- Backfill lifecycle stream (0011). Lifecycle/progress facts live on
-- backfill-run:<channel_id>, not in the source envelope of Slack fact rows.
CREATE UNIQUE INDEX events_backfill_run_terminal_dedup
    ON events (stream, kind, (payload->>'run_id'))
    WHERE stream LIKE 'backfill-run:%'
      AND kind IN ('backfill_run_started', 'backfill_run_finished');
CREATE UNIQUE INDEX events_backfill_page_committed_dedup
    ON events (stream, kind, (payload->>'run_id'), (payload->>'page_index'))
    WHERE stream LIKE 'backfill-run:%'
      AND kind = 'backfill_page_committed';
CREATE INDEX events_backfill_run_stream_idx
    ON events (stream, id DESC)
    WHERE stream LIKE 'backfill-run:%';
CREATE INDEX events_backfill_run_id_idx
    ON events (stream, (payload->>'run_id'), kind, id DESC)
    WHERE stream LIKE 'backfill-run:%'
      AND kind IN ('backfill_run_started', 'backfill_page_committed', 'backfill_run_finished');

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

-- Workspace inventory: a VIEW (not a table) derived from channel-list events.
-- Full-payload events and narrow drift events are combined per column so a
-- rename or membership update cannot erase unrelated metadata.
--
-- Fold invariant (column -> event kinds):
--   channel_id  -> channel_added, channel_info_refreshed, channel_renamed,
--                  channel_archived, channel_unarchived, channel_member_changed
--   name        -> channel_added, channel_info_refreshed, channel_renamed
--   is_member   -> channel_added, channel_info_refreshed, channel_member_changed
--   is_archived -> channel_added, channel_info_refreshed, channel_archived,
--                  channel_unarchived
--   is_im, is_mpim, im_user_id, topic, purpose, num_members
--               -> channel_added, channel_info_refreshed
-- channel_member_joined / channel_member_left are deliberately ignored: they
-- describe one user and cannot determine aggregate/self membership here.
--
-- If a new channel-list event kind mutates one of these columns, this view
-- MUST be updated and this comment amended.
CREATE OR REPLACE VIEW channels AS
WITH
  channel_added AS (
    SELECT DISTINCT ON (payload->>'id')
      payload->>'id' AS channel_id,
      payload,
      offset_in_stream
    FROM events
    WHERE stream = 'channel-list'
      AND kind = 'channel_added'
      AND payload->>'id' IS NOT NULL
    ORDER BY payload->>'id', offset_in_stream DESC
  ),
  channel_info_refreshed AS (
    SELECT DISTINCT ON (payload->>'id')
      payload->>'id' AS channel_id,
      payload,
      offset_in_stream
    FROM events
    WHERE stream = 'channel-list'
      AND kind = 'channel_info_refreshed'
      AND payload->>'id' IS NOT NULL
    ORDER BY payload->>'id', offset_in_stream DESC
  ),
  channel_renamed AS (
    SELECT DISTINCT ON (payload->>'channel_id')
      payload->>'channel_id' AS channel_id,
      payload,
      offset_in_stream
    FROM events
    WHERE stream = 'channel-list'
      AND kind = 'channel_renamed'
      AND payload->>'channel_id' IS NOT NULL
      AND payload->>'new_name' IS NOT NULL
    ORDER BY payload->>'channel_id', offset_in_stream DESC
  ),
  channel_archived AS (
    SELECT DISTINCT ON (payload->>'channel_id')
      payload->>'channel_id' AS channel_id,
      offset_in_stream
    FROM events
    WHERE stream = 'channel-list'
      AND kind = 'channel_archived'
      AND payload->>'channel_id' IS NOT NULL
    ORDER BY payload->>'channel_id', offset_in_stream DESC
  ),
  channel_unarchived AS (
    SELECT DISTINCT ON (payload->>'channel_id')
      payload->>'channel_id' AS channel_id,
      offset_in_stream
    FROM events
    WHERE stream = 'channel-list'
      AND kind = 'channel_unarchived'
      AND payload->>'channel_id' IS NOT NULL
    ORDER BY payload->>'channel_id', offset_in_stream DESC
  ),
  channel_member_changed AS (
    SELECT DISTINCT ON (payload->>'channel_id')
      payload->>'channel_id' AS channel_id,
      payload,
      offset_in_stream
    FROM events
    WHERE stream = 'channel-list'
      AND kind = 'channel_member_changed'
      AND payload->>'channel_id' IS NOT NULL
      AND payload->>'is_member' IS NOT NULL
    ORDER BY payload->>'channel_id', offset_in_stream DESC
  ),
  channel_ids AS (
    SELECT channel_id FROM channel_added
    UNION
    SELECT channel_id FROM channel_info_refreshed
    UNION
    SELECT channel_id FROM channel_renamed
    UNION
    SELECT channel_id FROM channel_archived
    UNION
    SELECT channel_id FROM channel_unarchived
    UNION
    SELECT channel_id FROM channel_member_changed
  ),
  sources AS (
    SELECT
      ids.channel_id,
      added.payload AS added_payload,
      added.offset_in_stream AS added_offset,
      refreshed.payload AS refreshed_payload,
      refreshed.offset_in_stream AS refreshed_offset,
      renamed.payload AS renamed_payload,
      renamed.offset_in_stream AS renamed_offset,
      archived.offset_in_stream AS archived_offset,
      unarchived.offset_in_stream AS unarchived_offset,
      member_changed.payload AS member_changed_payload,
      member_changed.offset_in_stream AS member_changed_offset
    FROM channel_ids ids
    LEFT JOIN channel_added added USING (channel_id)
    LEFT JOIN channel_info_refreshed refreshed USING (channel_id)
    LEFT JOIN channel_renamed renamed USING (channel_id)
    LEFT JOIN channel_archived archived USING (channel_id)
    LEFT JOIN channel_unarchived unarchived USING (channel_id)
    LEFT JOIN channel_member_changed member_changed USING (channel_id)
  ),
  latest_offsets AS (
    SELECT
      sources.*,
      GREATEST(
        CASE WHEN added_payload->>'name' IS NOT NULL THEN added_offset END,
        CASE WHEN refreshed_payload->>'name' IS NOT NULL THEN refreshed_offset END,
        renamed_offset
      ) AS name_offset,
      GREATEST(
        CASE WHEN added_payload->>'is_member' IS NOT NULL THEN added_offset END,
        CASE WHEN refreshed_payload->>'is_member' IS NOT NULL THEN refreshed_offset END,
        member_changed_offset
      ) AS is_member_offset,
      GREATEST(
        CASE WHEN added_payload->>'is_archived' IS NOT NULL THEN added_offset END,
        CASE WHEN refreshed_payload->>'is_archived' IS NOT NULL THEN refreshed_offset END,
        archived_offset,
        unarchived_offset
      ) AS is_archived_offset,
      GREATEST(
        CASE WHEN added_payload->>'is_im' IS NOT NULL THEN added_offset END,
        CASE WHEN refreshed_payload->>'is_im' IS NOT NULL THEN refreshed_offset END
      ) AS is_im_offset,
      GREATEST(
        CASE WHEN added_payload->>'is_mpim' IS NOT NULL THEN added_offset END,
        CASE WHEN refreshed_payload->>'is_mpim' IS NOT NULL THEN refreshed_offset END
      ) AS is_mpim_offset,
      GREATEST(
        CASE WHEN added_payload->>'im_user_id' IS NOT NULL THEN added_offset END,
        CASE WHEN refreshed_payload->>'im_user_id' IS NOT NULL THEN refreshed_offset END
      ) AS im_user_id_offset,
      GREATEST(
        CASE WHEN added_payload->>'topic' IS NOT NULL THEN added_offset END,
        CASE WHEN refreshed_payload->>'topic' IS NOT NULL THEN refreshed_offset END
      ) AS topic_offset,
      GREATEST(
        CASE WHEN added_payload->>'purpose' IS NOT NULL THEN added_offset END,
        CASE WHEN refreshed_payload->>'purpose' IS NOT NULL THEN refreshed_offset END
      ) AS purpose_offset,
      GREATEST(
        CASE WHEN added_payload->>'num_members' IS NOT NULL THEN added_offset END,
        CASE WHEN refreshed_payload->>'num_members' IS NOT NULL THEN refreshed_offset END
      ) AS num_members_offset
    FROM sources
  )
SELECT
  channel_id,
  CASE
    WHEN renamed_offset = name_offset THEN renamed_payload->>'new_name'
    WHEN refreshed_offset = name_offset THEN refreshed_payload->>'name'
    WHEN added_offset = name_offset THEN added_payload->>'name'
  END AS name,
  CASE
    WHEN refreshed_offset = is_im_offset THEN (refreshed_payload->>'is_im')::boolean
    WHEN added_offset = is_im_offset THEN (added_payload->>'is_im')::boolean
  END AS is_im,
  CASE
    WHEN refreshed_offset = is_mpim_offset THEN (refreshed_payload->>'is_mpim')::boolean
    WHEN added_offset = is_mpim_offset THEN (added_payload->>'is_mpim')::boolean
  END AS is_mpim,
  CASE
    WHEN member_changed_offset = is_member_offset THEN (member_changed_payload->>'is_member')::boolean
    WHEN refreshed_offset = is_member_offset THEN (refreshed_payload->>'is_member')::boolean
    WHEN added_offset = is_member_offset THEN (added_payload->>'is_member')::boolean
  END AS is_member,
  CASE
    WHEN archived_offset = is_archived_offset THEN true
    WHEN unarchived_offset = is_archived_offset THEN false
    WHEN refreshed_offset = is_archived_offset THEN (refreshed_payload->>'is_archived')::boolean
    WHEN added_offset = is_archived_offset THEN (added_payload->>'is_archived')::boolean
  END AS is_archived,
  CASE
    WHEN refreshed_offset = im_user_id_offset THEN refreshed_payload->>'im_user_id'
    WHEN added_offset = im_user_id_offset THEN added_payload->>'im_user_id'
  END AS im_user_id,
  CASE
    WHEN refreshed_offset = topic_offset THEN refreshed_payload->>'topic'
    WHEN added_offset = topic_offset THEN added_payload->>'topic'
  END AS topic,
  CASE
    WHEN refreshed_offset = purpose_offset THEN refreshed_payload->>'purpose'
    WHEN added_offset = purpose_offset THEN added_payload->>'purpose'
  END AS purpose,
  CASE
    WHEN refreshed_offset = num_members_offset THEN (refreshed_payload->>'num_members')::int
    WHEN added_offset = num_members_offset THEN (added_payload->>'num_members')::int
  END AS num_members
FROM latest_offsets;

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

-- Current active message bodies derived from channel event streams.
CREATE VIEW active_messages AS
WITH
  base_events AS (
    SELECT stream, substr(stream, 9) AS channel_id, payload->>'ts' AS effective_ts, payload, offset_in_stream
    FROM events
    WHERE stream LIKE 'channel:%' AND kind = 'message' AND is_valid_slack_ts(payload->>'ts')
  ),
  change_events AS (
    SELECT
      stream,
      substr(stream, 9) AS channel_id,
      payload->'message'->>'ts' AS effective_ts,
      payload->>'previous_ts' AS previous_ts,
      payload->'message' AS payload,
      offset_in_stream
    FROM events
    WHERE stream LIKE 'channel:%' AND kind = 'message_changed' AND is_valid_slack_ts(payload->'message'->>'ts')
  ),
  supplanted_by_change AS (
    SELECT stream, previous_ts AS effective_ts
    FROM change_events
    WHERE is_valid_slack_ts(previous_ts) AND previous_ts <> effective_ts
  ),
  tombstones AS (
    SELECT stream, payload->>'deleted_ts' AS effective_ts
    FROM events
    WHERE stream LIKE 'channel:%' AND kind = 'message_deleted' AND is_valid_slack_ts(payload->>'deleted_ts')
  ),
  latest_per_ts AS (
    SELECT DISTINCT ON (stream, effective_ts)
      stream, channel_id, effective_ts, payload, offset_in_stream
    FROM (
      SELECT stream, channel_id, effective_ts, payload, offset_in_stream, 1 AS priority FROM change_events
      UNION ALL
      SELECT stream, channel_id, effective_ts, payload, offset_in_stream, 0 AS priority FROM base_events
    ) chained
    ORDER BY stream, effective_ts, offset_in_stream DESC, priority DESC
  )
SELECT
  l.stream,
  l.channel_id,
  l.effective_ts AS ts,
  (l.effective_ts)::numeric AS ts_numeric,
  l.payload AS active_payload,
  l.payload->>'thread_ts' AS thread_ts,
  l.offset_in_stream
FROM latest_per_ts l
WHERE NOT EXISTS (
  SELECT 1 FROM supplanted_by_change s WHERE s.stream = l.stream AND s.effective_ts = l.effective_ts
)
AND NOT EXISTS (
  SELECT 1 FROM tombstones t WHERE t.stream = l.stream AND t.effective_ts = l.effective_ts
);

-- Current active thread-parent metadata. Requires a base active parent body.
CREATE VIEW active_thread_parents AS
WITH
  base_parents AS (
    SELECT
      stream,
      channel_id,
      ts AS parent_ts,
      (active_payload->>'reply_count')::int AS reply_count,
      active_payload->>'latest_reply' AS latest_reply,
      offset_in_stream
    FROM active_messages
    WHERE (active_payload->>'reply_count')::int > 0
  ),
  parent_updates AS (
    SELECT
      stream,
      substr(stream, 9) AS channel_id,
      payload->>'parent_ts' AS parent_ts,
      (payload->>'reply_count')::int AS reply_count,
      payload->>'latest_reply' AS latest_reply,
      offset_in_stream
    FROM events
    WHERE stream LIKE 'channel:%' AND kind = 'parent_replied' AND is_valid_slack_ts(payload->>'parent_ts')
  ),
  candidates AS (
    SELECT stream, channel_id, parent_ts, reply_count, latest_reply, offset_in_stream FROM base_parents
    UNION ALL
    SELECT pu.stream, pu.channel_id, pu.parent_ts, pu.reply_count, pu.latest_reply, pu.offset_in_stream
    FROM parent_updates pu
    WHERE EXISTS (
      SELECT 1 FROM base_parents bp WHERE bp.stream = pu.stream AND bp.parent_ts = pu.parent_ts
    )
  )
SELECT DISTINCT ON (stream, parent_ts)
  stream,
  channel_id,
  parent_ts,
  reply_count,
  latest_reply,
  offset_in_stream AS effective_offset
FROM candidates
ORDER BY stream, parent_ts, offset_in_stream DESC;

CREATE VIEW channel_backfill_state AS
WITH started AS (
    SELECT
        stream,
        substr(stream, length('backfill-run:') + 1) AS channel_id,
        payload->>'run_id' AS run_id,
        created_at,
        id
    FROM events
    WHERE stream LIKE 'backfill-run:%'
      AND kind = 'backfill_run_started'
      AND payload ? 'run_id'
),
latest_started AS (
    SELECT DISTINCT ON (stream)
        stream,
        channel_id,
        run_id,
        created_at,
        id
    FROM started
    ORDER BY stream, id DESC
),
finished AS (
    SELECT DISTINCT ON (stream, payload->>'run_id')
        stream,
        payload->>'run_id' AS run_id,
        payload->>'outcome' AS outcome,
        created_at,
        id
    FROM events
    WHERE stream LIKE 'backfill-run:%'
      AND kind = 'backfill_run_finished'
      AND payload ? 'run_id'
    ORDER BY stream, payload->>'run_id', id DESC
),
latest_page AS (
    SELECT DISTINCT ON (stream, payload->>'run_id')
        stream,
        payload->>'run_id' AS run_id,
        (payload->>'page_index')::bigint AS latest_page_index,
        (payload->>'has_more')::boolean AS latest_has_more,
        payload->>'slack_cursor' AS latest_slack_cursor,
        id
    FROM events
    WHERE stream LIKE 'backfill-run:%'
      AND kind = 'backfill_page_committed'
      AND payload ? 'run_id'
      AND payload ? 'page_index'
    ORDER BY stream, payload->>'run_id', (payload->>'page_index')::bigint DESC, id DESC
)
SELECT
    latest_started.channel_id,
    latest_started.run_id AS last_run_id,
    finished.outcome AS last_run_outcome,
    latest_started.created_at AS last_run_started_at,
    finished.created_at AS last_run_finished_at,
    latest_page.latest_page_index,
    latest_page.latest_has_more,
    latest_page.latest_slack_cursor
FROM latest_started
LEFT JOIN finished
    ON finished.stream = latest_started.stream
   AND finished.run_id = latest_started.run_id
LEFT JOIN latest_page
    ON latest_page.stream = latest_started.stream
   AND latest_page.run_id = latest_started.run_id;

CREATE VIEW channel_ingest_head AS
SELECT
    substr(stream, length('channel:') + 1) AS channel_id,
    max(payload->>'ts') AS latest_ts
FROM events
WHERE stream LIKE 'channel:%'
  AND kind = 'message'
  AND is_valid_slack_ts(payload->>'ts')
GROUP BY stream;

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
