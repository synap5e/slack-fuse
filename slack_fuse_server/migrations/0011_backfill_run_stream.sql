-- 0011_backfill_run_stream — event-sourced backfill lifecycle.
--
-- Per-channel lifecycle facts live on backfill-run:<channel_id> streams. The
-- stream replaces health-event folds for resume/catchup policy while leaving
-- slurper-health backfill events in place for one release of operator overlap.

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
