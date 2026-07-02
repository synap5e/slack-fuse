-- Normalized active Slack-message views.
--
-- These views derive current message state from the append-only Slack-content
-- streams only. They store no progress/scheduling facts; callers should filter
-- by stream = 'channel:' || $channel_id so the planner can use stream indexes.

CREATE OR REPLACE FUNCTION is_valid_slack_ts(ts text) RETURNS bool
  LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT
  AS $$ SELECT ts ~ '^[1-9][0-9]{9}\.[0-9]{6}$' $$;

CREATE INDEX IF NOT EXISTS events_message_changed_target_idx
    ON events (stream, (payload->'message'->>'ts'), offset_in_stream DESC)
    WHERE kind = 'message_changed';

CREATE INDEX IF NOT EXISTS events_message_deleted_target_idx
    ON events (stream, (payload->>'deleted_ts'))
    WHERE kind = 'message_deleted';

CREATE INDEX IF NOT EXISTS events_parent_replied_target_idx
    ON events (stream, (payload->>'parent_ts'), offset_in_stream DESC)
    WHERE kind = 'parent_replied';

CREATE OR REPLACE VIEW active_messages AS
WITH
  base_events AS (
    SELECT
      stream,
      substr(stream, 9) AS channel_id,
      payload->>'ts' AS effective_ts,
      payload AS payload,
      offset_in_stream
    FROM events
    WHERE stream LIKE 'channel:%'
      AND kind = 'message'
      AND is_valid_slack_ts(payload->>'ts')
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
    WHERE stream LIKE 'channel:%'
      AND kind = 'message_changed'
      AND is_valid_slack_ts(payload->'message'->>'ts')
  ),
  supplanted_by_change AS (
    SELECT stream, previous_ts AS effective_ts
    FROM change_events
    WHERE is_valid_slack_ts(previous_ts)
      AND previous_ts <> effective_ts
  ),
  tombstones AS (
    SELECT
      stream,
      payload->>'deleted_ts' AS effective_ts
    FROM events
    WHERE stream LIKE 'channel:%'
      AND kind = 'message_deleted'
      AND is_valid_slack_ts(payload->>'deleted_ts')
  ),
  latest_per_ts AS (
    SELECT DISTINCT ON (stream, effective_ts)
      stream,
      channel_id,
      effective_ts,
      payload,
      offset_in_stream
    FROM (
      SELECT stream, channel_id, effective_ts, payload, offset_in_stream, 1 AS priority
        FROM change_events
      UNION ALL
      SELECT stream, channel_id, effective_ts, payload, offset_in_stream, 0 AS priority
        FROM base_events
    ) chained
    -- offset_in_stream is authoritative. priority is only a defensive tie-break
    -- for impossible same-offset rows; if they exist, prefer corrected payload.
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
  SELECT 1
  FROM supplanted_by_change s
  WHERE s.stream = l.stream
    AND s.effective_ts = l.effective_ts
)
AND NOT EXISTS (
  SELECT 1
  FROM tombstones t
  WHERE t.stream = l.stream
    AND t.effective_ts = l.effective_ts
);

CREATE OR REPLACE VIEW active_thread_parents AS
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
    WHERE stream LIKE 'channel:%'
      AND kind = 'parent_replied'
      AND is_valid_slack_ts(payload->>'parent_ts')
  ),
  candidates AS (
    SELECT stream, channel_id, parent_ts, reply_count, latest_reply, offset_in_stream
      FROM base_parents
    UNION ALL
    SELECT pu.stream, pu.channel_id, pu.parent_ts, pu.reply_count, pu.latest_reply, pu.offset_in_stream
      FROM parent_updates pu
      WHERE EXISTS (
        SELECT 1
        FROM base_parents bp
        WHERE bp.stream = pu.stream
          AND bp.parent_ts = pu.parent_ts
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
