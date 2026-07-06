-- Day-presence gap detection.
--
-- This is the optimized direct-events query from docs/probe-detection-queries.md,
-- shaped for GET /gap-candidates. It intentionally bypasses the active_messages fold and
-- checks the exact Slack sample timestamp against non-tombstoned local message
-- rows.

WITH samples AS (
  SELECT DISTINCT ON (
      payload->'call_params'->>'channel',
      payload->'call_params'->>'oldest'
  )
      payload->'call_params'->>'channel' AS channel_id,
      payload->'call_params'->>'oldest' AS day_start_text,
      payload->'call_params'->>'latest' AS day_end_text,
      (payload->'call_params'->>'oldest')::numeric AS day_start_num,
      payload->'response'->'messages'->0->>'ts' AS slack_sample_ts,
      created_at AS sampled_at
  FROM events
  WHERE stream = 'slurper-health'
    AND kind = 'conversations_history_sampled'
    AND payload->'call_params' ? 'oldest'
    AND payload->'call_params' ? 'latest'
  ORDER BY
      payload->'call_params'->>'channel',
      payload->'call_params'->>'oldest',
      created_at DESC
)
SELECT
    s.channel_id,
    to_timestamp(s.day_start_num)::date AS day,
    s.day_start_text::double precision AS oldest_ts,
    s.day_end_text::double precision AS latest_ts,
    s.slack_sample_ts,
    s.sampled_at,
    'day_presence' AS gap_type
FROM samples s
WHERE s.slack_sample_ts IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM events m
    WHERE m.stream = 'channel:' || s.channel_id
      AND m.kind = 'message'
      AND m.payload->>'ts' = s.slack_sample_ts
      AND NOT EXISTS (
        SELECT 1 FROM events d
        WHERE d.stream = m.stream
          AND d.kind = 'message_deleted'
          AND d.payload->>'deleted_ts' = m.payload->>'ts'
      )
  )
ORDER BY s.sampled_at DESC;
