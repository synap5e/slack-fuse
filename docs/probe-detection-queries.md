# Probe Detection Queries

The probe sweep writes raw Slack API captures to `events` on the
`slurper-health` stream:

- `conversations_history_sampled`
- `conversations_list_sampled`
- `users_list_sampled`
- `probe_sweep_completed`

`probe_sweep_completed` includes `triggered_by` (`scheduled` or `manual`) and
`requested` (null for scheduled/all-jobs sweeps, otherwise the requested
`job_id`/`target`). The liveness query below intentionally counts both
scheduled and manual completions: a manual smoke sweep proves the task can still
run, so it is valid heartbeat evidence.

The list captures concatenate paginated Slack responses into one logical
payload. Expected size is modest: roughly hundreds of KB for a workspace with
around 1000 channels or users. If this becomes slow, add targeted indexes after
observing real query plans; v1 does not add JSONB GIN indexes.

## Sweep Liveness

Default `probe_sweep_interval_s` is 1 hour. Alert when no heartbeat landed in
twice that interval:

```sql
SELECT max(created_at) AS last_probe_sweep_completed
FROM events
WHERE stream = 'slurper-health'
  AND kind = 'probe_sweep_completed'
HAVING max(created_at) IS NULL OR max(created_at) < now() - interval '2 hours';
```

## Sampling-purpose discriminators

Three jobs share the `conversations_history_sampled` kind and are disjoint on
`call_params` keys:

- newest-message: neither `oldest` nor `latest`
- older-than-oldest: `latest` only
- day-presence: both `oldest` and `latest`

Queries over history samples must filter on this key shape, not just the kind.

## Older History Not Walked

The older-history job calls:

```text
conversations.history(channel=<channel>, latest=<local_oldest_ts>, limit=1)
```

Slack timestamp bounds are exclusive by default. If Slack returns any message,
there is older channel history not represented locally.

```sql
WITH latest_samples AS (
  SELECT DISTINCT ON (payload->'call_params'->>'channel')
      created_at,
      payload
  FROM events
  WHERE stream = 'slurper-health'
    AND kind = 'conversations_history_sampled'
    AND payload->'call_params'->>'latest' IS NOT NULL
    AND NOT (COALESCE(payload->'call_params', '{}'::jsonb) ? 'oldest')
  ORDER BY payload->'call_params'->>'channel', created_at DESC
)
SELECT
    payload->'call_params'->>'channel' AS channel_id,
    payload->'call_params'->>'latest' AS local_oldest_ts_at_capture,
    payload->'response'->'messages'->0->>'ts' AS older_sample_ts,
    created_at AS captured_at
FROM latest_samples
WHERE jsonb_array_length(COALESCE(payload->'response'->'messages', '[]'::jsonb)) > 0
ORDER BY created_at DESC;
```

## Newest Message Drift

This catches head drift: Slack has a newest message more than 5 minutes newer
than the locally active message set. It does not catch mid-stream gaps after a
later local message has arrived; that is the day-presence job's territory
(see "Day-Presence Gaps" below).

```sql
WITH history_samples AS (
  SELECT DISTINCT ON (payload->'call_params'->>'channel')
      payload->'call_params'->>'channel' AS channel_id,
      payload->'response'->'messages'->0->>'ts' AS slack_newest_ts,
      created_at
  FROM events
  WHERE stream = 'slurper-health'
    AND kind = 'conversations_history_sampled'
    AND NOT (COALESCE(payload->'call_params', '{}'::jsonb) ? 'latest')
    AND NOT (COALESCE(payload->'call_params', '{}'::jsonb) ? 'oldest')
  ORDER BY payload->'call_params'->>'channel', created_at DESC
),
candidates AS (
  SELECT id, stream, payload->>'ts' AS ts
  FROM events
  WHERE stream LIKE 'channel:%'
    AND kind = 'message'
    AND payload->>'ts' ~ '^[0-9]+\.[0-9]+$'
  UNION ALL
  SELECT id, stream, payload->'message'->>'ts' AS ts
  FROM events
  WHERE stream LIKE 'channel:%'
    AND kind = 'message_changed'
    AND payload->'message'->>'ts' ~ '^[0-9]+\.[0-9]+$'
),
active_candidates AS (
  SELECT c.*
  FROM candidates c
  WHERE NOT EXISTS (
      SELECT 1
      FROM events d
      WHERE d.stream = c.stream
        AND d.kind = 'message_deleted'
        AND d.payload->>'deleted_ts' = c.ts
        AND d.id > c.id
  )
    AND NOT EXISTS (
      SELECT 1
      FROM events ch
      WHERE ch.stream = c.stream
        AND ch.kind = 'message_changed'
        AND ch.payload->>'previous_ts' = c.ts
        AND ch.payload->'message'->>'ts' <> c.ts
        AND ch.id > c.id
  )
),
local_newest AS (
  SELECT
      replace(stream, 'channel:', '') AS channel_id,
      max(ts::numeric) AS local_newest_ts
  FROM active_candidates
  GROUP BY stream
)
SELECT
    h.channel_id,
    h.slack_newest_ts,
    l.local_newest_ts::text,
    (h.slack_newest_ts::numeric - l.local_newest_ts) AS delta_seconds,
    h.created_at AS captured_at
FROM history_samples h
LEFT JOIN local_newest l USING (channel_id)
WHERE h.slack_newest_ts ~ '^[0-9]+\.[0-9]+$'
  AND (l.local_newest_ts IS NULL OR h.slack_newest_ts::numeric - l.local_newest_ts > 300)
ORDER BY delta_seconds DESC NULLS FIRST;
```

## Day-Presence Gaps

The day-presence job calls:

```text
conversations.history(channel=<channel>, oldest=<day 00:00:00.000000>,
                      latest=<day 23:59:59.999999>, limit=1)
```

for one UTC day per channel per sweep, rotating over the last 30 complete UTC
days (stalest day first, most-recent-day tiebreak; each (channel, day) pair is
resampled on a 7-day cadence). This catches mid-stream gaps: a catchup hole
where a single post-reconnect live message advanced the local newest ts past a
lost window, so head/tail probes see nothing wrong.

With `limit=1` Slack returns its newest message inside the window. If that
exact ts is not an active local message, we lost it (or everything around it).
This also catches partial-day gaps where the local view has *some* messages
for the day but not Slack's newest one.

```sql
-- Rewritten 2026-07-05: the earlier version used the ``active_messages`` view
-- and folded the whole channel stream per row — 2min+ at prod scale (~300K
-- events). The version below reads events directly using the shipped indexes:
-- ``events_message_dedup`` (partial unique on ``(stream, kind, payload->>'ts')``
-- for ``kind='message'``) covers both the exact-ts existence check and the day-
-- range count as text comparisons (Slack ts is fixed-width so lex ordering
-- equals numeric ordering). ``events_message_deleted_target_idx`` covers the
-- tombstone anti-join. Completes in single-digit seconds at prod scale.

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
    s.slack_sample_ts,
    (SELECT count(*)
       FROM events m
      WHERE m.stream = 'channel:' || s.channel_id
        AND m.kind = 'message'
        AND m.payload->>'ts' >= s.day_start_text
        AND m.payload->>'ts' <= s.day_end_text) AS local_message_rows_in_day,
    s.sampled_at
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
```

The ``local_message_rows_in_day`` column is the raw ``kind='message'`` row
count in the day range, without subtracting tombstones. For the alert
predicate itself (``NOT EXISTS`` on Slack's sampled ts) the tombstone
anti-join keeps semantics identical to the prior ``active_messages``
version — a locally-deleted message that Slack still shows would not
alert.

Known benign-result classes (read `sampled_at` before paging anyone):

- **Delete race**: a message deleted in Slack after the capture leaves a stale
  sample pointing at a now-tombstoned ts. Self-heals on the next weekly
  resample while the day is inside the 30-day window; samples for days that
  have slid out of the window are never refreshed, so treat old `sampled_at`
  rows with suspicion.
- **Backfill/catchup in progress**: the local side of the comparison is
  computed at query time, so a gap that backfill has since filled disappears
  from this query on its own. A row that persists across a completed backfill
  is a real loss.
- Boundary exclusivity (Slack's `oldest`/`latest` are exclusive) can only make
  Slack's window smaller than the local comparison window, i.e. it can delay a
  detection but never fabricate one.

## Missing Channel Inventory

Channels present in Slack inventory but absent from channel-list facts are
highest severity.

```sql
WITH latest_inventory AS (
  SELECT payload
  FROM events
  WHERE stream = 'slurper-health'
    AND kind = 'conversations_list_sampled'
  ORDER BY created_at DESC
  LIMIT 1
),
sampled AS (
  SELECT channel->>'id' AS channel_id
  FROM latest_inventory, jsonb_array_elements(payload->'response'->'channels') AS channel
),
known AS (
  SELECT DISTINCT payload->>'id' AS channel_id
  FROM events
  WHERE stream = 'channel-list'
    AND kind IN ('channel_added', 'channel_info_refreshed')
    AND payload ? 'id'
)
SELECT sampled.channel_id
FROM sampled
LEFT JOIN known USING (channel_id)
WHERE known.channel_id IS NULL
ORDER BY sampled.channel_id;
```

Channels that local channel-list facts say are in scope, but that are absent
from the latest Slack inventory sample, usually mean archived/left/access-loss
drift that the slurper missed.

```sql
WITH latest_inventory AS (
  SELECT payload
  FROM events
  WHERE stream = 'slurper-health'
    AND kind = 'conversations_list_sampled'
  ORDER BY created_at DESC
  LIMIT 1
),
sampled AS (
  SELECT channel->>'id' AS channel_id
  FROM latest_inventory, jsonb_array_elements(payload->'response'->'channels') AS channel
),
base AS (
  SELECT DISTINCT ON (payload->>'id')
      payload->>'id' AS channel_id,
      id,
      payload
  FROM events
  WHERE stream = 'channel-list'
    AND kind IN ('channel_added', 'channel_info_refreshed')
    AND payload ? 'id'
  ORDER BY payload->>'id', id DESC
),
member_changes AS (
  SELECT DISTINCT ON (payload->>'channel_id')
      payload->>'channel_id' AS channel_id,
      (payload->>'is_member')::boolean AS is_member,
      id
  FROM events
  WHERE stream = 'channel-list'
    AND kind = 'channel_member_changed'
  ORDER BY payload->>'channel_id', id DESC
),
archive_changes AS (
  SELECT DISTINCT ON (payload->>'channel_id')
      payload->>'channel_id' AS channel_id,
      kind = 'channel_archived' AS is_archived,
      id
  FROM events
  WHERE stream = 'channel-list'
    AND kind IN ('channel_archived', 'channel_unarchived')
  ORDER BY payload->>'channel_id', id DESC
),
current_local AS (
  SELECT
      b.channel_id,
      COALESCE(m.is_member, (b.payload->>'is_member')::boolean, false) AS is_member,
      COALESCE(a.is_archived, (b.payload->>'is_archived')::boolean, false) AS is_archived,
      COALESCE((b.payload->>'is_im')::boolean, false) AS is_im,
      COALESCE((b.payload->>'is_mpim')::boolean, false) AS is_mpim
  FROM base b
  LEFT JOIN member_changes m USING (channel_id)
  LEFT JOIN archive_changes a USING (channel_id)
)
SELECT current_local.channel_id
FROM current_local
LEFT JOIN sampled USING (channel_id)
WHERE sampled.channel_id IS NULL
  AND current_local.is_archived = false
  AND (current_local.is_member OR current_local.is_im OR current_local.is_mpim)
ORDER BY current_local.channel_id;
```

## Missing Active Users

Active, non-bot users present in the latest Slack `users.list` sample but absent
from `user_added` facts indicate silent user inventory loss.

```sql
WITH latest_users AS (
  SELECT payload
  FROM events
  WHERE stream = 'slurper-health'
    AND kind = 'users_list_sampled'
  ORDER BY created_at DESC
  LIMIT 1
),
sampled AS (
  SELECT member->>'id' AS user_id
  FROM latest_users, jsonb_array_elements(payload->'response'->'members') AS member
  WHERE COALESCE((member->>'deleted')::boolean, false) = false
    AND COALESCE((member->>'is_bot')::boolean, false) = false
),
known AS (
  SELECT DISTINCT payload->>'id' AS user_id
  FROM events
  WHERE stream = 'users'
    AND kind = 'user_added'
    AND payload ? 'id'
)
SELECT sampled.user_id
FROM sampled
LEFT JOIN known USING (user_id)
WHERE known.user_id IS NULL
ORDER BY sampled.user_id;
```

## User Count Drift

Operator-interesting, not necessarily data loss:

```sql
WITH samples AS (
  SELECT
      e.created_at,
      created_at::date AS day,
      count(*) FILTER (
        WHERE COALESCE((member->>'deleted')::boolean, false) = false
          AND COALESCE((member->>'is_bot')::boolean, false) = false
      ) AS active_count
  FROM events e
  CROSS JOIN LATERAL jsonb_array_elements(e.payload->'response'->'members') AS member
  WHERE e.stream = 'slurper-health'
    AND e.kind = 'users_list_sampled'
  GROUP BY created_at::date, e.id
),
latest_per_day AS (
  SELECT DISTINCT ON (day) day, active_count
  FROM samples
  ORDER BY day, created_at DESC
)
SELECT
    day,
    active_count,
    active_count - lag(active_count) OVER (ORDER BY day) AS day_over_day_delta
FROM latest_per_day
ORDER BY day DESC;
```
