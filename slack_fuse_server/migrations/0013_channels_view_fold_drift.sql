-- Fold every channel-list event kind that mutates operator-visible channel
-- metadata. Full-payload events (channel_added / channel_info_refreshed) and
-- narrow drift events must be combined per column: choosing one latest row
-- would erase fields that are absent from a rename or membership event.
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

DROP VIEW IF EXISTS channels;

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
