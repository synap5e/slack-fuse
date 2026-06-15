-- Replace the cluster-side `channels` table with a VIEW derived from the
-- events log. Before this migration, the table existed but was never written
-- to (verified empty in production), because no slurper code path UPSERTed
-- into it — channel metadata only lived in `channel_added` event payloads.
--
-- A dual-write (UPSERT alongside the event INSERT) would have worked but
-- conflates "what happened" (the event) with "what is" (the materialization).
-- The cleanest event-sourcing-respecting fix is to drop the table and expose
-- a VIEW so operators can continue to `JOIN channels USING (channel_id)` —
-- the data lives in one place (the events table) and there's no drift risk.
--
-- For a workspace with ~400 channels the DISTINCT ON scan is fast; if it ever
-- becomes a hot read path, promote to a server-side projector task that
-- maintains a real materialization table from the events stream (mirroring
-- the client projector's pattern). See BACKLOG.

DROP TABLE IF EXISTS channels;

CREATE OR REPLACE VIEW channels AS
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
