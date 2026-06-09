-- Sprint 1 follow-up: schema-level dedup for channel-list channel_added events.
--
-- The slurper now emits one `channel_added` event per workspace conversation on
-- startup (`populate_channels_once`, mirroring `populate_users_once`). Without
-- this a fresh split-mode client never sees channels until Slack happens to push
-- a live channel-structure event, because the client only subscribes to
-- per-channel streams for channels present in its local table.
--
-- Mirrors `events_users_added_dedup` (migration 0002): the startup populate uses
-- runtime dedup (SELECT FOR UPDATE on stream_heads + SELECT-existing-id before
-- INSERT) so re-running on restart is idempotent. This partial unique index
-- makes that idempotency a hard schema invariant for any writer.
--
-- Keyed on `payload ->> 'id'` because the `channel_added` payload is the full
-- channel object (`Channel.model_dump`), whose primary key field is `id` — the
-- same shape the live socket-mode path emits (slurper/socket.py
-- `_channel_added_write`). The index is partial on kind='channel_added' so the
-- other `channel-list` kinds (channel_renamed / channel_archived /
-- channel_member_changed, keyed on `channel_id`) are unaffected.

CREATE UNIQUE INDEX IF NOT EXISTS events_channels_added_dedup
    ON events (stream, kind, (payload ->> 'id'))
    WHERE stream = 'channel-list' AND kind = 'channel_added';
