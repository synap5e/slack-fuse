-- Sprint 1 follow-up: schema-level dedup for users-stream user_added events.
--
-- Sprint 1E (commit 98a4f8d) emits one `user_added` event per workspace user
-- on slurper startup and uses runtime dedup (SELECT FOR UPDATE on stream_heads
-- plus SELECT-existing-id-before-INSERT) to be idempotent across restarts.
--
-- Post-Sprint-1 review (2026-06-08) flagged that this is weaker than the
-- message-backfill `events_message_dedup` partial unique index: any future
-- writer that bypasses `_lock_users_stream()` could insert duplicates. The
-- event log is the durable contract, so idempotency belongs in the database.
--
-- This partial unique index makes the dedup a hard schema invariant for any
-- writer.

CREATE UNIQUE INDEX IF NOT EXISTS events_users_added_dedup
    ON events (stream, kind, (payload ->> 'id'))
    WHERE stream = 'users' AND kind = 'user_added';
