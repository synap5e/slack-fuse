-- Socket-mode extended subscriptions: replay dedup for newly captured events.
--
-- These indexes are intentionally narrow and partial. They make the new
-- handlers idempotent against Socket Mode replay without changing existing
-- message edit/delete semantics or adding a blanket uniqueness rule over the
-- append-only event log.

CREATE UNIQUE INDEX IF NOT EXISTS events_parent_replied_dedup
    ON events (stream, kind, (payload ->> 'parent_ts'), (payload ->> 'reply_count'))
    WHERE kind = 'parent_replied';

CREATE UNIQUE INDEX IF NOT EXISTS events_channel_id_changed_dedup
    ON events (stream, kind, (payload ->> 'old_channel_id'), (payload ->> 'new_channel_id'), (payload ->> 'event_ts'))
    WHERE stream = 'channel-list' AND kind = 'channel_id_changed';

CREATE UNIQUE INDEX IF NOT EXISTS events_channel_history_changed_dedup
    ON events (stream, kind, (payload ->> 'channel_id'), (payload ->> 'latest'), (payload ->> 'ts'), (payload ->> 'event_ts'))
    WHERE stream = 'channel-list' AND kind = 'channel_history_changed';

CREATE UNIQUE INDEX IF NOT EXISTS events_channel_member_user_dedup
    ON events (stream, kind, (payload ->> 'channel_id'), (payload ->> 'user_id'), (payload ->> 'event_ts'))
    WHERE stream = 'channel-list' AND kind IN ('channel_member_joined', 'channel_member_left');

CREATE UNIQUE INDEX IF NOT EXISTS events_tokens_revoked_dedup
    ON events (stream, kind, (payload -> 'tokens'))
    WHERE stream = 'slurper-health' AND kind = 'tokens_revoked';
