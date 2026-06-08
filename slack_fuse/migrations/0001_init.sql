-- 0001_init — client projections store.
-- Mirrors slack_fuse/schema.sql (RFC §Schemas → Client: projections store).

CREATE TABLE chunks (
    channel_id TEXT NOT NULL,
    message_ts NUMERIC(20, 6) NOT NULL,
    content_md TEXT NOT NULL,
    reply_count INT NOT NULL DEFAULT 0,
    accessed_at TIMESTAMPTZ,
    PRIMARY KEY (channel_id, message_ts)
);

CREATE TABLE thread_chunks (
    channel_id TEXT NOT NULL,
    thread_ts NUMERIC(20, 6) NOT NULL,
    reply_ts NUMERIC(20, 6) NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('parent', 'reply')),
    content_md TEXT NOT NULL,
    accessed_at TIMESTAMPTZ,
    PRIMARY KEY (channel_id, thread_ts, reply_ts)
);

CREATE TABLE channels (
    channel_id TEXT PRIMARY KEY,
    name TEXT,
    is_im BOOLEAN,
    is_mpim BOOLEAN,
    is_member BOOLEAN,
    is_archived BOOLEAN,
    im_user_id TEXT,
    topic TEXT,
    purpose TEXT,
    tier TEXT NOT NULL DEFAULT 'hot'
        CHECK (tier IN ('hot', 'hidden', 'blocked')),
    tier_source TEXT NOT NULL DEFAULT 'auto'
        CHECK (tier_source IN ('auto', 'manual')),
    subscribed BOOLEAN NOT NULL DEFAULT TRUE,
    last_accessed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE users (
    user_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE cursors (
    stream TEXT PRIMARY KEY,
    applied_offset BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE chunk_mentions (
    channel_id TEXT NOT NULL,
    message_ts NUMERIC(20, 6) NOT NULL,
    mention_kind TEXT NOT NULL CHECK (mention_kind IN ('user', 'channel')),
    mentioned_id TEXT NOT NULL,
    PRIMARY KEY (channel_id, message_ts, mention_kind, mentioned_id),
    FOREIGN KEY (channel_id, message_ts)
        REFERENCES chunks (channel_id, message_ts)
        ON DELETE CASCADE
);
CREATE INDEX chunk_mentions_lookup_idx
    ON chunk_mentions (mention_kind, mentioned_id);

CREATE TABLE thread_chunk_mentions (
    channel_id TEXT NOT NULL,
    thread_ts NUMERIC(20, 6) NOT NULL,
    reply_ts NUMERIC(20, 6) NOT NULL,
    mention_kind TEXT NOT NULL CHECK (mention_kind IN ('user', 'channel')),
    mentioned_id TEXT NOT NULL,
    PRIMARY KEY (channel_id, thread_ts, reply_ts, mention_kind, mentioned_id),
    FOREIGN KEY (channel_id, thread_ts, reply_ts)
        REFERENCES thread_chunks (channel_id, thread_ts, reply_ts)
        ON DELETE CASCADE
);
CREATE INDEX thread_chunk_mentions_lookup_idx
    ON thread_chunk_mentions (mention_kind, mentioned_id);

CREATE TABLE inodes (
    path TEXT PRIMARY KEY,
    inode BIGINT NOT NULL UNIQUE GENERATED ALWAYS AS IDENTITY (START WITH 2)
);

CREATE TABLE connection_state (
    id INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    last_frame_at TIMESTAMPTZ,
    last_slurper_health TEXT NOT NULL DEFAULT 'unknown'
        CHECK (last_slurper_health IN
            ('unknown', 'healthy', 'degraded', 'disconnected', 'auth_failed')),
    last_health_update_at TIMESTAMPTZ
);
INSERT INTO connection_state (id) VALUES (1);

CREATE TABLE stream_caught_up (
    stream TEXT PRIMARY KEY,
    caught_up_at TIMESTAMPTZ NOT NULL,
    at_offset BIGINT NOT NULL
);
