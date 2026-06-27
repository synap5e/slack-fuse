-- slack-fuse client schema (projections store).
--
-- Authoritative reference copy of RFC §Schemas → Client: projections store.
-- The migration runner applies migrations/0001_init.sql (identical content);
-- this file exists so the schema is reviewable as a single document.

-- One pre-rendered markdown block per top-level message.
-- Composing channel.md = SELECT WHERE channel_id = ? AND message_ts
-- in [start, end), ORDER BY message_ts. No local-tz date column;
-- the FUSE layer derives date folders at read time from message_ts +
-- the process's local timezone, so the same events project to the
-- same chunks on every device regardless of tz. PK supports
-- thread-reply parent lookup by (channel_id, message_ts) directly.
CREATE TABLE chunks (
    channel_id TEXT NOT NULL,
    message_ts NUMERIC(20, 6) NOT NULL,   -- Slack ts as UTC epoch
    content_md TEXT NOT NULL,             -- output of render_message_structural(...)
    reply_count INT NOT NULL DEFAULT 0,
    accessed_at TIMESTAMPTZ,              -- unused in v1; v2 LRU eviction
    PRIMARY KEY (channel_id, message_ts)
);
-- PK index covers: per-day reads (range scan on message_ts), full-channel
-- iteration, and thread-reply parent lookup (`WHERE channel_id = ?
-- AND message_ts = $thread_ts`). No additional index needed.

-- One pre-rendered block per message in a thread (parent + replies).
CREATE TABLE thread_chunks (
    channel_id TEXT NOT NULL,
    thread_ts NUMERIC(20, 6) NOT NULL,
    reply_ts NUMERIC(20, 6) NOT NULL,     -- equals thread_ts for parent
    role TEXT NOT NULL CHECK (role IN ('parent', 'reply')),
    content_md TEXT NOT NULL,
    accessed_at TIMESTAMPTZ,
    PRIMARY KEY (channel_id, thread_ts, reply_ts)
);
-- PK supports per-thread read (`WHERE channel_id = ? AND thread_ts = ?
-- ORDER BY reply_ts`). No additional index needed.

-- Mirrored channel inventory + per-client tier preferences.
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

-- Local user cache. Mirrored from server.
CREATE TABLE users (
    user_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Per-stream applied offset. The projector advances these.
CREATE TABLE cursors (
    stream TEXT PRIMARY KEY,
    applied_offset BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Side table: which user/channel IDs are mentioned inside which
-- chunks. Lets `user_added` / `user_renamed` / `channel_renamed`
-- events invalidate the affected inodes in O(N) instead of
-- `WHERE content_md LIKE …` over the whole chunks table. Populated
-- as chunks are written, even when the mention falls back to a UID
-- literal at read time (so a later `user_added` for that UID can
-- still find and invalidate it — see cross-stream race below).
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

-- Persistent FUSE inode mapping. Allocated on first lookup; never
-- recycled. Survives mount restarts so `find` outputs, fd-based
-- watching, and tools that cache inodes don't break across restarts.
CREATE TABLE inodes (
    path TEXT PRIMARY KEY,
    inode BIGINT NOT NULL UNIQUE GENERATED ALWAYS AS IDENTITY (START WITH 2)
);
-- Inode 1 is reserved for the filesystem root.

-- Tracks last successful contact with the server (any frame received).
-- Used by the FUSE read layer to decide whether to append a "content
-- may be stale" trailer. Updated on every frame from the WS connection.
CREATE TABLE connection_state (
    id INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    last_frame_at TIMESTAMPTZ,
    last_slurper_health TEXT NOT NULL DEFAULT 'unknown'
        CHECK (last_slurper_health IN
            ('unknown', 'healthy', 'degraded', 'disconnected', 'auth_failed')),
    last_health_update_at TIMESTAMPTZ
);
INSERT INTO connection_state (id) VALUES (1);

-- Per-stream catch-up state. Set when a `caught_up` frame arrives
-- for the stream; cleared when the WS reconnects. The FUSE read
-- layer uses this to drive the "initial catch-up incomplete"
-- trailer condition per stream.
CREATE TABLE stream_caught_up (
    stream TEXT PRIMARY KEY,
    caught_up_at TIMESTAMPTZ NOT NULL,
    at_offset BIGINT NOT NULL
);

-- Channels this client pinned because they appeared in the server-side
-- blocked_channels table. This is local sync bookkeeping, not operator policy;
-- the server table remains the SSOT.
CREATE TABLE server_block_sync (
    channel_id TEXT PRIMARY KEY,
    synced_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
