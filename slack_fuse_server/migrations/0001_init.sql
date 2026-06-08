-- 0001_init — server event store.
-- Mirrors slack_fuse_server/schema.sql (RFC §Schemas → Server: events store).

CREATE TABLE events (
    id BIGSERIAL PRIMARY KEY,
    stream TEXT NOT NULL,
    offset_in_stream BIGINT NOT NULL,
    kind TEXT NOT NULL,
    ts TEXT,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (stream, offset_in_stream)
);
CREATE INDEX events_stream_offset_idx ON events (stream, offset_in_stream);

CREATE UNIQUE INDEX events_message_dedup
    ON events (stream, kind, (payload->>'ts'))
    WHERE kind = 'message';

CREATE TABLE snapshots (
    stream TEXT NOT NULL,
    at_offset BIGINT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload_bytes BIGINT NOT NULL,
    events_covered BIGINT NOT NULL,
    generation_duration_ms INT NOT NULL,
    generation_trigger TEXT NOT NULL
        CHECK (generation_trigger IN ('event_count', 'time', 'manual')),
    PRIMARY KEY (stream, at_offset)
);

CREATE TABLE snapshot_uses (
    snapshot_stream TEXT NOT NULL,
    snapshot_at_offset BIGINT NOT NULL,
    used_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    client_since_offset BIGINT NOT NULL,
    events_skipped BIGINT NOT NULL,
    FOREIGN KEY (snapshot_stream, snapshot_at_offset)
        REFERENCES snapshots (stream, at_offset)
);
CREATE INDEX snapshot_uses_lookup_idx
    ON snapshot_uses (snapshot_stream, snapshot_at_offset);

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
    num_members INT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE users (
    user_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE stream_heads (
    stream TEXT PRIMARY KEY,
    next_offset BIGINT NOT NULL DEFAULT 1
);

CREATE TABLE health_log (
    id BIGSERIAL PRIMARY KEY,
    kind TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE backfill_overrides (
    channel_id TEXT PRIMARY KEY,
    max_messages BIGINT,
    set_by TEXT NOT NULL DEFAULT 'admin',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
