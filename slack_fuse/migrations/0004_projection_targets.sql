-- 0004_projection_targets -- durable per-materialization invalidation state.
--
-- The identity columns are intentionally nullable. PostgreSQL PRIMARY KEY
-- columns are implicitly NOT NULL, while an ordinary UNIQUE constraint treats
-- NULLs as distinct; neither can represent the layout singleton or the other
-- partially-null target shapes. UNIQUE NULLS NOT DISTINCT provides the
-- required null-safe identity and supports ON CONFLICT upserts.

CREATE TABLE projection_targets (
    target_kind         TEXT        NOT NULL,
    channel_id          TEXT,
    local_day           DATE,
    thread_ts           NUMERIC(20, 6),
    target_generation   BIGINT      NOT NULL DEFAULT 1,
    rendered_generation BIGINT      NOT NULL DEFAULT 0,
    renderer_version    TEXT        NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT projection_targets_identity UNIQUE NULLS NOT DISTINCT (
        target_kind, channel_id, local_day, thread_ts
    ),
    CONSTRAINT projection_targets_kind_check CHECK (
        target_kind IN ('channel-meta', 'day', 'thread', 'layout')
    ),
    CONSTRAINT projection_targets_shape_check CHECK (
        (target_kind = 'channel-meta'
            AND channel_id IS NOT NULL AND local_day IS NULL AND thread_ts IS NULL)
        OR (target_kind = 'day'
            AND channel_id IS NOT NULL AND local_day IS NOT NULL AND thread_ts IS NULL)
        OR (target_kind = 'thread'
            AND channel_id IS NOT NULL AND local_day IS NOT NULL AND thread_ts IS NOT NULL)
        OR (target_kind = 'layout'
            AND channel_id IS NULL AND local_day IS NULL AND thread_ts IS NULL)
    ),
    CONSTRAINT projection_targets_generation_check CHECK (
        target_generation >= 1
        AND rendered_generation >= 0
        AND rendered_generation <= target_generation
    )
);

-- Fast lookup for the coalescer's pending-work scan. The null-safe identity
-- constraint above covers target resolution by stable identity.
CREATE INDEX projection_targets_pending_idx
    ON projection_targets (updated_at)
    WHERE rendered_generation < target_generation;

-- Seed the singleton in an epoch that deliberately cannot match the code's
-- post-ledger renderer version. PR 3 will fail closed for this stale epoch (and
-- for missing historical target rows), so pre-ledger files are grandfathered
-- dirty instead of being mistaken for current bytes.
INSERT INTO projection_targets (
    target_kind,
    channel_id,
    local_day,
    thread_ts,
    target_generation,
    renderer_version
) VALUES (
    'layout', NULL, NULL, NULL, 1, 'pre-ledger'
) ON CONFLICT ON CONSTRAINT projection_targets_identity DO NOTHING;
