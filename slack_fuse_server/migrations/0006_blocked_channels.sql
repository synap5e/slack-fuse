-- 0006_blocked_channels — mutable operator policy for channel blocks.
--
-- Blocks are not Slack facts and are not replayed through the events table.
-- They are operator-maintained intent, so they live in a mutable table.

CREATE TABLE blocked_channels (
    channel_id TEXT PRIMARY KEY,
    blocked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    reason TEXT
);
