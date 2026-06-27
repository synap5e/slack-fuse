-- 0002_block_sync — remembers which manual-blocked rows came from the server
-- blocked_channels SSOT, so unblock sync can demote only those rows back to
-- auto tier without clobbering unrelated local tier CLI choices.

CREATE TABLE server_block_sync (
    channel_id TEXT PRIMARY KEY,
    synced_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
