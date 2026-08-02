-- Query-derived Slack facts used by the workspace channel inventory.
--
-- Unlike pushed Slack events, search.messages totals are refreshed in place.
-- A failed refresh updates only refresh_status/refreshed_at, preserving the
-- last useful total for operator visibility.
CREATE TABLE channel_message_totals (
    channel_id TEXT PRIMARY KEY,
    total BIGINT NOT NULL,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    refresh_status TEXT NOT NULL DEFAULT 'ok'
);
