ALTER TABLE slack_event_inbox
    ADD COLUMN IF NOT EXISTS source_transport TEXT NOT NULL DEFAULT 'http';
