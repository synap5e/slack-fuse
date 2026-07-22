-- Durable Slack Events API inbox and universal per-event dispatch dedup.

CREATE TABLE IF NOT EXISTS slack_event_inbox (
    event_id         TEXT        PRIMARY KEY,
    envelope         JSONB       NOT NULL,
    received_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at     TIMESTAMPTZ,
    attempt_count    INTEGER     NOT NULL DEFAULT 0,
    last_attempt_at  TIMESTAMPTZ,
    next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    dispatch_error   TEXT,
    dead_lettered_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS slack_event_inbox_pending
    ON slack_event_inbox (next_attempt_at, received_at)
    WHERE processed_at IS NULL AND dead_lettered_at IS NULL;

-- The migration runner wraps each file in a transaction, so this deliberately
-- uses a normal blocking index build rather than CREATE INDEX CONCURRENTLY.
CREATE UNIQUE INDEX IF NOT EXISTS events_slack_event_id_dedup
    ON events (stream, kind, (source->>'slack_event_id'))
    WHERE source ? 'slack_event_id';
