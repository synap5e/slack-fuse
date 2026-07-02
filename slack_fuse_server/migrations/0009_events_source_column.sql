-- 0009_events_source_column — ambient ingestion metadata on every event.
--
-- `source` carries facts about the ingestion transaction (producer, boot/task/
-- run ids, Slack cursors, commit, span id — see slurper/ingestion.py). It is
-- NULL for all pre-migration rows and for writes made outside an ingestion
-- scope; every partial index below therefore excludes legacy rows for free.
-- Adding a nullable jsonb column is metadata-only (no table rewrite).

ALTER TABLE events ADD COLUMN source JSONB;

-- Restart-resume: latest history-page row per channel stream.
CREATE INDEX events_source_backfill_history_idx
    ON events (stream, offset_in_stream DESC)
    WHERE source->>'producer' = 'backfill-history-page';

-- Thread completion/resume: replies rows per (stream, thread_ts).
CREATE INDEX events_source_backfill_replies_idx
    ON events (stream, (source->>'thread_ts'), offset_in_stream DESC)
    WHERE source->>'producer' = 'backfill-replies-page';

-- Forensic correlation: which commit / boot / span wrote these rows.
CREATE INDEX events_source_commit_idx
    ON events ((source->>'commit'))
    WHERE source IS NOT NULL;

CREATE INDEX events_source_boot_idx
    ON events ((source->>'boot_id'))
    WHERE source IS NOT NULL;

CREATE INDEX events_source_span_idx
    ON events ((source->>'span_id'))
    WHERE source IS NOT NULL;
