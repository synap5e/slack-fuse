-- Replace the `health_log` table with a VIEW over events.
--
-- Before this migration, `HealthEmitter._emit_sync` performed a dual-write
-- inside one transaction: append the event to `events` (the source of
-- truth), then INSERT INTO health_log the same payload (a convenience
-- materialization that mirrored `events WHERE stream='slurper-health'`).
--
-- The dual-write was atomic so there was no drift risk, but it's the same
-- ES anti-pattern as the (now-also-removed) `channels` table — duplicate
-- relational state next to the log. We drop the table and expose a VIEW;
-- operators can keep running `SELECT … FROM health_log …` queries
-- unchanged. The event INSERT in `_emit_sync` is now the sole write path.
--
-- Note: the VIEW's `id` column is `events.id` (globally-monotonic BIGSERIAL)
-- rather than the old `health_log.id` (per-table sequence). For any query
-- that filtered/ordered by `id` within the health log this is still
-- monotonic per the `slurper-health` stream, so ordering semantics are
-- preserved; the absolute values differ. No production query relies on the
-- raw id range, so this is a safe change.

DROP TABLE IF EXISTS health_log;

CREATE OR REPLACE VIEW health_log AS
SELECT id, kind, payload, created_at
FROM events
WHERE stream = 'slurper-health';
