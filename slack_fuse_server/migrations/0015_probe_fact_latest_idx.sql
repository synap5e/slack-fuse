-- 0015_probe_fact_latest_idx — restart-safe fact-probe cadence lookup.
--
-- Interpreted probes store fixed-width UTC observation timestamps in events.ts.
-- The partial predicate keeps this index tiny: ordinary message/event writes
-- pay no index maintenance, while MAX(ts) for a registered fact kind reads the
-- newest index boundary instead of scanning the append-only event history.

CREATE INDEX events_probe_fact_latest_idx
    ON events (kind, ts DESC NULLS LAST)
    WHERE kind IN ('channel_message_count_probed');
