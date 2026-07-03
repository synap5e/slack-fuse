-- 0010_thread_parent_hint_idx — partial indexes for the resume-plan thread worklist.
--
-- `find_resume_plan` needs "every thread parent this stream has seen" without
-- paying the `active_thread_parents` fold (DISTINCT ON over unioned CTEs plus
-- two anti-joins), which blows the 30s session statement_timeout cold-cache at
-- production scale (~250K events; the 2026-07-03 CrashLoop). These indexes
-- contain only rows that carry a `reply_count` key — thread parents are a tiny
-- fraction of a channel's events — so the direct worklist query in
-- `backfill/resume.py` reads a few thousand index entries instead of folding
-- the whole stream.
--
-- Predicates use only `?` / `->` (no casts): index predicates are evaluated on
-- every INSERT, and a cast like `(payload->>'reply_count')::int` would make a
-- malformed payload abort ingestion writes. The `> 0` filter stays query-side.

CREATE INDEX events_message_parent_hint_idx
    ON events (stream, (payload->>'ts'))
    WHERE kind = 'message' AND payload ? 'reply_count';

CREATE INDEX events_changed_parent_hint_idx
    ON events (stream, ((payload->'message')->>'ts'))
    WHERE kind = 'message_changed' AND payload->'message' ? 'reply_count';
