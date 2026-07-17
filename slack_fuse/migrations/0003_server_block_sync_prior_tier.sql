-- 0003_server_block_sync_prior_tier
--
-- FINDING-14 (2026-07-17 adversarial review): preserve a channel's local
-- (tier, tier_source) at the moment a server-side block first applies, so
-- the subsequent server-side unblock can restore it instead of resetting
-- to auto/default.
--
-- Pre-fix behavior loss cases:
--   * operator-pinned ``tier='hot'`` channel goes through a server
--     block+unblock cycle and comes out ``auto`` (pin lost);
--   * locally CLI-blocked (``tier='blocked', tier_source='manual'``)
--     channel that the server independently blocks then unblocks comes
--     out ``auto`` (local block silently removed).
--
-- The 0002 migration comment stated the table's purpose was to avoid
-- clobbering unrelated local tier CLI choices — this migration completes
-- that intent.

ALTER TABLE server_block_sync
    ADD COLUMN prior_tier TEXT,
    ADD COLUMN prior_tier_source TEXT;
