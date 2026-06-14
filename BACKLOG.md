# Backlog

Tracked issues that aren't blocking but should be revisited. Add new entries with a heading + date; link to commits or PRs when resolved.

---

## Open

### Backfill: `channel_not_found` on `conversations.info` aborts the Job

**Discovered**: 2026-06-15 during bulk legacy-cache backfill of ~380 channels.

**Symptom**: 3 distinct channels (~0.8% of legacy-cache contents) hard-failed with
`SlackAPIError: Slack API error on conversations.info: channel_not_found`
raised in `slack_fuse_server/slurper/channels.py:_ensure_channel_added_sync` →
`get_channel_info`. Affected channels are present in the local legacy cache
(`~/.cache/slack-fuse/messages/<id>/`) but no longer accessible to the user
token (channels I've left, group DMs that closed, etc.).

**Current behaviour**: Job hits BackoffLimit and dies; no events written for
that channel. The bulk loop continues; failure is silent unless an operator
inspects job state.

**Options**:
1. Catch `channel_not_found` in `_ensure_channel_added_sync`, log a warning,
   and skip the channel cleanly. Bulk loop reports the skip count at the end.
2. Synthesize a minimal `channel_added` event from the first message JSON in
   the cache (we know `channel_id`, and the cache may carry enough metadata
   to derive a name). Preserves the archived content even when access is gone.
3. Leave as-is and document via an operator query that lists channels with
   cache data but zero events on cluster.

**Recommendation**: option 1 (skip + log) is the cheapest correct behaviour;
option 2 is nice-to-have if archived history of left-channels matters.
