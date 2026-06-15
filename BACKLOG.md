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

---

### Slurper-side `channels` table is never populated

**Discovered**: 2026-06-15 while building `scripts/k8s/channel-volume.sh`.

**Symptom**: `slack_fuse_server/schema.sql` defines a `channels` table
intended as "Workspace inventory. Mirrored from Slack via events into a
queryable materialization for fast channel-list answers." The table exists
on the cluster but contains 0 rows. All channel metadata lives only in the
`channel_added` / `channel_renamed` event payloads on the `channel-list`
stream; consumers needing names must derive them via JSON queries.

**Current behaviour**: tools that want channel names have to do
`SELECT DISTINCT ON (payload->>'id') ... FROM events WHERE stream='channel-list' ORDER BY id DESC`
instead of a clean join. Works but ugly and slower as event count grows.

**Fix**: have the slurper UPSERT into `channels` whenever it emits a
`channel_added` / `channel_renamed` / `channel_archived` / `channel_member_changed`
event, mirroring how the client-side projector maintains its `channels`
table. ~30 lines of code, no schema change needed.

---

### Live-events gap between legacy-cache cutoff and Socket Mode start

**Discovered**: 2026-06-15 during projection-coverage check.

**Symptom**: Per-channel "newest cached day" in the operator's legacy cache
ranges from 2026-05-08 to 2026-06-09; cluster Socket Mode first delivered
events at 2026-06-14 22:31 (after Event Subscriptions were enabled in the
Slack app config). Messages posted in the gap window — up to ~5 weeks for
some channels — are missing from both sources.

**Fix**: expose `--since TS` on `scripts/k8s/backfill-job.sh` (both
backfillers already accept `since_ts`), then per-channel:

```
since = (SELECT max(ts::numeric) FROM events WHERE stream='channel:CXXX' AND kind='message')
```

Run the gap-fill via `--source slack-api --since <since>`. The
`events_message_dedup` partial unique index makes it idempotent — any
overlap with the existing backfilled range gets dropped at insert.

**Workflow**: use `channel-volume.sh` first to identify firehose channels
to add to `always_blocked_channel_ids`, then run the gap-fill on the
remainder.

---

### ~~Projector stalls indefinitely when WS connection drops mid-run~~ (fixed `758274b`)

**Discovered**: 2026-06-15 while measuring projection rate post-gap-fill.

**Symptom**: Apply rate observed to be exactly zero (Δ0 chunks, Δ0
applied_offset in 30s) despite the local service being `active` per
systemd and the cluster having ~32k unapplied events ready to deliver.

Logs revealed two stuck loops running side-by-side for ~2 hours:

1. `slack_fuse.projector.health_subscriber: signature read failed: the
   connection is closed` — fired **once per second**, indicating the
   health subscriber task is hot-spinning on a closed connection
   without reopening it.
2. `slack_fuse.__main__: projector exited (the connection is closed);
   reconnecting in 300s` — fires every 5 minutes (the outer projector
   reconnect backoff), but each reconnect attempt fails immediately
   and triggers another 5-min wait.

The outer reconnect doesn't tear down + recreate the health
subscriber's connection state, so the subscriber remains stuck even
across reconnect attempts. Net effect: process alive, no work done.

**Workaround**: `systemctl --user restart slack-fuse-split.service`
clears it. Post-restart, apply rate is healthy (~7000 events/min in
the observed run).

**Fix candidates**:
1. Make the health subscriber abort + reraise on consecutive
   connection-closed reads instead of warn-and-retry. The outer
   reconnect would catch the failure and rebuild the whole projector
   task tree including the subscriber.
2. Tie the health subscriber's lifecycle explicitly to the WS connection
   lifetime (cancel on disconnect, restart on connect) so it can't
   outlive the connection it's reading from.

**Impact**: silent data divergence between cluster and client until the
operator notices. Worth fixing relatively soon; for now monitoring is
"is `applied_offset` advancing?"
