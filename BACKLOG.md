# Backlog

Tracked issues that aren't blocking but should be revisited. Add new entries with a heading + date; link to commits or PRs when resolved.

---

## Open

### ~~Backfill: `channel_not_found` on `conversations.info` aborts the Job~~ (fixed, pending image rollout)

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

### ~~Slurper-side `channels` table is never populated~~ (fixed: migration 0004 replaces with VIEW, pending image rollout)

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

**Fix (recommended)**: replace the empty table with a regular `VIEW`
backed by the events log:

```sql
DROP TABLE channels;
CREATE VIEW channels AS
  SELECT DISTINCT ON (payload->>'id')
    payload->>'id'         AS channel_id,
    payload->>'name'       AS name,
    (payload->>'is_im')::boolean    AS is_im,
    (payload->>'is_mpim')::boolean  AS is_mpim,
    (payload->>'is_member')::boolean AS is_member,
    -- … other columns …
  FROM events
  WHERE stream = 'channel-list'
    AND kind IN ('channel_added', 'channel_renamed', 'channel_archived', 'channel_unarchived')
    AND payload ? 'id'
  ORDER BY payload->>'id', id DESC;
```

The view is always fresh, has zero dual-write risk, and tooling can
`JOIN channels USING (channel_id)` like a normal table.

**Alternatives considered**:
- *UPSERT into the table from the slurper write path* — rejected: that's a
  dual-write (event + side-effect mutation in the same module). Same
  failure modes as any non-atomic write pair.
- *Separate server-side projector task* (mirroring the client projector):
  architecturally correct, more code; promote to this if `channels` ever
  needs to be on a hot read path.
- *Materialized view with scheduled REFRESH*: pragmatic if the VIEW gets
  too slow, but for ~400 channels the live query is fast.

---

### ~~Live-events gap between legacy-cache cutoff and Socket Mode start~~ (fixed `f6119a8` + bulk-backfill run)

**Discovered**: 2026-06-15 during projection-coverage check.

**Symptom**: Per-channel "newest cached day" in the operator's legacy cache
ranges from 2026-05-08 to 2026-06-09; cluster Socket Mode first delivered
events at 2026-06-14 22:31 (after Event Subscriptions were enabled in the
Slack app config). Messages posted in the gap window — up to ~5 weeks for
some channels — were missing from both sources.

**Fix landed**: `--since EPOCH` wired through the slurper CLI, plus a
`--gap-fill` mode on `scripts/k8s/bulk-backfill.sh` that computes the
per-channel `since` from cluster `max(ts)` and runs slack-api backfills.
Bug along the way: the slurper was only filtering yielded messages and
not passing `oldest=` to Slack, so a 1-day gap against a 2-year channel
would repaginate the whole history; fixed in `f6119a8`.

**Backfill result**: 22,264 messages caught across 352 channels (~0.8%
of channels failed with `channel_not_found` — same root cause as item 1).

**Forward operational practice**: any new gap (slurper outage, network
blip wider than Delayed Events covers) → re-run `scripts/k8s/bulk-backfill.sh
--gap-fill`. The `events_message_dedup` index makes re-runs idempotent.

---

### ES audit: non-event mutations to projection state

**Discovered**: 2026-06-15 audit after recognising the `channels`-UPSERT
proposal was a dual-write anti-pattern.

Three places mutate projection / materialization state outside the event
log. None are silent-divergence risks today (each has rationale or scope
limits), but they shape badly if the system ever needs full replay or
multi-consumer projections.

**3a. `slack-fuse tier` CLI directly UPDATEs `channels`** (`slack_fuse/cli/tier.py:195, 266`)

`set_channel_tier` and `reset_channel_tier_to_auto` mutate the projection
without emitting an event. Replaying events to rebuild the projection
would lose every manual tier override. Same shape as the ad-hoc raw psql
`UPDATE` an operator might run (and did, in this session, to flip two
firehose channels to blocked).

**Cleanest fix**: introduce a `client-overrides` stream and event types
like `manual_tier_set { channel_id, tier }`. The CLI emits an event;
`apply_event` handles it like any other. Replay works.

**3b. ~~`slurper/health.py` dual-writes event + `health_log` in one TX~~**
(fixed: migration 0005 drops the table + creates a VIEW, dual-write
removed from `slurper/health.py`, pending image rollout)

**3c. ~~The empty cluster `channels` table~~** — fixed alongside 3b in
migrations 0004/0005.

**Why none of these are P0**: the projections work correctly today; the
audit is forward-looking. Real concern is the day someone tries to
rebuild projections from the event log and silently loses operator-set
state. Either move overrides into events (3a) or be explicit they're
external policy (like `always_blocked_channel_ids`).

---

### FUSE mount wedge — architectural fix landed, host-level condition still seen

**Status**: architectural fix landed (`87487d0`). Per-callback connection
pool + 30s trio timeout + 25s PG ``statement_timeout``. Concurrent
callbacks no longer serialize behind one limiter slot; a slow SQL aborts
at the PG layer and surfaces as ``FUSEError(EIO)``; a pure-Python hang
times out at the trio layer with the same result. 4 regression tests
pin the new behaviour.

**Smoke test after deploy revealed a separate host-level issue.** With
the fix running, two concurrent ``cat`` operations on different
``channel.md`` files still wedged the mount: process went D-state on
``folio_wait_bit_common`` (same wchan as the original report), but
``VmSwap=0`` on the fresh process and the disk was idle — *not* a swap
stall. Scanning the wider system found:

- A `claude` process D-state on FUSE wchan for **2 days**
- A `bat` process D-state on FUSE wchan for ~2.5 hours
- Other long-lived D-states from before today's session

So the host has an accumulating supply of FUSE-wedged processes
unrelated to slack-fuse's architecture — possibly a kernel bug, a
specific FUSE/pyfuse3 race, or a system condition (memory allocator
pressure, cgroup, or zram interaction — system has 27 GB used in zram
swap with 14-day uptime).

**Reproduction shape on the new code**: open two concurrent reads on
different channel.md files; slack-fuse goes D-state on
``folio_wait_bit_common``; the cats D-state on ``fuse_s``. Recovery
needs ``fusermount3 -uz`` + service restart.

**Root cause (diagnosed via ``agent-sudo`` + ``/proc/<pid>/stack``)**:
the wedged slack-fuse daemons have IDENTICAL kernel stacks:

```
folio_wait_bit_common
__filemap_get_folio_mpol.cold
fuse_dev_do_write
fuse_dev_write
do_iter_readv_writev
do_writev
```

This is the kernel-side of ``write(fuse_dev_fd, response, ...)``. The
daemon's Python code finished computing the response and called write
to send it back; the kernel can't allocate a folio to receive it and
sleeps on the folio's bit. Every downstream FUSE client (``cat``,
``head``, ``bat``, ``claude``, ``stat``) is correctly waiting on
``fuse_simple_request`` for that response. The wedge starts at the
daemon's write path, not in slack-fuse Python.

This is reproduced for non-slack-fuse FUSE workloads on the same host
(``claude`` D-state on FUSE for 2 days; ``bat`` for 2.5 hours). The
condition is host-level, not slack-fuse-specific.

**Trigger identified (2026-06-22) — `game-mode on` tears down
backing services that slack-fuse-split is in-flight against.**
Evidence (none of it my inference; all grounded in operator's own
documented experience):

- ``/home/simon/bin/game-mode`` ``cmd_on`` comment, verbatim:
  *"a session that's holding a FUSE handle or a postgres connection
  when its backing service stops can end up in
  ``__fuse_simple_request`` / connection-reset wedges that only
  SIGKILL-after-FUSE-abort recovers from (see
  lesson_fuse_orphan_recovery.md). Freezing first puts sessions in
  cgroup-v2 TASK_FROZEN — they can't issue new requests, can't be
  wedged."*
- ``GAME_MODE_STOP_SERVICES`` includes ``claude-hooks-postgres.service``,
  the local Postgres backing slack-fuse-split's projector. Its lesson
  comment cites: *"stopping claude-hooks-postgres cascaded to
  claude-hooksd → claude-session-fuse → /views/claude-sessions became
  a stale mount."*
- ``lesson_fuse_orphan_recovery.md`` documents the kernel stack we
  observed (``fuse_s``, ``__fuse_simple_request``) and prescribes
  ``echo 1 > /sys/fs/fuse/connections/<id>/abort`` as the only
  always-works recovery primitive.
- slack-fuse-split runs as a user systemd unit in ``app.slice``, NOT
  in tmux. ``game-mode --freeze`` only freezes tmux sessions
  (``session-freeze``), so slack-fuse-split stays runnable while its
  PG socket disappears. It's the workflow event, not any system-wide
  config, that triggers the wedge.
- Timing: client crash logs show ``psycopg.OperationalError:
  connection is bad: connection to server on socket
  /run/user/1000/local-postgres/...`` exactly matching game-mode-on
  events.

(Earlier passes wrongly blamed ``vm.swappiness=150`` and the
``vm.dirty_ratio=0`` readings. Both were retracted under pressure;
neither was mechanism-grounded.)

**No operator mitigation here** until the actual trigger is named.
Restarting an individual daemon clears its own wedge but doesn't
prevent recurrence; the other 6 FUSE mounts continue to wedge.

**Defenses shipped:**

1. **Recovery watchdog** (``scripts/watchdog/``): systemd timer-driven
   detection via ``/proc/<pid>/stat`` (never touches the FUSE path);
   threshold default 90s. Recovery now follows the lesson_fuse_orphan_
   recovery sequence: ``echo 1 > /sys/fs/fuse/connections/<id>/abort``
   (the only always-works primitive — pure sysfs write, no FUSE
   traffic) → ``fusermount3 -uz`` → ``systemctl restart``. Live-verified
   against the 6h53m wedge on 2026-06-21: full recovery in under 5s,
   projection state preserved. Bounds impact to ~120s worst case.

**Prevention not yet implemented** — add
``slack-fuse-split.service`` to ``game-mode``'s
``GAME_MODE_STOP_SERVICES`` so it gets cleanly stopped before
``claude-hooks-postgres.service`` is torn down, then restarted in
``cmd_off``. Mirrors the protection tmux sessions get via ``--freeze``.
This is operator-side (their ``/home/simon/bin/game-mode``), not a
slack-fuse code change. Left as a follow-up for the operator.

**Status of the slack-fuse architectural fix** (``87487d0``): still
correct and worth keeping — removes the v1-derived
``CapacityLimiter(1)`` bottleneck, gives concurrent FUSE callbacks
distinct postgres connections, surfaces SQL timeouts as
``FUSEError(EIO)`` instead of indefinite stalls. Independently of the
host vm config, this prevents one slow query from queueing every
subsequent FUSE upcall — a real concurrency win the host has to be
healthy enough to deliver.

The architectural fix lives at ``87487d0`` + ``f817a35``. This entry
stays open as "host-level FUSE wedge" until the operator applies the
``vm.dirty_*`` mitigation, since slack-fuse cannot fix it from
userspace.

**Discovered**: 2026-06-15. After a cluster rollout + DM backfill landed a
burst of projector writes, `cat /views/slack-split/dms/luke/channel.md`
went into D-state and never returned. A follow-up `head` on the same
file also D-stated on the same `folio_wait_bit_common` wchan. Mount was
unrecoverable without `fusermount3 -uz` + service restart.

**Root cause (architectural, not the trigger)**: `cmd_mount_split` in
`slack_fuse/__main__.py:323` creates a `CapacityLimiter(1)` and shares it
across every FUSE callback (`getattr`, `lookup`, `readdir`, `opendir`,
`open`, `read` in `fuse_ops_v2.py`). The limiter exists because all FUSE
work runs on a single shared `psycopg` connection (`fuse_conn`) which
isn't thread-safe. Capacity 1 = strictly serial. Any callback that
takes a long time (PG query stalled behind WAL fsyncs during heavy
projector writes, swap stall on the daemon process, etc.) holds the
slot; every subsequent FUSE upcall queues behind it. The kernel pages
allocated for queued reads stay locked → callers go D-state → mount
looks dead from outside.

A read of `channel.md` during heavy projector activity is exactly the
trigger shape that exposes this: render walks `chunks` + `thread_chunks`
+ `chunk_mentions`, all heavily contended by the projector's UPSERT
storm at that moment.

**Fix candidates** (ranked by invasiveness):

1. **Pool the FUSE-side PG connections**, drop the limiter (or raise it
   to match the pool). Each callback borrows a connection from a small
   pool (size 4–8), runs its read, returns it. One slow callback no
   longer blocks the others.
2. **Timeout the FUSE callbacks** (Slack convention: 30s+). Failing
   callbacks return EIO instead of D-stating the kernel forever.
   Defense-in-depth even after #1; without it, a *truly* stuck callback
   still wedges its own caller (just not the mount).
3. **Cap concurrent in-flight callbacks** to a number larger than 1 but
   smaller than infinity, so a stuck-callback storm can't exhaust the
   process. ~equivalent to #1 with pool size = cap.
4. **Move FUSE I/O to async psycopg** so a single conn can serve
   concurrent callbacks. Bigger change; only worth it if #1 isn't
   enough.

**Recommendation**: #1 + #2. Pool of 4 connections + 30s timeout. ~50
lines. Removes the structural fragility without rewriting the I/O model.

**Why this is the most important open BACKLOG item**: the other
findings are correctness/cleanliness with no user-visible failure mode
under normal operation. This one can take the mount down on a normal
read during normal load — and the recovery requires the operator to
know about `fusermount3 -uz`.

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


---

### Projector psycopg pool needs pre-ping (recycle dead handles across PG restarts)

**Discovered**: 2026-06-27 during workspace dump-and-reingest, after local
postgres restarted out-of-band (system reboot, OOM, anything that bounces
the PG process).

**Symptom**: the daemon's psycopg connection pool kept handing out
pre-restart connections to every consumer. `block-sync` task logged
`psycopg.OperationalError: the connection is lost` every 30s for
several minutes; FUSE callbacks that picked the same pool entry
returned `EIO`, so `ls` / `dust` hit "Input/output error (os error 5)"
on a fully-mounted, otherwise healthy daemon. Retrying a read often
landed on a different (good) pool entry and succeeded — making the
failure transient + confusing.

**Current behaviour**: pool returns dead handles until each consumer
hits the lost-connection exception, then surfaces it. No retry or
handle recycling at the pool layer. The user's FUSE read is what
notices the dead PG — not the projector framework.

**Workaround**: `systemctl --user restart slack-fuse-split.service`
flushes the pool and rebuilds connections against current PG.
Mount becomes clean immediately.

**Fix candidates**:
1. Enable psycopg's pool pre-ping semantics — issue a cheap `SELECT 1`
   before handing a pooled connection to a consumer, recycle on
   failure. Costs an extra round-trip per acquire.
2. Catch `OperationalError` at every pool consumer and explicitly
   discard + retry on a fresh connection (per-site, harder to keep
   complete coverage).
3. Make pool acquire honor a `validate=True` knob via a
   `setup`/`reset` hook that pings on lend.

**Recommendation**: option 1 — narrow change, single place, predictable
overhead. ~20 LoC enhancement in the pool wiring (look at
`slack_fuse/__main__.py` or wherever the projector pool is constructed).
Probably worth pairing with a small test using a stub PG that disconnects
mid-pool.

**Impact**: rare in normal operation (PG doesn't restart often), but the
failure mode is misleading enough to warrant fixing — the user thought
the mount was wedging when it was actually a stale-pool symptom.

---

### Workspace channel inventory view (`_workspace/channels.md`)

**Discovered**: 2026-06-27 during the dump-and-reingest while wanting
a real-time progress denominator. Slack's `search.messages` API exposes
a per-channel total message count (with `query=in:#<name>`, `count=1`,
read `messages.total`), giving authoritative size data we don't have
elsewhere.

**Symptom / motivation**: backfill progress, channel sizing for
manual-backfill decisions, block-list candidates, workspace overview
— all rely on knowing "how many messages does this channel have?"
Currently the only path is ad-hoc SQL + a one-off `search.messages`
sweep, which:

- requires kubectl exec into the slurper pod
- has no UI surface
- has no caching — every check pays Tier 2 rate budget
- doesn't expose non-joined channels' sizes (which we'd want before
  deciding whether to manually backfill them)

**Proposed shape**: `_workspace/channels.md` ghost file rendering a
per-channel inventory table:

| Name | Messages | Ingested | Status | Member | Created |

Status column maps `done` / `in_progress` / `blocked` / `not_started` /
`not_joined` / `unavailable`. Sorted by total messages desc.

**Server side**:
- New `channel_message_totals` table (channel_id PK, total BIGINT,
  refreshed_at TIMESTAMPTZ, refresh_status TEXT)
- Periodic refresh task (6h cadence) — Tier 2 throttle, 3.5s between
  calls, ~24 min per cycle for ~418 visible channels
- HTTP `GET /channel-stats` joining the totals + blocked_channels +
  latest channel-list payload + live events count
- CLI `slack-fuse-server refresh-channel-totals` for one-shot

**Client side**:
- `_workspace/channels.md` ghost file
- Background-warmed cache (same shape as `_workspace/gaps.md` warmer)
  so FUSE callbacks never block on server fetch
- Markdown renderer

**Architectural note**: the search-derived count is a fact about Slack
but it's *query-derived* (we asked, Slack told us), not pushed via
the events stream. It belongs in a refreshed table, not an event kind.
Same shape as `backfill_overrides` and `blocked_channels` — distinct
from both the events log (immutable upstream facts) and operator-policy
tables (mutable operator intent).

**Pitfalls** (for the eventual implementor):
- Search API requires user token, not bot token
- `is_im` channels can't be queried via `in:#<name>` — handle/skip
- Slack's total has approximation caveats above ~10K (mark
  `refresh_status='approximate'`)
- Don't truncate the totals table on refresh — preserve last-known
  on error so the view stays useful

**Estimated scope**: ~200-250 LoC + tests. Self-contained handoff;
prompt already drafted at
`/home/simon/.agent-handoff/2026-06-27/workspace-channels-view/prompt.md`
(queued for after the current backfill cycle settles).

**Impact**: changes the operational story from "ad-hoc SQL via kubectl"
to "cat the file". Reusable for every future "how big is X / how
complete are we" question.

---

### Trailer false-positives "server unreachable" on quiet streams

**Discovered**: 2026-06-27 during dump-and-reingest. User read
`/views/slack-split/channels/general/channel.md` (top-level metadata
view); rendered fine in 21ms but appended:

```
> ⚠ Content may be stale. Last successful sync: never. Reason: server unreachable.
```

**Symptom**: false positive. The mount + server + WS connection are
all healthy. `slurper-health` stream had a frame 1 minute ago; the
projector's cursors are advancing actively across many channels; the
HTTP server returns 200 in milliseconds.

**Root cause**: `slack_fuse/projector/trailer.py::staleness_reason`
uses **per-stream** ``last_frame_at`` as the WS-liveness signal:

```python
if state.last_frame_at is None or (now_real - state.last_frame_at) > timedelta(seconds=stale_after_s):
    if not caught_up:
        return "catching up after reconnect"
    return "server unreachable"
```

For a stream that's naturally quiet — `channel-list` when no channel
metadata is drifting, or a per-channel stream that's been backfilled
and has no live messages — no frames arrive for >5min and the trailer
fires. The threshold's intent was "WS disconnect", but it conflates
"no traffic on this stream" with "no connectivity".

**Current behaviour**: every read of a top-level `channel.md` (or any
read whose freshness derives from `channel-list`) on a stable
workspace appends a misleading "server unreachable" trailer with
"Last successful sync: never". Users see the warning and reasonably
conclude the daemon is broken.

**Fix candidates**:
1. **Use workspace-wide liveness signal**: track the last frame across
   ANY stream and use that as the WS-disconnect proxy. `slurper-health`
   is the natural heartbeat — it emits regularly for various reasons.
2. **Explicit WS-state tracking**: tie staleness to the actual WS
   connection state (e.g. `connection_state` table or socket
   reconnect events) instead of inferring from data flow.
3. **Per-stream threshold tuning**: bump `stale_after_s` for streams
   that are known to be quiet (channel-list, users). Brittle.

**Recommendation**: option 1. Cheapest fix, doesn't change the
overall trailer architecture, and aligns with what `slurper-health`
was designed for. The current `last_frame_at` parameter shape stays;
it just gets sourced from a workspace-wide MAX instead of the
queried stream.

**Impact**: ergonomic — operators see the warning and don't know if
it's real or noise. Doesn't affect data correctness, but every
warning the user has to mentally filter erodes trust in the trailer.

---

### FUSE getattr returns `st_blocks=0` — du/dust show everything as 0B

**Discovered**: 2026-06-27 while inspecting `/views/slack-split/channels/general` with `dust`.

**Symptom**: every file in the mount shows up as 0B in `dust` and
default-mode `du`, even though `stat` returns the correct `Size` and
`cat`/`wc -c` return the real bytes.

```
$ stat -c "size=%s blocks=%b" channel.md
size=4035 blocks=0

$ dust .                           # 0B everywhere
$ dust --apparent-size .           # real sizes
$ du -b channel.md                 # 4035
```

**Root cause**: `_make_file_attr` (and friends in
`slack_fuse/fuse_ops_v2.py`) set `st_size` correctly but leave
`st_blocks` at 0. There's no real disk block allocation behind these
files — content is rendered on read from the projector's `chunks` /
`thread_chunks` tables — so 0 is technically accurate. But `du`/`dust`
default to `st_blocks * 512` as "disk usage", which produces zeros
across the board.

**Current behaviour**: users have to know to pass `--apparent-size`
(or `du -b`, or `du --apparent-size`) to get usable output. First-time
users find it confusing — "the mount is empty?"

**Fix candidates**:
1. **Set `st_blocks = ceil(st_size / 512)`** in `_make_file_attr` (and
   the originals + control-surface attr factories). 5-line change,
   purely additive, no test risk. Every disk-usage tool Just Works.
2. **Document the workaround in `README.md` / `CLAUDE.md`** — tell
   users to pass `--apparent-size`. Cheaper, less ergonomic.

**Recommendation**: option 1. It's tiny, the value is genuinely
meaningful (`ceil(st_size / 512)` is what a tmpfs / overlayfs returns
for content of size N — same shape), and disk-usage tooling is a
common enough operation that "works by default" is the right default.

**Impact**: ergonomic only. Doesn't affect correctness or data. Worth
fixing because the alternative is "every user discovers it the first
time they run `dust` and gets confused."

---

### Slurper auto-backfill task silently hangs for hours

**Discovered**: 2026-06-28 during the dump-and-reingest. Slurper made
its last log line at `2026-06-28 00:17:13 UTC` (a `backfill_progress`
event for gtm at 14,000 messages), then went silent for ~12 hours.

**Symptom**: no logs, no API calls, no events written. Yet:

- pod was `Ready=True`, no restart
- `/health` returned 200 in 4ms
- Socket Mode still appeared connected (no disconnect events)
- Cluster PG was fine (uptime since 2026-06-14, 0 restarts)
- No `429` / rate-limited / fatal-error events in the logs

Only signal that anything was wrong: the **kubelet readiness probe
failed at ~48 min before discovery** with `context deadline
exceeded`, but the pod recovered to Ready before the kubelet acted on
it. So even k8s-level autoremediation didn't catch it.

**Workaround**: `kubectl rollout restart deploy/slack-fuse-server`.
Slurper rebuilds, refresh sweep + catchup + auto-backfill resume.
Already-completed channels dedup; the in-flight channel starts over
but its already-written events stay (dedup again).

**Root cause**: unknown. The shape of the freeze:

- Last successful step: `backfill_progress` health event (PG write)
- Next expected step: thread-replies pagination resumed (next
  `conversations.replies` call), or transition to next channel
- Neither happened

Possible culprits:
1. **Trio nursery deadlock** — some interaction between the auto-
   backfill task, the WS server, Socket Mode runner, or the snapshot
   scheduler that produces a circular wait. Hard to spot without a
   live thread-dump.
2. **httpx connection pool exhaustion** — if all pool slots are
   held by another task and the backfiller waits forever for a new
   one. The `SlackClient` uses a single shared `httpx.Client`; pool
   defaults are 100 connections, but if requests aren't returning,
   slots stay held.
3. **Socket Mode WS task starved out trio scheduling** — if some
   task hits a tight loop without yields, lower-priority tasks
   never run. Less likely but worth checking.
4. **Slack API call hangs without timeout** — `conversations.replies`
   has `timeout=30.0` per the SlackClient constructor, but if a long
   sequence of redirects or keep-alive issues causes the underlying
   socket to stall, trio's `to_thread.run_sync` might wait.

**Fix candidates**:
1. **Add a watchdog**: a periodic trio task that compares "wall
   clock" to "last slurper-health event timestamp"; if no health
   event in N minutes, log a warning + `sys.exit(1)` to let k8s
   restart the pod. Crude but effective.
2. **Make the kubelet readiness probe stricter**: include a check
   that ensures backfill events are recent during a backfill cycle.
   k8s would auto-recover.
3. **Investigate root cause**: requires reproducing or attaching a
   debugger to a live wedged process — `py-spy dump --pid <PID>`
   would show what every thread is doing.

**Recommendation**: option 1 first (watchdog + sys.exit), in parallel
with whatever investigation lands. The wedge mode is the worst kind
— silent, no symptoms, productive-looking pod. Even an imperfect
heuristic is better than 12 hours of lost work next time.

**Impact**: cost the equivalent of ~12 channels' worth of backfill
during this cycle (~14 hours of pace). For an ongoing slurper that
should run for months, the failure mode is unacceptable as-is.
