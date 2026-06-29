# Backlog

Things explicitly deferred for later. Closed items are removed (not
archived). "Closed pending image rollout" is not a backlog state —
roll the image and clear the entry.

---

## Open

### FUSE mount wedge — host-level condition

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

**Defenses shipped:**

1. **Recovery watchdog** (``scripts/watchdog/``): systemd timer-driven
   detection via ``/proc/<pid>/stat`` (never touches the FUSE path);
   threshold default 90s. Recovery follows the lesson_fuse_orphan_
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

---

### FUSE passthrough + dirty-set + coalesced disk projection

**Discovered**: 2026-06-29 while benchmarking ripgrep throughput. Live
mount serves ~18 files/sec (every file goes through FUSE round-trip);
the archive on disk serves ~135,000 files/sec. ~7,500× gap. Kernel-push
(`invalidate_inode`) doesn't help cold scans because each file is read
once — no kernel cache to hit. The architectural fix is FUSE
**passthrough**: Linux ≥ 6.9 + libfuse ≥ 3.16 (May/Sept 2024) let the
daemon delegate `read()` directly to a backing fd. Kernel reads from
disk; FUSE daemon is bypassed for the read path entirely.

**Stack check** (already done): kernel 7.1.1 ✓, libfuse 3.18.2 ✓,
pyfuse3 3.4.2 has NO passthrough bindings ✗. The Python binding gap
is the only real blocker.

**Why this is a natural fallout from v2 event-sourcing.** Without v2
you'd be solving a cache-coherence problem; with v2 you're just adding
a sink. v2 already gives:

- Deterministic renders (offset + chunk row → byte-identical output)
- Per-key chunking matching the on-disk layout
- Idempotent re-renders (atomic temp+rename is safe)
- Existing `InvalidationSink` plumbing (one extra line to add disk write)
- `stream_caught_up` + `applied_offset` for "is this chunk stable?"
- In-memory render LRU already exists in `fuse_ops_v2.py` — just move
  the storage backend to disk

**Three-component design** (sharper than "stale tolerance contract"):

```
event arrives ─→ InvalidationSink
                 - add path to dirty_set      (in-memory hash set)
                 - invalidate_inode           (drops kernel page cache)

coalescer ─────→ every stale_threshold:
                 for path in dirty_set:
                   render_to_disk_atomic     (temp + rename)
                   remove from set
                   invalidate_inode          (kernel re-asks → daemon
                                              now returns passthrough)

read(path) ────→ open():
                   if path in dirty_set:  no FOPEN_PASSTHROUGH; read()
                                          renders JIT (what v2 does today)
                   else:                  FOPEN_PASSTHROUGH + backing fd;
                                          kernel reads disk directly
```

**Why dirty-set is right over "stale tolerance" contract**: reads are
always *fresh*. JIT path returns bytes computed from latest
applied_offset; passthrough path only serves files byte-identical to
JIT output (the set is the proof). Stale threshold controls disk
write rate, NOT read freshness. Hot channels can fire 50 msgs/sec —
one disk write per stale_threshold per key, not per event.

**stale_threshold ladder** (mirrors v1's `_date_ttl` / `_thread_ttl`):

| Key | Threshold | Why |
|---|---|---|
| Today, busy or quiet channel feed | 1s | Feels live to humans |
| Today, active thread (< 1h) | 1s | Same |
| Today, idle thread (> 24h) | 30s | Long-tail; readers tolerate slack |
| Yesterday | ∞ until invalidate | Locked-in; only edits/deletes change it |
| Older than 7d | ∞ | Effectively immutable |

**Expected throughput gain**: ripgrep over live mount goes from ~18
files/sec to **~50,000-100,000 files/sec** cold (limited by
`getattr`/`lookup` round-trips, which the kernel attr-cache absorbs
after first pass), approaching the archive's ~135K/sec on warm runs.

**Subtle caveat to document**: `FOPEN_PASSTHROUGH` is per-open and
persists for the life of the fd. Long-held fds reading a path that
later transitions clean→dirty keep reading from the cached backing
fd until close. Same edge case as today's `keep_cache=True`: rg/cat/
modern editors re-open and self-correct; only processes holding fds
indefinitely see drift. Document, accept.

**Implementation plan** (four pieces, smaller than it sounds):

1. **Disk-backed render output**. Extend the existing render path so
   each render also atomically writes to a known on-disk location
   (mirror the FUSE tree under `~/.cache/slack-fuse/projection/` or
   similar). Pure I/O; ~30 lines.
2. **Dirty set in `fuse_ops_v2.py`**. Add `_dirty_paths: set[str]`
   plus the sink hook + coalescer task. ~50 lines + tests.
3. **`open()` passthrough decision**. Check dirty set; return with or
   without `FOPEN_PASSTHROUGH` + backing fd. Trivial once (4) lands.
4. **pyfuse3 passthrough binding gap**. Two options:
   - **Patch pyfuse3 upstream** — Cython bindings for
     `fuse_passthrough_open()` and `FOPEN_PASSTHROUGH`. ~150 LoC plus
     tests. Cleanest if upstream merges; pyfuse3 is barely maintained
     so a vendored fork is plausible.
   - **Small native extension** — ctypes/Cython wrapper that calls
     `fuse_passthrough_open()` and returns the backing-id to pyfuse3's
     `open()` callback. ~50 LoC of C plus a ctypes shim. More
     surgical, no upstream dependency.

   Lean toward option B (native extension) for the spike; option A
   later if it's worth upstreaming.

**Decision points before starting**:

- Where does the projection live on disk? Single tree mirroring FUSE
  layout, separate from archive? Same as archive but extended to today?
- Coalescer task: one per-process trio task walking the dirty set, or
  per-key timers? Per-process simpler.
- Disk overhead: archive is 572MB today; projection covering today
  too is probably 1-1.5GB. Trivial.
- Crash recovery: on startup, treat all paths as dirty until the
  projector has caught up. Re-flush on the first coalescer tick.

**Why not now**: backfill ingest still has ~2h remaining; pyfuse3
binding work benefits from a focused session not interleaved with
other work; current performance is workable (archive for broad scans,
live mount for "what did Alice say" interactive queries). Big win
when done; nothing blocked on it.

**Suggested order**: spike (4) first (the binding) — that's the only
unknown. Once a 20-line test program proves passthrough works
end-to-end on this stack, (1)-(3) drop in mechanically.

---

### Skip thread-expansion when local thread is already caught up

**Discovered**: 2026-06-30 watching proj-cloud's backfill spend ~9 hours
in the thread-expansion phase writing rows that all dedup'd to no-ops.
proj-cloud's history pagination finished by 17:57; the next 8+ hours
were `conversations.replies` calls per thread parent, paying the 2-8s
throttle per call, hitting the dedup index, inserting zero new rows.
Socket-mode events had already filled the threads in.

**Optimization**: in `backfill_channel`'s thread-expansion loop (in
`slack_fuse_server/backfill/api.py`, where `_expand_threads` walks
`thread_parents`), check local state before calling
`conversations.replies`. Skip the fetch when both hold:

1. We have a `message` event with `ts == parent.latest_reply`
   (newest reply timestamp from the parent's payload), AND
2. Count of locally-stored events with `thread_ts == parent.ts` equals
   `parent.reply_count` (including the parent itself, or off-by-one
   per Slack's convention — verify against `tests/_fake_slack/`).

If either fails, fetch normally.

**Why it's safe**:
- False negative (we fetch unnecessarily) is the worst-case if our
  cached `reply_count` is stale; that's the status quo.
- False positive (we skip a real gap) requires our local thread to
  have the exact `latest_reply` ts but be missing intermediate
  replies — Slack doesn't insert replies in the middle of a thread's
  timeline retroactively, so this shape doesn't occur in practice.
- The dedup index remains the correctness backstop: if we somehow
  miss a reply, the next backfill re-walk catches it (no
  `latest_reply` match → fetch).

**Wins**:
- Hours of API budget reclaimed on busy channels where socket-mode
  already filled the threads.
- Frees Tier 3 quota for actually-new work.
- Auto-backfill cycle completes faster after restart — when combined
  with skip-completed-channels (Wave 1 D), the only API work that
  actually fires is for genuinely new gaps.

**Where to wire the check**: emit a `slurper.backfill.thread_skip`
span when the skip fires, so Loki can confirm in production how many
threads the optimization actually saves. Per-channel run that wastes
4500 thread fetches × 5s avg = 6+ hours saved on a single big channel.

**Edge case**: huddle channels' transcript "threads" are a separate
flow (`transcript.py`); this optimization doesn't apply. The
predicate is on `reply_count` which the huddle path doesn't touch.

**Distinct from**: skip-completed-channels (Wave 1 D), which avoids
re-walking ENTIRE channels. This is a finer-grained per-thread skip
within a channel that IS being walked.

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

### Probe-event pattern — channel message counts + wider pattern

**Discovered**: 2026-06-28, post Wave 2 deploy. Triggered by the
question "what's the % progress of the backfill?" — we have no
authoritative denominator until a channel is fully backfilled.

**Specific item**: add a `channel_message_count_probed` event kind.
Slurper periodically calls `search.messages?query=in:<channel>` and
emits one event per channel per period with the total count from the
API. Tier 2 (`search.messages`: 60/min). Lets `/livez` (or a new
endpoint) compute "% complete" as `sum(events_written from
backfill_completed) / sum(latest probed count)`. Cheap to implement
once the pattern shape is decided.

**Wider pattern to think through** before building the specific item.
Today our event kinds split cleanly into two shapes:

- **Push-driven** (Socket Mode): `message`, `channel_added`,
  `user_added`, `member_joined_channel`, `reaction_added`, etc.
- **Diff-driven refreshes** (`channel_info_refreshed`): fire ONLY when
  a periodic `conversations.info` sweep detects payload drift.

A **probe event** is a third shape: slurper-initiated, periodic,
captures authoritative Slack API state regardless of drift, immutable
in the events log. The latest probe wins; older ones are history.

Candidate probes — picked specifically because Slack EITHER lacks a
push event for them OR we don't subscribe today. (Thread replies are
covered by existing `message.*` subscriptions regardless of the
parent's age, so they don't fit this pattern.)

1. **`channel_message_count_probed`** — the asked-for one. Backfill %
   visibility. Tier 2.
2. **`channel_pin_count_probed`** — `pin_added`/`pin_removed` socket
   events exist but we don't subscribe. `pins.list` is cheap.
3. **`workspace_emoji_probed`** — `emoji.list` for custom emoji.
   `emoji_changed` socket event exists but we don't subscribe.
   Useful for rendering markdown output.
4. **`channel_bookmark_probed`** — no socket event exists. Some teams
   use bookmarks as canvas pointers.

Design points to settle BEFORE writing any of them:

- **One probe-sweep task or per-probe tasks?** One sweep is simpler
  (one supervisor entry, one limiter; the sweep walks a registry of
  probe kinds with their own intervals). Per-probe scales the nursery
  + supervisor surface unnecessarily.
- **TTL + cadence per kind.** `channel_message_count` could refresh
  every 6h; `workspace_emoji` daily; `pins` weekly. Make this part of
  ServerConfig.
- **Tier budget accounting.** `search.messages` (Tier 2, 60/min) for
  N channels at interval T must respect the ceiling. Bake into the
  sweep.
- **Failure handling.** API failure = no event written. Last probe
  stays as truth. Consumers shouldn't assume any cadence.
- **Spans wrap probes.** Each probe emits `slurper.probe.<kind>`
  spans for cost visibility — natural follow-on from Wave 2.C.
- **Distinct from refreshes.** `channel_info_refreshed` fires on
  diff; probes fire on period regardless. Two different consumers;
  don't piggyback.

**Recommendation**: spec the probe-event shape (one sweep task,
registry of probe kinds, per-kind TTL via config) as one design pass,
then implement the first probe (`channel_message_count_probed`) as
the proof. Other probes drop in cheaply afterward.

---

### Clean up repo

**Discovered**: 2026-06-28.

Stale paths that should be deleted from main:

- **`slack_fuse_poc_b/`** — POC B for the renderer-split byte-equivalence proof from June 9. The split shipped in `slack_fuse_render/`; the POC is leftover scratch (24K, two files + __pycache__).
- **`.wt/synap5e/poc/a-events-to-postgres`** + **`.wt/synap5e/poc/b-renderer-split`** worktrees — early development POC branches still listed in `git worktree list`. Check for unmerged history before removing.
- Any other `poc_*` / `sprint*` worktree branches that were created during early development and have since shipped or been abandoned.
