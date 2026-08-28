# slack_fuse/projector — subscriber, applier, ledger, coalescer

Everything between "a frame arrived on the wire" and "bytes are legal to serve". 29 modules, ~5.4k LOC. This is where the correctness lives.

## Pipeline

```
ws_client.WSClient          one socket, capability handshake, per-stream subscribe
  └ per_stream.StreamApplier  one queue + one worker per stream, serial apply
      └ apply.apply_event      ONE transaction: rows + mentions + cursor + ledger bump
          └ projection_ledger.bump_targets
coalescer.run_coalescer     5s tick → DiskProjection.{reconcile_startup,reconcile_layout,discover_pending,flush_dirty}
  └ disk_projection.DiskProjection  render → atomic replace → invalidate → CAS mark clean
```

Out-of-band writers that reuse the same ledger contract: `snapshot_fetch` (destructive full replace), `rerender` (upsert-only), `block_sync` (visibility).

## WHERE TO LOOK

| Task | File |
|---|---|
| Event kind renders wrong / missing | `apply.py::_dispatch` (dispatch per event kind) |
| File shows stale bytes | `projection_ledger.py::is_target_clean`, `disk_projection.py::_flush_target` |
| File never appears | `disk_projection.py::discover_pending`, ledger row `target_generation` vs `rendered_generation` |
| Reconnect / resubscribe bug | `ws_client.py::reconcile_subscriptions`, `SubscriptionState` |
| Whole channel wrong after gap | `snapshot_fetch.py::fetch_and_apply_snapshot` |
| "Content may be stale" wrong | `trailer.py::classify_trailer` + `health_subscriber.py::read_signature` |
| PG bounced, mount misbehaved | `reconnecting_conn.py` |
| Ghost file (`_gaps`, `_probes`, `channels.md`) empty | the `*_warmer.py` pair, not the FUSE layer |

## Transaction invariants (breaking these is silent corruption)

- **One event = one transaction.** Data rows, `chunk_mentions`, cursor advance and ledger bump commit together (`apply.py:1-22,156-170`). Commit is the linearization point for validity.
- **Projector connections must be autocommit** (`apply.py:127-140`). Without it `conn.transaction()` opens a savepoint that vanishes when the socket dies — writes silently disappear.
- **`SELECT tier ... FOR UPDATE` is the first statement in apply** (`apply.py:260`). Serialises live apply against block/unblock.
- **Mention lookups stay inside the writer's transaction** (`apply.py:657-662,817-820`). A separate READ COMMITTED lookup misses the uncommitted message.
- **Lock order is chunks-then-cursor.** `snapshot_fetch.py:258-268` creates the cursor row before `FOR UPDATE` to preserve it.
- **Never retry inside a transaction** (`reconnecting_conn.py:98-105,496-498`). COMMIT transport failures are ambiguous → `commit_outcome=unknown`, never retried.
- **No HTTP or file I/O while holding a row lock** (`projection_ledger.py:235-239`).

## Ledger protocol

- Identity is `TargetKey(target_kind, channel_id, local_day, thread_ts)` — stable, resolved to a mutable path only immediately before writing (`disk_projection.py:467-496`).
- Render → `_atomic_write_bytes` → **invalidate kernel** → `mark_target_rendered` CAS on `target_generation`. Invalidation before CAS, always: a mid-read that started on old bytes must never see them marked clean.
- A bump during render makes the CAS miss → key requeued, intermediate bytes never published.
- Reader gate is dual: the target row **and** the singleton `layout` row must both be clean at the current `RENDERER_VERSION` (`projection_ledger.py:176-179`) — closes the slug-reassignment window.
- Generations are monotonic; first mutation starts at generation 2.
- Failed invalidation keeps the row pending (`disk_projection.py:85-96`); only ENOENT/EBADF are benign.

## ANTI-PATTERNS

- Calling `pyfuse3.invalidate_inode` from the trio loop. Dispatch to a worker thread (`per_stream.py:279-283`, `health_subscriber.py:183-188`). Deadlocks against in-flight reads.
- Making `snapshot_fetch` additive. It is an authoritative full-state replacement and **must** delete absent rows, empty snapshot included (`snapshot_fetch.py:149-153`).
- Making `rerender` destructive. It is deliberately upsert-only, no delete-absent, no cursor advance (`rerender.py:23-49`) so it cannot race live apply.
- Making the per-stream queue bounded, or `send` instead of `send_nowait` (`per_stream.py:151`). One slow stream would stall all WS input.
- Advancing the cursor past a failed offset (`per_stream.py:250-251`). Poison the stream and replay from the durable cursor instead.
- Deleting the `# pyright: ignore[reportPrivateUsage]` on `block_sync.py:13` / `block_fetch.py:14` / `probe_fetch.py:13` — the cross-module private reuse is deliberate.
- Assuming two coalescers can run. `_inflight` models a single owner (`disk_projection.py:90-96`).
- Deriving the health signature from raw timestamps instead of `frame_stale` (`health_subscriber.py:1-35`) — thrashes the kernel cache during healthy operation.

## Naming traps

`block_fetch.py` and `gaps_fetch.py` also POST (mutating). `block_sync.py` also reconciles WS subscriptions. `health_subscriber.py` polls local PG, it does not subscribe to anything. `coalescer.py` is only the trio lifecycle wrapper — logic is in `DiskProjection`.
