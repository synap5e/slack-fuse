# scripts — operator tooling, some of it destructive

Not part of the shipped packages. These run against **real** services: the live k8s cluster, the live mount, the live systemd units.

## Inventory

| Path | What it does | Safety |
|---|---|---|
| `break-test/break_test.py` | Five sequential failure scenarios against `slack-fuse-split.service`, `local-postgres.service` and `/views/slack-split`: idle PG stop, PG stop under FUSE traffic, three rapid PG flaps, clean SIGTERM/restart, daemon SIGKILL/respawn. Journals + probe logs to `/tmp/slack-fuse-break/<run-id>/`, `summary.json` at the end. | **DESTRUCTIVE.** Stops your database and your mount. |
| `compare-mounts.py` | Per-channel coverage comparison: cluster events vs v2 projection vs v1 legacy cache. | Read-only, enforced (`:17-18`). |
| `k8s/backfill-job.sh` | One-channel backfill Job, optionally watched. Defaults to legacy cache; `--source slack-api` available. | Writes to cluster. |
| `k8s/bulk-backfill.sh` | Spaced backfill Jobs across cached channels; skips blocked/already-ingested unless forced. Gap-fill mode uses `--since`. | Writes to cluster, high API spend. |
| `k8s/channel-volume.sh` | Event volume by channel/week. | Read-only. |
| `k8s/message-history.sh` | Edit/delete history or one message's full timeline. | Read-only. |
| `watchdog/slack-fuse-watchdog.{sh,service,timer}` | Every 30s: detect daemon D-state **or** nonzero `/sys/fs/fuse/connections/<id>/waiting`, then sysfs abort → lazy unmount → systemd restart. | Recovers a wedge by force. |
| `../tools/debug_subscribe.py` | Trio WS client that prints server frames. | Read-only. |

`k8s/README.md` and `watchdog/README.md` carry the real operating detail.

## Rules

- **`break_test.py` is not a pytest test and must never be run in CI, in a handoff, or "to check something".** It only runs when a human has asked for it, on a host they are watching. It hard-codes the retired `-split` unit and mountpoint names, so it is stale as well as dangerous — read it before trusting it.
- **The watchdog watches two oracles, not one.** Daemon D-state alone missed an overnight wedge where the process sat in `epoll_wait` with 7 queued kernel requests. Do not simplify it back to a process-state check.
- **Never read from a wedged mount to diagnose it.** `cat`/`ls`/`bat`/`rg` against a wedged FUSE mount enters uninterruptible sleep and takes your shell with it. Use `/sys/fs/fuse/connections/<id>/waiting` and `/proc/<pid>/stack`.
- **Don't restart a dependency service you don't own** (local Postgres, the cluster). Escalate. Cluster node problems go to the homelab owner, not to you.
- Bulk backfill costs real Slack API budget and shares the tier-2 pacer with the running server. Check `_control/status` and pod health before firing one.

## Note

Several scripts predate the v1 island deletion and the `/views/slack-split` → `/views/slack` rename. Verify unit and path names against the current host before running anything that writes.
