# slack-fuse FUSE wedge watchdog

A small recovery defense for the host-level FUSE wedge documented in
`BACKLOG.md`. The daemon occasionally goes D-state in `fuse_dev_write
→ folio_wait_bit_common` while writing a response back to the kernel.
D-state ignores SIGKILL so systemd's normal `Restart=on-failure` can't
recover it on its own. **But `fusermount3 -uz` works regardless of
daemon state** — it operates on the kernel mount table — so the
recovery shape is:

1. Detect the daemon in D-state for longer than a threshold (read via
   `/proc/<pid>/stat`; never touch the FUSE path or the watchdog
   itself D-states).
2. `fusermount3 -uz /views/slack-split` — detaches the mount without
   waiting for in-flight ops.
3. `systemctl --user restart slack-fuse-split.service`. systemd starts a
   fresh instance with a new PID. The old wedged daemon stays D-state
   as an orphan in the same control group (systemd logs "Found
   left-over process … Ignoring") but takes no FUSE traffic and is
   harmless.

Live-tested 2026-06-21: against a 6h53m-old wedge, recovery sequence
completed in under 5 seconds, mount fully usable, projection state
preserved (postgres was unaffected throughout).

## Files

- `slack-fuse-watchdog.sh` — one-shot wedge check. Reads `/proc/<pid>/stat`
  (NEVER touches the FUSE path — would just D-state the watchdog too).
  Tracks consecutive D-state duration across invocations via
  `$XDG_RUNTIME_DIR/slack-fuse-watchdog/`. Runs `fusermount3 -uz` once the
  configured threshold is exceeded.
- `slack-fuse-watchdog.service` — systemd user unit, oneshot wrapper.
- `slack-fuse-watchdog.timer` — fires the service every 30s.

## Install

```sh
mkdir -p ~/.config/systemd/user
cp ~/agentic/slack-fuse/scripts/watchdog/slack-fuse-watchdog.{service,timer} \
   ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now slack-fuse-watchdog.timer
```

Verify with `systemctl --user list-timers slack-fuse-watchdog.timer` —
should show the next firing within 30s.

## Knobs

Env vars on the service (override in `~/.config/systemd/user/slack-fuse-watchdog.service.d/override.conf`):

- `SLACK_FUSE_UNIT` — unit to monitor (default `slack-fuse-split.service`)
- `SLACK_FUSE_MOUNT` — path to lazy-unmount on wedge (default `/views/slack-split`)
- `WEDGE_THRESHOLD_S` — consecutive D-state seconds before action (default `90`)

## How to tell the watchdog acted

```sh
journalctl --user -u slack-fuse-watchdog.service --since today | grep watchdog
```

A successful recovery looks like:

```
[watchdog] PID 1234567 just entered D-state; tracker armed at 1718900000
[watchdog] PID 1234567 in D-state for 30s (threshold 90s); continuing to wait
[watchdog] PID 1234567 in D-state for 60s (threshold 90s); continuing to wait
[watchdog] PID 1234567 in D-state for 90s — exceeding 90s. Force-unmounting /views/slack-split
[watchdog]   fusermount3: (no output on success)
[watchdog] fusermount3 -uz succeeded — daemon will exit on next write() and systemd will respawn
```

## What this DOESN'T fix

The host-level kernel/FUSE condition that triggers the wedge in the
first place. Root cause is unidentified (see BACKLOG entry). This is a
detect-and-recover band-aid, not a cure. Multiple unrelated FUSE
filesystems on this host wedge with the same pattern; this watchdog
only recovers slack-fuse-split.
