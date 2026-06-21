#!/usr/bin/env bash
# scripts/watchdog/slack-fuse-watchdog.sh
#
# Detect a wedged FUSE daemon and break it loose by lazy-unmounting.
#
# THE WEDGE we defend against (see BACKLOG → "FUSE mount wedge"):
# the daemon goes D-state in `fuse_dev_write → folio_wait_bit_common`
# while writing a response back to the kernel. D-state ignores SIGKILL,
# so the daemon can't be restarted by systemd — but `fusermount3 -uz`
# operates on the kernel-side mount table and detaches immediately
# regardless of daemon state. After detach, the daemon's stuck write()
# eventually returns EIO, the process exits, and systemd respawns it.
#
# This script is a one-shot check meant to be driven by a systemd timer.
# It is deliberately read-only against the FUSE mount itself (touching
# the FUSE path from inside the watchdog would just D-state the
# watchdog too — see CLAUDE.md / poison-pill rule). All detection is
# done via /proc.
#
# Usage (operator-side; needs no root because fusermount3 is setuid):
#   ./slack-fuse-watchdog.sh
#
# Env knobs:
#   SLACK_FUSE_UNIT       systemd unit to monitor (default: slack-fuse-split.service)
#   SLACK_FUSE_MOUNT      path to force-unmount on wedge (default: /views/slack-split)
#   WEDGE_THRESHOLD_S     consecutive D-state seconds before action (default: 90)
#   WATCHDOG_STATE_DIR    where to track D-state run length across runs
#                         (default: $XDG_RUNTIME_DIR/slack-fuse-watchdog)

set -uo pipefail

UNIT="${SLACK_FUSE_UNIT:-slack-fuse-split.service}"
MOUNT="${SLACK_FUSE_MOUNT:-/views/slack-split}"
THRESHOLD_S="${WEDGE_THRESHOLD_S:-90}"
STATE_DIR="${WATCHDOG_STATE_DIR:-${XDG_RUNTIME_DIR:-/tmp}/slack-fuse-watchdog}"
STATE_FILE="$STATE_DIR/d-state-first-seen"

mkdir -p "$STATE_DIR"

log() {
    # journal-friendly: stderr lines get tagged by the systemd unit.
    echo "[watchdog] $*" >&2
}

# Fetch the daemon PID. 0 means "not running" — nothing to do.
PID=$(systemctl --user show "$UNIT" -p MainPID --value 2>/dev/null || echo 0)
if [[ -z "$PID" || "$PID" == "0" ]]; then
    rm -f "$STATE_FILE"
    log "$UNIT not running; nothing to check"
    exit 0
fi

# /proc/<pid>/stat field 3 is the process state code. R = running, S = sleep,
# D = uninterruptible disk sleep, Z = zombie. We want D specifically — that
# is the wedge shape (any other state is recoverable on its own).
read_state() {
    local stat_line
    if ! stat_line=$(awk '{print $3}' "/proc/$1/stat" 2>/dev/null); then
        echo "?"
        return
    fi
    echo "$stat_line"
}

STATE=$(read_state "$PID")

# Healthy: clear any prior D-state tracking and exit.
if [[ "$STATE" != "D" ]]; then
    if [[ -f "$STATE_FILE" ]]; then
        log "PID $PID transitioned out of D-state; clearing tracker"
        rm -f "$STATE_FILE"
    fi
    exit 0
fi

# D-state: record the first-seen timestamp so subsequent runs can measure
# duration. The state file holds an epoch-seconds value and the pid (so a
# pid recycle restarts the clock cleanly).
NOW=$(date +%s)
if [[ -f "$STATE_FILE" ]]; then
    SEEN_PID=$(awk 'NR==1' "$STATE_FILE")
    SEEN_AT=$(awk 'NR==2' "$STATE_FILE")
else
    SEEN_PID=""
    SEEN_AT=""
fi

if [[ "$SEEN_PID" != "$PID" || -z "$SEEN_AT" ]]; then
    printf '%s\n%s\n' "$PID" "$NOW" > "$STATE_FILE"
    log "PID $PID just entered D-state; tracker armed at $NOW"
    exit 0
fi

AGE=$(( NOW - SEEN_AT ))
if (( AGE < THRESHOLD_S )); then
    log "PID $PID in D-state for ${AGE}s (threshold ${THRESHOLD_S}s); continuing to wait"
    exit 0
fi

# Threshold exceeded — full recovery has two steps:
#
# 1. ``fusermount3 -uz`` (lazy unmount). Detaches the mount from the
#    namespace immediately; works regardless of daemon state because it
#    operates on the kernel mount table. After this, any FUSE clients
#    blocked on ``fuse_simple_request`` see the channel torn down.
#
# 2. ``systemctl --user restart`` the unit. The daemon stays D-state
#    (its `write()` is still waiting on a folio bit even after the
#    channel is gone), but systemd is happy to start a fresh instance
#    with a new PID — it logs "Found left-over process … Ignoring" and
#    proceeds. The orphan daemon dies whenever its kernel wait
#    eventually returns (which it may never, but it's harmless: takes
#    no FUSE traffic, just sits in kernel sleep).
log "PID $PID in D-state for ${AGE}s — exceeding ${THRESHOLD_S}s. Triggering recovery."

if fusermount3 -uz "$MOUNT" 2>&1 | sed 's/^/[watchdog]   fusermount3: /' >&2; then
    log "fusermount3 -uz succeeded; mount detached"
else
    log "fusermount3 -uz failed; mount may already be gone"
fi

if systemctl --user restart "$UNIT" 2>&1 | sed 's/^/[watchdog]   systemctl: /' >&2; then
    log "$UNIT restarted; recovery complete (old wedged PID may linger as orphan)"
else
    log "$UNIT restart failed; manual intervention required"
fi

# Clear the tracker so the next run can re-detect (against either the
# old wedged pid finally exiting, or the new pid systemd respawned).
rm -f "$STATE_FILE"
exit 0
