#!/usr/bin/env bash
# scripts/watchdog/slack-fuse-watchdog.sh
#
# Detect a wedged FUSE daemon and break it loose by lazy-unmounting.
#
# TWO WEDGE SHAPES this defends against (both trigger the same recovery):
#
# 1. Daemon in D-state. Classic shape (BACKLOG → "FUSE mount wedge"):
#    the daemon is stuck in `fuse_dev_write → folio_wait_bit_common`
#    writing a response back to the kernel. D-state ignores SIGKILL so
#    systemd can't restart it — but `fusermount3 -uz` operates on the
#    kernel-side mount table and detaches immediately regardless of
#    daemon state.
#
# 2. Kernel-side backlog. Overnight 2026-07-06 shape (see chat.md):
#    daemon sits idle in `epoll_wait` (S-state, looks healthy) while
#    the kernel has N unanswered requests queued for the connection.
#    Never produces D-state on the daemon PID, so shape-1 misses it
#    entirely. Detected via `/sys/fs/fuse/connections/<id>/waiting`
#    staying non-zero for a full threshold window — the kernel's own
#    counter is daemon-agnostic.
#
# The script is a one-shot check meant to be driven by a systemd timer.
# It is deliberately read-only against the FUSE mount itself (touching
# the FUSE path from inside the watchdog would just wedge the watchdog
# too — see CLAUDE.md / poison-pill rule). All detection is done via
# /proc and /sys.
#
# Usage (operator-side; needs no root because fusermount3 is setuid):
#   ./slack-fuse-watchdog.sh
#
# Env knobs:
#   SLACK_FUSE_UNIT              systemd unit to monitor (default: slack-fuse-split.service)
#   SLACK_FUSE_MOUNT             mount path to force-unmount on wedge (default: /views/slack-split)
#   WEDGE_THRESHOLD_S            consecutive D-state seconds before action (default: 90)
#   WEDGE_WAITING_THRESHOLD_S    seconds of continuous waiting>0 before action (default: 60)
#   WATCHDOG_STATE_DIR           where to track detector state across runs
#                                (default: $XDG_RUNTIME_DIR/slack-fuse-watchdog)

set -uo pipefail

UNIT="${SLACK_FUSE_UNIT:-slack-fuse-split.service}"
MOUNT="${SLACK_FUSE_MOUNT:-/views/slack-split}"
THRESHOLD_S="${WEDGE_THRESHOLD_S:-90}"
WAITING_THRESHOLD_S="${WEDGE_WAITING_THRESHOLD_S:-60}"
STATE_DIR="${WATCHDOG_STATE_DIR:-${XDG_RUNTIME_DIR:-/tmp}/slack-fuse-watchdog}"
D_STATE_FILE="$STATE_DIR/d-state-first-seen"
WAITING_STATE_FILE="$STATE_DIR/waiting-first-seen"

mkdir -p "$STATE_DIR"

log() {
    # journal-friendly: stderr lines get tagged by the systemd unit.
    echo "[watchdog] $*" >&2
}

# Fetch the daemon PID. 0 means "not running" — clear both trackers.
PID=$(systemctl --user show "$UNIT" -p MainPID --value 2>/dev/null || echo 0)
if [[ -z "$PID" || "$PID" == "0" ]]; then
    rm -f "$D_STATE_FILE" "$WAITING_STATE_FILE"
    log "$UNIT not running; nothing to check"
    exit 0
fi

# Map mount path → FUSE connection id via /proc/self/mountinfo. Field 3
# is "<major>:<minor>" and the minor IS the conn id. Pure kernel data
# so this is safe to read from the watchdog (never touches the FUSE
# mount itself). Hoisted here — both detectors need it, not just recovery.
CONN_ID=""
MOUNT_LINE=$(awk -v m="$MOUNT" '$5==m {print; exit}' /proc/self/mountinfo)
if [[ -n "$MOUNT_LINE" ]]; then
    DEV=$(awk '{print $3}' <<<"$MOUNT_LINE")
    CONN_ID="${DEV#*:}"
fi

# /proc/<pid>/stat field 3 is the process state code. R = running,
# S = sleep, D = uninterruptible disk sleep, Z = zombie.
read_state() {
    local stat_line
    if ! stat_line=$(awk '{print $3}' "/proc/$1/stat" 2>/dev/null); then
        echo "?"
        return
    fi
    echo "$stat_line"
}

# Kernel's own count of unanswered client requests for the connection.
# Returns -1 when the file can't be read (no conn id, or the connection
# went away between mountinfo and sysfs). 0 means healthy / idle.
read_waiting() {
    local id="$1"
    if [[ -z "$id" ]]; then
        echo -1
        return
    fi
    local file="/sys/fs/fuse/connections/$id/waiting"
    if [[ ! -r "$file" ]]; then
        echo -1
        return
    fi
    local val
    val=$(awk '{print $1+0}' "$file" 2>/dev/null)
    echo "${val:-0}"
}

STATE=$(read_state "$PID")
WAITING=$(read_waiting "$CONN_ID")
NOW=$(date +%s)

FIRE_RECOVERY=false
REASONS=()

# --- Detector 1: daemon in D-state for ≥ THRESHOLD_S ---
if [[ "$STATE" == "D" ]]; then
    if [[ -f "$D_STATE_FILE" ]]; then
        SEEN_PID=$(awk 'NR==1' "$D_STATE_FILE")
        SEEN_AT=$(awk 'NR==2' "$D_STATE_FILE")
    else
        SEEN_PID=""
        SEEN_AT=""
    fi
    if [[ "$SEEN_PID" != "$PID" || -z "$SEEN_AT" ]]; then
        printf '%s\n%s\n' "$PID" "$NOW" > "$D_STATE_FILE"
        log "PID $PID just entered D-state; tracker armed at $NOW"
    else
        AGE=$(( NOW - SEEN_AT ))
        if (( AGE < THRESHOLD_S )); then
            log "PID $PID in D-state for ${AGE}s (threshold ${THRESHOLD_S}s); continuing to wait"
        else
            FIRE_RECOVERY=true
            REASONS+=("D-state for ${AGE}s")
        fi
    fi
else
    if [[ -f "$D_STATE_FILE" ]]; then
        log "PID $PID no longer in D-state; clearing tracker"
        rm -f "$D_STATE_FILE"
    fi
fi

# --- Detector 2: kernel waiting > 0 for ≥ WAITING_THRESHOLD_S ---
# A brief spike is fine (a slow-but-answering daemon); the tracker only
# fires when the count stays non-zero across the whole window. A healthy
# daemon under load clears waiting in milliseconds — even a slow legit
# request (~2s /gap-candidates) drains long before this threshold.
if (( WAITING > 0 )); then
    if [[ -f "$WAITING_STATE_FILE" ]]; then
        SEEN_CONN=$(awk 'NR==1' "$WAITING_STATE_FILE")
        SEEN_AT=$(awk 'NR==2' "$WAITING_STATE_FILE")
    else
        SEEN_CONN=""
        SEEN_AT=""
    fi
    if [[ "$SEEN_CONN" != "$CONN_ID" || -z "$SEEN_AT" ]]; then
        printf '%s\n%s\n' "$CONN_ID" "$NOW" > "$WAITING_STATE_FILE"
        log "FUSE conn $CONN_ID has waiting=$WAITING; tracker armed at $NOW"
    else
        AGE=$(( NOW - SEEN_AT ))
        if (( AGE < WAITING_THRESHOLD_S )); then
            log "FUSE conn $CONN_ID waiting=$WAITING for ${AGE}s (threshold ${WAITING_THRESHOLD_S}s); continuing to wait"
        else
            FIRE_RECOVERY=true
            REASONS+=("waiting=$WAITING for ${AGE}s")
        fi
    fi
elif (( WAITING == 0 )) && [[ -f "$WAITING_STATE_FILE" ]]; then
    log "FUSE conn $CONN_ID waiting cleared; clearing tracker"
    rm -f "$WAITING_STATE_FILE"
fi
# WAITING == -1 (unreadable) → don't touch the tracker; conn probably
# transitioning between mount/unmount and will resolve on next tick.

if ! $FIRE_RECOVERY; then
    exit 0
fi

# --- Recovery (three steps; see lesson_fuse_orphan_recovery.md) ---
# 1. sysfs abort — the ONLY primitive that always works. Pure sysfs
#    write, no FUSE traffic, can't itself wedge. Kills queued client
#    requests with ENODEV/EIO and wakes the daemon's read /dev/fuse.
# 2. fusermount3 -uz — cleans up the stale mount entry.
# 3. systemctl --user restart — brings the unit back. The orphan
#    daemon (if any) can linger; harmless once the connection is gone.
log "Triggering recovery: ${REASONS[*]}"

if [[ -n "$CONN_ID" ]]; then
    ABORT_FILE="/sys/fs/fuse/connections/$CONN_ID/abort"
    if [[ -w "$ABORT_FILE" ]]; then
        if echo 1 > "$ABORT_FILE" 2>&1; then
            log "FUSE conn $CONN_ID aborted via sysfs"
        else
            log "abort write failed; continuing to fusermount3"
        fi
    else
        log "no writable abort file at $ABORT_FILE (conn already gone?); continuing"
    fi
else
    log "$MOUNT not in /proc/self/mountinfo; skipping sysfs abort"
fi

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

# Clear BOTH trackers so next run re-detects from scratch (against
# either the old wedged pid finally exiting, or the new pid systemd
# respawned).
rm -f "$D_STATE_FILE" "$WAITING_STATE_FILE"
exit 0
