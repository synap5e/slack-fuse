#!/usr/bin/env bash
# scripts/k8s/bulk-backfill.sh
#
# Iterate every channel in the operator's legacy cache and submit a backfill
# Job for it via `backfill-job.sh`. Skips channels that are:
#
#   - In `ClientConfig.always_blocked_channel_ids` (read from
#     ~/.config/slack-fuse/config.toml)
#   - Already fully represented in the cluster's events table (any events on
#     `channel:<id>` means a prior backfill ran — dedup makes a re-run safe
#     but pointlessly expensive)
#
# Defaults to one submission every 30s so the cluster, NFS, and projector
# don't all peak together. Each Job's runtime is independent of submission
# pacing — they'll naturally overlap.
#
# Usage:
#   scripts/k8s/bulk-backfill.sh [--sleep-s 30] [--dry-run] [--force]
#
# Flags:
#   --sleep-s N   Seconds between Job submissions. Default 30.
#   --dry-run     Print what would be submitted without applying.
#   --force       Don't skip channels that already have events on the cluster.

set -euo pipefail

CACHE_DIR="${SLACK_FUSE_LEGACY_CACHE_DIR:-$HOME/.cache/slack-fuse/messages}"
CONFIG_FILE="${SLACK_FUSE_CLIENT_CONFIG:-$HOME/.config/slack-fuse/config.toml}"
KUBECONTEXT="${KUBECONTEXT:-admin@k8s-homelab}"
NAMESPACE="${NAMESPACE:-apps}"
HERE="$(dirname "$(readlink -f "$0")")"
SUBMIT_SLEEP_S=30
DRY_RUN=0
FORCE=0

while [ $# -gt 0 ]; do
    case "$1" in
        --sleep-s) SUBMIT_SLEEP_S="$2"; shift 2 ;;
        --dry-run) DRY_RUN=1; shift ;;
        --force) FORCE=1; shift ;;
        *) echo "unknown flag: $1" >&2; exit 64 ;;
    esac
done

if [ ! -d "$CACHE_DIR" ]; then
    echo "no cache dir at $CACHE_DIR" >&2
    exit 1
fi

# Extract always_blocked_channel_ids from the TOML config. Strict: only
# matches uppercase-C-prefixed Slack channel IDs INSIDE the
# always_blocked_channel_ids array.
BLOCKED=$(awk '
    /always_blocked_channel_ids[[:space:]]*=/ { in_arr = 1; next }
    in_arr && /\]/ { in_arr = 0 }
    in_arr && match($0, /"(C[A-Z0-9]+)"/, m) { print m[1] }
' "$CONFIG_FILE" 2>/dev/null | sort -u)
echo "Always-blocked from config: $(echo "$BLOCKED" | wc -l) channels"

# All cache directories — Slack channel IDs are 11 chars; filter sanity.
CANDIDATES=$(ls "$CACHE_DIR" | grep -E '^C[A-Z0-9]{10}$' | sort -u)
TOTAL=$(echo "$CANDIDATES" | wc -l)
echo "Cache holds $TOTAL channel directories"

if [ "$FORCE" -ne 1 ]; then
    ALREADY=$(kubectl --context "$KUBECONTEXT" exec -n "$NAMESPACE" deploy/slack-fuse-postgres -- \
        psql -U slack_fuse -d slack_fuse_server -t -A -c "
            SELECT DISTINCT REPLACE(stream, 'channel:', '')
            FROM events
            WHERE stream LIKE 'channel:%'
        " 2>/dev/null | grep -E '^C[A-Z0-9]+$' | sort -u || true)
    echo "Cluster already has events for $(echo "$ALREADY" | grep -c . 2>/dev/null || echo 0) channels (will skip; pass --force to override)"
else
    ALREADY=""
fi

# Final TODO list = candidates - blocked - already
TODO=$(comm -23 <(echo "$CANDIDATES") <(echo "$BLOCKED") | comm -23 - <(echo "$ALREADY"))
COUNT=$(echo "$TODO" | grep -c . 2>/dev/null || echo 0)
echo
echo "==> $COUNT channels to backfill (sleep ${SUBMIT_SLEEP_S}s between submissions)"
echo "==> Estimated submission time: $((COUNT * SUBMIT_SLEEP_S / 60))min"
echo

if [ "$COUNT" -eq 0 ]; then
    echo "Nothing to do."
    exit 0
fi

if [ "$DRY_RUN" -eq 1 ]; then
    echo "--- DRY RUN: would submit ---"
    echo "$TODO"
    exit 0
fi

i=0
for ch in $TODO; do
    i=$((i + 1))
    echo "[$i/$COUNT] $(date +%T) submitting $ch..."
    "$HERE/backfill-job.sh" "$ch" 2>&1 | grep -E "created|error|Error" | head -1 || true
    if [ "$i" -lt "$COUNT" ]; then
        sleep "$SUBMIT_SLEEP_S"
    fi
done

echo
echo "All $COUNT submissions queued. Tail with:"
echo "  kubectl --context $KUBECONTEXT get jobs -n $NAMESPACE -l app=slack-fuse-backfill -w"
echo "Or per-channel logs:"
echo "  kubectl --context $KUBECONTEXT logs -n $NAMESPACE -l app=slack-fuse-backfill,slack-fuse/channel-id=<ID>"
