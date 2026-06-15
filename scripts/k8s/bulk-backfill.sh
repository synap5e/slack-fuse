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
#                                [--gap-fill]
#
# Flags:
#   --sleep-s N   Seconds between Job submissions. Default 30.
#   --dry-run     Print what would be submitted without applying.
#   --force       Don't skip channels that already have events on the cluster.
#   --gap-fill    Switch to slack-api source with per-channel --since = max(ts
#                 on cluster). Use this after the initial legacy-cache load
#                 to pick up messages posted in the window between legacy
#                 polling cutoff and Socket Mode start. Requires the slurper
#                 image to support the --since CLI flag.

set -euo pipefail

CACHE_DIR="${SLACK_FUSE_LEGACY_CACHE_DIR:-$HOME/.cache/slack-fuse/messages}"
CONFIG_FILE="${SLACK_FUSE_CLIENT_CONFIG:-$HOME/.config/slack-fuse/config.toml}"
KUBECONTEXT="${KUBECONTEXT:-admin@k8s-homelab}"
NAMESPACE="${NAMESPACE:-apps}"
HERE="$(dirname "$(readlink -f "$0")")"
SUBMIT_SLEEP_S=30
DRY_RUN=0
FORCE=0
# --gap-fill mode: pivot from legacy-cache backfill (used for initial load)
# to slack-api backfill bounded by --since=<per-channel max ts on cluster>.
# Picks up everything posted between the legacy cutoff and Socket Mode
# start. Requires a slurper image with the --since CLI flag.
GAP_FILL=0

while [ $# -gt 0 ]; do
    case "$1" in
        --sleep-s) SUBMIT_SLEEP_S="$2"; shift 2 ;;
        --dry-run) DRY_RUN=1; shift ;;
        --force) FORCE=1; shift ;;
        --gap-fill) GAP_FILL=1; shift ;;
        *) echo "unknown flag: $1" >&2; exit 64 ;;
    esac
done

# Extract always_blocked_channel_ids from the TOML config. Strict: only
# matches uppercase-C-prefixed Slack channel IDs INSIDE the
# always_blocked_channel_ids array.
BLOCKED=$(awk '
    /always_blocked_channel_ids[[:space:]]*=/ { in_arr = 1; next }
    in_arr && /\]/ { in_arr = 0 }
    in_arr && match($0, /"(C[A-Z0-9]+)"/, m) { print m[1] }
' "$CONFIG_FILE" 2>/dev/null | sort -u)
echo "Always-blocked from config: $(echo "$BLOCKED" | wc -l) channels"

if [ "$GAP_FILL" -eq 1 ]; then
    # Gap-fill mode: candidates = every channel currently on the cluster.
    # For each, compute since = max(ts on cluster). The slurper API call
    # will paginate forward from there; the events_message_dedup index
    # drops any overlap.
    echo "Mode: GAP-FILL via slack-api, per-channel --since = max(ts on cluster)"
    SINCE_MAP=$(kubectl --context "$KUBECONTEXT" exec -n "$NAMESPACE" deploy/slack-fuse-postgres -- \
        psql -U slack_fuse -d slack_fuse_server -t -A -F$'\t' -c "
            SELECT REPLACE(stream, 'channel:', ''), max(ts::numeric)
            FROM events
            WHERE stream LIKE 'channel:%' AND kind='message' AND ts IS NOT NULL
            GROUP BY stream
            ORDER BY stream;
        " 2>/dev/null | grep -P '^C[A-Z0-9]+\t')
    CANDIDATES=$(echo "$SINCE_MAP" | cut -f1 | sort -u)
    ALREADY=""
else
    # Initial bulk mode: candidates = legacy-cache directories.
    if [ ! -d "$CACHE_DIR" ]; then
        echo "no cache dir at $CACHE_DIR" >&2
        exit 1
    fi
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
fi

# Final TODO list = candidates - blocked - already
TODO=$(comm -23 <(echo "$CANDIDATES") <(echo "$BLOCKED") | comm -23 - <(echo "$ALREADY"))
COUNT=$(echo "$TODO" | grep -c . 2>/dev/null || echo 0)
echo
echo "==> $COUNT channels to process (sleep ${SUBMIT_SLEEP_S}s between submissions)"
echo "==> Estimated submission time: $((COUNT * SUBMIT_SLEEP_S / 60))min"
echo

if [ "$COUNT" -eq 0 ]; then
    echo "Nothing to do."
    exit 0
fi

if [ "$DRY_RUN" -eq 1 ]; then
    echo "--- DRY RUN: would submit ---"
    if [ "$GAP_FILL" -eq 1 ]; then
        echo "$TODO" | while read -r ch; do
            since=$(echo "$SINCE_MAP" | awk -F'\t' -v c="$ch" '$1==c {print $2}')
            echo "  $ch --since $since"
        done
    else
        echo "$TODO"
    fi
    exit 0
fi

i=0
for ch in $TODO; do
    i=$((i + 1))
    if [ "$GAP_FILL" -eq 1 ]; then
        since=$(echo "$SINCE_MAP" | awk -F'\t' -v c="$ch" '$1==c {print $2}')
        echo "[$i/$COUNT] $(date +%T) gap-fill $ch since=$since..."
        "$HERE/backfill-job.sh" "$ch" --source slack-api --since "$since" 2>&1 | grep -E "created|error|Error" | head -1 || true
    else
        echo "[$i/$COUNT] $(date +%T) submitting $ch..."
        "$HERE/backfill-job.sh" "$ch" 2>&1 | grep -E "created|error|Error" | head -1 || true
    fi
    if [ "$i" -lt "$COUNT" ]; then
        sleep "$SUBMIT_SLEEP_S"
    fi
done

echo
echo "All $COUNT submissions queued. Tail with:"
echo "  kubectl --context $KUBECONTEXT get jobs -n $NAMESPACE -l app=slack-fuse-backfill -w"
echo "Or per-channel logs:"
echo "  kubectl --context $KUBECONTEXT logs -n $NAMESPACE -l app=slack-fuse-backfill,slack-fuse/channel-id=<ID>"
