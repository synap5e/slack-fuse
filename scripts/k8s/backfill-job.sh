#!/usr/bin/env bash
# scripts/k8s/backfill-job.sh
#
# Run a one-off `slack-fuse-server backfill <channel-id>` Job against the
# cluster server's postgres + Slack tokens. Writes to the same `events` table
# the running server uses; the server's NOTIFY-driven live tail loop will push
# the new events to any subscribed client (e.g. your local split mount), so
# the projection materialises in real time.
#
# Usage:
#   scripts/k8s/backfill-job.sh <channel-id> [--source slack-api|legacy-cache]
#                              [--allow-large] [--max-messages N]
#                              [--watch]
#
# Examples:
#   scripts/k8s/backfill-job.sh C077358GZLM
#   scripts/k8s/backfill-job.sh C077358GZLM --watch
#   scripts/k8s/backfill-job.sh C077358GZLM --source slack-api --max-messages 50000
#
# Bulk:
#   for ch in $(psql -A -t -c "SELECT channel_id FROM channels WHERE subscribed AND ..."); do
#     scripts/k8s/backfill-job.sh "$ch"
#     sleep 30  # space out Slack-API hits across runs
#   done

set -euo pipefail

if [ $# -lt 1 ]; then
    echo "usage: $0 <channel-id> [--source slack-api|legacy-cache] [--allow-large] [--max-messages N] [--since EPOCH] [--watch]" >&2
    exit 64
fi

CHANNEL_ID="$1"; shift
WATCH=0
# Default to legacy-cache (much faster than slack-api) — operator-side NFS
# share at ~/.cache/slack-fuse/messages/ is mounted into the pod, the
# LegacyCacheBackfiller reads from /app/.cache/slack-fuse/messages/.
# Override with `--source slack-api` when the cache isn't available.
SOURCE="legacy-cache"
EXTRA_ARGS=()

while [ $# -gt 0 ]; do
    case "$1" in
        --watch) WATCH=1; shift ;;
        --source) SOURCE="$2"; shift 2 ;;
        --allow-large) EXTRA_ARGS+=("--allow-large"); shift ;;
        --max-messages) EXTRA_ARGS+=("--max-messages" "$2"); shift 2 ;;
        # --since EPOCH: lower bound on ts. Use this for gap-fills against
        # slack-api so we don't repaginate the whole channel history. The
        # events_message_dedup index makes overlap free, but pagination still
        # costs API budget.
        --since) EXTRA_ARGS+=("--since" "$2"); shift 2 ;;
        *) echo "unknown flag: $1" >&2; exit 64 ;;
    esac
done

# NFS share serving the legacy cache. Operator's PC must be running an NFS
# server with /home/simon/.cache/slack-fuse/messages exported read-only to
# the cluster node. See scripts/k8s/README.md for the one-time setup.
NFS_SERVER="${SLACK_FUSE_LEGACY_NFS_SERVER:-10.0.100.1}"
NFS_PATH="${SLACK_FUSE_LEGACY_NFS_PATH:-/home/simon/.cache/slack-fuse/messages}"

CONTEXT="${KUBECONTEXT:-admin@k8s-homelab}"
NAMESPACE="${NAMESPACE:-apps}"
# Pinned image must match what's deployed for the slack-fuse-server. If you
# bump the server image, bump this too — backfill writes events with the same
# schema the server projects, so the two must agree.
IMAGE="${SLACK_FUSE_IMAGE:-ghcr.io/synap5e/slack-fuse:sha-a9117fe@sha256:ec43a78f73703e70c5fec2ac8be5c264e38abc023c627abf702577c81530cb9b}"

NAME="slack-fuse-backfill-$(echo "$CHANNEL_ID" | tr '[:upper:]' '[:lower:]')-$(date +%s)"

CLI_ARGS=("backfill" "$CHANNEL_ID" "--source" "$SOURCE" "${EXTRA_ARGS[@]}")
# Each arg as a double-quoted YAML scalar so float values (e.g. --since
# 1778275867.152699) don't get parsed as numbers and rejected by k8s.
# None of our args contain double quotes, so no escaping needed.
ARGS_YAML=$(printf '            - "%s"\n' "${CLI_ARGS[@]}")

# Legacy-cache source requires the operator's ~/.cache/slack-fuse/messages
# directory mounted at the path the LegacyCacheBackfiller expects in-pod:
# /app/.cache/slack-fuse/messages/. slack-api source needs no mount.
if [ "$SOURCE" = "legacy-cache" ]; then
  VOLUME_YAML="
      volumes:
        - name: legacy-cache
          nfs:
            server: $NFS_SERVER
            path: $NFS_PATH
            readOnly: true"
  VOLUME_MOUNT_YAML="
          volumeMounts:
            - name: legacy-cache
              mountPath: /app/.cache/slack-fuse/messages
              readOnly: true"
else
  VOLUME_YAML=""
  VOLUME_MOUNT_YAML=""
fi

# shellcheck disable=SC2155
manifest=$(cat <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: $NAME
  namespace: $NAMESPACE
  labels:
    app: slack-fuse-backfill
    slack-fuse/channel-id: "$CHANNEL_ID"
spec:
  ttlSecondsAfterFinished: 86400  # auto-clean after 1 day
  backoffLimit: 0                  # don't retry; manual rerun if a transient failure
  template:
    spec:
      restartPolicy: Never
      imagePullSecrets:
        - name: ghcr-pull
      containers:
        - name: backfill
          image: $IMAGE
          args:
$ARGS_YAML
          env:
            - name: SLACK_FUSE_SERVER_DATABASE_URL
              valueFrom: { secretKeyRef: { name: slack-fuse-env, key: DATABASE_URL } }
            - name: SLACK_FUSE_SERVER_SHARED_SECRET
              valueFrom: { secretKeyRef: { name: slack-fuse-env, key: SHARED_SECRET } }
            - name: SLACK_FUSE_SERVER_SLACK_USER_TOKEN
              valueFrom: { secretKeyRef: { name: slack-fuse-env, key: SLACK_USER_TOKEN } }
            - name: SLACK_FUSE_SERVER_SLACK_APP_TOKEN
              valueFrom: { secretKeyRef: { name: slack-fuse-env, key: SLACK_APP_TOKEN } }
            - name: SLACK_FUSE_SERVER_SLACK_BOT_TOKEN
              valueFrom: { secretKeyRef: { name: slack-fuse-env, key: SLACK_BOT_TOKEN } }
          resources:
            limits: { memory: 512Mi, cpu: 500m }
            requests: { memory: 256Mi, cpu: 250m }$VOLUME_MOUNT_YAML$VOLUME_YAML
EOF
)

echo "$manifest" | kubectl --context "$CONTEXT" apply -f -
echo
echo "Job submitted: $NAMESPACE/$NAME"
echo "Tail logs:    kubectl --context $CONTEXT logs -n $NAMESPACE -f job/$NAME"
echo "Status:       kubectl --context $CONTEXT get job/$NAME -n $NAMESPACE"

if [ "$WATCH" -eq 1 ]; then
    echo
    echo "--- streaming logs ---"
    # Wait for the pod to exist, then tail
    until kubectl --context "$CONTEXT" -n "$NAMESPACE" get pod -l "job-name=$NAME" -o name 2>/dev/null | grep -q pod/; do
        sleep 1
    done
    kubectl --context "$CONTEXT" logs -n "$NAMESPACE" -f "job/$NAME"
fi
