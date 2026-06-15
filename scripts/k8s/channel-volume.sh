#!/usr/bin/env bash
# scripts/k8s/channel-volume.sh
#
# Per-channel and per-week message volume against the cluster events table.
# Use it to spot firehose channels worth adding to
# `always_blocked_channel_ids` before paying slack-api budget on a gap-fill.
#
# Modes:
#   channel-volume.sh                       — top N channels by total volume
#                                              with last-4w / last-1w activity
#   channel-volume.sh <channel-id>          — per-week breakdown for that channel
#
# Flags (top mode only):
#   --limit N        — top N channels (default 30)
#   --order total|last_4w|last_1w  — sort key (default: total)
#
# Examples:
#   scripts/k8s/channel-volume.sh
#   scripts/k8s/channel-volume.sh --limit 20 --order last_4w
#   scripts/k8s/channel-volume.sh C046S4RH6GG

set -euo pipefail

CONTEXT="${KUBECONTEXT:-admin@k8s-homelab}"
NAMESPACE="${NAMESPACE:-apps}"
PG_DEPLOY="${PG_DEPLOY:-slack-fuse-postgres}"
PG_USER="${PG_USER:-slack_fuse}"
PG_DB="${PG_DB:-slack_fuse_server}"

psql_exec() {
    kubectl --context "$CONTEXT" exec -i -n "$NAMESPACE" "deploy/$PG_DEPLOY" -- \
        psql -X -q -U "$PG_USER" -d "$PG_DB" -P pager=off "$@"
}

usage() {
    sed -n '4,21p' "$0" | sed 's/^# \{0,1\}//'
    exit 64
}

limit=30
order="total"
channel=""

while [ $# -gt 0 ]; do
    case "$1" in
        --limit) limit="$2"; shift 2 ;;
        --order) order="$2"; shift 2 ;;
        -h|--help) usage ;;
        C[A-Z0-9]*|D[A-Z0-9]*|G[A-Z0-9]*) channel="$1"; shift ;;
        *) echo "unknown arg: $1" >&2; usage ;;
    esac
done

case "$order" in
    total|last_4w|last_1w) ;;
    *) echo "invalid --order: $order (use total|last_4w|last_1w)" >&2; exit 64 ;;
esac

if [ -n "$channel" ]; then
    # Per-week breakdown for one channel. Week = ISO week starting Monday.
    psql_exec <<SQL
SELECT
    to_char(date_trunc('week', to_timestamp(ts::numeric)), 'YYYY-MM-DD') AS week_start,
    count(*) FILTER (WHERE kind = 'message')         AS messages,
    count(*) FILTER (WHERE kind = 'message_changed') AS edits,
    count(*) FILTER (WHERE kind = 'message_deleted') AS deletes
FROM events
WHERE stream = 'channel:${channel//\'/}'
  AND ts IS NOT NULL
GROUP BY week_start
ORDER BY week_start;
SQL
    exit 0
fi

# Top-by-volume aggregate. `last_4w` / `last_1w` use the events.ts (Slack
# message ts) so they reflect when the message was posted, not when we
# received it — important for distinguishing currently-active firehoses
# from once-busy-now-quiet channels.
#
# Channel name comes from the most recent channel_added / channel_renamed
# event on the channel-list stream (the slurper-side `channels` table is
# not populated, see BACKLOG).
psql_exec <<SQL
WITH names AS (
    SELECT DISTINCT ON (payload->>'id')
        payload->>'id' AS channel_id,
        payload->>'name' AS name
    FROM events
    WHERE stream = 'channel-list'
      AND kind IN ('channel_added', 'channel_renamed')
      AND payload ? 'id'
    ORDER BY payload->>'id', id DESC
),
agg AS (
    SELECT
        REPLACE(stream, 'channel:', '') AS channel_id,
        count(*) FILTER (WHERE kind = 'message') AS total,
        count(*) FILTER (
            WHERE kind = 'message'
              AND ts IS NOT NULL
              AND ts::numeric > extract(epoch from now() - interval '4 weeks')
        ) AS last_4w,
        count(*) FILTER (
            WHERE kind = 'message'
              AND ts IS NOT NULL
              AND ts::numeric > extract(epoch from now() - interval '1 week')
        ) AS last_1w,
        to_char(
            to_timestamp(min(ts::numeric) FILTER (WHERE kind = 'message' AND ts IS NOT NULL)),
            'YYYY-MM-DD'
        ) AS first_msg,
        to_char(
            to_timestamp(max(ts::numeric) FILTER (WHERE kind = 'message' AND ts IS NOT NULL)),
            'YYYY-MM-DD'
        ) AS last_msg
    FROM events
    WHERE stream LIKE 'channel:%'
    GROUP BY channel_id
)
SELECT
    a.channel_id,
    coalesce(n.name, '?') AS name,
    a.total,
    a.last_4w,
    a.last_1w,
    a.first_msg,
    a.last_msg
FROM agg a
LEFT JOIN names n ON n.channel_id = a.channel_id
ORDER BY $order DESC, total DESC
LIMIT $limit;
SQL
