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
#                                              with an ASCII bar chart
#
# Flags (top mode only):
#   --limit N        — top N channels (default 30)
#   --order total|last_4w|last_1w  — sort key (default: total)
#   --all            — include channels already in always_blocked
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
CONFIG_FILE="${SLACK_FUSE_CLIENT_CONFIG:-$HOME/.config/slack-fuse/config.toml}"

psql_exec() {
    kubectl --context "$CONTEXT" exec -i -n "$NAMESPACE" "deploy/$PG_DEPLOY" -- \
        psql -X -q -U "$PG_USER" -d "$PG_DB" -P pager=off "$@"
}

# Read always_blocked_channel_ids from the operator's client config. Returns
# a newline-separated list (empty if config absent).
read_blocked() {
    awk '
        /always_blocked_channel_ids[[:space:]]*=/ { in_arr = 1; next }
        in_arr && /\]/ { in_arr = 0 }
        in_arr && match($0, /"(C[A-Z0-9]+)"/, m) { print m[1] }
    ' "$CONFIG_FILE" 2>/dev/null
}

usage() {
    sed -n '4,22p' "$0" | sed 's/^# \{0,1\}//'
    exit 64
}

limit=30
order="total"
channel=""
include_blocked=0

while [ $# -gt 0 ]; do
    case "$1" in
        --limit) limit="$2"; shift 2 ;;
        --order) order="$2"; shift 2 ;;
        --all) include_blocked=1; shift ;;
        -h|--help) usage ;;
        C[A-Z0-9]*|D[A-Z0-9]*|G[A-Z0-9]*) channel="$1"; shift ;;
        *) echo "unknown arg: $1" >&2; usage ;;
    esac
done

case "$order" in
    total|last_4w|last_1w) ;;
    *) echo "invalid --order: $order (use total|last_4w|last_1w)" >&2; exit 64 ;;
esac

###############################################################################
# Per-channel mode: header + weekly bar chart
###############################################################################
if [ -n "$channel" ]; then
    # Fetch header metadata in one round-trip: name + totals + range.
    # -A: unaligned, -t: tuples only, -F: separator. We get one line, split
    # on the chosen separator.
    IFS=$'\t' read -r name total first_msg last_msg edits deletes last_4w < <(
        psql_exec -t -A -F$'\t' <<SQL
SELECT
    coalesce(
        (SELECT payload->>'name'
         FROM events
         WHERE stream='channel-list'
           AND kind IN ('channel_added','channel_renamed')
           AND payload->>'id' = '${channel//\'/}'
         ORDER BY id DESC LIMIT 1),
        '?'
    ),
    count(*) FILTER (WHERE kind='message'),
    coalesce(to_char(to_timestamp(min(ts::numeric) FILTER (WHERE kind='message')), 'YYYY-MM-DD'), '-'),
    coalesce(to_char(to_timestamp(max(ts::numeric) FILTER (WHERE kind='message')), 'YYYY-MM-DD'), '-'),
    count(*) FILTER (WHERE kind='message_changed'),
    count(*) FILTER (WHERE kind='message_deleted'),
    count(*) FILTER (
        WHERE kind='message'
          AND ts IS NOT NULL
          AND ts::numeric > extract(epoch from now() - interval '4 weeks')
    )
FROM events
WHERE stream = 'channel:${channel//\'/}';
SQL
    )

    blocked_marker=""
    if read_blocked | grep -qx "$channel"; then
        blocked_marker="  [ALWAYS-BLOCKED]"
    fi

    printf '\n'
    printf 'Channel:  #%s (%s)%s\n' "$name" "$channel" "$blocked_marker"
    printf 'Total:    %s messages | %s edits | %s deletes\n' "$total" "$edits" "$deletes"
    printf 'Range:    %s → %s | last 4w: %s msgs\n' "$first_msg" "$last_msg" "$last_4w"
    printf '\n'

    if [ "${total:-0}" -eq 0 ]; then
        printf '(no messages on cluster — channel may be subscribed but empty, or backfill skipped)\n'
        exit 0
    fi

    # Weekly bar chart. Bar scales to the channel's own peak (40-char max).
    # Drop edits/deletes columns from this view — header already shows totals.
    psql_exec <<SQL
WITH weekly AS (
    SELECT
        date_trunc('week', to_timestamp(ts::numeric)) AS week_start,
        count(*) FILTER (WHERE kind='message') AS messages
    FROM events
    WHERE stream='channel:${channel//\'/}'
      AND ts IS NOT NULL
      AND kind='message'
    GROUP BY week_start
)
SELECT
    to_char(week_start, 'YYYY-MM-DD') AS week,
    messages,
    repeat('█', greatest(
        CASE WHEN messages = 0 THEN 0 ELSE 1 END,
        (messages::float * 40 / nullif(max(messages) OVER (), 0))::int
    )) AS bar
FROM weekly
ORDER BY week_start;
SQL
    exit 0
fi

###############################################################################
# Top mode: table sorted by chosen volume column, with blocked indicator
###############################################################################

# Stash the always_blocked list as a temp table inside a single psql session
# so we can JOIN it. (Heredoc with multiple statements; \copy not needed.)
blocked_list=$(read_blocked | tr '\n' ',' | sed 's/,$//' | sed "s/[^,]*/'&'/g")
blocked_sql_filter=""
if [ "$include_blocked" -ne 1 ] && [ -n "$blocked_list" ]; then
    blocked_sql_filter="WHERE a.channel_id NOT IN ($blocked_list)"
fi

psql_exec <<SQL
WITH names AS (
    SELECT DISTINCT ON (payload->>'id')
        payload->>'id' AS channel_id,
        payload->>'name' AS name
    FROM events
    WHERE stream='channel-list'
      AND kind IN ('channel_added','channel_renamed')
      AND payload ? 'id'
    ORDER BY payload->>'id', id DESC
),
agg AS (
    SELECT
        REPLACE(stream, 'channel:', '') AS channel_id,
        count(*) FILTER (WHERE kind='message') AS total,
        count(*) FILTER (
            WHERE kind='message'
              AND ts IS NOT NULL
              AND ts::numeric > extract(epoch from now() - interval '4 weeks')
        ) AS last_4w,
        count(*) FILTER (
            WHERE kind='message'
              AND ts IS NOT NULL
              AND ts::numeric > extract(epoch from now() - interval '1 week')
        ) AS last_1w,
        to_char(
            to_timestamp(min(ts::numeric) FILTER (WHERE kind='message' AND ts IS NOT NULL)),
            'YYYY-MM-DD'
        ) AS first_msg,
        to_char(
            to_timestamp(max(ts::numeric) FILTER (WHERE kind='message' AND ts IS NOT NULL)),
            'YYYY-MM-DD'
        ) AS last_msg
    FROM events
    WHERE stream LIKE 'channel:%'
    GROUP BY channel_id
)
SELECT
    coalesce(n.name, '?') AS name,
    a.channel_id,
    a.total,
    a.last_4w,
    a.last_1w,
    a.first_msg,
    a.last_msg,
    repeat('█', greatest(
        CASE WHEN a.$order = 0 THEN 0 ELSE 1 END,
        (a.$order::float * 30 / nullif(max(a.$order) OVER (), 0))::int
    )) AS bar
FROM agg a
LEFT JOIN names n ON n.channel_id = a.channel_id
$blocked_sql_filter
ORDER BY $order DESC, total DESC
LIMIT $limit;
SQL
