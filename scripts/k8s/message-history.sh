#!/usr/bin/env bash
# scripts/k8s/message-history.sh
#
# Read-only operator tool for inspecting the slack-fuse-server `events` table.
# The projection (chunks/thread_chunks) only carries the *current* state of a
# message; the events table is the append-only source of truth, so edits and
# deletes are recoverable by querying it directly.
#
# Three subcommands:
#
#   edits   [--channel CHID] [--limit N] [--since 'INTERVAL']
#           Recent message_changed events. Columns:
#             id | stream | when | ts | old → new (truncated)
#
#   deletes [--channel CHID] [--limit N] [--since 'INTERVAL']
#           Recent message_deleted events. Columns:
#             id | stream | when | deleted_ts | last_known_text
#
#   show <event-id>            (resolves stream+ts from the row)
#   show <channel-id> <ts>     (direct)
#           Full timeline for one message: post → edits → delete. Each row
#           shows the kind, when it was recorded, who edited (if known),
#           and the text snapshot.
#
# Examples:
#   scripts/k8s/message-history.sh edits --limit 5
#   scripts/k8s/message-history.sh edits --channel C046S4RH6GG --since '24 hours'
#   scripts/k8s/message-history.sh deletes --limit 20
#   scripts/k8s/message-history.sh show C046S4RH6GG 1708802304.438289
#   scripts/k8s/message-history.sh show 23415
#
# All queries run against the cluster postgres pod via `kubectl exec`. The
# events table is append-only; this script is read-only.

set -euo pipefail

CONTEXT="${KUBECONTEXT:-admin@k8s-homelab}"
NAMESPACE="${NAMESPACE:-apps}"
PG_DEPLOY="${PG_DEPLOY:-slack-fuse-postgres}"
PG_USER="${PG_USER:-slack_fuse}"
PG_DB="${PG_DB:-slack_fuse_server}"

psql_exec() {
    # Single-shot SQL via kubectl exec. -q suppresses NOTICE chatter,
    # -X skips ~/.psqlrc inside the pod.
    kubectl --context "$CONTEXT" exec -i -n "$NAMESPACE" "deploy/$PG_DEPLOY" -- \
        psql -X -q -U "$PG_USER" -d "$PG_DB" -P pager=off "$@"
}

usage() {
    sed -n '4,28p' "$0" | sed 's/^# \{0,1\}//'
    exit 64
}

cmd="${1:-}"; shift || usage

case "$cmd" in
    edits|deletes)
        # message_changed | message_deleted listing
        channel=""
        limit=20
        since="7 days"
        while [ $# -gt 0 ]; do
            case "$1" in
                --channel) channel="$2"; shift 2 ;;
                --limit) limit="$2"; shift 2 ;;
                --since) since="$2"; shift 2 ;;
                *) echo "unknown flag: $1" >&2; usage ;;
            esac
        done

        kind=$([ "$cmd" = "edits" ] && echo "message_changed" || echo "message_deleted")

        # Channel filter: pass as parametrized literal, not interpolated SQL.
        if [ -n "$channel" ]; then
            stream_filter="AND stream = 'channel:${channel//\'/}'"
        else
            stream_filter=""
        fi

        # Note: the slurper currently drops `previous_message` on
        # `message_changed` (socket.py:144) but keeps it on `message_deleted`
        # (line 154). For edits we show only the new text + editor; the
        # historical text lives in the corresponding `message` row, surfaced
        # via `show <id>`.
        if [ "$cmd" = "edits" ]; then
            text_expr="coalesce(payload->'message'->'edited'->>'user', '(bot/unfurl)') ||
                       ': ' ||
                       coalesce(substring(payload->'message'->>'text' for 90), '(empty/file-only)')"
            ts_label="ts"
        else
            text_expr="coalesce(substring(payload->'previous_message'->>'text' for 90),
                                '(no previous_message)')"
            ts_label="deleted_ts"
        fi

        psql_exec <<SQL
SELECT id,
       stream,
       to_char(created_at, 'YYYY-MM-DD HH24:MI:SS') AS when,
       ts AS $ts_label,
       $text_expr AS text
FROM events
WHERE kind = '$kind'
  AND created_at >= now() - interval '$since'
  $stream_filter
ORDER BY id DESC
LIMIT $limit;
SQL
        ;;

    show)
        # Resolve to (stream, ts) — either from event_id or explicit args
        if [ $# -eq 1 ] && [[ "$1" =~ ^[0-9]+$ ]]; then
            event_id="$1"
            # Look up the row's stream + ts. If the row has a NULL ts (e.g. a
            # channel-list event) we'll get no history rows — that's intended.
            read -r stream ts < <(
                psql_exec -t -A -F' ' <<SQL
SELECT stream, coalesce(ts, '')
FROM events WHERE id = $event_id;
SQL
            )
            if [ -z "${stream:-}" ]; then
                echo "no event with id=$event_id" >&2
                exit 1
            fi
            if [ -z "${ts:-}" ]; then
                echo "event $event_id has NULL ts (not a message event); cannot trace history" >&2
                exit 1
            fi
        elif [ $# -eq 2 ]; then
            channel="$1"
            ts="$2"
            stream="channel:$channel"
        else
            echo "usage: $0 show <event-id> | <channel-id> <ts>" >&2
            usage
        fi

        echo "stream=$stream ts=$ts"
        echo "---"

        # Each kind exposes the message text differently:
        # - message            → payload->>'text'
        # - message_changed    → payload->'message'->>'text' (new); previous in payload->'previous_message'->>'text'
        # - message_deleted    → payload->'previous_message'->>'text' (last-known)
        # 'editor' is the editing user, only set on message_changed.
        psql_exec <<SQL
SELECT id,
       kind,
       to_char(created_at, 'YYYY-MM-DD HH24:MI:SS') AS when,
       coalesce(
           payload->'message'->'edited'->>'user',
           payload->>'user',
           ''
       ) AS actor,
       CASE kind
         WHEN 'message' THEN coalesce(substring(payload->>'text' for 220), '(empty)')
         WHEN 'message_changed' THEN
             '(new)     ' || coalesce(substring(payload->'message'->>'text' for 200), '(empty)')
         WHEN 'message_deleted' THEN
             '(deleted; last) ' || coalesce(substring(payload->'previous_message'->>'text' for 200), '(no previous_message)')
         ELSE payload::text
       END AS text
FROM events
WHERE stream = '${stream//\'/}'
  AND ts = '${ts//\'/}'
  AND kind IN ('message', 'message_changed', 'message_deleted')
ORDER BY id ASC;
SQL
        ;;

    -h|--help|help|"")
        usage
        ;;

    *)
        echo "unknown subcommand: $cmd" >&2
        usage
        ;;
esac
