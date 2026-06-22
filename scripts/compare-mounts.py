#!/usr/bin/env python3
"""Compare v2 (split-mode) vs v1 (legacy) coverage and flag drift.

Reads the three layers without touching either FUSE mount:

- **Cluster events table** (postgres-server pod via ``kubectl exec``) —
  the canonical "what messages exist" source of truth.
- **v2 client projection** (local postgres ``slack_fuse_split`` DB) —
  what /views/slack-split serves.
- **v1 legacy cache** (``~/.cache/slack-fuse/messages/`` JSON files) —
  what /views/slack served until the polling service stopped writing.

For each channel: count messages on the cluster, count chunks in v2,
count messages in the v1 cache. Print a per-channel table sorted by
delta, plus aggregate totals. Flag the channels where v2 and v1 disagree.

This script is read-only — never writes, never modifies state. Safe to
run any time the cluster + local PG are reachable.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import subprocess
import sys
from pathlib import Path

import psycopg
from psycopg.rows import TupleRow

# =============================================================================
# Config
# =============================================================================

KUBECONTEXT = "admin@k8s-homelab"
NAMESPACE = "apps"
SERVER_DEPLOY = "slack-fuse-postgres"
SERVER_DB = "slack_fuse_server"
SERVER_USER = "slack_fuse"

LOCAL_PG_URL = (
    "postgresql:///slack_fuse_split?host=/run/user/1000/local-postgres&port=5433"
)
V1_CACHE_DIR = Path.home() / ".cache" / "slack-fuse" / "messages"


# =============================================================================
# Data classes
# =============================================================================


@dataclasses.dataclass(frozen=True)
class ChannelStats:
    channel_id: str
    name: str
    cluster_msgs: int = 0
    cluster_first_ts: float | None = None
    cluster_last_ts: float | None = None
    v2_top_chunks: int = 0
    v2_thread_chunks: int = 0
    v2_first_ts: float | None = None
    v2_last_ts: float | None = None
    v1_cache_files: int = 0
    v1_first_date: str | None = None
    v1_last_date: str | None = None
    v1_msgs: int = 0

    @property
    def v2_total(self) -> int:
        return self.v2_top_chunks + self.v2_thread_chunks

    @property
    def cluster_v2_delta(self) -> int:
        return self.cluster_msgs - self.v2_total

    @property
    def cluster_v1_delta(self) -> int:
        return self.cluster_msgs - self.v1_msgs


# =============================================================================
# Cluster (server-side) queries
# =============================================================================


def cluster_query(sql: str) -> str:
    r = subprocess.run(
        [
            "kubectl",
            "--context",
            KUBECONTEXT,
            "exec",
            "-n",
            NAMESPACE,
            f"deploy/{SERVER_DEPLOY}",
            "--",
            "psql",
            "-X",
            "-q",
            "-U",
            SERVER_USER,
            "-d",
            SERVER_DB,
            "-P",
            "pager=off",
            "-t",
            "-A",
            "-F",
            "\t",
            "-c",
            sql,
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    if r.returncode != 0:
        msg = f"cluster query failed: {r.stderr}"
        raise RuntimeError(msg)
    return r.stdout


def fetch_cluster_per_channel() -> dict[str, tuple[int, float | None, float | None]]:
    """{channel_id: (msg_count, first_ts_epoch, last_ts_epoch)}."""
    sql = """
        SELECT REPLACE(stream, 'channel:', '') AS chid,
               count(*) FILTER (WHERE kind='message')::bigint AS msgs,
               min(ts::numeric) FILTER (WHERE kind='message') AS first_ts,
               max(ts::numeric) FILTER (WHERE kind='message') AS last_ts
        FROM events
        WHERE stream LIKE 'channel:%'
          AND ts IS NOT NULL
        GROUP BY stream
    """
    out: dict[str, tuple[int, float | None, float | None]] = {}
    for line in cluster_query(sql).splitlines():
        parts = line.split("\t")
        if len(parts) < 4:
            continue
        chid, msgs, first_ts, last_ts = parts[0], parts[1], parts[2], parts[3]
        if not chid:
            continue
        out[chid] = (
            int(msgs) if msgs.isdigit() else 0,
            float(first_ts) if first_ts else None,
            float(last_ts) if last_ts else None,
        )
    return out


def fetch_cluster_channel_names() -> dict[str, str]:
    """Resolve channel_id → name from the latest channel_added/renamed event."""
    sql = """
        SELECT DISTINCT ON (payload->>'id')
               payload->>'id' AS chid,
               payload->>'name' AS name
        FROM events
        WHERE stream = 'channel-list'
          AND kind IN ('channel_added','channel_renamed')
          AND payload ? 'id'
        ORDER BY payload->>'id', id DESC
    """
    out: dict[str, str] = {}
    for line in cluster_query(sql).splitlines():
        parts = line.split("\t")
        if len(parts) >= 2 and parts[0]:
            out[parts[0]] = parts[1] or "?"
    return out


# =============================================================================
# v2 client projection queries
# =============================================================================


def fetch_v2_per_channel() -> dict[str, tuple[int, int, float | None, float | None]]:
    """{channel_id: (top_chunks, thread_replies, first_ts, last_ts)}."""
    with psycopg.connect(LOCAL_PG_URL) as conn:
        with conn.cursor() as cur:
            _ = cur.execute(
                """
                SELECT channel_id,
                       count(*)::bigint AS top_chunks,
                       min(message_ts)::float AS first_ts,
                       max(message_ts)::float AS last_ts
                FROM chunks
                GROUP BY channel_id
                """
            )
            chunks: dict[str, tuple[int, float | None, float | None]] = {}
            for chid, n, first_ts, last_ts in cur.fetchall():
                chunks[str(chid)] = (
                    int(n),
                    float(first_ts) if first_ts is not None else None,
                    float(last_ts) if last_ts is not None else None,
                )
            _ = cur.execute(
                """
                SELECT channel_id, count(*)::bigint
                FROM thread_chunks
                WHERE role = 'reply'
                GROUP BY channel_id
                """
            )
            threads: dict[str, int] = {str(c): int(n) for c, n in cur.fetchall()}
    out: dict[str, tuple[int, int, float | None, float | None]] = {}
    for chid, (top, first_ts, last_ts) in chunks.items():
        out[chid] = (top, threads.get(chid, 0), first_ts, last_ts)
    for chid, n in threads.items():
        if chid not in out:
            out[chid] = (0, n, None, None)
    return out


# =============================================================================
# v1 legacy cache scan
# =============================================================================


def scan_v1_cache() -> dict[str, tuple[int, str | None, str | None, int]]:
    """{channel_id: (day_file_count, first_date, last_date, message_count)}.

    Each ``YYYY-MM-DD.json`` file contains a JSON array of message dicts.
    """
    out: dict[str, tuple[int, str | None, str | None, int]] = {}
    if not V1_CACHE_DIR.exists():
        return out
    for chan_dir in sorted(V1_CACHE_DIR.iterdir()):
        if not chan_dir.is_dir():
            continue
        chid = chan_dir.name
        if not chid or not chid[0] in "CDG":
            continue
        day_files = sorted(p for p in chan_dir.iterdir() if p.name.endswith(".json") and not p.name.endswith(".done"))
        # Filter to actual YYYY-MM-DD.json files.
        day_files = [p for p in day_files if len(p.stem) == 10 and p.stem[4] == "-" and p.stem[7] == "-"]
        if not day_files:
            out[chid] = (0, None, None, 0)
            continue
        # Count messages by reading each file. This is slow for big channels
        # but accurate. We cap at 2000 messages per channel to keep total
        # runtime bounded — channels above that are firehoses we mostly
        # already know about.
        msg_count = 0
        for p in day_files:
            try:
                data = json.loads(p.read_text())
                if isinstance(data, list):
                    msg_count += len(data)
            except (OSError, json.JSONDecodeError):
                continue
        out[chid] = (
            len(day_files),
            day_files[0].stem,
            day_files[-1].stem,
            msg_count,
        )
    return out


# =============================================================================
# Comparison + reporting
# =============================================================================


def collate(
    cluster: dict[str, tuple[int, float | None, float | None]],
    v2: dict[str, tuple[int, int, float | None, float | None]],
    v1: dict[str, tuple[int, str | None, str | None, int]],
    names: dict[str, str],
) -> list[ChannelStats]:
    all_ids = set(cluster) | set(v2) | set(v1)
    rows: list[ChannelStats] = []
    for chid in all_ids:
        cm, cf, cl = cluster.get(chid, (0, None, None))
        top, thr, vf, vl = v2.get(chid, (0, 0, None, None))
        files, v1_first, v1_last, v1_msgs = v1.get(chid, (0, None, None, 0))
        rows.append(
            ChannelStats(
                channel_id=chid,
                name=names.get(chid, "?"),
                cluster_msgs=cm,
                cluster_first_ts=cf,
                cluster_last_ts=cl,
                v2_top_chunks=top,
                v2_thread_chunks=thr,
                v2_first_ts=vf,
                v2_last_ts=vl,
                v1_cache_files=files,
                v1_first_date=v1_first,
                v1_last_date=v1_last,
                v1_msgs=v1_msgs,
            )
        )
    return rows


def fmt_ts(ts: float | None) -> str:
    if ts is None:
        return "-"
    import datetime as dt  # noqa: PLC0415 — local import keeps this helper cheap to import

    return dt.datetime.fromtimestamp(ts, tz=dt.UTC).strftime("%Y-%m-%d")


def summarize(rows: list[ChannelStats]) -> None:
    n = len(rows)
    cluster_total = sum(r.cluster_msgs for r in rows)
    v2_total = sum(r.v2_total for r in rows)
    v1_total = sum(r.v1_msgs for r in rows)
    cluster_chans = sum(1 for r in rows if r.cluster_msgs > 0)
    v2_chans = sum(1 for r in rows if r.v2_total > 0)
    v1_chans = sum(1 for r in rows if r.v1_msgs > 0)

    print("=" * 70)
    print("  AGGREGATE")
    print("=" * 70)
    print(f"  channels seen                : {n}")
    print(f"  cluster: {cluster_chans:>5d} channels  /  {cluster_total:>9,d} messages")
    print(f"  v2     : {v2_chans:>5d} channels  /  {v2_total:>9,d} chunks+replies")
    print(f"  v1     : {v1_chans:>5d} channels  /  {v1_total:>9,d} cached messages")
    print(f"  cluster - v2 delta           : {cluster_total - v2_total:>9,d}")
    print(f"  cluster - v1 delta           : {cluster_total - v1_total:>9,d}")
    print(f"  v2 - v1 delta                : {v2_total - v1_total:>9,d}")
    print()

    # Cluster-vs-v2 should be ~0 for channels v2 subscribes to. Channels
    # in always_blocked won't have chunks. Channels v2 hasn't subscribed
    # to yet (cold tier) also won't.
    drift_v2 = [r for r in rows if r.cluster_msgs > 0 and r.v2_total == 0]
    drift_v1 = [r for r in rows if r.cluster_msgs > 0 and r.v1_msgs == 0]
    print("=" * 70)
    print("  COVERAGE GAPS (cluster has messages, projection has nothing)")
    print("=" * 70)
    print(f"  channels in cluster but missing from v2 : {len(drift_v2)}")
    print(f"  channels in cluster but missing from v1 : {len(drift_v1)}")
    print()


def print_table(rows: list[ChannelStats], *, top_n: int = 30, by: str = "cluster") -> None:
    print("=" * 100)
    print(f"  TOP {top_n} CHANNELS BY {by} message count")
    print("=" * 100)
    key: dict[str, callable[[ChannelStats], int]] = {  # type: ignore[type-arg]
        "cluster": lambda r: r.cluster_msgs,
        "v2": lambda r: r.v2_total,
        "v1": lambda r: r.v1_msgs,
        "delta_v2": lambda r: abs(r.cluster_v2_delta),
        "delta_v1": lambda r: abs(r.cluster_v1_delta),
    }
    rows_sorted = sorted(rows, key=key[by], reverse=True)[:top_n]
    header = (
        f"  {'name':<32s}  {'cluster':>9s}  {'v2':>9s}  {'v1':>9s}  "
        f"{'c-v2':>7s}  {'c-v1':>7s}  {'v2 last':<10s}  {'v1 last':<10s}"
    )
    print(header)
    print("  " + "-" * (len(header) - 2))
    for r in rows_sorted:
        name = (r.name or "?")[:30]
        print(
            f"  {name:<32s}  {r.cluster_msgs:>9,d}  {r.v2_total:>9,d}  {r.v1_msgs:>9,d}  "
            f"{r.cluster_v2_delta:>+7,d}  {r.cluster_v1_delta:>+7,d}  "
            f"{fmt_ts(r.v2_last_ts):<10s}  {r.v1_last_date or '-':<10s}"
        )
    print()


# =============================================================================
# Entry point
# =============================================================================


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--top", type=int, default=20, help="rows per table (default 20)")
    parser.add_argument("--by", default="cluster", choices=["cluster", "v2", "v1", "delta_v2", "delta_v1"])
    parser.add_argument("--skip-v1", action="store_true", help="skip the legacy cache scan (faster)")
    parser.add_argument("--json", help="also write per-channel rows as JSON to this path")
    args = parser.parse_args(argv)

    print("# fetching cluster …", file=sys.stderr)
    cluster = fetch_cluster_per_channel()
    names = fetch_cluster_channel_names()
    print(f"# cluster: {len(cluster)} channel streams, {len(names)} known names", file=sys.stderr)

    print("# fetching v2 projection …", file=sys.stderr)
    v2 = fetch_v2_per_channel()
    print(f"# v2: {len(v2)} channels in chunks/thread_chunks", file=sys.stderr)

    if args.skip_v1:
        v1 = {}
    else:
        print("# scanning v1 cache (may take a few seconds) …", file=sys.stderr)
        v1 = scan_v1_cache()
        print(f"# v1: {len(v1)} channel dirs in {V1_CACHE_DIR}", file=sys.stderr)

    rows = collate(cluster, v2, v1, names)
    summarize(rows)
    print_table(rows, top_n=args.top, by=args.by)

    if args.json:
        Path(args.json).write_text(
            json.dumps([dataclasses.asdict(r) for r in rows], indent=2, default=str)
        )
        print(f"# wrote {args.json}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
