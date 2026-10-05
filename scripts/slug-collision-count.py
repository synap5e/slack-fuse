#!/usr/bin/env -S uv run --no-sync --project /home/simon/agentic/slack-fuse --script
"""Count (channel, day) groups where v2's thread-slug dedup silently drops a thread.

Uses v2's own derivation (dedup_thread_slug_map with live mention resolution), so the count is exactly what the
mount does. A drop is any day whose returned map has fewer entries than it had parents.
"""

from __future__ import annotations

import tomllib
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

import psycopg

from slack_fuse.__main__ import _resolve_local_zoneinfo  # pyright: ignore[reportPrivateUsage]
from slack_fuse.fuse_v2_helpers import dedup_thread_slug_map, ts_to_local_date

dsn = tomllib.loads((Path.home() / ".config/slack-fuse/config.toml").read_text())["database_url"]
tz = _resolve_local_zoneinfo()
with psycopg.connect(dsn) as conn:
    rows = conn.execute(
        "SELECT channel_id, message_ts, content_md FROM chunks WHERE reply_count > 0 ORDER BY channel_id, message_ts"
    ).fetchall()
    groups: dict[tuple[str, object], list[tuple[Decimal, str]]] = defaultdict(list)
    for channel_id, ts, md in rows:
        groups[channel_id, ts_to_local_date(ts, tz)].append((ts, md))
    dropped_days = 0
    dropped_threads = 0
    examples: list[str] = []
    for (channel_id, day), parents in groups.items():
        if len(parents) < 2:
            continue
        got = dedup_thread_slug_map(parents, conn)
        lost = len(parents) - len(got)
        if lost:
            dropped_days += 1
            dropped_threads += lost
            if len(examples) < 5:
                examples.append(f"{channel_id} {day}: {len(parents)} parents -> {len(got)} slugs {sorted(got)[:4]}")
print(f"thread parents: {len(rows)}, channel-days with threads: {len(groups)}")
print(f"channel-days that drop a thread: {dropped_days}; threads dropped: {dropped_threads}")
for e in examples:
    print("  " + e)
