#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["psycopg[binary]>=3.2"]
# ///
"""Edge-triggered liveness watch for slack-fuse, meant to run under cmdwatch with --on-match 'ALERT|RECOVERED'.

Two independent checks, because each misses the other's failure:

- mount: connection_state.last_frame_at. The server pings every 30s, so a healthy client is never more than ~30s
  stale. Catches a dead server, a dead network path, or a stuck client. Misses an ingest stop: pings keep flowing.
- ingest: the newest NATS-transport event in the server's event log (k8s-homelab). Catches a wedged NATS shim
  (2026-10-06: 23 min of silence while every other signal stayed green). Threshold 60 min: the longest quiet gap in
  16 days of history was 52 min, so night-time quiet does not page.

Prints ALERT on a transition to bad and RECOVERED on the way back, plus an hourly heartbeat.
"""

from __future__ import annotations

import subprocess
import time
import tomllib
from datetime import UTC, datetime
from pathlib import Path

import psycopg

CONFIG = Path.home() / ".config/slack-fuse/config.toml"
THRESHOLD_S = 180
INGEST_THRESHOLD_S = 3600
INGEST_EVERY = 5
POLL_S = 60
HEARTBEAT_S = 3600
KUBECTL = ["kubectl", "--context", "k8s-homelab", "-n", "apps"]
INGEST_SQL = (
    "SELECT coalesce(extract(epoch FROM now() - max(created_at))::int, -1) FROM events "
    "WHERE source->>'transport' = 'nats' AND id > (SELECT max(id) - 50000 FROM events)"
)


def log(msg: str) -> None:
    print(f"{datetime.now(UTC):%Y-%m-%dT%H:%M:%SZ} {msg}", flush=True)


def check(dsn: str) -> tuple[bool, str]:
    try:
        with psycopg.connect(dsn, connect_timeout=10) as conn:
            row = conn.execute(
                "SELECT last_frame_at, extract(epoch FROM now() - last_frame_at) FROM connection_state WHERE id = 1"
            ).fetchone()
    except psycopg.Error as exc:
        return False, f"local projection PG unreachable: {type(exc).__name__}: {exc}".splitlines()[0]
    if row is None or row[0] is None:
        return False, "connection_state.last_frame_at is NULL"
    age = float(row[1])
    detail = f"last_frame_at={row[0]:%Y-%m-%dT%H:%M:%SZ} age={age:.0f}s"
    return age <= THRESHOLD_S, detail


def check_ingest() -> tuple[bool | None, str]:
    """None = could not tell (cluster unreachable); not an alert by itself."""
    try:
        pod = subprocess.run(
            [*KUBECTL, "get", "pod", "-l", "app=slack-fuse-postgres", "-o", "name"],
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        ).stdout.split()[0]
        out = subprocess.run(
            [*KUBECTL, "exec", pod, "--", "psql", "-U", "slack_fuse", "-d", "slack_fuse_server", "-Atc", INGEST_SQL],
            capture_output=True,
            text=True,
            timeout=60,
            check=True,
        ).stdout.strip()
    except (subprocess.SubprocessError, OSError, IndexError) as exc:
        return None, f"server event log unreadable: {type(exc).__name__}"
    age = int(out)
    detail = f"newest nats event age={age}s"
    return 0 <= age <= INGEST_THRESHOLD_S, detail


class Edge:
    def __init__(self, name: str) -> None:
        self.name = name
        self.healthy: bool | None = None
        self.last_beat = 0.0

    def report(self, ok: bool, detail: str, bad_text: str, good_text: str) -> None:
        if ok != self.healthy:
            if not ok:
                log(f"ALERT {bad_text}: {detail}")
            elif self.healthy is False:
                log(f"RECOVERED {good_text}: {detail}")
            else:
                log(f"ok {self.name} {detail}")
            self.healthy = ok
            self.last_beat = time.monotonic()
        elif time.monotonic() - self.last_beat >= HEARTBEAT_S:
            log(f"{'ok' if ok else 'still-bad'} {self.name} {detail}")
            self.last_beat = time.monotonic()


def main() -> None:
    dsn = tomllib.loads(CONFIG.read_text())["database_url"]
    mount, ingest = Edge("mount"), Edge("ingest")
    log(f"watching mount (>{THRESHOLD_S}s) and server ingest (>{INGEST_THRESHOLD_S}s), poll {POLL_S}s")
    tick = 0
    while True:
        ok, detail = check(dsn)
        mount.report(ok, detail, "slack-fuse mount not receiving from server", "slack-fuse mount receiving again")
        if tick % INGEST_EVERY == 0:
            ingest_ok, ingest_detail = check_ingest()
            if ingest_ok is None:
                log(f"note {ingest_detail}")
            else:
                ingest.report(
                    ingest_ok,
                    ingest_detail,
                    "slack-fuse server not ingesting from NATS",
                    "slack-fuse server ingesting again",
                )
        tick += 1
        time.sleep(POLL_S)


if __name__ == "__main__":
    main()
