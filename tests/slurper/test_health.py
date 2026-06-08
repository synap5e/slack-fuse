"""Health emitter: writes a `slurper-health` event AND mirrors to `health_log`.

Acceptance criteria 3 & 4. Async bodies run via `trio.run` so the suite needs
no pytest-trio mode configured.
"""

from __future__ import annotations

import json

import psycopg
import trio
from psycopg.rows import TupleRow

from slack_fuse_server.slurper.health import HealthEmitter, HealthKind
from slack_fuse_server.slurper.offsets import OffsetWriter


def _events(conn: psycopg.Connection[TupleRow], stream: str) -> list[tuple[int, str, object]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT offset_in_stream, kind, payload FROM events WHERE stream = %s ORDER BY offset_in_stream",
            (stream,),
        )
        return [(int(r[0]), str(r[1]), r[2]) for r in cur.fetchall()]


def _health_log(conn: psycopg.Connection[TupleRow]) -> list[tuple[str, object]]:
    with conn.cursor() as cur:
        cur.execute("SELECT kind, payload FROM health_log ORDER BY id")
        return [(str(r[0]), r[1]) for r in cur.fetchall()]


def test_emit_writes_event_and_mirrors_health_log(server_conn: psycopg.Connection[TupleRow]) -> None:
    async def body() -> None:
        health = HealthEmitter(OffsetWriter(server_conn, trio.CapacityLimiter(1)))
        off1 = await health.emit(HealthKind.SLACK_HEALTHY)
        off2 = await health.emit(HealthKind.SOCKET_MODE_RECONNECTED, {"gap_seconds": 12.5})
        assert (off1, off2) == (1, 2)

    trio.run(body)

    events = _events(server_conn, "slurper-health")
    assert [(o, k) for o, k, _ in events] == [(1, "slack_healthy"), (2, "socket_mode_reconnected")]
    # Second event carries its payload.
    assert events[1][2] == {"gap_seconds": 12.5}

    log = _health_log(server_conn)
    assert log == [("slack_healthy", {}), ("socket_mode_reconnected", {"gap_seconds": 12.5})]
    # health_log mirror and event payload agree.
    assert json.dumps(log[1][1]) == json.dumps(events[1][2])
