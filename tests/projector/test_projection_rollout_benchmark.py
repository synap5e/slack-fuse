"""Rollout evidence for the projection-ledger reader cutover.

This is intentionally a pytest benchmark rather than a hard performance gate:
it records before/after callback distributions under real PostgreSQL writes,
plus the fail-closed layout-churn window. Bounds only catch catastrophic
regressions; the emitted p50/p95/p99 values are the rollout artifact.
"""

from __future__ import annotations

import functools
import logging
import math
import time
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, cast
from zoneinfo import ZoneInfo

import pytest
import trio
from psycopg.rows import TupleRow

from slack_fuse.fuse_ops_v2 import SlackFuseOpsV2
from slack_fuse.models import JsonObject
from slack_fuse.projector.apply import apply_event
from slack_fuse.projector.disk_projection import DiskProjection
from slack_fuse.projector.pool import ConnectionPool
from slack_fuse.projector.projection_ledger import RENDERER_VERSION, TargetKey, bump_targets
from slack_fuse_server.wire.frames import EventFrame
from tests.fuse_v2.conftest import mark_stream_caught_up, seed_channel, seed_chunk, set_connection_state

if TYPE_CHECKING:
    from collections.abc import Sequence

    from psycopg import Connection

    from tests.projector.conftest import ClientConnFactory


pytestmark = pytest.mark.benchmark

log = logging.getLogger(__name__)

_TZ = ZoneInfo("UTC")
_DAY = date(2026, 8, 2)
_TS = Decimal("1785661200.000000")
_LAYOUT = TargetKey("layout", None, None, None)
_READ_CHANNEL_COUNT = 16
_EVENT_CHANNEL_COUNT = 16
_SAMPLES = 1000
_READ_SIZE = 131072


@dataclass(frozen=True, slots=True)
class _Distribution:
    samples: int
    duration_s: float
    p50_ms: float
    p95_ms: float
    p99_ms: float


@dataclass(frozen=True, slots=True)
class _SustainedWorkload:
    apply_conns: tuple[Connection[TupleRow], Connection[TupleRow]]
    projection: DiskProjection
    event_channels: tuple[str, ...]
    offsets: dict[str, int]


def _percentile(sorted_values: Sequence[float], fraction: float) -> float:
    position = (len(sorted_values) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return sorted_values[lower]
    weight = position - lower
    return sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight


def _distribution(latencies_ms: list[float], duration_s: float) -> _Distribution:
    values = sorted(latencies_ms)
    return _Distribution(
        samples=len(values),
        duration_s=duration_s,
        p50_ms=_percentile(values, 0.50),
        p95_ms=_percentile(values, 0.95),
        p99_ms=_percentile(values, 0.99),
    )


def _set_target(
    conn: Connection[TupleRow],
    key: TargetKey,
    *,
    target_generation: int,
    rendered_generation: int,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO projection_targets ("
            " target_kind, channel_id, local_day, thread_ts, target_generation, "
            " rendered_generation, renderer_version"
            ") VALUES (%s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT ON CONSTRAINT projection_targets_identity DO UPDATE SET "
            " target_generation = EXCLUDED.target_generation, "
            " rendered_generation = EXCLUDED.rendered_generation, "
            " renderer_version = EXCLUDED.renderer_version, updated_at = now()",
            (
                key.target_kind,
                key.channel_id,
                key.local_day,
                key.thread_ts,
                target_generation,
                rendered_generation,
                RENDERER_VERSION,
            ),
        )


def _message_frame(channel_id: str, offset: int, text: str) -> EventFrame:
    payload = cast(
        "JsonObject",
        {"type": "message", "ts": str(_TS), "user": "U1", "text": text},
    )
    return EventFrame(
        stream=f"channel:{channel_id}",
        offset=offset,
        kind="message",
        ts=str(_TS),
        payload=payload,
    )


def _seed_benchmark_world(
    conn: Connection[TupleRow],
    projection: DiskProjection,
) -> tuple[list[str], tuple[str, ...]]:
    _set_target(conn, _LAYOUT, target_generation=1, rendered_generation=1)
    read_paths: list[str] = []
    for index in range(_READ_CHANNEL_COUNT):
        channel_id = f"CBREAD{index:02d}"
        name = f"bench-read-{index:02d}"
        seed_channel(conn, channel_id, name, tier="hot")
        seed_chunk(conn, channel_id, _TS, f"## 09:00 @bot\n\nread payload {index}\n")
        mark_stream_caught_up(conn, f"channel:{channel_id}", at_offset=1)
        key = TargetKey("day", channel_id, _DAY, None)
        _set_target(conn, key, target_generation=1, rendered_generation=0)
        projection.mark_target_dirty(key)
        read_paths.append(f"/channels/{name}/2026-08/02/channel.md")
    event_channels: list[str] = []
    for index in range(_EVENT_CHANNEL_COUNT):
        channel_id = f"CBEVENT{index:02d}"
        seed_channel(conn, channel_id, f"bench-event-{index:02d}", tier="hot")
        event_channels.append(channel_id)
    set_connection_state(conn, last_slurper_health="healthy", last_frame_at_offset_s=1.0)
    assert len(projection.flush_dirty(_READ_CHANNEL_COUNT)) == _READ_CHANNEL_COUNT
    return read_paths, tuple(event_channels)


async def _flush_pending_loop(projection: DiskProjection, *, reconcile_layout: bool) -> None:
    while True:
        if reconcile_layout:
            _ = await trio.to_thread.run_sync(projection.reconcile_layout)
        _ = await trio.to_thread.run_sync(projection.discover_pending, 256)
        _ = await trio.to_thread.run_sync(projection.flush_dirty, 256)
        await trio.sleep(0)


def _apply_one_sync(
    conn: Connection[TupleRow],
    projection: DiskProjection,
    frame: EventFrame,
    started: trio.Event,
) -> None:
    trio.from_thread.run_sync(started.set)
    _ = apply_event(conn, frame, tz=_TZ, projection=projection)


async def _measure_reads(
    ops: SlackFuseOpsV2,
    inodes: list[int],
    *,
    pace_s: float = 0.0,
) -> tuple[list[float], float]:
    latencies_ms: list[float] = []
    started_phase = time.perf_counter()
    for sample in range(_SAMPLES):
        started_read = time.perf_counter()
        content = await ops.read(inodes[sample % len(inodes)], 0, _READ_SIZE)
        latencies_ms.append((time.perf_counter() - started_read) * 1000)
        assert content
        if pace_s:
            await trio.sleep(pace_s)
    return latencies_ms, time.perf_counter() - started_phase


async def _measure_sustained_cycles(
    ops: SlackFuseOpsV2,
    inodes: list[int],
    workload: _SustainedWorkload,
) -> tuple[list[float], float]:
    latencies_ms: list[float] = []
    started_phase = time.perf_counter()
    for sample in range(_SAMPLES):
        channel_id = workload.event_channels[sample % len(workload.event_channels)]
        workload.offsets[channel_id] += 1
        frame = _message_frame(channel_id, workload.offsets[channel_id], f"event {sample}")
        apply_started = trio.Event()
        call = functools.partial(
            _apply_one_sync,
            workload.apply_conns[sample % len(workload.apply_conns)],
            workload.projection,
            frame,
            apply_started,
        )
        async with trio.open_nursery() as cycle_nursery:
            cycle_nursery.start_soon(trio.to_thread.run_sync, call)
            await apply_started.wait()
            started_read = time.perf_counter()
            content = await ops.read(inodes[sample % len(inodes)], 0, _READ_SIZE)
            latencies_ms.append((time.perf_counter() - started_read) * 1000)
            assert content
    return latencies_ms, time.perf_counter() - started_phase


async def _run_sustained_phase(
    ops: SlackFuseOpsV2,
    inodes: list[int],
    workload: _SustainedWorkload,
) -> _Distribution:
    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            functools.partial(
                _flush_pending_loop,
                workload.projection,
                reconcile_layout=False,
            )
        )
        latencies_ms, duration_s = await _measure_sustained_cycles(ops, inodes, workload)
        nursery.cancel_scope.cancel()
    return _distribution(latencies_ms, duration_s)


def _bump_layout(conn: Connection[TupleRow]) -> None:
    with conn.transaction(), conn.cursor() as cur:
        bump_targets(cur, (_LAYOUT,), RENDERER_VERSION)


async def _layout_churn(conn: Connection[TupleRow]) -> None:
    while True:
        await trio.sleep(0.1)
        await trio.to_thread.run_sync(_bump_layout, conn)


async def _run_churn_phase(
    ops: SlackFuseOpsV2,
    inodes: list[int],
    projection: DiskProjection,
    churn_conn: Connection[TupleRow],
) -> _Distribution:
    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            functools.partial(_flush_pending_loop, projection, reconcile_layout=True)
        )
        nursery.start_soon(_layout_churn, churn_conn)
        latencies_ms, duration_s = await _measure_reads(ops, inodes, pace_s=0.001)
        nursery.cancel_scope.cancel()
    return _distribution(latencies_ms, duration_s)


def _log_distribution(workload: str, phase: str, distribution: _Distribution) -> None:
    log.info(
        "projector-benchmark workload=%s phase=%s samples=%d duration_s=%.3f "
        "p50_ms=%.3f p95_ms=%.3f p99_ms=%.3f",
        workload,
        phase,
        distribution.samples,
        distribution.duration_s,
        distribution.p50_ms,
        distribution.p95_ms,
        distribution.p99_ms,
    )


@pytest.mark.trio
async def test_projection_rollout_latency_distributions(
    client_conn_factory: ClientConnFactory,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING, logger="slack_fuse.fuse_ops_v2")
    caplog.set_level(logging.WARNING, logger="slack_fuse.projector.disk_projection")
    caplog.set_level(logging.INFO, logger=__name__)
    projection_conn = client_conn_factory()
    inode_conn = client_conn_factory()
    apply_conns = (client_conn_factory(), client_conn_factory())
    churn_conn = client_conn_factory()
    projection = DiskProjection(projection_conn, _TZ, root=tmp_path / "projection")
    read_paths, event_channels = _seed_benchmark_world(projection_conn, projection)
    pool = ConnectionPool(client_conn_factory, max_size=4)
    baseline_ops = SlackFuseOpsV2(
        inode_conn,
        _TZ,
        trio.CapacityLimiter(16),
        pool=pool,
        disk_projection=projection,
        disk_projection_enabled=False,
        trailer_enabled=False,
    )
    projected_ops = SlackFuseOpsV2(
        inode_conn,
        _TZ,
        trio.CapacityLimiter(16),
        pool=pool,
        disk_projection=projection,
        disk_projection_enabled=True,
        trailer_enabled=False,
    )
    baseline_inodes = [baseline_ops.inodes.get_or_create(path) for path in read_paths]
    projected_inodes = [projected_ops.inodes.get_or_create(path) for path in read_paths]
    offsets = dict.fromkeys(event_channels, 0)
    try:
        before = await _run_sustained_phase(
            baseline_ops,
            baseline_inodes,
            _SustainedWorkload(apply_conns, projection, event_channels, offsets),
        )
        after = await _run_sustained_phase(
            projected_ops,
            projected_inodes,
            _SustainedWorkload(apply_conns, projection, event_channels, offsets),
        )
        churn = await _run_churn_phase(projected_ops, projected_inodes, projection, churn_conn)
    finally:
        await pool.aclose()

    _log_distribution("sustained-disjoint-events", "before-jit", before)
    _log_distribution("sustained-disjoint-events", "after-ledger", after)
    _log_distribution("channel-list-churn-100ms", "after-ledger", churn)

    for distribution in (before, after, churn):
        assert distribution.samples == _SAMPLES
        assert distribution.duration_s > 0
        assert distribution.p50_ms <= distribution.p95_ms <= distribution.p99_ms
        assert distribution.p99_ms < 1000.0
