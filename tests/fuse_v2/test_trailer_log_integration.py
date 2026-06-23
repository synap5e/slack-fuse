"""Integration tests for the trailer-decision JSONL log + config wiring.

Sprint 3C, Parts A2/A3 (per-read JSONL emission through ``SlackFuseOpsV2.read``)
and B4 (the three ``ClientConfig`` knobs actually changing behaviour). These go
through the real read path against a migrated client DB.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import pytest
import trio

from slack_fuse.fuse_ops_v2 import SlackFuseOpsV2
from slack_fuse.projector.health_subscriber import read_signature, watch_health_once
from slack_fuse.projector.trailer import FALLBACK_CHANNEL_REASON, FALLBACK_USER_REASON
from slack_fuse.projector.trailer_log import TrailerDecision, TrailerLog, decision_from_json
from tests.fuse_v2.conftest import (
    FakePyfuse3,
    mark_stream_caught_up,
    seed_channel,
    seed_chunk,
    seed_user,
    set_connection_state,
)

if TYPE_CHECKING:
    from pathlib import Path

    from psycopg import Connection
    from psycopg.rows import TupleRow

_DAY_PATH = "/channels/general/2026-06/08/channel.md"


def _ts(dt: datetime) -> Decimal:
    return Decimal(str(dt.timestamp()))


def _seed_day(
    conn: Connection[TupleRow],
    *,
    body: str = "## 14:30 <@U1>\n\nHello world\n",
    mentioned_user_ids: tuple[str, ...] = ("U1",),
    mentioned_channel_ids: tuple[str, ...] = (),
    seed_u1: bool = True,
) -> None:
    seed_channel(conn, "C1", "general", tier="hot")
    if seed_u1:
        seed_user(conn, "U1", "alice")
    seed_chunk(
        conn,
        "C1",
        _ts(datetime(2026, 6, 8, 14, 30, tzinfo=UTC)),
        body,
        mentioned_user_ids=mentioned_user_ids,
        mentioned_channel_ids=mentioned_channel_ids,
    )


def _make_ops(
    conn: Connection[TupleRow],
    fake: FakePyfuse3,
    *,
    trailer_log: TrailerLog | None = None,
    stale_after_s: float = 60.0,
    trailer_enabled: bool = True,
) -> SlackFuseOpsV2:
    return SlackFuseOpsV2(
        conn=conn,
        local_tz=ZoneInfo("UTC"),
        limiter=trio.CapacityLimiter(1),
        notify_store=fake.notify_store,
        invalidate_inode=fake.invalidate_inode,
        trailer_log=trailer_log,
        stale_after_s=stale_after_s,
        trailer_enabled=trailer_enabled,
    )


@pytest.fixture
def log_path(tmp_path: Path) -> Path:
    return tmp_path / "trailer.jsonl"


def _decisions(path: Path) -> list[TrailerDecision]:
    if not path.exists():
        return []
    return [decision_from_json(json.loads(line)) for line in path.read_text(encoding="utf-8").splitlines()]


async def _read_day(ops: SlackFuseOpsV2) -> bytes:
    inode = ops.inodes.get_or_create(_DAY_PATH)
    return await ops.read(inode, 0, 200_000)


# ============================================================================
# Part A2/A3: per-read JSONL emission, one record per scenario
# ============================================================================


@pytest.mark.trio
async def test_read_emits_clean_decision(
    client_conn: Connection[TupleRow],
    fake_pyfuse3: FakePyfuse3,
    log_path: Path,
) -> None:
    _seed_day(client_conn)
    set_connection_state(client_conn, last_slurper_health="healthy", last_frame_at_offset_s=1.0)
    mark_stream_caught_up(client_conn, "channel:C1", at_offset=10)
    tlog = TrailerLog.open(log_path)
    try:
        content = await _read_day(_make_ops(client_conn, fake_pyfuse3, trailer_log=tlog))
    finally:
        tlog.close()

    assert b"Content may be stale" not in content
    decisions = _decisions(log_path)
    assert len(decisions) == 1
    assert decisions[0].kind == "clean"
    assert decisions[0].reasons == []
    assert decisions[0].stream == "channel:C1"
    assert decisions[0].inode is not None


@pytest.mark.trio
async def test_read_emits_stale_disconnect_decision(
    client_conn: Connection[TupleRow],
    fake_pyfuse3: FakePyfuse3,
    log_path: Path,
) -> None:
    _seed_day(client_conn)
    set_connection_state(client_conn, last_slurper_health="disconnected", last_frame_at_offset_s=1.0)
    mark_stream_caught_up(client_conn, "channel:C1", at_offset=10)
    tlog = TrailerLog.open(log_path)
    try:
        content = await _read_day(_make_ops(client_conn, fake_pyfuse3, trailer_log=tlog))
    finally:
        tlog.close()

    assert b"socket-mode disconnected" in content
    decision = _decisions(log_path)[-1]
    assert decision.kind == "stale"
    assert decision.reasons == ["socket-mode disconnected"]
    assert decision.last_health == "disconnected"


@pytest.mark.trio
async def test_read_emits_stale_old_frame_decision(
    client_conn: Connection[TupleRow],
    fake_pyfuse3: FakePyfuse3,
    log_path: Path,
) -> None:
    _seed_day(client_conn)
    # Healthy slurper, but no frame in 120s → server-unreachable at the 60s
    # default threshold.
    set_connection_state(client_conn, last_slurper_health="healthy", last_frame_at_offset_s=120.0)
    mark_stream_caught_up(client_conn, "channel:C1", at_offset=10)
    tlog = TrailerLog.open(log_path)
    try:
        content = await _read_day(_make_ops(client_conn, fake_pyfuse3, trailer_log=tlog))
    finally:
        tlog.close()

    assert b"server unreachable" in content
    decision = _decisions(log_path)[-1]
    assert decision.kind == "stale"
    assert decision.reasons == ["server unreachable"]


@pytest.mark.trio
async def test_read_emits_fallback_user_decision(
    client_conn: Connection[TupleRow],
    fake_pyfuse3: FakePyfuse3,
    log_path: Path,
) -> None:
    # Mention an un-seeded user → unresolved fallback (no trailer, but the read
    # must skip notify_store and log a fallback decision).
    _seed_day(
        client_conn,
        body="## 14:30 <@UGHOST>\n\nhi there\n",
        mentioned_user_ids=("UGHOST",),
        seed_u1=False,
    )
    set_connection_state(client_conn, last_slurper_health="healthy", last_frame_at_offset_s=1.0)
    mark_stream_caught_up(client_conn, "channel:C1", at_offset=10)
    tlog = TrailerLog.open(log_path)
    try:
        content = await _read_day(_make_ops(client_conn, fake_pyfuse3, trailer_log=tlog))
    finally:
        tlog.close()

    assert b"Content may be stale" not in content
    assert fake_pyfuse3.notify_calls == []  # fallback gates priming
    decision = _decisions(log_path)[-1]
    assert decision.kind == "fallback"
    assert decision.reasons == [FALLBACK_USER_REASON]


@pytest.mark.trio
async def test_read_emits_fallback_channel_decision(
    client_conn: Connection[TupleRow],
    fake_pyfuse3: FakePyfuse3,
    log_path: Path,
) -> None:
    _seed_day(
        client_conn,
        body="## 14:30 <@U1>\n\nsee <#CGHOST> for details\n",
        mentioned_user_ids=("U1",),
        mentioned_channel_ids=("CGHOST",),
    )
    set_connection_state(client_conn, last_slurper_health="healthy", last_frame_at_offset_s=1.0)
    mark_stream_caught_up(client_conn, "channel:C1", at_offset=10)
    tlog = TrailerLog.open(log_path)
    try:
        _ = await _read_day(_make_ops(client_conn, fake_pyfuse3, trailer_log=tlog))
    finally:
        tlog.close()

    decision = _decisions(log_path)[-1]
    assert decision.kind == "fallback"
    assert decision.reasons == [FALLBACK_CHANNEL_REASON]


@pytest.mark.trio
async def test_no_log_when_path_unset(
    client_conn: Connection[TupleRow],
    fake_pyfuse3: FakePyfuse3,
    log_path: Path,
) -> None:
    _seed_day(client_conn)
    set_connection_state(client_conn, last_slurper_health="healthy", last_frame_at_offset_s=1.0)
    mark_stream_caught_up(client_conn, "channel:C1", at_offset=10)
    # trailer_log=None → no file is created, read still works.
    content = await _read_day(_make_ops(client_conn, fake_pyfuse3, trailer_log=None))
    assert b"Hello world" in content
    assert not log_path.exists()


# ============================================================================
# Part B4: config knobs change behaviour
# ============================================================================


@pytest.mark.trio
async def test_b4_stale_after_disconnect_s_read_path(
    client_conn: Connection[TupleRow],
    fake_pyfuse3: FakePyfuse3,
) -> None:
    """A 45s-old frame is stale at a 30s threshold, clean at a 120s threshold —
    proving ``stale_after_disconnect_s`` is wired into the read path."""
    _seed_day(client_conn)
    set_connection_state(client_conn, last_slurper_health="healthy", last_frame_at_offset_s=45.0)
    mark_stream_caught_up(client_conn, "channel:C1", at_offset=10)

    strict = await _read_day(_make_ops(client_conn, fake_pyfuse3, stale_after_s=30.0))
    assert b"server unreachable" in strict

    lenient = await _read_day(_make_ops(client_conn, FakePyfuse3(), stale_after_s=120.0))
    assert b"Content may be stale" not in lenient


def test_b4_stale_after_disconnect_s_subscriber(client_conn: Connection[TupleRow]) -> None:
    """The health subscriber keys its ``frame_stale`` flip off the configured
    ``stale_after_s``: a 45s-old frame flips the signature at 30s but not 120s."""
    mark_stream_caught_up(client_conn, "channel:C1", at_offset=10)
    set_connection_state(client_conn, last_slurper_health="healthy", last_frame_at_offset_s=1.0)
    baseline_30 = read_signature(client_conn, stale_after_s=30.0)
    baseline_120 = read_signature(client_conn, stale_after_s=120.0)

    set_connection_state(client_conn, last_frame_at_offset_s=45.0)

    fired = [0]

    def cb() -> int:
        fired[0] += 1
        return 0

    _ = watch_health_once(client_conn, baseline_30, cb, stale_after_s=30.0)
    assert fired[0] == 1  # 45s > 30s → frame_stale flipped → invalidation fired

    fired[0] = 0
    _ = watch_health_once(client_conn, baseline_120, cb, stale_after_s=120.0)
    assert fired[0] == 0  # 45s < 120s → still fresh → no invalidation


@pytest.mark.trio
async def test_b4_stale_trailer_disabled_skips_trailer(
    client_conn: Connection[TupleRow],
    fake_pyfuse3: FakePyfuse3,
    log_path: Path,
) -> None:
    """``stale_trailer_enabled=False``: a disconnected read returns no trailer
    and is even primed, but the decision is still classified+logged as stale."""
    _seed_day(client_conn)
    set_connection_state(client_conn, last_slurper_health="disconnected", last_frame_at_offset_s=1.0)
    mark_stream_caught_up(client_conn, "channel:C1", at_offset=10)
    tlog = TrailerLog.open(log_path)
    try:
        content = await _read_day(_make_ops(client_conn, fake_pyfuse3, trailer_log=tlog, trailer_enabled=False))
    finally:
        tlog.close()

    assert b"Content may be stale" not in content
    assert b"socket-mode disconnected" not in content
    # Staleness no longer gates priming when the trailer is disabled.
    # 2026-06-24: notify_store calls were removed; the priming-decision is
    # still tracked via primed_inodes (asserted in the kernel_cache_invariants
    # suite). The classification check below is what this test is about.
    # ...but the classification is still recorded for false-positive analysis.
    decision = _decisions(log_path)[-1]
    assert decision.kind == "stale"
    assert decision.reasons == ["socket-mode disconnected"]


@pytest.mark.trio
async def test_b4_stale_trailer_enabled_default_appends_trailer(
    client_conn: Connection[TupleRow],
    fake_pyfuse3: FakePyfuse3,
) -> None:
    """Control for the previous test: with the trailer enabled (default), the
    same disconnected read DOES carry the trailer and is NOT primed."""
    _seed_day(client_conn)
    set_connection_state(client_conn, last_slurper_health="disconnected", last_frame_at_offset_s=1.0)
    mark_stream_caught_up(client_conn, "channel:C1", at_offset=10)
    content = await _read_day(_make_ops(client_conn, fake_pyfuse3, trailer_enabled=True))
    assert b"socket-mode disconnected" in content
    assert fake_pyfuse3.notify_calls == []


@pytest.mark.trio
async def test_boolean_caught_up_missing_trails(
    client_conn: Connection[TupleRow],
    fake_pyfuse3: FakePyfuse3,
) -> None:
    """Boolean "did initial replay complete" check: a stream with NO caught_up
    row trails ("catching up after reconnect"); a stream WITH any caught_up
    row stays clean (no time-window check)."""
    _seed_day(client_conn)
    set_connection_state(client_conn, last_slurper_health="healthy", last_frame_at_offset_s=1.0)

    # No caught_up row for channel:C1 → still catching up.
    stale = await _read_day(_make_ops(client_conn, fake_pyfuse3))
    assert b"catching up after reconnect" in stale

    # Insert a caught_up row of any age → clean (boolean-only, not windowed).
    mark_stream_caught_up(client_conn, "channel:C1", at_offset=10, seconds_ago=3600.0)
    fresh = await _read_day(_make_ops(client_conn, FakePyfuse3()))
    assert b"Content may be stale" not in fresh
