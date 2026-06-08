"""Unit tests for the pure trailer-decision module (Sprint 3C, Part A).

Covers the classifier matrix (``classify_trailer`` over a grid of state +
fallback combinations), the trailer-text renderer, and the JSONL log
round-trip. All pure — no DB, no pyfuse3.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from slack_fuse.projector.trailer import (
    FALLBACK_CHANNEL_REASON,
    FALLBACK_USER_REASON,
    StalenessState,
    TrailerDecision,
    classify_trailer,
    render_trailer,
    staleness_reason,
)
from slack_fuse.projector.trailer_log import (
    TrailerLog,
    decision_from_json,
    decision_to_json,
)

if TYPE_CHECKING:
    from pathlib import Path


_NOW = datetime(2026, 6, 8, 12, 0, tzinfo=UTC)


def _state(
    *,
    health: str = "healthy",
    frame_seconds_ago: float = 1.0,
    caught_up: bool = True,
    caught_up_seconds_ago: float | None = 1.0,
    caught_up_offset: int | None = 42,
) -> StalenessState:
    """Build a ``StalenessState`` relative to ``_NOW``.

    ``caught_up_seconds_ago=None`` leaves ``last_caught_up_at`` unset (the
    pre-3C boolean-only path); a float backdates the caught_up timestamp so the
    catch-up window can be crossed.
    """
    last_caught_up_at = None if caught_up_seconds_ago is None else _NOW - timedelta(seconds=caught_up_seconds_ago)
    return StalenessState(
        last_frame_at=_NOW - timedelta(seconds=frame_seconds_ago),
        last_slurper_health=health,
        last_health_update_at=_NOW,
        initial_catch_up_done_for_stream=caught_up,
        last_caught_up_at=last_caught_up_at,
        caught_up_offset=caught_up_offset,
    )


# ============================================================================
# classify_trailer matrix
# ============================================================================


def test_classify_clean() -> None:
    decision = classify_trailer(_state(), stream="channel:C1", now=_NOW)
    assert decision.kind == "clean"
    assert decision.reasons == []
    assert decision.stream == "channel:C1"
    assert decision.at == _NOW
    assert decision.caught_up_offset == 42
    assert decision.inode is None


def test_classify_stale_disconnected() -> None:
    decision = classify_trailer(_state(health="disconnected"), stream="channel:C1", now=_NOW)
    assert decision.kind == "stale"
    assert decision.reasons == ["socket-mode disconnected"]


def test_classify_stale_auth_failed() -> None:
    decision = classify_trailer(_state(health="auth_failed"), stream="channel:C1", now=_NOW)
    assert decision.kind == "stale"
    assert decision.reasons == ["auth token invalid"]


def test_classify_stale_degraded() -> None:
    decision = classify_trailer(_state(health="degraded"), stream="channel:C1", now=_NOW)
    assert decision.kind == "stale"
    assert decision.reasons == ["slack ingestion unhealthy"]


def test_classify_stale_old_frame() -> None:
    decision = classify_trailer(_state(frame_seconds_ago=120), stream="channel:C1", now=_NOW)
    assert decision.kind == "stale"
    assert decision.reasons == ["server unreachable"]


def test_classify_stale_not_caught_up() -> None:
    decision = classify_trailer(_state(caught_up=False), stream="channel:C1", now=_NOW)
    assert decision.kind == "stale"
    assert decision.reasons == ["catching up after reconnect"]


def test_classify_stale_catchup_window_crossed() -> None:
    # Caught up (boolean True) but the caught_up timestamp predates the window.
    decision = classify_trailer(
        _state(caught_up_seconds_ago=30),
        stream="channel:C1",
        now=_NOW,
        catchup_window_s=10.0,
    )
    assert decision.kind == "stale"
    assert decision.reasons == ["catching up after reconnect"]


def test_classify_clean_catchup_within_window() -> None:
    decision = classify_trailer(
        _state(caught_up_seconds_ago=5),
        stream="channel:C1",
        now=_NOW,
        catchup_window_s=10.0,
    )
    assert decision.kind == "clean"


def test_classify_fallback_user() -> None:
    decision = classify_trailer(
        _state(),
        stream="channel:C1",
        now=_NOW,
        fallback_reasons=[FALLBACK_USER_REASON],
    )
    assert decision.kind == "fallback"
    assert decision.reasons == [FALLBACK_USER_REASON]


def test_classify_fallback_channel() -> None:
    decision = classify_trailer(
        _state(),
        stream="channel:C1",
        now=_NOW,
        fallback_reasons=[FALLBACK_CHANNEL_REASON],
    )
    assert decision.kind == "fallback"
    assert decision.reasons == [FALLBACK_CHANNEL_REASON]


def test_classify_stale_takes_priority_over_fallback() -> None:
    # A disconnected stream with an unresolved mention classifies as stale; the
    # notify_store gate is closed either way, but the trailer reason wins.
    decision = classify_trailer(
        _state(health="disconnected"),
        stream="channel:C1",
        now=_NOW,
        fallback_reasons=[FALLBACK_USER_REASON],
    )
    assert decision.kind == "stale"
    assert decision.reasons == ["socket-mode disconnected"]


def test_classify_non_default_stale_after_s() -> None:
    # Frame 90s old: stale at the default 60s, but clean with a 120s threshold.
    state = _state(frame_seconds_ago=90)
    assert classify_trailer(state, stream="s", now=_NOW, stale_after_s=120.0).kind == "clean"
    assert classify_trailer(state, stream="s", now=_NOW, stale_after_s=60.0).kind == "stale"


def test_classify_matches_staleness_reason() -> None:
    # classify_trailer must agree with the underlying staleness_reason it wraps.
    for state in (_state(), _state(health="disconnected"), _state(frame_seconds_ago=120)):
        decision = classify_trailer(state, stream="s", now=_NOW)
        reason = staleness_reason(state, now=_NOW)
        if reason is None:
            assert decision.kind == "clean"
        else:
            assert decision.reasons == [reason]


# ============================================================================
# render_trailer
# ============================================================================


def test_render_trailer_stale_emits_text() -> None:
    decision = classify_trailer(_state(health="disconnected"), stream="s", now=_NOW)
    text = render_trailer(decision)
    assert text is not None
    assert "socket-mode disconnected" in text
    assert text.startswith("\n---\n\n")


def test_render_trailer_clean_is_none() -> None:
    assert render_trailer(classify_trailer(_state(), stream="s", now=_NOW)) is None


def test_render_trailer_fallback_is_none() -> None:
    decision = classify_trailer(_state(), stream="s", now=_NOW, fallback_reasons=[FALLBACK_USER_REASON])
    assert render_trailer(decision) is None


# ============================================================================
# JSONL round-trip
# ============================================================================


def _sample_decisions() -> list[TrailerDecision]:
    return [
        classify_trailer(_state(), stream="channel:C1", now=_NOW),
        classify_trailer(_state(health="disconnected"), stream="channel:C2", now=_NOW),
        classify_trailer(_state(), stream="channel:C3", now=_NOW, fallback_reasons=[FALLBACK_USER_REASON]),
        # A decision with no frame and no caught_up offset (None fields).
        TrailerDecision(kind="clean", reasons=[], stream="channel-list", inode=7, at=_NOW),
    ]


def test_decision_json_round_trip_in_memory() -> None:
    for decision in _sample_decisions():
        parsed = decision_from_json(decision_to_json(decision))
        assert parsed == decision


def test_trailer_log_write_and_parse_back(tmp_path: Path) -> None:
    path = tmp_path / "sub" / "trailer.jsonl"
    decisions = _sample_decisions()
    tlog = TrailerLog.open(path)
    try:
        for d in decisions:
            tlog.write(d)
    finally:
        tlog.close()

    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == len(decisions)
    parsed = [decision_from_json(json.loads(line)) for line in lines]
    assert parsed == decisions


def test_trailer_log_appends_across_opens(tmp_path: Path) -> None:
    path = tmp_path / "trailer.jsonl"
    first = TrailerLog.open(path)
    first.write(classify_trailer(_state(), stream="s1", now=_NOW))
    first.close()
    second = TrailerLog.open(path)
    second.write(classify_trailer(_state(health="disconnected"), stream="s2", now=_NOW))
    second.close()
    assert len(path.read_text(encoding="utf-8").splitlines()) == 2


def test_trailer_log_write_after_close_is_noop(tmp_path: Path) -> None:
    path = tmp_path / "trailer.jsonl"
    tlog = TrailerLog.open(path)
    tlog.write(classify_trailer(_state(), stream="s", now=_NOW))
    tlog.close()
    tlog.write(classify_trailer(_state(), stream="s", now=_NOW))  # must not raise
    assert len(path.read_text(encoding="utf-8").splitlines()) == 1


def test_decision_from_json_rejects_bad_kind() -> None:
    with pytest.raises(ValueError, match="unknown trailer decision kind"):
        _ = decision_from_json({"kind": "bogus", "reasons": []})
