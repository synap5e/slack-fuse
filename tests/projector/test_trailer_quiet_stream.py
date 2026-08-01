"""Regression tests for workspace-wide trailer liveness classification."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from slack_fuse.projector.trailer import StalenessState, classify_trailer, render_trailer

_NOW = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)


def _state(
    *,
    stream_frame_seconds_ago: float | None,
    workspace_frame_seconds_ago: float | None,
    caught_up: bool = True,
) -> StalenessState:
    return StalenessState(
        last_frame_at=(
            None if stream_frame_seconds_ago is None else _NOW - timedelta(seconds=stream_frame_seconds_ago)
        ),
        workspace_last_frame_at=(
            None if workspace_frame_seconds_ago is None else _NOW - timedelta(seconds=workspace_frame_seconds_ago)
        ),
        last_slurper_health="healthy",
        last_health_update_at=_NOW,
        initial_catch_up_done_for_stream=caught_up,
        caught_up_offset=42 if caught_up else None,
    )


def test_quiet_stream_healthy_workspace_has_no_trailer() -> None:
    state = _state(stream_frame_seconds_ago=30 * 60, workspace_frame_seconds_ago=30)

    decision = classify_trailer(state, stream="channel-list", now=_NOW)

    assert decision.kind == "clean"
    assert decision.reasons == []
    assert render_trailer(decision) is None


def test_quiet_stream_disconnected_workspace_is_server_unreachable() -> None:
    state = _state(stream_frame_seconds_ago=30 * 60, workspace_frame_seconds_ago=30 * 60)

    decision = classify_trailer(state, stream="channel-list", now=_NOW)

    assert decision.kind == "stale"
    assert decision.reasons == ["server unreachable"]
    assert "server unreachable" in (render_trailer(decision) or "")


def test_live_stream_still_catching_up_has_catch_up_trailer() -> None:
    state = _state(stream_frame_seconds_ago=1, workspace_frame_seconds_ago=1, caught_up=False)

    decision = classify_trailer(state, stream="channel:C1", now=_NOW)

    assert decision.kind == "stale"
    assert decision.reasons == ["catching up after reconnect"]


def test_general_channel_21ms_read_regression_has_no_false_positive() -> None:
    """Pin the 2026-06-27 shape: idle channel-list, live health stream."""
    state = _state(stream_frame_seconds_ago=None, workspace_frame_seconds_ago=60)

    decision = classify_trailer(state, stream="channel-list", now=_NOW)

    assert decision.kind == "clean"
    assert decision.reasons == []
    assert render_trailer(decision) is None
