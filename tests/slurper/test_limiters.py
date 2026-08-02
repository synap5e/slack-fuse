"""Shared Slack tier pacing."""

from __future__ import annotations

import pytest
import trio

from slack_fuse_server.slurper.limiters import SlackTierPacer


@pytest.mark.trio
async def test_tier_pacer_spaces_concurrent_request_starts() -> None:
    """Independent jobs sharing one pacer cannot burst in the same tier."""
    now = 0.0
    starts: list[float] = []

    async def advance(delay_s: float) -> None:
        nonlocal now
        now += delay_s
        await trio.lowlevel.checkpoint()

    pacer = SlackTierPacer(3.5, clock=lambda: now, sleep=advance)

    async def request_start() -> None:
        await pacer.wait()
        starts.append(now)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(request_start)
        nursery.start_soon(request_start)

    assert sorted(starts) == [0.0, 3.5]
