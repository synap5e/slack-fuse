"""Resource-scoped concurrency gates for slurper work."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import trio

type PacerClock = Callable[[], float]
type PacerSleep = Callable[[float], Awaitable[None]]


class SlackTierPacer:
    """Process-wide minimum spacing between calls in one Slack API tier.

    A ``CapacityLimiter`` bounds concurrent worker threads but does not cap
    request starts per minute. This lock-protected pacer is therefore shared by
    every producer using the same Tier-2 ``search.messages`` method budget
    (currently channel totals and probe facts), ensuring their calls cannot
    burst independently when periodic sweeps overlap. Other Slack methods have
    independent per-method budgets and retain their own pacing/retry behavior.
    """

    def __init__(
        self,
        minimum_interval_s: float,
        *,
        clock: PacerClock = trio.current_time,
        sleep: PacerSleep = trio.sleep,
    ) -> None:
        if minimum_interval_s < 0:
            raise ValueError("Slack tier pacing interval must not be negative")
        self.minimum_interval_s = minimum_interval_s
        self._clock = clock
        self._sleep = sleep
        self._next_start_at = 0.0
        self._lock = trio.Lock()

    async def wait(self) -> None:
        """Wait until this tier's next globally permitted request start."""
        async with self._lock:
            now = self._clock()
            delay_s = self._next_start_at - now
            if delay_s > 0:
                await self._sleep(delay_s)
                now = self._clock()
            self._next_start_at = max(now, self._next_start_at) + self.minimum_interval_s


@dataclass(frozen=True, slots=True)
class SlurperLimiters:
    """Concurrency gates by resource class.

    Consumers select the gate for the resource they are about to touch. Work
    that does not fit a more specific class should use ``writer`` as the
    conservative default, so it cannot fan out unbounded worker threads.
    """

    slack_api: trio.CapacityLimiter
    writer: trio.CapacityLimiter
    snapshot: trio.CapacityLimiter
    admin_read: trio.CapacityLimiter
    slack_tier2: SlackTierPacer
