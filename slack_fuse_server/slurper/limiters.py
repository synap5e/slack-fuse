"""Resource-scoped concurrency gates for slurper work."""

from __future__ import annotations

from dataclasses import dataclass

import trio


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
