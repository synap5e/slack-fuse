"""In-memory task liveness registry for long-running slurper tasks."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import MappingProxyType

import trio

from slack_fuse_server._json import JsonObject


@dataclass(frozen=True, slots=True)
class TaskPhase:
    """A single declared phase of a long-running task."""

    task_name: str
    phase: str
    details: JsonObject
    entered_at: datetime
    deadline: datetime | None


class TaskSupervisor:
    """In-memory registry of current task phases.

    Methods are deliberately synchronous. The registry is mutated and read from
    the trio event loop; avoiding awaits inside these methods keeps each update
    atomic without introducing a lock.
    """

    def __init__(self, *, clock: Callable[[], datetime] = lambda: datetime.now(UTC)) -> None:
        self._phases: dict[str, TaskPhase] = {}
        self._clock = clock

    def declare(
        self,
        task_name: str,
        phase: str,
        *,
        details: JsonObject | None = None,
        deadline_s: float | None = None,
    ) -> None:
        """Record the latest phase for ``task_name``. Latest wins."""
        entered_at = self._clock()
        deadline = None if deadline_s is None else entered_at + timedelta(seconds=deadline_s)
        self._phases[task_name] = TaskPhase(
            task_name=task_name,
            phase=phase,
            details={} if details is None else dict(details),
            entered_at=entered_at,
            deadline=deadline,
        )

    def all_phases(self) -> Mapping[str, TaskPhase]:
        """Return the latest phase for every declared task."""
        return MappingProxyType(dict(self._phases))

    def overdue(self) -> list[TaskPhase]:
        """Return phases whose deadline has passed."""
        now = self._clock()
        return [phase for phase in self._phases.values() if phase.deadline is not None and phase.deadline <= now]


@asynccontextmanager
async def phase(
    supervisor: TaskSupervisor,
    task_name: str,
    phase_name: str,
    *,
    details: JsonObject | None = None,
    deadline_s: float | None = None,
) -> AsyncIterator[None]:
    """Wrap a block in a declared phase.

    Exiting the context does not synthesize a done marker; the next explicit
    declaration is the meaningful transition signal.
    """
    supervisor.declare(task_name, phase_name, details=details, deadline_s=deadline_s)
    await trio.lowlevel.checkpoint_if_cancelled()
    yield
