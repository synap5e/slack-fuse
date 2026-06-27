"""In-memory state + status mapping for the ``_control/`` write surface.

The ``_control/`` FUSE namespace (see ``fuse_ops_v2``) is a Plan-9-style
ctl/status surface: write a control file to trigger an action, read ``status``
for the last outcomes. The state lives entirely in process memory — a daemon
restart resets it, which is fine because the actions themselves (workspace /
single-channel refresh) are server-side and idempotent.

``ControlState`` is thread-safe: the FUSE write handlers run on worker threads
(``trio.to_thread.run_sync``) while the status read can land on a different
worker, so every access takes the lock. The lock is only ever held across the
trivial in-memory record/render — never across an HTTP call.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

#: HTTP status code → user-facing verb for the ``status`` file. ``0`` is the
#: client-side sentinel for "couldn't reach the server at all" (transport error
#: or timeout) and reads the same as a real 503.
_STATUS_VERBS: dict[int, str] = {
    0: "server_unavailable",
    202: "queued",
    401: "unauthorised",
    403: "unauthorised",
    409: "busy",
    503: "server_unavailable",
}


def result_for_status(code: int) -> str:
    """Map an HTTP status code (or the ``0`` transport sentinel) to a verb."""
    return _STATUS_VERBS.get(code, f"http_{code}")


def _utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class RefreshOutcome:
    """One recorded refresh result. ``channel`` is set only for per-channel."""

    at: str
    result: str
    channel: str | None = None

    def to_json(self) -> dict[str, str]:
        payload = {"at": self.at, "result": self.result}
        if self.channel is not None:
            payload["channel"] = self.channel
        return payload


class ControlState:
    """Thread-safe holder for the last workspace / per-channel refresh outcome."""

    def __init__(self, now_fn: Callable[[], datetime] = _utcnow) -> None:
        self._now_fn = now_fn
        self._lock = threading.Lock()
        self._workspace: RefreshOutcome | None = None
        self._channel: RefreshOutcome | None = None
        self._rerender: RefreshOutcome | None = None
        self._block: RefreshOutcome | None = None
        self._unblock: RefreshOutcome | None = None
        self._backfill: RefreshOutcome | None = None

    def _stamp(self) -> str:
        return self._now_fn().astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    def record_workspace(self, result: str) -> None:
        with self._lock:
            self._workspace = RefreshOutcome(at=self._stamp(), result=result)

    def record_channel(self, channel: str, result: str) -> None:
        with self._lock:
            self._channel = RefreshOutcome(at=self._stamp(), result=result, channel=channel)

    def record_rerender(self, channel: str, result: str) -> None:
        with self._lock:
            self._rerender = RefreshOutcome(at=self._stamp(), result=result, channel=channel)

    def record_block(self, channel: str, result: str) -> None:
        with self._lock:
            self._block = RefreshOutcome(at=self._stamp(), result=result, channel=channel)

    def record_unblock(self, channel: str, result: str) -> None:
        with self._lock:
            self._unblock = RefreshOutcome(at=self._stamp(), result=result, channel=channel)

    def record_backfill(self, channel: str, result: str) -> None:
        with self._lock:
            self._backfill = RefreshOutcome(at=self._stamp(), result=result, channel=channel)

    def render(self) -> bytes:
        """Serialize the current state to the ``status`` file body."""
        with self._lock:
            workspace = self._workspace
            channel = self._channel
            rerender = self._rerender
            block = self._block
            unblock = self._unblock
            backfill = self._backfill
        payload: dict[str, dict[str, str] | None] = {
            "last_workspace_refresh": workspace.to_json() if workspace is not None else None,
            "last_channel_refresh": channel.to_json() if channel is not None else None,
            "last_rerender": rerender.to_json() if rerender is not None else None,
            "last_block": block.to_json() if block is not None else None,
            "last_unblock": unblock.to_json() if unblock is not None else None,
            "last_backfill": backfill.to_json() if backfill is not None else None,
        }
        return (json.dumps(payload, indent=2) + "\n").encode()


__all__ = ["ControlState", "RefreshOutcome", "result_for_status"]
