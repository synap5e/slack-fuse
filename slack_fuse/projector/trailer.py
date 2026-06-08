"""Pure trailer-decision logic — staleness classification + trailer text.

Extracted from ``slack_fuse/fuse_v2_helpers.py`` (Sprint 3C) so the per-read
trailer decision is observable in one place. Everything here is pure: no DB, no
I/O, no pyfuse3. The I/O wrappers (``fetch_staleness_state`` and the
byte-assembly in ``fuse_ops_v2``) stay at the call site and feed these
classifiers their state values.

Three layers:

* :class:`StalenessState` — the read-path's snapshot of the staleness inputs
  (carried from ``connection_state`` + ``stream_caught_up``).
* :func:`staleness_reason` / :func:`format_trailer` — the classifier + text
  renderer, unchanged in behaviour from the pre-3C inline versions.
* :class:`TrailerDecision` + :func:`classify_trailer` / :func:`render_trailer`
  — the observable decision record the JSONL log (``trailer_log.py``) persists,
  plus a pure wrapper that folds staleness + unresolved-mention fallback into a
  single ``kind``.

Per RFC §Offline behaviour → Staleness conditions and §Trailer format.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Final, Literal

# WS-disconnect staleness threshold. Mirrors RFC §Configuration →
# ``stale_after_disconnect_s`` (config.py default) and §Offline behaviour →
# Staleness conditions #1 ("reconnecting unsuccessfully for at least 60 s").
# Both the read-path classifier (``staleness_reason``) and the health
# subscriber's time-aware signature key off this single constant so the
# "trailer never lies" invariant can't drift between them. Config wires the
# real value through; this is the backwards-compat default at call sites that
# don't pass one.
STALE_AFTER_DISCONNECT_S: Final = 60.0

# Catch-up freshness window default (RFC §Configuration → ``catchup_window_s``).
# A ``stream_caught_up`` row older than this — relative to the read clock — is
# treated as "no recent confirmation we're at head", i.e. still catching up.
DEFAULT_CATCHUP_WINDOW_S: Final = 10.0

# Reason strings recorded for an unresolved-mention fallback decision. These do
# NOT append a trailer (fallback only gates ``notify_store``); they exist so the
# JSONL decision log can attribute a non-primed read to its cause.
FALLBACK_USER_REASON: Final = "unresolved-user-mention"
FALLBACK_CHANNEL_REASON: Final = "unresolved-channel-mention"


# ============================================================================
# Staleness classifier
# ============================================================================


@dataclass(frozen=True, slots=True)
class StalenessState:
    """Per-RFC §Offline behaviour → Source of truth for staleness.

    ``last_caught_up_at`` / ``caught_up_offset`` are optional (default
    ``None``) so callers that predate the catch-up-window wiring — and the unit
    tests that only exercise the health/frame conditions — keep constructing
    the state with the original four fields. When ``last_caught_up_at`` is
    ``None`` the catch-up-window check is skipped and the boolean
    ``initial_catch_up_done_for_stream`` is authoritative (the pre-3C
    behaviour).
    """

    last_frame_at: datetime | None
    last_slurper_health: str
    last_health_update_at: datetime | None
    initial_catch_up_done_for_stream: bool
    last_caught_up_at: datetime | None = None
    caught_up_offset: int | None = None


_DISCONNECTED_STATES: Final[frozenset[str]] = frozenset({"disconnected"})
_DEGRADED_STATES: Final[frozenset[str]] = frozenset({"degraded", "auth_failed"})


def staleness_reason(
    state: StalenessState,
    *,
    now: datetime | None = None,
    stale_after_s: float = STALE_AFTER_DISCONNECT_S,
    catchup_window_s: float = DEFAULT_CATCHUP_WINDOW_S,
) -> str | None:
    """Return the trailer reason string, or ``None`` if content is current.

    Per RFC §Offline behaviour → Staleness conditions, exactly three
    fundamental degradation states append a trailer. The reason strings come
    from §Trailer format.

    ``stale_after_s`` (the WS-disconnect threshold) and ``catchup_window_s``
    (the catch-up freshness window) are wired from ``ClientConfig`` by the read
    path; both keep their module-default for call sites that don't override.
    """
    health = state.last_slurper_health
    if health == "auth_failed":
        return "auth token invalid"
    if health in _DISCONNECTED_STATES:
        return "socket-mode disconnected"
    if health in _DEGRADED_STATES:
        return "slack ingestion unhealthy"

    now_real = now if now is not None else datetime.now(UTC)

    # Catch-up freshness (B3 / RFC §Configuration → catchup_window_s). A
    # caught_up row older than the window means we have no *recent* confirmation
    # the stream is at head — treat it as still catching up. ``last_caught_up_at
    # is None`` falls back to the boolean (pre-3C behaviour), so unit tests and
    # any caller that doesn't surface the timestamp are unaffected.
    #
    # CAVEAT: a ``caught_up`` frame is emitted once per (re)connection per stream
    # (RFC §Offline behaviour → condition 3), not on a heartbeat, so once a
    # stream has been connected longer than ``catchup_window_s`` this fires on
    # every read. The window is therefore best paired with a server that
    # re-emits ``caught_up`` periodically, or treated as a deliberately
    # conservative bake-in knob (measure the rate via the trailer-decision log,
    # and use ``stale_trailer_enabled=False`` to compare against no-trailer).
    caught_up = state.initial_catch_up_done_for_stream
    if (
        caught_up
        and state.last_caught_up_at is not None
        and (now_real - state.last_caught_up_at) > timedelta(seconds=catchup_window_s)
    ):
        caught_up = False

    # WS disconnect detection — last_frame_at older than the threshold
    # indicates we are reconnecting unsuccessfully.
    if state.last_frame_at is None or (now_real - state.last_frame_at) > timedelta(seconds=stale_after_s):
        # No frame in the window: be conservative and trail. The
        # belt-and-suspenders invalidation invariant means this stops the
        # moment a frame arrives.
        if not caught_up:
            return "catching up after reconnect"
        return "server unreachable"

    if not caught_up:
        return "catching up after reconnect"

    return None


def format_trailer(reason: str, last_frame_at: datetime | None) -> str:
    """Compose the staleness trailer (see RFC §Trailer format)."""
    if last_frame_at is None:
        ts = "never"
    else:
        ts = last_frame_at.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    return f"\n---\n\n> ⚠ Content may be stale. Last successful sync: {ts}. Reason: {reason}.\n"


# ============================================================================
# Observable decision record
# ============================================================================


TrailerKind = Literal["clean", "stale", "fallback"]


@dataclass(frozen=True, slots=True)
class TrailerDecision:
    """One per-read trailer decision — the contract with the JSONL log.

    Stable on purpose: the bake-in false-positive analysis parses these fields
    out of ``trailer_log.py``'s output, so add fields rather than repurposing
    existing ones.

    * ``kind`` — the dominant classification. ``stale`` (a staleness trailer is
      appended) takes priority over ``fallback`` (an unresolved ``<@U…>`` /
      ``<#C…>`` mention suppressed ``notify_store`` without a trailer), which
      takes priority over ``clean``.
    * ``reasons`` — the specific reason strings behind ``kind`` (the trailer
      reason for ``stale``; the unresolved-mention kinds for ``fallback``;
      empty for ``clean``).
    * ``stream`` — the staleness stream the decision keyed off (e.g.
      ``channel:C123`` or ``channel-list``).
    * ``inode`` — the FUSE inode the read served (filled in by ``read()``;
      ``None`` when classified outside a FUSE callback, e.g. in unit tests).
    """

    kind: TrailerKind
    reasons: list[str] = field(default_factory=list[str])
    stream: str = ""
    inode: int | None = None
    at: datetime | None = None
    last_frame_at: datetime | None = None
    last_health: str | None = None
    caught_up_offset: int | None = None


def classify_trailer(  # noqa: PLR0913  (pure classifier with explicit state + window knobs)
    state: StalenessState,
    *,
    stream: str,
    now: datetime,
    stale_after_s: float = STALE_AFTER_DISCONNECT_S,
    catchup_window_s: float = DEFAULT_CATCHUP_WINDOW_S,
    fallback_reasons: Sequence[str] = (),
) -> TrailerDecision:
    """Fold staleness + unresolved-mention fallback into one decision record.

    Pure: no DB, no I/O. ``fallback_reasons`` is the (possibly empty) set of
    unresolved-mention reasons the renderer reported for this read;
    ``staleness_reason`` is recomputed here from ``state`` so the classifier is
    the single source of truth for ``kind``. The ``inode`` is stamped by the
    read path via :func:`dataclasses.replace` once it's known.
    """
    reason = staleness_reason(state, now=now, stale_after_s=stale_after_s, catchup_window_s=catchup_window_s)
    if reason is not None:
        kind: TrailerKind = "stale"
        reasons = [reason]
    elif fallback_reasons:
        kind = "fallback"
        reasons = list(fallback_reasons)
    else:
        kind = "clean"
        reasons = []
    return TrailerDecision(
        kind=kind,
        reasons=reasons,
        stream=stream,
        inode=None,
        at=now,
        last_frame_at=state.last_frame_at,
        last_health=state.last_slurper_health,
        caught_up_offset=state.caught_up_offset,
    )


def render_trailer(decision: TrailerDecision) -> str | None:
    """Trailer text for a decision, or ``None`` when nothing is appended.

    Only ``stale`` decisions append a trailer; ``fallback`` suppresses
    ``notify_store`` but never writes a warning into the bytes.
    """
    if decision.kind != "stale" or not decision.reasons:
        return None
    return format_trailer(decision.reasons[0], decision.last_frame_at)


__all__ = [
    "DEFAULT_CATCHUP_WINDOW_S",
    "FALLBACK_CHANNEL_REASON",
    "FALLBACK_USER_REASON",
    "STALE_AFTER_DISCONNECT_S",
    "StalenessState",
    "TrailerDecision",
    "TrailerKind",
    "classify_trailer",
    "format_trailer",
    "render_trailer",
    "staleness_reason",
]
