"""Watch ``connection_state`` + ``stream_caught_up`` and re-invalidate the
kernel page cache for every primed inode whenever the staleness classification
could change.

Per RFC §FUSE read path → Trailer / kernel-cache invariant
(belt-and-suspenders):

> on every transition of ``connection_state`` (any field) and on every
> ``stream_caught_up`` insert, the projector calls ``invalidate_inode`` for
> every inode it has ever primed via ``notify_store``.

The mechanism is intentionally simple — poll the two tables at a fixed
interval, compare against the last observed signature, and invoke an
invalidator callback on change. We poll rather than ``LISTEN/NOTIFY`` so the
subscriber doesn't share a connection / event loop with the projector's main
applier task and so the FUSE process can run it standalone in tests.

Two subtleties the naive "hash the rows" signature gets wrong (both flagged by
the pre-3B reviews):

* **Time-driven staleness (review P0-1 / GPT, Gemini Class 2).** The WS
  "no frame for ``stale_after_disconnect_s``" condition is wall-clock driven —
  ``last_frame_at`` stops mutating the moment the server dies silently, so a
  signature built from raw DB values never changes at the 60 s boundary and
  primed-clean bytes would be served without a trailer forever. The signature
  therefore folds in a *derived* ``frame_stale`` boolean computed against the
  caller-supplied ``now`` rather than the raw timestamp; the poll loop
  recomputes it every tick so the boundary crossing fires invalidation with no
  DB mutation.

* **Healthy-operation thrashing (review P0-3 / Gemini Class 6).** Because
  ``last_frame_at`` is bumped on *every* WS frame (pings, live events), a
  signature carrying the raw timestamp changes every poll during normal
  operation and would invalidate the whole page cache every second, defeating
  ``notify_store`` entirely. Carrying only the ``frame_stale`` boolean (which
  stays ``False`` while frames keep arriving) keeps the signature stable while
  healthy.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Final

import trio

from slack_fuse.fuse_v2_helpers import STALE_AFTER_DISCONNECT_S

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow

log = logging.getLogger(__name__)


DEFAULT_POLL_INTERVAL_S: Final = 1.0

NowFn = Callable[[], datetime]


def _utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class HealthSignature:
    """A snapshot of the inputs the staleness trailer depends on.

    Two signatures compare equal iff the FUSE read path would compute an
    identical ``staleness_reason`` for every stream. Deliberately does NOT
    carry the raw ``last_frame_at`` / ``last_health_update_at`` timestamps:

    * ``last_frame_at`` is replaced by the derived ``frame_stale`` boolean so
      the signature is stable while frames keep arriving but flips the instant
      wall-clock crosses the disconnect threshold (see module docstring).
    * ``last_health_update_at`` doesn't affect ``staleness_reason`` at all, so
      carrying it would only cause spurious invalidations on redundant health
      events.

    Catch-up state is folded in as ``(count, max_offset, max_at)``. ``max_at``
    is load-bearing: ``record_caught_up`` is an UPSERT that always restamps
    ``caught_up_at = now()``, so an existing stream being re-marked caught-up —
    which leaves both ``count`` and ``max_offset`` unchanged — still moves
    ``max_at`` and is detected (review P1-7 / GPT: the count+max signature
    missed the upsert path).
    """

    last_slurper_health: str | None
    frame_stale: bool
    caught_up_count: int
    caught_up_max_offset: int
    caught_up_max_at: datetime | None


def read_signature(
    conn: Connection[TupleRow],
    *,
    now: datetime | None = None,
    stale_after_s: float = STALE_AFTER_DISCONNECT_S,
) -> HealthSignature:
    """SELECT the current ``HealthSignature`` from ``conn``.

    ``now`` (defaulting to the real UTC clock) drives the ``frame_stale``
    computation so callers — production poll loop and tests alike — can advance
    wall-clock time without mutating the DB row.
    """
    now_real = now if now is not None else _utcnow()
    with conn.cursor() as cur:
        _ = cur.execute("SELECT last_frame_at, last_slurper_health FROM connection_state WHERE id = 1")
        row = cur.fetchone()
        last_frame_at: datetime | None = None
        last_slurper_health: str | None = None
        if row is not None:
            last_frame_at = row[0] if isinstance(row[0], datetime) else None
            last_slurper_health = None if row[1] is None else str(row[1])
        _ = cur.execute("SELECT COUNT(*), COALESCE(MAX(at_offset), 0), MAX(caught_up_at) FROM stream_caught_up")
        row2 = cur.fetchone()
        count = 0 if row2 is None else int(row2[0])
        max_offset = 0 if row2 is None else int(row2[1])
        max_at = row2[2] if row2 is not None and isinstance(row2[2], datetime) else None
    frame_stale = last_frame_at is None or (now_real - last_frame_at).total_seconds() > stale_after_s
    return HealthSignature(
        last_slurper_health=last_slurper_health,
        frame_stale=frame_stale,
        caught_up_count=count,
        caught_up_max_offset=max_offset,
        caught_up_max_at=max_at,
    )


InvalidateCallback = Callable[[], int]


async def watch_health(  # noqa: PLR0913  (keyword-only polling/test tuning knobs)
    conn: Connection[TupleRow],
    on_change: InvalidateCallback,
    *,
    poll_interval_s: float = DEFAULT_POLL_INTERVAL_S,
    stale_after_s: float = STALE_AFTER_DISCONNECT_S,
    now_fn: NowFn = _utcnow,
    iterations: int | None = None,
) -> None:
    """Trio task: poll ``read_signature`` and fire ``on_change`` on each change.

    The signature is recomputed against ``now_fn()`` every tick, so the
    wall-clock-driven ``frame_stale`` flip is detected within one poll interval
    even when no DB row changed (review P0-1). ``iterations`` caps the loop
    count (tests use a small value); ``None`` means "forever" — production
    spawns this under the FUSE-mount nursery and lets the main scope cancel it
    on shutdown.
    """
    last = read_signature(conn, now=now_fn(), stale_after_s=stale_after_s)
    log.info("health_subscriber: started; baseline signature=%s", last)
    iter_count = 0
    while True:
        await trio.sleep(poll_interval_s)
        try:
            current = read_signature(conn, now=now_fn(), stale_after_s=stale_after_s)
        except Exception as exc:  # noqa: BLE001  (subscriber must not die on bursty DB)
            log.warning("health_subscriber: signature read failed: %s", exc)
        else:
            if current != last:
                log.info("health_subscriber: state change detected, firing invalidator")
                try:
                    invalidated = on_change()
                except Exception as exc:  # noqa: BLE001  (subscriber must not die on FUSE quirks)
                    log.warning("health_subscriber: invalidator raised: %s", exc)
                else:
                    log.debug("health_subscriber: invalidated %d primed inodes", invalidated)
                last = current
        iter_count += 1
        if iterations is not None and iter_count >= iterations:
            return


def watch_health_once(
    conn: Connection[TupleRow],
    last_seen: HealthSignature,
    on_change: InvalidateCallback,
    *,
    now: datetime | None = None,
    stale_after_s: float = STALE_AFTER_DISCONNECT_S,
) -> HealthSignature:
    """Synchronous one-shot variant — useful for the integration tests.

    Returns the (possibly updated) signature for the caller to thread through
    the next call. ``now`` lets tests cross the staleness threshold without a
    DB mutation.
    """
    current = read_signature(conn, now=now, stale_after_s=stale_after_s)
    if current != last_seen:
        _ = on_change()
    return current
