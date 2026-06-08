"""Watch ``connection_state`` + ``stream_caught_up`` and re-invalidate the
kernel page cache for every primed inode whenever they change.

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
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Final

import trio

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow

log = logging.getLogger(__name__)


DEFAULT_POLL_INTERVAL_S: Final = 1.0


@dataclass(frozen=True, slots=True)
class HealthSignature:
    """A snapshot of the fields the staleness trailer depends on.

    Two signatures compare equal iff the FUSE read path would compute an
    identical ``staleness_reason`` for any stream. Catch-up state is folded in
    as a count + max-offset because every inserted row counts as a transition.
    """

    last_frame_at: datetime | None
    last_slurper_health: str | None
    last_health_update_at: datetime | None
    caught_up_count: int
    caught_up_max_offset: int


def read_signature(conn: Connection[TupleRow]) -> HealthSignature:
    """SELECT the current ``HealthSignature`` from ``conn``."""
    with conn.cursor() as cur:
        _ = cur.execute(
            "SELECT last_frame_at, last_slurper_health, last_health_update_at FROM connection_state WHERE id = 1"
        )
        row = cur.fetchone()
        last_frame_at: datetime | None = None
        last_slurper_health: str | None = None
        last_health_update_at: datetime | None = None
        if row is not None:
            last_frame_at = row[0] if isinstance(row[0], datetime) else None
            last_slurper_health = None if row[1] is None else str(row[1])
            last_health_update_at = row[2] if isinstance(row[2], datetime) else None
        _ = cur.execute("SELECT COUNT(*), COALESCE(MAX(at_offset), 0) FROM stream_caught_up")
        row2 = cur.fetchone()
        count = 0 if row2 is None else int(row2[0])
        max_offset = 0 if row2 is None else int(row2[1])
    return HealthSignature(
        last_frame_at=last_frame_at,
        last_slurper_health=last_slurper_health,
        last_health_update_at=last_health_update_at,
        caught_up_count=count,
        caught_up_max_offset=max_offset,
    )


InvalidateCallback = Callable[[], int]


async def watch_health(
    conn: Connection[TupleRow],
    on_change: InvalidateCallback,
    *,
    poll_interval_s: float = DEFAULT_POLL_INTERVAL_S,
    iterations: int | None = None,
) -> None:
    """Trio task: poll ``read_signature`` and fire ``on_change`` on each change.

    ``iterations`` caps the loop count (tests use a small value). ``None``
    means "forever" — production usage spawns this under the projector
    nursery and lets the main scope cancel it on shutdown.
    """
    last = read_signature(conn)
    log.info("health_subscriber: started; baseline signature=%s", last)
    iter_count = 0
    while True:
        await trio.sleep(poll_interval_s)
        try:
            current = read_signature(conn)
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
) -> HealthSignature:
    """Synchronous one-shot variant — useful for the integration tests.

    Returns the (possibly updated) signature for the caller to thread through
    the next call.
    """
    current = read_signature(conn)
    if current != last_seen:
        _ = on_change()
    return current
