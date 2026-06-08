"""Tests for the projector's ``health_subscriber``.

These specifically exercise the watch-loop semantics (signature change →
on_change fires; no change → nothing fires), independent of FUSE wiring.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import trio

from slack_fuse.projector.health_subscriber import (
    HealthSignature,
    read_signature,
    watch_health,
    watch_health_once,
)
from tests.fuse_v2.conftest import (
    mark_stream_caught_up,
    set_connection_state,
)

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow


def test_signature_changes_when_health_changes(client_conn: Connection[TupleRow]) -> None:
    baseline = read_signature(client_conn)
    set_connection_state(client_conn, last_slurper_health="degraded")
    after = read_signature(client_conn)
    assert after != baseline


def test_signature_changes_when_stream_caught_up_added(client_conn: Connection[TupleRow]) -> None:
    baseline = read_signature(client_conn)
    mark_stream_caught_up(client_conn, "channel:CABC", at_offset=7)
    after = read_signature(client_conn)
    assert after != baseline
    assert after.caught_up_count == baseline.caught_up_count + 1


def test_watch_health_once_skips_when_unchanged(client_conn: Connection[TupleRow]) -> None:
    calls = [0]

    def cb() -> int:
        calls[0] += 1
        return 0

    baseline = read_signature(client_conn)
    same = watch_health_once(client_conn, baseline, cb)
    assert calls[0] == 0
    assert same == baseline


def test_watch_health_once_fires_on_change(client_conn: Connection[TupleRow]) -> None:
    calls = [0]

    def cb() -> int:
        calls[0] += 1
        return 0

    baseline = read_signature(client_conn)
    set_connection_state(client_conn, last_slurper_health="degraded")
    _ = watch_health_once(client_conn, baseline, cb)
    assert calls[0] == 1


@pytest.mark.trio
async def test_watch_health_trio_loop_fires_invalidator(
    client_conn: Connection[TupleRow],
) -> None:
    """Drive the trio watch loop with virtual time; mutate state and assert
    the invalidator callback fires on the next tick.
    """
    invalidated = [0]

    def cb() -> int:
        invalidated[0] += 1
        return 0

    async def mutator() -> None:
        # Wait for the loop to take its baseline + start sleeping, then mutate.
        await trio.sleep(0.001)
        set_connection_state(client_conn, last_slurper_health="degraded")

    async with trio.open_nursery() as nursery:
        nursery.start_soon(mutator)
        await watch_health(client_conn, cb, poll_interval_s=0.001, iterations=3)
        nursery.cancel_scope.cancel()

    assert invalidated[0] >= 1


def test_signature_is_frozen_dataclass(client_conn: Connection[TupleRow]) -> None:
    sig = read_signature(client_conn)
    assert isinstance(sig, HealthSignature)
    with pytest.raises((AttributeError, Exception)):
        sig.caught_up_count = 99  # pyright: ignore[reportAttributeAccessIssue]
