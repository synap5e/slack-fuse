"""Day-range bound computation across three timezones, end-to-end via the DB.

The same UTC chunk timestamps must bucket into different local dates depending
on the mount's ``ZoneInfo``. This is the spec gate:

    Day-range bound computation: test 3 timezones (UTC, NZST, PST) give
    consistent results.

"Consistent" here means: for the same set of chunk UTC timestamps, each
``ZoneInfo`` slots them into the local calendar dates we'd expect, and the
day file picks up exactly the chunks whose UTC ts falls within the local-tz
day window.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import trio

from slack_fuse.fuse_ops_v2 import SlackFuseOpsV2
from tests.fuse_v2.conftest import (
    NOOP_INVALIDATE_INODE,
    NOOP_NOTIFY_STORE,
    mark_stream_caught_up,
    seed_channel,
    seed_chunk,
    set_connection_state,
)

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow


def _ts(dt: datetime) -> Decimal:
    return Decimal(str(dt.timestamp()))


def _seed_three_messages_around_midnight(conn: Connection[TupleRow]) -> None:
    """Seed three chunks at UTC times that straddle local midnight differently
    depending on tz.

    - 06:00 UTC: NZST 18:00 (same day in NZ), LA 23:00 (previous local day in DST).
    - 12:00 UTC: NZST midnight crossing, LA 05:00.
    - 23:00 UTC: NZST 11:00 (next calendar day), LA 16:00 (same day).
    """
    seed_channel(conn, "C1", "general", tier="hot")
    seed_chunk(
        conn,
        "C1",
        _ts(datetime(2026, 6, 8, 6, 0, tzinfo=UTC)),
        "## 06:00 morning\n\nfirst\n",
    )
    seed_chunk(
        conn,
        "C1",
        _ts(datetime(2026, 6, 8, 12, 0, tzinfo=UTC)),
        "## 12:00 noon\n\nsecond\n",
    )
    seed_chunk(
        conn,
        "C1",
        _ts(datetime(2026, 6, 8, 23, 0, tzinfo=UTC)),
        "## 23:00 night\n\nthird\n",
    )
    set_connection_state(conn, last_slurper_health="healthy", last_frame_at_offset_s=1.0)
    mark_stream_caught_up(conn, "channel:C1")


def _build(conn: Connection[TupleRow], tz: ZoneInfo) -> SlackFuseOpsV2:
    return SlackFuseOpsV2(
        conn=conn,
        local_tz=tz,
        limiter=trio.CapacityLimiter(1),
        notify_store=NOOP_NOTIFY_STORE,
        invalidate_inode=NOOP_INVALIDATE_INODE,
    )


def test_utc_buckets_all_three_into_06_08(client_conn: Connection[TupleRow]) -> None:
    _seed_three_messages_around_midnight(client_conn)
    ops = _build(client_conn, ZoneInfo("UTC"))

    # All three messages land in 2026-06-08 under UTC.
    months = {m for m, _ in ops.list_dir_for_test("/channels/general") if _}
    assert "2026-06" in months
    days = {d for d, _ in ops.list_dir_for_test("/channels/general/2026-06")}
    assert days == {"08"}

    resolved = ops.resolve_content_for_test("/channels/general/2026-06/08/channel.md")
    assert resolved is not None
    text = resolved[0].decode()
    assert "first" in text and "second" in text and "third" in text


def test_pacific_auckland_buckets_third_into_next_day(client_conn: Connection[TupleRow]) -> None:
    _seed_three_messages_around_midnight(client_conn)
    ops = _build(client_conn, ZoneInfo("Pacific/Auckland"))

    # NZST = UTC+12 in June. So:
    #   06:00 UTC → 18:00 NZ on 2026-06-08
    #   12:00 UTC → 00:00 NZ on 2026-06-09  (rolls over)
    #   23:00 UTC → 11:00 NZ on 2026-06-09
    days = {d for d, _ in ops.list_dir_for_test("/channels/general/2026-06")}
    assert days == {"08", "09"}

    eight = ops.resolve_content_for_test("/channels/general/2026-06/08/channel.md")
    nine = ops.resolve_content_for_test("/channels/general/2026-06/09/channel.md")
    assert eight is not None and nine is not None
    assert "first" in eight[0].decode()
    assert "second" in nine[0].decode()
    assert "third" in nine[0].decode()


def test_la_buckets_first_into_previous_day(client_conn: Connection[TupleRow]) -> None:
    _seed_three_messages_around_midnight(client_conn)
    ops = _build(client_conn, ZoneInfo("America/Los_Angeles"))

    # PDT = UTC-7 in June. So:
    #   06:00 UTC → 23:00 LA on 2026-06-07
    #   12:00 UTC → 05:00 LA on 2026-06-08
    #   23:00 UTC → 16:00 LA on 2026-06-08
    days = {d for d, _ in ops.list_dir_for_test("/channels/general/2026-06")}
    assert days == {"07", "08"}

    seven = ops.resolve_content_for_test("/channels/general/2026-06/07/channel.md")
    eight = ops.resolve_content_for_test("/channels/general/2026-06/08/channel.md")
    assert seven is not None and eight is not None
    assert "first" in seven[0].decode()
    assert "second" in eight[0].decode()
    assert "third" in eight[0].decode()


def test_three_tz_pick_consistent_chunk_counts(client_conn: Connection[TupleRow]) -> None:
    """No matter the timezone, the union of all daily channel.md files for
    the month must include each chunk exactly once.
    """
    _seed_three_messages_around_midnight(client_conn)
    seen_per_tz: dict[str, set[str]] = {}
    for tz_key in ("UTC", "Pacific/Auckland", "America/Los_Angeles"):
        ops = _build(client_conn, ZoneInfo(tz_key))
        all_text = ""
        for day, _ in ops.list_dir_for_test("/channels/general/2026-06"):
            resolved = ops.resolve_content_for_test(f"/channels/general/2026-06/{day}/channel.md")
            assert resolved is not None
            all_text += resolved[0].decode()
        seen_per_tz[tz_key] = {chunk for chunk in ("first", "second", "third") if chunk in all_text}
    expected = {"first", "second", "third"}
    assert seen_per_tz["UTC"] == expected
    assert seen_per_tz["Pacific/Auckland"] == expected
    assert seen_per_tz["America/Los_Angeles"] == expected
