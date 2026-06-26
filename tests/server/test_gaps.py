"""Gap detection over the events log.

A gap is a run of consecutive UTC days with no ``message`` events,
bounded on BOTH sides by days that DO have message events. Trailing
silence and leading silence are explicitly NOT gaps. These tests pin the
boundary semantics so a future change can't quietly reclassify them.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

from slack_fuse_server._json import JsonObject
from slack_fuse_server.gaps import (
    GapRange,
    _bounded_gaps,  # pyright: ignore[reportPrivateUsage]
    find_gaps_for_channel,
    find_gaps_workspace,
    render_channel_gaps,
    render_workspace_gaps,
)
from slack_fuse_server.slurper.offsets import EventRecord, write_event

if TYPE_CHECKING:
    import psycopg
    from psycopg.rows import TupleRow


_CH = "C_GAPS"
_STREAM = f"channel:{_CH}"


def _ts_for_day(d: date) -> str:
    """Slack ts (UTC epoch seconds) at midnight + 1 second on the given day."""
    import calendar  # noqa: PLC0415 — calendar.timegm is the standard "UTC date → epoch" path.
    from datetime import datetime as _dt  # noqa: PLC0415

    midnight = _dt(d.year, d.month, d.day)
    return f"{calendar.timegm(midnight.timetuple()) + 1}.000000"


def _seed_message(
    conn: psycopg.Connection[TupleRow],
    *,
    day: date,
    seq: int = 0,
) -> None:
    """Drop one message event on the given UTC day. ``seq`` disambiguates
    so multiple messages on the same day don't dedup against each other."""
    ts = f"{_ts_for_day(day).split('.')[0]}.{seq:06d}"
    payload: JsonObject = {
        "ts": ts,
        "user": "U1",
        "text": f"msg seq={seq}",
        "subtype": None,
        "thread_ts": None,
        "reply_count": 0,
        "files": [],
        "edited": None,
        "reactions": [],
    }
    _ = write_event(
        conn,
        EventRecord(stream=_STREAM, kind="message", ts=ts, payload=payload, dedup=True),
    )


# ============================================================================
# Pure logic: _bounded_gaps over a day sequence
# ============================================================================


def test_bounded_gaps_empty_sequence_has_no_gaps() -> None:
    assert _bounded_gaps([]) == []


def test_bounded_gaps_single_day_has_no_gaps() -> None:
    assert _bounded_gaps([date(2026, 6, 1)]) == []


def test_bounded_gaps_consecutive_days_have_no_gaps() -> None:
    assert _bounded_gaps([date(2026, 6, 1), date(2026, 6, 2), date(2026, 6, 3)]) == []


def test_bounded_gaps_single_missing_day_is_a_one_day_gap() -> None:
    result = _bounded_gaps([date(2026, 6, 1), date(2026, 6, 3)])
    assert result == [GapRange(start=date(2026, 6, 2), end=date(2026, 6, 2))]
    assert result[0].day_count == 1


def test_bounded_gaps_multi_day_run_is_one_range() -> None:
    result = _bounded_gaps([date(2026, 6, 1), date(2026, 6, 10)])
    assert result == [GapRange(start=date(2026, 6, 2), end=date(2026, 6, 9))]
    assert result[0].day_count == 8


def test_bounded_gaps_multiple_separate_runs() -> None:
    result = _bounded_gaps(
        [
            date(2026, 6, 1),
            date(2026, 6, 5),
            date(2026, 6, 6),
            date(2026, 6, 15),
            date(2026, 6, 16),
        ],
    )
    assert result == [
        GapRange(start=date(2026, 6, 2), end=date(2026, 6, 4)),
        GapRange(start=date(2026, 6, 7), end=date(2026, 6, 14)),
    ]


def test_bounded_gaps_trailing_silence_excluded() -> None:
    """No phantom gap after the last observed day. By spec, trailing
    silence might just mean 'channel went quiet' or 'we're still today'."""
    # Even with a huge final-day-to-now distance, no gap is emitted.
    result = _bounded_gaps([date(2026, 6, 1), date(2026, 6, 2)])
    assert result == []


def test_bounded_gaps_leading_silence_excluded() -> None:
    """No phantom gap before the first observed day."""
    result = _bounded_gaps([date(2026, 6, 20), date(2026, 6, 21)])
    assert result == []


# ============================================================================
# Integration: SQL → days → gaps
# ============================================================================


def test_find_gaps_empty_channel_has_no_gaps(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    assert find_gaps_for_channel(server_conn, _CH) == []


def test_find_gaps_single_day_channel_has_no_gaps(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    _seed_message(server_conn, day=date(2026, 6, 1))
    assert find_gaps_for_channel(server_conn, _CH) == []


def test_find_gaps_round_trip_one_missing_day(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    _seed_message(server_conn, day=date(2026, 6, 1))
    _seed_message(server_conn, day=date(2026, 6, 3))
    gaps = find_gaps_for_channel(server_conn, _CH)
    assert gaps == [GapRange(start=date(2026, 6, 2), end=date(2026, 6, 2))]


def test_find_gaps_two_separate_runs(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    for d in (date(2026, 6, 1), date(2026, 6, 5), date(2026, 6, 6), date(2026, 6, 12)):
        _seed_message(server_conn, day=d)
    gaps = find_gaps_for_channel(server_conn, _CH)
    assert gaps == [
        GapRange(start=date(2026, 6, 2), end=date(2026, 6, 4)),
        GapRange(start=date(2026, 6, 7), end=date(2026, 6, 11)),
    ]


def test_find_gaps_multiple_messages_per_day_collapse(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    """5 messages on day 1, 0 on day 2, 3 on day 3 → still one 1-day gap."""
    for seq in range(5):
        _seed_message(server_conn, day=date(2026, 6, 1), seq=seq)
    for seq in range(3):
        _seed_message(server_conn, day=date(2026, 6, 3), seq=seq)
    gaps = find_gaps_for_channel(server_conn, _CH)
    assert gaps == [GapRange(start=date(2026, 6, 2), end=date(2026, 6, 2))]


def test_find_gaps_ignores_non_message_events(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    """Future-proofs the heartbeat-event hook: until the server has a 'I
    checked and saw nothing' event kind, non-``message`` events on the
    stream must NOT count toward the active-day set. (When that hook
    lands, _collect_active_days will get explicit UNION semantics.)
    """
    _seed_message(server_conn, day=date(2026, 6, 1))
    # Hypothetical non-message event on day 2 — should NOT close the gap.
    _ = write_event(
        server_conn,
        EventRecord(
            stream=_STREAM,
            kind="channel_archived",  # any non-message kind
            ts=_ts_for_day(date(2026, 6, 2)),
            payload={"note": "not a message"},
        ),
    )
    _seed_message(server_conn, day=date(2026, 6, 3))
    gaps = find_gaps_for_channel(server_conn, _CH)
    assert gaps == [GapRange(start=date(2026, 6, 2), end=date(2026, 6, 2))]


# ============================================================================
# Workspace aggregation
# ============================================================================


def test_workspace_gaps_aggregates_across_channels(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    # Channel A: has a 1-day gap
    stream_a = "channel:C_A"
    for d, seq in [(date(2026, 6, 1), 0), (date(2026, 6, 3), 0)]:
        ts = f"{_ts_for_day(d).split('.')[0]}.{seq:06d}"
        _ = write_event(
            server_conn,
            EventRecord(
                stream=stream_a,
                kind="message",
                ts=ts,
                payload={"ts": ts, "user": "U1", "text": "a", "subtype": None,
                         "thread_ts": None, "reply_count": 0, "files": [],
                         "edited": None, "reactions": []},
                dedup=True,
            ),
        )
    # Channel B: no gaps
    stream_b = "channel:C_B"
    for d in (date(2026, 6, 1), date(2026, 6, 2)):
        ts = f"{_ts_for_day(d).split('.')[0]}.000000"
        _ = write_event(
            server_conn,
            EventRecord(
                stream=stream_b,
                kind="message",
                ts=ts,
                payload={"ts": ts, "user": "U1", "text": "b", "subtype": None,
                         "thread_ts": None, "reply_count": 0, "files": [],
                         "edited": None, "reactions": []},
                dedup=True,
            ),
        )

    result = find_gaps_workspace(server_conn)
    # B has no gaps → omitted from the workspace view entirely.
    assert "C_A" in result
    assert "C_B" not in result
    assert result["C_A"] == [GapRange(start=date(2026, 6, 2), end=date(2026, 6, 2))]


# ============================================================================
# Markdown rendering
# ============================================================================


def test_render_channel_gaps_empty_when_no_gaps(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    _seed_message(server_conn, day=date(2026, 6, 1))
    _seed_message(server_conn, day=date(2026, 6, 2))
    body = render_channel_gaps(server_conn, _CH)
    assert body == b""


def test_render_channel_gaps_includes_summary_and_ranges(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    _seed_message(server_conn, day=date(2026, 6, 1))
    _seed_message(server_conn, day=date(2026, 6, 5))  # 3-day gap
    body = render_channel_gaps(server_conn, _CH)
    assert b"# Gaps for" in body
    assert b"channel_id: `C_GAPS`" in body
    assert b"2026-06-01" in body
    assert b"2026-06-05" in body
    assert b"2026-06-02 \xe2\x86\x92 2026-06-04 (3 days)" in body
    assert b"1 gap range(s)" in body


def test_render_workspace_gaps_empty_message_when_no_gaps_anywhere(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    body = render_workspace_gaps(server_conn)
    assert b"No gaps detected" in body


def test_render_workspace_gaps_ranks_by_total_missing_days(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    # Small gap on C_S (1 day), huge gap on C_L (5 days).
    for stream_id, gap_start, gap_end in [
        ("C_S", date(2026, 6, 1), date(2026, 6, 3)),   # 1-day gap (2026-06-02)
        ("C_L", date(2026, 6, 1), date(2026, 6, 7)),   # 5-day gap (06-02 → 06-06)
    ]:
        for ts_day in (gap_start, gap_end):
            ts = f"{_ts_for_day(ts_day).split('.')[0]}.000000"
            _ = write_event(
                server_conn,
                EventRecord(
                    stream=f"channel:{stream_id}",
                    kind="message",
                    ts=ts,
                    payload={"ts": ts, "user": "U1", "text": "x", "subtype": None,
                             "thread_ts": None, "reply_count": 0, "files": [],
                             "edited": None, "reactions": []},
                    dedup=True,
                ),
            )
    body = render_workspace_gaps(server_conn).decode()
    # C_L (5 missing) appears before C_S (1 missing).
    idx_large = body.index("C_L")
    idx_small = body.index("C_S")
    assert idx_large < idx_small
