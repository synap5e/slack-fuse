"""Detect zero-message-day gaps in the event log.

A "gap" is a consecutive run of UTC days with no ``message`` events on a
channel stream, bounded on BOTH sides by days that DO have message events.
Trailing silence (channel just stopped, or hasn't ticked yet today) is not
a gap — it might be normal. Leading silence (no events before some date)
isn't either — it's just "we hadn't seen the channel yet."

This is the read-only diagnostic surface for ``gaps.md`` ghost files
(per-channel and workspace-wide). Triggering a backfill against a
detected gap stays on the operator-side CLI / k8s job path; this module
NEVER mutates state.

Future extension hook: the user wants a "checked-and-saw-nothing"
heartbeat event so a day with no messages but WITH a heartbeat is
provably empty, not just missing. ``_collect_active_days`` is the spot
to union heartbeat-event days into the active set — extend the SQL with
an OR on the heartbeat kind once that's in place.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow


@dataclass(frozen=True, slots=True)
class GapRange:
    """One contiguous run of missing days, inclusive on both ends.

    ``start`` and ``end`` are equal for a single-day gap. ``day_count`` is
    ``(end - start).days + 1``.
    """

    start: date
    end: date

    @property
    def day_count(self) -> int:
        return (self.end - self.start).days + 1


def _collect_active_days(
    conn: Connection[TupleRow],
    channel_id: str,
) -> list[date]:
    """Return sorted distinct UTC days that have at least one message event.

    UTC matches the events-table storage convention (Slack ts is UTC epoch
    seconds; we don't apply local-tz buckets here so the gap definition is
    timezone-agnostic at the server boundary). The FUSE client renders the
    bytes against its own tz where day boundaries matter for display.
    """
    stream = f"channel:{channel_id}"
    with conn.cursor() as cur:
        _ = cur.execute(
            """
            SELECT DISTINCT date_trunc('day', to_timestamp(ts::numeric)) AS day
            FROM events
            WHERE stream = %s
              AND kind = 'message'
              AND ts IS NOT NULL
            ORDER BY day
            """,
            (stream,),
        )
        return [row[0].date() if isinstance(row[0], datetime) else row[0] for row in cur.fetchall()]


def _collect_active_days_workspace(
    conn: Connection[TupleRow],
) -> dict[str, list[date]]:
    """Single query across all channels; group active-day sets in Python.

    One scan over the events table is far cheaper than 400+ per-channel
    round-trips for the workspace summary.
    """
    with conn.cursor() as cur:
        _ = cur.execute(
            """
            SELECT stream, date_trunc('day', to_timestamp(ts::numeric)) AS day
            FROM events
            WHERE stream LIKE 'channel:%'
              AND kind = 'message'
              AND ts IS NOT NULL
            GROUP BY stream, day
            ORDER BY stream, day
            """,
        )
        rows = cur.fetchall()
    by_channel: dict[str, list[date]] = {}
    for stream_raw, day_raw in rows:
        if not isinstance(stream_raw, str) or not stream_raw.startswith("channel:"):
            continue
        channel_id = stream_raw.removeprefix("channel:")
        day = day_raw.date() if isinstance(day_raw, datetime) else day_raw
        if not isinstance(day, date):
            continue
        by_channel.setdefault(channel_id, []).append(day)
    return by_channel


def _bounded_gaps(active_days: Iterable[date]) -> list[GapRange]:
    """Walk a sorted, distinct day sequence and emit gap ranges between them.

    Leading silence (before first active day) and trailing silence (after
    last active day) are NOT emitted — by definition a gap is bounded on
    both sides by activity. A single-day stream yields no gaps; an empty
    stream yields no gaps.
    """
    days = list(active_days)
    if len(days) < 2:
        return []
    gaps: list[GapRange] = []
    prev = days[0]
    for current in days[1:]:
        delta = (current - prev).days
        if delta > 1:
            gap_start = prev + timedelta(days=1)
            gap_end = current - timedelta(days=1)
            gaps.append(GapRange(start=gap_start, end=gap_end))
        prev = current
    return gaps


def find_gaps_for_channel(conn: Connection[TupleRow], channel_id: str) -> list[GapRange]:
    """All bounded gap ranges for one channel, ordered earliest first."""
    return _bounded_gaps(_collect_active_days(conn, channel_id))


def find_gaps_workspace(conn: Connection[TupleRow]) -> dict[str, list[GapRange]]:
    """All bounded gap ranges per channel for the whole workspace.

    Channels with no events at all are omitted. Channels with no gaps are
    also omitted — the workspace view is "where the holes are", not a
    full inventory.
    """
    active_by_channel = _collect_active_days_workspace(conn)
    result: dict[str, list[GapRange]] = {}
    for channel_id, days in active_by_channel.items():
        gaps = _bounded_gaps(days)
        if gaps:
            result[channel_id] = gaps
    return result


# ============================================================================
# Markdown rendering
# ============================================================================


def _fetch_channel_names(conn: Connection[TupleRow]) -> dict[str, str]:
    """Map channel_id → human-readable name from the channel-list stream.

    Uses the latest ``channel_added``/``channel_renamed`` payload per id so
    a renamed channel surfaces under its current name in the summary. Used
    only for display; gap detection itself is name-blind.
    """
    with conn.cursor() as cur:
        _ = cur.execute(
            """
            SELECT DISTINCT ON (payload->>'id')
                payload->>'id' AS chid,
                payload->>'name' AS name
            FROM events
            WHERE stream = 'channel-list'
              AND kind IN ('channel_added', 'channel_renamed')
              AND payload ? 'id'
            ORDER BY payload->>'id', id DESC
            """,
        )
        return {
            str(row[0]): str(row[1]) if row[1] is not None else "?"
            for row in cur.fetchall()
            if row[0] is not None
        }


def _format_gap_line(gap: GapRange) -> str:
    if gap.day_count == 1:
        return f"- {gap.start.isoformat()} (1 day)"
    return f"- {gap.start.isoformat()} → {gap.end.isoformat()} ({gap.day_count} days)"


def render_channel_gaps(
    conn: Connection[TupleRow],
    channel_id: str,
) -> bytes:
    """Render the per-channel ``gaps.md`` body.

    Returns empty bytes when there are no gaps — the FUSE side converts
    that to ENOENT so a clean channel doesn't materialize an empty ghost
    file (mirrors how channel.original.md handles empty days).
    """
    gaps = find_gaps_for_channel(conn, channel_id)
    if not gaps:
        return b""
    names = _fetch_channel_names(conn)
    name = names.get(channel_id, "?")
    active_days = _collect_active_days(conn, channel_id)
    first_seen = active_days[0].isoformat() if active_days else "?"
    last_seen = active_days[-1].isoformat() if active_days else "?"
    total_missing = sum(g.day_count for g in gaps)
    lines = [
        f"# Gaps for {name}",
        "",
        f"- channel_id: `{channel_id}`",
        f"- first event day: {first_seen}",
        f"- last event day: {last_seen}",
        f"- {len(gaps)} gap range(s), {total_missing} missing day(s) total",
        "",
        "## Missing day ranges",
        "",
        *(_format_gap_line(g) for g in gaps),
        "",
    ]
    return ("\n".join(lines)).encode()


def render_workspace_gaps(conn: Connection[TupleRow]) -> bytes:
    """Render the workspace ``/_workspace/gaps.md`` body.

    Channels are listed in descending order of total missing days so the
    biggest holes are at the top. Channels with no gaps don't appear at all.
    """
    by_channel = find_gaps_workspace(conn)
    if not by_channel:
        return b"# Workspace gaps\n\nNo gaps detected.\n"
    names = _fetch_channel_names(conn)
    ranked = sorted(
        by_channel.items(),
        key=lambda kv: (-sum(g.day_count for g in kv[1]), kv[0]),
    )
    lines: list[str] = [
        "# Workspace gaps",
        "",
        f"{len(ranked)} channel(s) with gaps. Ranked by total missing days.",
        "",
    ]
    for channel_id, gaps in ranked:
        name = names.get(channel_id, "?")
        total = sum(g.day_count for g in gaps)
        lines.append(f"## {name} (`{channel_id}`) — {total} missing day(s), {len(gaps)} range(s)")
        lines.append("")
        lines.extend(_format_gap_line(g) for g in gaps)
        lines.append("")
    return ("\n".join(lines)).encode()
