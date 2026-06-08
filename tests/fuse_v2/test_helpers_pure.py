"""Pure-function tests for ``slack_fuse.fuse_v2_helpers``.

No database required. Exercises path parsing, range-bound arithmetic across
three timezones (UTC, NZST → Pacific/Auckland, PST → America/Los_Angeles),
slug derivation, and the staleness-reason classifier.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from slack_fuse.fuse_v2_helpers import (
    ChannelRow,
    StalenessState,
    build_channel_slug,
    conv_root_for,
    day_channel_frontmatter,
    dedup_thread_slug_map,
    derive_thread_slug,
    format_trailer,
    is_valid_day,
    is_valid_month,
    local_day_utc_range,
    local_month_utc_range,
    parse_day_date,
    parse_path,
    slug_map_for,
    staleness_reason,
    ts_to_local_date,
)

# ============================================================================
# Path parsing
# ============================================================================


def test_parse_path_root() -> None:
    assert parse_path("/") == []


def test_parse_path_strips_leading_and_trailing_slashes() -> None:
    assert parse_path("/channels/general/") == ["channels", "general"]


def test_parse_path_nested() -> None:
    assert parse_path("/channels/general/2026-06/08/thread-foo/thread.md") == [
        "channels",
        "general",
        "2026-06",
        "08",
        "thread-foo",
        "thread.md",
    ]


def test_is_valid_month_yes() -> None:
    assert is_valid_month("2026-06")


def test_is_valid_month_no() -> None:
    assert not is_valid_month("2026/06")
    assert not is_valid_month("2026-13")
    assert not is_valid_month("xx-yy")
    assert not is_valid_month("2026-6")


def test_is_valid_day() -> None:
    assert is_valid_day("01")
    assert is_valid_day("31")
    assert not is_valid_day("32")
    assert not is_valid_day("1")
    assert not is_valid_day("00")


def test_parse_day_date_valid() -> None:
    assert parse_day_date("2026-06", "08") == date(2026, 6, 8)


def test_parse_day_date_invalid_combo() -> None:
    # Feb 30 doesn't exist.
    assert parse_day_date("2026-02", "30") is None
    assert parse_day_date("notamonth", "01") is None


# ============================================================================
# Day-range bounds — three timezones (UTC, NZST, PST)
# ============================================================================


def test_local_day_utc_range_utc() -> None:
    tz = ZoneInfo("UTC")
    start, end = local_day_utc_range(date(2026, 6, 8), tz)
    # 2026-06-08 00:00 UTC → epoch 1780531200, +86400.
    assert end - start == Decimal("86400")
    assert datetime.fromtimestamp(float(start), tz=UTC) == datetime(2026, 6, 8, 0, 0, tzinfo=UTC)


def test_local_day_utc_range_pacific_auckland() -> None:
    tz = ZoneInfo("Pacific/Auckland")
    # 2026-06-08 00:00 NZST is 2026-06-07 12:00 UTC (NZST = UTC+12 in June).
    start, end = local_day_utc_range(date(2026, 6, 8), tz)
    start_dt = datetime.fromtimestamp(float(start), tz=UTC)
    end_dt = datetime.fromtimestamp(float(end), tz=UTC)
    assert start_dt == datetime(2026, 6, 7, 12, 0, tzinfo=UTC)
    assert end_dt == datetime(2026, 6, 8, 12, 0, tzinfo=UTC)


def test_local_day_utc_range_la() -> None:
    tz = ZoneInfo("America/Los_Angeles")
    # 2026-06-08 is in DST; LA local 00:00 = UTC 07:00 (UTC-7 PDT).
    start, end = local_day_utc_range(date(2026, 6, 8), tz)
    start_dt = datetime.fromtimestamp(float(start), tz=UTC)
    end_dt = datetime.fromtimestamp(float(end), tz=UTC)
    assert start_dt == datetime(2026, 6, 8, 7, 0, tzinfo=UTC)
    assert end_dt == datetime(2026, 6, 9, 7, 0, tzinfo=UTC)


def test_local_day_utc_range_dst_spring_forward() -> None:
    """LA spring-forward 2026-03-08: clocks jump 02:00 → 03:00 local."""
    tz = ZoneInfo("America/Los_Angeles")
    start, end = local_day_utc_range(date(2026, 3, 8), tz)
    diff_seconds = float(end - start)
    # 23 hours on a spring-forward day.
    assert diff_seconds == 23 * 3600


def test_local_month_utc_range_utc() -> None:
    tz = ZoneInfo("UTC")
    start, end = local_month_utc_range("2026-06", tz)
    assert datetime.fromtimestamp(float(start), tz=UTC) == datetime(2026, 6, 1, 0, 0, tzinfo=UTC)
    assert datetime.fromtimestamp(float(end), tz=UTC) == datetime(2026, 7, 1, 0, 0, tzinfo=UTC)


def test_ts_to_local_date_consistency_across_tz() -> None:
    # 2026-06-08 03:00 UTC, derived rather than hardcoded so this stays right.
    ts = Decimal(str(datetime(2026, 6, 8, 3, 0, tzinfo=UTC).timestamp()))
    assert ts_to_local_date(ts, ZoneInfo("UTC")) == date(2026, 6, 8)
    # NZST: 2026-06-08 15:00 → same day.
    assert ts_to_local_date(ts, ZoneInfo("Pacific/Auckland")) == date(2026, 6, 8)
    # LA: 2026-06-07 20:00 PDT → previous day.
    assert ts_to_local_date(ts, ZoneInfo("America/Los_Angeles")) == date(2026, 6, 7)


def test_local_month_utc_range_december_rollover() -> None:
    tz = ZoneInfo("UTC")
    _start, end = local_month_utc_range("2026-12", tz)
    assert datetime.fromtimestamp(float(end), tz=UTC) == datetime(2027, 1, 1, 0, 0, tzinfo=UTC)


# ============================================================================
# Slug derivation
# ============================================================================


def _channel_row(channel_id: str, name: str = "", **kw: object) -> ChannelRow:
    im_user_raw = kw.get("im_user_id")
    return ChannelRow(
        channel_id=channel_id,
        name=name,
        is_im=bool(kw.get("is_im")),
        is_mpim=bool(kw.get("is_mpim")),
        is_member=bool(kw.get("is_member", True)),
        is_archived=bool(kw.get("is_archived")),
        im_user_id=im_user_raw if isinstance(im_user_raw, str) else None,
        tier=str(kw.get("tier", "hot")),
    )


def test_conv_root_for() -> None:
    assert conv_root_for(_channel_row("C1", "general", is_member=True)) == "channels"
    assert conv_root_for(_channel_row("C2", "engineering", is_member=False)) == "other-channels"
    assert conv_root_for(_channel_row("D1", "", is_im=True, im_user_id="U1")) == "dms"
    assert conv_root_for(_channel_row("G1", "", is_mpim=True)) == "group-dms"


def test_build_channel_slug_non_dm() -> None:
    counts: dict[str, int] = {}
    row = _channel_row("C1", "General")
    assert build_channel_slug(row, {}, counts) == "general"


def test_build_channel_slug_dedup() -> None:
    counts: dict[str, int] = {}
    a = _channel_row("C1", "Engineering")
    b = _channel_row("C2", "Engineering")
    assert build_channel_slug(a, {}, counts) == "engineering"
    assert build_channel_slug(b, {}, counts) == "engineering-2"


def test_build_channel_slug_dm_uses_user_display() -> None:
    counts: dict[str, int] = {}
    row = _channel_row("D1", "", is_im=True, im_user_id="U999")
    users = {"U999": "Alice Smith"}
    assert build_channel_slug(row, users, counts) == "alice-smith"


def test_slug_map_is_deterministic() -> None:
    rows = [
        _channel_row("C1", "general"),
        _channel_row("C2", "general"),
        _channel_row("C3", "random"),
    ]
    m = slug_map_for(rows, {})
    assert m == {"C1": "general", "C2": "general-2", "C3": "random"}


def test_derive_thread_slug_from_body() -> None:
    content = "## 14:30 <@U999>\n\nHello world, can someone help with the deploy?\n\n> Thread: 2 replies\n"
    assert derive_thread_slug(content, Decimal("1700000000.000")).startswith("hello-world")


def test_derive_thread_slug_fallback_for_empty_body() -> None:
    content = "## 14:30 <@U999>\n\n\n"
    out = derive_thread_slug(content, Decimal("1700000000.000"))
    assert out == "ts-1700000000.000"


def test_dedup_thread_slug_map_orders_by_ts() -> None:
    parents = [
        (Decimal("1700000000"), "## 14:30 <@U1>\n\nDeploy update\n"),
        (Decimal("1700000100"), "## 14:32 <@U2>\n\nDeploy update\n"),
    ]
    m = dedup_thread_slug_map(parents)
    assert list(m.keys()) == ["deploy-update", "deploy-update-2"]
    assert m["deploy-update"] == Decimal("1700000000")
    assert m["deploy-update-2"] == Decimal("1700000100")


# ============================================================================
# Frontmatter
# ============================================================================


def test_day_channel_frontmatter_is_yaml() -> None:
    row = _channel_row("C1", "general")
    out = day_channel_frontmatter(row, date(2026, 6, 8))
    assert out.startswith("---\n")
    assert "channel: general" in out
    assert "channel_id: C1" in out
    assert "date: 2026-06-08" in out


# ============================================================================
# Staleness classifier
# ============================================================================


def _state(
    *,
    health: str = "healthy",
    frame_seconds_ago: float = 1.0,
    caught_up: bool = True,
) -> StalenessState:
    now = datetime(2026, 6, 8, 12, 0, tzinfo=UTC)
    return StalenessState(
        last_frame_at=now - timedelta(seconds=frame_seconds_ago),
        last_slurper_health=health,
        last_health_update_at=now,
        initial_catch_up_done_for_stream=caught_up,
    )


def _now() -> datetime:
    return datetime(2026, 6, 8, 12, 0, tzinfo=UTC)


def test_staleness_clean() -> None:
    assert staleness_reason(_state(), now=_now()) is None


def test_staleness_disconnected_health() -> None:
    reason = staleness_reason(_state(health="disconnected"), now=_now())
    assert reason == "socket-mode disconnected"


def test_staleness_degraded_health() -> None:
    reason = staleness_reason(_state(health="degraded"), now=_now())
    assert reason == "slack ingestion unhealthy"


def test_staleness_auth_failed() -> None:
    reason = staleness_reason(_state(health="auth_failed"), now=_now())
    assert reason == "auth token invalid"


def test_staleness_old_frame_implies_server_unreachable() -> None:
    reason = staleness_reason(_state(frame_seconds_ago=120, caught_up=True), now=_now())
    assert reason == "server unreachable"


def test_staleness_not_caught_up_implies_catching_up() -> None:
    reason = staleness_reason(_state(caught_up=False, frame_seconds_ago=1), now=_now())
    assert reason == "catching up after reconnect"


def test_format_trailer_includes_timestamp() -> None:
    out = format_trailer("server unreachable", datetime(2026, 6, 8, 9, 42, 11, tzinfo=UTC))
    assert "2026-06-08 09:42:11 UTC" in out
    assert "Reason: server unreachable" in out
    assert out.startswith("\n---\n\n")
