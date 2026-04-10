# pyright: reportPrivateUsage=false
"""Tests for SlackStore._api_call backoff state machine + cached_only_mode.

Focus on the typed wrapper that replaced string-based dispatch in the refactor.
We construct a real SlackStore but stub disk_cache so it doesn't read the
user's real cache, and feed lambdas to _api_call directly so we don't need a
fake client.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from datetime import datetime, timedelta
from typing import Any

import httpx
import pytest

from slack_fuse import disk_cache, store
from slack_fuse.api import FatalAPIError, RateLimitedError, SlackAPIError, SlackClient
from slack_fuse.models import Message, Thread
from slack_fuse.store import (
    _OLD_MSG_TTL,
    _RECENT_MSG_TTL,
    _THREAD_AGE_1H,
    _THREAD_AGE_24H,
    _THREAD_TTL_MID_FRACTION,
    _THREAD_TTL_RECENT,
    _THREAD_TTL_VERY_RECENT,
    SlackStore,
    _CachedThread,
)
from slack_fuse.user_cache import UserCache

from .stubs import (
    deterministic_random,
    make_stub_get_history,
    stub_get_channel_list,
    stub_get_day_messages,
    stub_get_huddle_index,
    stub_get_known_dates,
    stub_load_from_disk,
    stub_put_day_messages,
    stub_put_known_dates,
)


@pytest.fixture(autouse=True)
def disable_disk_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(disk_cache, "get_channel_list", stub_get_channel_list)
    monkeypatch.setattr(disk_cache, "get_huddle_index", stub_get_huddle_index)
    monkeypatch.setattr(disk_cache, "get_known_dates", stub_get_known_dates)


@pytest.fixture(autouse=True)
def deterministic_jitter(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(store.random, "random", deterministic_random)


@pytest.fixture
def fresh_store(monkeypatch: pytest.MonkeyPatch) -> Iterator[SlackStore]:
    monkeypatch.setattr(UserCache, "_load_from_disk", stub_load_from_disk)
    client = SlackClient(token="xoxp-fake")
    users = UserCache(client.http)
    yield SlackStore(client=client, users=users)


def _returning(value: object) -> Callable[[], Any]:
    return lambda: value


def _raising(exc: BaseException) -> Callable[[], Any]:
    def _go() -> Any:
        raise exc

    return _go


def test_api_call_returns_value_and_resets_backoff(fresh_store: SlackStore) -> None:
    sentinel = object()
    assert fresh_store._api_call(_returning(sentinel)) is sentinel

    # Recorded failure → backed off.
    _ = fresh_store._api_call(_raising(httpx.ConnectError("boom")))
    assert fresh_store._backoff.is_backed_off

    # Manually clear the wait so the next call is allowed; success should reset state.
    fresh_store._backoff.until = 0.0
    _ = fresh_store._api_call(_returning("ok"))
    assert fresh_store._backoff.is_backed_off is False


def test_api_call_rate_limit_records_backoff_and_short_circuits(
    fresh_store: SlackStore,
) -> None:
    result = fresh_store._api_call(_raising(RateLimitedError(retry_after=42.0)))
    assert result is None
    assert fresh_store._backoff.is_backed_off
    # Subsequent call must not invoke the callable
    assert fresh_store._api_call(_raising(RuntimeError("must not run"))) is None


def test_api_call_fatal_error_is_sticky(fresh_store: SlackStore) -> None:
    assert fresh_store._api_call(_raising(FatalAPIError("token_revoked"))) is None
    assert fresh_store.is_auth_fatal
    # All future calls return None even with a callable that would succeed
    assert fresh_store._api_call(_returning("anything")) is None
    # And does not invoke
    assert fresh_store._api_call(_raising(RuntimeError("nope"))) is None


def test_api_call_network_errors_record_failure(fresh_store: SlackStore) -> None:
    """Both timeouts and connect errors should be swallowed and recorded."""
    assert fresh_store._api_call(_raising(httpx.ReadTimeout("slow"))) is None
    assert fresh_store._backoff.is_backed_off
    fresh_store._backoff.until = 0.0  # clear deadline so the next call runs
    assert fresh_store._api_call(_raising(httpx.ConnectError("nope"))) is None
    assert fresh_store._backoff.is_backed_off


def test_api_call_unrecognized_exceptions_propagate(fresh_store: SlackStore) -> None:
    """The wrapper only swallows known recoverable errors. Bugs should bubble up."""
    with pytest.raises(KeyError):
        _ = fresh_store._api_call(_raising(KeyError("oops")))


def test_cached_only_mode_short_circuits_without_invoking(
    fresh_store: SlackStore,
) -> None:
    invoked = {"count": 0}

    def _track() -> str:
        invoked["count"] += 1
        return "result"

    with fresh_store.cached_only_mode():
        assert fresh_store._api_call(_track) is None

    assert invoked["count"] == 0
    # And the flag clears when the contextmanager exits
    assert fresh_store._api_call(_track) == "result"
    assert invoked["count"] == 1


def test_cached_only_mode_resets_after_exception(fresh_store: SlackStore) -> None:
    with pytest.raises(RuntimeError, match="boom"), fresh_store.cached_only_mode():
        raise RuntimeError("boom")
    assert fresh_store._api_call(_returning("after")) == "after"


def test_cached_only_mode_nested_ref_counting(fresh_store: SlackStore) -> None:
    """Nested cached_only_mode should stay cached until ALL exits."""
    with fresh_store.cached_only_mode():
        assert fresh_store._api_call(_returning("x")) is None
        with fresh_store.cached_only_mode():
            assert fresh_store._api_call(_returning("x")) is None
        # Still cached — outer context still active
        assert fresh_store._api_call(_returning("x")) is None
    # Now fully exited
    assert fresh_store._api_call(_returning("ok")) == "ok"


def test_already_backed_off_short_circuits_without_invoking(
    fresh_store: SlackStore,
) -> None:
    fresh_store._backoff.until = time.monotonic() + 1000
    invoked = {"count": 0}

    def _track() -> str:
        invoked["count"] += 1
        return "x"

    assert fresh_store._api_call(_track) is None
    assert invoked["count"] == 0


def test_force_refresh_clears_fatal_state(fresh_store: SlackStore) -> None:
    _ = fresh_store._api_call(_raising(FatalAPIError("token_revoked")))
    assert fresh_store.is_auth_fatal

    fresh_store.force_refresh()

    assert fresh_store.is_auth_fatal is False
    assert fresh_store._api_call(_returning("ok")) == "ok"


# === _date_ttl: today vs. earlier local-day boundary ===


def _local_date_offset(days: int) -> str:
    """Return a YYYY-MM-DD string offset from today's local date."""
    return (datetime.now().astimezone() - timedelta(days=days)).strftime("%Y-%m-%d")


def test_date_ttl_today_is_recent(fresh_store: SlackStore) -> None:
    assert fresh_store._date_ttl(_local_date_offset(0)) == _RECENT_MSG_TTL


def test_date_ttl_yesterday_is_locked_forever(fresh_store: SlackStore) -> None:
    assert fresh_store._date_ttl(_local_date_offset(1)) == _OLD_MSG_TTL


def test_date_ttl_arbitrary_past_date_is_locked(fresh_store: SlackStore) -> None:
    assert fresh_store._date_ttl(_local_date_offset(30)) == _OLD_MSG_TTL


def test_date_ttl_invalid_date_falls_back_to_recent(fresh_store: SlackStore) -> None:
    """Garbage date string should not crash; treat it as recent (safe default)."""
    assert fresh_store._date_ttl("not-a-date") == _RECENT_MSG_TTL


# === _thread_ttl: activity-based TTL tiers ===


def _ts_seconds_ago(seconds: float) -> str:
    """Return a Slack-style ts string for a moment `seconds` before now."""
    return f"{time.time() - seconds:.6f}"


def test_thread_ttl_very_recent(fresh_store: SlackStore) -> None:
    """Last reply < 1 hour ago -> 60s TTL."""
    ts = _ts_seconds_ago(1800)  # 30 min ago
    assert fresh_store._thread_ttl(ts, ts) == _THREAD_TTL_VERY_RECENT


def test_thread_ttl_recent(fresh_store: SlackStore) -> None:
    """Last reply < 24 hours ago -> 600s TTL."""
    ts = _ts_seconds_ago(6 * _THREAD_AGE_1H)  # 6 hours ago
    assert fresh_store._thread_ttl(ts, ts) == _THREAD_TTL_RECENT


def test_thread_ttl_mid_range(fresh_store: SlackStore) -> None:
    """Last reply 3 days ago -> 5% of age."""
    age = 3 * _THREAD_AGE_24H
    ts = _ts_seconds_ago(age)
    ttl = fresh_store._thread_ttl(ts, ts)
    expected = age * _THREAD_TTL_MID_FRACTION
    # Allow a small delta (a few seconds) from time.time() drift between
    # _ts_seconds_ago and the method call.
    assert abs(ttl - expected) < 10.0


def test_thread_ttl_old(fresh_store: SlackStore) -> None:
    """Last reply >= 7 days ago -> infinite TTL."""
    ts = _ts_seconds_ago(10 * _THREAD_AGE_24H)  # 10 days ago
    assert fresh_store._thread_ttl(ts, ts) == _OLD_MSG_TTL


def test_thread_ttl_none_last_reply_uses_thread_ts(fresh_store: SlackStore) -> None:
    """When last_reply_ts is None, falls back to thread_ts for age calculation."""
    ts = _ts_seconds_ago(6 * _THREAD_AGE_1H)  # 6 hours ago
    assert fresh_store._thread_ttl(ts, None) == _THREAD_TTL_RECENT


def test_thread_ttl_last_reply_overrides_thread_ts(fresh_store: SlackStore) -> None:
    """last_reply_ts takes precedence over an old thread_ts."""
    old_parent = _ts_seconds_ago(10 * _THREAD_AGE_24H)  # 10 days old parent
    recent_reply = _ts_seconds_ago(1800)  # 30 min ago reply
    assert fresh_store._thread_ttl(old_parent, recent_reply) == _THREAD_TTL_VERY_RECENT


def test_thread_ttl_invalid_ts_falls_back_to_very_recent(fresh_store: SlackStore) -> None:
    """Garbage ts -> very recent TTL (safe default)."""
    assert fresh_store._thread_ttl("not-a-float", None) == _THREAD_TTL_VERY_RECENT


# === get_thread: truly old threads survive indefinitely ===


def test_get_thread_old_thread_in_memory_does_not_expire(
    fresh_store: SlackStore,
) -> None:
    """A 7+ day old thread has infinite TTL and never expires from in-memory cache."""
    eight_days_ago = _ts_seconds_ago(8 * _THREAD_AGE_24H)
    parent = Message.model_validate({"ts": eight_days_ago, "user": "U1"})
    thread = Thread(parent=parent, replies=())

    # Backdate the in-memory entry by an hour — irrelevant for inf TTL.
    one_hour_ago = time.monotonic() - 3600
    fresh_store._thread_cache["C1", eight_days_ago] = _CachedThread(
        thread=thread,
        fetched_at=one_hour_ago,
    )

    result = fresh_store.get_thread("C1", eight_days_ago)
    assert result is thread


# === Opportunistic thread invalidation via latest_reply ===


def test_day_messages_invalidates_stale_thread_cache(
    fresh_store: SlackStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """get_day_messages evicts a cached thread when latest_reply is newer."""
    channel_id = "C123"
    date_str = datetime.now().astimezone().strftime("%Y-%m-%d")
    thread_ts = _ts_seconds_ago(_THREAD_AGE_1H)  # 1 hour ago

    # Seed a cached thread with one reply
    old_reply_ts = _ts_seconds_ago(1800)
    parent = Message(ts=thread_ts, user="U1", reply_count=2, thread_ts=thread_ts)
    reply = Message(ts=old_reply_ts, user="U2", thread_ts=thread_ts)
    thread = Thread(parent=parent, replies=(reply,))
    fresh_store._thread_cache[channel_id, thread_ts] = _CachedThread(
        thread=thread,
        fetched_at=time.monotonic(),
    )

    # API returns a parent whose latest_reply is newer than our cached reply
    new_reply_ts = _ts_seconds_ago(60)
    api_parent = Message(
        ts=thread_ts,
        user="U1",
        reply_count=3,
        thread_ts=thread_ts,
        latest_reply=new_reply_ts,
    )
    monkeypatch.setattr(fresh_store._client, "get_history", make_stub_get_history([api_parent]))
    monkeypatch.setattr(disk_cache, "get_day_messages", stub_get_day_messages)
    monkeypatch.setattr(disk_cache, "put_day_messages", stub_put_day_messages)
    monkeypatch.setattr(disk_cache, "put_known_dates", stub_put_known_dates)

    fresh_store.get_day_messages(channel_id, date_str)

    assert (channel_id, thread_ts) not in fresh_store._thread_cache


def test_day_messages_keeps_thread_cache_when_up_to_date(
    fresh_store: SlackStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """get_day_messages does NOT evict when latest_reply matches cached data."""
    channel_id = "C123"
    date_str = datetime.now().astimezone().strftime("%Y-%m-%d")
    thread_ts = _ts_seconds_ago(_THREAD_AGE_1H)

    reply_ts = _ts_seconds_ago(1800)
    parent = Message(ts=thread_ts, user="U1", reply_count=1, thread_ts=thread_ts)
    reply = Message(ts=reply_ts, user="U2", thread_ts=thread_ts)
    thread = Thread(parent=parent, replies=(reply,))
    fresh_store._thread_cache[channel_id, thread_ts] = _CachedThread(
        thread=thread,
        fetched_at=time.monotonic(),
    )

    # latest_reply matches the cached last reply — no eviction
    api_parent = Message(
        ts=thread_ts,
        user="U1",
        reply_count=1,
        thread_ts=thread_ts,
        latest_reply=reply_ts,
    )
    monkeypatch.setattr(fresh_store._client, "get_history", make_stub_get_history([api_parent]))
    monkeypatch.setattr(disk_cache, "get_day_messages", stub_get_day_messages)
    monkeypatch.setattr(disk_cache, "put_day_messages", stub_put_day_messages)
    monkeypatch.setattr(disk_cache, "put_known_dates", stub_put_known_dates)

    fresh_store.get_day_messages(channel_id, date_str)

    assert (channel_id, thread_ts) in fresh_store._thread_cache


# === Rate-limit jitter: must never produce a delay below retry_after ===


def test_rate_limit_jitter_never_goes_below_retry_after(fresh_store: SlackStore) -> None:
    """record_rate_limit uses positive-only jitter, so the computed
    `until` is always >= monotonic() + retry_after."""
    before = time.monotonic()
    fresh_store._backoff.record_rate_limit(retry_after=10.0)
    # Even with deterministic random (0.5), jitter should only be additive
    assert fresh_store._backoff.until >= before + 10.0


# === SlackAPIError (non-fatal ok=false) is caught by _api_call ===


def test_api_call_catches_slack_api_error(fresh_store: SlackStore) -> None:
    """Non-fatal SlackAPIError should be caught and recorded as a failure."""
    result = fresh_store._api_call(_raising(SlackAPIError("non-fatal: too_many_attachments")))
    assert result is None
    assert fresh_store._backoff.is_backed_off
    # Should NOT be fatal
    assert not fresh_store.is_auth_fatal
