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
from slack_fuse.api import FatalAPIError, RateLimitedError, SlackClient
from slack_fuse.models import Message, Thread
from slack_fuse.store import _OLD_MSG_TTL, _RECENT_MSG_TTL, SlackStore, _CachedThread
from slack_fuse.user_cache import UserCache

from .stubs import (
    deterministic_random,
    stub_get_channel_list,
    stub_get_huddle_index,
    stub_get_known_dates,
    stub_load_from_disk,
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
    users = UserCache(token="xoxp-fake")
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


# === _thread_ttl: same boundary, but on a Slack ts (float-as-string) ===


def _local_ts_offset(days: int) -> str:
    """Return a Slack-style ts string for a moment offset from `now` in local tz."""
    moment = datetime.now().astimezone() - timedelta(days=days)
    return f"{moment.timestamp():.6f}"


def test_thread_ttl_today_parent_is_recent(fresh_store: SlackStore) -> None:
    assert fresh_store._thread_ttl(_local_ts_offset(0)) == _RECENT_MSG_TTL


def test_thread_ttl_yesterday_parent_is_locked(fresh_store: SlackStore) -> None:
    assert fresh_store._thread_ttl(_local_ts_offset(1)) == _OLD_MSG_TTL


def test_thread_ttl_old_parent_is_locked(fresh_store: SlackStore) -> None:
    assert fresh_store._thread_ttl(_local_ts_offset(30)) == _OLD_MSG_TTL


def test_thread_ttl_invalid_ts_falls_back_to_recent(fresh_store: SlackStore) -> None:
    assert fresh_store._thread_ttl("not-a-float") == _RECENT_MSG_TTL


# === get_thread: old threads survive past 5-minute mark ===


def test_get_thread_old_thread_in_memory_does_not_expire(
    fresh_store: SlackStore,
) -> None:
    """Once an old thread is in memory, the 5-min TTL doesn't apply.

    Regression: previously the in-memory cache used a hardcoded 5-min TTL,
    so an old thread would expire and re-hit the API. With _thread_ttl,
    old threads have inf TTL.
    """
    yesterday_ts = _local_ts_offset(1)
    parent = Message.model_validate({"ts": yesterday_ts, "user": "U1"})
    thread = Thread(parent=parent, replies=())

    # Backdate the in-memory entry by an hour — way past _RECENT_MSG_TTL.
    one_hour_ago = time.monotonic() - 3600
    fresh_store._thread_cache["C1", yesterday_ts] = _CachedThread(
        thread=thread,
        fetched_at=one_hour_ago,
    )

    # Should still return from in-memory because the thread is old.
    result = fresh_store.get_thread("C1", yesterday_ts)
    assert result is thread
