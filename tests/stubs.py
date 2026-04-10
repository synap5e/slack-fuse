"""Typed stub functions used by test fixtures.

basedpyright in strict mode complains about lambda parameters whose types
can't be inferred when passed to ``monkeypatch.setattr``. We hand it real
named functions with explicit types instead.
"""

from __future__ import annotations

from collections.abc import Callable

from slack_fuse.models import JsonObject, Message
from slack_fuse.user_cache import UserCache


def stub_get_channel_list() -> None:
    """Replacement for ``disk_cache.get_channel_list`` that returns no cache."""
    return None


def stub_get_huddle_index() -> None:
    """Replacement for ``disk_cache.get_huddle_index`` that returns no cache."""
    return None


def stub_get_known_dates(_channel_id: str) -> None:
    """Replacement for ``disk_cache.get_known_dates`` that returns no cache."""
    return None


def stub_load_from_disk(_self: UserCache) -> None:
    """Replacement for ``UserCache._load_from_disk`` that does nothing."""
    return None


def deterministic_random() -> float:
    """Replacement for ``random.random`` so backoff jitter is predictable."""
    return 0.5


def stub_get_day_messages(_channel_id: str, _date_str: str) -> list[JsonObject] | None:
    """Replacement for ``disk_cache.get_day_messages`` that returns no cache."""
    return None


def stub_put_day_messages(_channel_id: str, _date_str: str, _messages: list[JsonObject]) -> None:
    """Replacement for ``disk_cache.put_day_messages`` that does nothing."""
    return None


def stub_put_known_dates(_channel_id: str, _dates: set[str]) -> None:
    """Replacement for ``disk_cache.put_known_dates`` that does nothing."""
    return None


def make_stub_get_history(
    messages: list[Message],
) -> Callable[[str, str | None, str | None, int], list[Message]]:
    """Create a typed replacement for ``SlackClient.get_history``."""

    def _stub(
        _channel_id: str,
        _oldest: str | None = None,
        _latest: str | None = None,
        _limit: int = 200,
    ) -> list[Message]:
        return messages

    return _stub
