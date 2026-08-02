"""Typed ``search.messages`` helper for per-channel message totals.

Slack's search API is user-scoped: callers must pass an ``httpx.Client``
authenticated with the configured *user* token. A bot token can return empty
or rejected search results and must not be used here.

DMs cannot be addressed with ``in:#channel-name``. The channel-total sweep
detects them from channel metadata and records ``unavailable`` without calling
this helper.
"""

from __future__ import annotations

from dataclasses import dataclass

import httpx
from pydantic import BaseModel, ConfigDict, Field

_SEARCH_MESSAGES_URL = "https://slack.com/api/search.messages"


class SearchMessagesError(RuntimeError):
    """Slack returned an unsuccessful or incomplete search response."""


class _WireModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore")


class _Paging(_WireModel):
    total: int | None = Field(default=None, ge=0)


class _Messages(_WireModel):
    total: int = Field(ge=0)
    paging: _Paging | None = None


class _SearchMessagesResponse(_WireModel):
    ok: bool
    error: str | None = None
    messages: _Messages | None = None


@dataclass(frozen=True, slots=True)
class SearchMessageTotal:
    total: int
    approximate: bool


def search_channel_message_total(
    http_client: httpx.Client,
    channel_name: str,
) -> SearchMessageTotal:
    """Return Slack's search-derived total for one named channel.

    Slack documents a default 10,000-result search ceiling (some workspaces
    have higher limits). When the top-level total reaches that threshold and
    the accompanying paging total disagrees, preserve the value but flag it as
    approximate so the inventory does not present false precision.
    """
    clean_name = channel_name.strip().lstrip("#")
    if not clean_name:
        raise ValueError("channel_name must not be empty")

    response = http_client.get(
        _SEARCH_MESSAGES_URL,
        params={"query": f"in:#{clean_name}", "count": 1},
    )
    response.raise_for_status()
    payload = _SearchMessagesResponse.model_validate_json(response.content)
    if not payload.ok:
        raise SearchMessagesError(payload.error or "search.messages returned ok=false")
    if payload.messages is None:
        raise SearchMessagesError("search.messages returned no messages object")

    total = payload.messages.total
    paging_total = payload.messages.paging.total if payload.messages.paging is not None else None
    approximate = total >= 10_000 and paging_total is not None and paging_total != total
    return SearchMessageTotal(total=total, approximate=approximate)


__all__ = ["SearchMessageTotal", "SearchMessagesError", "search_channel_message_total"]
