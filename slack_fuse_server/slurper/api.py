"""Slack Web API client (lifted from `slack_fuse/api.py`).

Sprint 1 does not redesign the Slack-API layer — this is a near-verbatim lift
of the single-process client. Pydantic at the I/O boundary: every Slack
response is validated into a typed model before it leaves this module, so
downstream slurper code never sees `dict[str, Any]`. The domain models are
imported from `slack_fuse.models`, the shared contract (see
`slack_fuse_server.backfill.types`, which already depends on it).

Exception hierarchy: `SlackAPIError` base (every `ok=false`), `RateLimitedError`
(429), `FatalAPIError` (401/403 + unrecoverable body errors).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import TypeVar, cast

import httpx
from pydantic import BaseModel

from slack_fuse.models import (
    Channel,
    ChatGetPermalinkResponse,
    ConversationsHistoryResponse,
    ConversationsInfoResponse,
    ConversationsListResponse,
    ConversationsRepliesResponse,
    FilesInfoResponse,
    HuddleInfo,
    JsonObject,
    Message,
    SearchFile,
    SearchFilesResponse,
    SlackFile,
    Thread,
)

log = logging.getLogger(__name__)

_BASE_URL = "https://slack.com/api"
_PAGE_DELAY = 0.1  # Internal apps get generous rate limits

_T = TypeVar("_T", bound=BaseModel)


@dataclass(frozen=True, slots=True)
class Validated[M]:
    """Lossless capture of a Slack API response.

    Event sourcing's value is being able to go back and project information
    we weren't using at the time — but only if we KEPT it. Pydantic
    ``model_dump`` drops fields we don't declare and reshapes nested ones
    (e.g. our ``topic`` is the flat string lifted from
    ``topic: {value, creator, last_set}``). Persisting that to the events
    table threw away everything we hadn't thought to use yet.

    ``Validated`` pairs the raw dict from the wire (the slurper persists
    this) with the validated model (the slurper uses this for in-process
    logic). The events table stays the lossless source of truth; future
    projections can read fields the current model doesn't even know about.
    """

    raw: JsonObject
    model: M


# Slack ok=False values that mean "stop, this won't recover"
_FATAL_BODY_ERRORS = frozenset({"token_revoked", "invalid_auth", "account_inactive"})


# === Exception hierarchy ===


class SlackAPIError(Exception):
    """Base for all Slack-API-related errors raised by this module."""


class RateLimitedError(SlackAPIError):
    def __init__(self, retry_after: float | None = None) -> None:
        self.retry_after = retry_after
        super().__init__(f"Rate limited (retry_after={retry_after})")


class FatalAPIError(SlackAPIError):
    """401/403 or unrecoverable body error — stop retrying."""


class ChannelNotFoundError(SlackAPIError):
    """``conversations.info`` (etc.) returned ``ok=false, error=channel_not_found``.

    Distinct from the generic ``SlackAPIError`` so callers can choose to skip
    that channel cleanly (e.g. admin backfill for a channel the user token no
    longer has access to) instead of failing the whole operation. Subclass of
    ``SlackAPIError`` so existing broad catches still cover it.
    """


# === Client ===


class SlackClient:
    """Synchronous client for the Slack Web API."""

    def __init__(self, token: str) -> None:
        self._token = token
        self._http = httpx.Client(
            headers={"Authorization": f"Bearer {token}"},
            timeout=30.0,
        )

    @property
    def token(self) -> str:
        """Read-only access for modules that make their own httpx calls."""
        return self._token

    @property
    def http(self) -> httpx.Client:
        """Shared HTTP client for modules that need authenticated requests."""
        return self._http

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._http.close()

    def _get_raw(
        self,
        method: str,
        params: dict[str, str] | None = None,
    ) -> JsonObject:
        """Low-level GET. Handles HTTP/body errors, returns parsed JSON dict."""
        resp = self._http.get(f"{_BASE_URL}/{method}", params=params)
        return self._handle_response(resp, method)

    def _post_raw(
        self,
        method: str,
        data: dict[str, str] | None = None,
    ) -> JsonObject:
        """POST variant for endpoints that take a form body."""
        resp = self._http.post(f"{_BASE_URL}/{method}", data=data)
        return self._handle_response(resp, method)

    def _handle_response(self, resp: httpx.Response, method: str) -> JsonObject:
        if resp.status_code == 429:
            retry_after_raw = resp.headers.get("retry-after")
            try:
                retry_after = float(retry_after_raw) if retry_after_raw else None
            except ValueError:
                retry_after = None
            raise RateLimitedError(retry_after)
        if resp.status_code in (401, 403):
            raise FatalAPIError(f"HTTP {resp.status_code} on {method}")
        resp.raise_for_status()
        body: JsonObject = resp.json()
        ok = body.get("ok")
        if ok is not True:
            error_val = body.get("error", "unknown")
            error = error_val if isinstance(error_val, str) else "unknown"
            if error in _FATAL_BODY_ERRORS:
                raise FatalAPIError(f"Slack API error: {error}")
            if error == "channel_not_found":
                raise ChannelNotFoundError(f"Slack API error on {method}: {error}")
            raise SlackAPIError(f"Slack API error on {method}: {error}")
        return body

    def _get(
        self,
        method: str,
        params: dict[str, str] | None,
        response_type: type[_T],
    ) -> _T:
        """Typed GET — validates the response into the given Pydantic model.

        Use this for endpoints whose response is consumed in-process only
        (huddle index, permalink lookup, etc). For endpoints whose response
        we PERSIST to the events table, use :meth:`_get_validated` so the
        raw payload survives the trip.
        """
        return response_type.model_validate(self._get_raw(method, params))

    def _get_validated(
        self,
        method: str,
        params: dict[str, str] | None,
        response_type: type[_T],
    ) -> Validated[_T]:
        """GET that preserves the raw wire dict alongside the validated model.

        Callers that persist response data to the events table use
        ``Validated.raw`` for the payload and ``Validated.model`` for any
        in-process logic.
        """
        raw = self._get_raw(method, params)
        return Validated(raw=raw, model=response_type.model_validate(raw))

    def _post(
        self,
        method: str,
        data: dict[str, str] | None,
        response_type: type[_T],
    ) -> _T:
        """Typed POST — validates the response into the given Pydantic model."""
        return response_type.model_validate(self._post_raw(method, data))

    # === High-level methods ===

    def list_conversations(
        self,
        types: str = "public_channel,private_channel,mpim,im",
    ) -> list[Validated[Channel]]:
        """List all conversations the user has access to.

        Returns each channel as a ``Validated[Channel]`` so persistence
        sites can write the raw dict (lossless) while in-process logic
        keeps using the typed model.
        """
        channels: list[Validated[Channel]] = []
        cursor = ""
        while True:
            params: dict[str, str] = {
                "types": types,
                "limit": "200",
                "exclude_archived": "true",
            }
            if cursor:
                params["cursor"] = cursor
            page = self._get_validated("conversations.list", params, ConversationsListResponse)
            # Pair each validated model with its raw dict by index — the raw
            # array in ``page.raw["channels"]`` matches ``page.model.channels``
            # one-to-one because both come from the same wire payload.
            raw_list = page.raw.get("channels")
            if not isinstance(raw_list, list):
                raw_list = []
            for raw_item, model_item in zip(raw_list, page.model.channels, strict=False):
                if isinstance(raw_item, dict):
                    channels.append(Validated(raw=cast(JsonObject, raw_item), model=model_item))
            cursor = page.model.response_metadata.next_cursor
            if not cursor:
                break
            time.sleep(_PAGE_DELAY)
        return channels

    def get_channel_info(self, channel_id: str) -> Validated[Channel]:
        """Fetch info for a single channel by ID, lossless."""
        page = self._get_validated(
            "conversations.info", {"channel": channel_id}, ConversationsInfoResponse
        )
        if page.model.channel is None:
            raise SlackAPIError(f"conversations.info returned no channel for {channel_id}")
        raw_channel = page.raw.get("channel")
        if not isinstance(raw_channel, dict):
            # ``page.model.channel`` was validated above so the dict IS there;
            # this only fires if the wire response was reshaped after validate.
            raise SlackAPIError(f"conversations.info missing 'channel' dict for {channel_id}")
        return Validated(raw=cast(JsonObject, raw_channel), model=page.model.channel)

    def get_history(
        self,
        channel_id: str,
        oldest: str | None = None,
        latest: str | None = None,
        limit: int = 200,
    ) -> list[Message]:
        """Fetch messages from a channel within a time range."""
        messages: list[Message] = []
        cursor = ""
        while True:
            params: dict[str, str] = {
                "channel": channel_id,
                "limit": str(limit),
            }
            if oldest:
                params["oldest"] = oldest
            if latest:
                params["latest"] = latest
            if cursor:
                params["cursor"] = cursor
            resp = self._get(
                "conversations.history",
                params,
                ConversationsHistoryResponse,
            )
            messages.extend(resp.messages)
            if not resp.has_more:
                break
            cursor = resp.response_metadata.next_cursor
            if not cursor:
                break
            time.sleep(_PAGE_DELAY)
        # API returns newest first; reverse to chronological
        messages.reverse()
        return messages

    def get_history_page(
        self,
        channel_id: str,
        cursor: str = "",
        oldest: float | None = None,
    ) -> Validated[ConversationsHistoryResponse]:
        """Fetch a single page of conversation history (used by backfill).

        `oldest` (Slack's API parameter name) is the inclusive lower bound on
        ts. Setting it tells Slack to skip messages older than the cutoff
        before paging starts, which is critical for gap-fill efficiency: a
        24-hour-old `--since` against a 2-year-old channel should be one
        page of API spend, not 30.

        Returns the wrapped response so the backfill loop can persist the
        raw message dicts (lossless) — index-pair ``page.raw["messages"][i]``
        with ``page.model.messages[i]``.
        """
        params: dict[str, str] = {
            "channel": channel_id,
            "limit": "200",
        }
        if cursor:
            params["cursor"] = cursor
        if oldest is not None:
            params["oldest"] = f"{oldest:.6f}"
        return self._get_validated("conversations.history", params, ConversationsHistoryResponse)

    def get_replies(self, channel_id: str, thread_ts: str) -> Thread:
        """Fetch all replies in a thread."""
        messages: list[Message] = []
        cursor = ""
        while True:
            params: dict[str, str] = {
                "channel": channel_id,
                "ts": thread_ts,
                "limit": "200",
            }
            if cursor:
                params["cursor"] = cursor
            resp = self._get(
                "conversations.replies",
                params,
                ConversationsRepliesResponse,
            )
            messages.extend(resp.messages)
            if not resp.has_more:
                break
            cursor = resp.response_metadata.next_cursor
            if not cursor:
                break
            time.sleep(_PAGE_DELAY)

        if not messages:
            parent = Message(ts=thread_ts, user="unknown", text="")
            return Thread(parent=parent)

        return Thread(parent=messages[0], replies=tuple(messages[1:]))

    def get_permalink(self, channel_id: str, message_ts: str) -> str:
        """Fetch Slack's canonical permalink for a message."""
        resp = self._get(
            "chat.getPermalink",
            {"channel": channel_id, "message_ts": message_ts},
            ChatGetPermalinkResponse,
        )
        return resp.permalink

    def get_file_info(self, file_id: str) -> SlackFile | None:
        """Get file metadata. Returns None if Slack reports the file is missing."""
        resp = self._get("files.info", {"file": file_id}, FilesInfoResponse)
        return resp.file

    def get_huddle_info(self, file_id: str) -> HuddleInfo | None:
        """Get huddle info from a canvas file."""
        f = self.get_file_info(file_id)
        if f is None or not f.is_huddle_canvas:
            return None
        return HuddleInfo(
            canvas_file_id=file_id,
            transcript_file_id=f.huddle_transcript_file_id,
            date_start=f.huddle_date_start,
            date_end=f.huddle_date_end,
        )

    def search_huddle_canvases(self) -> list[SearchFile]:
        """Search for all huddle note canvases. Returns typed SearchFile records."""
        out: list[SearchFile] = []
        page = 1
        while True:
            resp = self._get(
                "search.files",
                {
                    "query": "Huddle notes",
                    "sort": "timestamp",
                    "sort_dir": "desc",
                    "count": "100",
                    "page": str(page),
                },
                SearchFilesResponse,
            )
            if not resp.files.matches:
                break
            out.extend(resp.files.matches)
            if len(out) >= resp.files.total:
                break
            page += 1
            time.sleep(_PAGE_DELAY)
        return out
