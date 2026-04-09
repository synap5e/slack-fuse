"""Slack Web API client.

Pydantic at the I/O boundary: every Slack response is validated into a typed
model before it leaves this module. Downstream code never sees `dict[str, Any]`.
"""

from __future__ import annotations

import logging
import time
from typing import TypeVar

import httpx
from pydantic import BaseModel

from .models import (
    Channel,
    ConversationsHistoryResponse,
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


# === Client ===


class SlackClient:
    """Synchronous client for the Slack Web API."""

    def __init__(self, token: str) -> None:
        self._token = token

    @property
    def token(self) -> str:
        """Read-only access for modules that make their own httpx calls (canvas, transcript)."""
        return self._token

    def _get_raw(
        self,
        method: str,
        params: dict[str, str] | None = None,
    ) -> JsonObject:
        """Low-level GET. Handles HTTP/body errors, returns parsed JSON dict."""
        resp = httpx.get(
            f"{_BASE_URL}/{method}",
            params=params,
            headers={"Authorization": f"Bearer {self._token}"},
            timeout=30.0,
        )
        return self._handle_response(resp, method)

    def _post_raw(
        self,
        method: str,
        data: dict[str, str] | None = None,
    ) -> JsonObject:
        """POST variant for endpoints that take a form body."""
        resp = httpx.post(
            f"{_BASE_URL}/{method}",
            data=data,
            headers={"Authorization": f"Bearer {self._token}"},
            timeout=30.0,
        )
        return self._handle_response(resp, method)

    def _handle_response(self, resp: httpx.Response, method: str) -> JsonObject:
        if resp.status_code == 429:
            retry_after_raw = resp.headers.get("retry-after")
            retry_after = float(retry_after_raw) if retry_after_raw else None
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
            log.warning("Slack API error on %s: %s", method, error)
        return body

    def _get(
        self,
        method: str,
        params: dict[str, str] | None,
        response_type: type[_T],
    ) -> _T:
        """Typed GET — validates the response into the given Pydantic model."""
        return response_type.model_validate(self._get_raw(method, params))

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
    ) -> list[Channel]:
        """List all conversations the user has access to."""
        channels: list[Channel] = []
        cursor = ""
        while True:
            params: dict[str, str] = {
                "types": types,
                "limit": "200",
                "exclude_archived": "true",
            }
            if cursor:
                params["cursor"] = cursor
            resp = self._get("conversations.list", params, ConversationsListResponse)
            channels.extend(resp.channels)
            cursor = resp.response_metadata.next_cursor
            if not cursor:
                break
            time.sleep(_PAGE_DELAY)
        return channels

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
                "conversations.history", params, ConversationsHistoryResponse,
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
    ) -> ConversationsHistoryResponse:
        """Fetch a single page of conversation history (used by backfill)."""
        params: dict[str, str] = {
            "channel": channel_id,
            "limit": "200",
        }
        if cursor:
            params["cursor"] = cursor
        return self._get("conversations.history", params, ConversationsHistoryResponse)

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
                "conversations.replies", params, ConversationsRepliesResponse,
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

    def download_file(self, url: str) -> bytes:
        """Download a file from Slack using authentication."""
        resp = httpx.get(
            url,
            headers={"Authorization": f"Bearer {self._token}"},
            timeout=60.0,
            follow_redirects=True,
        )
        resp.raise_for_status()
        return resp.content
