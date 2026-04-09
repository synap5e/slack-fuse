"""Slack Web API client."""

from __future__ import annotations

import logging
import time
from typing import Any

import httpx

from .models import (
    Channel,
    Edited,
    FileAttachment,
    HuddleInfo,
    Message,
    Reaction,
    Thread,
)

log = logging.getLogger(__name__)

_BASE_URL = "https://slack.com/api"
_PAGE_DELAY = 0.1  # Internal apps get generous rate limits


class RateLimitedError(Exception):
    def __init__(self, retry_after: float | None = None) -> None:
        self.retry_after = retry_after
        super().__init__(f"Rate limited (retry_after={retry_after})")


class FatalAPIError(Exception):
    """401/403 — stop retrying."""


class SlackClient:
    """Synchronous client for Slack Web API."""

    def __init__(self, token: str) -> None:
        self._token = token

    def _get(self, method: str, params: dict[str, str] | None = None) -> Any:
        resp = httpx.get(
            f"{_BASE_URL}/{method}",
            params=params,
            headers={"Authorization": f"Bearer {self._token}"},
            timeout=30.0,
        )
        if resp.status_code == 429:
            retry_after_raw = resp.headers.get("retry-after")
            retry_after = float(retry_after_raw) if retry_after_raw else None
            raise RateLimitedError(retry_after)
        if resp.status_code in (401, 403):
            raise FatalAPIError(f"HTTP {resp.status_code} on {method}")
        resp.raise_for_status()
        data = resp.json()
        if not data.get("ok"):
            error = data.get("error", "unknown")
            if error in ("token_revoked", "invalid_auth", "account_inactive"):
                raise FatalAPIError(f"Slack API error: {error}")
            log.warning("Slack API error on %s: %s", method, error)
        return data

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
            data = self._get("conversations.list", params)
            for ch in data.get("channels", []):
                channels.append(_parse_channel(ch))
            cursor = data.get("response_metadata", {}).get("next_cursor", "")
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
            data = self._get("conversations.history", params)
            for msg in data.get("messages", []):
                messages.append(parse_message(msg))
            if not data.get("has_more", False):
                break
            cursor = data.get("response_metadata", {}).get("next_cursor", "")
            if not cursor:
                break
            time.sleep(_PAGE_DELAY)
        # API returns newest first; reverse to chronological
        messages.reverse()
        return messages

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
            data = self._get("conversations.replies", params)
            for msg in data.get("messages", []):
                messages.append(parse_message(msg))
            if not data.get("has_more", False):
                break
            cursor = data.get("response_metadata", {}).get("next_cursor", "")
            if not cursor:
                break
            time.sleep(_PAGE_DELAY)

        if not messages:
            parent = Message(ts=thread_ts, user="unknown", text="")
            return Thread(parent=parent)

        return Thread(parent=messages[0], replies=tuple(messages[1:]))

    def get_file_info(self, file_id: str) -> dict[str, Any]:
        """Get file metadata including download URLs and huddle info."""
        data = self._get("files.info", {"file": file_id})
        return data.get("file", {})

    def get_huddle_info(self, file_id: str) -> HuddleInfo | None:
        """Get huddle info from a canvas file."""
        file_data = self.get_file_info(file_id)
        if not file_data.get("is_huddle_canvas"):
            return None
        return HuddleInfo(
            canvas_file_id=file_id,
            transcript_file_id=file_data.get("huddle_transcript_file_id"),
            date_start=file_data.get("huddle_date_start", 0),
            date_end=file_data.get("huddle_date_end", 0),
        )

    def search_huddle_canvases(self) -> list[dict[str, Any]]:
        """Search for all huddle note canvases."""
        results: list[dict[str, Any]] = []
        page = 1
        while True:
            data = self._get("search.files", {
                "query": "Huddle notes",
                "sort": "timestamp",
                "sort_dir": "desc",
                "count": "100",
                "page": str(page),
            })
            matches = data.get("files", {}).get("matches", [])
            if not matches:
                break
            results.extend(matches)
            total = data.get("files", {}).get("total", 0)
            if len(results) >= total:
                break
            page += 1
            time.sleep(_PAGE_DELAY)
        return results

    def get_history_page(
        self,
        channel_id: str,
        cursor: str = "",
    ) -> dict[str, Any]:
        """Fetch a single page of conversation history (for backfill)."""
        params: dict[str, str] = {
            "channel": channel_id,
            "limit": "200",
        }
        if cursor:
            params["cursor"] = cursor
        return self._get("conversations.history", params)

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


def _parse_channel(data: dict[str, Any]) -> Channel:
    return Channel(
        id=data["id"],
        name=data.get("name", data["id"]),
        is_private=data.get("is_private", False),
        is_im=data.get("is_im", False),
        is_mpim=data.get("is_mpim", False),
        topic=data.get("topic", {}).get("value", ""),
        purpose=data.get("purpose", {}).get("value", ""),
        num_members=data.get("num_members", 0),
        is_member=data.get("is_member", False),
        im_user_id=data.get("user"),
    )


def parse_message(data: dict[str, Any]) -> Message:
    reactions = tuple(
        Reaction(
            name=r["name"],
            count=r.get("count", 0),
            users=tuple(r.get("users", [])),
        )
        for r in data.get("reactions", [])
    )

    files = tuple(
        FileAttachment(
            id=f["id"],
            name=f.get("name", ""),
            title=f.get("title", ""),
            filetype=f.get("filetype", ""),
            mimetype=f.get("mimetype", ""),
            size=f.get("size", 0),
            url_private=f.get("url_private", ""),
            url_private_download=f.get("url_private_download", ""),
            is_huddle_canvas=f.get("is_huddle_canvas", False),
            huddle_transcript_file_id=f.get("huddle_transcript_file_id"),
        )
        for f in data.get("files", [])
    )

    edited_raw = data.get("edited")
    edited = (
        Edited(user=edited_raw["user"], ts=edited_raw["ts"])
        if edited_raw
        else None
    )

    return Message(
        ts=data["ts"],
        user=data.get("user", data.get("bot_id", "unknown")),
        text=data.get("text", ""),
        thread_ts=data.get("thread_ts"),
        reply_count=data.get("reply_count", 0),
        reactions=reactions,
        files=files,
        edited=edited,
        subtype=data.get("subtype"),
    )
