"""User ID → display name resolution with persistent disk cache.

I/O boundary models live in `models.py`; this file validates every Slack
response into one of those models so the rest of the cache logic is fully typed.
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

import httpx

from .models import (
    BotsInfoResponse,
    JsonObject,
    UsersInfoResponse,
    UsersListResponse,
)

log = logging.getLogger(__name__)

_CACHE_PATH = Path.home() / ".cache" / "slack-fuse" / "users.json"
_TTL = 86400.0  # 24 hours
_BASE_URL = "https://slack.com/api"


class UserCache:
    """Resolves Slack user IDs to display names.

    Persists to disk so the cache survives restarts. Unknown IDs
    trigger an immediate API fetch.
    """

    def __init__(self, http: httpx.Client) -> None:
        self._http = http
        self._users: dict[str, str] = {}  # user_id -> display_name
        self._loaded_at: float = 0.0
        self._load_from_disk()

    def _load_from_disk(self) -> None:
        if _CACHE_PATH.exists():
            try:
                data: JsonObject = json.loads(_CACHE_PATH.read_text())
                raw_users = data.get("users", {})
                if isinstance(raw_users, dict):
                    self._users = {k: v for k, v in raw_users.items() if isinstance(v, str)}
                loaded_at = data.get("loaded_at", 0.0)
                if isinstance(loaded_at, (int, float)):
                    self._loaded_at = float(loaded_at)
                log.info("Loaded %d users from disk cache", len(self._users))
            except (json.JSONDecodeError, KeyError):
                log.warning("Corrupt user cache, starting fresh")

    def _save_to_disk(self) -> None:
        _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = _CACHE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps({"users": self._users, "loaded_at": self._loaded_at}))
        os.replace(tmp, _CACHE_PATH)

    def _get_json(self, path: str, params: dict[str, str] | None = None) -> JsonObject:
        resp = self._http.get(f"{_BASE_URL}{path}", params=params)
        resp.raise_for_status()
        body: JsonObject = resp.json()
        return body

    def _fetch_user(self, user_id: str) -> str:
        """Fetch a single user's or bot's display name from the API.

        Caches misses (returns the raw id) so we don't retry on every lookup.
        """
        try:
            if user_id.startswith("B"):
                bot_resp = BotsInfoResponse.model_validate(
                    self._get_json("/bots.info", {"bot": user_id}),
                )
                if not bot_resp.ok or bot_resp.bot is None:
                    name = user_id
                else:
                    name = bot_resp.bot.name or user_id
            else:
                user_resp = UsersInfoResponse.model_validate(
                    self._get_json("/users.info", {"user": user_id}),
                )
                if not user_resp.ok or user_resp.user is None:
                    name = user_id
                else:
                    name = user_resp.user.display() or user_id
            self._users[user_id] = name
            self._save_to_disk()
            return name
        except (httpx.HTTPError, ValueError) as e:
            log.warning("Failed to fetch user/bot %s: %s", user_id, e)
            self._users[user_id] = user_id
            self._save_to_disk()
            return user_id

    def populate(self) -> None:
        """Bulk-load all workspace users. Called once on first run or when stale."""
        if self._users and (time.time() - self._loaded_at) < _TTL:
            return

        log.info("Populating user cache from users.list")
        cursor = ""
        count = 0
        try:
            while True:
                params: dict[str, str] = {"limit": "200"}
                if cursor:
                    params["cursor"] = cursor
                resp = UsersListResponse.model_validate(
                    self._get_json("/users.list", params),
                )
                if not resp.ok:
                    log.warning("users.list failed: %s", resp.error)
                    break
                for member in resp.members:
                    self._users[member.id] = member.display()
                    count += 1
                cursor = resp.response_metadata.next_cursor
                if not cursor:
                    break
        except (httpx.HTTPError, ValueError) as e:
            log.warning("Error populating user cache: %s", e)

        self._loaded_at = time.time()
        self._save_to_disk()
        log.info("User cache populated with %d users", count)

    def get_display_name(self, user_id: str) -> str:
        """Resolve user ID to display name. Fetches from API if unknown."""
        name = self._users.get(user_id)
        if name is not None:
            return name
        return self._fetch_user(user_id)
