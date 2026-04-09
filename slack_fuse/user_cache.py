"""User ID → display name resolution with persistent disk cache."""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

import httpx

log = logging.getLogger(__name__)

_CACHE_PATH = Path.home() / ".cache" / "slack-fuse" / "users.json"
_TTL = 86400.0  # 24 hours


class UserCache:
    """Resolves Slack user IDs to display names.

    Persists to disk so the cache survives restarts. Unknown IDs
    trigger an immediate API fetch.
    """

    def __init__(self, token: str) -> None:
        self._token = token
        self._users: dict[str, str] = {}  # user_id -> display_name
        self._loaded_at: float = 0.0
        self._load_from_disk()

    def _load_from_disk(self) -> None:
        if _CACHE_PATH.exists():
            try:
                data = json.loads(_CACHE_PATH.read_text())
                self._users = data.get("users", {})
                self._loaded_at = data.get("loaded_at", 0.0)
                log.info("Loaded %d users from disk cache", len(self._users))
            except (json.JSONDecodeError, KeyError):
                log.warning("Corrupt user cache, starting fresh")

    def _save_to_disk(self) -> None:
        _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        _CACHE_PATH.write_text(
            json.dumps({"users": self._users, "loaded_at": self._loaded_at})
        )

    def _api_get(self, path: str, params: dict[str, str] | None = None) -> Any:
        resp = httpx.get(
            f"https://slack.com/api{path}",
            params=params,
            headers={"Authorization": f"Bearer {self._token}"},
            timeout=15.0,
        )
        resp.raise_for_status()
        return resp.json()

    def _fetch_user(self, user_id: str) -> str | None:
        """Fetch a single user's or bot's display name from the API."""
        try:
            if user_id.startswith("B"):
                # Bot ID — use bots.info
                data = self._api_get("/bots.info", {"bot": user_id})
                if not data.get("ok"):
                    # Cache the miss so we don't retry
                    self._users[user_id] = user_id
                    self._save_to_disk()
                    return user_id
                bot = data.get("bot", {})
                name = bot.get("name", user_id)
            else:
                data = self._api_get("/users.info", {"user": user_id})
                if not data.get("ok"):
                    self._users[user_id] = user_id
                    self._save_to_disk()
                    return user_id
                user = data["user"]
                profile = user.get("profile", {})
                name = (
                    profile.get("display_name")
                    or profile.get("real_name")
                    or user.get("name", user_id)
                )
            self._users[user_id] = name
            self._save_to_disk()
            return name
        except Exception:
            log.warning("Failed to fetch user/bot %s", user_id, exc_info=True)
            # Cache the ID itself so we don't retry
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
                data = self._api_get("/users.list", params)
                if not data.get("ok"):
                    log.warning("users.list failed: %s", data.get("error"))
                    break
                for member in data.get("members", []):
                    uid = member["id"]
                    profile = member.get("profile", {})
                    name = (
                        profile.get("display_name")
                        or profile.get("real_name")
                        or member.get("name", uid)
                    )
                    self._users[uid] = name
                    count += 1
                cursor = data.get("response_metadata", {}).get("next_cursor", "")
                if not cursor:
                    break
        except Exception:
            log.warning("Error populating user cache", exc_info=True)

        self._loaded_at = time.time()
        self._save_to_disk()
        log.info("User cache populated with %d users", count)

    def get_display_name(self, user_id: str) -> str:
        """Resolve user ID to display name. Fetches from API if unknown."""
        name = self._users.get(user_id)
        if name is not None:
            return name
        fetched = self._fetch_user(user_id)
        return fetched or user_id
