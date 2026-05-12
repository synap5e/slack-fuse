"""Token management for Slack API access."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

_CONFIG_PATH = Path.home() / ".config" / "slack-fuse" / "config.json"
_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"


@dataclass(frozen=True)
class SlackTokens:
    user_token: str
    app_token: str | None = None
    workspace_url: str | None = None


def _parse_env_file(path: Path) -> dict[str, str]:
    """Parse a KEY=value .env file, ignoring comments and blank lines."""
    result: dict[str, str] = {}
    if not path.exists():
        return result
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, _, value = line.partition("=")
        if key:
            result[key.strip()] = value.strip()
    return result


def load_mountpoint() -> str | None:
    """Resolve the FUSE mountpoint from env, .env, or ~/.config/slack-fuse/config.json.

    Returns the first non-empty value found, with `~` expanded. Returns None if
    none of the sources have it set, leaving the caller free to apply its own
    default. Mirrors `load_tokens()`'s precedence so a `.env` next to the
    install picks up overrides when subshells don't inherit the user systemd
    environment (e.g. `environment.d` only reaches PAM-launched shells).
    """
    raw = os.environ.get("SLACK_FUSE_MOUNTPOINT")
    if not raw:
        raw = _parse_env_file(_ENV_PATH).get("SLACK_FUSE_MOUNTPOINT")
    if not raw and _CONFIG_PATH.exists():
        config = json.loads(_CONFIG_PATH.read_text())
        raw = config.get("mountpoint") or config.get("SLACK_FUSE_MOUNTPOINT")
    if not raw:
        return None
    return os.path.expanduser(raw)


def load_tokens() -> SlackTokens:
    """Load Slack tokens from environment variables, falling back to .env or config file."""
    user_token = os.environ.get("SLACK_USER_TOKEN")
    app_token = os.environ.get("SLACK_APP_TOKEN")
    workspace_url = os.environ.get("SLACK_WORKSPACE_URL")

    if not user_token or not app_token or not workspace_url:
        env_vars = _parse_env_file(_ENV_PATH)
        user_token = user_token or env_vars.get("SLACK_USER_TOKEN")
        app_token = app_token or env_vars.get("SLACK_APP_TOKEN")
        workspace_url = workspace_url or env_vars.get("SLACK_WORKSPACE_URL")

    if (not user_token or not app_token or not workspace_url) and _CONFIG_PATH.exists():
        config = json.loads(_CONFIG_PATH.read_text())
        user_token = user_token or config.get("user_token")
        app_token = app_token or config.get("app_token")
        workspace_url = workspace_url or config.get("workspace_url") or config.get("SLACK_WORKSPACE_URL")

    if not user_token:
        msg = "SLACK_USER_TOKEN not set. Set it in the environment or in ~/.config/slack-fuse/config.json"
        raise RuntimeError(msg)

    return SlackTokens(
        user_token=user_token,
        app_token=app_token if app_token and not app_token.startswith("xapp-your") else None,
        workspace_url=workspace_url.rstrip("/") if workspace_url else None,
    )
