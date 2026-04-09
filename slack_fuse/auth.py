"""Token management for Slack API access."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

_CONFIG_PATH = Path.home() / ".config" / "slack-fuse" / "config.json"


@dataclass(frozen=True)
class SlackTokens:
    user_token: str
    app_token: str | None = None


def load_tokens() -> SlackTokens:
    """Load Slack tokens from environment variables, falling back to config file."""
    user_token = os.environ.get("SLACK_USER_TOKEN")
    app_token = os.environ.get("SLACK_APP_TOKEN")

    if not user_token and _CONFIG_PATH.exists():
        config = json.loads(_CONFIG_PATH.read_text())
        user_token = config.get("user_token")
        app_token = app_token or config.get("app_token")

    if not user_token:
        msg = (
            "SLACK_USER_TOKEN not set. "
            "Set it in the environment or in ~/.config/slack-fuse/config.json"
        )
        raise RuntimeError(msg)

    return SlackTokens(
        user_token=user_token,
        app_token=app_token if app_token and not app_token.startswith("xapp-your") else None,
    )
