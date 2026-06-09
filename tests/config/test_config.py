"""Config loaders: TOML population, env precedence, required fields."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from pydantic import ValidationError

from slack_fuse.config import load_client_config
from slack_fuse_server.config import load_server_config

_SERVER_TOML = """\
slack_user_token = "xoxp-user"
slack_app_token = "xapp-app"
slack_bot_token = "xoxb-bot"
shared_secret = "topsecret"
database_url = "postgresql:///custom_server_db"
listen_addr = "0.0.0.0:9999"
snapshot_every_n_events = 1234
backfill_abort_at = 99999
backfill_page_sleep_min_s = 12.5
"""

_CLIENT_TOML = """\
shared_secret = "topsecret"
server_url = "ws://homelab:8765"
database_url = "postgresql:///custom_client_db"
mountpoint = "/mnt/slack"
stale_trailer_enabled = false
"""


@pytest.fixture(autouse=True)
def isolate_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Drop host SLACK_FUSE[_SERVER]_ env vars so they don't leak into loads."""
    for key in list(os.environ):
        if key.startswith("SLACK_FUSE_"):
            monkeypatch.delenv(key, raising=False)


def _write(tmp_path: Path, name: str, body: str) -> Path:
    path = tmp_path / name
    path.write_text(body)
    return path


def test_server_config_from_toml(tmp_path: Path) -> None:
    cfg = load_server_config(_write(tmp_path, "server.toml", _SERVER_TOML))
    assert cfg.slack_user_token == "xoxp-user"
    assert cfg.slack_app_token == "xapp-app"
    assert cfg.shared_secret == "topsecret"
    assert cfg.database_url == "postgresql:///custom_server_db"
    assert cfg.listen_addr == "0.0.0.0:9999"
    assert cfg.snapshot_every_n_events == 1234
    assert cfg.backfill_abort_at == 99999
    assert abs(cfg.backfill_page_sleep_min_s - 12.5) < 1e-9
    # Untouched keys fall back to RFC defaults.
    assert cfg.backfill_warn_at == 5000
    assert cfg.snapshot_max_age_hours == 24


def test_client_config_from_toml(tmp_path: Path) -> None:
    cfg = load_client_config(_write(tmp_path, "client.toml", _CLIENT_TOML))
    assert cfg.shared_secret == "topsecret"
    assert cfg.server_url == "ws://homelab:8765"
    assert cfg.database_url == "postgresql:///custom_client_db"
    assert cfg.mountpoint == "/mnt/slack"
    assert cfg.stale_trailer_enabled is False
    # Default retained.
    assert abs(cfg.stale_after_disconnect_s - 60.0) < 1e-9


def test_env_overrides_toml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SLACK_FUSE_SERVER_DATABASE_URL", "postgresql:///from_env")
    cfg = load_server_config(_write(tmp_path, "server.toml", _SERVER_TOML))
    assert cfg.database_url == "postgresql:///from_env"


def test_client_env_overrides_toml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SLACK_FUSE_MOUNTPOINT", "/env/mount")
    cfg = load_client_config(_write(tmp_path, "client.toml", _CLIENT_TOML))
    assert cfg.mountpoint == "/env/mount"


def test_missing_required_field_raises(tmp_path: Path) -> None:
    incomplete = _write(tmp_path, "server.toml", 'slack_user_token = "x"\n')  # no app token / secret
    with pytest.raises(ValidationError):
        load_server_config(incomplete)
