# pyright: reportPrivateUsage=false
"""Tests for auth.py config-loading helpers."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from slack_fuse import auth


def test_load_mountpoint_env_var_wins(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SLACK_FUSE_MOUNTPOINT", "/from/env")
    monkeypatch.setattr(auth, "_ENV_PATH", tmp_path / "nope.env")
    monkeypatch.setattr(auth, "_CONFIG_PATH", tmp_path / "nope.json")
    assert auth.load_mountpoint() == "/from/env"


def test_load_mountpoint_falls_back_to_env_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("SLACK_FUSE_MOUNTPOINT", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("SLACK_FUSE_MOUNTPOINT=/from/env-file\n# comment\n")
    monkeypatch.setattr(auth, "_ENV_PATH", env_file)
    monkeypatch.setattr(auth, "_CONFIG_PATH", tmp_path / "nope.json")
    assert auth.load_mountpoint() == "/from/env-file"


def test_load_mountpoint_falls_back_to_config_json(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("SLACK_FUSE_MOUNTPOINT", raising=False)
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"mountpoint": "/from/config"}))
    monkeypatch.setattr(auth, "_ENV_PATH", tmp_path / "nope.env")
    monkeypatch.setattr(auth, "_CONFIG_PATH", config)
    assert auth.load_mountpoint() == "/from/config"


def test_load_mountpoint_returns_none_when_unset(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("SLACK_FUSE_MOUNTPOINT", raising=False)
    monkeypatch.setattr(auth, "_ENV_PATH", tmp_path / "nope.env")
    monkeypatch.setattr(auth, "_CONFIG_PATH", tmp_path / "nope.json")
    assert auth.load_mountpoint() is None


def test_load_mountpoint_expands_tilde(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SLACK_FUSE_MOUNTPOINT", "~/views/slack")
    monkeypatch.setattr(auth, "_ENV_PATH", tmp_path / "nope.env")
    monkeypatch.setattr(auth, "_CONFIG_PATH", tmp_path / "nope.json")
    result = auth.load_mountpoint()
    assert result is not None
    assert "~" not in result
    assert result.endswith("/views/slack")
