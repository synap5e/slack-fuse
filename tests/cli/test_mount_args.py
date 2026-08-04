"""CLI argument wiring for `slack-fuse mount`.

The mount resolves its mountpoint as ``args.mountpoint or config.mountpoint``.
That only lets ``ClientConfig.mountpoint`` win if argparse does NOT supply a
default for the positional — previously the legacy `_default_mountpoint()`
default always won, making the configured mountpoint dead. These tests pin
that the positional now defaults to ``None`` so the config value can take effect.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import TYPE_CHECKING, NoReturn, cast
from zoneinfo import ZoneInfo

import psycopg
import pytest
import trio

import slack_fuse.__main__ as main_module
import slack_fuse.config as config_module
import slack_fuse.fuse_ops_v2 as fuse_ops_module
import slack_fuse.migrations.runner as migrations_runner
import slack_fuse.pg_health as pg_health_module
import slack_fuse.projector.disk_projection as disk_projection_module
import slack_fuse.projector.pool as pool_module
import slack_fuse.projector.reconnecting_conn as reconnecting_module
from slack_fuse.__main__ import build_parser, cmd_mount
from slack_fuse.config import load_client_config

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow


class _MountConstructed(Exception):
    pass


class _MigrationConnection:
    autocommit = False

    def close(self) -> None:
        return None


def _fake_object(*_args: object, **_kwargs: object) -> object:
    return object()


def _no_migrations(_conn: object, _directory: Path) -> list[str]:
    return []


def _ignore_subprocess(*_args: object, **_kwargs: object) -> None:
    return None


def _isolate_client_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in tuple(os.environ):
        if key.startswith("SLACK_FUSE_"):
            monkeypatch.delenv(key, raising=False)


def test_mount_mountpoint_defaults_to_none() -> None:
    parser = build_parser()
    args = parser.parse_args(["mount"])
    # None (not the legacy default) so cmd_mount's `args.mountpoint or
    # config.mountpoint` can fall back to the configured mountpoint.
    assert args.mountpoint is None


def test_mount_explicit_mountpoint_is_preserved() -> None:
    parser = build_parser()
    args = parser.parse_args(["mount", "/custom/mnt"])
    assert args.mountpoint == "/custom/mnt"


def test_mount_wires_loaded_disk_projection_flag_into_real_ops(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The typed TOML flag must reach the live reader gate in cmd_mount."""
    _isolate_client_env(monkeypatch)
    mountpoint = tmp_path / "mount"
    config_path = tmp_path / "client.toml"
    config_path.write_text(
        'shared_secret = "test-secret"\n'
        'database_url = "postgresql:///unused"\n'
        f'mountpoint = "{mountpoint}"\n'
        "disk_projection_enabled = true\n"
    )

    def load_test_config() -> config_module.ClientConfig:
        return load_client_config(config_path)

    def connect_fake(_dsn: str) -> Connection[TupleRow]:
        return cast("Connection[TupleRow]", _MigrationConnection())

    real_ops = fuse_ops_module.SlackFuseOpsV2
    mounted_ops: list[fuse_ops_module.SlackFuseOpsV2] = []

    def capture_mount_ops(
        conn: object,
        local_tz: object,
        limiter: object,
        **kwargs: object,
    ) -> NoReturn:
        enabled = kwargs.get("disk_projection_enabled")
        assert isinstance(enabled, bool)
        mounted_ops.append(
            real_ops(
                cast("Connection[TupleRow]", conn),
                cast(ZoneInfo, local_tz),
                cast(trio.CapacityLimiter, limiter),
                disk_projection_enabled=enabled,
            )
        )
        raise _MountConstructed

    monkeypatch.setattr(config_module, "load_client_config", load_test_config)
    monkeypatch.setattr(psycopg, "connect", connect_fake)
    monkeypatch.setattr(migrations_runner, "apply_migrations", _no_migrations)
    monkeypatch.setattr(reconnecting_module, "ReconnectingConnection", _fake_object)
    monkeypatch.setattr(pool_module, "ConnectionPool", _fake_object)
    monkeypatch.setattr(disk_projection_module, "DiskProjection", _fake_object)
    monkeypatch.setattr(pg_health_module, "PgHealth", _fake_object)
    monkeypatch.setattr(main_module.httpx, "Client", _fake_object)
    monkeypatch.setattr(main_module.subprocess, "run", _ignore_subprocess)
    monkeypatch.setattr(fuse_ops_module, "SlackFuseOpsV2", capture_mount_ops)

    with pytest.raises(_MountConstructed):
        cmd_mount(argparse.Namespace(mountpoint=None, debug=False))

    assert len(mounted_ops) == 1
    assert mounted_ops[0]._disk_projection_enabled  # pyright: ignore[reportPrivateUsage]
