"""CLI argument wiring for `slack-fuse mount`.

The mount resolves its mountpoint as ``args.mountpoint or config.mountpoint``.
That only lets ``ClientConfig.mountpoint`` win if argparse does NOT supply a
default for the positional — previously the legacy `_default_mountpoint()`
default always won, making the configured mountpoint dead. These tests pin
that the positional now defaults to ``None`` so the config value can take effect.
"""

from __future__ import annotations

from slack_fuse.__main__ import build_parser


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
