"""CLI entry point for slack-fuse."""

from __future__ import annotations

import argparse
import logging
import os
import signal
import subprocess
import sys
from pathlib import Path


def _default_mountpoint() -> str:
    return os.path.expanduser("~/views/slack")


_REFRESH_INTERVAL = 1800  # 30 minutes, matches _CHANNEL_LIST_TTL


def _env_bool(name: str, default: bool) -> bool:
    """Parse a boolean env var. Accepts 1/0, true/false, yes/no, on/off."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    val = raw.strip().lower()
    if val in ("1", "true", "yes", "on"):
        return True
    if val in ("0", "false", "no", "off", ""):
        return False
    msg = f"{name} must be a boolean (got {raw!r})"
    raise ValueError(msg)


def cmd_mount(args: argparse.Namespace) -> None:
    """Mount the FUSE filesystem."""
    import pyfuse3
    import trio

    from .api import SlackClient
    from .archive import archive_all
    from .auth import load_tokens
    from .backfill import backfill_all
    from .fuse_ops import SlackFuseOps
    from .store import SlackStore
    from .user_cache import UserCache

    mountpoint = Path(args.mountpoint)
    mountpoint.mkdir(parents=True, exist_ok=True)

    # Clean stale mount if present (e.g. after a crash)
    import subprocess as _sp

    _sp.run(["fusermount3", "-uz", str(mountpoint)], capture_output=True)

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # Quiet httpx request logging — our store already logs what matters
    logging.getLogger("httpx").setLevel(
        logging.DEBUG if args.debug else logging.WARNING,
    )

    tokens = load_tokens()
    client = SlackClient(tokens.user_token)
    users = UserCache(tokens.user_token)
    users.populate()

    store = SlackStore(client, users)

    # Preload channel list so first ls is instant
    store.list_channels(kind="channels")

    ops = SlackFuseOps(store)

    fuse_options: set[str] = {"fsname=slack-fuse", "ro"}
    if args.debug:
        fuse_options.add("debug")

    def _handle_usr1(signum: int, frame: object) -> None:
        store.force_refresh()

    signal.signal(signal.SIGUSR1, _handle_usr1)

    pyfuse3.init(ops, str(mountpoint), fuse_options)

    backfill_enabled = _env_bool("SLACK_FUSE_BACKFILL", default=False)
    logging.getLogger(__name__).info(
        "Backfill: %s",
        "enabled" if backfill_enabled else "disabled",
    )

    async def _periodic_refresh() -> None:
        while True:
            await trio.sleep(_REFRESH_INTERVAL)
            store.list_channels(kind="channels")

    async def _run() -> None:
        async with trio.open_nursery() as nursery:
            nursery.start_soon(pyfuse3.main)
            nursery.start_soon(_periodic_refresh)
            nursery.start_soon(archive_all, store)
            if backfill_enabled:
                nursery.start_soon(backfill_all, client, store)

    try:
        trio.run(_run)
    except KeyboardInterrupt:
        pass
    finally:
        pyfuse3.close()


def cmd_unmount(args: argparse.Namespace) -> None:
    """Unmount the FUSE filesystem."""
    result = subprocess.run(
        ["fusermount3", "-u", args.mountpoint],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"Failed to unmount: {result.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    print(f"Unmounted {args.mountpoint}")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="slack-fuse",
        description="FUSE filesystem for Slack",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    mount_parser = sub.add_parser("mount", help="Mount the filesystem")
    mount_parser.add_argument(
        "mountpoint",
        nargs="?",
        default=_default_mountpoint(),
        help=f"Mount point (default: {_default_mountpoint()})",
    )
    mount_parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    mount_parser.set_defaults(func=cmd_mount)

    unmount_parser = sub.add_parser("unmount", help="Unmount the filesystem")
    unmount_parser.add_argument(
        "mountpoint",
        nargs="?",
        default=_default_mountpoint(),
        help=f"Mount point (default: {_default_mountpoint()})",
    )
    unmount_parser.set_defaults(func=cmd_unmount)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
