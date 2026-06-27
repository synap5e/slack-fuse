"""``slack-fuse rerender`` — re-render a channel's chunks with current code.

Escape-hatch CLI for the same operation as ``_control/rerender_channel``:
re-derive a channel's ``chunks`` / ``thread_chunks`` from the server's latest
snapshot using the renderer compiled into this build. Run after a renderer
change ships to refresh historical chunks rendered by the old code.

Unlike the in-mount control surface, this is a standalone process: it cannot
reach the running mount's kernel page cache, so it passes a
``NullInvalidationSink``. The DB rows are refreshed immediately; a ``cat`` of an
affected file in the mount may keep serving the kernel-cached bytes until that
inode's cache is dropped (next polling-TTL refresh, a live event on the file, or
a remount). Prefer ``_control/rerender_channel`` when the mount is up and you
want the change visible immediately.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any, Protocol

import httpx
import psycopg
from psycopg.rows import TupleRow

from slack_fuse.cli.tier import TierCommandError, _resolve_channel_id  # pyright: ignore[reportPrivateUsage]


class _SubparserRegistry(Protocol):
    def add_parser(self, name: str, **kwargs: Any) -> argparse.ArgumentParser: ...


def register_rerender_subcommand(subparsers: _SubparserRegistry) -> None:
    """Register ``slack-fuse rerender ...`` on the top-level CLI parser."""
    parser = subparsers.add_parser(
        "rerender",
        help="Re-render a channel's chunks with the current renderer",
        description=(
            "Re-derive a channel's chunks/thread_chunks from the server's latest "
            "snapshot using the current renderer code. Use after a renderer change "
            "ships to refresh historical chunks. Accepts a channel slug "
            "(e.g. 'general' or 'channels/general') or a channel id (e.g. 'C123')."
        ),
    )
    parser.add_argument(
        "slug_or_channel_id",
        help="Channel slug (e.g. 'general' or 'channels/general'), or channel ID (e.g. 'C123')",
    )
    parser.set_defaults(func=cmd_rerender)


def cmd_rerender(args: argparse.Namespace) -> None:
    """Entry point used by ``slack_fuse.__main__``."""
    raw_target = getattr(args, "slug_or_channel_id", None)
    if not isinstance(raw_target, str):
        msg = "rerender command arguments are invalid"
        raise ValueError(msg)

    from slack_fuse.config import load_client_config
    from slack_fuse.projector.rerender import rerender_channel
    from slack_fuse.projector.ws_client import derive_http_base

    config = load_client_config()
    base_http_url = derive_http_base(config.server_url)

    conn: psycopg.Connection[TupleRow] = psycopg.connect(config.database_url)
    conn.autocommit = True
    try:
        channel_id = _resolve_channel_id(conn, raw_target)
        if channel_id is None:
            print(f"Error: unknown channel slug or id: {raw_target}", file=sys.stderr)
            sys.exit(2)

        with httpx.Client(timeout=httpx.Timeout(connect=2.0, read=30.0, write=2.0, pool=5.0)) as http_client:
            result = rerender_channel(
                http_client,
                base_http_url,
                conn,
                channel_id,
                shared_secret=config.shared_secret,
            )
    except TierCommandError as exc:
        # Raised by the shared resolver on an ambiguous bare slug.
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(exc.exit_code)
    except (psycopg.Error, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()

    if result.status == "rerendered":
        print(
            f"{result.channel_id}: re-rendered {result.chunks} chunk(s), "
            f"{result.thread_chunks} thread-chunk(s)"
        )
        return
    if result.status == "no_snapshot":
        print(f"{result.channel_id}: no server snapshot yet; nothing to re-render")
        return
    print(f"Error: {result.channel_id}: rerender failed ({result.status})", file=sys.stderr)
    sys.exit(1)
