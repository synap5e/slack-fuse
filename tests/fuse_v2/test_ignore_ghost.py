"""Tests for the always-present ``/.ignore`` virtual file."""

from __future__ import annotations

import errno
import os
from typing import TYPE_CHECKING, cast
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pyfuse3
import pytest
import trio

from slack_fuse.fuse_ops_v2 import (
    _IMMUTABLE_FILE_TIMEOUT_S,  # pyright: ignore[reportPrivateUsage]
    IGNORE_FILE_CONTENT,
    IGNORE_FILE_INODE,
    IGNORE_FILE_NAME,
    SlackFuseOpsV2,
)
from slack_fuse.pg_health import PgHealth
from tests._fuse_harness import capture_readdir

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow


@pytest.mark.trio
async def test_ignore_appears_in_root_readdir(ops: SlackFuseOpsV2) -> None:
    ctx = MagicMock(spec=pyfuse3.RequestContext)
    fh = await ops.opendir(pyfuse3.ROOT_INODE, ctx)
    token = cast("pyfuse3.ReaddirToken", None)

    with capture_readdir() as entries:
        await ops.readdir(fh, 0, token)
    await ops.releasedir(fh)

    assert IGNORE_FILE_NAME.encode() in {name for name, _attr, _next_id in entries}


@pytest.mark.trio
async def test_ignore_content_matches_bytes_constant(ops: SlackFuseOpsV2) -> None:
    ctx = MagicMock(spec=pyfuse3.RequestContext)
    fi = await ops.open(IGNORE_FILE_INODE, os.O_RDONLY, ctx)

    content = await ops.read(fi.fh, 0, len(IGNORE_FILE_CONTENT))
    await ops.release(fi.fh)

    assert content == IGNORE_FILE_CONTENT


@pytest.mark.trio
async def test_ignore_getattr_is_immutable_timeout(ops: SlackFuseOpsV2) -> None:
    attr = await ops.getattr(IGNORE_FILE_INODE, MagicMock(spec=pyfuse3.RequestContext))

    assert attr.st_ino == IGNORE_FILE_INODE
    assert attr.st_size == len(IGNORE_FILE_CONTENT)
    assert attr.attr_timeout == _IMMUTABLE_FILE_TIMEOUT_S
    assert attr.entry_timeout == _IMMUTABLE_FILE_TIMEOUT_S


@pytest.mark.trio
async def test_ignore_write_open_returns_erofs(ops: SlackFuseOpsV2) -> None:
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        _ = await ops.open(IGNORE_FILE_INODE, os.O_WRONLY, MagicMock(spec=pyfuse3.RequestContext))

    assert exc_info.value.errno == errno.EROFS


@pytest.mark.trio
async def test_ignore_appears_regardless_of_pg_health(client_conn: Connection[TupleRow]) -> None:
    pg_health = PgHealth(MagicMock())
    ops = SlackFuseOpsV2(
        client_conn,
        ZoneInfo("UTC"),
        trio.CapacityLimiter(1),
        pg_health=pg_health,
    )

    for pg_is_down in (False, True):
        if pg_is_down:
            pg_health.mark_down(reason="test")
        attr = await ops.lookup(
            pyfuse3.ROOT_INODE,
            IGNORE_FILE_NAME.encode(),
            MagicMock(spec=pyfuse3.RequestContext),
        )
        assert attr.st_ino == IGNORE_FILE_INODE
