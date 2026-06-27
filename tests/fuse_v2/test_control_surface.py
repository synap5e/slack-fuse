"""Tests for the ``_control/`` write surface (Plan-9 ctl/status).

Covers: directory/file listing + attrs, status JSON rendering, the
write→release trigger flow (workspace + per-channel, slug resolution), the
read-only enforcement that replaces the dropped ``ro`` mount option, and the
HTTP POST helpers.
"""

from __future__ import annotations

import errno
import json
import os
import stat
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import httpx
import pyfuse3
import pytest
import trio

from slack_fuse.control import ControlState, result_for_status
from slack_fuse.fuse_ops_v2 import SlackFuseOpsV2
from slack_fuse.projector.refresh_fetch import post_refresh_channel, post_refresh_channels
from tests.fuse_v2.conftest import seed_channel

if TYPE_CHECKING:
    from collections.abc import Callable

    from psycopg import Connection
    from psycopg.rows import TupleRow


# ============================================================================
# Construction helper
# ============================================================================


def _make_control_ops(
    conn: Connection[TupleRow],
    tz: ZoneInfo,
    *,
    workspace_fn: Callable[[], int] | None = None,
    channel_fn: Callable[[str], int] | None = None,
    state: ControlState | None = None,
) -> tuple[SlackFuseOpsV2, ControlState]:
    control_state = state if state is not None else ControlState()
    ops = SlackFuseOpsV2(
        conn=conn,
        local_tz=tz,
        limiter=trio.CapacityLimiter(1),
        control_state=control_state,
        control_refresh_workspace=workspace_fn,
        control_refresh_channel=channel_fn,
    )
    return ops, control_state


# ============================================================================
# ControlState + status mapping (pure)
# ============================================================================


def test_result_for_status_mapping() -> None:
    assert result_for_status(202) == "queued"
    assert result_for_status(409) == "busy"
    assert result_for_status(401) == "unauthorised"
    assert result_for_status(403) == "unauthorised"
    assert result_for_status(503) == "server_unavailable"
    assert result_for_status(0) == "server_unavailable"
    assert result_for_status(500) == "http_500"


def test_control_state_render_empty_and_recorded() -> None:
    fixed = datetime(2026, 6, 27, 11, 0, 0, tzinfo=UTC)
    state = ControlState(now_fn=lambda: fixed)

    empty = json.loads(state.render())
    assert empty == {"last_workspace_refresh": None, "last_channel_refresh": None}

    state.record_workspace("queued")
    state.record_channel("C0ALLT6Q3SQ", "busy")
    payload = json.loads(state.render())
    assert payload["last_workspace_refresh"] == {"at": "2026-06-27T11:00:00Z", "result": "queued"}
    assert payload["last_channel_refresh"] == {
        "at": "2026-06-27T11:00:00Z",
        "result": "busy",
        "channel": "C0ALLT6Q3SQ",
    }


# ============================================================================
# Listing + attrs
# ============================================================================


def test_control_dir_listed_when_enabled(client_conn: Connection[TupleRow], utc_tz: ZoneInfo) -> None:
    ops, _ = _make_control_ops(client_conn, utc_tz)
    assert ("_control", True) in ops.list_dir_for_test("/")
    contents = dict(ops.list_dir_for_test("/_control"))
    assert set(contents) == {"refresh_channels", "refresh_channel", "status"}
    assert all(is_dir is False for is_dir in contents.values())


def test_control_dir_absent_when_disabled(client_conn: Connection[TupleRow], utc_tz: ZoneInfo) -> None:
    ops = SlackFuseOpsV2(conn=client_conn, local_tz=utc_tz, limiter=trio.CapacityLimiter(1))
    assert ("_control", True) not in ops.list_dir_for_test("/")
    assert ops.list_dir_for_test("/_control") == []
    assert not ops.is_dir_for_test("/_control")


def test_control_is_dir_classification(client_conn: Connection[TupleRow], utc_tz: ZoneInfo) -> None:
    ops, _ = _make_control_ops(client_conn, utc_tz)
    assert ops.is_dir_for_test("/_control")
    assert not ops.is_dir_for_test("/_control/status")
    assert not ops.is_dir_for_test("/_control/refresh_channels")


def test_control_file_modes(client_conn: Connection[TupleRow], utc_tz: ZoneInfo) -> None:
    ops, _ = _make_control_ops(client_conn, utc_tz)
    status_attr = ops.control_file_attr_for_test("/_control/status", 10)
    assert status_attr is not None
    assert stat.S_IMODE(status_attr.st_mode) == 0o444

    for name in ("refresh_channels", "refresh_channel"):
        attr = ops.control_file_attr_for_test(f"/_control/{name}", 11)
        assert attr is not None
        assert stat.S_IMODE(attr.st_mode) == 0o644
        assert attr.st_size == 0


# ============================================================================
# Reads
# ============================================================================


def test_control_read_bytes(client_conn: Connection[TupleRow], utc_tz: ZoneInfo) -> None:
    ops, _ = _make_control_ops(client_conn, utc_tz)
    # ctl trigger files read back empty (Plan-9 style).
    assert ops.control_read_for_test("/_control/refresh_channels") == b""
    assert ops.control_read_for_test("/_control/refresh_channel") == b""
    # status is the JSON body.
    status = json.loads(ops.control_read_for_test("/_control/status") or b"{}")
    assert status == {"last_workspace_refresh": None, "last_channel_refresh": None}
    # not a control file.
    assert ops.control_read_for_test("/channels/general/channel.md") is None


@pytest.mark.trio
async def test_cat_status_via_read_callback(client_conn: Connection[TupleRow], utc_tz: ZoneInfo) -> None:
    ops, _ = _make_control_ops(client_conn, utc_tz)
    inode = ops.inodes.get_or_create("/_control/status")
    fi = await ops.open(inode, os.O_RDONLY, pyfuse3.RequestContext())
    data = await ops.read(fi.fh, 0, 65536)
    assert json.loads(data) == {"last_workspace_refresh": None, "last_channel_refresh": None}


@pytest.mark.trio
async def test_cat_refresh_trigger_returns_empty(client_conn: Connection[TupleRow], utc_tz: ZoneInfo) -> None:
    ops, _ = _make_control_ops(client_conn, utc_tz)
    inode = ops.inodes.get_or_create("/_control/refresh_channels")
    fi = await ops.open(inode, os.O_RDONLY, pyfuse3.RequestContext())
    assert await ops.read(fi.fh, 0, 65536) == b""


# ============================================================================
# Write → release trigger flow
# ============================================================================


@pytest.mark.trio
async def test_write_refresh_channels_fires_workspace(
    client_conn: Connection[TupleRow], utc_tz: ZoneInfo
) -> None:
    calls: list[str] = []

    def workspace_fn() -> int:
        calls.append("ws")
        return 202

    ops, _ = _make_control_ops(client_conn, utc_tz, workspace_fn=workspace_fn)
    inode = ops.inodes.get_or_create("/_control/refresh_channels")
    fi = await ops.open(inode, os.O_WRONLY, pyfuse3.RequestContext())
    assert await ops.write(fi.fh, 0, b"1\n") == 2
    await ops.release(fi.fh)

    assert calls == ["ws"]
    status = json.loads(ops.control_read_for_test("/_control/status") or b"{}")
    assert status["last_workspace_refresh"]["result"] == "queued"
    # per-fh buffer cleaned up on release (no leak).
    assert ops.control_write_buffer_count() == 0


@pytest.mark.trio
async def test_empty_write_does_not_fire(client_conn: Connection[TupleRow], utc_tz: ZoneInfo) -> None:
    calls: list[str] = []

    def workspace_fn() -> int:
        calls.append("ws")
        return 202

    ops, _ = _make_control_ops(client_conn, utc_tz, workspace_fn=workspace_fn)
    inode = ops.inodes.get_or_create("/_control/refresh_channels")
    fi = await ops.open(inode, os.O_WRONLY, pyfuse3.RequestContext())
    # An ``echo -n > file`` truncate with no bytes is a no-op.
    await ops.release(fi.fh)
    assert calls == []
    status = json.loads(ops.control_read_for_test("/_control/status") or b"{}")
    assert status["last_workspace_refresh"] is None


@pytest.mark.trio
async def test_write_refresh_channel_by_id(client_conn: Connection[TupleRow], utc_tz: ZoneInfo) -> None:
    seed_channel(client_conn, "C0ALLT6Q3SQ", "proj-cloud")
    seen: list[str] = []

    def channel_fn(channel_id: str) -> int:
        seen.append(channel_id)
        return 202

    ops, _ = _make_control_ops(client_conn, utc_tz, channel_fn=channel_fn)
    inode = ops.inodes.get_or_create("/_control/refresh_channel")
    fi = await ops.open(inode, os.O_WRONLY, pyfuse3.RequestContext())
    await ops.write(fi.fh, 0, b"C0ALLT6Q3SQ\n")
    await ops.release(fi.fh)

    assert seen == ["C0ALLT6Q3SQ"]
    status = json.loads(ops.control_read_for_test("/_control/status") or b"{}")
    assert status["last_channel_refresh"] == {
        **status["last_channel_refresh"],
        "result": "queued",
        "channel": "C0ALLT6Q3SQ",
    }


@pytest.mark.trio
async def test_write_refresh_channel_by_slug(client_conn: Connection[TupleRow], utc_tz: ZoneInfo) -> None:
    seed_channel(client_conn, "C0ALLT6Q3SQ", "proj-cloud")
    seen: list[str] = []

    def channel_fn(channel_id: str) -> int:
        seen.append(channel_id)
        return 202

    ops, _ = _make_control_ops(client_conn, utc_tz, channel_fn=channel_fn)
    inode = ops.inodes.get_or_create("/_control/refresh_channel")
    fi = await ops.open(inode, os.O_WRONLY, pyfuse3.RequestContext())
    await ops.write(fi.fh, 0, b"proj-cloud\n")
    await ops.release(fi.fh)

    # Slug resolved to the channel id before the POST.
    assert seen == ["C0ALLT6Q3SQ"]
    status = json.loads(ops.control_read_for_test("/_control/status") or b"{}")
    assert status["last_channel_refresh"]["channel"] == "C0ALLT6Q3SQ"
    assert status["last_channel_refresh"]["result"] == "queued"


@pytest.mark.trio
async def test_write_refresh_channel_unknown_slug(
    client_conn: Connection[TupleRow], utc_tz: ZoneInfo
) -> None:
    seen: list[str] = []

    def channel_fn(channel_id: str) -> int:
        seen.append(channel_id)
        return 202

    ops, _ = _make_control_ops(client_conn, utc_tz, channel_fn=channel_fn)
    inode = ops.inodes.get_or_create("/_control/refresh_channel")
    fi = await ops.open(inode, os.O_WRONLY, pyfuse3.RequestContext())
    await ops.write(fi.fh, 0, b"does-not-exist\n")
    await ops.release(fi.fh)

    # No POST fired for an unresolvable token.
    assert seen == []
    status = json.loads(ops.control_read_for_test("/_control/status") or b"{}")
    assert status["last_channel_refresh"]["result"] == "unknown_channel"
    assert status["last_channel_refresh"]["channel"] == "does-not-exist"


@pytest.mark.trio
async def test_write_buffers_isolated_between_handles(
    client_conn: Connection[TupleRow], utc_tz: ZoneInfo
) -> None:
    seen: list[str] = []

    def channel_fn(channel_id: str) -> int:
        seen.append(channel_id)
        return 202

    seed_channel(client_conn, "C1", "alpha")
    seed_channel(client_conn, "C2", "beta")
    ops, _ = _make_control_ops(client_conn, utc_tz, channel_fn=channel_fn)
    inode = ops.inodes.get_or_create("/_control/refresh_channel")

    fi_a = await ops.open(inode, os.O_WRONLY, pyfuse3.RequestContext())
    fi_b = await ops.open(inode, os.O_WRONLY, pyfuse3.RequestContext())
    assert fi_a.fh != fi_b.fh
    # Interleaved writes must not cross-contaminate the per-fh buffers.
    await ops.write(fi_a.fh, 0, b"alpha")
    await ops.write(fi_b.fh, 0, b"beta")
    await ops.release(fi_a.fh)
    await ops.release(fi_b.fh)

    assert seen == ["C1", "C2"]
    assert ops.control_write_buffer_count() == 0


# ============================================================================
# Read-only enforcement (replaces the dropped ``ro`` mount option)
# ============================================================================


@pytest.mark.trio
async def test_write_open_on_normal_file_is_erofs(
    client_conn: Connection[TupleRow], utc_tz: ZoneInfo
) -> None:
    ops, _ = _make_control_ops(client_conn, utc_tz)
    inode = ops.inodes.get_or_create("/channels/general/channel.md")
    with pytest.raises(pyfuse3.FUSEError) as excinfo:
        await ops.open(inode, os.O_WRONLY, pyfuse3.RequestContext())
    assert excinfo.value.errno == errno.EROFS


@pytest.mark.trio
async def test_write_open_on_status_is_erofs(client_conn: Connection[TupleRow], utc_tz: ZoneInfo) -> None:
    ops, _ = _make_control_ops(client_conn, utc_tz)
    inode = ops.inodes.get_or_create("/_control/status")
    with pytest.raises(pyfuse3.FUSEError) as excinfo:
        await ops.open(inode, os.O_WRONLY, pyfuse3.RequestContext())
    assert excinfo.value.errno == errno.EROFS


@pytest.mark.trio
async def test_setattr_truncate_accepted_on_trigger(
    client_conn: Connection[TupleRow], utc_tz: ZoneInfo
) -> None:
    ops, _ = _make_control_ops(client_conn, utc_tz)
    inode = ops.inodes.get_or_create("/_control/refresh_channels")
    attr = pyfuse3.EntryAttributes()
    fields = pyfuse3.SetattrFields()
    result = await ops.setattr(inode, attr, fields, None, pyfuse3.RequestContext())
    assert stat.S_IMODE(result.st_mode) == 0o644


@pytest.mark.trio
async def test_setattr_on_normal_file_is_erofs(
    client_conn: Connection[TupleRow], utc_tz: ZoneInfo
) -> None:
    ops, _ = _make_control_ops(client_conn, utc_tz)
    inode = ops.inodes.get_or_create("/channels/general/channel.md")
    attr = pyfuse3.EntryAttributes()
    fields = pyfuse3.SetattrFields()
    with pytest.raises(pyfuse3.FUSEError) as excinfo:
        await ops.setattr(inode, attr, fields, None, pyfuse3.RequestContext())
    assert excinfo.value.errno == errno.EROFS


@pytest.mark.trio
async def test_oversized_write_is_efbig(client_conn: Connection[TupleRow], utc_tz: ZoneInfo) -> None:
    ops, _ = _make_control_ops(client_conn, utc_tz)
    inode = ops.inodes.get_or_create("/_control/refresh_channel")
    fi = await ops.open(inode, os.O_WRONLY, pyfuse3.RequestContext())
    with pytest.raises(pyfuse3.FUSEError) as excinfo:
        await ops.write(fi.fh, 0, b"x" * 70000)
    assert excinfo.value.errno == errno.EFBIG


# ============================================================================
# HTTP POST helpers
# ============================================================================


def test_post_refresh_channels_sends_bearer() -> None:
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(202)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    assert post_refresh_channels(client, "http://srv/", shared_secret="sek") == 202
    assert captured[0].method == "POST"
    assert captured[0].url.path == "/refresh-channels"
    assert captured[0].headers["authorization"] == "Bearer sek"


def test_post_refresh_channels_no_secret_no_header() -> None:
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(409)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    assert post_refresh_channels(client, "http://srv") == 409
    assert "authorization" not in captured[0].headers


def test_post_refresh_channel_path_and_transport_error() -> None:
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(202)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    assert post_refresh_channel(client, "http://srv", "C123") == 202
    assert captured[0].url.path == "/refresh-channels/C123"

    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("unreachable", request=request)

    bad_client = httpx.Client(transport=httpx.MockTransport(boom))
    assert post_refresh_channel(bad_client, "http://srv", "C123") == 0
