"""Self-tests for the in-memory FUSE harness helpers."""

from __future__ import annotations

import pyfuse3
import pytest

from tests._fuse_harness import (
    capture_getattr,
    capture_lookup,
    capture_read,
    tier_aware_channels_factory,
)


class _DummyOps:
    def __init__(self) -> None:
        self.lookup_calls: list[tuple[int, bytes]] = []
        self.getattr_calls: list[int] = []
        self.read_calls: list[tuple[int, int, int]] = []

    async def lookup(self, parent_inode: int, name: bytes, _ctx: pyfuse3.RequestContext) -> pyfuse3.EntryAttributes:
        self.lookup_calls.append((parent_inode, name))
        return pyfuse3.EntryAttributes()

    async def getattr(self, inode: int, _ctx: pyfuse3.RequestContext) -> pyfuse3.EntryAttributes:
        self.getattr_calls.append(inode)
        return pyfuse3.EntryAttributes()

    async def read(self, fh: int, off: int, size: int) -> bytes:
        self.read_calls.append((fh, off, size))
        payload = b"synthetic-bytes"
        return payload[off : off + size]


@pytest.mark.trio
async def test_capture_helpers_invoke_ops() -> None:
    ops = _DummyOps()
    looked_up = await capture_lookup(ops.lookup, 11, "feed.md")
    got_attr = await capture_getattr(ops.getattr, 22)
    read = await capture_read(ops.read, 33, off=2, size=5)

    assert isinstance(looked_up, pyfuse3.EntryAttributes)
    assert isinstance(got_attr, pyfuse3.EntryAttributes)
    assert read == b"nthet"
    assert ops.lookup_calls == [(11, b"feed.md")]
    assert ops.getattr_calls == [22]
    assert ops.read_calls == [(33, 2, 5)]


def test_tier_aware_channels_factory_is_mixed() -> None:
    rows = tier_aware_channels_factory()
    tiers = {row["tier"] for row in rows}
    assert tiers == {"hot", "warm", "cold", "blocked"}
    assert any(row["tier"] == "blocked" and row["is_archived"] for row in rows)
