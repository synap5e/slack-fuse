"""In-memory pyfuse3 harness implementation. Re-exported from `__init__`."""

from __future__ import annotations

import contextlib
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass
from typing import TypedDict, cast

import pyfuse3

ReaddirEntry = tuple[bytes, object, int]
LookupOp = Callable[[int, bytes, pyfuse3.RequestContext], Awaitable[pyfuse3.EntryAttributes]]
GetattrOp = Callable[[int, pyfuse3.RequestContext], Awaitable[pyfuse3.EntryAttributes]]
ReadOp = Callable[[int, int, int], Awaitable[bytes]]


class ChannelTableRow(TypedDict):
    channel_id: str
    name: str
    tier: str
    tier_source: str
    is_member: bool
    is_archived: bool


@dataclass(frozen=True, slots=True)
class FakeRequestContext:
    """Minimal stand-in for `pyfuse3.RequestContext`.

    Carries the fields FUSE handlers read. Structural duck-typing is enough at
    runtime; tests pass this where a `pyfuse3.RequestContext` is annotated.
    """

    uid: int = 1000
    gid: int = 1000
    pid: int = 1234
    umask: int = 0o022


def fake_request_context() -> FakeRequestContext:
    return FakeRequestContext()


def _coerce_ctx(ctx: pyfuse3.RequestContext | None) -> pyfuse3.RequestContext:
    if ctx is not None:
        return ctx
    # Structural duck-typing is enough for pyfuse3 op handlers in tests.
    return cast("pyfuse3.RequestContext", fake_request_context())


@contextlib.contextmanager
def capture_readdir() -> Iterator[list[ReaddirEntry]]:
    """Intercept `pyfuse3.readdir_reply`, collecting the entries a `readdir`
    handler emits. Restores the original on exit."""
    captured: list[ReaddirEntry] = []
    original = pyfuse3.readdir_reply

    def _fake_reply(_token: object, name: bytes, attr: object, next_id: int) -> bool:
        captured.append((name, attr, next_id))
        return True

    pyfuse3.readdir_reply = _fake_reply  # pyright: ignore[reportAttributeAccessIssue]
    try:
        yield captured
    finally:
        pyfuse3.readdir_reply = original  # pyright: ignore[reportAttributeAccessIssue]


async def capture_lookup(
    lookup: LookupOp,
    parent_inode: int,
    name: str | bytes,
    *,
    ctx: pyfuse3.RequestContext | None = None,
) -> pyfuse3.EntryAttributes:
    """Invoke a pyfuse3 `lookup` op with sane test defaults."""
    encoded = name.encode("utf-8") if isinstance(name, str) else name
    return await lookup(parent_inode, encoded, _coerce_ctx(ctx))


async def capture_getattr(
    getattr_op: GetattrOp,
    inode: int,
    *,
    ctx: pyfuse3.RequestContext | None = None,
) -> pyfuse3.EntryAttributes:
    """Invoke a pyfuse3 `getattr` op with sane test defaults."""
    return await getattr_op(inode, _coerce_ctx(ctx))


async def capture_read(read_op: ReadOp, fh: int, *, off: int = 0, size: int = 131072) -> bytes:
    """Invoke a pyfuse3 `read` op and return raw bytes."""
    return await read_op(fh, off, size)


def tier_aware_channels_factory() -> list[ChannelTableRow]:
    """Build a deterministic channels-table stub with mixed logical tiers."""
    return [
        {
            "channel_id": "C-HOT-001",
            "name": "general",
            "tier": "hot",
            "tier_source": "manual",
            "is_member": True,
            "is_archived": False,
        },
        {
            "channel_id": "C-WARM-001",
            "name": "engineering",
            "tier": "warm",
            "tier_source": "auto",
            "is_member": True,
            "is_archived": False,
        },
        {
            "channel_id": "C-COLD-001",
            "name": "announcements",
            "tier": "cold",
            "tier_source": "auto",
            "is_member": True,
            "is_archived": False,
        },
        {
            "channel_id": "C-BLOCK-001",
            "name": "legacy-private",
            "tier": "blocked",
            "tier_source": "auto",
            "is_member": False,
            "is_archived": True,
        },
    ]
