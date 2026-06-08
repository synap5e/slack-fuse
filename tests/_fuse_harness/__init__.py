"""In-memory pyfuse3 harness — invoke FUSE handlers without mounting.

Exports:
- `fake_request_context()` stand-in for `pyfuse3.RequestContext`
- `capture_readdir()` context manager collecting emitted dir entries
- `capture_lookup()` / `capture_getattr()` / `capture_read()` async op helpers
- `tier_aware_channels_factory()` deterministic mixed-tier channels rows

Implementation lives in `harness.py`; re-exported here.
"""

from __future__ import annotations

from tests._fuse_harness.harness import (
    ChannelTableRow,
    FakeRequestContext,
    GetattrOp,
    LookupOp,
    ReaddirEntry,
    ReadOp,
    capture_getattr,
    capture_lookup,
    capture_read,
    capture_readdir,
    fake_request_context,
    tier_aware_channels_factory,
)

__all__ = [
    "ChannelTableRow",
    "FakeRequestContext",
    "GetattrOp",
    "LookupOp",
    "ReadOp",
    "ReaddirEntry",
    "capture_getattr",
    "capture_lookup",
    "capture_read",
    "capture_readdir",
    "fake_request_context",
    "tier_aware_channels_factory",
]
