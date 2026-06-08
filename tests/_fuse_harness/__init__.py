"""In-memory pyfuse3 harness — invoke FUSE handlers without mounting.

**Skeleton only.** Fleshed out in Sprint 2F / used by the Sprint 3B FUSE
adapter tests. Provides `fake_request_context()` (a stand-in for
`pyfuse3.RequestContext`) and `capture_readdir()` (a context manager that
intercepts `pyfuse3.readdir_reply` and collects the `(name, attributes,
next_id)` tuples a `readdir` handler emits).

Implementation lives in `harness.py`; re-exported here.
"""

from __future__ import annotations

from tests._fuse_harness.harness import (
    FakeRequestContext,
    ReaddirEntry,
    capture_readdir,
    fake_request_context,
)

__all__ = [
    "FakeRequestContext",
    "ReaddirEntry",
    "capture_readdir",
    "fake_request_context",
]
