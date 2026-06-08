"""In-memory pyfuse3 harness implementation. Re-exported from `__init__`."""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from dataclasses import dataclass

import pyfuse3

ReaddirEntry = tuple[bytes, object, int]


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
