"""FUSE-request-scoped logging context.

Every FUSE callback opens a ``fuse_op`` scope that pushes:

- ``req_id`` — a per-callback short ID (e.g. ``r#1234``). Makes it
  trivial to grep one logical FUSE operation out of a busy journal:
  every log line from kernel callback → ``_run_sync`` → sync body →
  PG query → render → response carries the same ID.
- ``op`` — the kernel-side FUSE method (``getattr``, ``read``, …).
- ``inode`` — the FUSE inode the kernel asked about.
- ``path`` — the slack-fuse path that resolved to that inode (or
  ``?`` if we haven't resolved it yet).
- ``elapsed_ms`` — wall-clock time the scope has been open. Logged on
  exit if the callback took long enough to be interesting (default
  >250ms — well below the 1s callback timeout).

The values live in :mod:`contextvars`, which trio propagates through
``trio.to_thread.run_sync`` automatically, so a sync body running on
a worker thread sees the same context as the originating event-loop
task. Log records get the fields via :class:`FuseContextFilter`,
attached once at startup; the format string then references them as
``%(req_id)s``, ``%(op)s``, etc.

The whole module is read-only from the perspective of the FUSE
code: the contract is "open a scope, log normally, close the scope".
There are no manual ``extra={...}`` parameters to thread through
every call site.
"""

from __future__ import annotations

import contextlib
import itertools
import logging
import time
from contextvars import ContextVar
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator


#: Monotonically-increasing counter for request IDs. Reset on process start;
#: collisions across process restarts are fine because we always carry the
#: process pid in the journal.
_req_counter = itertools.count(1)


_req_id_var: ContextVar[str | None] = ContextVar("fuse_req_id", default=None)
_op_var: ContextVar[str | None] = ContextVar("fuse_op", default=None)
_inode_var: ContextVar[int | None] = ContextVar("fuse_inode", default=None)
_path_var: ContextVar[str | None] = ContextVar("fuse_path", default=None)
_start_var: ContextVar[float | None] = ContextVar("fuse_start", default=None)


#: Default threshold for the "slow callback" warning on scope exit.
#: 250ms is well under the 1s callback timeout but well above the
#: typical 1-10ms for a cached-chunk read against a healthy local PG.
SLOW_THRESHOLD_S: float = 0.25


class FuseContextFilter(logging.Filter):
    """Inject FUSE-scope fields into every LogRecord.

    The fields default to ``-`` outside a scope so the format string
    never throws ``KeyError`` (e.g. for logs from the startup path
    before any FUSE op has fired).
    """

    def filter(self, record: logging.LogRecord) -> bool:
        record.req_id = _req_id_var.get() or "-"
        record.fuse_op = _op_var.get() or "-"
        inode = _inode_var.get()
        record.inode = str(inode) if inode is not None else "-"
        record.fuse_path = _path_var.get() or "-"
        return True


@contextlib.contextmanager
def fuse_op(
    op: str,
    *,
    inode: int | None = None,
    path: str | None = None,
    slow_threshold_s: float = SLOW_THRESHOLD_S,
    logger: logging.Logger | None = None,
) -> Iterator[str]:
    """Open a FUSE-request scope around a callback body.

    Pushes ``op``, ``inode``, ``path``, and a fresh ``req_id`` onto
    contextvars for the lifetime of the ``with`` block; logs a single
    ``slow op`` warning on exit if the body took longer than
    ``slow_threshold_s``. Yields the ``req_id`` for callers that want
    to embed it in custom log lines explicitly.

    Callers may call :func:`set_path` later inside the block once the
    path resolves from the inode — the slow-op warning then prints the
    resolved path rather than ``?``.
    """
    log = logger if logger is not None else logging.getLogger("slack_fuse.fuse")
    req_id = f"r#{next(_req_counter)}"
    op_tok = _op_var.set(op)
    rid_tok = _req_id_var.set(req_id)
    inode_tok = _inode_var.set(inode)
    path_tok = _path_var.set(path)
    start = time.monotonic()
    start_tok = _start_var.set(start)
    try:
        yield req_id
    finally:
        elapsed = time.monotonic() - start
        if elapsed >= slow_threshold_s:
            # NB: the format-filter is already attached at this point so
            # the log line carries req_id / op / inode / path from the
            # contextvars below — no extra args needed.
            log.warning("slow op: %.0fms", elapsed * 1000)
        _start_var.reset(start_tok)
        _path_var.reset(path_tok)
        _inode_var.reset(inode_tok)
        _req_id_var.reset(rid_tok)
        _op_var.reset(op_tok)


def set_path(path: str | None) -> None:
    """Update the current scope's ``path`` field.

    Useful after resolving inode → path inside a callback; subsequent
    log lines (and the slow-op warning) then show the human-readable
    path instead of just the inode.
    """
    _path_var.set(path)


def current_scope() -> dict[str, object]:
    """Snapshot of the current scope's fields. Test introspection only."""
    return {
        "req_id": _req_id_var.get(),
        "op": _op_var.get(),
        "inode": _inode_var.get(),
        "path": _path_var.get(),
        "start": _start_var.get(),
    }
