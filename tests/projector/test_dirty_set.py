"""Pure tests for the disk projection's ordered dirty set."""

from __future__ import annotations

import threading

import pytest

from slack_fuse.projector.dirty_set import DirtySet


def test_empty_and_over_limit_drains() -> None:
    dirty = DirtySet[str]()
    assert dirty.drain(10) == []

    dirty.mark("/a")
    dirty.mark("/b")
    dirty.mark("/a")
    assert dirty.drain(10) == ["/a", "/b"]
    assert dirty.drain(10) == []


def test_zero_limit_preserves_paths_and_negative_limit_rejected() -> None:
    dirty = DirtySet[str]()
    dirty.mark("/a")
    assert dirty.drain(0) == []
    assert len(dirty) == 1
    with pytest.raises(ValueError, match="non-negative"):
        dirty.drain(-1)


def test_concurrent_marks_during_drain_are_not_lost() -> None:
    dirty = DirtySet[str]()
    initial = {f"/initial/{index}" for index in range(100)}
    concurrent = {f"/concurrent/{index}" for index in range(100)}
    for path in initial:
        dirty.mark(path)

    start = threading.Barrier(2)

    def mark_concurrently() -> None:
        _ = start.wait()
        for path in concurrent:
            dirty.mark(path)

    worker = threading.Thread(target=mark_concurrently)
    worker.start()
    _ = start.wait()
    first = dirty.drain(50)
    worker.join()
    remainder = dirty.drain(1000)

    assert set(first) | set(remainder) == initial | concurrent
    assert len(first) + len(remainder) == len(initial) + len(concurrent)
