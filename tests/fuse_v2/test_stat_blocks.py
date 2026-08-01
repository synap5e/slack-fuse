# pyright: reportPrivateUsage=false
"""Regression: ``st_blocks`` on file EntryAttributes must reflect the file
size in 512-byte blocks, not leave the default 0.

Reported 2026-06-27, filed as BACKLOG.md "FUSE getattr st_blocks=0" — every
file in the mount showed as 0B in ``du`` / ``dust`` because both tools compute
usage from ``st_blocks`` regardless of ``st_size``. Fix landed in
``_make_file_attr`` (both fuse_ops_v2 and fuse_ops); this test pins the
formula.
"""

from __future__ import annotations

import pytest

from slack_fuse.fuse_ops import (
    _make_file_attr as _make_file_attr_v1,
)
from slack_fuse.fuse_ops_v2 import (
    _make_file_attr as _make_file_attr_v2,
)


@pytest.mark.parametrize(
    ("size", "expected_blocks"),
    [
        (0, 0),
        (1, 1),
        (511, 1),
        (512, 1),
        (513, 2),
        (1023, 2),
        (1024, 2),
        (1025, 3),
        (12_345, 25),  # 12_345 / 512 = 24.11 → 25
        (1_048_576, 2048),  # exactly 1 MiB → 2048 blocks
    ],
)
def test_make_file_attr_v2_reports_natural_block_count(size: int, expected_blocks: int) -> None:
    attr = _make_file_attr_v2(42, size)
    assert attr.st_size == size
    assert attr.st_blocks == expected_blocks, (
        f"size={size} → st_blocks should be ceil(size/512)={expected_blocks}, got {attr.st_blocks}"
    )


@pytest.mark.parametrize(
    ("size", "expected_blocks"),
    [(0, 0), (1, 1), (512, 1), (513, 2), (4096, 8)],
)
def test_make_file_attr_v1_reports_natural_block_count(size: int, expected_blocks: int) -> None:
    """Legacy v1 helper — /views/slack now symlinks to /views/slack-split, so
    the v1 path only serves through the symlink for consumers still hitting
    the old code (e.g. slack-fuse.service that's still installed but disabled).
    Keep it consistent."""
    attr = _make_file_attr_v1(42, size)
    assert attr.st_size == size
    assert attr.st_blocks == expected_blocks
