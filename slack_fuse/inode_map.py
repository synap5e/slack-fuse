"""Bidirectional path ↔ inode mapping for the FUSE filesystem."""

from __future__ import annotations

import pyfuse3


class InodeMap:
    """Manages bidirectional mapping between paths and inodes.

    Inode 1 is reserved for the root.
    """

    def __init__(self) -> None:
        self._path_to_inode: dict[str, int] = {"/": pyfuse3.ROOT_INODE}
        self._inode_to_path: dict[int, str] = {pyfuse3.ROOT_INODE: "/"}
        self._next_inode = pyfuse3.ROOT_INODE + 1

    def get_or_create(self, path: str) -> int:
        """Get existing inode for path, or create a new one."""
        if path in self._path_to_inode:
            return self._path_to_inode[path]
        inode = self._next_inode
        self._next_inode += 1
        self._path_to_inode[path] = inode
        self._inode_to_path[inode] = path
        return inode

    def get_inode(self, path: str) -> int | None:
        """Get inode for path, or None if not mapped."""
        return self._path_to_inode.get(path)

    def get_path(self, inode: int) -> str | None:
        """Get path for inode, or None if not mapped."""
        return self._inode_to_path.get(inode)

    @property
    def count(self) -> int:
        """Number of mapped inodes."""
        return len(self._inode_to_path)

    def clear(self) -> None:
        """Reset all mappings except root."""
        self._path_to_inode = {"/": pyfuse3.ROOT_INODE}
        self._inode_to_path = {pyfuse3.ROOT_INODE: "/"}
        self._next_inode = pyfuse3.ROOT_INODE + 1
