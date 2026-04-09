"""pyfuse3 Operations subclass for the Slack FUSE filesystem."""

from __future__ import annotations

import errno
import logging
import os
import stat
import time

import pyfuse3

from .inode_map import InodeMap
from .store import SlackStore

log = logging.getLogger(__name__)

# Path structure (channels — dms/group-dms/other-channels follow same pattern):
# /                                                         depth 0
# /channels/                                                depth 1
# /channels/<slug>/                                         depth 2
# /channels/<slug>/channel.md                               depth 3 (metadata file)
# /channels/<slug>/<YYYY-MM>/                               depth 3 (month dir)
# /channels/<slug>/<YYYY-MM>/<DD>/                          depth 4 (day dir)
# /channels/<slug>/<YYYY-MM>/<DD>/channel.md                depth 5 (day snapshot)
# /channels/<slug>/<YYYY-MM>/<DD>/feed.md                   depth 5 (day feed)
# /channels/<slug>/<YYYY-MM>/<DD>/<thread-slug>/            depth 5 (thread dir)
# /channels/<slug>/<YYYY-MM>/<DD>/<thread>/thread.md        depth 6
# /channels/<slug>/<YYYY-MM>/<DD>/<thread>/feed.md          depth 6
# /channels/<slug>/<YYYY-MM>/<DD>/<thread>/huddles/         depth 6
# /channels/<slug>/<YYYY-MM>/<DD>/<thread>/huddles/<slug>/  depth 7
# /channels/<slug>/<YYYY-MM>/<DD>/<thread>/huddles/<slug>/notes.md  depth 8

_TOP_DIRS = ("channels", "dms", "group-dms", "other-channels", "huddles")
_ROOT_DIRS = (*_TOP_DIRS, ".cached-only")
_CONV_ROOTS = frozenset(("channels", "dms", "group-dms", "other-channels"))


def _make_dir_attr(inode: int) -> pyfuse3.EntryAttributes:
    entry = pyfuse3.EntryAttributes()
    entry.st_ino = inode  # pyright: ignore[reportAttributeAccessIssue]
    entry.st_mode = stat.S_IFDIR | 0o555
    entry.st_nlink = 2
    entry.st_size = 0
    now_ns = int(time.time() * 1e9)
    entry.st_atime_ns = now_ns
    entry.st_mtime_ns = now_ns
    entry.st_ctime_ns = now_ns
    entry.st_uid = os.getuid()
    entry.st_gid = os.getgid()
    return entry


def _make_file_attr(inode: int, size: int) -> pyfuse3.EntryAttributes:
    entry = pyfuse3.EntryAttributes()
    entry.st_ino = inode  # pyright: ignore[reportAttributeAccessIssue]
    entry.st_mode = stat.S_IFREG | 0o444
    entry.st_nlink = 1
    entry.st_size = size
    now_ns = int(time.time() * 1e9)
    entry.st_atime_ns = now_ns
    entry.st_mtime_ns = now_ns
    entry.st_ctime_ns = now_ns
    entry.st_uid = os.getuid()
    entry.st_gid = os.getgid()
    return entry


def _make_symlink_attr(inode: int) -> pyfuse3.EntryAttributes:
    entry = pyfuse3.EntryAttributes()
    entry.st_ino = inode  # pyright: ignore[reportAttributeAccessIssue]
    entry.st_mode = stat.S_IFLNK | 0o777
    entry.st_nlink = 1
    entry.st_size = 0
    now_ns = int(time.time() * 1e9)
    entry.st_atime_ns = now_ns
    entry.st_mtime_ns = now_ns
    entry.st_ctime_ns = now_ns
    entry.st_uid = os.getuid()
    entry.st_gid = os.getgid()
    return entry


def _date_str(month: str, day: str) -> str:
    """Combine YYYY-MM and DD into YYYY-MM-DD."""
    return f"{month}-{day}"


class SlackFuseOps(pyfuse3.Operations):
    """Read-only FUSE operations for Slack."""

    def __init__(self, store: SlackStore) -> None:
        super().__init__()
        self._store = store
        self._inodes = InodeMap()
        self._content: dict[int, bytes] = {}

    def _parse_path(self, path: str) -> list[str]:
        stripped = path.strip("/")
        return stripped.split("/") if stripped else []

    def _strip_cached_prefix(
        self, path: str,
    ) -> tuple[str, bool]:
        """Strip .cached-only prefix. Returns (real_path, is_cached_only)."""
        parts = self._parse_path(path)
        if parts and parts[0] == ".cached-only":
            if len(parts) == 1:
                return "/", True
            return "/" + "/".join(parts[1:]), True
        return path, False

    # === Directory listing ===

    def _list_dir(self, path: str) -> list[tuple[str, bool]]:
        """List directory entries as (name, is_dir) tuples."""
        real_path, cached_only = self._strip_cached_prefix(path)
        if cached_only:
            with self._store.cached_only_mode():
                entries = self._list_dir_impl(real_path)
                # Don't nest .cached-only inside itself
                return [
                    e for e in entries if e[0] != ".cached-only"
                ]
        return self._list_dir_impl(real_path)

    def _list_dir_impl(
        self, path: str,
    ) -> list[tuple[str, bool]]:
        """List directory entries as (name, is_dir) tuples."""
        parts = self._parse_path(path)
        depth = len(parts)

        if depth == 0:
            return [(d, True) for d in _ROOT_DIRS]

        # /huddles/ tree — search-based index with symlinks
        if parts[0] == "huddles":
            return self._list_huddle_dir(parts)

        if parts[0] not in _CONV_ROOTS:
            return []

        # depth 1: /channels/ — list conversation slugs
        if depth == 1:
            channels = self._store.list_channels(kind=parts[0])
            return [(e.slug, True) for e in channels.values()]

        entry = self._store.get_channel_by_slug(parts[1])
        if entry is None:
            return []
        cid = entry.channel.id

        # depth 2: /channels/<slug>/ — channel.md + month dirs
        if depth == 2:
            dates = self._store.get_known_dates(cid)
            months = sorted({d[:7] for d in dates}, reverse=True)
            result: list[tuple[str, bool]] = [("channel.md", False)]
            result.extend((m, True) for m in months)
            return result

        # depth 3: /channels/<slug>/<YYYY-MM>/ — day dirs
        if depth == 3:
            if parts[2] == "channel.md":
                return []
            month = parts[2]
            dates = self._store.get_known_dates(cid)
            days = sorted(
                {d[8:] for d in dates if d[:7] == month},
                reverse=True,
            )
            return [(d, True) for d in days]

        # depth 4: /channels/<slug>/<YYYY-MM>/<DD>/ — day files + thread dirs
        if depth == 4:
            date = _date_str(parts[2], parts[3])
            threads = self._store.get_thread_slugs(cid, date)
            result = [("channel.md", False), ("feed.md", False)]
            result.extend((slug, True) for slug in threads)
            return result

        # depth 5: /channels/<slug>/<YYYY-MM>/<DD>/<thread-slug>/ — thread files
        if depth == 5:
            if parts[4] in ("channel.md", "feed.md"):
                return []
            date = _date_str(parts[2], parts[3])
            result = [("thread.md", False), ("feed.md", False)]
            # Check for huddle attachments from day messages (cheap — already cached)
            # instead of fetching the full thread
            thread_slugs = self._store.get_thread_slugs(cid, date)
            thread_ts = thread_slugs.get(parts[4])
            if thread_ts:
                day_msgs = self._store.get_day_messages(cid, date)
                for msg in day_msgs:
                    if msg.ts == thread_ts:
                        if any(f.is_huddle_canvas for f in msg.files):
                            result.append(("huddles", True))
                        break
            return result

        # depth 6: /channels/<slug>/<YYYY-MM>/<DD>/<thread>/huddles/
        if depth == 6 and parts[5] == "huddles":
            date = _date_str(parts[2], parts[3])
            thread_slugs = self._store.get_thread_slugs(cid, date)
            thread_ts = thread_slugs.get(parts[4])
            if thread_ts:
                huddles = self._store.get_huddles_for_thread(cid, thread_ts)
                return [(slug, True) for slug in huddles]
            return []

        # depth 7: /channels/<slug>/<YYYY-MM>/<DD>/<thread>/huddles/<huddle>/
        if depth == 7 and parts[5] == "huddles":
            result = [("notes.md", False)]
            # Check if transcript exists + add index symlink
            date = _date_str(parts[2], parts[3])
            thread_slugs = self._store.get_thread_slugs(cid, date)
            thread_ts = thread_slugs.get(parts[4])
            if thread_ts:
                huddles = self._store.get_huddles_for_thread(cid, thread_ts)
                huddle_data = huddles.get(parts[6])
                if huddle_data:
                    if huddle_data[2] is not None:
                        result.append(("transcript.md", False))
                    # Add symlink back to authoritative huddles index location
                    canvas_id = huddle_data[0].canvas_file_id
                    if self._store.find_huddle_index_entry_by_canvas(canvas_id):
                        result.append(("index", False))
            return result

        return []

    # === Huddle index ===

    def _list_huddle_dir(self, parts: list[str]) -> list[tuple[str, bool]]:
        depth = len(parts)
        index = self._store.get_huddle_index()

        # /huddles/ — list months
        if depth == 1:
            months = sorted({e["month"] for e in index}, reverse=True)
            return [(m, True) for m in months]

        # /huddles/<YYYY-MM>/ — list days
        if depth == 2:
            month = parts[1]
            days = sorted({e["day"] for e in index if e["month"] == month}, reverse=True)
            return [(d, True) for d in days]

        # /huddles/<YYYY-MM>/<DD>/ — list huddle dirs
        if depth == 3:
            month, day = parts[1], parts[2]
            return [
                (e["slug"], True)
                for e in index
                if e["month"] == month and e["day"] == day
            ]

        # /huddles/<YYYY-MM>/<DD>/<slug>/ — huddle content
        if depth == 4:
            entry = self._find_huddle_index_entry(parts[1], parts[2], parts[3])
            if entry is None:
                return []
            result: list[tuple[str, bool]] = [("notes.md", False)]
            data = self._store.get_huddle_by_canvas_id(entry["canvas_file_id"])
            if data and data[2] is not None:
                result.append(("transcript.md", False))
            return result

        return []

    def _find_huddle_index_entry(
        self, month: str, day: str, slug: str,
    ) -> dict[str, str] | None:
        index = self._store.get_huddle_index()
        for e in index:
            if e["month"] == month and e["day"] == day and e["slug"] == slug:
                return e
        return None

    def _is_index_backlink(self, parts: list[str]) -> bool:
        """Check if path is the 'index' symlink inside a channel-tree huddle dir.

        Path: channels/<slug>/<YYYY-MM>/<DD>/<thread>/huddles/<huddle>/index
        """
        if len(parts) != 8:
            return False
        if parts[0] not in _CONV_ROOTS:
            return False
        if parts[5] != "huddles" or parts[7] != "index":
            return False
        return True

    # === File content ===

    def _resolve_content(self, path: str) -> bytes | None:
        real_path, cached_only = self._strip_cached_prefix(path)
        if cached_only:
            with self._store.cached_only_mode():
                return self._resolve_content_impl(real_path)
        return self._resolve_content_impl(real_path)

    def _resolve_content_impl(self, path: str) -> bytes | None:
        parts = self._parse_path(path)
        depth = len(parts)

        # /huddles/<YYYY-MM>/<DD>/<slug>/notes.md or transcript.md (non-threaded)
        if (
            depth == 5
            and parts[0] == "huddles"
            and parts[4] in ("notes.md", "transcript.md")
        ):
            entry = self._find_huddle_index_entry(parts[1], parts[2], parts[3])
            if entry is None:
                return None
            data = self._store.get_huddle_by_canvas_id(entry["canvas_file_id"])
            if data is None:
                return None
            _info, notes_md, transcript_md = data
            if parts[4] == "transcript.md":
                return transcript_md.encode() if transcript_md else b"# Transcript\n\nNot available.\n"
            return notes_md.encode() if notes_md else b"# Huddle Notes\n\nNot available.\n"

        if depth < 3 or parts[0] not in _CONV_ROOTS:
            return None

        entry = self._store.get_channel_by_slug(parts[1])
        if entry is None:
            return None
        cid = entry.channel.id

        # /channels/<slug>/channel.md — channel metadata
        if depth == 3 and parts[2] == "channel.md":
            return self._store.render_channel_info(cid)

        # /channels/<slug>/<YYYY-MM>/<DD>/channel.md — day snapshot
        if depth == 5 and parts[4] == "channel.md":
            return self._store.render_day_channel(cid, _date_str(parts[2], parts[3]))

        # /channels/<slug>/<YYYY-MM>/<DD>/feed.md — day feed
        if depth == 5 and parts[4] == "feed.md":
            return self._store.render_day_feed(cid, _date_str(parts[2], parts[3]))

        # /channels/<slug>/<YYYY-MM>/<DD>/<thread>/thread.md
        if depth == 6 and parts[5] == "thread.md":
            return self._resolve_thread(parts, snapshot=True)

        # /channels/<slug>/<YYYY-MM>/<DD>/<thread>/feed.md
        if depth == 6 and parts[5] == "feed.md":
            return self._resolve_thread(parts, snapshot=False)

        # /channels/<slug>/<YYYY-MM>/<DD>/<thread>/huddles/<huddle>/notes.md
        if depth == 8 and parts[5] == "huddles" and parts[7] in ("notes.md", "transcript.md"):
            return self._resolve_huddle_file(parts)

        return None

    def _resolve_thread(self, parts: list[str], *, snapshot: bool) -> bytes | None:
        entry = self._store.get_channel_by_slug(parts[1])
        if entry is None:
            return None
        date = _date_str(parts[2], parts[3])
        thread_slugs = self._store.get_thread_slugs(entry.channel.id, date)
        thread_ts = thread_slugs.get(parts[4])
        if thread_ts is None:
            return None
        if snapshot:
            return self._store.render_thread_snapshot(entry.channel.id, thread_ts)
        return self._store.render_thread_feed(entry.channel.id, thread_ts)

    def _resolve_huddle_file(self, parts: list[str]) -> bytes | None:
        entry = self._store.get_channel_by_slug(parts[1])
        if entry is None:
            return None
        date = _date_str(parts[2], parts[3])
        thread_slugs = self._store.get_thread_slugs(entry.channel.id, date)
        thread_ts = thread_slugs.get(parts[4])
        if thread_ts is None:
            return None
        huddles = self._store.get_huddles_for_thread(entry.channel.id, thread_ts)
        huddle_data = huddles.get(parts[6])
        if huddle_data is None:
            return None
        _info, canvas_md, transcript_md = huddle_data
        if parts[7] == "transcript.md":
            if transcript_md is None:
                return b"# Transcript\n\nTranscript could not be loaded.\n"
            return transcript_md.encode()
        if canvas_md is None:
            return b"# Huddle Notes\n\nCanvas content could not be loaded.\n"
        return canvas_md.encode()

    # === Path classification ===

    def _is_dir(self, path: str) -> bool:
        real_path, cached_only = self._strip_cached_prefix(path)
        if cached_only:
            with self._store.cached_only_mode():
                return self._is_dir_impl(real_path)
        return self._is_dir_impl(real_path)

    def _is_dir_impl(self, path: str) -> bool:
        parts = self._parse_path(path)
        depth = len(parts)
        if depth == 0:
            return True
        if depth == 1:
            return parts[0] in _TOP_DIRS
        # /huddles/ tree
        if parts[0] == "huddles":
            return depth <= 4  # month, day, slug dirs; depth 5 = files
        if parts[0] not in _CONV_ROOTS:
            return False
        if depth == 2 and parts[0] in _CONV_ROOTS:
            return self._store.get_channel_by_slug(parts[1]) is not None
        if depth == 3 and parts[0] in _CONV_ROOTS:
            return parts[2] != "channel.md"  # month dir
        if depth == 4 and parts[0] in _CONV_ROOTS:
            return True  # day dir
        if depth == 5 and parts[0] in _CONV_ROOTS:
            return parts[4] not in ("channel.md", "feed.md")  # thread dir
        if depth == 6 and parts[0] in _CONV_ROOTS:
            if parts[5] in ("thread.md", "feed.md"):
                return False
            return parts[5] == "huddles"
        if depth == 7 and parts[5] == "huddles":
            return True  # huddle slug dir
        if depth == 8 and parts[7] in ("notes.md", "transcript.md", "index"):
            return False
        return False

    # === FUSE operations ===

    async def getattr(
        self,
        inode: int,
        ctx: pyfuse3.RequestContext,
    ) -> pyfuse3.EntryAttributes:
        path = self._inodes.get_path(inode)
        if path is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        real_path, _ = self._strip_cached_prefix(path)
        if self._is_index_backlink(self._parse_path(real_path)):
            return _make_symlink_attr(inode)

        if self._is_dir(path):
            return _make_dir_attr(inode)

        content = self._resolve_content(path)
        if content is not None:
            self._content[inode] = content
            return _make_file_attr(inode, len(content))

        raise pyfuse3.FUSEError(errno.ENOENT)

    async def lookup(
        self,
        parent_inode: int,
        name: bytes,
        ctx: pyfuse3.RequestContext,
    ) -> pyfuse3.EntryAttributes:
        parent_path = self._inodes.get_path(parent_inode)
        if parent_path is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        child_name = name.decode("utf-8", errors="surrogateescape")
        child_path = (
            f"/{child_name}" if parent_path == "/" else f"{parent_path}/{child_name}"
        )

        entries = self._list_dir(parent_path)
        found = False
        is_dir = False
        for entry_name, entry_is_dir in entries:
            if entry_name == child_name:
                found = True
                is_dir = entry_is_dir
                break

        if not found:
            raise pyfuse3.FUSEError(errno.ENOENT)

        inode = self._inodes.get_or_create(child_path)

        real_child, _ = self._strip_cached_prefix(child_path)
        if self._is_index_backlink(self._parse_path(real_child)):
            return _make_symlink_attr(inode)

        if is_dir:
            return _make_dir_attr(inode)

        content = self._resolve_content(child_path)
        if content is not None:
            self._content[inode] = content
            return _make_file_attr(inode, len(content))

        raise pyfuse3.FUSEError(errno.ENOENT)

    async def opendir(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        inode: int,
        ctx: pyfuse3.RequestContext,
    ) -> int:
        path = self._inodes.get_path(inode)
        if path is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        return inode

    async def readlink(self, inode: int, ctx: pyfuse3.RequestContext) -> bytes:
        path = self._inodes.get_path(inode)
        if path is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        real_path, _ = self._strip_cached_prefix(path)
        parts = self._parse_path(real_path)
        if not self._is_index_backlink(parts):
            raise pyfuse3.FUSEError(errno.EINVAL)

        # Resolve the channel-tree huddle to its index entry
        entry = self._store.get_channel_by_slug(parts[1])
        if entry is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        cid = entry.channel.id

        date = _date_str(parts[2], parts[3])
        thread_slugs = self._store.get_thread_slugs(cid, date)
        thread_ts = thread_slugs.get(parts[4])
        if thread_ts is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        huddles = self._store.get_huddles_for_thread(cid, thread_ts)
        huddle_data = huddles.get(parts[6])
        if huddle_data is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        canvas_id = huddle_data[0].canvas_file_id
        index_entry = self._store.find_huddle_index_entry_by_canvas(canvas_id)
        if index_entry is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        # Build relative path from huddle dir back to /huddles/<month>/<day>/<slug>
        # We're at: /channels/<slug>/<YYYY-MM>/<DD>/<thread>/huddles/<huddle>/index
        # Target:   /huddles/<YYYY-MM>/<DD>/<slug>
        # 7 levels up to root, then huddles/...
        target = f"../../../../../../../huddles/{index_entry['month']}/{index_entry['day']}/{index_entry['slug']}"
        return target.encode()

    async def readdir(
        self,
        fh: int,
        start_id: int,
        token: pyfuse3.ReaddirToken,
    ) -> None:
        path = self._inodes.get_path(fh)
        if path is None:
            return

        entries = self._list_dir(path)

        for idx, (name, is_dir) in enumerate(entries):
            if idx < start_id:
                continue

            child_path = (
                f"/{name}" if path == "/" else f"{path}/{name}"
            )
            child_inode = self._inodes.get_or_create(child_path)

            real_child, _ = self._strip_cached_prefix(child_path)
            if self._is_index_backlink(self._parse_path(real_child)):
                attr = _make_symlink_attr(child_inode)
            elif is_dir:
                attr = _make_dir_attr(child_inode)
            else:
                content = self._resolve_content(child_path)
                size = len(content) if content is not None else 0
                if content is not None:
                    self._content[child_inode] = content
                attr = _make_file_attr(child_inode, size)

            if not pyfuse3.readdir_reply(
                token,
                name.encode("utf-8"),
                attr,
                idx + 1,
            ):
                break

    async def open(
        self,
        inode: int,
        flags: int,
        ctx: pyfuse3.RequestContext,
    ) -> pyfuse3.FileInfo:
        path = self._inodes.get_path(inode)
        if path is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        if inode not in self._content:
            content = self._resolve_content(path)
            if content is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            self._content[inode] = content

        fi = pyfuse3.FileInfo()
        fi.fh = inode  # pyright: ignore[reportAttributeAccessIssue]
        fi.keep_cache = False  # Fresh content on each open
        return fi

    async def read(self, fh: int, off: int, size: int) -> bytes:
        content = self._content.get(fh)
        if content is None:
            raise pyfuse3.FUSEError(errno.EIO)
        return content[off : off + size]

    async def statfs(
        self, ctx: pyfuse3.RequestContext,
    ) -> pyfuse3.StatvfsData:
        stat_info = pyfuse3.StatvfsData()
        stat_info.f_bsize = 4096
        stat_info.f_frsize = 4096
        stat_info.f_blocks = 0
        stat_info.f_bfree = 0
        stat_info.f_bavail = 0
        stat_info.f_files = self._inodes.count
        stat_info.f_ffree = 0
        stat_info.f_favail = 0
        stat_info.f_namemax = 255
        return stat_info
