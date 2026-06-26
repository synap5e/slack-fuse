"""Pure helpers for the Sprint 3B FUSE adapter (``fuse_ops_v2.py``).

No pyfuse3 imports — these helpers run unmodified inside unit tests.
Everything here is path-string parsing, range-bound arithmetic, slug
derivation, frontmatter composition, and the small SQL helpers the adapter
calls. Anything that touches an actual ``Operations`` callback lives in
``fuse_ops_v2.py``.

Per RFC §FUSE read path and §Three-tier visibility model.
"""

from __future__ import annotations

import threading
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Final
from zoneinfo import ZoneInfo

from slack_fuse.projector.trailer import (
    FALLBACK_CHANNEL_REASON,
    FALLBACK_USER_REASON,
    STALE_AFTER_DISCONNECT_S,
    StalenessState,
    TrailerDecision,
    classify_trailer,
    format_trailer,
    render_trailer,
    staleness_reason,
)
from slack_fuse.slug import slugify
from slack_fuse_render import ChannelId, ChannelView, UserId, UserResolver, UserView, resolve_mentions
from slack_fuse_render.resolvers import ChannelResolver

# Re-exported from ``slack_fuse.projector.trailer`` (Sprint 3C extraction) so the
# existing ``fuse_ops_v2`` / health-subscriber / test imports keep resolving
# against ``fuse_v2_helpers`` while the pure trailer logic lives in one module.
__all__ = [
    "FALLBACK_CHANNEL_REASON",
    "FALLBACK_USER_REASON",
    "STALE_AFTER_DISCONNECT_S",
    "StalenessState",
    "TrailerDecision",
    "classify_trailer",
    "format_trailer",
    "render_trailer",
    "staleness_reason",
]

if TYPE_CHECKING:
    from psycopg import Connection, Cursor
    from psycopg.rows import TupleRow

CONV_ROOTS: Final[tuple[str, ...]] = ("channels", "dms", "group-dms", "other-channels")
ROOT_DIRS: Final[tuple[str, ...]] = CONV_ROOTS

# Slugs we use for the channel root metadata + day files.
CHANNEL_MD: Final = "channel.md"
THREAD_MD: Final = "thread.md"
# Ghost file: an originals-view of a day (pre-edit text + crossed-out deletes).
# Reachable via direct ``lookup`` (e.g. ``bat channel.original.md``) but
# DELIBERATELY excluded from ``readdir`` so recursive listings — including
# ``ls -la`` and find/rg sweeps — never accidentally trigger the slow,
# events-replay render path on the server side. See
# ``fuse_ops_v2.SlackFuseOpsV2._list_dir`` for the ``for_lookup=True`` branch
# that surfaces this name.
CHANNEL_ORIGINAL_MD: Final = "channel.original.md"

# Diagnostic ghost file at the channel root: lists UTC days with no message
# events bounded by days that DO have events. Same hide-from-readdir pattern
# as ``channel.original.md`` so a recursive ``rg`` never hits the
# events-aggregation slow path. Workspace-wide summary lives at
# ``/_workspace/gaps.md``.
GAPS_MD: Final = "gaps.md"
# Top-level namespace for read-only diagnostic surfaces (currently just
# gaps; future control-surface ghost files land here too). Listed by the
# root readdir so it's discoverable; its contents are listed normally
# inside.
WORKSPACE_DIR: Final = "_workspace"

# The `channel-list` stream is the staleness stream for channel-metadata
# (``channel.md``) reads — channel inventory/rename/archive flow over it.
CHANNEL_LIST_STREAM: Final = "channel-list"


#: Per-call FUSE conn borrowed from the connection pool. Set by
#: ``SlackFuseOpsV2._run_sync`` around every FUSE callback so the sync body
#: (helpers + ``PersistentInodeMap``) all use one connection owned by *that*
#: callback alone. Lives in ``fuse_v2_helpers`` (not ``fuse_ops_v2``) because
#: it's read from helpers; placing it here avoids a circular import.
#:
#: ``None`` means "pool mode is off" — the legacy conn-only-with-limiter
#: shape used by tests and the original v1 startup. In that case, callers
#: fall back to their own dedicated connection.
borrowed_fuse_conn: ContextVar[Connection[TupleRow] | None] = ContextVar(
    "borrowed_fuse_conn", default=None
)


# ============================================================================
# Path parsing
# ============================================================================


def parse_path(path: str) -> list[str]:
    """Split a FUSE-style absolute path into its parts."""
    stripped = path.strip("/")
    return stripped.split("/") if stripped else []


def is_valid_month(s: str) -> bool:
    """Return True if ``s`` is a strictly ``YYYY-MM`` string."""
    if len(s) != 7 or s[4] != "-":
        return False
    try:
        _ = datetime.strptime(s, "%Y-%m").date()
    except ValueError:
        return False
    return True


def is_valid_day(s: str) -> bool:
    """Return True if ``s`` is a strictly ``DD`` two-digit day."""
    if len(s) != 2:
        return False
    try:
        day = int(s)
    except ValueError:
        return False
    return 1 <= day <= 31


def parse_day_date(month: str, day: str) -> date | None:
    """Parse a (``YYYY-MM``, ``DD``) pair into a ``date``. Returns ``None`` if
    the combination doesn't form a real calendar day.
    """
    if not is_valid_month(month) or not is_valid_day(day):
        return None
    try:
        return datetime.strptime(f"{month}-{day}", "%Y-%m-%d").date()
    except ValueError:
        return None


# ============================================================================
# Local-tz UTC range bounds
# ============================================================================


def local_day_utc_range(day: date, tz: ZoneInfo) -> tuple[Decimal, Decimal]:
    """Return ``[start, end)`` UTC epoch seconds for the supplied local-tz day.

    Handles DST boundaries by going through ``ZoneInfo``: the local midnight
    timestamp may not be exactly 86400 s apart from the next local midnight.
    The renderer's structural pass renders ``HH:MM`` in local tz, so chunks
    bucket-by-day correctly when paired with this range.
    """
    start_local = datetime(day.year, day.month, day.day, tzinfo=tz)
    next_local = datetime.combine(day + timedelta(days=1), datetime.min.time()).replace(tzinfo=tz)
    start_utc = start_local.astimezone(UTC).timestamp()
    end_utc = next_local.astimezone(UTC).timestamp()
    return Decimal(str(start_utc)), Decimal(str(end_utc))


def local_month_utc_range(month: str, tz: ZoneInfo) -> tuple[Decimal, Decimal]:
    """UTC range covering every local-tz day in ``month`` (``YYYY-MM``)."""
    year_s, mon_s = month.split("-", 1)
    year = int(year_s)
    mon = int(mon_s)
    first = date(year, mon, 1)
    next_mon = date(year + (1 if mon == 12 else 0), 1 if mon == 12 else mon + 1, 1)
    start_local = datetime.combine(first, datetime.min.time()).replace(tzinfo=tz)
    end_local = datetime.combine(next_mon, datetime.min.time()).replace(tzinfo=tz)
    return Decimal(str(start_local.astimezone(UTC).timestamp())), Decimal(str(end_local.astimezone(UTC).timestamp()))


def ts_to_local_date(ts: Decimal, tz: ZoneInfo) -> date:
    """Render a Slack epoch ``ts`` as a local-tz calendar date."""
    return datetime.fromtimestamp(float(ts), tz=UTC).astimezone(tz).date()


# ============================================================================
# Channel rows + slug derivation
# ============================================================================


@dataclass(frozen=True, slots=True)
class ChannelRow:
    """Subset of a ``channels`` row the adapter needs at any single point."""

    channel_id: str
    name: str
    is_im: bool
    is_mpim: bool
    is_member: bool
    is_archived: bool
    im_user_id: str | None
    tier: str


def conv_root_for(row: ChannelRow) -> str:
    """The top-level FUSE directory ``row`` lives under."""
    if row.is_im:
        return "dms"
    if row.is_mpim:
        return "group-dms"
    if row.is_member:
        return "channels"
    return "other-channels"


def build_channel_slug(
    row: ChannelRow,
    users_display: dict[str, str],
    slug_counts: dict[str, int],
) -> str:
    """Derive a stable directory slug for ``row``.

    Mirrors the legacy ``store._build_slug``: DM channels use the partner's
    display name (from the local ``users`` table); other channels use their
    name. Collisions get a numeric suffix so ``slack-foo-bar`` and a second
    ``slack-foo-bar`` become ``…`` and ``…-2``. Stable when iterated in a
    deterministic order (e.g. ordered by ``channel_id``).
    """
    if row.is_im and row.im_user_id:
        display = users_display.get(row.im_user_id, row.im_user_id)
        base_slug = slugify(display) or row.channel_id[:12]
    else:
        base_slug = slugify(row.name) if row.name else ""
        if not base_slug:
            base_slug = row.channel_id[:12]
    count = slug_counts.get(base_slug, 0)
    slug_counts[base_slug] = count + 1
    return base_slug if count == 0 else f"{base_slug}-{count + 1}"


def slug_map_for(rows: list[ChannelRow], users_display: dict[str, str]) -> dict[str, str]:
    """Build ``{channel_id: slug}`` from an ordered list of channel rows."""
    counts: dict[str, int] = {}
    return {r.channel_id: build_channel_slug(r, users_display, counts) for r in rows}


def channel_row_to_view(row: ChannelRow) -> ChannelView:
    """Build the renderer's ``ChannelView`` from a row."""
    return ChannelView(
        channel_id=ChannelId(row.channel_id),
        name=row.name or row.channel_id,
        is_im=row.is_im,
        is_mpim=row.is_mpim,
    )


# ============================================================================
# Thread slug derivation
# ============================================================================


def _strip_structural_header(content_md: str) -> str:
    """Return the body text of a structural chunk, with the ``## HH:MM @author``
    header line and the trailing thread/file annotations stripped.

    The structural pass (``render_message_structural``) lays out a message as

        ## HH:MM <@U…>

        <body>

        :reaction: N  …

        :paperclip: [file](attachments/file)

        > Thread: N replies

    For slug derivation we want the first chunk of message text — the bit
    that says what the thread is about.
    """
    lines = content_md.split("\n")
    body_lines: list[str] = []
    for line in lines:
        if line.startswith("## "):
            continue
        if line.startswith(":") and " " in line and "  " in line:  # reaction line "  ".join
            continue
        if line.startswith("\U0001f4ce ["):  # file attachment line
            continue
        if line.startswith("[Huddle Notes]"):
            continue
        if line.startswith("> Thread:"):
            break
        body_lines.append(line)
    return "\n".join(body_lines).strip()


def derive_thread_slug(content_md: str, message_ts: Decimal) -> str:
    """Slug derived from a thread parent's structural ``content_md``.

    Falls back to ``ts-<message_ts>`` if the message body is empty or all
    placeholders (e.g. attachment-only).
    """
    body = _strip_structural_header(content_md)
    base = slugify(body[:120]) if body else ""
    if not base:
        return f"ts-{message_ts}"
    return base


def dedup_thread_slug_map(parents: list[tuple[Decimal, str]]) -> dict[str, Decimal]:
    """Build ``{thread_slug: thread_ts}`` from ordered (ts, content_md) parents.

    Order parents by ``message_ts`` ascending before calling so the dedup
    counter is stable across reads.
    """
    counts: dict[str, int] = {}
    out: dict[str, Decimal] = {}
    for ts, content_md in parents:
        base = derive_thread_slug(content_md, ts)
        count = counts.get(base, 0)
        counts[base] = count + 1
        slug = base if count == 0 else f"{base}-{count + 1}"
        out[slug] = ts
    return out


# ============================================================================
# Frontmatter
# ============================================================================


def channel_meta_frontmatter(row: ChannelRow) -> bytes:
    """Bytes returned for ``/<conv-root>/<slug>/channel.md`` — workspace-level
    metadata about the channel itself (name, id, topic/purpose tier).
    """
    lines = [
        "---",
        f"channel: {row.name or row.channel_id}",
        f"channel_id: {row.channel_id}",
        f"is_im: {str(row.is_im).lower()}",
        f"is_mpim: {str(row.is_mpim).lower()}",
        f"is_member: {str(row.is_member).lower()}",
        f"is_archived: {str(row.is_archived).lower()}",
        f"tier: {row.tier}",
        "---",
        "",
    ]
    return "\n".join(lines).encode()


def thread_frontmatter(row: ChannelRow, thread_ts: Decimal, reply_count: int, tz: ZoneInfo) -> str:
    """Build a thread.md YAML frontmatter block from row+thread-chunks data.

    Mirrors :func:`slack_fuse_render.thread_md_frontmatter` but takes raw
    ``thread_ts`` + ``reply_count`` instead of reconstructing the parent
    ``Message`` from the chunk row.
    """
    parent_date = ts_to_local_date(thread_ts, tz).strftime("%Y-%m-%d")
    return (
        "---\n"
        f"channel: {row.name or row.channel_id}\n"
        f"channel_id: {row.channel_id}\n"
        f'thread_ts: "{thread_ts}"\n'
        f"reply_count: {reply_count}\n"
        f"date: {parent_date}\n"
        "---\n"
    )


def day_channel_frontmatter(row: ChannelRow, day: date) -> str:
    """Build a day-file YAML frontmatter block for ``channel.md`` under a day."""
    return (
        "---\n"
        f"channel: {row.name or row.channel_id}\n"
        f"channel_id: {row.channel_id}\n"
        f"date: {day.strftime('%Y-%m-%d')}\n"
        "---\n"
    )


# ============================================================================
# Tier / connection helpers — SQL surface used by the adapter
# ============================================================================


def _channel_row_from_query(row: tuple[object, ...]) -> ChannelRow:
    return ChannelRow(
        channel_id=str(row[0]),
        name="" if row[1] is None else str(row[1]),
        is_im=bool(row[2]),
        is_mpim=bool(row[3]),
        is_member=bool(row[4]),
        is_archived=bool(row[5]),
        im_user_id=None if row[6] is None else str(row[6]),
        tier=str(row[7]),
    )


def fetch_conv_root_rows(conn: Connection[TupleRow], conv_root: str, *, allow_hidden: bool) -> list[ChannelRow]:
    """SELECT channels in the supplied conv-root, ordered by ``channel_id``.

    ``allow_hidden=False`` (the readdir path) returns ``tier = 'hot'`` only.
    ``allow_hidden=True`` (the lookup path) returns ``tier IN ('hot',
    'hidden')`` — ``blocked`` is never returned.

    Implementation note: psycopg's ``execute`` only accepts ``LiteralString``,
    so the SQL is fully written-out per (conv_root, allow_hidden) combination
    rather than composed dynamically.
    """
    with conn.cursor() as cur:
        if conv_root == "dms":
            if allow_hidden:
                _ = cur.execute(
                    "SELECT channel_id, name, is_im, is_mpim, is_member, is_archived, im_user_id, tier "
                    "FROM channels WHERE is_im = TRUE AND tier IN ('hot', 'hidden') ORDER BY channel_id"
                )
            else:
                _ = cur.execute(
                    "SELECT channel_id, name, is_im, is_mpim, is_member, is_archived, im_user_id, tier "
                    "FROM channels WHERE is_im = TRUE AND tier = 'hot' ORDER BY channel_id"
                )
        elif conv_root == "group-dms":
            if allow_hidden:
                _ = cur.execute(
                    "SELECT channel_id, name, is_im, is_mpim, is_member, is_archived, im_user_id, tier "
                    "FROM channels WHERE is_mpim = TRUE AND tier IN ('hot', 'hidden') ORDER BY channel_id"
                )
            else:
                _ = cur.execute(
                    "SELECT channel_id, name, is_im, is_mpim, is_member, is_archived, im_user_id, tier "
                    "FROM channels WHERE is_mpim = TRUE AND tier = 'hot' ORDER BY channel_id"
                )
        elif conv_root == "channels":
            if allow_hidden:
                _ = cur.execute(
                    "SELECT channel_id, name, is_im, is_mpim, is_member, is_archived, im_user_id, tier "
                    "FROM channels WHERE is_im = FALSE AND is_mpim = FALSE AND is_member = TRUE "
                    "AND tier IN ('hot', 'hidden') ORDER BY channel_id"
                )
            else:
                _ = cur.execute(
                    "SELECT channel_id, name, is_im, is_mpim, is_member, is_archived, im_user_id, tier "
                    "FROM channels WHERE is_im = FALSE AND is_mpim = FALSE AND is_member = TRUE "
                    "AND tier = 'hot' ORDER BY channel_id"
                )
        elif conv_root == "other-channels":
            if allow_hidden:
                _ = cur.execute(
                    "SELECT channel_id, name, is_im, is_mpim, is_member, is_archived, im_user_id, tier "
                    "FROM channels WHERE is_im = FALSE AND is_mpim = FALSE AND is_member = FALSE "
                    "AND tier IN ('hot', 'hidden') ORDER BY channel_id"
                )
            else:
                _ = cur.execute(
                    "SELECT channel_id, name, is_im, is_mpim, is_member, is_archived, im_user_id, tier "
                    "FROM channels WHERE is_im = FALSE AND is_mpim = FALSE AND is_member = FALSE "
                    "AND tier = 'hot' ORDER BY channel_id"
                )
        else:
            msg = f"unknown conv root {conv_root!r}"
            raise ValueError(msg)
        return [_channel_row_from_query(r) for r in cur.fetchall()]


def _slug_sort_key(row: ChannelRow) -> tuple[int, str]:
    """Ordering for slug assignment: ``hot`` channels win the unsuffixed slug,
    ties broken by ``channel_id``.

    Slug suffixes (``general-2``) depend on the order the colliding rows are
    consumed. If ``readdir`` (hot only) and ``lookup`` (hot + hidden) assigned
    over different row-sets, a hidden channel sorting before a hot one of the
    same name would steal the base slug on the lookup path while ``readdir``
    showed it on the hot one — so ``ls`` and ``cat`` disagreed (review P0-4 /
    Gemini Class 4). Assigning over the SAME hot+hidden set in a SAME
    deterministic order on both paths closes that hole; the ``hot``-first
    ordering keeps the hot channel on the unsuffixed slug.
    """
    return (0 if row.tier == "hot" else 1, row.channel_id)


def assign_conv_root_slugs(conn: Connection[TupleRow], conv_root: str) -> list[tuple[ChannelRow, str]]:
    """Assign a stable slug to every ``hot``/``hidden`` channel in ``conv_root``.

    The assignment is computed once over the full hot+hidden row-set so it is
    identical regardless of whether the caller intends to filter to hot
    (``readdir``) or allow hidden (``lookup``). ``blocked`` is never included.
    Returned in slug-assignment order (hot-first, then ``channel_id``).
    """
    rows = fetch_conv_root_rows(conn, conv_root, allow_hidden=True)
    users = fetch_users_for_dm_slugs(conn, rows)
    counts: dict[str, int] = {}
    return [(r, build_channel_slug(r, users, counts)) for r in sorted(rows, key=_slug_sort_key)]


def fetch_channel_by_slug(
    conn: Connection[TupleRow],
    conv_root: str,
    slug: str,
    *,
    allow_hidden: bool,
) -> ChannelRow | None:
    """Replay the (hot+hidden) slug assignment and return the matching row.

    ``allow_hidden=False`` additionally requires the matched row to be
    ``hot`` — so a hidden slug is ENOENT on the readdir path but resolvable on
    the lookup path, while both agree on which channel owns which slug.
    """
    for row, cur_slug in assign_conv_root_slugs(conn, conv_root):
        if cur_slug == slug and (allow_hidden or row.tier == "hot"):
            return row
    return None


def fetch_users_for_dm_slugs(conn: Connection[TupleRow], rows: list[ChannelRow]) -> dict[str, str]:
    """Display-name lookup keyed by ``im_user_id`` — DM rows only need it."""
    ids = [r.im_user_id for r in rows if r.is_im and r.im_user_id]
    if not ids:
        return {}
    with conn.cursor() as cur:
        _ = cur.execute("SELECT user_id, display_name FROM users WHERE user_id = ANY(%s)", (ids,))
        return {str(uid): str(dn) for uid, dn in cur.fetchall()}


def fetch_known_months(
    conn: Connection[TupleRow],
    channel_id: str,
    tz: ZoneInfo,
) -> list[str]:
    """Distinct ``YYYY-MM`` strings the channel has chunks in, newest first."""
    with conn.cursor() as cur:
        _ = cur.execute(
            "SELECT DISTINCT to_char((to_timestamp(message_ts) AT TIME ZONE %s)::date, 'YYYY-MM') AS month "
            "FROM chunks WHERE channel_id = %s ORDER BY month DESC",
            (tz.key, channel_id),
        )
        return [str(r[0]) for r in cur.fetchall()]


def fetch_known_days(
    conn: Connection[TupleRow],
    channel_id: str,
    month: str,
    tz: ZoneInfo,
) -> list[str]:
    """Distinct ``DD`` strings the channel has chunks in, newest first."""
    start, end = local_month_utc_range(month, tz)
    with conn.cursor() as cur:
        _ = cur.execute(
            "SELECT DISTINCT to_char((to_timestamp(message_ts) AT TIME ZONE %s)::date, 'DD') AS day "
            "FROM chunks WHERE channel_id = %s AND message_ts >= %s AND message_ts < %s "
            "ORDER BY day DESC",
            (tz.key, channel_id, start, end),
        )
        return [str(r[0]) for r in cur.fetchall()]


def fetch_day_chunks(
    conn: Connection[TupleRow],
    channel_id: str,
    day: date,
    tz: ZoneInfo,
) -> list[str]:
    """Return ``content_md`` rows for the channel-day, in ts-ascending order."""
    start, end = local_day_utc_range(day, tz)
    with conn.cursor() as cur:
        _ = cur.execute(
            "SELECT content_md FROM chunks "
            "WHERE channel_id = %s AND message_ts >= %s AND message_ts < %s "
            "ORDER BY message_ts",
            (channel_id, start, end),
        )
        return [str(r[0]) for r in cur.fetchall()]


def fetch_day_thread_parents(
    conn: Connection[TupleRow],
    channel_id: str,
    day: date,
    tz: ZoneInfo,
) -> list[tuple[Decimal, str]]:
    """Return ``(message_ts, content_md)`` for chunks with ``reply_count > 0``.

    Used to enumerate the threads that have a directory under the day folder.
    """
    start, end = local_day_utc_range(day, tz)
    with conn.cursor() as cur:
        _ = cur.execute(
            "SELECT message_ts, content_md FROM chunks "
            "WHERE channel_id = %s AND message_ts >= %s AND message_ts < %s AND reply_count > 0 "
            "ORDER BY message_ts",
            (channel_id, start, end),
        )
        return [(Decimal(str(ts)), str(md)) for ts, md in cur.fetchall()]


def fetch_thread_chunks(
    conn: Connection[TupleRow],
    channel_id: str,
    thread_ts: Decimal,
) -> tuple[list[str], int]:
    """Return ``(content_mds, reply_count)`` for a thread.

    The ``content_md`` list contains the parent first then replies ordered by
    ``reply_ts``. ``reply_count`` is read off the parent ``chunks`` row so the
    frontmatter agrees with how the day file rendered the parent.
    """
    with conn.cursor() as cur:
        _ = cur.execute(
            "SELECT content_md FROM thread_chunks WHERE channel_id = %s AND thread_ts = %s ORDER BY reply_ts",
            (channel_id, thread_ts),
        )
        contents = [str(r[0]) for r in cur.fetchall()]
        _ = cur.execute(
            "SELECT reply_count FROM chunks WHERE channel_id = %s AND message_ts = %s",
            (channel_id, thread_ts),
        )
        row = cur.fetchone()
    reply_count = int(row[0]) if row is not None else 0
    return contents, reply_count


# ============================================================================
# Inode persistence — backed by the ``inodes`` table
# ============================================================================


ROOT_INODE: Final = 1


class PersistentInodeMap:
    """Path ↔ inode mapping backed by the ``inodes`` Postgres table.

    The schema declares ``inode BIGINT GENERATED ALWAYS AS IDENTITY (START WITH
    2)`` so inode ``1`` is reserved for the FUSE root. Every other path-inode
    pair survives mount restarts: ``find`` outputs and inode-based watchers
    don't break across restarts. An in-memory LRU-free dict caches lookups; on
    miss it goes to the table.
    """

    def __init__(self, conn: Connection[TupleRow]) -> None:
        # Fallback connection for when no per-call conn is set on the
        # ``_borrowed_fuse_conn`` ContextVar (i.e. legacy conn-only mode in
        # tests, or callers like the invalidation sink that resolve paths
        # off the event loop). In pool mode, every method below picks up
        # the per-callback borrowed conn instead, so concurrent FUSE
        # callbacks never share a psycopg connection.
        self._fallback_conn = conn
        # Cross-thread cache + DB-call serialization. Even in pool mode the
        # in-memory dicts are touched by every callback; without a lock,
        # concurrent ``get_or_create`` on the same path can race. The lock
        # is cheap (microseconds) and only held during dict mutation +
        # DB call; the FUSE callback's main render work happens outside it.
        self._cache_lock = threading.Lock()
        self._path_to_inode: dict[str, int] = {"/": ROOT_INODE}
        self._inode_to_path: dict[int, str] = {ROOT_INODE: "/"}

    def _conn_for_io(self) -> Connection[TupleRow]:
        """Per-call borrowed conn (pool mode), else the fallback (tests)."""
        borrowed = borrowed_fuse_conn.get()
        return borrowed if borrowed is not None else self._fallback_conn

    def get_or_create(self, path: str) -> int:
        with self._cache_lock:
            cached = self._path_to_inode.get(path)
            if cached is not None:
                return cached
        conn = self._conn_for_io()
        with conn.cursor() as cur:
            _ = cur.execute(
                "INSERT INTO inodes (path) VALUES (%s) "
                "ON CONFLICT (path) DO UPDATE SET path = EXCLUDED.path RETURNING inode",
                (path,),
            )
            row = cur.fetchone()
        if row is None:  # pragma: no cover - INSERT … RETURNING always returns
            msg = f"failed to allocate inode for {path!r}"
            raise RuntimeError(msg)
        inode = int(row[0])
        with self._cache_lock:
            self._path_to_inode[path] = inode
            self._inode_to_path[inode] = path
        return inode

    def get_inode(self, path: str) -> int | None:
        with self._cache_lock:
            cached = self._path_to_inode.get(path)
            if cached is not None:
                return cached
        conn = self._conn_for_io()
        with conn.cursor() as cur:
            _ = cur.execute("SELECT inode FROM inodes WHERE path = %s", (path,))
            row = cur.fetchone()
        if row is None:
            return None
        inode = int(row[0])
        with self._cache_lock:
            self._path_to_inode[path] = inode
            self._inode_to_path[inode] = path
        return inode

    def get_path(self, inode: int) -> str | None:
        with self._cache_lock:
            cached = self._inode_to_path.get(inode)
            if cached is not None:
                return cached
        conn = self._conn_for_io()
        with conn.cursor() as cur:
            _ = cur.execute("SELECT path FROM inodes WHERE inode = %s", (inode,))
            row = cur.fetchone()
        if row is None:
            return None
        path = str(row[0])
        with self._cache_lock:
            self._path_to_inode[path] = inode
            self._inode_to_path[inode] = path
        return path

    def forget(self, inode: int) -> None:
        """Drop the in-memory cache entry for ``inode`` (the persistent DB row
        stays). Called from the FUSE ``forget`` callback so a long-running
        mount doesn't accumulate the whole traversed tree in memory (review
        P2-9 / Gemini Class 5). The next access re-reads the row from the
        ``inodes`` table, so inode numbers remain stable.
        """
        if inode == ROOT_INODE:
            return
        path = self._inode_to_path.pop(inode, None)
        if path is not None:
            _ = self._path_to_inode.pop(path, None)


# ============================================================================
# Mention resolution + miss tracking
# ============================================================================


class _SqlUserResolver:
    """``UserResolver`` backed by a fresh SELECT on the local ``users`` table.

    Hot-path; each call is one indexed lookup. The kernel-cache invariant
    forbids ``notify_store`` when *any* lookup misses, so a wrapped
    ``_MissTracking`` resolver records the miss for the caller.
    """

    def __init__(self, cur: Cursor[TupleRow]) -> None:
        self._cur = cur

    def resolve(self, user_id: UserId) -> UserView | None:
        _ = self._cur.execute("SELECT display_name FROM users WHERE user_id = %s", (user_id.value,))
        row = self._cur.fetchone()
        if row is None:
            return None
        return UserView(user_id=user_id, display_name=str(row[0]))


class _SqlChannelResolver:
    """``ChannelResolver`` backed by a fresh SELECT on the local ``channels`` table."""

    def __init__(self, cur: Cursor[TupleRow]) -> None:
        self._cur = cur

    def resolve(self, channel_id: ChannelId) -> ChannelView | None:
        _ = self._cur.execute(
            "SELECT name, is_im, is_mpim FROM channels WHERE channel_id = %s",
            (channel_id.value,),
        )
        row = self._cur.fetchone()
        if row is None:
            return None
        return ChannelView(
            channel_id=channel_id,
            name="" if row[0] is None else str(row[0]),
            is_im=bool(row[1]),
            is_mpim=bool(row[2]),
        )


@dataclass(slots=True)
class _MissTrackingUserResolver:
    base: UserResolver
    had_miss: bool = False

    def resolve(self, user_id: UserId) -> UserView | None:
        view = self.base.resolve(user_id)
        if view is None or not view.display_name:
            self.had_miss = True
        return view


@dataclass(slots=True)
class _MissTrackingChannelResolver:
    base: ChannelResolver
    had_miss: bool = False

    def resolve(self, channel_id: ChannelId) -> ChannelView | None:
        view = self.base.resolve(channel_id)
        if view is None or not view.name:
            self.had_miss = True
        return view


def resolve_with_miss_tracking(
    body: str,
    users: UserResolver,
    channels: ChannelResolver,
) -> tuple[str, list[str]]:
    """Substitute mentions in ``body`` and report which kinds fell back.

    The second component is the (possibly empty) list of fallback reasons — one
    of :data:`FALLBACK_USER_REASON` / :data:`FALLBACK_CHANNEL_REASON` per kind
    that fell back to a UID/CID literal (per RFC §FUSE read path →
    Unresolved-fallback / kernel-cache invariant: such bytes must NOT be
    ``notify_store``-d into the kernel page cache). A non-empty list means the
    read must skip ``notify_store``; the specific reasons feed the trailer
    decision log so a non-primed read is attributable.
    """
    user_track = _MissTrackingUserResolver(base=users)
    channel_track = _MissTrackingChannelResolver(base=channels)
    resolved = resolve_mentions(body, user_track, channel_track)
    reasons: list[str] = []
    if user_track.had_miss:
        reasons.append(FALLBACK_USER_REASON)
    if channel_track.had_miss:
        reasons.append(FALLBACK_CHANNEL_REASON)
    return resolved, reasons


def sql_resolvers_for(conn: Connection[TupleRow]) -> tuple[UserResolver, ChannelResolver]:
    """Build ``(UserResolver, ChannelResolver)`` backed by ``conn``.

    The resolvers share a single cursor for the lifetime of the call, which
    keeps each ``<@U…>`` substitution to one indexed SELECT.
    """
    cur = conn.cursor()
    return _SqlUserResolver(cur), _SqlChannelResolver(cur)


# ============================================================================
# Staleness trailer — I/O. The pure classifier (``StalenessState``,
# ``staleness_reason``, ``format_trailer``, ``TrailerDecision``) lives in
# ``slack_fuse.projector.trailer``; this wrapper only loads the state values.
# ============================================================================


def fetch_staleness_state(
    conn: Connection[TupleRow],
    stream: str,
) -> StalenessState:
    """SELECT ``connection_state`` + ``stream_caught_up`` for ``stream``.

    The ``stream_caught_up`` row's ``at_offset`` is recorded on the decision
    so the bake-in log can correlate a trailer to the stream head it was
    caught up to. The row's mere existence drives the boolean
    ``initial_catch_up_done_for_stream`` check — the time-based catch-up
    window check was removed (see ``trailer.StalenessState`` docstring).
    """
    with conn.cursor() as cur:
        _ = cur.execute(
            "SELECT last_frame_at, last_slurper_health, last_health_update_at FROM connection_state WHERE id = 1"
        )
        row = cur.fetchone()
        last_frame_at: datetime | None = None
        last_slurper_health = "unknown"
        last_health_update_at: datetime | None = None
        if row is not None:
            last_frame_at = row[0] if isinstance(row[0], datetime) else None
            last_slurper_health = "unknown" if row[1] is None else str(row[1])
            last_health_update_at = row[2] if isinstance(row[2], datetime) else None
        _ = cur.execute("SELECT at_offset FROM stream_caught_up WHERE stream = %s", (stream,))
        caught_up_row = cur.fetchone()
    caught_up_offset: int | None = None
    if caught_up_row is not None:
        caught_up_offset = None if caught_up_row[0] is None else int(caught_up_row[0])
    return StalenessState(
        last_frame_at=last_frame_at,
        last_slurper_health=last_slurper_health,
        last_health_update_at=last_health_update_at,
        initial_catch_up_done_for_stream=caught_up_row is not None,
        caught_up_offset=caught_up_offset,
    )
