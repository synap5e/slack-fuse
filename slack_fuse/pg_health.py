"""Track whether the local Postgres is currently reachable.

The slack-fuse-split projector reads every chunk from a local PG. When
that PG goes down — most commonly during a ``game-mode on`` cycle that
stops ``claude-hooks-postgres.service`` (see ``BACKLOG.md``) — the
daemon used to crash with ``psycopg.OperationalError`` on every FUSE
read, get respawned by systemd, and crash again until PG came back.

Instead:

1. We catch ``psycopg.OperationalError`` from FUSE callbacks and flip a
   shared :class:`PgHealth` flag.
2. Subsequent FUSE callbacks check the flag first and fast-fail with
   ``FUSEError(EIO)`` rather than repeatedly trying to acquire a dead
   pool connection.
3. The mount root grows a virtual ``NO_POSTGRES`` file with a plain-text
   explanation of the state and how to recover.
4. A background trio task probes PG with a cheap ``SELECT 1``. The
   moment one succeeds, the flag flips back to up and ``NO_POSTGRES``
   disappears.

The mount itself stays mounted and responsive throughout; only content
reads fail. Recovery is automatic.
"""

from __future__ import annotations

import contextlib
import logging
import threading
from typing import TYPE_CHECKING, Final

import trio

if TYPE_CHECKING:
    from collections.abc import Callable

    from psycopg import Connection
    from psycopg.rows import TupleRow


log = logging.getLogger(__name__)


#: Path (relative to mount root) that surfaces while PG is down.
NO_POSTGRES_NAME: Final = "NO_POSTGRES"

#: Reserved inode for ``/NO_POSTGRES``. Pinned to a value WELL above
#: any realistic allocation so it can never collide with a regular
#: path's inode (the inodes table IDENTITY starts at 2 and grows from
#: there). We need a hard reservation because PG is the thing that's
#: broken when this file matters — we can't allocate via the inode map.
#: ``2**53 - 1`` is the IEEE-754 max safe int; comfortably inside FUSE's
#: u64 ``ino_t`` and easy to spot in logs / strace.
NO_POSTGRES_INODE: Final = (1 << 53) - 1


_NO_POSTGRES_EXPLANATION = b"""\
# Postgres is currently unreachable

The local Postgres database that backs the slack-fuse projector
(`local-postgres.service` on UID 1000, socket
`/run/user/1000/local-postgres/.s.PGSQL.5433`) is not responding.

While this is the case:

- All channel/message reads return EIO.
- The mount itself stays responsive (`readdir`, `getattr`, this file).
- The cluster server is unaffected; events keep accumulating there.
- A background probe checks PG every 5 seconds; this file disappears
  the moment PG is reachable again.

## Common causes

- `game-mode on` stops local-postgres-adjacent services as part of its
  `GAME_MODE_STOP_SERVICES`; downstream consumers (including this
  projector) can be left holding broken connections. See `BACKLOG.md`
  for the FUSE-wedge mechanism this used to trigger before the
  projector learned to tolerate it.
- Manual `systemctl --user stop local-postgres.service`.
- pg_ctl crash; check the postgres log.

## How to investigate

    systemctl --user status local-postgres.service
    journalctl --user -u local-postgres.service --since '5 min ago'
    tail -100 ~/.local/state/local-postgres/postgres.log

If you just ran `game-mode on`, run `game-mode off` to restore.
"""


class PgHealth:
    """Thread-safe PG-availability flag with periodic probing.

    Read from worker threads (FUSE callbacks) via :meth:`is_down`.
    Mutated by ``mark_down`` (called from the same threads on
    ``psycopg.OperationalError``) and by ``mark_up`` (called from
    the probe task in :meth:`run` after a successful ``SELECT 1``).
    """

    def __init__(self, conn_factory: Callable[[], Connection[TupleRow]]) -> None:
        self._conn_factory = conn_factory
        self._lock = threading.Lock()
        self._down = False

    def is_down(self) -> bool:
        with self._lock:
            return self._down

    def mark_down(self, reason: str = "") -> None:
        with self._lock:
            was_up = not self._down
            self._down = True
        if was_up:
            suffix = f" ({reason})" if reason else ""
            log.warning("pg_health: marking PG down%s", suffix)

    def mark_up(self) -> None:
        with self._lock:
            was_down = self._down
            self._down = False
        if was_down:
            log.info("pg_health: PG reachable again; NO_POSTGRES will hide on next readdir")

    @property
    def explanation_bytes(self) -> bytes:
        return _NO_POSTGRES_EXPLANATION

    async def run(
        self,
        *,
        down_probe_interval_s: float = 5.0,
        up_probe_interval_s: float = 60.0,
    ) -> None:
        """Background task: probe PG to flip the flag back to up.

        When down, probes every ``down_probe_interval_s`` (default 5s)
        so the mount recovers shortly after PG comes back. When up,
        probes every ``up_probe_interval_s`` (default 60s) just to
        catch PG dying via something OTHER than a FUSE-callback
        OperationalError (e.g. PG dies overnight while idle).
        """
        while True:
            interval = down_probe_interval_s if self.is_down() else up_probe_interval_s
            await trio.sleep(interval)
            if await self._probe_once():
                if self.is_down():
                    self.mark_up()
            elif not self.is_down():
                # Up→down via the background probe (rare; usually a FUSE
                # callback catches it first).
                self.mark_down(reason="background probe failed")

    async def _probe_once(self) -> bool:
        try:
            conn = await trio.to_thread.run_sync(self._conn_factory)
        except Exception as exc:  # noqa: BLE001 — any failure means still-down
            log.debug("pg_health: connect failed: %s", exc)
            return False
        try:
            await trio.to_thread.run_sync(lambda: _probe_select_one(conn))
        except Exception as exc:  # noqa: BLE001
            log.debug("pg_health: SELECT 1 failed: %s", exc)
            return False
        finally:
            with contextlib.suppress(Exception):
                await trio.to_thread.run_sync(conn.close)
        return True


def _probe_select_one(conn: Connection[TupleRow]) -> None:
    with conn.cursor() as cur:
        _ = cur.execute("SELECT 1")
        _ = cur.fetchone()
