"""Periodic ``conversations.info`` refresh for channel metadata drift.

The webhook flow (Socket Mode → ``conversations.info`` →
``channel_added``) already captures full channel data for any channel
created after the slurper started. This task handles two cases the
webhook can't:

1. **Legacy channels** — channels we have ``channel_added`` events for
   from before 2026-06-27, when the slurper switched to raw-persistence.
   Those payloads were ``model_dump``-derived (lossy), so they're
   missing fields the model doesn't declare (e.g. ``created``).
2. **Drift** — channel metadata can change after the initial
   ``channel_added``: topic / purpose updates, num_members, the bot's
   ``is_member`` status. The webhook does cover renames and archive
   flips, but not all drift surfaces as an event.

Strategy: walk every known channel, hit ``conversations.info``, diff
against the latest known payload, emit a ``channel_info_refreshed``
event ONLY when something changed. In steady state on a stable
workspace this is near-zero events per cycle; on first run after
deploy it backfills every legacy channel.

Rate budget: ``conversations.info`` is Tier 3 (~50/min). 482 channels
at 1.5s/each = ~12 min per cycle. Default interval is 6h so we use
roughly 5% of the API budget per channel.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import httpx
import trio
from psycopg import Connection

from slack_fuse_server._json import JsonObject
from slack_fuse_server.slurper.api import ChannelNotFoundError, SlackAPIError, SlackClient
from slack_fuse_server.slurper.limiters import SlurperLimiters
from slack_fuse_server.slurper.offsets import EventRecord, OffsetWriter, assign_offset, insert_event
from slack_fuse_server.slurper.supervisor import TaskSupervisor, phase

if TYPE_CHECKING:
    import psycopg
    from psycopg.rows import TupleRow

log = logging.getLogger(__name__)

#: Refresh cadence. 6 hours strikes a balance: legacy backfill completes
#: within hours of deploy, drift catches up well before the next
#: weekly review, and we stay well under the Tier 3 rate ceiling.
DEFAULT_INTERVAL_S = 6 * 60 * 60.0

#: Per-channel sleep between ``conversations.info`` calls. Tier 3 budget
#: is 50/min; 1.5s/call = 40/min, leaving 20% headroom for other slurper
#: traffic (socket-mode enrichment etc).
_PER_CHANNEL_SLEEP_S = 1.5

_CHANNEL_LIST_STREAM = "channel-list"


async def refresh_channels_periodically(
    writer: OffsetWriter,
    client: SlackClient,
    limiters: SlurperLimiters,
    supervisor: TaskSupervisor | None = None,
    *,
    interval_s: float = DEFAULT_INTERVAL_S,
) -> None:
    """Trio task: refresh every known channel in a loop.

    Supervisor responsibility — the caller spawns this in the main
    nursery and decides whether to restart on exception. We catch
    everything inside the cycle so a single bad channel doesn't take
    the loop down.
    """
    while True:
        try:
            await _refresh_all_once(writer, client, limiters, supervisor)
        except Exception:
            log.exception("refresh: cycle failed; retrying after interval")
        if supervisor is not None:
            supervisor.declare("refresh", "sleeping_until", deadline_s=None)
        await trio.sleep(interval_s)


async def _refresh_all_once(
    writer: OffsetWriter,
    client: SlackClient,
    limiters: SlurperLimiters,
    supervisor: TaskSupervisor | None = None,
    *,
    task_name: str = "refresh",
) -> None:
    """One full pass: walk known channels, refresh each."""
    if supervisor is None:
        channel_ids = await writer.run_read(_list_known_channel_ids, limiter=limiters.admin_read)
    else:
        async with phase(supervisor, task_name, "listing_channels", deadline_s=30):
            channel_ids = await writer.run_read(_list_known_channel_ids, limiter=limiters.admin_read)
    log.info("refresh: starting cycle for %d channel(s)", len(channel_ids))
    refreshed = 0
    unchanged = 0
    not_found = 0
    errors = 0
    for channel_id in channel_ids:
        try:
            if supervisor is None:
                changed = await _refresh_one(writer, client, channel_id, limiters)
            else:
                async with phase(
                    supervisor,
                    task_name,
                    "refreshing_channel",
                    details={"channel_id": channel_id},
                    deadline_s=10,
                ):
                    changed = await _refresh_one(writer, client, channel_id, limiters)
        except ChannelNotFoundError:
            # Channel left / archived-and-purged — skip cleanly.
            not_found += 1
        except (SlackAPIError, httpx.HTTPError):
            log.warning("refresh: API error for %s", channel_id, exc_info=True)
            errors += 1
        else:
            if changed:
                refreshed += 1
            else:
                unchanged += 1
        await trio.sleep(_PER_CHANNEL_SLEEP_S)
    log.info(
        "refresh: cycle complete refreshed=%d unchanged=%d not_found=%d errors=%d",
        refreshed,
        unchanged,
        not_found,
        errors,
    )


def _list_known_channel_ids(conn: psycopg.Connection[TupleRow]) -> list[str]:
    """Distinct channel ids ever seen in ``channel-list`` events.

    Pulls from raw payloads so this works against both the pre-refactor
    ``channel_added`` payloads (lossy ``model_dump``) and the
    post-refactor raw ones — the ``id`` field exists in both shapes.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT payload->>'id'
            FROM events
            LEFT JOIN blocked_channels ON blocked_channels.channel_id = payload->>'id'
            WHERE stream = %s
              AND kind IN ('channel_added', 'channel_info_refreshed')
              AND payload ? 'id'
              AND blocked_channels.channel_id IS NULL
            ORDER BY payload->>'id'
            """,
            (_CHANNEL_LIST_STREAM,),
        )
        return [str(row[0]) for row in cur.fetchall() if row[0] is not None]


async def _refresh_one(
    writer: OffsetWriter,
    client: SlackClient,
    channel_id: str,
    limiters: SlurperLimiters,
) -> bool:
    """Refresh one channel. Returns True iff a ``channel_info_refreshed``
    event was written (i.e. the new payload differed from the latest
    known one)."""
    fresh = await trio.to_thread.run_sync(lambda: client.get_channel_info(channel_id), limiter=limiters.slack_api)
    return await writer.run_transaction(lambda conn: _maybe_write_refresh_sync(conn, channel_id, fresh.raw))


def _maybe_write_refresh_sync(
    conn: Connection[TupleRow],
    channel_id: str,
    fresh_raw: JsonObject,
) -> bool:
    """Compare ``fresh_raw`` to the most recent ``channel_added`` /
    ``channel_info_refreshed`` payload for this channel. Emit if they
    differ; no-op otherwise."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT payload FROM events
            WHERE stream = %s
              AND kind IN ('channel_added', 'channel_info_refreshed')
              AND payload->>'id' = %s
            ORDER BY id DESC
            LIMIT 1
            """,
            (_CHANNEL_LIST_STREAM, channel_id),
        )
        row = cur.fetchone()
        previous = row[0] if row is not None else None
        if isinstance(previous, dict) and previous == fresh_raw:
            return False
        offset = assign_offset(cur, _CHANNEL_LIST_STREAM)
        record = EventRecord(
            stream=_CHANNEL_LIST_STREAM,
            kind="channel_info_refreshed",
            ts=None,
            payload=fresh_raw,
        )
        insert_event(cur, offset, record)
        return True


# Public so external callers (one-shot CLI, tests) can run a single
# cycle without spawning the periodic loop.
async def refresh_channels_once(writer: OffsetWriter, client: SlackClient, limiters: SlurperLimiters) -> None:
    """One full pass; intended for a one-shot CLI or initial backfill."""
    await _refresh_all_once(writer, client, limiters)


# ===================================================================
# HTTP-triggered refresh: consumer + trigger
# ===================================================================


class RefreshTrigger:
    """Rendezvous-channel trigger used by the refresh HTTP endpoints to
    ask the in-process consumer for a sweep.

    Two modes:
    - ``request()`` (no arg): full workspace sweep
    - ``request_channel(channel_id)``: just one channel

    Rendezvous mode (``max_buffer_size=0``) gives "one in flight at a
    time" for free: ``send_nowait`` only succeeds if the consumer is
    currently parked in ``recv.receive()`` — if a sweep is already
    running, the call returns False and the endpoint responds 409.

    A per-channel refresh and a workspace refresh compete for the same
    slot, which is intentional: the workspace sweep is sequential and
    will pick the channel up anyway, so layering a per-channel request
    on top of a running workspace one would be redundant.
    """

    def __init__(self) -> None:
        # Item is ``None`` for workspace sweep, or a channel id string.
        self._send, self._recv = trio.open_memory_channel[str | None](max_buffer_size=0)

    def request(self) -> bool:
        return self._try_send(None)

    def request_channel(self, channel_id: str) -> bool:
        return self._try_send(channel_id)

    def _try_send(self, item: str | None) -> bool:
        try:
            self._send.send_nowait(item)
        except trio.WouldBlock:
            return False
        return True

    async def __aexit__(self, *_args: object) -> None:
        await self._send.aclose()
        await self._recv.aclose()

    async def consume(
        self,
        writer: OffsetWriter,
        client: SlackClient,
        limiters: SlurperLimiters,
        supervisor: TaskSupervisor | None = None,
        *,
        interval_s: float = DEFAULT_INTERVAL_S,
    ) -> None:
        """Trio task: drain refresh requests one at a time.

        Spawned in the main nursery alongside the periodic task. The two
        coexist cleanly — periodic ticks at 6h, HTTP triggers fire ad
        hoc; both share ``_refresh_all_once`` / ``_refresh_one``.
        """
        while True:
            if supervisor is not None:
                supervisor.declare("refresh-trigger", "waiting_for_trigger", deadline_s=None)
            try:
                item = await self._recv.receive()
            except trio.EndOfChannel:
                return
            try:
                details: JsonObject = {} if item is None else {"channel_id": item}
                if supervisor is None:
                    if item is None:
                        await _refresh_all_once(writer, client, limiters)
                    else:
                        await _refresh_one(writer, client, item, limiters)
                else:
                    async with phase(
                        supervisor,
                        "refresh-trigger",
                        "running",
                        details=details,
                        deadline_s=interval_s * 0.5,
                    ):
                        if item is None:
                            await _refresh_all_once(writer, client, limiters)
                        else:
                            await _refresh_one(writer, client, item, limiters)
            except ChannelNotFoundError:
                log.info("refresh: HTTP-triggered run for %s: channel not found", item)
            except Exception:
                log.exception("refresh: HTTP-triggered run failed")
