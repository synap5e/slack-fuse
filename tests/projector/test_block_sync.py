"""Client block-list reconciliation from the server blocked_channels SSOT."""

from __future__ import annotations

import functools
from typing import TYPE_CHECKING

import httpx
import psycopg
import pytest
import trio

import slack_fuse.projector.block_sync as block_sync_module
from slack_fuse.projector.block_sync import apply_blocked_channel_sync

if TYPE_CHECKING:
    from psycopg.rows import TupleRow

    from slack_fuse.projector.reconnecting_conn import TupleConnection
    from tests.projector.conftest import ClientConnFactory


def _seed_channel(
    conn: psycopg.Connection[TupleRow],
    channel_id: str,
    *,
    tier: str = "hot",
    tier_source: str = "auto",
    is_member: bool = True,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO channels (channel_id, name, is_im, is_mpim, is_member, is_archived, "
            "tier, tier_source, subscribed) VALUES (%s, %s, FALSE, FALSE, %s, FALSE, %s, %s, %s)",
            (channel_id, channel_id.lower(), is_member, tier, tier_source, tier != "blocked"),
        )


def _channel_row(conn: psycopg.Connection[TupleRow], channel_id: str) -> tuple[str, str, bool]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT tier, tier_source, subscribed FROM channels WHERE channel_id = %s",
            (channel_id,),
        )
        row = cur.fetchone()
    assert row is not None
    return str(row[0]), str(row[1]), bool(row[2])


def test_block_sync_forces_server_block_to_blocked_manual(
    client_conn: psycopg.Connection[TupleRow],
) -> None:
    _seed_channel(client_conn, "CBLOCK")

    apply_blocked_channel_sync(client_conn, {"CBLOCK"})

    assert _channel_row(client_conn, "CBLOCK") == ("blocked", "manual", False)
    with client_conn.cursor() as cur:
        cur.execute("SELECT channel_id FROM server_block_sync")
        assert cur.fetchall() == [("CBLOCK",)]


def test_block_sync_demotes_synced_block_to_auto_on_unblock(
    client_conn: psycopg.Connection[TupleRow],
) -> None:
    _seed_channel(client_conn, "CBLOCK")
    apply_blocked_channel_sync(client_conn, {"CBLOCK"})

    apply_blocked_channel_sync(client_conn, set())

    assert _channel_row(client_conn, "CBLOCK") == ("hot", "auto", True)
    with client_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM server_block_sync")
        row = cur.fetchone()
    assert row is not None and row[0] == 0


def test_block_sync_does_not_demote_untracked_local_manual_block(
    client_conn: psycopg.Connection[TupleRow],
) -> None:
    _seed_channel(client_conn, "CLOCAL", tier="blocked", tier_source="manual")

    apply_blocked_channel_sync(client_conn, set())

    assert _channel_row(client_conn, "CLOCAL") == ("blocked", "manual", False)


def test_apply_returns_newly_subscribed_channel_ids(
    client_conn: psycopg.Connection[TupleRow],
) -> None:
    """The WSClient consumes this set to send SubscribeFrame dynamically —
    without it, unblocking via ``_control/blocked_channels`` required a full
    mount restart before the applier/subscription woke up (2026-07-16)."""
    _seed_channel(client_conn, "CA")
    _seed_channel(client_conn, "CB")
    _seed_channel(client_conn, "CC")

    # Initial sync: all three visible rows become blocked.
    transitions_1 = apply_blocked_channel_sync(client_conn, {"CA", "CB", "CC"})
    assert transitions_1.newly_subscribed == frozenset()
    assert transitions_1.newly_blocked == frozenset({"CA", "CB", "CC"})

    # Second sync: CA and CB unblocked. Those are the transitions.
    transitions_2 = apply_blocked_channel_sync(client_conn, {"CC"})
    assert transitions_2.newly_subscribed == frozenset({"CA", "CB"})
    assert transitions_2.newly_blocked == frozenset()
    assert _channel_row(client_conn, "CA")[0] == "hot"
    assert _channel_row(client_conn, "CB")[0] == "hot"
    assert _channel_row(client_conn, "CC")[0] == "blocked"

    # Third sync: nothing changes. Empty transition set.
    transitions_3 = apply_blocked_channel_sync(client_conn, {"CC"})
    assert transitions_3.newly_subscribed == frozenset()
    assert transitions_3.newly_blocked == frozenset()


def test_apply_reports_reblocked_previously_synced_channel(
    client_conn: psycopg.Connection[TupleRow],
) -> None:
    """A channel made visible between sync cycles must invalidate again."""
    _seed_channel(client_conn, "CREBLOCK")
    first = apply_blocked_channel_sync(client_conn, {"CREBLOCK"})
    assert first.newly_blocked == frozenset({"CREBLOCK"})

    with client_conn.cursor() as cur:
        cur.execute(
            "UPDATE channels SET tier = 'hot', tier_source = 'manual', subscribed = TRUE "
            "WHERE channel_id = 'CREBLOCK'"
        )

    second = apply_blocked_channel_sync(client_conn, {"CREBLOCK"})

    assert second.newly_blocked == frozenset({"CREBLOCK"})
    assert second.newly_subscribed == frozenset()
    assert _channel_row(client_conn, "CREBLOCK") == ("blocked", "manual", False)


def test_apply_omits_channels_that_stay_blocked_by_local_manual(
    client_conn: psycopg.Connection[TupleRow],
) -> None:
    """A row still marked blocked/manual by the local operator after the server
    unblock isn't a "transition" — subscribing it would fight the operator."""
    _seed_channel(client_conn, "CLOCAL", tier="blocked", tier_source="manual")

    transitions = apply_blocked_channel_sync(client_conn, set())

    assert transitions.newly_subscribed == frozenset()
    assert transitions.newly_blocked == frozenset()
    assert _channel_row(client_conn, "CLOCAL")[0] == "blocked"


def test_finding_14_operator_hot_pin_survives_server_block_unblock(
    client_conn: psycopg.Connection[TupleRow],
) -> None:
    """Regression for FINDING-14 (2026-07-17): an operator's manual tier='hot'
    pin must survive a server block/unblock cycle. Pre-fix, unblock reset
    tier_source to 'auto' — the pin was silently lost."""
    _seed_channel(client_conn, "CPIN", tier="hot", tier_source="manual")

    # Server blocks CPIN — snapshot the pre-block (tier, tier_source) into
    # server_block_sync.
    _ = apply_blocked_channel_sync(client_conn, {"CPIN"})
    assert _channel_row(client_conn, "CPIN") == ("blocked", "manual", False)

    # Server unblocks CPIN — must restore ('hot', 'manual'), not reset to auto.
    transitions = apply_blocked_channel_sync(client_conn, set())

    assert transitions.newly_subscribed == frozenset({"CPIN"})
    assert transitions.newly_blocked == frozenset()
    assert _channel_row(client_conn, "CPIN") == ("hot", "manual", True), (
        "operator pin lost — pre-fix behavior; must be preserved."
    )


def test_finding_14_first_time_seen_channel_falls_back_to_auto(
    client_conn: psycopg.Connection[TupleRow],
) -> None:
    """A channel first seen via server block (no local row before) has no
    prior tier to restore — fall back to the auto default on unblock."""
    _seed_channel(client_conn, "CFRESH")  # tier='hot', tier_source='auto'

    _ = apply_blocked_channel_sync(client_conn, {"CFRESH"})
    transitions = apply_blocked_channel_sync(client_conn, set())

    # Auto row restored (would_be tier for public+member) is 'hot'.
    assert transitions.newly_subscribed == frozenset({"CFRESH"})
    assert transitions.newly_blocked == frozenset()
    assert _channel_row(client_conn, "CFRESH") == ("hot", "auto", True)


@pytest.mark.trio
async def test_block_sync_reconcile_recovers_from_committed_but_unacked_unblock(
    client_conn_factory: ClientConnFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    block_conn = client_conn_factory()
    verify_conn = client_conn_factory()
    _seed_channel(block_conn, "CAMBIGUOUS")
    _ = apply_blocked_channel_sync(block_conn, {"CAMBIGUOUS"})

    calls = 0
    real_sync_once = block_sync_module.sync_blocked_channels_once

    def commit_then_lose_ack(
        http_client: httpx.Client,
        base_http_url: str,
        conn: TupleConnection,
        *,
        shared_secret: str | None = None,
    ) -> block_sync_module.VisibilityChanges | None:
        nonlocal calls
        calls += 1
        changes = real_sync_once(
            http_client,
            base_http_url,
            conn,
            shared_secret=shared_secret,
        )
        if calls == 1:
            # The transaction above committed the unblock and deleted the
            # server_block_sync row; only the client-side acknowledgement is
            # lost. The next cycle therefore has an empty transition set.
            raise psycopg.OperationalError("fault injection: COMMIT acknowledgement lost")
        return changes

    monkeypatch.setattr(block_sync_module, "sync_blocked_channels_once", commit_then_lose_ack)

    def no_server_blocks(
        _http_client: httpx.Client,
        _base_http_url: str,
        *,
        shared_secret: str | None = None,
        timeout_s: float = 10.0,
    ) -> tuple[int, dict[str, object]]:
        del shared_secret, timeout_s
        return 200, {"blocked": []}

    monkeypatch.setattr(block_sync_module, "get_blocked_channels", no_server_blocks)

    class _FakeWSClient:
        def __init__(self) -> None:
            self.reconciled: list[frozenset[str]] = []

        async def reconcile_subscriptions(self, desired: frozenset[str]) -> None:
            self.reconciled.append(desired)

    fake_ws = _FakeWSClient()
    transitions: list[frozenset[str]] = []
    reconciled = trio.Event()

    async def on_newly_subscribed(ids: frozenset[str]) -> None:
        await trio.lowlevel.checkpoint()
        transitions.append(ids)

    async def reconcile(desired: frozenset[str]) -> None:
        await fake_ws.reconcile_subscriptions(desired)
        reconciled.set()

    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            functools.partial(
                block_sync_module.sync_blocked_channels_periodically,
                httpx.Client,
                "http://server.invalid",
                lambda: block_conn,
                interval_s=0.01,
                on_newly_subscribed=on_newly_subscribed,
                on_reconcile_subscriptions=reconcile,
            )
        )
        with trio.fail_after(5):
            await reconciled.wait()
        nursery.cancel_scope.cancel()

    with verify_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM server_block_sync WHERE channel_id = 'CAMBIGUOUS'")
        assert cur.fetchone() == (0,)
    assert transitions == []
    assert fake_ws.reconciled == [frozenset({"CAMBIGUOUS"})]
    assert calls == 2
