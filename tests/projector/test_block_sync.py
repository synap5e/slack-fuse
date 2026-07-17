"""Client block-list reconciliation from the server blocked_channels SSOT."""

from __future__ import annotations

from typing import TYPE_CHECKING

from slack_fuse.projector.block_sync import apply_blocked_channel_sync

if TYPE_CHECKING:
    import psycopg
    from psycopg.rows import TupleRow


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

    # Initial sync: all three blocked. No transitions yet (they weren't
    # previously synced), so the returned set is empty.
    transitions_1 = apply_blocked_channel_sync(client_conn, {"CA", "CB", "CC"})
    assert transitions_1 == frozenset()

    # Second sync: CA and CB unblocked. Those are the transitions.
    transitions_2 = apply_blocked_channel_sync(client_conn, {"CC"})
    assert transitions_2 == frozenset({"CA", "CB"})
    assert _channel_row(client_conn, "CA")[0] == "hot"
    assert _channel_row(client_conn, "CB")[0] == "hot"
    assert _channel_row(client_conn, "CC")[0] == "blocked"

    # Third sync: nothing changes. Empty transition set.
    transitions_3 = apply_blocked_channel_sync(client_conn, {"CC"})
    assert transitions_3 == frozenset()


def test_apply_omits_channels_that_stay_blocked_by_local_manual(
    client_conn: psycopg.Connection[TupleRow],
) -> None:
    """A row still marked blocked/manual by the local operator after the server
    unblock isn't a "transition" — subscribing it would fight the operator."""
    _seed_channel(client_conn, "CLOCAL", tier="blocked", tier_source="manual")

    transitions = apply_blocked_channel_sync(client_conn, set())

    assert transitions == frozenset()
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

    assert transitions == frozenset({"CPIN"})
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
    assert transitions == frozenset({"CFRESH"})
    assert _channel_row(client_conn, "CFRESH") == ("hot", "auto", True)
