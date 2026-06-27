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
