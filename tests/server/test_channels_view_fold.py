"""Latest-per-column channel-list view fold."""

from __future__ import annotations

from typing import TYPE_CHECKING

from psycopg.types.json import Jsonb

from slack_fuse_server._json import JsonObject

if TYPE_CHECKING:
    import psycopg
    from psycopg.rows import TupleRow

_CHANNEL_ID = "C_FOLD"


def _write(
    conn: psycopg.Connection[TupleRow],
    offset: int,
    kind: str,
    payload: JsonObject,
) -> None:
    """Insert one channel-list fact at an explicit view-ordering offset."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO events (stream, offset_in_stream, kind, ts, payload)
            VALUES ('channel-list', %s, %s, NULL, %s)
            """,
            (offset, kind, Jsonb(payload)),
        )


def _added_payload(*, is_member: bool = False) -> JsonObject:
    return {
        "id": _CHANNEL_ID,
        "name": "foo",
        "is_im": False,
        "is_mpim": False,
        "is_member": is_member,
        "is_archived": False,
        "im_user_id": "U_IM",
        "topic": "old",
        "purpose": "baseline",
        "num_members": 7,
    }


def _row(conn: psycopg.Connection[TupleRow]) -> tuple[object, ...]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT channel_id, name, is_im, is_mpim, is_member, is_archived,
                   im_user_id, topic, purpose, num_members
            FROM channels
            WHERE channel_id = %s
            """,
            (_CHANNEL_ID,),
        )
        row = cur.fetchone()
    assert row is not None
    return tuple(row)


def test_channel_added_baseline_is_unchanged(server_conn: psycopg.Connection[TupleRow]) -> None:
    _write(server_conn, 1, "channel_added", _added_payload())

    assert _row(server_conn) == (
        _CHANNEL_ID,
        "foo",
        False,
        False,
        False,
        False,
        "U_IM",
        "old",
        "baseline",
        7,
    )


def test_channel_info_refreshed_overrides_full_payload_columns(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    _write(server_conn, 1, "channel_added", _added_payload())
    _write(
        server_conn,
        2,
        "channel_info_refreshed",
        {"id": _CHANNEL_ID, "name": "bar", "is_member": True, "topic": "new"},
    )

    row = _row(server_conn)
    assert row[1] == "bar"
    assert row[4] is True
    assert row[7] == "new"


def test_channel_renamed_overrides_name_only(server_conn: psycopg.Connection[TupleRow]) -> None:
    _write(server_conn, 1, "channel_added", _added_payload())
    _write(server_conn, 100, "channel_renamed", {"channel_id": _CHANNEL_ID, "new_name": "baz"})

    row = _row(server_conn)
    assert row[1] == "baz"
    assert row[2:] == (False, False, False, False, "U_IM", "old", "baseline", 7)


def test_channel_archived_then_unarchived(server_conn: psycopg.Connection[TupleRow]) -> None:
    _write(server_conn, 1, "channel_added", _added_payload())
    _write(server_conn, 100, "channel_archived", {"channel_id": _CHANNEL_ID})
    assert _row(server_conn)[5] is True

    _write(server_conn, 101, "channel_unarchived", {"channel_id": _CHANNEL_ID})
    assert _row(server_conn)[5] is False


def test_channel_member_changed_overrides_self_membership(server_conn: psycopg.Connection[TupleRow]) -> None:
    _write(server_conn, 1, "channel_added", _added_payload(is_member=True))
    _write(server_conn, 100, "channel_member_changed", {"channel_id": _CHANNEL_ID, "is_member": False})

    assert _row(server_conn)[4] is False


def test_per_user_member_events_are_ignored(server_conn: psycopg.Connection[TupleRow]) -> None:
    _write(server_conn, 1, "channel_added", _added_payload())
    baseline = _row(server_conn)
    _write(
        server_conn,
        100,
        "channel_member_joined",
        {"channel_id": _CHANNEL_ID, "user_id": "U_OTHER"},
    )
    _write(
        server_conn,
        101,
        "channel_member_left",
        {"channel_id": _CHANNEL_ID, "user_id": "U_OTHER"},
    )

    assert _row(server_conn) == baseline


def test_latest_event_is_folded_per_column_not_per_row(server_conn: psycopg.Connection[TupleRow]) -> None:
    _write(server_conn, 1, "channel_added", _added_payload(is_member=True))
    _write(server_conn, 100, "channel_renamed", {"channel_id": _CHANNEL_ID, "new_name": "baz"})
    _write(server_conn, 101, "channel_member_changed", {"channel_id": _CHANNEL_ID, "is_member": False})

    row = _row(server_conn)
    assert row[1] == "baz"
    assert row[4] is False


def test_refresh_name_after_rename_wins(server_conn: psycopg.Connection[TupleRow]) -> None:
    _write(server_conn, 1, "channel_added", _added_payload())
    _write(server_conn, 100, "channel_renamed", {"channel_id": _CHANNEL_ID, "new_name": "mid"})
    _write(server_conn, 101, "channel_info_refreshed", {"id": _CHANNEL_ID, "name": "final"})

    assert _row(server_conn)[1] == "final"
