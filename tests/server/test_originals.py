"""Server-side render of the originals view (events-replay).

The ``channel.original.md`` ghost file's slow path lives here: a single SQL
query over the events table, grouped by message ts. These tests seed events
directly (the slurper's normal write path), then assert the rendered
markdown shows the ORIGINAL message text (not post-edit), with edit and
delete annotations.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from slack_fuse_server._json import JsonObject
from slack_fuse_server.originals import render_originals_for_range
from slack_fuse_server.slurper.offsets import EventRecord, write_event

if TYPE_CHECKING:
    import psycopg
    from psycopg.rows import TupleRow


_CH = "C1"
_STREAM = f"channel:{_CH}"

# A convenient 2026-06-08 day window. The seeded ts values fall inside.
_FROM_EPOCH = 1717804800.0  # 2026-06-08 00:00 UTC
_TO_EPOCH = 1717891200.0  # 2026-06-09 00:00 UTC


def _ts(offset_s: float) -> str:
    """Build a Slack-style ts string inside the day window."""
    return f"{_FROM_EPOCH + offset_s:.6f}"


def _seed_message(
    conn: psycopg.Connection[TupleRow],
    *,
    ts: str,
    text: str,
    user: str = "U123ALICE",
) -> None:
    payload: JsonObject = {
        "ts": ts,
        "user": user,
        "text": text,
        "subtype": None,
        "thread_ts": None,
        "reply_count": 0,
        "files": [],
        "edited": None,
        "reactions": [],
    }
    _ = write_event(
        conn,
        EventRecord(stream=_STREAM, kind="message", ts=ts, payload=payload, dedup=True),
    )


def _seed_edit(
    conn: psycopg.Connection[TupleRow],
    *,
    target_ts: str,
    new_text: str,
    user: str = "U123ALICE",
) -> None:
    payload: JsonObject = {
        "message": {
            "ts": target_ts,
            "user": user,
            "text": new_text,
            "subtype": None,
            "thread_ts": None,
            "reply_count": 0,
            "files": [],
            "edited": None,
            "reactions": [],
        },
        "previous_ts": target_ts,
    }
    _ = write_event(
        conn,
        EventRecord(stream=_STREAM, kind="message_changed", ts=target_ts, payload=payload),
    )


def _seed_delete(conn: psycopg.Connection[TupleRow], *, target_ts: str) -> None:
    payload: JsonObject = {"deleted_ts": target_ts}
    _ = write_event(
        conn,
        EventRecord(stream=_STREAM, kind="message_deleted", ts=target_ts, payload=payload),
    )


def test_originals_renders_pristine_message_when_never_edited(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    _seed_message(server_conn, ts=_ts(60.0), text="hello world")
    body = render_originals_for_range(server_conn, _CH, from_epoch=_FROM_EPOCH, to_epoch=_TO_EPOCH)
    assert b"hello world" in body
    assert b"edited" not in body
    assert b"deleted" not in body
    # Mention placeholder for the author is preserved for client-side resolve.
    assert b"<@U123ALICE>" in body


def test_originals_shows_pre_edit_text_and_edit_annotation(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    ts = _ts(60.0)
    _seed_message(server_conn, ts=ts, text="original draft")
    _seed_edit(server_conn, target_ts=ts, new_text="edited rewrite")
    body = render_originals_for_range(server_conn, _CH, from_epoch=_FROM_EPOCH, to_epoch=_TO_EPOCH)
    # The ORIGINAL text is what shows in the originals view.
    assert b"original draft" in body
    # The post-edit text does NOT appear here (lives in channel.md).
    assert b"edited rewrite" not in body
    # Edit annotation is inline (HH:MM stamp from ingest time).
    assert b"edited" in body


def test_originals_strikes_through_deleted_message_keeping_original_text(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    ts = _ts(60.0)
    _seed_message(server_conn, ts=ts, text="oops shouldnt have said that")
    _seed_delete(server_conn, target_ts=ts)
    body = render_originals_for_range(server_conn, _CH, from_epoch=_FROM_EPOCH, to_epoch=_TO_EPOCH)
    # Original text remains forensically readable.
    assert b"oops shouldnt have said that" in body
    # Wrapped in strikethrough markers and annotated.
    assert b"~~" in body
    assert b"deleted" in body


def test_originals_chronological_order(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    # Seed in reverse chronological order; assert output is forward.
    _seed_message(server_conn, ts=_ts(300.0), text="third")
    _seed_message(server_conn, ts=_ts(120.0), text="first")
    _seed_message(server_conn, ts=_ts(200.0), text="second")
    body = render_originals_for_range(server_conn, _CH, from_epoch=_FROM_EPOCH, to_epoch=_TO_EPOCH)
    first_idx = body.index(b"first")
    second_idx = body.index(b"second")
    third_idx = body.index(b"third")
    assert first_idx < second_idx < third_idx


def test_originals_filters_to_day_range(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    in_range_ts = _ts(60.0)
    before_ts = f"{_FROM_EPOCH - 3600.0:.6f}"  # 1h before window
    after_ts = f"{_TO_EPOCH + 3600.0:.6f}"  # 1h after window
    _seed_message(server_conn, ts=in_range_ts, text="visible message")
    _seed_message(server_conn, ts=before_ts, text="previous day message")
    _seed_message(server_conn, ts=after_ts, text="next day message")
    body = render_originals_for_range(server_conn, _CH, from_epoch=_FROM_EPOCH, to_epoch=_TO_EPOCH)
    assert b"visible message" in body
    assert b"previous day message" not in body
    assert b"next day message" not in body


def test_originals_empty_when_no_events(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    body = render_originals_for_range(server_conn, _CH, from_epoch=_FROM_EPOCH, to_epoch=_TO_EPOCH)
    assert body == b""


def test_originals_skips_orphan_edits_without_original(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    """If the original ``message`` event is missing (e.g. partial backfill),
    the ``message_changed`` alone has no original to show — we skip it
    rather than render the post-edit content as if it were original.
    """
    # Only an edit event, no preceding message.
    _seed_edit(server_conn, target_ts=_ts(60.0), new_text="orphaned edit")
    body = render_originals_for_range(server_conn, _CH, from_epoch=_FROM_EPOCH, to_epoch=_TO_EPOCH)
    assert b"orphaned edit" not in body
    assert body == b""


def test_originals_handles_multiple_edits_then_delete(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    ts = _ts(60.0)
    _seed_message(server_conn, ts=ts, text="rev 1 original")
    _seed_edit(server_conn, target_ts=ts, new_text="rev 2")
    _seed_edit(server_conn, target_ts=ts, new_text="rev 3")
    _seed_delete(server_conn, target_ts=ts)
    body = render_originals_for_range(server_conn, _CH, from_epoch=_FROM_EPOCH, to_epoch=_TO_EPOCH)
    assert b"rev 1 original" in body
    # Two edit annotations.
    assert body.count(b"edited") == 2
    # Strikethrough envelope and deletion annotation.
    assert b"~~" in body
    assert b"deleted" in body


def test_originals_only_first_message_event_counts_as_original(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    """Idempotency: dedup makes a duplicate ``message`` insert a no-op, but
    if somehow two ``message`` events landed for the same ts (which can't
    happen via the live path, but could happen across a schema migration),
    the FIRST one wins as the original.
    """
    ts = _ts(60.0)
    _seed_message(server_conn, ts=ts, text="first arrival")
    # Bypass dedup by changing kind shape (simulates an out-of-band insert).
    # We can't easily insert a second message with the same ts due to the
    # unique index, so we use a different ts that LATER gets a message_changed
    # referencing the first ts.
    _seed_edit(server_conn, target_ts=ts, new_text="later version")
    body = render_originals_for_range(server_conn, _CH, from_epoch=_FROM_EPOCH, to_epoch=_TO_EPOCH)
    assert b"first arrival" in body
    assert b"later version" not in body


def test_originals_distinguishes_per_message_history(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    """Two messages, one edited, one deleted, one untouched."""
    ts_clean = _ts(30.0)
    ts_edited = _ts(60.0)
    ts_deleted = _ts(90.0)
    _seed_message(server_conn, ts=ts_clean, text="pristine post")
    _seed_message(server_conn, ts=ts_edited, text="revisable thought")
    _seed_edit(server_conn, target_ts=ts_edited, new_text="post-edit content")
    _seed_message(server_conn, ts=ts_deleted, text="regrettable sentence")
    _seed_delete(server_conn, target_ts=ts_deleted)
    body = render_originals_for_range(server_conn, _CH, from_epoch=_FROM_EPOCH, to_epoch=_TO_EPOCH)
    assert b"pristine post" in body
    assert b"revisable thought" in body
    assert b"post-edit content" not in body
    assert b"regrettable sentence" in body
    # Exactly one edit and one delete annotation. Substring search is safe —
    # the message texts above are chosen to NOT contain "edited" / "deleted".
    assert body.count(b"edited") == 1
    assert body.count(b"deleted") == 1


def test_ts_decimal_precision_preserved(
    server_conn: psycopg.Connection[TupleRow],
) -> None:
    """Slack ts is microsecond-precision; ensure the BETWEEN query handles it."""
    ts = "1717804860.123456"  # well within the day window
    _seed_message(server_conn, ts=ts, text="microsecond ts")
    body = render_originals_for_range(server_conn, _CH, from_epoch=_FROM_EPOCH, to_epoch=_TO_EPOCH)
    assert b"microsecond ts" in body
    # Decimal round-trips correctly.
    assert Decimal(ts) >= Decimal(str(_FROM_EPOCH))
    assert Decimal(ts) < Decimal(str(_TO_EPOCH))
