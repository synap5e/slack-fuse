"""Events-replay render of "what was originally posted, before any edits/deletes".

Backs ``GET /originals/{channel_id}?from=&to=`` and the ghost
``channel.original.md`` ghost file at ``/<conv>/<slug>/<YYYY-MM>/<DD>/``
on the FUSE client. No client-side projection or capture: every read
replays the events table inside one SQL query and renders fresh.

For each unique ``message_ts`` in the day range:

  * The ORIGINAL is the first ``message`` event with that ts (events table
    is gap-free per stream, so "first" means the lowest ``id`` for that
    ``(stream, kind='message', payload->>ts)`` combination).
  * Any later ``message_changed`` event for that ts records that an edit
    happened — we surface it as an inline ``edited HH:MM`` annotation
    under the original text, NOT the new content (this is the "original"
    view, so the new content is irrelevant here; it lives in channel.md).
  * A ``message_deleted`` event for that ts marks the message struck-out
    with a ``[deleted HH:MM]`` annotation.

Returns markdown with unresolved ``<@U…>`` / ``<#C…>`` placeholders, the
same shape ``chunks.content_md`` uses; the FUSE client resolves them via
its local users/channels tables before serving (same pipeline as
``channel.md``). Server is timezone-agnostic — the caller passes a UTC
epoch range, derived from its own local-tz day boundary.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from slack_fuse.models import Message
from slack_fuse_render.render import render_message_structural

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow


@dataclass(slots=True)
class _MessageHistoryBuilder:
    """Mutable per-ts builder while we walk events; finalised to _MessageHistory."""

    original_message: Message | None = None
    edited_at_epochs: list[float] | None = None
    deleted_at_epoch: float | None = None

    def add_edit(self, epoch: float) -> None:
        if self.edited_at_epochs is None:
            self.edited_at_epochs = []
        self.edited_at_epochs.append(epoch)


@dataclass(frozen=True, slots=True)
class _MessageHistory:
    """Per-ts replay state assembled from the events stream."""

    ts: Decimal
    original_message: Message
    edited_at_epochs: tuple[float, ...]
    deleted_at_epoch: float | None


def _ts_for_event(kind: str, payload: dict[str, object]) -> Decimal | None:
    """Extract the *target message* ts from an event payload.

    All three event kinds reference a single message by ts; this function
    pulls it from the right field depending on the event shape.
    """
    raw: object = None
    if kind == "message_changed":
        inner: object = payload.get("message")
        if not isinstance(inner, dict):
            return None
        raw = inner.get("ts")  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType, reportUnknownVariableType]
    elif kind == "message_deleted":
        raw = payload.get("deleted_ts")
    else:  # message
        raw = payload.get("ts")
    if not isinstance(raw, str):
        return None
    try:
        return Decimal(raw)
    except (ValueError, ArithmeticError):
        return None


def _replay_history(  # noqa: C901 - linear walk over event kinds; splitting hurts readability.
    conn: Connection[TupleRow],
    channel_id: str,
    *,
    from_epoch: float,
    to_epoch: float,
) -> Iterable[_MessageHistory]:
    """Walk events for ``channel:{channel_id}`` in [from, to), grouped by ts.

    Order:
      1. ``message`` events first per ts (insertion order = lowest id).
      2. Subsequent ``message_changed`` / ``message_deleted`` events appended.

    A ts that has no ``message`` event (only a ``message_changed`` because we
    never saw the original — possible if a backfill catch-up missed it) is
    skipped: there's no "original" to show. This matches the spec — the file
    documents what WAS posted, not what arrived later as an edit.
    """
    stream = f"channel:{channel_id}"
    with conn.cursor() as cur:
        _ = cur.execute(
            """
            SELECT id, kind, payload, created_at
            FROM events
            WHERE stream = %s
              AND kind IN ('message', 'message_changed', 'message_deleted')
              AND ts IS NOT NULL
              AND ts::numeric >= %s
              AND ts::numeric < %s
            ORDER BY ts::numeric, id
            """,
            (stream, Decimal(str(from_epoch)), Decimal(str(to_epoch))),
        )
        rows = cur.fetchall()

    # Group by ts. Python dicts preserve insertion order; the SQL ORDER BY
    # makes (ts) groups arrive in chronological order, so iterating the dict
    # values yields per-message histories in posting order.
    by_ts: dict[Decimal, _MessageHistoryBuilder] = {}
    for row in rows:
        _row_id, kind_raw, payload_raw, created_at_raw = row
        if not isinstance(kind_raw, str) or not isinstance(payload_raw, dict):
            continue
        if not isinstance(created_at_raw, datetime):
            continue
        kind: str = kind_raw
        payload: dict[str, object] = payload_raw  # pyright: ignore[reportUnknownVariableType]
        created_at: datetime = created_at_raw
        ts_dec = _ts_for_event(kind, payload)
        if ts_dec is None:
            continue

        builder = by_ts.setdefault(ts_dec, _MessageHistoryBuilder())
        if kind == "message" and builder.original_message is None:
            try:
                message = Message.model_validate(payload)
            except Exception:  # noqa: BLE001 — a malformed message is unrenderable; skip
                continue
            # Mirror channel.md: thread replies live in thread.md, not
            # channel.md. Skipping them here keeps the originals view
            # aligned with what ``ls`` shows next door. ``_is_thread_reply``
            # in apply.py uses the same rule: thread_ts set and pointing
            # somewhere other than ts itself.
            if message.thread_ts is not None and message.thread_ts != message.ts:
                continue
            builder.original_message = message
        elif kind == "message_changed":
            builder.add_edit(created_at.timestamp())
        elif kind == "message_deleted":
            # Tombstone time. ``created_at`` is the cluster's ingest time,
            # which is when the deletion EVENT landed — close enough to
            # actual deletion for human-readable display.
            builder.deleted_at_epoch = created_at.timestamp()

    for ts_dec, builder in by_ts.items():
        if builder.original_message is None:
            continue
        yield _MessageHistory(
            ts=ts_dec,
            original_message=builder.original_message,
            edited_at_epochs=tuple(builder.edited_at_epochs or ()),
            deleted_at_epoch=builder.deleted_at_epoch,
        )


def _format_hhmm(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=UTC).strftime("%H:%M UTC")


def _render_with_markers(history: _MessageHistory) -> str:
    """Render one message's original text with edit/delete annotations.

    Edits get a one-line ``<sub>`` annotation below the body; deletions wrap
    the entire rendered block in a strikethrough marker so the original text
    remains forensically readable.
    """
    body = render_message_structural(history.original_message)
    annotations: list[str] = []
    for epoch in history.edited_at_epochs:
        annotations.append(f"<sub>edited {_format_hhmm(epoch)}</sub>")
    if history.deleted_at_epoch is not None:
        annotations.append(f"<sub>**deleted {_format_hhmm(history.deleted_at_epoch)}**</sub>")
    block = body
    if annotations:
        block = body.rstrip() + "\n" + "\n".join(annotations) + "\n"
    if history.deleted_at_epoch is not None:
        # ``~~text~~`` reads naturally enough in plaintext; wrapping the whole
        # rendered block keeps the original mention placeholders intact so the
        # client-side resolver still substitutes display names cleanly.
        block = "~~\n" + block + "~~\n"
    return block


def render_originals_for_range(
    conn: Connection[TupleRow],
    channel_id: str,
    *,
    from_epoch: float,
    to_epoch: float,
) -> bytes:
    """Render the originals view for ``channel_id`` over the UTC-epoch range.

    Output is markdown with unresolved ``<@U…>`` / ``<#C…>`` placeholders,
    matching the ``chunks.content_md`` shape so the FUSE client's existing
    resolver pipeline applies without modification.
    """
    histories = list(_replay_history(conn, channel_id, from_epoch=from_epoch, to_epoch=to_epoch))
    if not histories:
        return b""
    blocks = [_render_with_markers(h) for h in histories]
    return ("\n".join(blocks) + "\n").encode()
