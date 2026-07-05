"""Backfill-run lifecycle events.

The lifecycle is event-sourced on ``backfill-run:<channel_id>`` streams. These
helpers keep payload shapes typed and keep writers from rebuilding JSON objects
at each call site.
"""

from __future__ import annotations

from typing import cast

from pydantic import BaseModel

from slack_fuse_server._json import JsonObject
from slack_fuse_server.backfill.types import (
    BackfillPageCommittedPayload,
    BackfillRunFinishedPayload,
    BackfillRunOutcome,
    BackfillRunStartedPayload,
    BackfillRunTrigger,
    MessageBatch,
)
from slack_fuse_server.slurper.ingestion import current_ingestion_context, new_ulid
from slack_fuse_server.slurper.offsets import EventRecord

BACKFILL_RUN_STREAM_PREFIX = "backfill-run:"


def backfill_run_stream(channel_id: str) -> str:
    return f"{BACKFILL_RUN_STREAM_PREFIX}{channel_id}"


def new_backfill_run_id() -> str:
    ctx = current_ingestion_context()
    if ctx is not None and ctx.run_id is not None:
        return ctx.run_id
    return new_ulid()


def resolve_trigger(explicit: BackfillRunTrigger | None, *, default: BackfillRunTrigger) -> BackfillRunTrigger:
    if explicit is not None:
        return explicit
    ctx = current_ingestion_context()
    if ctx is None or ctx.triggered_by is None:
        return default
    try:
        return BackfillRunTrigger(ctx.triggered_by)
    except ValueError:
        return default


def run_started_record(
    *,
    channel_id: str,
    run_id: str,
    triggered_by: BackfillRunTrigger,
    params: JsonObject | None = None,
) -> EventRecord:
    payload = BackfillRunStartedPayload(
        run_id=run_id,
        params={} if params is None else params,
        triggered_by=triggered_by,
    )
    return EventRecord(
        stream=backfill_run_stream(channel_id),
        kind="backfill_run_started",
        ts=None,
        payload=_model_payload(payload),
        dedup=True,
    )


def page_committed_record(
    *,
    batch: MessageBatch,
    run_id: str,
    messages_written: int,
) -> EventRecord:
    payload = BackfillPageCommittedPayload(
        run_id=run_id,
        page_index=batch.origin.page_index,
        has_more=batch.origin.has_more,
        final_page=batch.origin.final_page,
        slack_cursor=batch.origin.slack_cursor,
        messages_written=messages_written,
        kind=batch.kind,
        thread_ts=batch.origin.thread_ts,
    )
    return EventRecord(
        stream=backfill_run_stream(batch.channel_id),
        kind="backfill_page_committed",
        ts=None,
        payload=_model_payload(payload),
        dedup=True,
    )


def run_finished_record(  # noqa: PLR0913 - mirrors the persisted terminal payload shape.
    *,
    channel_id: str,
    run_id: str,
    outcome: BackfillRunOutcome,
    messages_written_total: int,
    elapsed_s: float,
    error_reason: str | None = None,
) -> EventRecord:
    payload = BackfillRunFinishedPayload(
        run_id=run_id,
        outcome=outcome,
        messages_written_total=messages_written_total,
        elapsed_s=elapsed_s,
        error_reason=error_reason,
    )
    return EventRecord(
        stream=backfill_run_stream(channel_id),
        kind="backfill_run_finished",
        ts=None,
        payload=_model_payload(payload),
        dedup=True,
    )


def started_params(*, since_ts: float | None = None, extra: JsonObject | None = None) -> JsonObject:
    params: JsonObject = {} if extra is None else dict(extra)
    if since_ts is not None:
        params["since_ts"] = since_ts
    return params


def _model_payload(model: BaseModel) -> JsonObject:
    return cast(JsonObject, model.model_dump(mode="json", exclude_none=True))
