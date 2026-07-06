"""Ambient ingestion metadata for the `events.source` envelope.

Every event row records *ambient facts about the ingestion transaction* in the
`source` jsonb column: which task instance wrote it, which cursor Slack
returned, which commit was deployed. The rules (see CLAUDE.md → "Events source
envelope"):

- Slack facts go in `payload`; ambient ingestion facts go in `source`.
- `source` never carries derived state — no running counters, no aggregate
  flags, nothing that answers "what work remains". If computing a field means
  folding rows from multiple events, it belongs in a view, not here.

Two layers compose the envelope:

- `IngestionContext` — task/run-scoped ambient fields, held in a `ContextVar`.
  Trio propagates the context into `to_thread.run_sync` bodies and
  `nursery.start_soon` children, so the sync psycopg write bodies see the
  context of the task that spawned them without explicit threading.
- Per-write explicit fields (`make_source`) — pagination cursors, API metadata,
  socket event timestamps — attached to individual `EventRecord`s by producers.

`compose_source()` merges the two (explicit fields win) plus the current write
span's id (published by `OffsetWriter._run_sync_recorded`); `insert_event`
calls it so every write path — `write_event`, `write_message_or_corrective`,
and the direct `assign_offset`+`insert_event` sites — gets the envelope
without per-site plumbing.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from ulid import ULID

from slack_fuse_server._json import JsonObject, JsonValue

if TYPE_CHECKING:
    from collections.abc import Iterator

_COMMIT_ENV = "GIT_COMMIT"
_IMAGE_DIGEST_ENV = "SLACK_FUSE_SERVER_IMAGE_DIGEST"

#: Allowed `triggered_by` values (documentation; not enforced at runtime).
TRIGGERED_BY_VALUES = frozenset({"startup", "scheduled", "reconnect", "control-surface", "admin-cli"})


def new_ulid() -> str:
    """A fresh ULID string — time-sortable, unique; used for boot/task/run/span ids."""
    return str(ULID())


@dataclass(frozen=True, slots=True)
class IngestionContext:
    """Ambient facts shared by every write of one task (or one run within it).

    `producer` names what is writing (see the CLAUDE.md taxonomy); records can
    override it per write for multi-shape producers (e.g. backfill's
    history/replies/corrective-parent records).
    """

    producer: str
    boot_id: str
    task_id: str
    run_id: str | None = None
    commit: str | None = None
    image_digest: str | None = None
    triggered_by: str | None = None

    def fields(self) -> JsonObject:
        """The context as source-envelope fields, Nones omitted."""
        out: dict[str, JsonValue] = {
            "producer": self.producer,
            "boot_id": self.boot_id,
            "task_id": self.task_id,
        }
        if self.run_id is not None:
            out["run_id"] = self.run_id
        if self.commit is not None:
            out["commit"] = self.commit
        if self.image_digest is not None:
            out["image_digest"] = self.image_digest
        if self.triggered_by is not None:
            out["triggered_by"] = self.triggered_by
        return out


_INGESTION_CTX: ContextVar[IngestionContext | None] = ContextVar("slack_fuse_ingestion_ctx", default=None)

#: Span id of the write operation currently running on this (thread) context.
#: Published by `OffsetWriter._run_sync_recorded` around the sync body so
#: `insert_event` can stamp the span join key without threading it through
#: every record. Lives here (not in `spans.py`) to keep the import DAG acyclic:
#: `spans` imports `offsets`, `offsets` imports this module.
CURRENT_SPAN_ID: ContextVar[str | None] = ContextVar("slack_fuse_current_span_id", default=None)


def current_ingestion_context() -> IngestionContext | None:
    return _INGESTION_CTX.get()


@contextmanager
def ingesting(ctx: IngestionContext) -> Iterator[IngestionContext]:
    """Scope `ctx` as the ambient ingestion context for the current task."""
    token = _INGESTION_CTX.set(ctx)
    try:
        yield ctx
    finally:
        _INGESTION_CTX.reset(token)


@dataclass(frozen=True, slots=True)
class BootContext:
    """Process-level identity, minted once per slurper boot / CLI invocation."""

    boot_id: str
    commit: str | None
    image_digest: str | None

    def task_context(
        self,
        producer: str,
        *,
        run_id: str | None = None,
        triggered_by: str | None = None,
    ) -> IngestionContext:
        """A fresh task-scoped context under this boot (new `task_id`)."""
        return IngestionContext(
            producer=producer,
            boot_id=self.boot_id,
            task_id=new_ulid(),
            run_id=run_id,
            commit=self.commit,
            image_digest=self.image_digest,
            triggered_by=triggered_by,
        )


def process_boot() -> BootContext:
    """Read the process-level identity once (env values are deploy-time facts)."""
    return BootContext(
        boot_id=new_ulid(),
        commit=os.environ.get(_COMMIT_ENV) or None,
        image_digest=os.environ.get(_IMAGE_DIGEST_ENV) or None,
    )


def derived_run_context(
    *,
    producer: str | None = None,
    triggered_by: str | None = None,
    run_id: str | None = None,
    new_run: bool = True,
) -> IngestionContext | None:
    """The current context specialized for one logical run (new `run_id`).

    Returns None when no ambient context is set (bare unit tests) so callers
    can skip the `ingesting()` scope entirely.
    """
    ctx = current_ingestion_context()
    if ctx is None:
        return None
    return replace(
        ctx,
        producer=producer if producer is not None else ctx.producer,
        run_id=run_id if run_id is not None else (new_ulid() if new_run else ctx.run_id),
        triggered_by=triggered_by if triggered_by is not None else ctx.triggered_by,
    )


@contextmanager
def ingesting_run(
    *,
    producer: str | None = None,
    triggered_by: str | None = None,
    run_id: str | None = None,
) -> Iterator[IngestionContext | None]:
    """Scope a per-run derivation of the ambient context (no-op without one)."""
    ctx = derived_run_context(producer=producer, triggered_by=triggered_by, run_id=run_id)
    if ctx is None:
        yield None
        return
    with ingesting(ctx):
        yield ctx


def make_source(  # noqa: PLR0913 - a keyword-only field bag; a dataclass here would just rename the problem.
    *,
    producer: str | None = None,
    slack_cursor: str | None = None,
    prior_cursor: str | None = None,
    page_index: int | None = None,
    has_more: bool | None = None,
    final_page: bool | None = None,
    thread_ts: str | None = None,
    oldest: str | None = None,
    api_endpoint: str | None = None,
    api_latency_ms: int | None = None,
    slack_request_id: str | None = None,
    attempt: int | None = None,
    slack_event_ts: str | None = None,
    day_file: str | None = None,
) -> JsonObject:
    """Explicit per-write source fields; Nones are omitted.

    Ambient fields (producer/boot/task/run/commit/…) come from the
    `IngestionContext` at insert time — pass `producer` here only to override
    the ambient value for this record.
    """
    fields: dict[str, JsonValue] = {
        "producer": producer,
        "slack_cursor": slack_cursor,
        "prior_cursor": prior_cursor,
        "page_index": page_index,
        "has_more": has_more,
        "final_page": final_page,
        "thread_ts": thread_ts,
        "oldest": oldest,
        "api_endpoint": api_endpoint,
        "api_latency_ms": api_latency_ms,
        "slack_request_id": slack_request_id,
        "attempt": attempt,
        "slack_event_ts": slack_event_ts,
        "day_file": day_file,
    }
    return {key: value for key, value in fields.items() if value is not None}


def compose_source(record_source: JsonObject | None) -> JsonObject | None:
    """Merge ambient context + current span id + per-record fields.

    Record fields win over ambient ones (e.g. a per-record `producer`
    override). Returns None when there is nothing to record, so writes outside
    any ingestion scope keep `source IS NULL`.
    """
    ctx = current_ingestion_context()
    merged: dict[str, JsonValue] = {} if ctx is None else dict(ctx.fields())
    span_id = CURRENT_SPAN_ID.get()
    if span_id is not None:
        merged["span_id"] = span_id
    if record_source:
        merged.update(record_source)
    return merged if merged else None
