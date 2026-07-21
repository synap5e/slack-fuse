"""The `Backfiller` protocol and its return/progress dataclasses.

Per RFC §Backfill → Backfill as a protocol. Two v1 implementations land later:
`LegacyCacheBackfiller` (reads `~/.cache/slack-fuse/` JSON, Sprint 2A) and
`SlackApiBackfiller` (paginates `conversations.history` / `.replies`,
Sprint 1A). Both produce `message` items the slurper writes via an
`INSERT ... ON CONFLICT DO NOTHING` keyed by the `events_message_dedup`
partial unique index, so re-running either is a no-op.

Typed IDs (`ChannelId`) come from `slack_fuse_render`, the shared leaf library
that owns the project's value types; `Message` is the existing Slack domain
model. Sprint 1 may lift a server-local copy of `Message` when it ports
`SlackClient`; until then the existing model is the contract.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

from slack_fuse.models import Message
from slack_fuse_render import ChannelId
from slack_fuse_server._json import JsonObject
from slack_fuse_server.slurper.api import Validated
from slack_fuse_server.slurper.offsets import EventRecord


class BackfillAbortReason(StrEnum):
    """Why a per-channel backfill stopped early (drives `backfill_aborted`)."""

    EXCEEDED_DEFAULT_LIMIT = "exceeded_default_limit"
    OPERATOR_BLOCKED = "operator_blocked"
    CHANNEL_NOT_FOUND = "channel_not_found"


class BackfillRunTrigger(StrEnum):
    """What asked for a backfill-run stream lifecycle."""

    STARTUP = "startup"
    ADMIN_CLI = "admin-cli"
    CONTROL_SURFACE = "control-surface"
    REFILL_WINDOW = "refill-window"
    PROBE_REFILL = "probe-refill"
    SELF_JOIN = "self-join"


class BackfillRunOutcome(StrEnum):
    """Terminal outcome for ``backfill_run_finished``."""

    COMPLETED = "completed"
    SIZE_CAPPED = "size_capped"
    CHANNEL_NOT_FOUND = "channel_not_found"
    OPERATOR_BLOCKED = "operator_blocked"
    FATAL_ERROR = "fatal_error"


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class BackfillRunStartedPayload(_FrozenModel):
    run_id: str
    params: JsonObject = Field(default_factory=dict)
    triggered_by: BackfillRunTrigger


class BackfillPageCommittedPayload(_FrozenModel):
    run_id: str
    page_index: int
    has_more: bool
    final_page: bool
    slack_cursor: str
    messages_written: int
    kind: Literal["history_page", "replies_page"]
    thread_ts: str | None = None


class BackfillRunFinishedPayload(_FrozenModel):
    run_id: str
    outcome: BackfillRunOutcome
    messages_written_total: int
    elapsed_s: float
    error_reason: str | None = None


@dataclass(frozen=True, slots=True)
class BackfillProgress:
    """In-flight per-channel progress. Mirrors the `/metrics` backfill
    `in_progress` shape (RFC §/metrics)."""

    channel_id: ChannelId
    messages_so_far: int


@dataclass(frozen=True, slots=True)
class BackfillResult:
    """Outcome of backfilling one channel.

    `aborted` is True when a size threshold tripped (`abort_reason` set), in
    which case the channel's events table holds only the truncated head; live
    events continue to flow. Carries the per-channel metrics the slurper emits
    (message count, events written, wall-clock elapsed).
    """

    channel_id: ChannelId
    messages: int
    events_written: int
    elapsed_s: float
    aborted: bool = False
    abort_reason: BackfillAbortReason | None = None


@dataclass(frozen=True, slots=True)
class MessageBatchOrigin:
    """Diagnostic source coordinates for one source page.

    ``slack_cursor`` is Slack's next cursor for API pages, or a source-local
    page token for legacy-cache batches. It is copied into the
    ``backfill_page_committed`` payload as a lifecycle fact.
    """

    channel_id: str
    thread_ts: str | None
    page_index: int
    slack_cursor: str
    has_more: bool = False
    final_page: bool = True


@dataclass(frozen=True, slots=True)
class MessageBatch:
    """One source page of messages ready for atomic persistence."""

    kind: Literal["history_page", "replies_page"]
    channel_id: str
    records: tuple[EventRecord, ...]
    origin: MessageBatchOrigin


class Backfiller(Protocol):
    """A source of historical `message` events for the slurper.

    The slurper runs implementations in priority order (legacy cache first,
    Slack API second), draining `channels_to_backfill()` and, for each,
    `messages_pages_for_channel()`. Writes are idempotent on `(stream,
    slack_ts)`.
    """

    @property
    def name(self) -> str:
        """Stable identifier: `'legacy-cache'` or `'slack-api'`."""
        ...

    def channels_to_backfill(self) -> AsyncIterator[ChannelId]:
        """Yield the channels this backfiller can supply history for."""
        ...

    def messages_for_channel(
        self,
        channel_id: ChannelId,
        since_ts: float | None = None,
    ) -> AsyncIterator[Validated[Message]]:
        """Yield historical messages for `channel_id`, oldest first.

        Each yield is the lossless ``Validated`` pair: the raw wire / cache
        dict for persistence, plus the validated ``Message`` model for any
        in-process logic (thread-parent detection, since-filter).

        ``since_ts=None`` means from the oldest available; a value means
        only messages newer than it (used to gap-fill after the legacy
        cache's tip).
        """
        ...

    def messages_pages_for_channel(
        self,
        channel_id: ChannelId,
        since_ts: float | None = None,
    ) -> AsyncIterator[MessageBatch]:
        """Yield one ``MessageBatch`` per source page.

        API backfill yields all Slack history pages first, then one replies
        batch per Slack replies page for each discovered thread parent. The
        legacy cache adapter treats one day JSON file as one atomic source page.
        """
        ...
