# pyright: reportPrivateUsage=false
"""Transport-neutral dispatch for Slack Events API envelopes.

The pure translation helpers still live in ``slurper.socket`` for API
compatibility with existing callers; the connection runner itself delegates
all routing here.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import httpx
from pydantic import ValidationError

from slack_fuse.models import CHANNEL_LIST_EVENT_TYPES, Channel, EventsApiPayload, SocketEventPayload
from slack_fuse_server._json import JsonObject
from slack_fuse_server.slack_events.types import (
    DispatchErrorCode,
    DispatchPermanentError,
    DispatchTransientError,
    SlackEventSource,
)
from slack_fuse_server.slurper.api import RateLimitedError, SlackAPIError, SlackClient, Validated
from slack_fuse_server.slurper.channels import ensure_channel_added_from_info
from slack_fuse_server.slurper.health import HealthEmitter, HealthKind
from slack_fuse_server.slurper.ingestion import dispatching_slack_event, make_source
from slack_fuse_server.slurper.limiters import SlurperLimiters
from slack_fuse_server.slurper.offsets import PG_TIMEOUT_EXCEPTIONS, EventRecord, OffsetWriter
from slack_fuse_server.slurper.socket import (
    _ARCHIVE_EVENTS,
    _CREATE_EVENTS,
    _MEMBER_EVENTS,
    _MEMBERSHIP_LOST_EVENTS,
    _RAW_CHANNEL_LIST_EVENTS,
    _RENAME_EVENTS,
    _TOKEN_REVOKED_EVENT,
    _UNARCHIVE_EVENTS,
    _event_ts,
    _member_event_write,
    channel_added_write,
    raw_channel_list_write,
    translate_message_event,
)
from slack_fuse_server.slurper.spans import run_sync_with_span
from slack_fuse_server.slurper.users import apply_team_join_event, apply_user_change_event

if TYPE_CHECKING:
    from collections.abc import Callable

    from slack_fuse_server.slurper.spans import SpanRecorder

log = logging.getLogger(__name__)


class SlackEventDispatcher:
    """Route a validated Slack envelope without owning transport lifecycle."""

    def __init__(  # noqa: PLR0913, PLR0917 - dependencies are explicit transport-neutral capabilities.
        self,
        writer: OffsetWriter,
        client: SlackClient,
        self_user_id: str,
        limiters: SlurperLimiters,
        health: HealthEmitter,
        on_self_join: Callable[[str], bool] | None = None,
    ) -> None:
        if not self_user_id:
            msg = "SlackEventDispatcher requires self_user_id"
            raise ValueError(msg)
        self._writer = writer
        self._client = client
        self._self_user_id = self_user_id
        self._limiters = limiters
        self._health = health
        self._on_self_join = on_self_join

    async def dispatch(
        self,
        payload: EventsApiPayload,
        raw_event: JsonObject,
        source_ctx: SlackEventSource,
        span: SpanRecorder | None = None,
    ) -> None:
        """Dispatch one inner event, raising only typed expected failures."""
        event = payload.event
        if event is None:
            raise DispatchPermanentError(DispatchErrorCode.MALFORMED_PAYLOAD)
        event_id = source_ctx.event_id or payload.event_id
        if not event_id:
            raise DispatchPermanentError(DispatchErrorCode.MALFORMED_PAYLOAD)
        with dispatching_slack_event(event_id, source_ctx.transport):
            await self._dispatch_event(event, raw_event, span=span)

    async def _dispatch_event(
        self,
        event: SocketEventPayload,
        raw_event: JsonObject,
        *,
        span: SpanRecorder | None,
    ) -> None:
        if event.type == "message":
            record = translate_message_event(event, raw_event)
            if record is not None:
                await self._write_event(record, span=span)
            elif span is not None:
                span.mark_skipped()
            return
        if event.type == _TOKEN_REVOKED_EVENT:
            await self._handle_tokens_revoked(raw_event, span=span)
            return
        if event.type == "team_join":
            await self._apply_team_join(event, raw_event)
            return
        if event.type == "user_change":
            await self._apply_user_change(event)
            return
        if event.type in CHANNEL_LIST_EVENT_TYPES:
            wrote = await self._handle_structural_event(event, raw_event, span=span)
            if not wrote and span is not None:
                span.mark_skipped()
            return
        log.debug("slack-events: ignoring event type %s", event.type)
        if span is not None:
            span.mark_skipped()

    async def _write_event(self, record: EventRecord, *, span: SpanRecorder | None) -> bool:
        try:
            offset = await self._writer.write_event(record, span=span)
        except PG_TIMEOUT_EXCEPTIONS as exc:
            if span is not None:
                span.mark_timeout(type(exc).__name__)
            raise DispatchTransientError(DispatchErrorCode.PG_TIMEOUT) from exc
        if span is not None:
            span.set("events_written", 1 if offset is not None else 0)
            if offset is not None:
                span.set("offset", offset)
        return offset is not None

    async def _handle_tokens_revoked(self, raw_event: JsonObject, *, span: SpanRecorder | None) -> None:
        await self._write_event(
            EventRecord(
                stream="slurper-health",
                kind=_TOKEN_REVOKED_EVENT,
                ts=None,
                payload=raw_event,
                dedup=True,
            ),
            span=span,
        )
        try:
            await self._health.emit(HealthKind.AUTH_TOKEN_INVALID, {"reason": _TOKEN_REVOKED_EVENT})
        except PG_TIMEOUT_EXCEPTIONS as exc:
            if span is not None:
                span.mark_timeout(type(exc).__name__)
            raise DispatchTransientError(DispatchErrorCode.PG_TIMEOUT) from exc

    async def _apply_team_join(self, event: SocketEventPayload, raw_event: JsonObject) -> None:
        try:
            await apply_team_join_event(self._writer, event, raw_event)
        except (ValidationError, ValueError, *PG_TIMEOUT_EXCEPTIONS) as exc:
            raise DispatchTransientError(DispatchErrorCode.TEAM_JOIN_APPLY_FAILED) from exc

    async def _apply_user_change(self, event: SocketEventPayload) -> None:
        try:
            await apply_user_change_event(self._writer, self._client, event, self._limiters)
        except (httpx.HTTPError, SlackAPIError, ValueError, *PG_TIMEOUT_EXCEPTIONS) as exc:
            raise DispatchTransientError(DispatchErrorCode.USER_CHANGE_APPLY_FAILED) from exc

    async def _handle_structural_event(
        self,
        event: SocketEventPayload,
        raw_event: JsonObject,
        *,
        span: SpanRecorder | None,
    ) -> bool:
        if event.type in _MEMBER_EVENTS:
            return await self._handle_member_event(event, raw_event, span=span)
        raw_write = raw_channel_list_write(event, raw_event)
        wrote_any = False
        if raw_write is not None:
            wrote_any = await self._write_event(raw_write, span=span)
        if event.type in _RAW_CHANNEL_LIST_EVENTS:
            return wrote_any
        channel_id = event.channel
        if not channel_id:
            return wrote_any
        write = await self._build_structural_write(event, channel_id, span=span)
        if write is None:
            return wrote_any
        return await self._write_event(write, span=span) or wrote_any

    async def _handle_member_event(
        self,
        event: SocketEventPayload,
        raw_event: JsonObject,
        *,
        span: SpanRecorder | None,
    ) -> bool:
        membership = _member_event_write(event, raw_event)
        if membership is None:
            return False
        user_id = membership.payload.get("user_id")
        is_self = isinstance(user_id, str) and user_id == self._self_user_id
        wrote_any = False
        self_join_error: DispatchTransientError | None = None
        if is_self and event.type == "member_joined_channel":
            try:
                wrote_any = await self._handle_self_join(event, raw_event, span=span)
            except DispatchTransientError as exc:
                # Preserve the membership fact for Socket Mode while ensuring
                # the durable HTTP inbox retries the missing self-join work.
                self_join_error = exc
        wrote_any = await self._write_event(membership, span=span) or wrote_any
        if self_join_error is not None:
            raise self_join_error
        if is_self and event.type == "member_left_channel":
            wrote_any = await self._write_event(
                EventRecord(
                    stream="channel-list",
                    kind="channel_member_changed",
                    ts=None,
                    payload={"channel_id": event.channel, "is_member": False},
                    source=make_source(slack_event_ts=_event_ts(raw_event)),
                ),
                span=span,
            ) or wrote_any
        return wrote_any

    async def _handle_self_join(
        self,
        event: SocketEventPayload,
        raw_event: JsonObject,
        *,
        span: SpanRecorder | None,
    ) -> bool:
        channel_id = event.channel
        validated = await self._fetch_channel(channel_id, span=span)
        try:
            inserted = await ensure_channel_added_from_info(
                self._writer,
                validated,
                source=make_source(triggered_by="self-join", slack_event_ts=_event_ts(raw_event)),
            )
        except PG_TIMEOUT_EXCEPTIONS as exc:
            if span is not None:
                span.mark_timeout(type(exc).__name__)
            raise DispatchTransientError(DispatchErrorCode.PG_TIMEOUT) from exc
        if span is not None:
            span.set("channel_added", inserted)
        self._queue_self_join_backfill(channel_id)
        return inserted

    def _queue_self_join_backfill(self, channel_id: str) -> None:
        if self._on_self_join is None:
            return
        try:
            accepted = self._on_self_join(channel_id)
        except Exception as exc:
            log.error(
                "slack-events: self-join backfill callback failed channel_id=%s exception_type=%s",
                channel_id,
                type(exc).__name__,
            )
            raise DispatchTransientError(DispatchErrorCode.UNKNOWN_TRANSIENT) from exc
        if not accepted:
            log.warning("slack-events: self-join backfill queue busy channel_id=%s", channel_id)
            raise DispatchTransientError(DispatchErrorCode.UNKNOWN_TRANSIENT)

    async def _build_structural_write(
        self,
        event: SocketEventPayload,
        channel_id: str,
        *,
        span: SpanRecorder | None,
    ) -> EventRecord | None:
        if event.type in _MEMBERSHIP_LOST_EVENTS:
            return EventRecord(
                stream="channel-list",
                kind="channel_member_changed",
                ts=None,
                payload={"channel_id": channel_id, "is_member": False},
            )
        if event.type in _ARCHIVE_EVENTS:
            return EventRecord(
                stream="channel-list", kind="channel_archived", ts=None, payload={"channel_id": channel_id}
            )
        if event.type in _UNARCHIVE_EVENTS:
            return EventRecord(
                stream="channel-list", kind="channel_unarchived", ts=None, payload={"channel_id": channel_id}
            )
        validated = await self._fetch_channel(channel_id, span=span)
        if event.type in _CREATE_EVENTS:
            return channel_added_write(validated.raw)
        if event.type in _RENAME_EVENTS:
            return EventRecord(
                stream="channel-list",
                kind="channel_renamed",
                ts=None,
                payload={"channel_id": channel_id, "new_name": validated.model.name},
            )
        return None

    async def _fetch_channel(self, channel_id: str, *, span: SpanRecorder | None) -> Validated[Channel]:
        try:
            return await run_sync_with_span(
                lambda: self._client.get_channel_info(channel_id),
                limiter=self._limiters.slack_api,
                span=span,
            )
        except RateLimitedError as exc:
            raise DispatchTransientError(DispatchErrorCode.SLACK_RATE_LIMITED) from exc
        except (SlackAPIError, httpx.HTTPError) as exc:
            raise DispatchTransientError(DispatchErrorCode.CONVERSATIONS_INFO_FAILED) from exc


__all__ = [
    "DispatchErrorCode",
    "DispatchPermanentError",
    "DispatchTransientError",
    "SlackEventDispatcher",
    "SlackEventSource",
]
