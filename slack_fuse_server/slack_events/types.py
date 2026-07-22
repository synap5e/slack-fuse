"""Typed transport metadata and sanitized dispatch failures."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Literal


class DispatchErrorCode(StrEnum):
    PG_TIMEOUT = "pg_timeout"
    CONVERSATIONS_INFO_FAILED = "conversations_info_failed"
    TEAM_JOIN_APPLY_FAILED = "team_join_apply_failed"
    USER_CHANGE_APPLY_FAILED = "user_change_apply_failed"
    SLACK_RATE_LIMITED = "slack_rate_limited"
    UNKNOWN_TRANSIENT = "unknown_transient"
    MALFORMED_PAYLOAD = "malformed_payload"
    UNKNOWN_PERMANENT = "unknown_permanent"


class DispatchTransientError(Exception):
    def __init__(self, code: DispatchErrorCode) -> None:
        self.code = code
        super().__init__(code.value)


class DispatchPermanentError(Exception):
    def __init__(self, code: DispatchErrorCode) -> None:
        self.code = code
        super().__init__(code.value)


@dataclass(frozen=True, slots=True)
class SlackEventSource:
    transport: Literal["socket", "http"]
    event_id: str
    api_endpoint: str = "events_api"
