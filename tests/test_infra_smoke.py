"""Smoke tests for the cross-cutting test infrastructure (Sprint 2F seeds).

Proves the fake Slack transport, the synthetic-event generator, and the FUSE
harness skeleton are importable and behave, so downstream tracks can build on
them.
"""

from __future__ import annotations

from typing import cast

import httpx
import pyfuse3

from slack_fuse.models import (
    ConversationsHistoryResponse,
    ConversationsListResponse,
    FilesInfoResponse,
    UsersListResponse,
)
from slack_fuse_server.wire.frames import EventFrame, FrameAdapter
from tests._fuse_harness import capture_readdir, fake_request_context
from tests._synthetic_events import channel_message_events, channel_reply_events


def test_fake_slack_conversations_list(fake_slack_http: httpx.Client) -> None:
    data = ConversationsListResponse.model_validate(fake_slack_http.get("/conversations.list").json())
    assert data.ok
    assert any(c.name == "general" for c in data.channels)


def test_fake_slack_fixtures_validate_against_models(fake_slack_http: httpx.Client) -> None:
    assert UsersListResponse.model_validate(fake_slack_http.get("/users.list").json()).ok
    assert ConversationsHistoryResponse.model_validate(fake_slack_http.get("/conversations.history").json()).ok
    assert FilesInfoResponse.model_validate(fake_slack_http.get("/files.info").json()).ok


def test_fake_slack_unknown_method_is_not_ok(fake_slack_http: httpx.Client) -> None:
    body = fake_slack_http.get("/does.not.exist").json()
    assert body["ok"] is False
    assert body["error"] == "fake_not_implemented"


def test_synthetic_message_events_are_deterministic_and_framable() -> None:
    events = list(channel_message_events("C1", 5))
    assert [e.offset for e in events] == [1, 2, 3, 4, 5]
    assert all(e.stream == "channel:C1" and e.kind == "message" for e in events)
    # Deterministic across runs.
    assert [e.ts for e in events] == [e.ts for e in channel_message_events("C1", 5)]
    for event in events:
        restored = FrameAdapter.validate_python(event.to_frame().model_dump())
        assert isinstance(restored, EventFrame)


def test_synthetic_reply_events_carry_thread_ts() -> None:
    replies = list(channel_reply_events("C1", thread_ts="1700000000.000000", count=3))
    assert all(r.payload["thread_ts"] == "1700000000.000000" for r in replies)


def test_fuse_harness_capture_readdir() -> None:
    ctx = fake_request_context()
    assert ctx.uid == 1000
    attr = pyfuse3.EntryAttributes()
    token = cast("pyfuse3.ReaddirToken", None)
    with capture_readdir() as captured:
        ok = pyfuse3.readdir_reply(token, b"channel.md", attr, 1)
    assert ok is True
    assert captured == [(b"channel.md", attr, 1)]
