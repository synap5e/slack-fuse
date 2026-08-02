"""Typed search.messages channel-total helper."""

from __future__ import annotations

import httpx
import pytest

from slack_fuse_server.search_messages import SearchMessagesError, search_channel_message_total


def test_search_channel_message_total_uses_real_wire_shape() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/search.messages"
        assert request.url.params["query"] == "in:#proj-cloud"
        assert request.url.params["count"] == "1"
        return httpx.Response(200, json={"ok": True, "messages": {"total": 32361, "paging": {"total": 32361}}})

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = search_channel_message_total(client, "proj-cloud")

    assert result.total == 32361
    assert result.approximate is False


def test_search_channel_message_total_marks_disagreeing_large_total_approximate() -> None:
    transport = httpx.MockTransport(
        lambda _request: httpx.Response(
            200,
            json={"ok": True, "messages": {"total": 12000, "paging": {"total": 10000}}},
        )
    )
    with httpx.Client(transport=transport) as client:
        result = search_channel_message_total(client, "large-channel")
    assert result.total == 12000
    assert result.approximate is True


def test_search_channel_message_total_rejects_slack_error() -> None:
    transport = httpx.MockTransport(
        lambda _request: httpx.Response(200, json={"ok": False, "error": "not_allowed_token_type"})
    )
    with (
        httpx.Client(transport=transport) as client,
        pytest.raises(SearchMessagesError, match="not_allowed_token_type"),
    ):
        _ = search_channel_message_total(client, "general")
