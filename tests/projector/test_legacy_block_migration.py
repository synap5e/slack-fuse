# pyright: reportPrivateUsage=false
"""Legacy always_blocked_channel_ids startup migration."""

from __future__ import annotations

import json
import logging

import httpx

from slack_fuse.__main__ import _migrate_legacy_always_blocked


def test_legacy_always_blocked_migration_posts_idempotently() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"channel_id": json.loads(request.content)["channel_id"]})

    client = httpx.Client(transport=httpx.MockTransport(handler))

    _migrate_legacy_always_blocked(
        client,
        "http://srv",
        frozenset({"CB", "CA"}),
        shared_secret="sek",
        log=logging.getLogger("test"),
    )
    _migrate_legacy_always_blocked(
        client,
        "http://srv",
        frozenset({"CB", "CA"}),
        shared_secret="sek",
        log=logging.getLogger("test"),
    )

    assert [request.url.path for request in requests] == ["/blocked-channels"] * 4
    payloads = [json.loads(request.content) for request in requests]
    assert payloads == [
        {"channel_id": "CA", "reason": "migrated from always_blocked_channel_ids config"},
        {"channel_id": "CB", "reason": "migrated from always_blocked_channel_ids config"},
        {"channel_id": "CA", "reason": "migrated from always_blocked_channel_ids config"},
        {"channel_id": "CB", "reason": "migrated from always_blocked_channel_ids config"},
    ]
    assert all(request.headers["authorization"] == "Bearer sek" for request in requests)
