# pyright: reportPrivateUsage=false
"""Public Slack webhook authentication, routing, and framing."""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

import httpx
import pytest
import trio

from slack_fuse_server._json import JsonObject
from slack_fuse_server.http.server import HttpRequest
from slack_fuse_server.http.slack_webhook import (
    MAX_SLACK_BODY_BYTES,
    SlackWebhookDeps,
    route_slack_webhook,
    serve_slack_webhook_connection,
    serve_slack_webhook_on_listeners,
    verify_slack_signature,
)
from slack_fuse_server.slack_events.dispatcher import SlackEventDispatcher
from slack_fuse_server.slack_events.inbox import InboxWriter, consume
from slack_fuse_server.slurper.api import SlackClient
from slack_fuse_server.slurper.health import HealthEmitter
from tests.conftest import make_test_limiters, make_test_writer

if TYPE_CHECKING:
    from tests.conftest import ServerConnFactory

_SECRET = "test-signing-secret"
_NOW = 1_800_000_000


@dataclass(slots=True)
class _RecordingInbox:
    rows: list[tuple[str, JsonObject]] = field(default_factory=list)

    async def enqueue(self, event_id: str, envelope: JsonObject) -> bool:
        self.rows.append((event_id, envelope))
        return True


def _signature(body: bytes, timestamp: int = _NOW) -> str:
    base = b"v0:" + str(timestamp).encode() + b":" + body
    return "v0=" + hmac.new(_SECRET.encode(), base, hashlib.sha256).hexdigest()


def _request(
    payload: JsonObject,
    *,
    timestamp: int = _NOW,
    signature_header: bytes = b"X-Slack-Signature",
    timestamp_header: bytes = b"X-Slack-Request-Timestamp",
) -> HttpRequest:
    body = json.dumps(payload, separators=(",", ":")).encode()
    return HttpRequest(
        method="POST",
        target="/slack/events",
        body=body,
        headers=((timestamp_header, str(timestamp).encode()), (signature_header, _signature(body, timestamp).encode())),
    )


def _deps(inbox: _RecordingInbox) -> SlackWebhookDeps:
    return SlackWebhookDeps(signing_secret=_SECRET, inbox=cast(InboxWriter, inbox), clock=lambda: float(_NOW))


@asynccontextmanager
async def _running_webhook(deps: SlackWebhookDeps) -> AsyncIterator[str]:
    listeners = await trio.open_tcp_listeners(0, host="127.0.0.1")
    sockname = cast(tuple[str, int], listeners[0].socket.getsockname())
    async with trio.open_nursery() as nursery:
        nursery.start_soon(serve_slack_webhook_on_listeners, listeners, deps)
        await trio.lowlevel.checkpoint()
        try:
            yield f"http://127.0.0.1:{sockname[1]}"
        finally:
            nursery.cancel_scope.cancel()


@pytest.mark.trio
async def test_signed_url_verification_returns_raw_text_challenge() -> None:
    inbox = _RecordingInbox()
    request = _request({"type": "url_verification", "challenge": "abc123"})

    response = await route_slack_webhook(request, _deps(inbox))

    assert response.status_code == 200
    assert response.content_type == "text/plain; charset=utf-8"
    assert response.body == b"abc123"
    assert inbox.rows == []


@pytest.mark.trio
async def test_url_verification_requires_valid_hmac_before_challenge() -> None:
    inbox = _RecordingInbox()
    request = HttpRequest(
        method="POST",
        target="/slack/events",
        body=b'{"type":"url_verification","challenge":"do-not-return"}',
        headers=((b"X-Slack-Request-Timestamp", str(_NOW).encode()),),
    )

    response = await route_slack_webhook(request, _deps(inbox))

    assert response.status_code == 401
    assert b"do-not-return" not in response.body
    assert inbox.rows == []


@pytest.mark.parametrize("header", [b"x-slack-signature", b"X-Slack-Signature", b"X-SLACK-SIGNATURE"])
def test_signature_header_names_are_case_insensitive(header: bytes) -> None:
    request = _request({"type": "url_verification", "challenge": "ok"}, signature_header=header)
    assert verify_slack_signature(request, _SECRET, now=float(_NOW)) is None


@pytest.mark.parametrize("signature", [b"", b"v1=bad", b"v0=xyz", b"v0=" + b"0" * 63])
def test_malformed_hmac_is_401(signature: bytes) -> None:
    request = HttpRequest(
        method="POST",
        target="/slack/events",
        body=b"{}",
        headers=((b"x-slack-request-timestamp", str(_NOW).encode()), (b"x-slack-signature", signature)),
    )
    assert verify_slack_signature(request, _SECRET, now=float(_NOW)) == 401


@pytest.mark.parametrize(
    ("timestamp", "expected"),
    [(_NOW - 300, None), (_NOW + 300, None), (_NOW - 301, 400), (_NOW + 301, 400)],
)
def test_timestamp_skew_boundary(timestamp: int, expected: int | None) -> None:
    request = _request({"type": "url_verification", "challenge": "ok"}, timestamp=timestamp)
    assert verify_slack_signature(request, _SECRET, now=float(_NOW)) == expected


def test_missing_and_non_integer_timestamp_are_400() -> None:
    body = b"{}"
    missing = HttpRequest(
        method="POST",
        target="/slack/events",
        body=body,
        headers=((b"x-slack-signature", _signature(body).encode()),),
    )
    malformed = HttpRequest(
        method="POST",
        target="/slack/events",
        body=body,
        headers=((b"x-slack-signature", _signature(body).encode()), (b"x-slack-request-timestamp", b"soon")),
    )
    assert verify_slack_signature(missing, _SECRET, now=float(_NOW)) == 400
    assert verify_slack_signature(malformed, _SECRET, now=float(_NOW)) == 400


@pytest.mark.trio
async def test_event_callback_enqueues_raw_envelope() -> None:
    inbox = _RecordingInbox()
    payload: JsonObject = {
        "type": "event_callback",
        "event_id": "Ev123",
        "event_time": _NOW,
        "event": {"type": "message", "channel": "C1", "ts": "1.000001", "text": "hello"},
        "undeclared_outer_field": {"kept": True},
    }
    response = await route_slack_webhook(_request(payload), _deps(inbox))
    assert response.status_code == 200
    assert inbox.rows == [("Ev123", payload)]


@pytest.mark.trio
async def test_signed_http_to_inbox_to_dispatcher_to_events_smoke(
    server_conn_factory: ServerConnFactory,
    fake_slack_http: httpx.Client,
) -> None:
    inbox_conn = server_conn_factory()
    consumer_conn = server_conn_factory()
    event_conn = server_conn_factory()
    query = server_conn_factory()
    event_writer = make_test_writer(event_conn)
    client = SlackClient("xoxp-test")
    client._http = fake_slack_http
    dispatcher = SlackEventDispatcher(
        event_writer,
        client,
        "U_SELF",
        make_test_limiters(),
        HealthEmitter(event_writer),
    )
    deps = SlackWebhookDeps(
        signing_secret=_SECRET,
        inbox=InboxWriter(inbox_conn),
        clock=lambda: float(_NOW),
    )
    payload: JsonObject = {
        "type": "event_callback",
        "event_id": "EvEndToEnd",
        "event_time": _NOW,
        "event": {"type": "message", "channel": "C_E2E", "ts": "1.000001", "text": "durable"},
    }
    response = await route_slack_webhook(_request(payload), deps)
    assert response.status_code == 200

    async with trio.open_nursery() as nursery:
        nursery.start_soon(consume, consumer_conn, dispatcher, HealthEmitter(event_writer))
        with trio.fail_after(2):
            while True:
                with query.cursor() as cur:
                    cur.execute(
                        "SELECT e.source->>'slack_event_id', i.processed_at IS NOT NULL "
                        "FROM events e JOIN slack_event_inbox i ON i.event_id = 'EvEndToEnd' "
                        "WHERE e.stream = 'channel:C_E2E' AND e.kind = 'message'"
                    )
                    row = cur.fetchone()
                if row is not None and row[1]:
                    break
                await trio.sleep(0.01)
        nursery.cancel_scope.cancel()

    assert row == ("EvEndToEnd", True)
    with query.cursor() as cur:
        cur.execute("SELECT processed_at IS NOT NULL FROM slack_event_inbox WHERE event_id = 'EvEndToEnd'")
        inbox_row = cur.fetchone()
    assert inbox_row == (True,)


@pytest.mark.trio
async def test_duplicate_and_concurrent_http_deliveries_dispatch_once(
    server_conn_factory: ServerConnFactory,
    fake_slack_http: httpx.Client,
) -> None:
    inbox_conn = server_conn_factory()
    consumer_conn = server_conn_factory()
    event_conn = server_conn_factory()
    query = server_conn_factory()
    event_writer = make_test_writer(event_conn)
    client = SlackClient("xoxp-test")
    client._http = fake_slack_http
    dispatcher = SlackEventDispatcher(
        event_writer,
        client,
        "U_SELF",
        make_test_limiters(),
        HealthEmitter(event_writer),
    )
    deps = SlackWebhookDeps(
        signing_secret=_SECRET,
        inbox=InboxWriter(inbox_conn),
        clock=lambda: float(_NOW),
    )
    payload: JsonObject = {
        "type": "event_callback",
        "event_id": "EvHttpDuplicate",
        "event_time": _NOW,
        "event": {"type": "message", "channel": "C_DUP", "ts": "2.000001", "text": "once"},
    }
    request = _request(payload)
    responses: list[int] = []

    async def post() -> None:
        responses.append((await route_slack_webhook(request, deps)).status_code)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(post)
        nursery.start_soon(post)
    assert responses == [200, 200]
    assert (await route_slack_webhook(request, deps)).status_code == 200

    async with trio.open_nursery() as nursery:
        nursery.start_soon(consume, consumer_conn, dispatcher, HealthEmitter(event_writer))
        with trio.fail_after(2):
            while True:
                with query.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) FROM events WHERE stream = 'channel:C_DUP' AND kind = 'message'"
                    )
                    row = cur.fetchone()
                if row is not None and row[0] == 1:
                    break
                await trio.sleep(0.01)
        nursery.cancel_scope.cancel()

    with query.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM slack_event_inbox WHERE event_id = 'EvHttpDuplicate'")
        inbox_row = cur.fetchone()
    assert inbox_row == (1,)


@pytest.mark.trio
async def test_signed_webhook_self_join_runs_full_chain(
    server_conn_factory: ServerConnFactory,
) -> None:
    inbox_conn = server_conn_factory()
    consumer_conn = server_conn_factory()
    event_conn = server_conn_factory()
    query = server_conn_factory()
    event_writer = make_test_writer(event_conn)
    channel_info: JsonObject = {
        "ok": True,
        "channel": {
            "id": "C_WEBHOOK_JOIN",
            "name": "incident-webhook-join",
            "is_channel": True,
            "is_private": False,
            "is_archived": False,
            "is_member": True,
            "topic": {"value": "", "creator": "", "last_set": 0},
            "purpose": {"value": "", "creator": "", "last_set": 0},
            "num_members": 2,
        },
    }

    def respond(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=channel_info)

    queued: list[str] = []
    with httpx.Client(
        base_url="https://slack.com/api",
        transport=httpx.MockTransport(respond),
    ) as slack_http:
        client = SlackClient("xoxp-test")
        client._http = slack_http
        dispatcher = SlackEventDispatcher(
            event_writer,
            client,
            "U_SELF",
            make_test_limiters(),
            HealthEmitter(event_writer),
            lambda channel_id: not queued.append(channel_id),
        )
        deps = SlackWebhookDeps(
            signing_secret=_SECRET,
            inbox=InboxWriter(inbox_conn),
            clock=lambda: float(_NOW),
        )
        payload: JsonObject = {
            "type": "event_callback",
            "event_id": "EvSignedSelfJoin",
            "event_time": _NOW,
            "event": {
                "type": "member_joined_channel",
                "channel": "C_WEBHOOK_JOIN",
                "user": "U_SELF",
                "event_ts": "4.000001",
            },
        }
        assert (await route_slack_webhook(_request(payload), deps)).status_code == 200

        async with trio.open_nursery() as nursery:
            nursery.start_soon(consume, consumer_conn, dispatcher, HealthEmitter(event_writer))

            def processed() -> bool:
                with query.cursor() as cur:
                    cur.execute(
                        "SELECT processed_at IS NOT NULL FROM slack_event_inbox "
                        "WHERE event_id = 'EvSignedSelfJoin'"
                    )
                    row = cur.fetchone()
                return row == (True,)

            with trio.fail_after(2):
                while not processed():
                    await trio.sleep(0.01)
            nursery.cancel_scope.cancel()

    with query.cursor() as cur:
        cur.execute(
            "SELECT kind, source->>'triggered_by', source->>'slack_event_id' "
            "FROM events WHERE stream = 'channel-list' ORDER BY offset_in_stream"
        )
        rows = cur.fetchall()
    assert rows == [
        ("channel_added", "self-join", "EvSignedSelfJoin"),
        ("channel_member_joined", None, "EvSignedSelfJoin"),
    ]
    assert queued == ["C_WEBHOOK_JOIN"]


@pytest.mark.trio
async def test_app_rate_limited_acks_without_inbox(caplog: pytest.LogCaptureFixture) -> None:
    inbox = _RecordingInbox()
    response = await route_slack_webhook(_request({"type": "app_rate_limited"}), _deps(inbox))
    assert response.status_code == 200
    assert inbox.rows == []
    assert "app_rate_limited" in caplog.text


@pytest.mark.trio
async def test_realistic_45k_message_is_accepted() -> None:
    inbox = _RecordingInbox()
    payload: JsonObject = {
        "type": "event_callback",
        "event_id": "EvLargeMessage",
        "event_time": _NOW,
        "event": {
            "type": "message",
            "channel": "C_LARGE",
            "ts": "3.000001",
            "text": "x" * 45_000,
        },
    }
    request = _request(payload)
    assert 25 * 1024 < len(request.body) < MAX_SLACK_BODY_BYTES
    response = await route_slack_webhook(request, _deps(inbox))
    assert response.status_code == 200
    assert [event_id for event_id, _ in inbox.rows] == ["EvLargeMessage"]


@pytest.mark.trio
@pytest.mark.parametrize(
    ("method", "target"),
    [
        ("GET", "/slack/events"),
        ("POST", "/healthz"),
        ("GET", "/snapshot/x"),
        ("GET", "/originals"),
        ("GET", "/gaps"),
        ("POST", "/backfill-channel/C1"),
    ],
)
async def test_public_port_isolation(method: str, target: str) -> None:
    async with _running_webhook(_deps(_RecordingInbox())) as base_url, httpx.AsyncClient(
        base_url=base_url
    ) as client:
        response = await client.request(method, target)
    assert response.status_code == 404


class _MemoryStream(trio.abc.Stream):
    def __init__(self, request: bytes, *, delay_after_first_read_s: float = 0.0) -> None:
        self.request = request
        self.response = bytearray()
        self.reads = 0
        self.delay = delay_after_first_read_s

    async def send_all(self, data: bytes | bytearray | memoryview) -> None:
        self.response.extend(data)

    async def wait_send_all_might_not_block(self) -> None:
        return None

    async def receive_some(self, max_bytes: int | None = None) -> bytes:
        self.reads += 1
        if self.reads > 1 and self.delay:
            await trio.sleep(self.delay)
        if not self.request:
            return b""
        limit = len(self.request) if max_bytes is None else max_bytes
        chunk = self.request[:limit]
        self.request = self.request[limit:]
        return chunk

    async def aclose(self) -> None:
        return None


def _raw_http_request(body: bytes, *, content_length: int | None = None) -> bytes:
    length = len(body) if content_length is None else content_length
    headers = (
        b"POST /slack/events HTTP/1.1\r\nHost: test\r\nContent-Length: "
        + str(length).encode()
        + b"\r\nX-Slack-Request-Timestamp: "
        + str(_NOW).encode()
        + b"\r\nX-Slack-Signature: "
        + _signature(body).encode()
        + b"\r\n\r\n"
    )
    return headers + body


def _challenge_body(size: int) -> bytes:
    prefix = b'{"type":"url_verification","challenge":"'
    suffix = b'"}'
    return prefix + (b"x" * (size - len(prefix) - len(suffix))) + suffix


@pytest.mark.trio
@pytest.mark.parametrize("size", [25 * 1024 + 1, MAX_SLACK_BODY_BYTES])
async def test_large_valid_bodies_up_to_one_mib_pass(size: int) -> None:
    body = _challenge_body(size)
    stream = _MemoryStream(_raw_http_request(body))
    await serve_slack_webhook_connection(stream, _deps(_RecordingInbox()))
    assert bytes(stream.response).startswith(b"HTTP/1.1 200")


@pytest.mark.trio
async def test_body_over_one_mib_is_413() -> None:
    body = b"{}"
    stream = _MemoryStream(_raw_http_request(body, content_length=MAX_SLACK_BODY_BYTES + 1))
    await serve_slack_webhook_connection(stream, _deps(_RecordingInbox()))
    assert bytes(stream.response).startswith(b"HTTP/1.1 413")


@pytest.mark.trio
async def test_slow_body_hits_end_to_end_deadline_without_enqueue() -> None:
    inbox = _RecordingInbox()
    body = _challenge_body(20_000)
    stream = _MemoryStream(_raw_http_request(body), delay_after_first_read_s=3.0)
    await serve_slack_webhook_connection(stream, _deps(inbox))
    assert bytes(stream.response).startswith(b"HTTP/1.1 500") or not stream.response
    assert inbox.rows == []
