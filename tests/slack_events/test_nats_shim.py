# TODO(integration): real NATS test once medina credentials land.
"""NATS shim message validation and durable inbox handoff."""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import psycopg
import pytest
import trio

from slack_fuse_server._json import JsonObject
from slack_fuse_server.slack_events import nats_shim
from slack_fuse_server.slack_events.inbox import InboxWriter
from slack_fuse_server.slack_events.nats_shim import NatsShimDeps, process_nats_message
from slack_fuse_server.slack_events.types import SlackEventTransport

if TYPE_CHECKING:
    from tests.conftest import ServerConnFactory

_SECRET = "test-signing-secret"


@dataclass(slots=True)
class _FakeNatsMessage:
    body: bytes
    headers: Mapping[str, str]
    acked: bool = False
    termed: bool = False

    async def ack(self) -> None:
        self.acked = True

    async def term(self) -> None:
        self.termed = True


@dataclass(slots=True)
class _RecordingInbox:
    rows: list[tuple[str, JsonObject, SlackEventTransport]] = field(default_factory=list)

    async def enqueue(
        self,
        event_id: str,
        envelope: JsonObject,
        source_transport: SlackEventTransport = "http",
    ) -> bool:
        self.rows.append((event_id, envelope, source_transport))
        return True


@dataclass(slots=True)
class _FailingInbox:
    async def enqueue(
        self,
        event_id: str,
        envelope: JsonObject,
        source_transport: SlackEventTransport = "http",
    ) -> bool:
        del event_id, envelope, source_transport
        raise psycopg.OperationalError("test db down")


def _body(event_id: str = "EvNats") -> bytes:
    payload: JsonObject = {
        "type": "event_callback",
        "event_id": event_id,
        "event_time": int(time.time()),
        "event": {"type": "message", "channel": "C_NATS", "ts": "1.000001", "text": "hello"},
    }
    return json.dumps(payload, separators=(",", ":")).encode()


def _signature(body: bytes, timestamp: int) -> str:
    base = b"v0:" + str(timestamp).encode() + b":" + body
    return "v0=" + hmac.new(_SECRET.encode(), base, hashlib.sha256).hexdigest()


def _headers(body: bytes, timestamp: int) -> dict[str, str]:
    return {
        "X-Slack-Signature": _signature(body, timestamp),
        "X-Slack-Request-Timestamp": str(timestamp),
    }


def _flip_signature(signature: str) -> str:
    replacement = "0" if signature[-1] != "0" else "1"
    return signature[:-1] + replacement


@pytest.mark.trio
async def test_replay_older_than_five_minutes_is_accepted() -> None:
    timestamp = int(time.time()) - 600
    body = _body("EvOldReplay")
    message = _FakeNatsMessage(body=body, headers=_headers(body, timestamp))
    inbox = _RecordingInbox()

    status = await process_nats_message(message, NatsShimDeps(signing_secret=_SECRET, inbox=inbox))

    assert status == "ok"
    assert [(event_id, transport) for event_id, _, transport in inbox.rows] == [("EvOldReplay", "nats")]
    assert message.acked is True
    assert message.termed is False


@pytest.mark.trio
@pytest.mark.parametrize("case", ["body", "signature"])
async def test_tampered_signature_or_body_is_termed_without_insert(case: str) -> None:
    timestamp = int(time.time())
    signed_body = _body("EvSigned")
    delivered_body = _body("EvDelivered") if case == "body" else signed_body
    headers = _headers(signed_body, timestamp)
    if case == "signature":
        headers["X-Slack-Signature"] = _flip_signature(headers["X-Slack-Signature"])
    message = _FakeNatsMessage(body=delivered_body, headers=headers)
    inbox = _RecordingInbox()

    status = await process_nats_message(message, NatsShimDeps(signing_secret=_SECRET, inbox=inbox))

    assert status == "hmac_reject"
    assert inbox.rows == []
    assert message.acked is False
    assert message.termed is True


@pytest.mark.trio
async def test_duplicate_delivery_inserts_one_inbox_row(server_conn_factory: ServerConnFactory) -> None:
    conn = server_conn_factory()
    inbox = InboxWriter(conn)
    deps = NatsShimDeps(signing_secret=_SECRET, inbox=inbox)
    timestamp = int(time.time())
    body = _body("EvDuplicateNats")
    first = _FakeNatsMessage(body=body, headers=_headers(body, timestamp))
    second = _FakeNatsMessage(body=body, headers=_headers(body, timestamp))

    assert await process_nats_message(first, deps) == "ok"
    assert await process_nats_message(second, deps) == "ok"

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*), MIN(source_transport) FROM slack_event_inbox WHERE event_id = 'EvDuplicateNats'")
        row = cur.fetchone()
    assert row == (1, "nats")
    assert first.acked is True
    assert second.acked is True
    assert first.termed is False
    assert second.termed is False


@pytest.mark.trio
async def test_db_failure_leaves_message_unacked_and_untermed() -> None:
    timestamp = int(time.time())
    body = _body("EvDbDown")
    message = _FakeNatsMessage(body=body, headers=_headers(body, timestamp))

    status = await process_nats_message(message, NatsShimDeps(signing_secret=_SECRET, inbox=_FailingInbox()))

    assert status == "dberror"
    assert message.acked is False
    assert message.termed is False


@pytest.mark.trio
async def test_iteration_crash_does_not_kill_sibling_tasks(monkeypatch: pytest.MonkeyPatch) -> None:
    """A shim iteration crash must retry, not tear down the supervisor nursery.

    Regression: 2026-09-02 the first prod enable took the whole server down
    because a `nats_shim.connect` timeout propagated up to the supervisor
    nursery, cancelling the WS listener + webhook listener too. Verifies here
    that any exception from `_run_shim_iteration` is caught, retried with
    backoff, and never reaches the caller's nursery.
    """
    monkeypatch.setattr(nats_shim, "_NATS_RETRY_INITIAL_BACKOFF_S", 0.01)
    monkeypatch.setattr(nats_shim, "_NATS_RETRY_MAX_BACKOFF_S", 0.01)

    iterations = 0

    async def _boom(config: nats_shim.NatsShimConfig, deps: nats_shim.NatsShimDeps) -> None:
        nonlocal iterations
        del config, deps
        iterations += 1
        await trio.lowlevel.checkpoint()
        raise TimeoutError("simulated connect timeout")

    monkeypatch.setattr(nats_shim, "_run_shim_iteration", _boom)

    sibling_ran = False

    async def _sibling() -> None:
        nonlocal sibling_ran
        await trio.sleep(0.05)
        sibling_ran = True

    config = nats_shim.NatsShimConfig(
        url="nats://ignored:4222",
        ca_path=object(),  # pyright: ignore[reportArgumentType]
        cert_path=object(),  # pyright: ignore[reportArgumentType]
        key_path=object(),  # pyright: ignore[reportArgumentType]
        subject="ignored",
        durable_name="ignored",
        stream_name="ignored",
    )
    deps = NatsShimDeps(signing_secret=_SECRET, inbox=_RecordingInbox())

    async with trio.open_nursery() as nursery:
        nursery.start_soon(nats_shim.run_nats_shim, config, deps)
        nursery.start_soon(_sibling)
        await trio.sleep(0.1)
        nursery.cancel_scope.cancel()

    assert iterations >= 2, "shim should have retried at least once after the first crash"
    assert sibling_ran, "sibling task must not be cancelled by the shim crash"
