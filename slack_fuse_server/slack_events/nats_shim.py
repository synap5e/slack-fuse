"""NATS JetStream shim feeding the durable Slack Events API inbox."""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
import threading
import time
from collections.abc import Callable, Coroutine, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol, cast

import trio
from nats.aio.client import Client as NatsClient
from nats.aio.msg import Msg as NatsPyMessage
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, ConsumerConfig
from nats.js.client import JetStreamContext
from pydantic import ValidationError

from slack_fuse.models import EventsApiPayload
from slack_fuse_server._json import JsonObject
from slack_fuse_server.http.slack_webhook import verify_hmac_v0
from slack_fuse_server.slack_events.types import SlackEventTransport
from slack_fuse_server.slurper.ingestion import new_ulid

log = logging.getLogger(__name__)

NATS_TRIO_BUFFER_SIZE = 64
_NATS_FETCH_TIMEOUT_S = 1.0
_NATS_BACKPRESSURE_SLEEP_S = 0.05

type NatsShimProcessStatus = Literal["ok", "hmac_reject", "malformed", "dberror", "ack_error"]
type CoroutineFunction = Callable[[], Coroutine[object, object, None]]
type _NatsSpanStatus = Literal[
    "ok",
    "fail",
    "hmac_reject",
    "malformed",
    "dberror",
    "ack_error",
]


@dataclass(frozen=True, slots=True)
class NatsShimConfig:
    url: str
    ca_path: Path
    cert_path: Path
    key_path: Path
    subject: str
    durable_name: str
    stream_name: str


class NatsShimInbox(Protocol):
    async def enqueue(
        self,
        event_id: str,
        envelope: JsonObject,
        source_transport: SlackEventTransport = "http",
    ) -> bool: ...


@dataclass(frozen=True, slots=True)
class NatsShimDeps:
    signing_secret: str
    inbox: NatsShimInbox


class NatsShimMessage(Protocol):
    @property
    def body(self) -> bytes: ...

    @property
    def headers(self) -> Mapping[str, str]: ...

    async def ack(self) -> None: ...

    async def term(self) -> None: ...


@dataclass(frozen=True, slots=True)
class _SlackVerificationHeaders:
    timestamp_text: str
    signature: str


@dataclass(frozen=True, slots=True)
class _HeaderParseError:
    status: NatsShimProcessStatus
    reason: str
    missing: tuple[str, ...]
    observed: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _NatsConnection:
    client: NatsClient
    subscription: JetStreamContext.PullSubscription


class _AsyncioNatsMessage:
    def __init__(self, message: NatsPyMessage, loop: asyncio.AbstractEventLoop) -> None:
        self._message = message
        self._loop = loop

    @property
    def body(self) -> bytes:
        return self._message.data

    @property
    def headers(self) -> Mapping[str, str]:
        return self._message.headers or {}

    async def ack(self) -> None:
        await _run_on_asyncio_loop(self._loop, self._message.ack)

    async def term(self) -> None:
        await _run_on_asyncio_loop(self._loop, self._message.term)


async def run_nats_shim(config: NatsShimConfig, deps: NatsShimDeps) -> None:
    """Run the asyncio NATS client in one thread and process messages in Trio."""
    send_ch, recv_ch = trio.open_memory_channel[NatsShimMessage](NATS_TRIO_BUFFER_SIZE)
    async with send_ch, recv_ch, trio.open_nursery() as nursery:
        nursery.start_soon(_run_asyncio_bridge, config, send_ch)
        nursery.start_soon(process_nats_messages, recv_ch, deps)


async def process_nats_messages(
    recv_ch: trio.MemoryReceiveChannel[NatsShimMessage],
    deps: NatsShimDeps,
) -> None:
    async for message in recv_ch:
        await process_nats_message(message, deps)


async def process_nats_message(message: NatsShimMessage, deps: NatsShimDeps) -> NatsShimProcessStatus:
    """Validate one JetStream delivery, enqueue it durably, then ACK or TERM."""
    started_ns = time.monotonic_ns()
    status: NatsShimProcessStatus = "malformed"
    event_id = ""
    try:
        parsed_headers = _parse_slack_headers(message.headers)
        if isinstance(parsed_headers, _HeaderParseError):
            status = parsed_headers.status
            log.error(
                "nats shim: malformed verification headers reason=%s missing=%s observed_headers=%s",
                parsed_headers.reason,
                ",".join(parsed_headers.missing),
                ",".join(parsed_headers.observed),
            )
            await message.term()
            return status

        if not verify_hmac_v0(
            message.body,
            parsed_headers.timestamp_text,
            parsed_headers.signature,
            deps.signing_secret,
        ):
            log.warning("nats shim: HMAC rejected")
            status = "hmac_reject"
            await message.term()
            return status

        envelope = _decode_event_callback(message.body)
        if envelope is None:
            await message.term()
            status = "malformed"
            return status
        payload, raw_envelope = envelope
        event_id = payload.event_id

        try:
            await deps.inbox.enqueue(payload.event_id, raw_envelope, source_transport="nats")
        except Exception as exc:
            log.exception(
                "nats shim: enqueue failed; leaving message for redelivery exception_type=%s",
                type(exc).__name__,
            )
            status = "dberror"
            return status

        try:
            await message.ack()
        except Exception as exc:
            log.exception("nats shim: ACK failed after enqueue exception_type=%s", type(exc).__name__)
            status = "ack_error"
            return status
        status = "ok"
        return status
    finally:
        _emit_nats_span(
            "nats_shim.enqueue",
            status,
            _duration_ms(started_ns),
            {"event_id": event_id} if event_id else None,
        )


def _decode_event_callback(body: bytes) -> tuple[EventsApiPayload, JsonObject] | None:
    try:
        parsed_raw = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        log.warning("nats shim: malformed JSON body")
        return None
    if not isinstance(parsed_raw, dict):
        log.warning("nats shim: JSON body is not an object")
        return None
    envelope = cast(JsonObject, parsed_raw)
    try:
        payload = EventsApiPayload.model_validate(envelope)
    except ValidationError:
        log.warning("nats shim: malformed Events API payload")
        return None
    if payload.type != "event_callback" or not payload.event_id or payload.event is None:
        log.warning("nats shim: unsupported Events API envelope type=%s", payload.type)
        return None
    return (payload, envelope)


def _parse_slack_headers(headers: Mapping[str, str]) -> _SlackVerificationHeaders | _HeaderParseError:
    signature = _case_insensitive_header(headers, "x-slack-signature")
    timestamp = _case_insensitive_header(headers, "x-slack-request-timestamp")
    missing: list[str] = []
    if signature is None:
        missing.append("X-Slack-Signature")
    if timestamp is None:
        missing.append("X-Slack-Request-Timestamp")
    if missing:
        return _HeaderParseError(
            status="malformed",
            reason="missing_header",
            missing=tuple(missing),
            observed=_observed_header_names(headers),
        )
    if timestamp is None or not timestamp.isascii() or not timestamp.isdecimal():
        return _HeaderParseError(
            status="malformed",
            reason="bad_timestamp",
            missing=(),
            observed=_observed_header_names(headers),
        )
    return _SlackVerificationHeaders(timestamp_text=timestamp, signature=signature or "")


def _case_insensitive_header(headers: Mapping[str, str], wanted: str) -> str | None:
    wanted_lower = wanted.lower()
    for key, value in headers.items():
        if key.lower() == wanted_lower:
            return value
    return None


def _observed_header_names(headers: Mapping[str, str]) -> tuple[str, ...]:
    return tuple(sorted(headers)[:10])


async def _run_asyncio_bridge(config: NatsShimConfig, send_ch: trio.MemorySendChannel[NatsShimMessage]) -> None:
    stop_event = threading.Event()

    async def _stop_when_cancelled() -> None:
        try:
            await trio.sleep_forever()
        finally:
            stop_event.set()

    try:
        async with trio.open_nursery() as nursery:
            nursery.start_soon(_stop_when_cancelled)
            await trio.to_thread.run_sync(
                lambda: _run_asyncio_bridge_sync(config, send_ch, stop_event),
                abandon_on_cancel=False,
            )
            nursery.cancel_scope.cancel()
    finally:
        stop_event.set()
        await send_ch.aclose()


def _run_asyncio_bridge_sync(
    config: NatsShimConfig,
    send_ch: trio.MemorySendChannel[NatsShimMessage],
    stop_event: threading.Event,
) -> None:
    asyncio.run(_asyncio_bridge_main(config, send_ch, stop_event))


async def _asyncio_bridge_main(
    config: NatsShimConfig,
    send_ch: trio.MemorySendChannel[NatsShimMessage],
    stop_event: threading.Event,
) -> None:
    loop = asyncio.get_running_loop()
    conn = await _connect_nats(config)
    try:
        await _consume_subscription(conn.subscription, loop, send_ch, stop_event)
    finally:
        await _close_nats(conn)


async def _connect_nats(config: NatsShimConfig) -> _NatsConnection:
    disconnected_at_ns: int | None = None

    async def disconnected_cb() -> None:  # noqa: RUF029 - nats-py requires async callbacks.
        nonlocal disconnected_at_ns
        disconnected_at_ns = time.monotonic_ns()
        log.warning("nats shim: disconnected")

    async def reconnected_cb() -> None:  # noqa: RUF029 - nats-py requires async callbacks.
        nonlocal disconnected_at_ns
        finished_ns = time.monotonic_ns()
        duration_ms = 0 if disconnected_at_ns is None else int((finished_ns - disconnected_at_ns) / 1_000_000)
        disconnected_at_ns = None
        log.warning("nats shim: reconnected")
        _emit_nats_span("nats_shim.reconnect", "ok", duration_ms)

    async def error_cb(exc: Exception) -> None:  # noqa: RUF029 - nats-py requires async callbacks.
        log.warning("nats shim: client error exception_type=%s", type(exc).__name__)

    started_ns = time.monotonic_ns()
    try:
        tls_context = _build_tls_context(config)
        client = NatsClient()
        await client.connect(
            servers=config.url,
            name="slack-fuse-nats-shim",
            tls=tls_context,
            max_reconnect_attempts=-1,
            reconnect_time_wait=2,
            disconnected_cb=disconnected_cb,
            reconnected_cb=reconnected_cb,
            error_cb=error_cb,
        )
        jetstream = cast("Callable[[], JetStreamContext]", client.jetstream)
        js = jetstream()
        consumer_config = ConsumerConfig(
            name=config.durable_name,
            durable_name=config.durable_name,
            ack_policy=AckPolicy.EXPLICIT,
            filter_subject=config.subject,
        )
        sub = await js.pull_subscribe(
            config.subject,
            durable=config.durable_name,
            stream=config.stream_name,
            config=consumer_config,
            pending_msgs_limit=NATS_TRIO_BUFFER_SIZE,
        )
    except Exception as exc:
        _emit_nats_span("nats_shim.connect", "fail", _duration_ms(started_ns), {"error_type": type(exc).__name__})
        raise
    _emit_nats_span(
        "nats_shim.connect",
        "ok",
        _duration_ms(started_ns),
        {"subject": config.subject, "durable": config.durable_name, "stream": config.stream_name},
    )
    return _NatsConnection(client=client, subscription=sub)


async def _consume_subscription(
    sub: JetStreamContext.PullSubscription,
    loop: asyncio.AbstractEventLoop,
    send_ch: trio.MemorySendChannel[NatsShimMessage],
    stop_event: threading.Event,
) -> None:
    while not stop_event.is_set():
        try:
            messages = await sub.fetch(batch=NATS_TRIO_BUFFER_SIZE, timeout=_NATS_FETCH_TIMEOUT_S)
        except NatsTimeoutError:
            continue
        for raw_message in messages:
            if stop_event.is_set():
                break
            await _send_to_trio(send_ch, _AsyncioNatsMessage(raw_message, loop), stop_event)


async def _close_nats(conn: _NatsConnection) -> None:
    try:
        await conn.subscription.unsubscribe()
    except Exception as exc:  # noqa: BLE001 - cleanup continues through best-effort close failures.
        log.warning("nats shim: subscription cleanup failed exception_type=%s", type(exc).__name__)
    try:
        await conn.client.close()
    except Exception as exc:  # noqa: BLE001 - cleanup continues through best-effort close failures.
        log.warning("nats shim: connection cleanup failed exception_type=%s", type(exc).__name__)


async def _send_to_trio(
    send_ch: trio.MemorySendChannel[NatsShimMessage],
    message: NatsShimMessage,
    stop_event: threading.Event,
) -> None:
    while not stop_event.is_set():
        try:
            trio.from_thread.run_sync(send_ch.send_nowait, message)
        except trio.WouldBlock:
            await asyncio.sleep(_NATS_BACKPRESSURE_SLEEP_S)
            continue
        except (trio.BrokenResourceError, trio.ClosedResourceError):
            stop_event.set()
        return


def _build_tls_context(config: NatsShimConfig) -> ssl.SSLContext:
    tls_context = ssl.create_default_context(cafile=str(config.ca_path))
    tls_context.load_cert_chain(certfile=str(config.cert_path), keyfile=str(config.key_path))
    return tls_context


async def _run_on_asyncio_loop(
    loop: asyncio.AbstractEventLoop,
    func: CoroutineFunction,
) -> None:
    future = asyncio.run_coroutine_threadsafe(func(), loop)
    await trio.to_thread.run_sync(lambda: future.result(), abandon_on_cancel=False)


def _duration_ms(started_ns: int) -> int:
    return int((time.monotonic_ns() - started_ns) / 1_000_000)


def _emit_nats_span(
    op: str,
    status: _NatsSpanStatus,
    duration_ms: int,
    extra: Mapping[str, object] | None = None,
) -> None:
    result = _span_result_for_status(status)
    fields: dict[str, object] = {
        "op": op,
        "task": "nats-shim",
        "result": result,
        "status": status,
        "duration_ms": duration_ms,
        "limiter_wait_ms": 0,
        "sync_ms": 0,
        "span_id": new_ulid(),
    }
    extra_fields = dict(extra or {})
    fields.update(extra_fields)
    extra_format = " ".join(f"{key}=%({key})s" for key in sorted(extra_fields))
    message = (
        "slurper-span op=%(op)s task=%(task)s result=%(result)s status=%(status)s "
        "duration_ms=%(duration_ms)d limiter_wait_ms=%(limiter_wait_ms)d sync_ms=%(sync_ms)d "
        "span_id=%(span_id)s"
    )
    if extra_format:
        message = f"{message} {extra_format}"
    log.info(message, fields)


def _span_result_for_status(status: _NatsSpanStatus) -> str:
    if status == "ok":
        return "ok"
    if status == "hmac_reject" or status == "malformed":
        return "skipped"
    return "error"


__all__ = [
    "NATS_TRIO_BUFFER_SIZE",
    "NatsShimConfig",
    "NatsShimDeps",
    "NatsShimMessage",
    "NatsShimProcessStatus",
    "process_nats_message",
    "process_nats_messages",
    "run_nats_shim",
]
