# pyright: reportPrivateUsage=false
"""WSClient recovery behavior around pooled connections and subscriptions."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast
from zoneinfo import ZoneInfo

import httpx
import psycopg
import pytest
import trio

import slack_fuse.projector.ws_client as ws_client_module
from slack_fuse.projector.per_stream import StreamApplier
from slack_fuse.projector.pool import ConnectionPool
from slack_fuse.projector.snapshot_fetch import SnapshotResult
from slack_fuse.projector.ws_client import SubscriptionState, WSClient, WSClientOptions
from slack_fuse_server.wire.frames import (
    CaughtUpFrame,
    ErrorCode,
    ErrorFrame,
    ServerCapabilitiesFrame,
    SnapshotAtFrame,
    UnsubscribeFrame,
)

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow
    from trio_websocket import WebSocketConnection

    from tests.projector.conftest import ClientConnFactory


class _RecordingPool:
    def __init__(self, conn: Connection[TupleRow]) -> None:
        self._conn = conn
        self.releases: list[tuple[Connection[TupleRow], bool]] = []

    async def acquire(self) -> Connection[TupleRow]:
        return self._conn

    async def release(self, conn: Connection[TupleRow], *, discard: bool = False) -> None:
        self.releases.append((conn, discard))


@pytest.mark.trio
async def test_snapshot_operational_error_discards_pool_conn(
    client_conn_factory: ClientConnFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_conn = client_conn_factory()
    snapshot_conn = client_conn_factory()
    recording_pool = _RecordingPool(snapshot_conn)

    async def fail_snapshot(*_args: object, **_kwargs: object) -> None:
        await trio.lowlevel.checkpoint()
        raise psycopg.OperationalError("fault injection: snapshot connection lost")

    monkeypatch.setattr(ws_client_module, "fetch_and_apply_snapshot", fail_snapshot)
    async with httpx.AsyncClient() as http:
        client = WSClient(
            WSClientOptions(server_url="ws://server.invalid", base_http_url="http://server.invalid"),
            client_conn_factory,
            state_conn,
            tz=ZoneInfo("UTC"),
            http_client=http,
        )
        client._pool = cast("ConnectionPool", recording_pool)
        client._desired_channel_ids.add("CSNAPSHOT")
        client._subscription_state["channel:CSNAPSHOT"] = SubscriptionState.PENDING
        client._subscription_tokens["channel:CSNAPSHOT"] = 1

        await client._handle_snapshot(
            SnapshotAtFrame(stream="channel:CSNAPSHOT", at=42, url="/snapshot"),
            1,
        )

    assert recording_pool.releases == [(snapshot_conn, True)]
    assert client._subscription_state["channel:CSNAPSHOT"] is SubscriptionState.FAILED


@pytest.mark.trio
async def test_reconcile_unsubscribes_on_desired_set_shrink(
    client_conn_factory: ClientConnFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_conn = client_conn_factory()
    client = WSClient(
        WSClientOptions(server_url="ws://server.invalid"),
        client_conn_factory,
        state_conn,
        tz=ZoneInfo("UTC"),
    )
    client._appliers = cast(
        "dict[str, StreamApplier]",
        {"users": object(), "channel:CACTIVE": object(), "channel:CEXTRA": object()},
    )
    client._subscription_state = {
        "users": SubscriptionState.ACTIVE,
        "channel:CACTIVE": SubscriptionState.ACTIVE,
        "channel:CEXTRA": SubscriptionState.ACTIVE,
    }
    requested: list[frozenset[str]] = []
    removed: list[frozenset[str]] = []

    async def record_subscribe(_client: WSClient, ids: frozenset[str]) -> None:
        await trio.lowlevel.checkpoint()
        requested.append(ids)

    async def record_unsubscribe(_client: WSClient, ids: frozenset[str]) -> bool:
        await trio.lowlevel.checkpoint()
        removed.append(ids)
        return True

    monkeypatch.setattr(WSClient, "subscribe_channels", record_subscribe)
    monkeypatch.setattr(WSClient, "unsubscribe_channels", record_unsubscribe)

    await client.reconcile_subscriptions(frozenset({"CACTIVE", "CMISSING"}))

    assert removed == [frozenset({"CEXTRA"})]
    assert requested == [frozenset({"CMISSING"})]


@pytest.mark.trio
async def test_failed_subscribe_is_retried_on_next_reconcile(
    client_conn_factory: ClientConnFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = WSClient(
        WSClientOptions(server_url="ws://server.invalid"),
        client_conn_factory,
        client_conn_factory(),
        tz=ZoneInfo("UTC"),
    )
    client._appliers = cast("dict[str, StreamApplier]", {"channel:CFAIL": object()})
    client._subscription_state = {"channel:CFAIL": SubscriptionState.FAILED}
    requested: list[frozenset[str]] = []

    async def record_subscribe(_client: WSClient, ids: frozenset[str]) -> None:
        await trio.lowlevel.checkpoint()
        requested.append(ids)

    async def record_unsubscribe(_client: WSClient, _ids: frozenset[str]) -> bool:
        await trio.lowlevel.checkpoint()
        return True

    monkeypatch.setattr(WSClient, "subscribe_channels", record_subscribe)
    monkeypatch.setattr(WSClient, "unsubscribe_channels", record_unsubscribe)

    await client.reconcile_subscriptions(frozenset({"CFAIL"}))

    assert requested == [frozenset({"CFAIL"})]


class _RecordingApplier:
    def __init__(self) -> None:
        self.frames: list[object] = []
        self.closed = False

    async def enqueue(self, frame: object) -> None:
        self.frames.append(frame)

    async def close(self) -> None:
        self.closed = True


class _ClosingWebSocket:
    def __init__(self) -> None:
        self.closes: list[tuple[int, str]] = []
        self.sent_messages: list[str] = []

    async def aclose(self, code: int = 1000, reason: str = "") -> None:
        self.closes.append((code, reason))

    async def send_message(self, message: str) -> None:
        if '"type":"unsubscribe"' in message:
            msg = "old server would reject UnsubscribeFrame"
            raise AssertionError(msg)
        self.sent_messages.append(message)


@pytest.mark.trio
async def test_client_uses_unsubscribe_frame_when_server_advertises_it(
    client_conn_factory: ClientConnFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = WSClient(
        WSClientOptions(server_url="ws://server.invalid"),
        client_conn_factory,
        client_conn_factory(),
        tz=ZoneInfo("UTC"),
    )
    stream = "channel:CEXTRA"
    applier = _RecordingApplier()
    client._appliers = cast("dict[str, StreamApplier]", {stream: applier})
    client._subscription_state = {stream: SubscriptionState.ACTIVE}
    client._ws = cast("WebSocketConnection", object())
    sent: list[object] = []

    async def record_send(_client: WSClient, frame: object) -> None:
        await trio.lowlevel.checkpoint()
        sent.append(frame)

    monkeypatch.setattr(WSClient, "_send_frame", record_send)

    await client._dispatch_frame(ServerCapabilitiesFrame(supported_frames=["unsubscribe"]))
    await client.unsubscribe_channels(frozenset({"CEXTRA"}))

    assert sent == [UnsubscribeFrame(stream=stream)]
    assert stream not in client._appliers
    assert stream not in client._subscription_state
    assert applier.closed


@pytest.mark.trio
async def test_client_falls_back_to_controlled_reconnect_when_server_does_not_advertise_unsubscribe(
    client_conn_factory: ClientConnFactory,
) -> None:
    client = WSClient(
        WSClientOptions(server_url="ws://server.invalid"),
        client_conn_factory,
        client_conn_factory(),
        tz=ZoneInfo("UTC"),
    )
    stream = "channel:CEXTRA"
    applier = _RecordingApplier()
    ws = _ClosingWebSocket()
    client._appliers = cast("dict[str, StreamApplier]", {stream: applier})
    client._subscription_state = {stream: SubscriptionState.ACTIVE}
    client._ws = cast("WebSocketConnection", ws)

    await client.reconcile_subscriptions(frozenset())

    assert ws.closes == [(1000, "subscription set changed")]
    assert ws.sent_messages == []
    assert stream not in client._appliers
    assert stream not in client._subscription_state
    assert applier.closed


@pytest.mark.trio
async def test_old_server_new_client_does_not_break_on_shrink(
    client_conn_factory: ClientConnFactory,
) -> None:
    client = WSClient(
        WSClientOptions(server_url="ws://old-server.invalid"),
        client_conn_factory,
        client_conn_factory(),
        tz=ZoneInfo("UTC"),
    )
    stream = "channel:COLD"
    ws = _ClosingWebSocket()
    client._appliers = cast("dict[str, StreamApplier]", {stream: _RecordingApplier()})
    client._subscription_state = {stream: SubscriptionState.ACTIVE}
    client._ws = cast("WebSocketConnection", ws)

    await client.reconcile_subscriptions(frozenset())

    assert ws.sent_messages == []
    assert ws.closes == [(1000, "subscription set changed")]


@pytest.mark.trio
async def test_unsubscribe_during_in_flight_snapshot_does_not_resubscribe(
    client_conn_factory: ClientConnFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream = "channel:CSNAPSHOT"
    snapshot_conn = client_conn_factory()
    recording_pool = _RecordingPool(snapshot_conn)
    started = trio.Event()
    release = trio.Event()

    async def delayed_snapshot(*_args: object, **_kwargs: object) -> SnapshotResult:
        started.set()
        await release.wait()
        return SnapshotResult(stream=stream, at_offset=42, records_applied=1)

    monkeypatch.setattr(ws_client_module, "fetch_and_apply_snapshot", delayed_snapshot)
    async with httpx.AsyncClient() as http:
        client = WSClient(
            WSClientOptions(server_url="ws://server.invalid"),
            client_conn_factory,
            client_conn_factory(),
            tz=ZoneInfo("UTC"),
            http_client=http,
        )
        applier = _RecordingApplier()
        client._pool = cast("ConnectionPool", recording_pool)
        client._appliers = cast("dict[str, StreamApplier]", {stream: applier})
        client._desired_channel_ids.add("CSNAPSHOT")
        client._subscription_state[stream] = SubscriptionState.PENDING
        client._subscription_tokens[stream] = 7
        client._server_capabilities = frozenset({"unsubscribe"})
        client._ws = cast("WebSocketConnection", object())
        sent: list[object] = []

        async def record_send(_client: WSClient, frame: object) -> None:
            await trio.lowlevel.checkpoint()
            sent.append(frame)

        monkeypatch.setattr(WSClient, "_send_frame", record_send)
        async with trio.open_nursery() as nursery:
            nursery.start_soon(client._handle_snapshot, SnapshotAtFrame(stream=stream, at=42, url="/snapshot"), 7)
            await started.wait()
            await client.reconcile_subscriptions(frozenset())
            release.set()

    assert sent == [UnsubscribeFrame(stream=stream)]
    assert stream not in client._appliers
    assert stream not in client._subscription_state
    assert stream not in client._subscription_tokens
    assert applier.closed


@pytest.mark.trio
async def test_subscription_state_transitions_pending_active_failed(
    client_conn_factory: ClientConnFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = WSClient(
        WSClientOptions(server_url="ws://server.invalid"),
        client_conn_factory,
        client_conn_factory(),
        tz=ZoneInfo("UTC"),
    )
    stream = "channel:CSTATE"
    applier = _RecordingApplier()
    client._appliers = cast("dict[str, StreamApplier]", {stream: applier})
    client._desired_channel_ids.add("CSTATE")
    states_during_send: list[SubscriptionState] = []

    async def record_send(_client: WSClient, _frame: object) -> None:
        await trio.lowlevel.checkpoint()
        states_during_send.append(client._subscription_state[stream])

    monkeypatch.setattr(WSClient, "_send_frame", record_send)

    await client._subscribe_stream(stream, since=0)
    assert states_during_send == [SubscriptionState.PENDING]
    assert client._subscription_state[stream] is SubscriptionState.PENDING

    caught_up = CaughtUpFrame(stream=stream, head_offset=0)
    await client._dispatch_frame(caught_up)
    assert client._subscription_state[stream] is SubscriptionState.ACTIVE

    await client._dispatch_frame(ErrorFrame(code=ErrorCode.STREAM_NOT_FOUND, stream=stream))
    assert client._subscription_state[stream] is SubscriptionState.FAILED
    assert applier.frames == [caught_up]


@pytest.mark.trio
async def test_subscribe_send_failure_marks_state_failed(
    client_conn_factory: ClientConnFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = WSClient(
        WSClientOptions(server_url="ws://server.invalid"),
        client_conn_factory,
        client_conn_factory(),
        tz=ZoneInfo("UTC"),
    )
    stream = "channel:CFAIL"
    client._appliers = cast("dict[str, StreamApplier]", {stream: _RecordingApplier()})
    client._desired_channel_ids.add("CFAIL")

    async def fail_send(_client: WSClient, _frame: object) -> None:
        await trio.lowlevel.checkpoint()
        msg = "fault injection: subscribe send failed"
        raise RuntimeError(msg)

    monkeypatch.setattr(WSClient, "_send_frame", fail_send)

    with pytest.raises(RuntimeError, match="subscribe send failed"):
        await client._subscribe_stream(stream, since=0)

    assert client._subscription_state[stream] is SubscriptionState.FAILED
