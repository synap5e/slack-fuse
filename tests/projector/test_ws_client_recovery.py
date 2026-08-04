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
from slack_fuse.projector.ws_client import SubscriptionState, WSClient, WSClientOptions
from slack_fuse_server.wire.frames import (
    CaughtUpFrame,
    ErrorCode,
    ErrorFrame,
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
        client._subscription_state["channel:CSNAPSHOT"] = SubscriptionState.PENDING

        await client._handle_snapshot(
            SnapshotAtFrame(stream="channel:CSNAPSHOT", at=42, url="/snapshot")
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

    async def record_unsubscribe(_client: WSClient, ids: frozenset[str]) -> None:
        await trio.lowlevel.checkpoint()
        removed.append(ids)

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

    async def record_unsubscribe(_client: WSClient, _ids: frozenset[str]) -> None:
        await trio.lowlevel.checkpoint()

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


@pytest.mark.trio
async def test_unsubscribe_channels_sends_frame_and_retires_applier(
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

    await client.unsubscribe_channels(frozenset({"CEXTRA"}))

    assert sent == [UnsubscribeFrame(stream=stream)]
    assert stream not in client._appliers
    assert stream not in client._subscription_state
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

    async def fail_send(_client: WSClient, _frame: object) -> None:
        await trio.lowlevel.checkpoint()
        msg = "fault injection: subscribe send failed"
        raise RuntimeError(msg)

    monkeypatch.setattr(WSClient, "_send_frame", fail_send)

    with pytest.raises(RuntimeError, match="subscribe send failed"):
        await client._subscribe_stream(stream, since=0)

    assert client._subscription_state[stream] is SubscriptionState.FAILED
