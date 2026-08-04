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
from slack_fuse.projector.ws_client import WSClient, WSClientOptions
from slack_fuse_server.wire.frames import SnapshotAtFrame

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow

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

        await client._handle_snapshot(
            SnapshotAtFrame(stream="channel:CSNAPSHOT", at=42, url="/snapshot")
        )

    assert recording_pool.releases == [(snapshot_conn, True)]


@pytest.mark.trio
async def test_reconcile_subscriptions_only_adds_missing_channels(
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
    requested: list[frozenset[str]] = []

    async def record_subscribe(_client: WSClient, ids: frozenset[str]) -> None:
        await trio.lowlevel.checkpoint()
        requested.append(ids)

    monkeypatch.setattr(WSClient, "subscribe_channels", record_subscribe)

    await client.reconcile_subscriptions(frozenset({"CACTIVE", "CMISSING"}))

    assert requested == [frozenset({"CMISSING"})]
