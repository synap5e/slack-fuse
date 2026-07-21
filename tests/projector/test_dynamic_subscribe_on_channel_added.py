# pyright: reportPrivateUsage=false
"""A newly discovered channel becomes live without restarting the projector."""

from __future__ import annotations

import psycopg
import trio
from psycopg.rows import TupleRow

from slack_fuse.projector.per_stream import StreamApplier
from slack_fuse.projector.ws_client import WSClient, WSClientOptions
from slack_fuse_server.wire.frames import EventFrame, SubscribeFrame
from tests.projector.conftest import ClientConnFactory


class _SubscribeProbeClient(WSClient):
    def __init__(
        self,
        options: WSClientOptions,
        connection_factory: ClientConnFactory,
        state_conn: psycopg.Connection[TupleRow],
    ) -> None:
        super().__init__(options, connection_factory, state_conn)
        self.started_streams: list[str] = []
        self.sent_subscribes: list[SubscribeFrame] = []

    def _make_applier(self, stream: str) -> StreamApplier:
        self.started_streams.append(stream)
        return super()._make_applier(stream)

    async def _send_frame(self, frame: object) -> None:
        if isinstance(frame, SubscribeFrame):
            self.sent_subscribes.append(frame)

    async def drive(self) -> None:
        async with trio.open_nursery() as nursery:
            self._nursery = nursery
            channel_list = await self._ensure_applier("channel-list")
            first = EventFrame(
                stream="channel-list",
                offset=1,
                kind="channel_added",
                payload={"id": "C_NEW", "name": "incident-88", "is_member": True},
            )
            await self._dispatch_frame(first)
            with trio.fail_after(5):
                while channel_list.health().applied_offset < 1:
                    await trio.sleep(0.01)

            replay = first.model_copy(update={"offset": 2})
            await self._dispatch_frame(replay)
            with trio.fail_after(5):
                while channel_list.health().applied_offset < 2:
                    await trio.sleep(0.01)

            for applier in self._appliers.values():
                await applier.close()
        self._nursery = None
        await self._pool.aclose()


def test_channel_added_starts_one_dynamic_subscription(
    client_conn_factory: ClientConnFactory,
) -> None:
    state_conn = client_conn_factory()
    client = _SubscribeProbeClient(
        WSClientOptions(server_url="ws://unused.test"),
        client_conn_factory,
        state_conn,
    )

    trio.run(client.drive)

    assert client.started_streams.count("channel:C_NEW") == 1
    channel_subscribes = [frame for frame in client.sent_subscribes if frame.stream == "channel:C_NEW"]
    assert channel_subscribes == [SubscribeFrame(stream="channel:C_NEW", since=0)]
    with state_conn.cursor() as cur:
        cur.execute("SELECT name, is_member FROM channels WHERE channel_id = 'C_NEW'")
        assert cur.fetchone() == ("incident-88", True)
