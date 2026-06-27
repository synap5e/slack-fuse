"""Composition tests for `slack_fuse_server.dispatch`.

The RFC requires `/health`, `/metrics`, and `/ws` on a single listen address.
These tests bind one TCP port, run the shared dispatch over it, and exercise all
three endpoints — proving the same-port Upgrade dispatch routes HTTP and
WebSocket correctly and that `/metrics` reflects the live WS subscriber.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import cast

import httpx
import psycopg
import pytest
import trio
from psycopg.conninfo import make_conninfo
from psycopg.rows import TupleRow
from psycopg.types.json import Jsonb
from trio_websocket import WebSocketConnection, open_websocket

import slack_fuse_server.migrations as server_migrations
from slack_fuse.migrations.runner import apply_migrations
from slack_fuse_server._json import JsonObject
from slack_fuse_server.dispatch import serve_dispatch_on_listeners
from slack_fuse_server.http.dto import MetricsResponse
from slack_fuse_server.http.handlers import BackfillDeps, BlockedChannelsDeps, GapsDeps, OriginalsDeps, RefreshDeps
from slack_fuse_server.http.metrics import MetricsAggregator, SubscriberSnapshot
from slack_fuse_server.wire.frames import CaughtUpFrame, EventFrame, Frame, FrameAdapter, SubscribeFrame
from slack_fuse_server.wire.server import WireServer, WireServerOptions

pytestmark = pytest.mark.trio

_SERVER_MIGRATIONS = Path(server_migrations.__file__).parent
_NO_HEARTBEAT_S = 3_600.0


def _prepare_database(pg_conn: psycopg.Connection[TupleRow]) -> str:
    apply_migrations(pg_conn, _SERVER_MIGRATIONS)
    with pg_conn.cursor() as cur:
        cur.execute("SELECT current_schema()")
        row = cur.fetchone()
    assert row is not None
    return make_conninfo(pg_conn.info.dsn, options=f"-c search_path={row[0]}")


def _seed_stream(pg_conn: psycopg.Connection[TupleRow], stream: str, payloads: list[JsonObject]) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO stream_heads (stream, next_offset) VALUES (%s, %s)",
            (stream, len(payloads) + 1),
        )
        for offset, payload in enumerate(payloads, start=1):
            cur.execute(
                "INSERT INTO events (stream, offset_in_stream, kind, ts, payload) VALUES (%s, %s, %s, %s, %s)",
                (stream, offset, "message", payload.get("ts"), Jsonb(payload)),
            )
    pg_conn.commit()


def _build_metrics(database_url: str, wire_server: WireServer) -> MetricsAggregator:
    def _subscribers() -> list[SubscriberSnapshot]:
        return [
            SubscriberSnapshot(
                client_id=info.client_id,
                connected_since=info.connected_since,
                subscriptions=info.subscriptions,
            )
            for info in wire_server.connection_infos()
        ]

    return MetricsAggregator(
        database_url=database_url,
        server_started_at=datetime(2026, 6, 8, tzinfo=UTC),
        socket_mode_state=lambda: "connected",
        subscribers=_subscribers,
    )


@asynccontextmanager
async def _running_dispatch(  # noqa: PLR0913 - test helper mirrors dispatch dependency wiring.
    database_url: str,
    *,
    gaps_deps: GapsDeps | None = None,
    originals_deps: OriginalsDeps | None = None,
    refresh_deps: RefreshDeps | None = None,
    blocked_channels_deps: BlockedChannelsDeps | None = None,
    backfill_deps: BackfillDeps | None = None,
) -> AsyncIterator[tuple[int, WireServer]]:
    wire_server = WireServer(
        database_url,
        options=WireServerOptions(heartbeat_interval_s=_NO_HEARTBEAT_S, client_timeout_s=_NO_HEARTBEAT_S),
    )
    metrics = _build_metrics(database_url, wire_server)
    listeners = await trio.open_tcp_listeners(0, host="127.0.0.1")
    port = cast(tuple[str, int], listeners[0].socket.getsockname())[1]
    handler = partial(
        serve_dispatch_on_listeners,
        listeners,
        wire_server=wire_server,
        metrics_source=metrics,
        gaps_deps=gaps_deps,
        originals_deps=originals_deps,
        refresh_deps=refresh_deps,
        blocked_channels_deps=blocked_channels_deps,
        backfill_deps=backfill_deps,
    )
    async with trio.open_nursery() as nursery:
        nursery.start_soon(handler)
        await trio.sleep(0.05)
        try:
            yield port, wire_server
        finally:
            nursery.cancel_scope.cancel()


class _FakeRefreshTrigger:
    """Test stub mirroring :class:`RefreshTrigger` without the trio
    rendezvous channel. ``ready=False`` simulates "consumer is busy" so
    the next request gets 409."""

    def __init__(self, *, ready: bool = True) -> None:
        self.ready = ready
        self.calls = 0
        self.channel_calls: list[str] = []

    def request(self) -> bool:
        self.calls += 1
        return self.ready

    def request_channel(self, channel_id: str) -> bool:
        self.channel_calls.append(channel_id)
        return self.ready


class _FakeBackfillTrigger:
    def __init__(self, *, ready: bool = True) -> None:
        self.ready = ready
        self.channel_calls: list[str] = []

    def request_channel(self, channel_id: str) -> bool:
        self.channel_calls.append(channel_id)
        return self.ready


async def _recv_frame(ws: WebSocketConnection, *, timeout_s: float = 1.0) -> Frame:
    with trio.fail_after(timeout_s):
        return FrameAdapter.validate_json(await ws.get_message())


async def test_health_served_on_shared_port(pg_conn: psycopg.Connection[TupleRow]) -> None:
    database_url = _prepare_database(pg_conn)
    async with (
        _running_dispatch(database_url) as (port, _wire),
        httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client,
    ):
        response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"ok": True}


async def test_metrics_served_on_shared_port(pg_conn: psycopg.Connection[TupleRow]) -> None:
    database_url = _prepare_database(pg_conn)
    _seed_stream(pg_conn, "channel:C1", [{"ts": "1.000001", "text": "one"}])
    async with (
        _running_dispatch(database_url) as (port, _wire),
        httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client,
    ):
        response = await client.get("/metrics")
    assert response.status_code == 200
    parsed = MetricsResponse.model_validate(response.json())
    assert parsed.subscribers.active_ws_connections == 0
    assert any(stream.stream == "channel:C1" for stream in parsed.streams)


async def test_unknown_path_returns_404_on_shared_port(pg_conn: psycopg.Connection[TupleRow]) -> None:
    database_url = _prepare_database(pg_conn)
    async with (
        _running_dispatch(database_url) as (port, _wire),
        httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client,
    ):
        response = await client.get("/nope")
    assert response.status_code == 404


async def test_gaps_endpoint_forwards_deps_through_dispatch(
    pg_conn: psycopg.Connection[TupleRow],
) -> None:
    """Regression for the 2026-06-26 wiring leak: ``serve_connection`` in
    dispatch.py accepted ``gaps_deps`` as a kwarg but didn't forward it to
    ``serve_http_connection``, so ``GET /gaps`` always returned 503
    ``service_unavailable`` even with deps wired upstream. The whole point of
    this test is to walk the EXACT call chain the production slurper uses
    (dispatch → connection → http → route) and prove deps survive the trip.

    The same shape catches future regressions when someone adds a new deps
    kwarg to one layer but forgets a downstream forwarder.
    """
    database_url = _prepare_database(pg_conn)
    gaps_deps = GapsDeps(database_url=database_url)
    async with (
        _running_dispatch(database_url, gaps_deps=gaps_deps) as (port, _wire),
        httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client,
    ):
        response = await client.get("/gaps")
    # The body might say "No gaps detected" — what we care about is that
    # the route handler ran with deps, NOT that it returned 503 because
    # the dispatch dropped the kwarg.
    assert response.status_code == 200, (
        f"got {response.status_code}; dispatch wiring likely dropped gaps_deps. "
        f"body={response.text[:200]}"
    )
    assert response.headers["content-type"].startswith("text/markdown")


async def test_gaps_endpoint_503_without_deps_proves_503_is_the_failure_mode(
    pg_conn: psycopg.Connection[TupleRow],
) -> None:
    """Pin the negative side: when gaps_deps isn't wired AT ALL (production
    bug we want to catch elsewhere), the route returns 503 — not 404, not
    a crash. This pins the production diagnostic so the dispatch-wiring
    test above doesn't accidentally pass via the 404 path if the route
    block ever gets reordered.
    """
    database_url = _prepare_database(pg_conn)
    async with (
        _running_dispatch(database_url, gaps_deps=None) as (port, _wire),
        httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client,
    ):
        response = await client.get("/gaps")
    assert response.status_code == 503


async def test_refresh_endpoint_returns_202_when_accepted(
    pg_conn: psycopg.Connection[TupleRow],
) -> None:
    """``POST /refresh-channels`` with the correct shared secret returns
    202 and bumps the trigger's call counter — the actual sweep runs
    in a background consumer in production; here the fake just records
    that the dispatch chain forwarded the request."""
    database_url = _prepare_database(pg_conn)
    trigger = _FakeRefreshTrigger(ready=True)
    deps = RefreshDeps(shared_secret="test-secret", trigger=trigger)
    async with (
        _running_dispatch(database_url, refresh_deps=deps) as (port, _wire),
        httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client,
    ):
        response = await client.post(
            "/refresh-channels",
            headers={"Authorization": "Bearer test-secret"},
        )
    assert response.status_code == 202
    assert response.json() == {"status": "refresh queued"}
    assert trigger.calls == 1


async def test_refresh_endpoint_returns_409_when_busy(
    pg_conn: psycopg.Connection[TupleRow],
) -> None:
    """``ready=False`` simulates "consumer already running a sweep". The
    endpoint must return 409, not queue a second one — keeps the API
    cost bounded under bursty hammering."""
    database_url = _prepare_database(pg_conn)
    trigger = _FakeRefreshTrigger(ready=False)
    deps = RefreshDeps(shared_secret="test-secret", trigger=trigger)
    async with (
        _running_dispatch(database_url, refresh_deps=deps) as (port, _wire),
        httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client,
    ):
        response = await client.post(
            "/refresh-channels",
            headers={"Authorization": "Bearer test-secret"},
        )
    assert response.status_code == 409
    assert response.json() == {"status": "refresh already in progress"}


async def test_refresh_endpoint_rejects_missing_secret(
    pg_conn: psycopg.Connection[TupleRow],
) -> None:
    database_url = _prepare_database(pg_conn)
    trigger = _FakeRefreshTrigger()
    deps = RefreshDeps(shared_secret="test-secret", trigger=trigger)
    async with (
        _running_dispatch(database_url, refresh_deps=deps) as (port, _wire),
        httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client,
    ):
        response = await client.post("/refresh-channels")
    assert response.status_code == 401
    # The auth check fires BEFORE the trigger — trigger.request() must
    # NOT have been called.
    assert trigger.calls == 0


async def test_refresh_endpoint_rejects_wrong_secret(
    pg_conn: psycopg.Connection[TupleRow],
) -> None:
    database_url = _prepare_database(pg_conn)
    trigger = _FakeRefreshTrigger()
    deps = RefreshDeps(shared_secret="test-secret", trigger=trigger)
    async with (
        _running_dispatch(database_url, refresh_deps=deps) as (port, _wire),
        httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client,
    ):
        response = await client.post(
            "/refresh-channels",
            headers={"Authorization": "Bearer wrong"},
        )
    assert response.status_code == 401
    assert trigger.calls == 0


async def test_refresh_endpoint_accepts_x_slack_fuse_secret_header(
    pg_conn: psycopg.Connection[TupleRow],
) -> None:
    """Backwards-compat with the WS auth shape — both header forms work."""
    database_url = _prepare_database(pg_conn)
    trigger = _FakeRefreshTrigger()
    deps = RefreshDeps(shared_secret="test-secret", trigger=trigger)
    async with (
        _running_dispatch(database_url, refresh_deps=deps) as (port, _wire),
        httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client,
    ):
        response = await client.post(
            "/refresh-channels",
            headers={"X-Slack-Fuse-Secret": "test-secret"},
        )
    assert response.status_code == 202


async def test_refresh_endpoint_unauthenticated_when_no_secret_configured(
    pg_conn: psycopg.Connection[TupleRow],
) -> None:
    """If the server isn't configured with a shared secret, the endpoint
    accepts any request. Matches the WS auth's shape — and matches the
    fact that on a no-secret deploy, the WS is also wide open."""
    database_url = _prepare_database(pg_conn)
    trigger = _FakeRefreshTrigger()
    deps = RefreshDeps(shared_secret=None, trigger=trigger)
    async with (
        _running_dispatch(database_url, refresh_deps=deps) as (port, _wire),
        httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client,
    ):
        response = await client.post("/refresh-channels")
    assert response.status_code == 202


async def test_refresh_endpoint_returns_503_without_deps(
    pg_conn: psycopg.Connection[TupleRow],
) -> None:
    """No deps wired → 503 (the standard dispatch-missing failure mode),
    NOT 404. Same shape as the existing gaps endpoint regression test."""
    database_url = _prepare_database(pg_conn)
    async with (
        _running_dispatch(database_url, refresh_deps=None) as (port, _wire),
        httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client,
    ):
        response = await client.post("/refresh-channels")
    assert response.status_code == 503


async def test_refresh_channel_endpoint_routes_channel_id(
    pg_conn: psycopg.Connection[TupleRow],
) -> None:
    """``POST /refresh-channels/{channel_id}`` reaches the per-channel
    branch of the trigger (``request_channel``), not the workspace one."""
    database_url = _prepare_database(pg_conn)
    trigger = _FakeRefreshTrigger(ready=True)
    deps = RefreshDeps(shared_secret=None, trigger=trigger)
    async with (
        _running_dispatch(database_url, refresh_deps=deps) as (port, _wire),
        httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client,
    ):
        response = await client.post("/refresh-channels/C0ALLT6Q3SQ")
    assert response.status_code == 202
    assert response.json() == {"status": "refresh queued for C0ALLT6Q3SQ"}
    assert trigger.channel_calls == ["C0ALLT6Q3SQ"]
    # Workspace path NOT called.
    assert trigger.calls == 0


async def test_refresh_channel_endpoint_returns_409_when_busy(
    pg_conn: psycopg.Connection[TupleRow],
) -> None:
    database_url = _prepare_database(pg_conn)
    trigger = _FakeRefreshTrigger(ready=False)
    deps = RefreshDeps(shared_secret=None, trigger=trigger)
    async with (
        _running_dispatch(database_url, refresh_deps=deps) as (port, _wire),
        httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client,
    ):
        response = await client.post("/refresh-channels/C0ALLT6Q3SQ")
    assert response.status_code == 409


async def test_refresh_channel_endpoint_requires_secret_when_configured(
    pg_conn: psycopg.Connection[TupleRow],
) -> None:
    database_url = _prepare_database(pg_conn)
    trigger = _FakeRefreshTrigger()
    deps = RefreshDeps(shared_secret="s3cret", trigger=trigger)
    async with (
        _running_dispatch(database_url, refresh_deps=deps) as (port, _wire),
        httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client,
    ):
        response = await client.post("/refresh-channels/C0ALLT6Q3SQ")
    assert response.status_code == 401
    assert trigger.channel_calls == []


async def test_blocked_channels_endpoint_idempotent_and_requires_auth(
    pg_conn: psycopg.Connection[TupleRow],
) -> None:
    database_url = _prepare_database(pg_conn)
    deps = BlockedChannelsDeps(shared_secret="test-secret", database_url=database_url)
    async with (
        _running_dispatch(database_url, blocked_channels_deps=deps) as (port, _wire),
        httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client,
    ):
        unauth = await client.get("/blocked-channels")
        first = await client.post(
            "/blocked-channels",
            headers={"Authorization": "Bearer test-secret"},
            json={"channel_id": "C0BLOCK", "reason": "noisy"},
        )
        second = await client.post(
            "/blocked-channels",
            headers={"Authorization": "Bearer test-secret"},
            json={"channel_id": "C0BLOCK", "reason": "ignored"},
        )
        listed = await client.get(
            "/blocked-channels",
            headers={"X-Slack-Fuse-Secret": "test-secret"},
        )

    assert unauth.status_code == 401
    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["channel_id"] == "C0BLOCK"
    assert second.json()["reason"] == "noisy"
    assert listed.status_code == 200
    assert listed.json()["blocked"] == [second.json()]


async def test_delete_blocked_channels_idempotent(
    pg_conn: psycopg.Connection[TupleRow],
) -> None:
    database_url = _prepare_database(pg_conn)
    deps = BlockedChannelsDeps(shared_secret=None, database_url=database_url)
    async with (
        _running_dispatch(database_url, blocked_channels_deps=deps) as (port, _wire),
        httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client,
    ):
        first = await client.delete("/blocked-channels/C0BLOCK")
        second = await client.delete("/blocked-channels/C0BLOCK")

    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json() == {"status": "unblocked", "channel_id": "C0BLOCK"}


async def test_backfill_channel_endpoint_auth_and_rejects_blocked(
    pg_conn: psycopg.Connection[TupleRow],
) -> None:
    database_url = _prepare_database(pg_conn)
    blocked_deps = BlockedChannelsDeps(shared_secret="test-secret", database_url=database_url)
    trigger = _FakeBackfillTrigger()
    backfill_deps = BackfillDeps(shared_secret="test-secret", database_url=database_url, trigger=trigger)
    async with (
        _running_dispatch(
            database_url,
            blocked_channels_deps=blocked_deps,
            backfill_deps=backfill_deps,
        ) as (port, _wire),
        httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client,
    ):
        unauth = await client.post("/backfill-channel/C0BLOCK")
        _ = await client.post(
            "/blocked-channels",
            headers={"Authorization": "Bearer test-secret"},
            json={"channel_id": "C0BLOCK"},
        )
        blocked = await client.post(
            "/backfill-channel/C0BLOCK",
            headers={"Authorization": "Bearer test-secret"},
        )

    assert unauth.status_code == 401
    assert blocked.status_code == 409
    assert blocked.json() == {"status": "blocked"}
    assert trigger.channel_calls == []


async def test_backfill_channel_endpoint_queues_when_allowed(
    pg_conn: psycopg.Connection[TupleRow],
) -> None:
    database_url = _prepare_database(pg_conn)
    trigger = _FakeBackfillTrigger()
    deps = BackfillDeps(shared_secret=None, database_url=database_url, trigger=trigger)
    async with (
        _running_dispatch(database_url, backfill_deps=deps) as (port, _wire),
        httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client,
    ):
        response = await client.post("/backfill-channel/C0OK")

    assert response.status_code == 202
    assert response.json() == {"status": "backfill queued for C0OK"}
    assert trigger.channel_calls == ["C0OK"]


async def test_refresh_channel_endpoint_rejects_blocked(
    pg_conn: psycopg.Connection[TupleRow],
) -> None:
    database_url = _prepare_database(pg_conn)
    with pg_conn.cursor() as cur:
        cur.execute("INSERT INTO blocked_channels (channel_id) VALUES ('C0BLOCK')")
    pg_conn.commit()
    trigger = _FakeRefreshTrigger()
    deps = RefreshDeps(shared_secret=None, trigger=trigger, database_url=database_url)
    async with (
        _running_dispatch(database_url, refresh_deps=deps) as (port, _wire),
        httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client,
    ):
        response = await client.post("/refresh-channels/C0BLOCK")

    assert response.status_code == 409
    assert response.json() == {"status": "channel blocked"}
    assert trigger.channel_calls == []


async def test_refresh_endpoint_rejects_non_post(
    pg_conn: psycopg.Connection[TupleRow],
) -> None:
    """GET /refresh-channels is 405 — explicitly verbs the contract.
    Mutating endpoints don't accept idempotent verbs."""
    database_url = _prepare_database(pg_conn)
    trigger = _FakeRefreshTrigger()
    deps = RefreshDeps(shared_secret=None, trigger=trigger)
    async with (
        _running_dispatch(database_url, refresh_deps=deps) as (port, _wire),
        httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client,
    ):
        response = await client.get("/refresh-channels")
    assert response.status_code == 405


async def test_websocket_served_on_shared_port(pg_conn: psycopg.Connection[TupleRow]) -> None:
    database_url = _prepare_database(pg_conn)
    stream = "channel:C1"
    _seed_stream(pg_conn, stream, [{"ts": "1.000001", "text": "one"}, {"ts": "2.000001", "text": "two"}])
    async with (
        _running_dispatch(database_url) as (port, _wire),
        open_websocket("127.0.0.1", port, "/ws", use_ssl=False) as ws,
    ):
        await ws.send_message(SubscribeFrame(stream=stream, since=0).model_dump_json())
        first = await _recv_frame(ws)
        second = await _recv_frame(ws)
        caught_up = await _recv_frame(ws)
    assert isinstance(first, EventFrame)
    assert first.offset == 1
    assert isinstance(second, EventFrame)
    assert second.offset == 2
    assert isinstance(caught_up, CaughtUpFrame)
    assert caught_up.head_offset == 2


async def test_http_and_ws_coexist_on_one_port(pg_conn: psycopg.Connection[TupleRow]) -> None:
    """A live WS connection and HTTP requests share the same port; /metrics sees the subscriber."""
    database_url = _prepare_database(pg_conn)
    stream = "channel:C1"
    _seed_stream(pg_conn, stream, [{"ts": "1.000001", "text": "one"}])
    async with _running_dispatch(database_url) as (port, _wire):
        base_url = f"http://127.0.0.1:{port}"
        async with open_websocket("127.0.0.1", port, "/ws", use_ssl=False) as ws:
            await ws.send_message(SubscribeFrame(stream=stream, since=0).model_dump_json())
            assert isinstance(await _recv_frame(ws), EventFrame)
            assert isinstance(await _recv_frame(ws), CaughtUpFrame)

            # HTTP still works while the WS connection is open, and reports it.
            async with httpx.AsyncClient(base_url=base_url) as client:
                health = await client.get("/health")
                metrics = await client.get("/metrics")
    assert health.status_code == 200
    assert metrics.status_code == 200
    parsed = MetricsResponse.model_validate(metrics.json())
    assert parsed.subscribers.active_ws_connections == 1
    assert parsed.subscribers.by_client[0].subscriptions == 1
