"""Tests for the Sprint 1C HTTP server (`/health`, `/metrics`)."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

import h11
import httpx
import pytest
import trio

from slack_fuse_server.http.dto import (
    BackfillMetrics,
    MetricsResponse,
    RateLimitBudget,
    SlackMetrics,
    StreamMetrics,
    SubscribersMetrics,
)
from slack_fuse_server.http.handlers import LivezDeps, ProbeDeps
from slack_fuse_server.http.metrics import MetricsSource
from slack_fuse_server.http.server import (
    HttpRequest,
    parse_listen_addr,
    route_request,
    serve_http_connection,
    serve_http_on_listeners,
)
from slack_fuse_server.slurper.probes import JOB_CHANNEL_INVENTORY, JOB_CHANNEL_NEWEST_MESSAGE
from slack_fuse_server.slurper.supervisor import TaskSupervisor


@dataclass(frozen=True, slots=True)
class StaticMetricsSource:
    payload: MetricsResponse

    def snapshot(self) -> MetricsResponse:
        return self.payload


@dataclass(slots=True)
class _RecordingProbeTrigger:
    accepted: bool = True
    calls: list[tuple[str | None, str | None]] | None = None

    def request(self, *, job_id: str | None = None, target: str | None = None) -> bool:
        if self.calls is None:
            self.calls = []
        self.calls.append((job_id, target))
        return self.accepted


def _sample_metrics() -> MetricsResponse:
    now = datetime(2026, 6, 8, 7, 0, 0, tzinfo=UTC)
    return MetricsResponse(
        server_started_at=now,
        slack=SlackMetrics(
            socket_mode_state="connected",
            last_event_at=now,
            rate_limit_budget=RateLimitBudget(remaining_pct=93),
            last_health_kind="slack_healthy",
        ),
        streams=[StreamMetrics(stream="users", head_offset=12, events_per_min=1)],
        backfill=BackfillMetrics(completed_count=3, aborted_count=1),
        subscribers=SubscribersMetrics(active_ws_connections=0),
    )


@asynccontextmanager
async def _running_server(metrics_source: MetricsSource, livez_deps: LivezDeps | None = None):
    listeners = await trio.open_tcp_listeners(0, host="127.0.0.1")
    sockname = cast(tuple[str, int], listeners[0].socket.getsockname())
    port = sockname[1]

    async def serve() -> None:
        await serve_http_on_listeners(listeners, metrics_source, livez_deps=livez_deps)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(serve)
        await trio.sleep(0.05)
        try:
            yield f"http://127.0.0.1:{port}"
        finally:
            nursery.cancel_scope.cancel()


def test_parse_listen_addr() -> None:
    assert parse_listen_addr("127.0.0.1:8765") == ("127.0.0.1", 8765)
    assert parse_listen_addr("[::1]:8765") == ("::1", 8765)


def test_route_request_health() -> None:
    response = route_request(
        HttpRequest(method="GET", target="/health"),
        metrics_source=StaticMetricsSource(_sample_metrics()),
    )
    assert response.status_code == 200
    assert response.body == b'{"ok":true}'


def test_route_request_metrics() -> None:
    payload = _sample_metrics()
    response = route_request(HttpRequest(method="GET", target="/metrics"), metrics_source=StaticMetricsSource(payload))
    assert response.status_code == 200
    assert MetricsResponse.model_validate_json(response.body) == payload


def test_route_request_livez_empty_registry() -> None:
    response = route_request(
        HttpRequest(method="GET", target="/livez"),
        metrics_source=StaticMetricsSource(_sample_metrics()),
        livez_deps=LivezDeps(supervisor=TaskSupervisor()),
    )

    assert response.status_code == 200
    assert response.body == b'{"phases":{}}'


def test_route_request_livez_all_phases_healthy() -> None:
    now = datetime(2026, 6, 28, 3, 14, 15, 123456, tzinfo=UTC)
    supervisor = TaskSupervisor(clock=lambda: now)
    supervisor.declare("socket", "connected_waiting_for_frame", deadline_s=None)
    supervisor.declare("refresh", "refreshing_channel", details={"channel_id": "C1"}, deadline_s=10)

    response = route_request(
        HttpRequest(method="GET", target="/livez"),
        metrics_source=StaticMetricsSource(_sample_metrics()),
        livez_deps=LivezDeps(supervisor=supervisor),
    )

    assert response.status_code == 200
    assert json.loads(response.body) == {
        "phases": {
            "socket": {
                "phase": "connected_waiting_for_frame",
                "details": {},
                "entered_at": "2026-06-28T03:14:15.123456+00:00",
                "deadline": None,
            },
            "refresh": {
                "phase": "refreshing_channel",
                "details": {"channel_id": "C1"},
                "entered_at": "2026-06-28T03:14:15.123456+00:00",
                "deadline": "2026-06-28T03:14:25.123456+00:00",
            },
        }
    }


def test_route_request_livez_one_overdue() -> None:
    current = datetime(2026, 6, 28, 3, 14, 15, tzinfo=UTC)
    supervisor = TaskSupervisor(clock=lambda: current)
    supervisor.declare("auto-backfill", "channel", details={"channel_id": "C1"}, deadline_s=5)
    current = datetime(2026, 6, 28, 3, 14, 21, tzinfo=UTC)

    response = route_request(
        HttpRequest(method="GET", target="/livez"),
        metrics_source=StaticMetricsSource(_sample_metrics()),
        livez_deps=LivezDeps(supervisor=supervisor),
    )

    assert response.status_code == 503
    assert json.loads(response.body) == {
        "overdue": [
            {
                "task_name": "auto-backfill",
                "phase": "channel",
                "details": {"channel_id": "C1"},
                "entered_at": "2026-06-28T03:14:15+00:00",
                "deadline": "2026-06-28T03:14:20+00:00",
            }
        ],
        "phases": {
            "auto-backfill": {
                "phase": "channel",
                "details": {"channel_id": "C1"},
                "entered_at": "2026-06-28T03:14:15+00:00",
                "deadline": "2026-06-28T03:14:20+00:00",
            }
        },
    }


def test_route_request_livez_mix_healthy_and_overdue_degrades() -> None:
    current = datetime(2026, 6, 28, 3, 14, 15, tzinfo=UTC)
    supervisor = TaskSupervisor(clock=lambda: current)
    supervisor.declare("socket", "connected_waiting_for_frame", deadline_s=None)
    supervisor.declare("catchup", "catching_up_channel", details={"channel_id": "C2"}, deadline_s=300)
    current = datetime(2026, 6, 28, 3, 19, 16, tzinfo=UTC)

    response = route_request(
        HttpRequest(method="GET", target="/livez"),
        metrics_source=StaticMetricsSource(_sample_metrics()),
        livez_deps=LivezDeps(supervisor=supervisor),
    )

    payload = json.loads(response.body)
    assert response.status_code == 503
    assert [item["task_name"] for item in payload["overdue"]] == ["catchup"]
    assert set(payload["phases"]) == {"socket", "catchup"}


def test_route_request_not_found() -> None:
    response = route_request(
        HttpRequest(method="GET", target="/nope"),
        metrics_source=StaticMetricsSource(_sample_metrics()),
    )
    assert response.status_code == 404
    assert response.body == b'{"error":"not_found"}'


def test_route_request_method_not_allowed() -> None:
    response = route_request(
        HttpRequest(method="POST", target="/health"),
        metrics_source=StaticMetricsSource(_sample_metrics()),
    )
    assert response.status_code == 405
    assert response.body == b'{"error":"method_not_allowed"}'


def test_route_request_probe_sweep_unwired_is_503() -> None:
    response = route_request(
        HttpRequest(method="POST", target="/probe-sweep"),
        metrics_source=StaticMetricsSource(_sample_metrics()),
    )

    assert response.status_code == 503
    assert response.body == b'{"error":"service_unavailable"}'


def test_route_request_probe_sweep_all_jobs_queues() -> None:
    trigger = _RecordingProbeTrigger()
    response = route_request(
        HttpRequest(method="POST", target="/probe-sweep"),
        metrics_source=StaticMetricsSource(_sample_metrics()),
        probe_deps=ProbeDeps(shared_secret=None, trigger=trigger),
    )

    assert response.status_code == 202
    assert json.loads(response.body) == {"status": "probe sweep queued"}
    assert trigger.calls == [(None, None)]


def test_route_request_probe_sweep_unknown_job_is_400() -> None:
    trigger = _RecordingProbeTrigger()
    response = route_request(
        HttpRequest(method="POST", target="/probe-sweep/not_a_job"),
        metrics_source=StaticMetricsSource(_sample_metrics()),
        probe_deps=ProbeDeps(shared_secret=None, trigger=trigger),
    )

    assert response.status_code == 400
    assert json.loads(response.body) == {"status": "unknown_job"}
    assert trigger.calls is None


def test_route_request_probe_sweep_workspace_job_rejects_target() -> None:
    trigger = _RecordingProbeTrigger()
    response = route_request(
        HttpRequest(method="POST", target=f"/probe-sweep/{JOB_CHANNEL_INVENTORY}/C123"),
        metrics_source=StaticMetricsSource(_sample_metrics()),
        probe_deps=ProbeDeps(shared_secret=None, trigger=trigger),
    )

    assert response.status_code == 400
    assert json.loads(response.body) == {"status": "bad_target"}
    assert trigger.calls is None


def test_route_request_probe_sweep_target_queues() -> None:
    trigger = _RecordingProbeTrigger()
    response = route_request(
        HttpRequest(method="POST", target=f"/probe-sweep/{JOB_CHANNEL_NEWEST_MESSAGE}/C123"),
        metrics_source=StaticMetricsSource(_sample_metrics()),
        probe_deps=ProbeDeps(shared_secret=None, trigger=trigger),
    )

    assert response.status_code == 202
    assert trigger.calls == [(JOB_CHANNEL_NEWEST_MESSAGE, "C123")]


def test_route_request_probe_sweep_target_rejects_whitespace() -> None:
    trigger = _RecordingProbeTrigger()
    response = route_request(
        HttpRequest(method="POST", target=f"/probe-sweep/{JOB_CHANNEL_NEWEST_MESSAGE}/C%20123"),
        metrics_source=StaticMetricsSource(_sample_metrics()),
        probe_deps=ProbeDeps(shared_secret=None, trigger=trigger),
    )

    assert response.status_code == 400
    assert json.loads(response.body) == {"status": "bad_target"}
    assert trigger.calls is None


def test_route_request_probe_sweep_busy_is_409() -> None:
    trigger = _RecordingProbeTrigger(accepted=False)
    response = route_request(
        HttpRequest(method="POST", target=f"/probe-sweep/{JOB_CHANNEL_NEWEST_MESSAGE}"),
        metrics_source=StaticMetricsSource(_sample_metrics()),
        probe_deps=ProbeDeps(shared_secret=None, trigger=trigger),
    )

    assert response.status_code == 409
    assert json.loads(response.body) == {"status": "probe sweep already busy"}


@pytest.mark.trio
async def test_http_server_health_endpoint() -> None:
    async with _running_server(StaticMetricsSource(_sample_metrics())) as base_url, httpx.AsyncClient(
        base_url=base_url
    ) as client:
        response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"ok": True}


@pytest.mark.trio
async def test_http_server_livez_endpoint() -> None:
    supervisor = TaskSupervisor()
    supervisor.declare("socket", "connected_waiting_for_frame", deadline_s=None)
    async with _running_server(
        StaticMetricsSource(_sample_metrics()),
        livez_deps=LivezDeps(supervisor=supervisor),
    ) as base_url, httpx.AsyncClient(base_url=base_url) as client:
        response = await client.get("/livez")
    assert response.status_code == 200
    assert response.json()["phases"]["socket"]["phase"] == "connected_waiting_for_frame"


@pytest.mark.trio
async def test_http_server_metrics_endpoint_round_trip() -> None:
    expected = _sample_metrics()
    async with _running_server(StaticMetricsSource(expected)) as base_url, httpx.AsyncClient(
        base_url=base_url
    ) as client:
        response = await client.get("/metrics")
    assert response.status_code == 200
    parsed = MetricsResponse.model_validate(response.json())
    assert parsed == expected


class _FailingStream(trio.abc.Stream):
    """A stream that raises BrokenResourceError on send_all — simulates a
    client that hung up mid-response (kubelet probe timeout, curl abort,
    projector connection drop). The handler must not propagate the exception
    to the accept loop, or the whole process crashes (2026-07-05 prod).
    """

    def __init__(self, raise_on_send: BaseException, request_bytes: bytes) -> None:
        self._raise_on_send = raise_on_send
        self._request_bytes = request_bytes
        self.closed = False

    async def send_all(self, data: bytes | bytearray | memoryview) -> None:
        raise self._raise_on_send

    async def wait_send_all_might_not_block(self) -> None:  # pragma: no cover - unused
        return None

    async def receive_some(self, max_bytes: int | None = None) -> bytes:
        if not self._request_bytes:
            return b""
        chunk = self._request_bytes if max_bytes is None else self._request_bytes[:max_bytes]
        self._request_bytes = self._request_bytes[len(chunk):]
        return chunk

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.trio
@pytest.mark.parametrize(
    "exc",
    [
        trio.BrokenResourceError("simulated peer hangup"),
        trio.ClosedResourceError("simulated peer close"),
    ],
)
async def test_serve_connection_swallows_client_hangup(
    exc: BaseException, caplog: pytest.LogCaptureFixture
) -> None:
    """Client disconnecting mid-response must not crash the accept loop.

    Regression: 2026-07-05 saw slack-fuse-server restart 43 times in 32h
    because ``trio.BrokenResourceError`` from ``_send_response`` ->
    ``stream.send_all`` escaped ``_serve_connection`` -> the nursery ->
    the trio.serve_tcp task, exiting the whole process with exit code 1.
    """
    request = b"GET /health HTTP/1.1\r\nHost: x\r\n\r\n"
    stream = _FailingStream(raise_on_send=exc, request_bytes=request)

    with caplog.at_level("INFO", logger="slack_fuse_server.http.server"):
        await serve_http_connection(stream, metrics_source=StaticMetricsSource(_sample_metrics()))

    assert stream.closed, "stream should be closed even when send fails"
    assert any("connection dropped by peer" in rec.message for rec in caplog.records), (
        "expected an INFO log record noting the peer hangup"
    )


@pytest.mark.trio
async def test_head_request_returns_headers_only_no_body() -> None:
    """HEAD responses carry the same headers as GET but MUST NOT include a
    body (RFC 9110 §9.3.2). h11 enforces this and would raise
    ``LocalProtocolError('Too much data for declared Content-Length')`` if
    we sent the body — which we did until 2026-07-23. Root cause of the
    prod crash-loop; containment landed same day.
    """
    request = b"HEAD /health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"

    class _Capture(trio.abc.Stream):
        def __init__(self) -> None:
            self.sent = bytearray()
            self._req = request
            self.closed = False

        async def send_all(self, data: bytes | bytearray | memoryview) -> None:
            self.sent.extend(bytes(data))

        async def wait_send_all_might_not_block(self) -> None:  # pragma: no cover
            return None

        async def receive_some(self, max_bytes: int | None = None) -> bytes:
            if not self._req:
                return b""
            chunk = self._req if max_bytes is None else self._req[:max_bytes]
            self._req = self._req[len(chunk):]
            return chunk

        async def aclose(self) -> None:
            self.closed = True

    stream = _Capture()
    # Must not raise. The old code raised LocalProtocolError inside
    # _send_response for any HEAD request because it always sent Data.
    await serve_http_connection(stream, metrics_source=StaticMetricsSource(_sample_metrics()))
    assert stream.closed
    text = bytes(stream.sent).decode("ascii", errors="replace")
    # A response status line is present (routing sees HEAD as non-GET → 405
    # today; whether we later relax that is a routing-side concern. Body-less
    # is the invariant this test pins).
    assert text.startswith("HTTP/1.1 ")
    # Content-Length reflects what a GET would have returned, per RFC.
    assert "content-length: " in text.lower()
    # And the response body itself MUST NOT appear (RFC 9110 §9.3.2).
    header_end = text.find("\r\n\r\n")
    assert header_end > 0
    body_bytes = text[header_end + 4:]
    assert body_bytes == "", f"HEAD response must have no body, got {body_bytes!r}"


@pytest.mark.trio
async def test_serve_connection_swallows_local_protocol_error(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A per-response serialization bug (h11 LocalProtocolError) must not
    escape ``_serve_connection`` — it would take down the trio nursery and
    kill the whole process, including slurper/socket/webhook siblings.

    Regression: 2026-07-23 saw the pod crash-loop after a wire ``/streams/*/
    snapshot`` response tripped ``h11._util.LocalProtocolError: Too much data
    for declared Content-Length``. That serialization bug is real and worth
    fixing on its own; but the containment invariant (one bad request must
    never nuke the process) is orthogonal, and this is what pins it.
    """
    async def raising_send(*_args: object, **_kwargs: object) -> None:  # noqa: RUF029 - matches the async signature of _send_response
        raise h11.LocalProtocolError("Too much data for declared Content-Length")

    monkeypatch.setattr("slack_fuse_server.http.server._send_response", raising_send)

    request = b"GET /health HTTP/1.1\r\nHost: x\r\n\r\n"
    stream = _CapturingStream(request_bytes=request)

    with caplog.at_level("ERROR", logger="slack_fuse_server.http.server"):
        await serve_http_connection(stream, metrics_source=StaticMetricsSource(_sample_metrics()))

    assert stream.closed, "stream should be closed even when send raises"
    assert any(
        "serialization protocol error" in rec.message and "/health" in rec.message for rec in caplog.records
    ), "expected an ERROR log naming the request target"


class _CapturingStream(trio.abc.Stream):
    """Stream that hands back a fixed request; send_all is a no-op sink."""

    def __init__(self, *, request_bytes: bytes) -> None:
        self._request_bytes = request_bytes
        self.closed = False

    async def send_all(self, data: bytes | bytearray | memoryview) -> None:
        return None

    async def wait_send_all_might_not_block(self) -> None:  # pragma: no cover - unused
        return None

    async def receive_some(self, max_bytes: int | None = None) -> bytes:
        if not self._request_bytes:
            return b""
        chunk = self._request_bytes if max_bytes is None else self._request_bytes[:max_bytes]
        self._request_bytes = self._request_bytes[len(chunk):]
        return chunk

    async def aclose(self) -> None:
        self.closed = True
