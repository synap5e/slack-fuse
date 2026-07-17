# pyright: reportPrivateUsage=false
"""Legacy always_blocked_channel_ids startup handling.

Behaviour changed 2026-07-16: the code used to unconditionally POST every
config entry to /blocked-channels on every startup, which silently reversed
any operator DELETE via _control/blocked_channels (unblocked ID → re-blocked
next boot). Now the code is inert with respect to server state — it fetches
the SSOT (server /blocked-channels), classifies each config entry, and logs
an actionable warning per class. Never POSTs.

FINDING-13 (2026-07-17): the classifier was parsing the wrong JSON key
(``blocked_channels`` instead of the server's ``blocked``). Tests passed
because the fixture served the wrong shape too. Fix: point the classifier
at the canonical ``blocked_channel_ids_from_payload`` helper AND make this
fixture serve the real server shape.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import httpx
import pytest

from slack_fuse.__main__ import _migrate_legacy_always_blocked


@dataclass
class _MockServer:
    transport: httpx.MockTransport
    calls: list[httpx.Request]


def _handler_returning_blocked(blocked_ids: list[str]) -> _MockServer:
    """Return the SAME shape the real server emits: ``{"blocked": [...]}``.

    Regression pin for FINDING-13.
    """
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.method == "GET" and request.url.path == "/blocked-channels":
            return httpx.Response(
                200,
                json={
                    "blocked": [
                        {"channel_id": cid, "blocked_at": "2026-01-01T00:00:00Z"} for cid in blocked_ids
                    ]
                },
            )
        return httpx.Response(500, json={"error": "unexpected route"})

    return _MockServer(transport=httpx.MockTransport(handler), calls=calls)


def _run(server: _MockServer, config_ids: frozenset[str], caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING):
        client = httpx.Client(transport=server.transport)
        _migrate_legacy_always_blocked(
            client,
            "http://srv",
            config_ids,
            shared_secret="sek",
            log=logging.getLogger("test"),
        )


def test_never_posts_on_startup(caplog: pytest.LogCaptureFixture) -> None:
    """The old bug was POSTing every config entry unconditionally. Regression pin."""
    server = _handler_returning_blocked(["CA", "CB"])
    _run(server, frozenset({"CA", "CB"}), caplog)
    methods = [r.method for r in server.calls]
    assert "POST" not in methods, f"legacy migration should never POST; saw {methods}"
    assert methods == ["GET"], methods


def test_already_server_blocked_warns_and_does_nothing(caplog: pytest.LogCaptureFixture) -> None:
    server = _handler_returning_blocked(["CA", "CB", "CC"])
    _run(server, frozenset({"CA", "CB"}), caplog)
    combined = " ".join(rec.getMessage() for rec in caplog.records)
    assert "already server-side blocked" in combined
    assert "'CA'" in combined and "'CB'" in combined


def test_orphan_config_entry_warns_but_does_not_re_add(caplog: pytest.LogCaptureFixture) -> None:
    """Regression pin for the footgun: an ID in config.toml but NOT server-side
    blocked (operator ran `echo <id> > _control/blocked_channels` to unblock it)
    used to be re-POSTed on every startup. Now we only warn.
    """
    server = _handler_returning_blocked([])
    _run(server, frozenset({"CGONE"}), caplog)
    assert [r.method for r in server.calls] == ["GET"], "orphan should not trigger a POST"
    combined = " ".join(rec.getMessage() for rec in caplog.records)
    assert "'CGONE'" in combined
    assert "unblocked them" in combined.lower() or "not server-side blocked" in combined


def test_empty_config_is_no_op(caplog: pytest.LogCaptureFixture) -> None:
    server = _handler_returning_blocked(["CA"])
    _run(server, frozenset(), caplog)
    assert server.calls == []
    assert caplog.records == []


def test_server_unreachable_leaves_config_alone(caplog: pytest.LogCaptureFixture) -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="")

    transport = httpx.MockTransport(handler)
    with caplog.at_level(logging.WARNING):
        client = httpx.Client(transport=transport)
        _migrate_legacy_always_blocked(
            client,
            "http://srv",
            frozenset({"CA"}),
            shared_secret="sek",
            log=logging.getLogger("test"),
        )
    combined = " ".join(rec.getMessage() for rec in caplog.records)
    assert "cannot classify" in combined


def test_regression_finding_13_wrong_json_key_would_fail_here(caplog: pytest.LogCaptureFixture) -> None:
    """The exact case FINDING-13 (2026-07-17) surfaced: an ID that IS
    server-side blocked must be classified as ``already server-side blocked``.
    Pre-fix, the classifier parsed the wrong key and this branch never fired —
    every config entry looked orphan. With the fix, ``CA`` classifies correctly.
    """
    server = _handler_returning_blocked(["CA"])
    _run(server, frozenset({"CA"}), caplog)
    combined = " ".join(rec.getMessage() for rec in caplog.records)
    assert "already server-side blocked" in combined
    assert "unblocked them" not in combined.lower(), (
        "CA should classify as already-blocked, NOT as an orphan (that was the bug)."
    )
