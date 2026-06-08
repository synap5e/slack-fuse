"""Fake Slack Web API transport. Re-exported from the package `__init__`."""

from __future__ import annotations

import json
from pathlib import Path

import httpx

from slack_fuse.models import JsonObject

_FIXTURES_DIR = Path(__file__).parent / "fixtures"


def load_fixtures() -> dict[str, JsonObject]:
    """Load every `fixtures/<method>.json` keyed by Slack method name."""
    fixtures: dict[str, JsonObject] = {}
    for path in sorted(_FIXTURES_DIR.glob("*.json")):
        fixtures[path.stem] = json.loads(path.read_text())
    return fixtures


def _method_of(request: httpx.Request) -> str:
    return request.url.path.removeprefix("/api/").strip("/")


def make_fake_slack_transport(overrides: dict[str, JsonObject] | None = None) -> httpx.MockTransport:
    """Build an `httpx.MockTransport` answering Slack Web API calls from fixtures."""
    fixtures = load_fixtures()
    if overrides is not None:
        fixtures = {**fixtures, **overrides}

    def handler(request: httpx.Request) -> httpx.Response:
        method = _method_of(request)
        body = fixtures.get(method)
        if body is None:
            return httpx.Response(200, json={"ok": False, "error": "fake_not_implemented", "method": method})
        return httpx.Response(200, json=body)

    return httpx.MockTransport(handler)
