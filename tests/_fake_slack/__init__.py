"""Fake Slack Web API — an httpx mock transport with deterministic responses.

Used by Sprint 1 (slurper), Sprint 2 tracks, and any test that needs to drive
Slack-shaped data without touching the network. Routes by the Slack method in
the request path (`/api/<method>`); response bodies are loaded from
`fixtures/<method>.json`. Unknown methods return `{ok: false,
error: "fake_not_implemented"}`. Pass `overrides={"conversations.history":
{...}}` to swap a single endpoint for a test.

Implementation lives in `transport.py`; re-exported here.

    transport = make_fake_slack_transport()
    with httpx.Client(base_url="https://slack.com/api", transport=transport) as http:
        resp = http.get("/conversations.list")
"""

from __future__ import annotations

from tests._fake_slack.transport import load_fixtures, make_fake_slack_transport

__all__ = ["load_fixtures", "make_fake_slack_transport"]
