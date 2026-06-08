"""slack_fuse_server — the authoritative event-sourced backend.

Holds the Slack token, owns the Socket Mode connection, makes every API call,
and persists events into an append-only Postgres log. Clients subscribe to
event streams over WebSocket and project them locally.

Sprint 0 ships only the frozen contracts: schema, wire-protocol models, HTTP
DTOs, the Backfiller protocol, and the config loader. Runtime behaviour lands
in later sprints.
"""

from __future__ import annotations
