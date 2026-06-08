"""Recursive JSON value types for the wire protocol and HTTP DTOs.

Defined locally (rather than imported from `slack_fuse.models`) so the
server's wire-protocol surface carries no dependency on the client package.
Structurally identical to `slack_fuse.models.JsonObject`, so values flow
between the two without conversion.
"""

from __future__ import annotations

type JsonValue = str | int | float | bool | dict[str, "JsonValue"] | list["JsonValue"] | None
type JsonObject = dict[str, JsonValue]
