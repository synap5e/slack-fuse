"""WebSocket wire-protocol models for the event stream.

See RFC §Wire protocol. Frames are JSON-encoded, one frame per WebSocket
message, discriminated on the `type` field.
"""

from __future__ import annotations
