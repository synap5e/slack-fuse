"""HTTP request/response DTOs for the server's one-shot RPC surface.

See RFC §Server-side HTTP surface. Same process and port as the WebSocket
server; plain JSON for small request/response bodies, JSONL (gzip) for bulk
data (the snapshot endpoint).
"""

from __future__ import annotations
