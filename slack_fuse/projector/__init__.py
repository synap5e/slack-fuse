"""Client projector — applies server events into the local chunks tables.

Per RFC §Wire protocol → Flow control and §Projection logic. The projector
subscribes to the server's event streams over a single WebSocket and applies
each incoming event into the client-side projections store. Per-stream queues
and parallel applier tasks (one trio task + one postgres connection per stream)
prevent head-of-line blocking: a slow apply on one stream cannot starve live
events for the rest.

Subpackages:

- `cursor` — read/advance `cursors.applied_offset` per stream
- `apply` — event-kind dispatcher: one TX per event-apply group; chunk INSERT
  + chunk_mentions INSERT + cursor advance commit atomically
- `per_stream` — per-stream queue + applier task (one trio task per stream)
- `snapshot_fetch` — HTTP `GET /streams/<id>/snapshot?at=<offset>` client
- `ws_client` — WebSocket subscriber + per-stream dispatcher
- `__main__` — entry point: wires config, applier nursery, WS client
"""

from __future__ import annotations
