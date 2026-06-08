"""Historical-data ingestion.

The slurper owns backfill. Live Socket Mode events only cover "from when the
server started"; everything older is fetched by a `Backfiller` and written
into the events log as `message` events indistinguishable from live ones
(modulo offset). See RFC §Backfill.
"""

from __future__ import annotations
