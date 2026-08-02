"""Built-in interpreted fact probes registered into the slurper sweep.

Currently EMPTY. `channel_message_count_probed` was deleted 2026-08-03
after the WTF-audit found it duplicated `channel_totals`' Slack sweep
with no consumer (`_workspace/channels.md` reads
`channel_message_totals`, not the event stream). The unified probe
framework in `slack_fuse_server/slurper/probes.py` stays — future
fact probes register here by appending to the returned tuple.

Migration 0015 left the partial index
`events_probe_fact_latest_idx WHERE kind IN ('channel_message_count_probed')`
in place; the index is empty going forward and costs ~nothing. Old
events of that kind persist in the events table and are handled by
the applier's no-op branch in `slack_fuse/projector/apply.py`.
"""

from __future__ import annotations

from slack_fuse_server.probes.registry import ProbeKind


def register_fact_probes() -> tuple[ProbeKind, ...]:
    """Return interpreted fact probes for the unified slurper registry."""
    return ()


__all__ = ["register_fact_probes"]
