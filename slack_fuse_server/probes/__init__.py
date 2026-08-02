"""Interpreted probe facts run by :mod:`slack_fuse_server.slurper.probes`.

This package contains composable registry contracts and fact implementations;
it deliberately has no scheduler. Raw detection samples and interpreted facts
share the single slurper sweep, with distinct ``SlurperHealthSink`` and
``EventFactsSink`` persistence policies.
"""

from __future__ import annotations

from slack_fuse_server.probes.defaults import register_fact_probes
from slack_fuse_server.probes.registry import (
    EventFactsSink,
    ProbeDeps,
    ProbeKind,
    ProbeScope,
    ProbeTarget,
    SlackTier,
    SlurperHealthSink,
    validate_registry,
)

__all__ = [
    "EventFactsSink",
    "ProbeDeps",
    "ProbeKind",
    "ProbeScope",
    "ProbeTarget",
    "SlackTier",
    "SlurperHealthSink",
    "register_fact_probes",
    "validate_registry",
]
