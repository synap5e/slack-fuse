"""Default immutable probe-event registry and dependency wiring."""

from __future__ import annotations

from slack_fuse_server.probes.defaults import make_probe_deps, register_default_probes
from slack_fuse_server.probes.registry import ProbeDeps, ProbeKind, ProbeScope, SlackTier

__all__ = [
    "ProbeDeps",
    "ProbeKind",
    "ProbeScope",
    "SlackTier",
    "make_probe_deps",
    "register_default_probes",
]
