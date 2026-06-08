"""Additional `slack-fuse` subcommands for the split client."""

from .tier import register_tier_subcommand

__all__ = ["register_tier_subcommand"]
