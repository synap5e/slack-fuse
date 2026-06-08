"""Frozen value types for the renderer-as-library.

These are the typed inputs the renderer presents. Per RFC §Renderer-as-library
→ Frozen-dataclass types: the library operates on typed IDs and view objects
right up until presentation. Display-name resolution is a separate, late-bound
step performed via the resolver protocols in `resolvers.py`.

Plain frozen dataclasses (not Pydantic) because they never cross an I/O
boundary — they are constructed by the client's resolver implementations from
already-validated `users` / `channels` table rows.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class UserId:
    value: str


@dataclass(frozen=True, slots=True)
class ChannelId:
    value: str


@dataclass(frozen=True, slots=True)
class UserView:
    """What the renderer needs to present a user.

    Per-client: the `display_name` reflects the consumer's current
    `users`-table row, so the same `UserId` may render differently on
    different machines.
    """

    user_id: UserId
    display_name: str


@dataclass(frozen=True, slots=True)
class ChannelView:
    channel_id: ChannelId
    name: str  # rendered name (with #, no leading)
    is_im: bool
    is_mpim: bool
