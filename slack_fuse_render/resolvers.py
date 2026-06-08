"""Late-binding resolver protocols for mention substitution.

`resolve_mentions` (see `__init__.py`) takes these protocols and substitutes
`<@U…>` / `<#C…>` placeholders at FUSE-read time. Concrete implementations on
the client back them by SELECTing from the local `users` / `channels` tables;
tests inject in-memory implementations.

The frozen value types (`UserId`, `ChannelId`, `UserView`, `ChannelView`) live
in `types.py` and are re-exported here so callers can import the whole resolver
vocabulary from one module.
"""

from __future__ import annotations

from typing import Protocol

from slack_fuse_render.types import ChannelId, ChannelView, UserId, UserView

__all__ = [
    "ChannelId",
    "ChannelResolver",
    "ChannelView",
    "UserId",
    "UserResolver",
    "UserView",
]


class UserResolver(Protocol):
    def resolve(self, user_id: UserId) -> UserView | None: ...


class ChannelResolver(Protocol):
    def resolve(self, channel_id: ChannelId) -> ChannelView | None: ...
