"""In-memory resolver stubs for the renderer tests.

Concrete implementations of the Sprint 0 `UserResolver` / `ChannelResolver`
protocols, backed by plain dicts. Production backs these with SELECTs against
the local `users` / `channels` tables; tests inject these.
"""

from __future__ import annotations

from slack_fuse_render import ChannelId, ChannelView, UserId, UserView


class StubUsers:
    def __init__(self, names: dict[str, str]) -> None:
        self._names = names

    def resolve(self, user_id: UserId) -> UserView | None:
        name = self._names.get(user_id.value)
        if name is None:
            return None
        return UserView(user_id=user_id, display_name=name)


class StubChannels:
    def __init__(self, names: dict[str, str]) -> None:
        self._names = names

    def resolve(self, channel_id: ChannelId) -> ChannelView | None:
        name = self._names.get(channel_id.value)
        if name is None:
            return None
        return ChannelView(channel_id=channel_id, name=name, is_im=False, is_mpim=False)
