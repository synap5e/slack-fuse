"""Per-connection subscription state for the WebSocket event stream."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class Subscription:
    """Mutable state for one subscribed stream on a single WS connection."""

    stream: str
    since: int
    last_sent_offset: int
    caught_up: bool = False
    caught_up_emitted: bool = False
    live_pending: bool = False
    generation: int = 0


class ConnectionSubscriptions:
    """Tracks all stream subscriptions owned by one WebSocket connection."""

    def __init__(self) -> None:
        self._subscriptions: dict[str, Subscription] = {}
        self._next_generation = 1

    def subscribe(self, stream: str, since: int) -> Subscription:
        generation = self._next_generation
        self._next_generation += 1
        subscription = Subscription(
            stream=stream,
            since=since,
            last_sent_offset=since,
            generation=generation,
        )
        self._subscriptions[stream] = subscription
        return subscription

    def remove(self, stream: str) -> None:
        self._subscriptions.pop(stream, None)

    def get(self, stream: str) -> Subscription | None:
        return self._subscriptions.get(stream)

    def is_current(self, stream: str, generation: int) -> bool:
        subscription = self._subscriptions.get(stream)
        return subscription is not None and subscription.generation == generation

    def mark_sent(self, stream: str, offset: int) -> None:
        subscription = self._subscriptions.get(stream)
        if subscription is not None and offset > subscription.last_sent_offset:
            subscription.last_sent_offset = offset

    def mark_caught_up(self, stream: str, head_offset: int) -> bool:
        subscription = self._subscriptions.get(stream)
        if subscription is None:
            return False
        subscription.last_sent_offset = max(subscription.last_sent_offset, head_offset)
        subscription.caught_up = True
        subscription.caught_up_emitted = True
        had_pending = subscription.live_pending
        subscription.live_pending = False
        return had_pending

    def mark_live_pending(self, stream: str | None = None) -> None:
        if stream is None:
            targets = self._subscriptions.values()
        else:
            subscription = self._subscriptions.get(stream)
            targets = () if subscription is None else (subscription,)
        for subscription in targets:
            if not subscription.caught_up:
                subscription.live_pending = True

    def caught_up_streams(self, stream: str | None = None) -> list[Subscription]:
        if stream is not None:
            subscription = self._subscriptions.get(stream)
            if subscription is None or not subscription.caught_up:
                return []
            return [subscription]
        return [subscription for subscription in self._subscriptions.values() if subscription.caught_up]

    def count(self) -> int:
        return len(self._subscriptions)
