# pyright: reportPrivateUsage=false
"""Tests for the InvalidationSink contract between SlackStore and fuse_ops.

Two halves:
- Sink dispatch: apply_event fan-out calls sink.day_changed / thread_changed /
  channel_list_changed for the expected keys (with a recorder stand-in for
  InodeInvalidator).
- Path resolution: InodeInvalidator maps (channel_id, date_str) and
  (channel_id, thread_ts) to the right FUSE paths and calls a stubbed
  pyfuse3.invalidate_inode with the matching inode.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime

import pytest
import trio

from slack_fuse import disk_cache, fuse_ops as fuse_ops_mod
from slack_fuse.api import SlackClient
from slack_fuse.fuse_ops import InodeInvalidator, SlackFuseOps
from slack_fuse.inode_map import InodeMap
from slack_fuse.models import Channel, Message, SocketEventPayload
from slack_fuse.store import ChannelEntry, SlackStore
from slack_fuse.user_cache import UserCache

from .stubs import (
    stub_get_channel_list,
    stub_get_huddle_index,
    stub_get_known_dates,
    stub_load_from_disk,
    stub_put_known_dates,
)


@dataclass
class RecorderSink:
    """Stand-in for InodeInvalidator that captures the calls it would make."""

    days: list[tuple[str, str]] = field(default_factory=list)
    threads: list[tuple[str, str]] = field(default_factory=list)
    channel_lists: int = 0

    def day_changed(self, channel_id: str, date_str: str) -> None:
        self.days.append((channel_id, date_str))

    def thread_changed(self, channel_id: str, thread_ts: str) -> None:
        self.threads.append((channel_id, thread_ts))

    def channel_list_changed(self) -> None:
        self.channel_lists += 1


@pytest.fixture(autouse=True)
def disable_disk_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(disk_cache, "get_channel_list", stub_get_channel_list)
    monkeypatch.setattr(disk_cache, "get_huddle_index", stub_get_huddle_index)
    monkeypatch.setattr(disk_cache, "get_known_dates", stub_get_known_dates)
    monkeypatch.setattr(disk_cache, "put_known_dates", stub_put_known_dates)
    monkeypatch.setattr(UserCache, "_load_from_disk", stub_load_from_disk)


@pytest.fixture
def store() -> Iterator[SlackStore]:
    client = SlackClient(token="xoxp-fake")
    users = UserCache(client.http)
    yield SlackStore(client=client, users=users)


def _today_ts() -> str:
    return f"{datetime.now().timestamp():.6f}"


def _today_date() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d")


# === Sink dispatch from apply_event ===


def test_top_level_message_calls_day_changed(store: SlackStore) -> None:
    sink = RecorderSink()
    store.set_invalidator(sink)
    ts = _today_ts()
    store.apply_event(
        SocketEventPayload.model_validate({
            "type": "message",
            "channel": "C1",
            "ts": ts,
            "user": "U1",
            "text": "hi",
        })
    )
    assert sink.days == [("C1", _today_date())]
    assert sink.threads == []


def test_reply_calls_thread_and_day(store: SlackStore) -> None:
    sink = RecorderSink()
    store.set_invalidator(sink)
    parent_ts = _today_ts()
    reply_ts = f"{float(parent_ts) + 5:.6f}"
    store.apply_event(
        SocketEventPayload.model_validate({
            "type": "message",
            "channel": "C1",
            "ts": reply_ts,
            "user": "U2",
            "text": "reply",
            "thread_ts": parent_ts,
        })
    )
    # Thread hit + parent-day bump
    assert sink.threads == [("C1", parent_ts)]
    assert sink.days == [("C1", _today_date())]


def test_thread_broadcast_hits_day_twice(store: SlackStore) -> None:
    """Broadcast → day bump (reply) + day append (broadcast visible in channel)."""
    sink = RecorderSink()
    store.set_invalidator(sink)
    parent_ts = _today_ts()
    reply_ts = f"{float(parent_ts) + 5:.6f}"
    store.apply_event(
        SocketEventPayload.model_validate({
            "type": "message",
            "subtype": "thread_broadcast",
            "channel": "C1",
            "ts": reply_ts,
            "user": "U2",
            "text": "bcast",
            "thread_ts": parent_ts,
        })
    )
    assert sink.threads == [("C1", parent_ts)]
    today = _today_date()
    # Two day hits on the same key (bump + append)
    assert sink.days == [("C1", today), ("C1", today)]


def test_edit_in_thread_hits_thread_only(store: SlackStore) -> None:
    sink = RecorderSink()
    store.set_invalidator(sink)
    parent_ts = _today_ts()
    reply_ts = f"{float(parent_ts) + 10:.6f}"
    store.apply_event(
        SocketEventPayload.model_validate({
            "type": "message",
            "subtype": "message_changed",
            "channel": "C1",
            "message": {
                "ts": reply_ts,
                "user": "U2",
                "text": "edited",
                "thread_ts": parent_ts,
            },
        })
    )
    assert sink.threads == [("C1", parent_ts)]
    assert sink.days == []


def test_delete_reply_bumps_parent_day(store: SlackStore) -> None:
    sink = RecorderSink()
    store.set_invalidator(sink)
    parent_ts = _today_ts()
    reply_ts = f"{float(parent_ts) + 5:.6f}"
    store.apply_event(
        SocketEventPayload.model_validate({
            "type": "message",
            "subtype": "message_deleted",
            "channel": "C1",
            "deleted_ts": reply_ts,
            "previous_message": {
                "ts": reply_ts,
                "user": "U2",
                "text": "gone",
                "thread_ts": parent_ts,
            },
        })
    )
    assert sink.threads == [("C1", parent_ts)]
    assert sink.days == [("C1", _today_date())]


def test_channel_created_calls_channel_list(store: SlackStore) -> None:
    sink = RecorderSink()
    store.set_invalidator(sink)
    store.apply_event(
        SocketEventPayload.model_validate({
            "type": "channel_created",
            "channel": {"id": "C99", "name": "new"},
        })
    )
    assert sink.channel_lists == 1
    assert sink.days == []
    assert sink.threads == []


def test_flush_event_logs_fans_out(store: SlackStore) -> None:
    """Unclean-close flush walks every drained key."""
    sink = RecorderSink()
    store.set_invalidator(sink)
    parent_ts = _today_ts()
    reply_ts = f"{float(parent_ts) + 5:.6f}"
    store.apply_event(
        SocketEventPayload.model_validate({
            "type": "message",
            "channel": "C1",
            "ts": reply_ts,
            "user": "U2",
            "text": "r",
            "thread_ts": parent_ts,
        })
    )
    sink.days.clear()
    sink.threads.clear()
    store.flush_event_logs()
    assert sink.days == [("C1", _today_date())]
    assert sink.threads == [("C1", parent_ts)]


def test_no_sink_is_a_noop(store: SlackStore) -> None:
    """apply_event must not crash when no sink is registered."""
    ts = _today_ts()
    store.apply_event(
        SocketEventPayload.model_validate({
            "type": "message",
            "channel": "C1",
            "ts": ts,
            "user": "U1",
        })
    )


# === InodeInvalidator path resolution ===


def _install_channel(store: SlackStore, channel_id: str, slug: str, *, kind: str = "channels") -> None:
    """Plant a ChannelEntry without round-tripping the API."""
    ch = Channel(
        id=channel_id,
        name=slug,
        is_im=(kind == "dms"),
        is_mpim=(kind == "group-dms"),
        is_member=(kind == "channels"),
    )
    # Freeze the channel-list time so _refresh_channels doesn't try to fetch
    store._channels[channel_id] = ChannelEntry(channel=ch, slug=slug)
    store._channel_list_time = 1e18


@pytest.fixture
def ops(store: SlackStore) -> SlackFuseOps:
    return SlackFuseOps(store, trio.CapacityLimiter(1))


def test_day_changed_invalidates_day_files_and_dir(
    store: SlackStore,
    ops: SlackFuseOps,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_channel(store, "C1", "general")
    # Pre-register inodes for the paths we expect to be invalidated
    inodes = ops._inodes
    expected = {
        "/channels/general/2026-04/17/channel.md": inodes.get_or_create("/channels/general/2026-04/17/channel.md"),
        "/channels/general/2026-04/17/feed.md": inodes.get_or_create("/channels/general/2026-04/17/feed.md"),
        "/channels/general/2026-04/17": inodes.get_or_create("/channels/general/2026-04/17"),
        "/channels/general/2026-04": inodes.get_or_create("/channels/general/2026-04"),
    }
    calls: list[int] = []

    def _recorder(ino: int, **_k: object) -> None:
        calls.append(ino)

    monkeypatch.setattr(fuse_ops_mod.pyfuse3, "invalidate_inode", _recorder)
    inv = InodeInvalidator(inodes, store)
    inv.day_changed("C1", "2026-04-17")
    assert set(calls) == set(expected.values())


def test_day_changed_skips_unknown_channel(
    store: SlackStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inodes = InodeMap()
    calls: list[int] = []

    def _recorder(ino: int, **_k: object) -> None:
        calls.append(ino)

    monkeypatch.setattr(fuse_ops_mod.pyfuse3, "invalidate_inode", _recorder)
    inv = InodeInvalidator(inodes, store)
    inv.day_changed("NOPE", "2026-04-17")
    assert calls == []


def test_day_changed_skips_malformed_date(
    store: SlackStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_channel(store, "C1", "general")
    inodes = InodeMap()
    calls: list[int] = []

    def _recorder(ino: int, **_k: object) -> None:
        calls.append(ino)

    monkeypatch.setattr(fuse_ops_mod.pyfuse3, "invalidate_inode", _recorder)
    inv = InodeInvalidator(inodes, store)
    inv.day_changed("C1", "not-a-date")
    assert calls == []


def test_thread_changed_invalidates_thread_files(
    store: SlackStore,
    ops: SlackFuseOps,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_channel(store, "C1", "general")
    # Seed a day cache so get_thread_slugs can resolve the slug.
    ts = f"{datetime(2026, 4, 17, 12, 0, 0).astimezone().timestamp():.6f}"
    parent = Message(
        ts=ts,
        user="U1",
        text="rollout plan",
        thread_ts=ts,
        reply_count=1,
        latest_reply=f"{float(ts) + 5:.6f}",
    )
    # Inject directly into the day cache
    from slack_fuse.store import _CachedDay  # noqa: PLC0415

    store._day_cache["C1", "2026-04-17"] = _CachedDay(
        messages=[parent],
        fetched_at=1e18,
        date="2026-04-17",
    )
    # Mark date as known so the store path treats the cache as valid
    store._known_dates["C1"] = {"2026-04-17"}

    slugs = store.get_thread_slugs("C1", "2026-04-17")
    assert slugs, "fixture should produce at least one thread slug"
    thread_slug = next(iter(slugs))

    inodes = ops._inodes
    thread_dir = f"/channels/general/2026-04/17/{thread_slug}"
    expected = {
        f"{thread_dir}/thread.md": inodes.get_or_create(f"{thread_dir}/thread.md"),
        f"{thread_dir}/feed.md": inodes.get_or_create(f"{thread_dir}/feed.md"),
        thread_dir: inodes.get_or_create(thread_dir),
    }
    calls: list[int] = []

    def _recorder(ino: int, **_k: object) -> None:
        calls.append(ino)

    monkeypatch.setattr(fuse_ops_mod.pyfuse3, "invalidate_inode", _recorder)
    inv = InodeInvalidator(inodes, store)
    inv.thread_changed("C1", ts)
    assert set(calls) == set(expected.values())


def test_thread_changed_unknown_slug_falls_back_to_day_dir(
    store: SlackStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_channel(store, "C1", "general")
    inodes = InodeMap()
    ts = f"{datetime(2026, 4, 17, 12, 0, 0).astimezone().timestamp():.6f}"
    day_dir_inode = inodes.get_or_create("/channels/general/2026-04/17")
    calls: list[int] = []

    def _recorder(ino: int, **_k: object) -> None:
        calls.append(ino)

    monkeypatch.setattr(fuse_ops_mod.pyfuse3, "invalidate_inode", _recorder)
    inv = InodeInvalidator(inodes, store)
    inv.thread_changed("C1", ts)
    assert calls == [day_dir_inode]


def test_channel_list_changed_hits_all_conv_roots(
    store: SlackStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inodes = InodeMap()
    roots = {
        "/channels": inodes.get_or_create("/channels"),
        "/dms": inodes.get_or_create("/dms"),
        "/group-dms": inodes.get_or_create("/group-dms"),
        "/other-channels": inodes.get_or_create("/other-channels"),
    }
    calls: list[int] = []

    def _recorder(ino: int, **_k: object) -> None:
        calls.append(ino)

    monkeypatch.setattr(fuse_ops_mod.pyfuse3, "invalidate_inode", _recorder)
    inv = InodeInvalidator(inodes, store)
    inv.channel_list_changed()
    assert set(calls) == set(roots.values())


def test_invalidate_swallows_oserror(
    store: SlackStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the kernel doesn't support invalidate_inode, we log and move on."""
    _install_channel(store, "C1", "general")
    inodes = InodeMap()
    inodes.get_or_create("/channels/general/2026-04/17/channel.md")

    def _raise(_ino: int, **_k: object) -> None:
        raise OSError(38, "ENOSYS")

    monkeypatch.setattr(fuse_ops_mod.pyfuse3, "invalidate_inode", _raise)
    inv = InodeInvalidator(inodes, store)
    inv.day_changed("C1", "2026-04-17")  # must not raise


def test_conv_root_for_maps_channel_flags(store: SlackStore) -> None:
    _install_channel(store, "C-CH", "ch", kind="channels")
    _install_channel(store, "C-DM", "dm", kind="dms")
    _install_channel(store, "C-MP", "mp", kind="group-dms")
    # Unjoined public channel ("other-channels")
    other = Channel(id="C-OT", name="ot", is_member=False, is_im=False, is_mpim=False)
    store._channels["C-OT"] = ChannelEntry(channel=other, slug="ot")

    assert store.conv_root_for("C-CH") == "channels"
    assert store.conv_root_for("C-DM") == "dms"
    assert store.conv_root_for("C-MP") == "group-dms"
    assert store.conv_root_for("C-OT") == "other-channels"
    assert store.conv_root_for("NOPE") is None
    assert store.slug_for("C-CH") == "ch"
    assert store.slug_for("NOPE") is None
