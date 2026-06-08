"""Pure projection + serialisation (no database).

Pins the fold semantics per stream family, the deterministic serialisation, and
the channel line's conformance to the HTTP `SnapshotLine` DTO Sprint 3A streams.
"""

from __future__ import annotations

import json

import pytest

from slack_fuse.models import Channel, Message, SlackUser
from slack_fuse_server._json import JsonObject
from slack_fuse_server.http.dto import SnapshotLine
from slack_fuse_server.snapshot.generator import (
    EventRow,
    canonical_json,
    project_stream,
    to_jsonl,
)


def _msg(ts: str, text: str, *, thread_ts: str | None = None) -> JsonObject:
    """A `message`-event payload in the exact shape the slurper writes."""
    return Message.model_validate({"ts": ts, "user": "U1", "text": text, "thread_ts": thread_ts}).model_dump(
        mode="json"
    )


# === channel:<id> ===


def test_channel_fold_keeps_undeleted_messages_sorted_by_ts() -> None:
    events: list[EventRow] = [
        ("message", _msg("100.000002", "second")),
        ("message", _msg("100.000001", "first")),
        ("message", _msg("100.000003", "third, will delete")),
        ("message_deleted", {"deleted_ts": "100.000003", "previous_message": None}),
    ]
    lines = project_stream("channel:C1", events)

    assert [line["ts"] for line in lines] == ["100.000001", "100.000002"]
    payloads = [line["payload"] for line in lines]
    assert all(isinstance(p, dict) for p in payloads)
    assert payloads[0]["text"] == "first"  # type: ignore[index]
    assert payloads[1]["text"] == "second"  # type: ignore[index]


def test_channel_fold_includes_thread_replies_flat() -> None:
    parent = _msg("200.000000", "parent")
    reply = _msg("200.000005", "reply", thread_ts="200.000000")
    lines = project_stream("channel:C1", [("message", parent), ("message", reply)])

    assert [line["ts"] for line in lines] == ["200.000000", "200.000005"]


def test_channel_message_changed_replaces_in_place() -> None:
    original = _msg("300.000000", "before edit")
    edited = _msg("300.000000", "after edit")
    events: list[EventRow] = [
        ("message", original),
        ("message_changed", {"message": edited, "previous_ts": "300.000000"}),
    ]
    lines = project_stream("channel:C1", events)

    assert len(lines) == 1
    assert lines[0]["payload"]["text"] == "after edit"  # type: ignore[index]


def test_channel_line_matches_snapshot_line_dto() -> None:
    lines = project_stream("channel:C1", [("message", _msg("400.000000", "hello"))])
    # Each channel line must round-trip through the Sprint-3A wire DTO.
    parsed = SnapshotLine.model_validate(lines[0])
    assert parsed.ts == "400.000000"
    assert parsed.payload["text"] == "hello"


# === users ===


def _user(uid: str, *, display: str = "", real: str = "", name: str = "") -> JsonObject:
    return SlackUser.model_validate({
        "id": uid,
        "name": name,
        "profile": {"display_name": display, "real_name": real},
    }).model_dump(mode="json")


def test_users_fold_applies_rename_and_profile_change() -> None:
    events: list[EventRow] = [
        ("user_added", _user("U2", display="Alice")),
        ("user_added", _user("U1", display="Bob")),
        ("user_renamed", {"user_id": "U1", "new_display_name": "Bobby"}),
        ("user_profile_changed", {"user_id": "U2", "profile_fields": {"display_name": "Alicia", "real_name": "A."}}),
    ]
    lines = project_stream("users", events)

    # Sorted by user id.
    assert [line["id"] for line in lines] == ["U1", "U2"]
    by_id = {line["id"]: line for line in lines}
    assert by_id["U1"]["profile"]["display_name"] == "Bobby"  # type: ignore[index]
    assert by_id["U2"]["profile"]["display_name"] == "Alicia"  # type: ignore[index]
    # Each users line is itself a valid SlackUser object.
    assert SlackUser.model_validate(by_id["U1"]).display() == "Bobby"


def test_users_fold_ignores_change_for_unknown_user() -> None:
    lines = project_stream("users", [("user_renamed", {"user_id": "Ughost", "new_display_name": "X"})])
    assert lines == []


# === channel-list ===


def _channel(cid: str, *, name: str = "", is_member: bool = True) -> JsonObject:
    return Channel.model_validate({"id": cid, "name": name, "is_member": is_member}).model_dump(mode="json")


def test_channel_list_fold_applies_structural_events() -> None:
    events: list[EventRow] = [
        ("channel_added", _channel("C2", name="general")),
        ("channel_added", _channel("C1", name="random", is_member=True)),
        ("channel_renamed", {"channel_id": "C1", "new_name": "random-renamed"}),
        ("channel_archived", {"channel_id": "C2"}),
        ("channel_member_changed", {"channel_id": "C1", "is_member": False}),
    ]
    lines = project_stream("channel-list", events)

    assert [line["id"] for line in lines] == ["C1", "C2"]
    by_id = {line["id"]: line for line in lines}
    assert by_id["C1"]["name"] == "random-renamed"
    assert by_id["C1"]["is_member"] is False
    assert by_id["C2"]["is_archived"] is True


def test_channel_list_unarchive_clears_flag() -> None:
    events: list[EventRow] = [
        ("channel_added", _channel("C1")),
        ("channel_archived", {"channel_id": "C1"}),
        ("channel_unarchived", {"channel_id": "C1"}),
    ]
    lines = project_stream("channel-list", events)
    assert lines[0]["is_archived"] is False


# === serialisation / determinism ===


def test_to_jsonl_is_one_line_per_item() -> None:
    events: list[EventRow] = [("message", _msg(f"50{i}.000000", f"m{i}")) for i in range(3)]
    lines = project_stream("channel:C1", events)
    jsonl = to_jsonl(lines)

    rows = jsonl.split("\n")
    assert len(rows) == len(lines) == 3
    for row, line in zip(rows, lines, strict=True):
        assert json.loads(row) == line


def test_projection_is_deterministic_regardless_of_arrival_order() -> None:
    forward: list[EventRow] = [("message", _msg(f"6{i:02d}.000000", f"m{i}")) for i in range(5)]
    reversed_order = list(reversed(forward))

    a = canonical_json(project_stream("channel:C1", forward))
    b = canonical_json(project_stream("channel:C1", reversed_order))
    assert a == b


def test_non_projectable_stream_raises() -> None:
    with pytest.raises(ValueError, match="non-projectable"):
        project_stream("slurper-health", [])
