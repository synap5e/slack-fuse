"""Tests for slack_fuse.models — boundary validation, wire-format quirks, round-trips.

Highest-leverage area: the refactor moved every Slack response through Pydantic
at the I/O boundary, so wire-format quirks (topic.value flatten, user/bot_id
fallback, IM user→im_user_id) are the biggest risk surface.
"""

from __future__ import annotations

import pytest

from slack_fuse.models import (
    BotsInfoResponse,
    Channel,
    ConversationsHistoryResponse,
    ConversationsListResponse,
    Edited,
    FileShare,
    FilesInfoResponse,
    HuddleIndexEntry,
    HuddleTranscription,
    Message,
    Reaction,
    SearchFilesResponse,
    SlackFile,
    UsersListResponse,
)

# === Channel ===


def test_channel_topic_purpose_flattened_from_wire_shape() -> None:
    ch = Channel.model_validate({
        "id": "C123",
        "name": "general",
        "topic": {"value": "Welcome", "creator": "U1", "last_set": 0},
        "purpose": {"value": "General chat", "creator": "U1", "last_set": 0},
    })
    assert ch.topic == "Welcome"
    assert ch.purpose == "General chat"


def test_channel_topic_with_empty_or_none_value_becomes_empty_string() -> None:
    ch = Channel.model_validate({
        "id": "C123",
        "topic": {"value": "", "creator": "", "last_set": 0},
        "purpose": {"value": None, "creator": "", "last_set": 0},
    })
    assert ch.topic == ""
    assert ch.purpose == ""


def test_channel_name_falls_back_to_id_when_missing_or_empty() -> None:
    assert Channel.model_validate({"id": "C99"}).name == "C99"
    assert Channel.model_validate({"id": "C99", "name": ""}).name == "C99"


def test_channel_im_user_id_extracted_from_user_field_when_im() -> None:
    ch = Channel.model_validate({"id": "D123", "is_im": True, "user": "U456"})
    assert ch.im_user_id == "U456"


def test_channel_user_field_ignored_when_not_im() -> None:
    ch = Channel.model_validate({"id": "C123", "user": "U456"})
    assert ch.im_user_id is None


def test_channel_round_trip_via_model_dump_json() -> None:
    original = Channel.model_validate({
        "id": "C123",
        "name": "general",
        "is_private": True,
        "is_member": True,
        "topic": {"value": "topic", "creator": "U1", "last_set": 0},
        "purpose": {"value": "purpose", "creator": "U1", "last_set": 0},
        "num_members": 42,
    })
    payload = original.model_dump(mode="json")
    assert Channel.model_validate(payload) == original


# === Message ===


@pytest.mark.parametrize(
    ("payload", "expected_user"),
    [
        ({"ts": "1.0", "bot_id": "B999"}, "B999"),
        ({"ts": "1.0", "user": None, "bot_id": "B7"}, "B7"),
        ({"ts": "1.0", "user": "", "bot_id": "B8"}, "B8"),
        ({"ts": "1.0", "text": "hi"}, "unknown"),
        ({"ts": "1.0", "user": "U1", "bot_id": "B1"}, "U1"),
        ({"ts": "1.0", "user": "", "bot_id": ""}, "unknown"),
    ],
)
def test_message_user_fallback_chain(payload: dict[str, object], expected_user: str) -> None:
    msg = Message.model_validate(payload)
    assert msg.user == expected_user


def test_message_optional_fields_default_correctly() -> None:
    msg = Message.model_validate({"ts": "1.0", "user": "U1"})
    assert msg.text == ""
    assert msg.thread_ts is None
    assert msg.reply_count == 0
    assert msg.reactions == ()
    assert msg.files == ()
    assert msg.edited is None
    assert msg.subtype is None


def test_message_round_trip_with_reactions_files_edited() -> None:
    original = Message.model_validate({
        "ts": "1234567890.123456",
        "user": "U1",
        "text": "hello *world*",
        "thread_ts": "1234567890.123456",
        "reply_count": 3,
        "reactions": [{"name": "wave", "count": 1, "users": ["U2"]}],
        "files": [{"id": "F1", "name": "x.txt"}],
        "edited": {"user": "U1", "ts": "1234567891.0"},
    })
    payload = original.model_dump(mode="json")
    restored = Message.model_validate(payload)
    assert restored == original
    assert restored.reactions[0] == Reaction(name="wave", count=1, users=("U2",))
    assert restored.edited == Edited(user="U1", ts="1234567891.0")


# === Response wrappers — happy + error path ===


def test_conversations_list_response_ok_and_error() -> None:
    ok = ConversationsListResponse.model_validate({
        "ok": True,
        "channels": [{"id": "C1", "name": "general"}],
        "response_metadata": {"next_cursor": "abc"},
    })
    assert ok.channels[0].id == "C1"
    assert ok.response_metadata.next_cursor == "abc"

    err = ConversationsListResponse.model_validate({"ok": False, "error": "ratelimited"})
    assert err.ok is False
    assert err.error == "ratelimited"
    assert err.channels == []


def test_conversations_history_response_default_when_minimal() -> None:
    resp = ConversationsHistoryResponse.model_validate({"ok": True})
    assert resp.messages == []
    assert resp.has_more is False
    assert resp.response_metadata.next_cursor == ""


def test_files_info_response_ok_and_error() -> None:
    ok = FilesInfoResponse.model_validate({
        "ok": True,
        "file": {
            "id": "F1",
            "is_huddle_canvas": True,
            "huddle_transcript_file_id": "F2",
        },
    })
    assert ok.file is not None
    assert ok.file.is_huddle_canvas is True
    assert ok.file.huddle_transcript_file_id == "F2"

    err = FilesInfoResponse.model_validate({"ok": False, "error": "file_not_found"})
    assert err.file is None


def test_search_files_response_ok_and_default() -> None:
    ok = SearchFilesResponse.model_validate({
        "ok": True,
        "files": {
            "matches": [
                {
                    "id": "F1",
                    "title": "Huddle Notes",
                    "timestamp": 1700000000,
                    "channels": ["C1", "C2"],
                }
            ],
            "total": 1,
        },
    })
    assert ok.files.total == 1
    assert ok.files.matches[0].channels == ("C1", "C2")

    minimal = SearchFilesResponse.model_validate({"ok": True})
    assert minimal.files.matches == ()
    assert minimal.files.total == 0


def test_users_and_bots_info_responses() -> None:
    users = UsersListResponse.model_validate({
        "ok": True,
        "members": [
            {"id": "U1", "name": "alice", "profile": {"display_name": "Alice"}},
            {"id": "U2", "name": "bob", "profile": {"real_name": "Robert"}},
        ],
    })
    assert users.members[0].display() == "Alice"
    assert users.members[1].display() == "Robert"

    bots = BotsInfoResponse.model_validate({"ok": True, "bot": {"id": "B1", "name": "MyBot"}})
    assert bots.bot is not None
    assert bots.bot.name == "MyBot"


# === SlackFile.shares ===


def test_slack_file_shares_public_and_private_iterable_as_documented() -> None:
    file = SlackFile.model_validate({
        "id": "F1",
        "shares": {
            "public": {
                "C1": [{"thread_ts": "1234.5", "ts": "1234.5"}],
                "C2": [{"ts": "1235.0"}],
            },
            "private": {"D1": [{"thread_ts": "1240.0"}]},
        },
    })
    assert file.shares.public["C1"] == [FileShare(thread_ts="1234.5", ts="1234.5")]
    assert list(file.shares.public.items())  # iterable as store._shares_to_context expects
    assert file.shares.private["D1"][0].thread_ts == "1240.0"


# === HuddleIndexEntry — slug must be mutable for store.py's dedup pass ===


def test_huddle_index_entry_round_trip_and_mutable_slug() -> None:
    e = HuddleIndexEntry(
        month="2026-04",
        day="09",
        slug="my-huddle",
        channel_id="C1",
        channel_slug="general",
        thread_ts="1700000000.000100",
        canvas_file_id="F1",
        conv_root="channels",
    )
    restored = HuddleIndexEntry.model_validate(e.model_dump(mode="json"))
    assert restored.canvas_file_id == "F1"
    # store.py mutates slug in the dedup pass — make sure it's still allowed
    e.slug = "my-huddle-2"
    assert e.slug == "my-huddle-2"


# === HuddleTranscription nested rich-text ===


def test_huddle_transcription_nested_rich_text() -> None:
    payload = {
        "blocks": {
            "elements": [
                {
                    "type": "rich_text_section",
                    "elements": [
                        {"type": "user", "user_id": "U1"},
                        {"type": "text", "text": " hello "},
                        {"type": "text", "text": "bold", "style": {"bold": True}},
                    ],
                }
            ]
        }
    }
    t = HuddleTranscription.model_validate(payload)
    section = t.blocks.elements[0]
    assert section.type == "rich_text_section"
    assert section.elements[0].user_id == "U1"
    assert section.elements[2].style.bold is True
