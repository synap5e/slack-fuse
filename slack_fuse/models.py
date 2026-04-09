"""Domain models for Slack FUSE filesystem."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class User:
    id: str
    name: str
    display_name: str
    real_name: str


@dataclass(frozen=True)
class Reaction:
    name: str
    count: int
    users: tuple[str, ...] = ()


@dataclass(frozen=True)
class FileAttachment:
    id: str
    name: str
    title: str
    filetype: str
    mimetype: str
    size: int
    url_private: str
    url_private_download: str
    is_huddle_canvas: bool = False
    huddle_transcript_file_id: str | None = None


@dataclass(frozen=True)
class Edited:
    user: str
    ts: str


@dataclass(frozen=True)
class Message:
    ts: str
    user: str
    text: str
    thread_ts: str | None = None
    reply_count: int = 0
    reactions: tuple[Reaction, ...] = ()
    files: tuple[FileAttachment, ...] = ()
    edited: Edited | None = None
    subtype: str | None = None


@dataclass(frozen=True)
class Thread:
    parent: Message
    replies: tuple[Message, ...] = ()


@dataclass(frozen=True)
class Channel:
    id: str
    name: str
    is_private: bool = False
    is_im: bool = False
    is_mpim: bool = False
    topic: str = ""
    purpose: str = ""
    num_members: int = 0
    is_member: bool = False
    im_user_id: str | None = None


@dataclass(frozen=True)
class DayMessages:
    channel_id: str
    date: str
    messages: tuple[Message, ...] = ()


@dataclass(frozen=True)
class HuddleInfo:
    canvas_file_id: str
    transcript_file_id: str | None
    date_start: int
    date_end: int
    canvas_content: str | None = None
    transcript_content: str | None = None


def message_to_dict(msg: Message) -> dict[str, Any]:
    return asdict(msg)


def message_from_dict(d: dict[str, Any]) -> Message:
    return Message(
        ts=d["ts"],
        user=d["user"],
        text=d["text"],
        thread_ts=d.get("thread_ts"),
        reply_count=d.get("reply_count", 0),
        reactions=tuple(Reaction(**r) if isinstance(r, dict) else r for r in d.get("reactions", ())),
        files=tuple(FileAttachment(**f) if isinstance(f, dict) else f for f in d.get("files", ())),
        edited=Edited(**d["edited"]) if d.get("edited") and isinstance(d["edited"], dict) else d.get("edited"),
        subtype=d.get("subtype"),
    )


def channel_to_dict(ch: Channel) -> dict[str, Any]:
    return asdict(ch)


def channel_from_dict(d: dict[str, Any]) -> Channel:
    return Channel(**d)
