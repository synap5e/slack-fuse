"""Domain + boundary models for slack-fuse.

Per ~/docs/dev/python/pydantic-io-boundaries.md:

- Slack API responses are validated at ingress via Pydantic models
  (`*Response` types). Once validated, downstream code passes typed
  values around — no `dict[str, Any]` cascading from `_get`.
- The same Pydantic models double as internal domain types, with
  `model_validator(mode='before')` handling the wire-format quirks
  (nested `topic.value`, `user`/`bot_id` fallback, etc.).
- `JsonObject` is the recursive JSON type for opaque pass-through data.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

# === Recursive JSON types ===

type JsonValue = (
    str | int | float | bool | dict[str, "JsonValue"] | list["JsonValue"] | None
)
type JsonObject = dict[str, JsonValue]


# === Common base ===


class _FrozenModel(BaseModel):
    """Base for immutable domain + wire models. Tolerates extra fields from Slack."""

    model_config = ConfigDict(frozen=True, extra="ignore")


# === Domain models (also serve as wire models for nested data) ===


class Reaction(_FrozenModel):
    name: str
    count: int = 0
    users: tuple[str, ...] = ()


class FileAttachment(_FrozenModel):
    id: str
    name: str = ""
    title: str = ""
    filetype: str = ""
    mimetype: str = ""
    size: int = 0
    url_private: str = ""
    url_private_download: str = ""
    is_huddle_canvas: bool = False
    huddle_transcript_file_id: str | None = None


class Edited(_FrozenModel):
    user: str
    ts: str


class Message(_FrozenModel):
    ts: str
    user: str
    text: str = ""
    thread_ts: str | None = None
    reply_count: int = 0
    reactions: tuple[Reaction, ...] = ()
    files: tuple[FileAttachment, ...] = ()
    edited: Edited | None = None
    subtype: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _resolve_user(cls, data: object) -> object:
        if not isinstance(data, dict):
            return data
        d: dict[str, Any] = data  # pyright: ignore[reportUnknownVariableType]
        if not d.get("user"):
            out: dict[str, Any] = {**d}
            out["user"] = d.get("bot_id") or "unknown"
            return out
        return d


class Thread(_FrozenModel):
    parent: Message
    replies: tuple[Message, ...] = ()


class Channel(_FrozenModel):
    id: str
    name: str = ""
    is_private: bool = False
    is_im: bool = False
    is_mpim: bool = False
    topic: str = ""
    purpose: str = ""
    num_members: int = 0
    is_member: bool = False
    im_user_id: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _flatten_wire_shape(cls, data: object) -> object:
        if not isinstance(data, dict):
            return data
        d: dict[str, Any] = data  # pyright: ignore[reportUnknownVariableType]
        out: dict[str, Any] = {**d}
        # Slack wire format nests topic/purpose: {value, creator, last_set}
        topic = out.get("topic")
        if isinstance(topic, dict):
            t: dict[str, Any] = topic  # pyright: ignore[reportUnknownVariableType]
            out["topic"] = t.get("value", "") or ""
        purpose = out.get("purpose")
        if isinstance(purpose, dict):
            p: dict[str, Any] = purpose  # pyright: ignore[reportUnknownVariableType]
            out["purpose"] = p.get("value", "") or ""
        # Channel name may be missing for IMs; fall back to id
        if not out.get("name"):
            out["name"] = out.get("id", "")
        # Slack wire format uses `user` for the IM partner; we expose it as `im_user_id`
        if "im_user_id" not in out and out.get("is_im") and out.get("user"):
            out["im_user_id"] = out["user"]
        return out


class HuddleInfo(_FrozenModel):
    canvas_file_id: str
    transcript_file_id: str | None = None
    date_start: int = 0
    date_end: int = 0


# === Wire-only file models ===


class FileShare(_FrozenModel):
    """A single share record inside files.info → file.shares.{public,private}.<channel_id>."""

    thread_ts: str | None = None
    ts: str | None = None


class FileShares(_FrozenModel):
    public: dict[str, list[FileShare]] = Field(default_factory=dict)
    private: dict[str, list[FileShare]] = Field(default_factory=dict)


class TextStyle(_FrozenModel):
    bold: bool = False
    italic: bool = False
    code: bool = False
    strike: bool = False


class RichTextElement(_FrozenModel):
    type: str
    user_id: str = ""
    text: str = ""
    style: TextStyle = Field(default_factory=TextStyle)


class RichTextSection(_FrozenModel):
    type: str
    elements: tuple[RichTextElement, ...] = ()


class TranscriptBlocks(_FrozenModel):
    """The `blocks` field of huddle_transcription. It's a single block (not a list)
    that contains the rich-text sections."""

    elements: tuple[RichTextSection, ...] = ()


class HuddleTranscription(_FrozenModel):
    blocks: TranscriptBlocks = Field(default_factory=TranscriptBlocks)


class SlackFile(_FrozenModel):
    """files.info → file. Only fields actually consumed are modelled."""

    id: str
    name: str = ""
    title: str = ""
    url_private: str = ""
    is_huddle_canvas: bool = False
    huddle_transcript_file_id: str | None = None
    huddle_date_start: int = 0
    huddle_date_end: int = 0
    huddle_transcription: HuddleTranscription | None = None
    shares: FileShares = Field(default_factory=FileShares)


class SearchFile(_FrozenModel):
    """A match in search.files."""

    id: str
    title: str = ""
    timestamp: int = 0
    channels: tuple[str, ...] = ()


class SearchFilesData(_FrozenModel):
    matches: tuple[SearchFile, ...] = ()
    total: int = 0


# === Wire-only user models ===


class SlackUserProfile(_FrozenModel):
    display_name: str = ""
    real_name: str = ""


class SlackUser(_FrozenModel):
    id: str
    name: str = ""
    profile: SlackUserProfile = Field(default_factory=SlackUserProfile)

    def display(self) -> str:
        return self.profile.display_name or self.profile.real_name or self.name or self.id


class BotInfo(_FrozenModel):
    id: str = ""
    name: str = ""


# === Slack Web API response wrappers ===


class _SlackResponse(BaseModel):
    """Common shape: every Slack API response carries `ok` (and `error` on failure)."""

    model_config = ConfigDict(extra="ignore")

    ok: bool
    error: str | None = None


class ResponseMetadata(BaseModel):
    model_config = ConfigDict(extra="ignore")
    next_cursor: str = ""


class ConversationsListResponse(_SlackResponse):
    channels: list[Channel] = Field(default_factory=list)
    response_metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)


class ConversationsHistoryResponse(_SlackResponse):
    messages: list[Message] = Field(default_factory=list)
    has_more: bool = False
    response_metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)


class ConversationsRepliesResponse(_SlackResponse):
    messages: list[Message] = Field(default_factory=list)
    has_more: bool = False
    response_metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)


class FilesInfoResponse(_SlackResponse):
    file: SlackFile | None = None


class SearchFilesResponse(_SlackResponse):
    files: SearchFilesData = Field(default_factory=SearchFilesData)


class UsersListResponse(_SlackResponse):
    members: list[SlackUser] = Field(default_factory=list)
    response_metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)


class UsersInfoResponse(_SlackResponse):
    user: SlackUser | None = None


class BotsInfoResponse(_SlackResponse):
    bot: BotInfo | None = None


# === Internal index ===


class HuddleIndexEntry(BaseModel):
    """One row of the huddle index. Replaces the old dict[str, str] payload.

    Mutable so the dedup pass in store.py can rewrite `slug` in place.
    """

    model_config = ConfigDict(extra="ignore")

    month: str
    day: str
    slug: str
    channel_id: str = ""
    channel_slug: str = ""
    thread_ts: str = ""
    canvas_file_id: str
    conv_root: str = "channels"
