"""Public renderer API — Sprint 0 stubs.

Per RFC §Renderer-as-library → Public API. Two-pass rendering: the structural
pass (`render_message_structural`) runs at chunk-write time and stores markdown
with unresolved `<@U…>`/`<#C…>` placeholders; `resolve_mentions` runs at
FUSE-read time to substitute them.

Sprint 0 ships the SHAPE only — every body raises `NotImplementedError`. The
real implementation (plus `mrkdwn.py`) lands in Sprint 2B. `__init__.py`
re-exports this surface so callers import `from slack_fuse_render import ...`.
"""

from __future__ import annotations

from slack_fuse.models import Message
from slack_fuse_render.resolvers import (
    ChannelId,
    ChannelResolver,
    ChannelView,
    UserId,
    UserResolver,
)


def render_message_structural(msg: Message) -> str:
    """Render a single message to markdown with unresolved `<@U…>`/`<#C…>`
    placeholders. Output is stored in `chunks.content_md`. Pure; no resolvers
    needed. Slack's `<@U…|cached-name>` form is normalised to `<@U…>`.
    """
    raise NotImplementedError


def resolve_mentions(
    md: str,
    users: UserResolver,
    channels: ChannelResolver,
) -> str:
    """Substitute `<@U…>` and `<#C…>` placeholders against the supplied
    resolvers. Called by the FUSE read layer during chunk concat. Unknown IDs
    fall back to the raw UID/CID literal (graceful degradation during the
    startup / cross-stream-race window).
    """
    raise NotImplementedError


def channel_md_frontmatter(channel: ChannelView, date: str) -> str:
    """YAML frontmatter block prepended to a `channel.md` day file."""
    raise NotImplementedError


def thread_md_frontmatter(channel: ChannelView, parent: Message) -> str:
    """YAML frontmatter block prepended to a `thread.md` file."""
    raise NotImplementedError


def extract_mention_user_ids(structural_md: str) -> set[UserId]:
    """Return the set of `UserId`s referenced by `<@U…>` placeholders in a
    structural chunk. Used to populate `chunk_mentions` (mention_kind='user')
    when the projector writes a chunk.
    """
    raise NotImplementedError


def extract_mention_channel_ids(structural_md: str) -> set[ChannelId]:
    """Return the set of `ChannelId`s referenced by `<#C…>` placeholders in a
    structural chunk. Used to populate `chunk_mentions`
    (mention_kind='channel') when the projector writes a chunk.
    """
    raise NotImplementedError
