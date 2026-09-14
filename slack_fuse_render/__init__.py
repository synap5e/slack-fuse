"""slack_fuse_render — the renderer as a pure, stateless library.

Per RFC §Renderer-as-library. The library does no file I/O and holds no state;
callers pass typed lookup protocols (`UserResolver` / `ChannelResolver`).
Rendering is two-pass:

- **Structural pass** (`render_message_structural`) runs at chunk-write time
  and stores markdown carrying *unresolved* `<@U…>` / `<#C…>` placeholders.
- **Mention-resolution pass** (`resolve_mentions`) runs at FUSE-read time and
  substitutes those placeholders against the consumer's local tables.
- **Thread-summary resolution** (`resolve_thread_summary_link` /
  `strip_thread_summary`) runs at the same late point but is *file-context*
  dependent: a thread parent's summary becomes a link in the day view and
  disappears in the thread view, from one stored chunk.

The public surface is re-exported here; implementations live in `render.py`
(Sprint 0 stubs, fleshed out in Sprint 2B) and the value types in `types.py` /
`resolvers.py`.
"""

from __future__ import annotations

from slack_fuse_render.mrkdwn import convert_structural
from slack_fuse_render.render import (
    LEGACY_THREAD_SUMMARY,
    THREAD_SUMMARY,
    channel_md_frontmatter,
    extract_mention_channel_ids,
    extract_mention_user_ids,
    has_thread_summary,
    render_message_structural,
    resolve_mentions,
    resolve_thread_summary_link,
    strip_thread_summary,
    thread_md_frontmatter,
    thread_summary_label,
    thread_summary_marker,
)
from slack_fuse_render.resolvers import (
    ChannelId,
    ChannelResolver,
    ChannelView,
    UserId,
    UserResolver,
    UserView,
)

__all__ = [
    "LEGACY_THREAD_SUMMARY",
    "THREAD_SUMMARY",
    "ChannelId",
    "ChannelResolver",
    "ChannelView",
    "UserId",
    "UserResolver",
    "UserView",
    "channel_md_frontmatter",
    "convert_structural",
    "extract_mention_channel_ids",
    "extract_mention_user_ids",
    "has_thread_summary",
    "render_message_structural",
    "resolve_mentions",
    "resolve_thread_summary_link",
    "strip_thread_summary",
    "thread_md_frontmatter",
    "thread_summary_label",
    "thread_summary_marker",
]
