"""slack_fuse_render — the renderer as a pure, stateless library.

Per RFC §Renderer-as-library. The library does no file I/O and holds no state;
callers pass typed lookup protocols (`UserResolver` / `ChannelResolver`).
Rendering is two-pass:

- **Structural pass** (`render_message_structural`) runs at chunk-write time
  and stores markdown carrying *unresolved* `<@U…>` / `<#C…>` placeholders.
- **Mention-resolution pass** (`resolve_mentions`) runs at FUSE-read time and
  substitutes those placeholders against the consumer's local tables.

The public surface is re-exported here; implementations live in `render.py`
(Sprint 0 stubs, fleshed out in Sprint 2B) and the value types in `types.py` /
`resolvers.py`.
"""

from __future__ import annotations

from slack_fuse_render.render import (
    channel_md_frontmatter,
    extract_mention_channel_ids,
    extract_mention_user_ids,
    render_message_structural,
    resolve_mentions,
    thread_md_frontmatter,
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
    "ChannelId",
    "ChannelResolver",
    "ChannelView",
    "UserId",
    "UserResolver",
    "UserView",
    "channel_md_frontmatter",
    "extract_mention_channel_ids",
    "extract_mention_user_ids",
    "render_message_structural",
    "resolve_mentions",
    "thread_md_frontmatter",
]
