# slack_fuse_render — the shared pure renderer

Slack message → markdown bytes. Imported by **both** sides: client (`projector/apply.py`, `projector/disk_projection.py`, `fuse_v2_helpers.py`) and server (`originals.py`, `backfill/{api,legacy,types}.py`, `slurper/{__main__,catchup}.py`). Five modules, small, and disproportionately load-bearing.

| Module | Exports |
|---|---|
| `render.py` | `render_message_structural`, `resolve_mentions`, `channel_md_frontmatter`, `thread_md_frontmatter`, `extract_mention_user_ids`, `extract_mention_channel_ids` |
| `mrkdwn.py` | `convert_structural` — Slack mrkdwn → markdown, mention regexes reused by `render.py` |
| `resolvers.py` | `UserResolver` / `ChannelResolver` protocols |
| `types.py` | frozen `UserId`, `ChannelId`, `UserView`, `ChannelView` |

## The two-pass contract

Structural pass stores **unresolved** placeholders (`<@U…>`, `<#C…>`) in the chunk. Display names resolve late, at presentation, against the reader's local tables. This exists so a user rename does not require rewriting every stored message.

Consequences you must not break:

- **Renderers are pure.** Models + a resolver in, bytes out. No I/O, no DB, no clock reads.
- **The structural pass must not resolve mentions.** Doing so bakes a name into storage and silently ages.
- **Frontmatter is identity.** `channel_id` / `thread_ts` in the header is what the disk-projection reader verifies before serving a file and what `permalink` reverses. Changing its shape is a `RENDERER_VERSION` event.

## RENDERER_VERSION

Defined at `slack_fuse/projector/projection_ledger.py:44`, currently `"v1"`, introduced by `1f0caa0`, never bumped.

**Bump it whenever projected bytes change structurally.** The ledger reader rejects disk output whose `renderer_version` does not match, and startup reconciliation re-dirties every stale row — so a bump is a full silent re-render, and *not* bumping after a byte-affecting change leaves the mount serving output from the old renderer indefinitely.

## Notes

`render.py` imports `slack_fuse.models.Message`, so this is not an independently installable package despite living at the top level — it ships in the same distribution. That coupling is the remaining obstacle to extracting it (see the primitives-library item in BACKLOG.md).

Byte-equivalence of the split was proven once in `docs/plans/poc-reports/poc-b.md`; the POC package and its worktrees were deleted afterwards.
