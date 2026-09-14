# slack_fuse_render — the shared pure renderer

Slack message → markdown bytes. Imported by **both** sides: client (`projector/apply.py`, `projector/disk_projection.py`, `fuse_v2_helpers.py`) and server (`originals.py`, `backfill/{api,legacy,types}.py`, `slurper/{__main__,catchup}.py`). Five modules, small, and disproportionately load-bearing.

| Module | Exports |
|---|---|
| `render.py` | `render_message_structural`, `resolve_mentions`, `resolve_thread_summary_link`, `strip_thread_summary`, `has_thread_summary`, `thread_summary_marker`, `thread_summary_label`, `channel_md_frontmatter`, `thread_md_frontmatter`, `extract_mention_user_ids`, `extract_mention_channel_ids` |
| `mrkdwn.py` | `convert_structural` — Slack mrkdwn → markdown, mention regexes reused by `render.py` |
| `resolvers.py` | `UserResolver` / `ChannelResolver` protocols |
| `types.py` | frozen `UserId`, `ChannelId`, `UserView`, `ChannelView` |

## The two-pass contract

Structural pass stores **unresolved** placeholders (`<@U…>`, `<#C…>`) in the chunk. Display names resolve late, at presentation, against the reader's local tables. This exists so a user rename does not require rewriting every stored message.

Consequences you must not break:

- **Renderers are pure.** Models + a resolver in, bytes out. No I/O, no DB, no clock reads.
- **The structural pass must not resolve mentions.** Doing so bakes a name into storage and silently ages.
- **Nothing presentational belongs in a chunk.** One chunk is read by two files, so anything whose right answer depends on *which* file is being assembled must be stored unresolved and decided late. See below.
- **Frontmatter is identity.** `channel_id` / `thread_ts` in the header is what the disk-projection reader verifies before serving a file and what `permalink` reverses. Changing its shape is a `RENDERER_VERSION` event.

## The thread summary — the second thing that resolves late

A thread parent's chunk is read by both `channel.md` (the day) and `thread.md` (the thread), so its summary line cannot be baked in: in the day it should be a link to the thread, in the thread it is a link to the file you are already reading. The structural pass therefore stores a marker,

```
<thread-summary reply_count="3"/>
```

and the assembler decides. `slack_fuse.fuse_v2_helpers.render_day_body` rewrites it to `[Thread: 3 replies](<thread-slug>/thread.md)`, `render_thread_body` strips it. Both the disk projection and the JIT mount call those two functions, which is the only thing stopping the two paths from drifting.

Three things to know:

- **The slug comes from `dedup_thread_slug_map`** — the same pass that names the thread directories — so the link and the directory agree by construction. No slug ⇒ plain `> Thread: N replies`, never a dangling link.
- **Chunks written before the marker existed** carry the literal `> Thread: N replies`. `LEGACY_THREAD_SUMMARY` matches it in both resolvers, so they render identically with no re-render; `projector.apply._patch_thread_indicator` upgrades one to the marker the next time its reply count moves.
- **Pluralisation happens at resolution**, in `thread_summary_label`. The structural pass used to hard-code `replies`, so a one-reply thread read `1 replies` unless the applier's patch path had rewritten it — two writers, one string, disagreeing.

## RENDERER_VERSION

Defined at `slack_fuse/projector/projection_ledger.py`, currently `"v2"`, introduced by `1f0caa0`. Bumped once, to `v2`, when the thread summary became a link (above).

**Bump it whenever projected bytes change structurally.** The ledger reader rejects disk output whose `renderer_version` does not match, and startup reconciliation re-dirties every stale row — so a bump is a full silent re-render, and *not* bumping after a byte-affecting change leaves the mount serving output from the old renderer indefinitely.

## Notes

`render.py` imports `slack_fuse.models.Message`, so this is not an independently installable package despite living at the top level — it ships in the same distribution. That coupling is the remaining obstacle to extracting it (see the primitives-library item in BACKLOG.md).

Byte-equivalence of the split was proven once in `docs/plans/poc-reports/poc-b.md`; the POC package and its worktrees were deleted afterwards.
