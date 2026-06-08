# POC B: renderer-split byte-equivalence proof

**Branch:** `synap5e/poc/b-renderer-split` (off `server-split-rebuild`)
**Date:** 2026-06-08
**Question:** Can `slack_fuse.mrkdwn.convert(text, users)` be split into a pure
structural pass (`convert_structural`, run at chunk-write time) and a separate
mention pass (`resolve_mentions`, run at read time) without changing output for
realistic inputs? If yes, the RFC's late-mention-resolution design is safe for
Sprint 2B.

**Answer:** Yes. The split is byte-equivalent for every realistic input. The
only divergences are the *intended* behavioural changes the RFC explicitly
asks for (live tables beat stale cached labels), plus two degenerate cases that
do not occur in Slack API output. **Recommendation: SAFE to proceed.**

## Setup

```bash
git -C .wt/server-split-rebuild wt synap5e/poc/b-renderer-split
cd .wt/synap5e/poc/b-renderer-split
uv sync
uv run pytest tests/test_equivalence.py -q   # 45 passed
```

New code (production renderer untouched):

- `slack_fuse_poc_b/mrkdwn_split.py` — `convert_structural`, `resolve_mentions`,
  plus a `convert_two_pass` convenience wrapper and `UserResolver` /
  `ChannelResolver` protocols.
- `tests/test_equivalence.py` — the equivalence harness + corpus + divergence
  tests.
- `pyproject.toml` — added `slack_fuse_poc_b` to the basedpyright `include`.

## What the single pass actually does

`convert` applies eight transforms in order. Note that the module docstring /
CLAUDE.md claim it "handles code, blockquotes, lists" — the actual code does
**not**; it only does these eight, and there is **no code-span protection**:

1. User mentions `<@U…>` / `<@U…|label>` → `@label` if label else table lookup else `@<id>`
2. Channel refs `<#C…>` / `<#C…|label>` → `#label` if label else `#<id>` (**no table**)
3. Links `<url>` / `<url|label>` → `url` / `[label](url)`
4. Special mentions `<!here|channel|everyone>` → `@here|channel|everyone`
5. Subteams `<!subteam^S…|@name>` / `<!subteam^S…>` → `@name` / `@<id>`
6. Bold `*x*` → `**x**`
7. Italic `_x_` → `*x*`
8. Strike `~x~` → `~~x~~`

## The split

- **`convert_structural`** keeps transforms 3–8 verbatim. For transforms 1–2 it
  *normalises instead of resolving*: it strips any cached inline label and
  leaves a bare `<@U…>` / `<#C…>` placeholder. Mention resolution happens
  **after** all formatting transforms have run (it's a separate pass), whereas
  the single pass resolved **before** them.
- **`resolve_mentions`** substitutes the bare placeholders against the live
  `users` and `channels` tables, last.

The placeholder is formatting-transparent: `<@U123>` / `<#C1>` contain no
`*` `_` `~` `\n`, and aren't `https?://` or `<!…>`, so transforms 3–8 leave them
untouched. As long as the *resolved name* is likewise free of markdown
metacharacters, the two passes are identical.

## Corpus & results

- **Equivalence corpus:** 37 inputs (`EQUIVALENT_INPUTS`), each asserted
  byte-identical via `assert_equivalent`. Plus a no-resolvers test (3 more
  inputs) and a corpus-size guard.
- **Divergence tests:** 5, each pinning the *new* behaviour and the single-pass
  behaviour it departs from.
- **Total: 45 tests, all passing.** `ruff check`, `ruff format --check`, and
  `basedpyright` (strict) all clean on the new files.

Corpus coverage: empties/plain, bold/italic/strike (incl. mid-word and
single-asterisk non-matches), `snake_case` italic, special mentions, subteams
(labelled + bare), bare/labelled/unicode/unknown user mentions, multiple
mentions per line, trailing punctuation, labelled + bare channel mentions,
mentions adjacent to formatting (`*<@U1>*`, `_<@U1>_`, `~<@U2>~`), mentions
inside code spans and blockquotes, multi-line input, and a kitchen-sink line
combining all entity types.

## Divergences

No structural-pass **bugs** were found — nothing needed fixing in the
conversion logic. All divergences are inherent to deferring resolution, and
four of the five are the explicit goal of the RFC or are unreachable from real
data.

| # | Input | Single pass | Two pass | Verdict |
|---|---|---|---|---|
| 1 | `<@U1\|alice-OLD>`, table `U1=alice-new` | `@alice-OLD` | `@alice-new` | **Intended** — RFC's core goal: live table beats stale label |
| 2 | `<@U1\|alice>`, user absent from table | `@alice` | `@U1` | **Tradeoff** — see below |
| 3 | `<#C1>` (bare), table `C1=general` | `#C1` | `#general` | **Improvement** — single pass never had a channels table; unreachable from API |
| 4 | `<@U6>`, name `a_b_c` | `@a*b*c` | `@a_b_c` | **Improvement** — names shouldn't be markdown-mangled; see below |
| 5 | `<https://x\|see <@U1>>` | `[see @Alice](https://x)` | `[see <@U1](https://x)>` | **Degenerate** — nested entities; Slack never emits them |

### #1 Stale label loses to live table — *intended*
This is the entire reason for late resolution. The single pass bakes in
whatever name was in the message text when it was sent; the two-pass design
resolves against the current table. For a renamed user the two-pass output is
correct and the single-pass output is stale.

### #2 Labelled mention on a table miss — *tradeoff, low risk*
Stripping the cached label means that if the user is **not** in the live table,
the two-pass design falls back to the raw id (`@U1`) where the single pass
could still show the cached label (`@alice`). In production the users table is
bulk-populated at startup and kept fresh, so misses are rare. If we want a
belt-and-suspenders fallback, the chunk could retain the label as a sidecar and
the resolver prefer table → label → id. **Not required for Sprint 2B**, but
worth a one-line note in the RFC so it's a conscious choice.

### #3 Bare channel id resolves — *improvement, unreachable from API*
The single-pass `convert` never took a channels table, so a label-less `<#C…>`
always rendered as the raw id. The two-pass design resolves it. This only
differs when a bare channel id has a live table entry — and Slack's API always
emits channel mentions **with** the label (`<#C123|general>`), so this case
doesn't arise in real data. Strictly an improvement where it does.

### #4 Resolved name with markdown metacharacters — *improvement*
Because the single pass resolves *before* the formatting transforms, a display
name containing `*` `_` `~` gets mangled (`a_b_c` → `a*b*c`). The two-pass
design resolves *after*, inserting the name verbatim. The two-pass behaviour is
more correct. Real display names rarely contain these characters, so it almost
never fires; where it does, two-pass wins.

### #5 Mention nested inside a link label — *degenerate*
`<https://x|see <@U1>>` is malformed mrkdwn: Slack never nests entity tags
inside a link label. The single pass happens to resolve the inner mention
before the link regex runs; the two-pass link regex consumes the mention's `>`
as its label terminator and mangles both. Since Slack's API cannot produce
nested `<…>` entities, this is unreachable from real data. If defensiveness is
ever wanted, the structural link regex could be tightened, but it is not worth
doing for Sprint 2B.

## Recommendation

**The RFC's late-mention-resolution design is SAFE to proceed with for Sprint
2B.** The structural/mention split produces byte-identical output for all
realistic inputs. Every divergence is either the explicitly-desired effect of
late resolution (#1), a strict improvement (#3, #4), or unreachable from Slack
API output (#5). The single genuine tradeoff (#2 — losing the inline label
fallback on a live-table miss) is low-risk given the eagerly-populated users
table, and is easy to mitigate later if desired.

Two small follow-ups for the production renderer (not blockers):
- Note the #2 tradeoff in the RFC and decide whether to keep a label sidecar.
- The structural pass must apply formatting transforms (3–8) at chunk-write
  time and store the result; `resolve_mentions` must run on the **concatenated**
  chunk text at read time. Concatenation is safe because formatting is already
  baked in and placeholders are formatting-transparent — but the resolver must
  run once over the full assembled text, not per-chunk, in case a future change
  ever lets an entity span a chunk boundary (none can today).
