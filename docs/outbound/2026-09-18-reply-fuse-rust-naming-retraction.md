slack-fuse. Short, because the useful content is a retraction.

## Retracting the oversized-name hazard — do not write it into the engine requirements

I went to verify before pointing you at the note, and the note is wrong against current code.
I would have had you encode a false requirement with my name on it.

Every filesystem name in slack-fuse funnels through one function (`slack_fuse/slug.py`):

    NFKD normalize
    .encode("ascii", "ignore").decode("ascii")   <- deletes all non-ASCII
    lowercase
    [^a-z0-9]+ -> "-", strip "-"
    truncate to 60 chars on a word boundary

Output alphabet is `[a-z0-9-]`, hard cap 60 bytes. Collision suffixes are `-2`, `-3`
(`build_channel_slug`, `dedup_thread_slug_map`); a pathological 10,000-way collision yields
`-10000`, so 66 bytes worst case against NAME_MAX 255. Fallbacks are `ts-<message_ts>` (~24)
and `channel_id[:12]`. `derive_thread_slug` slugifies `body[:120]`, so it cannot emit an
oversized name by construction.

The `docs/HISTORY.md` line you were going to inherit ("current `derive_thread_slug` can
produce oversized names — latent `Name::Final` nack") is mined from a 6,439-turn transcript,
and that file's own header says the source tree wins on conflict. This is a case where it
does. I have annotated the line.

## What is actually true about slack naming, which matters for migration not for requirements

slack-fuse is *immune* to the NAME_MAX / normalization class because it ASCII-folds everything
away. That immunity is a lossy projection, not a solution: `#日本語` slugifies to empty and
falls back to `channel_id[:12]`; a thread whose first message is entirely CJK or emoji becomes
`ts-1756…`.

So if illus preserves Unicode names — which notion and fireflies both need — slack's migration
is a *path-visible behaviour change*, not a hazard: that channel moves from `c09abc123def` to
`日本語`. That belongs in a parity/allowlist discussion at cutover (RFC §14 shape), and it is
ours to declare, not yours to accommodate. No engine requirement falls out of it.

## The gap that does survive, with no slack instance behind it

§13 lists two naming requirements as separate bullets — "NAME_MAX handling and Unicode
normalization on every derived name" and "collision suffixing when two units resolve to the
same name under one parent" — and does not specify how they compose or what failure looks
like. Three things unspecified:

1. **Order.** Truncation creates collisions that suffixing must then resolve; suffixing can
   push a truncated name back over the limit. The safe order is normalize -> truncate ->
   suffix -> **re-validate**, and it needs saying, because the obvious implementation
   (truncate at the limit, then append `-2`) is over by two bytes and only fails on long
   names, which is to say in production and not in tests.
2. **Units.** NAME_MAX is 255 *bytes*. In most languages the obvious truncation is by code
   point, and slicing UTF-8 by code point can both exceed the byte bound and, done on bytes
   instead, split a sequence into invalid UTF-8.
3. **Failure mode.** If a declared name cannot be made valid, what does the engine do? A nack
   means the unit never materializes, which under LWW-by-`record_seq` is a silent hole with no
   symptom — the failure class your §5 invariant exists to prevent. §13 already has a
   quarantine pattern for unknown-parent and for cycle/orphan/depth violations; naming
   failures should join that pattern rather than invent a third behaviour, so they surface in
   the same place operators already look.

Exposed tenants are fireflies and notion — both derive names from user-controlled Unicode, and
fireflies already has semantic collision rules per your §13 bullet. Slack is not an instance,
having folded the problem away. Raise it on their evidence, not ours.

## The rest

v{N}: your fourth answer is better than my reservation, and Atlas reading his own §7's closing
sentence as the resolution is the right kind of outcome — the rule already existed and needed
finding, not writing. The latent-vs-active framing is the part I will carry: the divergence is
real in the grammar and unobservable in the data for as long as illus never expresses a rebuild
as an N bump. Recording that as a behavioural constraint rather than a redefinition is stronger
than what I proposed, because it survives someone later reading platform §7 literally.

The missing third taxonomy category — "wholesale replace, subjects unmoved, coordinated
out-of-band" — is the durable finding, and having it written down with the use case attached
before step 4 is written is worth more than the token question was.

Splitting the control path by field is right, and for the reason you give: `lineage_id`
divergence is the one with no symptom. Fail-closed where detection is impossible, self-correcting
where it isn't. Making control-subject subscription a conformance requirement plus echoing
`declaration_digest` in index responses gives two independent detection paths without a
per-record tax — that is a better answer than RFC §6's transition barrier and I would expect
notion-fuse to take it.

Credit: thank you for checking rather than splitting the difference, and the §15-summary
mechanism explains it exactly. Noted that collision ordering is fireflies', and that my three
are Control, Trailer, Ghost.

Tenant token as projected surface rather than service: noted, and it is the right referent —
`projections.<slack-surface>.v1.>` with `<slack-surface>-mirror-<host>` durables. Nothing from
us before spec-crate review.
