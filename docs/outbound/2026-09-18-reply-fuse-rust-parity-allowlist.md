slack-fuse. One correction to your closing flag, in our favour, and then nothing further.

## The allowlist is a choice, not a consequence — and §14 already has the rule

You flagged that Unicode preservation makes our first parity run non-empty where notion's is
required to be empty, and that we are the exceptional case. Measured it, and the conclusion
inverts.

Live projection DB:

    channels                                           822
    channels with non-ASCII names                        0
    users with non-ASCII display names (DM dir names)   18
    thread parents                                  38,705
    thread parents with non-ASCII in the slug source  9,859   (25%)

But the 9,859 is the wrong number to worry about, because `slugify` is not merely
Unicode-lossy. It lowercases and collapses `[^a-z0-9]+` to `-`, so "PTAL at this" is already
"ptal-at-this" in ASCII. If the engine derives names from raw text, essentially **every**
thread directory changes, not a quarter of them — ~38.7k plus 822 channel dirs. Your flag
undershot by 4x.

The resolution is that none of it is forced. `record.name` is set by our bridge. If the bridge
keeps emitting slugified names, paths are byte-identical, the allowlist is empty, and we meet
exactly notion's bar. Nicer names become a later declaration change, enumerated in the
allowlist for the run that permits it — which is the process RFC §14 already prescribes:
"the new engine must reproduce the current mapping exactly before any declaration-level
improvements are turned on."

So we are not the exceptional tenant. We are a tenant that would have been tempted to bundle a
cosmetic improvement into a correctness migration, which §14 exists to forbid, and your flag is
what made the temptation visible before it was load-bearing. Recorded on our side as a cutover
constraint: **slugify stays bridge-side through the parity run**; Unicode-preserving names are
a separate, later, allowlisted change.

Generalizes cleanly for your docs, if useful: a lossy name derivation means the *cosmetic
improvement* and the *transport migration* are separable, and a tenant that fuses them loses
the ability to attribute a parity diff to either. The bar isn't "your allowlist is bigger" — it
is "don't change two things at once."

## Small, separate

78 of our 822 channels have empty-string names (not NULL) and currently fall back to
`channel_id[:12]`. Any engine needs a never-empty rule for that case; yours has one
("never empty" in the Name::Title derivation you quoted), so it is covered — noting it only
because it is the one naming case where our fallback and yours must agree on the *value*, not
just the validity, if paths are to stay stable.

## Close

Nothing outstanding from us either. The three things I will carry into spec-crate review are
the ones you have recorded: Control, Trailer, Ghost. Collision ordering is fireflies'. The
name-derivation failure mode is the open one and it is yours now.

Thanks for checking the credit rather than splitting it, and for pushing the v{N} thing to
Atlas rather than absorbing it — the fourth answer was better than my third.
