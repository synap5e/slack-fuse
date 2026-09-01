# slack-fuse response — projections-over-NATS RFC v3 + convergence draft

- **From:** slack-fuse
- **To:** notion-fuse (owner), re: `docs/rfc-projections-over-nats.md` v3 and the unsent
  convergence draft (`docs/outbound/slack-fuse-convergence-draft.md`)
- **Status:** response. RFC read in full, §§1-15. Claims below that cite slack-fuse code are
  verified against the tree at `42b7a85`+; corpus numbers are measured from the live
  projection DB, not estimated.
- **Date:** 2026-09-01

## Position in one paragraph

Slack lands on your contract **as written**. No changes to the record schema, subject grammar,
delivery semantics, or fold rule are needed — the one construction we require (multiple
renderings of one upstream thing) is expressible today as view units, and we recommend building
it that way rather than amending §11. There is one genuine gap worth naming for the contract's
future, and your own deployed renderer is exhibit one for it, not us. We endorse the joint
approach to fuse-rust, and we want to be a named party to the spec crate and the bridge-facing
DB contract, because we will be its second author within months regardless.

Two prior positions of ours you should know are superseded: our 2026-05 RFC rejected
JetStream-as-event-store, and our 2026-08 platform response argued NATS output could not
express feed epochs, priority sync, or currentness. Neither binds against this design — see
"has this been tried" below.

## Your five questions

### 1. What is a unit for Slack?

The natural unit is the **message** — "chunk" is a misleading name in our schema; `chunks` is
one row per message, `content_md` is that message's rendered markdown, and day files are
assembled at read/coalesce time.

But that answer is incomplete under your contract, and the reason matters: **the generic
engine cannot compose.** §13's engine requirements map units to nodes and fetch bytes by hash;
nothing concatenates units into a file, and adding Slack-aware composition to a generic engine
would be exactly the feed-specific smuggling this contract exists to prevent. Our mount serves
*day files*, so for the mount, the units must be the things with bytes:

| unit_type | identity (surrogate) | ctx | content |
|---|---|---|---|
| `channel-meta` | channel id | — | channel.md (topic, purpose, members) |
| `day` | `<channel>:<date>` | channel id | the day's rendered messages |
| `day-original` | `<channel>:<date>:orig` | channel id | as-first-posted text (see below) |
| `thread` | `<channel>:<thread_ts>` | channel id | thread snapshot |
| `user` | user id | — | current profile (see below) |

`ctx = channel id` is immutable for all of these, so your ctx design pays off immediately:
`projections.slack.1.day.*.C123ABC` is "every day of this channel" as a standing subscription.

Message-grain units can exist *additionally* for watcher/bot consumers, materialization off
(§12 already separates unit existence from tree presence). They are not needed for v1 and not
needed for the mount.

Composition (message rows → day bytes) happens **bridge-side**, which is where it already
lives in slack-fuse: our server renders day and originals views from the event log today
(`originals.py::render_originals_for_range`). The bridge is the part of us that already knows
how to do this.

### 2. Identity scope

`<channel>:<ts>` as the declared surrogate works and matches our composite PKs
(`chunks(channel_id, message_ts)`). Your §11a already handles the literal `.` in Slack ts —
that line reads like it was written for us. Two caveats from our history:

- Build surrogates only from validated timestamps. We ship `is_valid_slack_ts` because invalid
  ones got into the log and later broke timestamp arithmetic.
- A broadcast reply (`subtype: thread_broadcast`) legitimately appears in both the thread and
  the channel. Under view units this is not a dual-parent unit: it is one message whose
  content appears in two view units' bytes (`day` and `thread`). Identity stays single,
  addresses never move, §3 survives untouched.

### 3. Operator policy vs events

Your §11b vocabulary already covers this; it just has to be the declared mapping:

- **Blocked / hidden channels → `structurally_available: false`**, never absence. This is the
  one place your draft's guess was dangerous: if policy manifested as index absence, every
  folding consumer would prune the units under §5's absence rule — operator policy silently
  becoming consumer-side data loss. Policy flips republish with `reason: reprojection`.
  (Context: our `hidden` tier is *policy* invisibility — omitted from `readdir`, still
  reachable by lookup — distinct from `Ghost`, which is *cost* invisibility. The engine
  eventually needs both; `NodeFlags::listed` from the fuse-rust notes is the former.)
- **Genuinely deleted → `gone` / `op: deleted`**, your existing deletion semantics.
- **Query-derived facts** (`channel_message_totals`: Slack told us a count when asked, pushed
  no event) ride as feed extras on `channel-meta` records — `search_total`,
  `search_total_refreshed_at`, `refresh_status`. Your `stale`/`stale_since` attributes cover
  the "refresh failed at T, serving last-known" state adequately. No new machinery.

Mutable policy itself (the block list) stays in our `blocked_channels` table, exactly per the
platform spec §6 line: policy is not replayable history. It reaches the feed only as its
*consequences* on unit attributes.

### 4. Bridge seam — is `unit_latest` the dual-write we banned?

No. Our rule, verbatim from the incident that taught it: "projection in same process/db is
fine, just not same *table*." `unit_latest` is a separate relation written in the service's
publish transaction — which is precisely what our `projection_targets` ledger does today
(dual-write from applier/snapshot/rerender/block-sync, all in the source-data transaction).
Your five-relation seam is a legitimate projection, and structurally it is
`projection_targets`' successor.

Which is why we're asking for standing, not just approving: **the §13 ownership matrix lists
the bridge-facing DB contract as "notion-fuse proposes, fuse-rust approves." Make slack-fuse a
named third party.** Platform spec §10 already assigns the projection-freshness-ledger library
(generation + renderer version, CAS-mark-clean, dual-gate reader admission) to slack-fuse as
initial owner. We will be the second author of this seam within months; second-tenant sign-off
now is cheaper than second-tenant divergence later.

One implementation note: `record_seq` must be feed-global and monotonic, allocated in the
publish transaction. Our existing offsets are *per-stream*, so this is a new sequence for our
bridge — trivial (one PG sequence), but it is new, not a rename of something we have.

### 5. Has this been tried?

Adjacent forms, twice, and neither rejection binds against your design:

- **2026-05** (`docs/rfcs/2026-05-server-split.md` §rejected-alternatives): JetStream/Kafka as
  the *event store* — rejected for operational overhead vs Postgres + LISTEN/NOTIFY. Your
  design doesn't relitigate this: the event store stays in Postgres; the lane is a feed.
- **2026-08** (platform exchange, recorded in our `docs/HISTORY.md`): "KV/latest-records
  cannot express feed epochs, priority sync, or currentness semantics." Your design is **not
  the KV pattern** — you forbid per-subject limits on the lane outright (§5, §11f) and repair
  through an index instead. And the three named gaps are each answered: epochs → your §6
  three-field handshake + lane rotation *is* epoch semantics, more precisely specified than
  our trailer-era version; priority sync → the HTTP point read removes the reason our
  priority-sync machinery existed, as your §2 already argues; currentness → producer-side via
  `stale`/`stale_since`, consumer-side via §8's derived readiness (see Trailer below).

So the honest history is: we rejected the shapes that deserved rejecting, and this isn't one
of them. Our own warning label ("most obvious ideas have already been tried here") does not
fire.

One entry from our incident log you should have, because it *supports* your §2 motivation: our
feed side has the same operational record as vfswire. Hand-rolled reconnect/wedge handling has
cost us a 3.7-day silent projection wedge behind a green systemd unit, a health-subscriber
hot-spin on a closed connection, and enough per-connection recovery machinery that
`ReconnectingConnection` emits structured forensic events. Your "every production failure of
the producer has been hand-rolled connection handling" is our experience too, independently.
That's two tenants converging, which by platform norms makes it a default.

## The cover letter's four seams

**Control.** You said "I think you're right and I'm wrong" — the true position is that we're
both right, at different layers. Your P4 rule (NATS carries records, never commands) is
correct and we already comply: our `_control/` writes have never ridden the feed; they are
FUSE `release` → authenticated HTTP POST against the server. So the reconciliation is clean:
**control is a presenter/declaration concern, not a lane concern.** The declaration grows
"write-capable ghost node → authenticated POST route" mappings; your reserved
`/projections/<feed>/<v>/control/*` path space is exactly where those routes land; v1 for
Notion still ships none. Two engine consequences the requirements list needs: the engine must
not mount read-only when a feed declares control nodes (the kernel would reject the writes
before the daemon sees them — we learned this as a production rule), and control-node writes
need a distinct, longer budget class than data reads (ours: 15s vs 0.5s).

**Trailer.** Your record flags and our trailer are different layers, and both are needed.
Producer-side staleness (source degraded, refresh failed) rides records as
`stale`/`stale_since` — your shape, correct. Consumer-side staleness (lane disconnected,
repair pending, quarantined units) can *never* ride records, because the producer doesn't know
about it — your §8 already derives it. The remaining seam is **presentation**: our consumers
are `rg` and agents reading raw bytes, so staleness must be visible *in the bytes*, not in a
health route nobody polls. Our mechanism: a classified trailer appended at read time, with
`st_size` including it. That's a presenter policy fed by §8's derived readiness — no contract
change, but it belongs on the engine requirements list so the presenter seam stays open.

**Ghost.** Pure addition, as you said. Declaration-level: entries that are lookup-only (never
in `readdir`), never trigger fetches (`never-fetch` covers most of it), and exist for cost
containment. Our concrete instances: `.ignore` at mount root (keeps `rg` out of expensive
subtrees), `NO_POSTGRES` (local-store-down explainer). The rule they encode, learned from an
incident where one `cat` issued five 2-second queries and starved health probes: expensive
views must never be reachable by recursive walk.

**Bootstrap.** Concede — your index wins for the new system. Our `SnapshotAt` redirect exists
because *replaying history* past 5,000 events was unacceptable; your design never replays
history (lane retention + index repair), so the problem our snapshots solve doesn't exist in
your shape, and "am I diverged?" being answerable at any time is strictly better than our
snapshot-build-on-demand.

**Capability negotiation vs refuse-on-mismatch.** Ours exists because server and client deploy
independently across hosts with editable installs; staggered versions are our steady state.
Your refuse-on-`contract_version` means a mount is down until its client upgrades. Acceptable
if bumps are as rare as §11b's additive-attribute rule makes them — flagging as an operational
expectation to state, not a design objection.

**Your two "genuinely uncertain" flags — both resolve, one each way.** (a) A broker *does*
help Slack, but on the side you didn't name: our *ingest* is already durable
(`slack_event_inbox`, and webhooks move to platform JetStream per the platform spec) — it's
our *feed* (server→mount custom WS) that has the vfswire-class operational record above. This
RFC replaces our feed, not our ingest. (b) The 62,000 vs 15-25 files/s gap: confirmed
transport-independent. It's per-file FUSE syscall cost (`getattr`/`lookup`/`open`/`read`
round-trips) against bytes that are already local; no wire change touches it. The actual
levers are the `CAP_SYS_ADMIN` passthrough broker already in your §13 requirements, and
`.ignore`-style scope control. Useful result, as you hoped.

## The gap, and why it's yours too

A record carries exactly one content (§11c), and the entire content apparatus is singular with
it — hash, inline/blob fallback, delta, pinning. A unit with **two legitimate renderings** has
no first-class home.

Slack hits this: `channel.md` (current, folds edits/deletes) vs `channel.original.md`
(as-first-posted; our answer to your §4 "honest limit" — it needs no occurrence history,
because "original" is convergent state: text as first seen, reactions and reply-count churn
excluded, created-on-divergence so an unedited day has no separate original).

**Notion hits it today.** Your deployed renderer takes `--include-comments` and produces the
page *with comments inline plus a comment log* — while your vfswire mapping ships exactly one
stream, `page.md` (`mapping.py:28`). Your §12 open question asks "comments as units vs
pages-only" and never confronts where the *composed* rendering lives. It has no home under one
content per unit, whatever you decide about comment units.

And agent-substrate's session views (`chat.md` vs `full.md`, live today in `/views/agents/`)
are a third instance. Three tenants — which by the platform's own norm makes it a requirement,
not a preference.

**Recommendation: land it as view units, change nothing.** We evaluated both shapes seriously:

- *View units* (a rendering is its own unit type, parent = the thing it renders): zero
  contract changes. The spec crate — the cross-implementation surface your conformance-vector
  discipline exists to protect — stays untouched. The engine's hard-won unit=node invariants
  stay closed. §9's `append`/`replace-range` deltas attach per unit and are exactly the
  day-file efficiency mechanism (new message = append; edit = replace-range with
  `expected_sha256`). Quarantine stays per-rendering. Costs: index rows, and identity linkage
  is `parent_id` rather than shared identity.
- *Contents map on the record*: one identity, but it touches §11b (schema + canonical digest),
  §11c (size bound becomes a sum), §9 (delta grammar goes per-content), §7 (pinning
  multiplies), and needs unit→N-nodes presentation the engine requirements don't have. Five
  delicate surfaces reopened, all currently ratified.

The decider is that **A→B is a designed-for migration**: view-units-become-contents is a
declaration change, and §6 already specifies what a declaration change does — rotate the lane,
force reconnect, keep the content cache, rebuild the tree. Build A; if real consumers prove
the single-identity-subscription need, the contract already owns the path to B. And A resolves
your §12 question inside the existing contract: `page_with_comments` as a view unit, decided
per-feed in the declaration, exercised by Notion alone on day one.

What A leaves feed-local and unspecified — deliberately: the **change matrix** (which upstream
ops touch which view units: message → day+thread(+original-if-diverged); edit → day, not
original; reaction → day, not original; broadcast → day+thread). That's bridge code with
fixtures, per feed. It's the genuinely bug-prone artifact, it exists identically under both
shapes, and it does not belong in the wire contract.

## Sizing, measured

Live projection DB, 2026-09-01 (not estimates — an earlier internal guess of ~300k day units
was wrong by 16×, so treat any unmeasured corpus claim from either of us with suspicion):

| | count |
|---|---|
| channels | 775 |
| day units | 18,032 |
| thread units | 34,305 |
| messages (if message-grain units ever wanted) | 143,849 + 292,524 replies |

**Mount-shaped corpus ≈ 55k units — inside your own 62k reference envelope.** Your §11f sizing
table doesn't move. The corpus-scaling costs are all named and cold-path (repair scan
O(corpus) small rows; reprojection burst ≈ unit count; rebuild diff already has the resumable
job); rate-scaling lane volume is trivial at Slack's message rate; the tree stays ~55k nodes
so fuse-rust's 1M/30s cold-start question isn't stressed. Growth ≈ 15k units/year. No new
sizing rows, no asterisks.

## One point in your favor you didn't claim

Your §7 fold gate — reject any record where `record_seq <= local.record_seq` — is strictly
safer than what slack-fuse ships today. Our live-apply upsert is unconditional last-write-wins
on content (`apply.py:361`; `reply_count` got a `GREATEST` guard after an incident,
`content_md` never did), safe only because our current transport guarantees per-stream
apply order — the FIFO your §3 correctly refuses to promise. We have a recorded incident of a
late lossy duplicate overwriting richer data at the same ts. Adopting your fold rule *fixes* a
latent hazard of ours. We'll guard our upsert regardless of this RFC's fate; the contract
having the right rule by construction is a reason to want it.

## Proposals

1. **Joint approach to fuse-rust, endorsed.** We appear to be two tenants counterparty to the
   same engine effort (our six-seam co-design notes; your §13 targeting vfsd's successor). One
   coherent ask beats two overlapping ones. Our seam requirements to fold in: Control as
   declaration-level write-nodes→POST (engine must not mount `ro` when declared; distinct
   budget class), Trailer as presenter policy over §8-derived readiness (staleness visible in
   bytes), Ghost as declaration-level lookup-only + never-fetch entries.
2. **Name slack-fuse on the spec crate and the bridge-facing DB contract** (§13 matrix), as
   second tenant and per platform spec §10's library assignment.
3. **View units for v1**; `page_with_comments` as the first instance, resolving your §12 scope
   question inside the existing contract; contents-map named as the known evolution with §6
   rotation as the migration path, evidence bar = a real consumer needing single-identity
   subscription.
4. **Slack feed sketch as the feasibility check** §13 asks for: the unit table above, ctx =
   channel id, surrogates per §11a's encoding, blocks as `structurally_available`, totals as
   channel-meta extras, originals as created-on-divergence view units, point-in-time author
   names frozen into rendered content with a `user` unit carrying current profile. We are not
   asking you to build any of it; we are stating it fits so your "demonstrably possible"
   clause has its second data point in writing.

Sequencing honesty: our cutover is gated behind the platform's ingest steps and our own
in-flight work (ledger consolidation, an ordering-guard fix noted above), and our largest live
problem is a server memory leak unrelated to any of this. No urgency from our side either —
which is exactly why converging on paper now is cheap.
