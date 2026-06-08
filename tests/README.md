# slack-fuse test infrastructure

Shared fixtures and helpers for the server-split rebuild. Downstream sprint
tracks build their tests on these.

## Fixtures (`conftest.py`)

| Fixture | Scope | What it gives you |
|---|---|---|
| `database_url` | session | Resolved Postgres DSN. Uses `DATABASE_URL` when set; otherwise auto-provisions a temporary local Postgres (`initdb` + `pg_ctl`) for the session. Skips DB-backed tests only if temporary startup is unavailable/fails. |
| `pg_conn` | function | A `psycopg.Connection` isolated to a fresh per-test schema (pg_temp-style). `search_path` is the test schema, so unqualified DDL lands there and is dropped at teardown. |
| `server_conn_factory` | function | Factory yielding multiple autocommit connections into one migrated schema (for multi-backend locking/offset tests). |
| `server_conn` | function | One connection from `server_conn_factory` in a fresh migrated schema. |
| `fake_slack_transport` | function | An `httpx.MockTransport` answering Slack Web API calls from fixtures. |
| `fake_slack_http` | function | An `httpx.Client` wired to `fake_slack_transport` (`base_url=https://slack.com/api`). |

Running the Postgres-backed tests:

```bash
export DATABASE_URL="postgresql:///slack_fuse_test"   # any reachable empty DB
uv run pytest -q
```

Without `DATABASE_URL`, the suite attempts temporary auto-provision:

```bash
unset DATABASE_URL
uv run pytest -q
```

Opt out (force skip when no `DATABASE_URL`):

```bash
export SLACK_FUSE_TEST_DISABLE_AUTO_POSTGRES=1
unset DATABASE_URL
uv run pytest -q
```

Auto-provision requires `initdb` and `pg_ctl` on `PATH`.

## `tests/_fake_slack/`

Fake Slack Web API. `make_fake_slack_transport(overrides=None)` builds an
`httpx.MockTransport` that routes by the Slack method in the request path
(`/api/<method>`) and replies from `fixtures/<method>.json`. Unknown methods
return `{ok: false, error: "fake_not_implemented"}`. Pass `overrides` to swap a
single endpoint for one test. Fixtures cover: `users.list`, `users.info`,
`conversations.list`, `conversations.info`, `conversations.history`,
`conversations.replies`, `chat.getPermalink`, `files.info`,
`apps.connections.open`. All fixtures validate against `slack_fuse.models`.

## `tests/_synthetic_events/`

Deterministic event-stream generators producing `SyntheticEvent` records
(`stream`, `offset`, `kind`, `ts`, `payload`) that match the wire `EventFrame`
shape. `SyntheticEvent.to_frame()` yields a real `EventFrame`. No randomness —
reproducible across runs.

Current generator corpus:
- message stream: `channel_message_events`, `channel_reply_events`,
  `message_changed_events`, `message_deleted_events`
- channel-list stream: `channel_added_events`, `channel_renamed_events`,
  `channel_archived_events`
- users stream: `user_added_events`, `user_renamed_events` (plus back-compat
  alias `user_events`)
- reactions (v2 placeholders): `reaction_added_events`,
  `reaction_removed_events` (documented for future slurper emission)

When adding new generators, keep payloads aligned with wire `EventFrame.kind`
payload shape from the RFC and validate via `FrameAdapter` in
`tests/_synthetic_events/test_self.py`.

## `tests/_fuse_harness/`

In-memory pyfuse3 op harness (no mount required):

- `fake_request_context()` — stand-in for `pyfuse3.RequestContext`
- `capture_readdir()` — context manager intercepting
  `pyfuse3.readdir_reply`, collecting `(name, attributes, next_id)` tuples
- `capture_lookup()` — async helper invoking a `lookup` op with default context
- `capture_getattr()` — async helper invoking a `getattr` op with default context
- `capture_read()` — async helper invoking a `read` op and returning bytes
- `tier_aware_channels_factory()` — deterministic mixed-tier channels-table
  stub (`hot`, `warm`, `cold`, `blocked`) for readdir filtering tests

## Layout

```
tests/
  conftest.py            # DB fixtures + fake-slack fixtures
  wire/                  # WS frame round-trip tests
  http/                  # HTTP DTO round-trip tests
  config/                # config-loader tests
  migrations/            # migration-runner tests (DB)
  _fake_slack/           # fake Slack Web API + fixtures/
  _synthetic_events/     # synthetic event generators
  _fuse_harness/         # in-memory pyfuse3 harness
  test_*.py              # existing single-process tests + infra smoke
```
