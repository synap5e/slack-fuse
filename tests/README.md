# slack-fuse test infrastructure

Shared fixtures and helpers for the server-split rebuild. Downstream sprint
tracks build their tests on these.

## Fixtures (`conftest.py`)

| Fixture | Scope | What it gives you |
|---|---|---|
| `pg_conn` | function | A `psycopg.Connection` isolated to a fresh per-test schema (pg_temp-style). **Skips the test if `DATABASE_URL` is unset.** `search_path` is the test schema, so unqualified DDL lands there and is dropped at teardown. |
| `fake_slack_transport` | function | An `httpx.MockTransport` answering Slack Web API calls from fixtures. |
| `fake_slack_http` | function | An `httpx.Client` wired to `fake_slack_transport` (`base_url=https://slack.com/api`). |

Running the Postgres-backed tests:

```bash
export DATABASE_URL="postgresql:///slack_fuse_test"   # any reachable empty DB
uv run pytest -q
```

Without `DATABASE_URL`, the DB tests skip (reported, not failed); everything
else runs.

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
shape. `SyntheticEvent.to_frame()` yields a real `EventFrame`. Generators:
`channel_message_events`, `channel_reply_events`, `user_events`. No randomness
— reproducible across runs. Used by projector tests to drive chunk-write logic
without the slurper.

## `tests/_fuse_harness/`

**Skeleton** (fleshed out in Sprint 2F / used by 3B). Invoke `pyfuse3`
handlers without mounting:

- `fake_request_context()` — stand-in for `pyfuse3.RequestContext`.
- `capture_readdir()` — context manager intercepting `pyfuse3.readdir_reply`,
  collecting `(name, attributes, next_id)` tuples a `readdir` handler emits.

## Layout

```
tests/
  conftest.py            # pg_conn + fake-slack fixtures
  wire/                  # WS frame round-trip tests
  http/                  # HTTP DTO round-trip tests
  config/                # config-loader tests
  migrations/            # migration-runner tests (DB)
  _fake_slack/           # fake Slack Web API + fixtures/
  _synthetic_events/     # synthetic event generators
  _fuse_harness/         # in-memory pyfuse3 harness (skeleton)
  test_*.py              # existing single-process tests + infra smoke
```
