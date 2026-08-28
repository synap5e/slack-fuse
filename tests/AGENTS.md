# tests — 1,107 tests, real Postgres, faked Slack

Organised by subsystem, mirroring the source tree. No unit/integration split and no `slow` marker: DB-backed tests are simply the normal case.

## Where the coverage lives

| Directory | Covers | Files |
|---|---|---|
| `projector/` | apply, cursors, streams, snapshots, ledger, coalescer, disk projection, WS recovery, reconnect | 27 |
| `fuse_v2/` | paths, ghost files, control surface, kernel-cache invariants, trailers, callback budgets, PgHealth | 21 |
| `slurper/` | socket mode, webhook inbox, offsets, catchup, channels/users, health, probes, pacing, spans | 22 |
| `http/` | routes, DTOs, metrics, snapshot, webhook, resolve/permalink | 10 |
| `server/` | gaps, originals, channel fold, probe sweeps, search totals | 7 |
| `backfill/` | Slack API + legacy import, resume, skip predicate | 5 |
| `render/`, `wire/`, `snapshot/`, `migrations/`, `cli/`, `config/`, `dispatch/`, `tools/` | as named | 2-6 each |

Support (not tests): `_fake_slack/` (transport + 15 JSON fixtures), `_fuse_harness/` (in-memory pyfuse3), `_synthetic_events/` (deterministic EventFrame generators). Each has a `test_self.py` that tests the harness itself — keep those passing or every downstream test is built on sand.

## Fixtures

`conftest.py` (whole tree):
- `database_url` — session-scoped. Uses `DATABASE_URL`, else auto-provisions a **temporary real cluster** via `initdb` + `pg_ctl`. Skips if unavailable. `SLACK_FUSE_TEST_DISABLE_AUTO_POSTGRES=1` turns provisioning off.
- `pg_conn` — unique schema per test, dropped `CASCADE`. Migrations are applied by the test.
- `server_conn_factory` / `server_conn` — fresh migrated **server** schema, autocommit, multiple connections available.
- `fake_slack_transport` / `fake_slack_http` — `httpx.MockTransport` over `tests/_fake_slack/fixtures/*.json`, base URL `https://slack.com/api`. Unknown methods return `{"ok": false, "error": "fake_not_implemented"}`; per-test `overrides={...}` replaces individual responses.
- Helpers: `make_test_limiters()`, `make_test_writer()`, `RecordingSupervisor`.

`projector/conftest.py` — `client_conn_factory` / `client_conn` (migrated **client** schema), `RecordingSink`.
`fuse_v2/conftest.py` — re-exports the client fixtures, adds `fake_pyfuse3` (records `notify_store` / `invalidate_inode`), `utc_tz`, `ops` (a wired `SlackFuseOpsV2`), and `seed_*` helpers.

## Conventions

- **Real Postgres, never a fake.** Schema-per-test isolation, not transaction rollback. Multiple connections are deliberate — locking, LISTEN/NOTIFY and concurrent-writer behaviour is the thing under test. Connection doubles (`_FakeConnection`) appear only in `test_reconnecting_conn.py` and `test_skip_predicate.py`.
- **Trio via `pytest-trio`**: `@pytest.mark.trio`, or module-level `pytestmark`. Plenty of sync tests call `trio.run(...)` directly.
- **No `MockClock` / `autojump`.** Determinism comes from injected `clock`/`sleep` callables, monkeypatching, and explicit `trio.lowlevel.checkpoint()`. Keep it that way; an autojump clock would hide the real ordering bugs these tests exist to catch.
- Only `benchmark` is registered (`pyproject.toml`), used solely by `projector/test_projection_rollout_benchmark.py`. It is **not** excluded by default.
- `tests/*.py` carry a `BLE001` per-file ignore — broad excepts are allowed in tests only.

## Commands

```bash
uv run pytest                          # full suite, ~75-130s
uv run pytest tests/projector -x       # one subsystem
uv run pytest -m "not benchmark"
TZ=UTC uv run pytest                   # see below
```

## Known traps

- **9 server gap/day-presence tests fail under `America/Los_Angeles`** (both dev hosts' local zone) because their SQL uses `to_timestamp(...)::date` / `date_trunc(...)` without `AT TIME ZONE 'UTC'`. Green under `TZ=UTC`. It is a real SQL bug, not test-only date arithmetic — don't "fix" it by pinning TZ in conftest.
- **`test_resume_plan_fast_at_scale[1000|5000]`** fail 5/5 at 0.542-0.630s against a 0.5s ceiling. A threshold regression, not a flake.
- Deleting or `xfail`-ing a failing test to go green is a hard block here. See BACKLOG.md — both of the above are tracked.
