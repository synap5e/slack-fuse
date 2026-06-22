#!/usr/bin/env python3
"""Destructive break-testing harness for slack-fuse-split.

Runs a battery of scenarios that deliberately break parts of the system
(PG container, FUSE daemon, mount) and verifies the running daemon
recovers without crashing or wedging. The shape of each scenario:

  1. Capture baseline state (PID, NRestarts, journal cursor).
  2. Trigger the break.
  3. Observe daemon behaviour for a configurable window.
  4. Restore (clean cleanup).
  5. Verify expectations against the captured journal + final state.

Why not pytest: these need a real systemd service + a real PG container
+ a real FUSE mount. They are destructive, sequential, and require human
interpretation of the live journal for the deeper scenarios. Keeping
them out of the unit suite means they don't accidentally run in CI.

USAGE

  # List scenarios
  python3 break_test.py --list

  # Run a single scenario
  python3 break_test.py --only pg_stop_idle

  # Run an entire group (e.g. all PG scenarios)
  python3 break_test.py --group pg

  # Run everything (be ready for ~5 minutes)
  python3 break_test.py --all

Output: a per-scenario pass/fail summary plus structured artifacts under
``/tmp/slack-fuse-break/<run-id>/`` for postmortem.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path

# =============================================================================
# Config + constants
# =============================================================================

UNIT = "slack-fuse-split.service"
MOUNT = "/views/slack-split"
# The host runs TWO PG instances: a native one on port 5433
# (``local-postgres.service``, pg_ctl-launched, the one slack-fuse-split's
# database_url points at) and a podman-compose container on port 5432
# (``claude-hooks-postgres.service``, used by claude-hooks). The slack-fuse
# tests must drive the native one — stopping the container does nothing
# to the FUSE projector.
PG_UNIT = "local-postgres.service"
PG_SOCKET = Path("/run/user/1000/local-postgres/.s.PGSQL.5433")
ARTIFACT_ROOT = Path("/tmp/slack-fuse-break")

# =============================================================================
# Helpers — system state
# =============================================================================


@dataclasses.dataclass(frozen=True)
class DaemonSnapshot:
    pid: int
    nrestarts: int
    proc_state: str  # ps STAT field: e.g. "Ssl", "Ds"
    wchan: str

    @property
    def alive(self) -> bool:
        return self.pid > 0

    @property
    def wedged(self) -> bool:
        return self.proc_state.startswith("D")


def systemctl_show(unit: str, prop: str) -> str:
    r = subprocess.run(
        ["systemctl", "--user", "show", unit, "-p", prop, "--value"],
        check=False,
        capture_output=True,
        text=True,
    )
    return r.stdout.strip()


def daemon_snapshot() -> DaemonSnapshot:
    pid_s = systemctl_show(UNIT, "MainPID")
    nr_s = systemctl_show(UNIT, "NRestarts")
    pid = int(pid_s) if pid_s.isdigit() else 0
    nr = int(nr_s) if nr_s.isdigit() else 0
    proc_state = "?"
    wchan = "?"
    if pid > 0 and Path(f"/proc/{pid}").exists():
        try:
            stat_line = Path(f"/proc/{pid}/stat").read_text()
            # field 3 is state code (single letter, optionally followed by flags)
            parts = stat_line.split()
            proc_state = parts[2] if len(parts) > 2 else "?"
            wchan_path = Path(f"/proc/{pid}/wchan")
            if wchan_path.exists():
                wchan = wchan_path.read_text().strip() or "0"
        except OSError:
            pass
    return DaemonSnapshot(pid=pid, nrestarts=nr, proc_state=proc_state, wchan=wchan)


def pg_running() -> bool:
    """True iff the PG container is up AND accepting connections."""
    state = systemctl_show(PG_UNIT, "ActiveState")
    if state != "active":
        return False
    # ``pg_isready`` via a quick PG psql or just check the socket. The socket
    # often lingers from earlier instances, so we do a connect probe.
    r = subprocess.run(
        ["psql", "postgresql:///postgres?host=/run/user/1000/local-postgres&port=5433", "-c", "SELECT 1"],
        check=False,
        capture_output=True,
        timeout=5,
    )
    return r.returncode == 0


def stop_pg() -> None:
    """Stop the native PG. ``pg_ctl stop -m fast`` would be cleanest but
    requires the same UID as the postmaster; ``systemctl stop`` works
    because the unit is a user unit and we are that user. Confirms the
    socket is actually gone before returning so callers can rely on the
    outage window."""
    subprocess.run(["systemctl", "--user", "stop", PG_UNIT], check=False, capture_output=True, timeout=30)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if not pg_running():
            return
        time.sleep(0.2)


def start_pg() -> None:
    subprocess.run(["systemctl", "--user", "start", PG_UNIT], check=False, capture_output=True, timeout=30)


def wait_for_pg_up(timeout_s: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if pg_running():
            return True
        time.sleep(0.5)
    return False


# =============================================================================
# Helpers — journal capture
# =============================================================================


def journal_lines_since(since_iso: str, *, unit: str = UNIT) -> list[str]:
    r = subprocess.run(
        ["journalctl", "--user", "-u", unit, "--since", since_iso, "--no-pager", "-o", "short-iso"],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return r.stdout.splitlines()


_INTERESTING = (
    "pg_health",
    "OperationalError",
    "reachable",
    "slow op:",
    "EIO",
    "FUSEError",
    "ERROR ",
    "Exception",
    "Traceback",
    "unexpected error",
    "left-over process",
    "Restart=on-failure",
)


def interesting_lines(lines: list[str]) -> list[str]:
    out = []
    for ln in lines:
        if any(k in ln for k in _INTERESTING):
            out.append(ln)
    return out


# =============================================================================
# Helpers — FUSE op probes (never touched without a hard timeout)
# =============================================================================


def fuse_probe(path: str, *, op: str = "stat", timeout_s: float = 3.0) -> tuple[bool, str]:
    """Run a small FUSE op against ``path`` under a hard timeout. Returns
    ``(ok, output)`` where ``ok`` is True iff the command exited 0 within
    the timeout. Subprocess timeout SIGKILLs the child if needed — but
    SIGKILL doesn't deliver to D-state, so an effective timeout limits the
    test's blast radius, not the daemon's."""
    if op == "stat":
        cmd = ["stat", "-c", "%n inode=%i size=%s", path]
    elif op == "ls":
        cmd = ["ls", path]
    elif op == "cat":
        cmd = ["cat", path]
    else:
        msg = f"unknown probe op {op!r}"
        raise ValueError(msg)
    try:
        r = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=timeout_s)
    except subprocess.TimeoutExpired:
        return False, f"timeout after {timeout_s}s"
    return r.returncode == 0, (r.stdout or r.stderr).strip()[:200]


# =============================================================================
# Scenario framework
# =============================================================================


@dataclasses.dataclass
class ScenarioResult:
    name: str
    group: str
    passed: bool
    notes: list[str]
    artifacts_dir: Path
    duration_s: float


class Scenario:
    name: str
    group: str
    description: str

    def setup(self) -> None:
        pass

    def run(self, ctx: "RunContext") -> None:  # pragma: no cover — overridden
        raise NotImplementedError

    def teardown(self) -> None:
        pass


class RunContext:
    """State carried into each scenario: the artifact dir to write to, a
    place to drop assertion notes, etc."""

    def __init__(self, artifact_dir: Path) -> None:
        self.artifact_dir = artifact_dir
        self.notes: list[str] = []
        self.passed = True

    def note(self, msg: str) -> None:
        print(f"  · {msg}")
        self.notes.append(msg)

    def expect(self, cond: bool, msg: str) -> None:
        if cond:
            self.note(f"PASS: {msg}")
        else:
            self.note(f"FAIL: {msg}")
            self.passed = False


# =============================================================================
# Scenarios — Group A: PG reachability
# =============================================================================


class PgStopIdle(Scenario):
    name = "pg_stop_idle"
    group = "pg"
    description = "Stop PG with no FUSE traffic; daemon should stay alive (PID stable)."

    def run(self, ctx: RunContext) -> None:
        before = daemon_snapshot()
        ctx.note(f"baseline: pid={before.pid} nrestarts={before.nrestarts} state={before.proc_state}")
        ctx.expect(before.alive and not before.wedged, "daemon healthy at start")
        if not pg_running():
            ctx.note("PG is not running — starting first")
            start_pg()
            wait_for_pg_up()

        since = now_iso()
        stop_pg()
        ctx.note("PG container stopped — sleeping 10s")
        time.sleep(10)
        start_pg()
        ctx.note("PG container restarted — sleeping 8s")
        time.sleep(8)

        after = daemon_snapshot()
        ctx.note(f"after: pid={after.pid} nrestarts={after.nrestarts} state={after.proc_state}")
        ctx.expect(after.pid == before.pid, "daemon PID unchanged across PG cycle")
        ctx.expect(after.nrestarts == before.nrestarts, "NRestarts did not increment")
        ctx.expect(not after.wedged, "daemon not D-state after cycle")

        # Capture journal for postmortem.
        lines = journal_lines_since(since)
        (ctx.artifact_dir / "journal.txt").write_text("\n".join(lines))
        intr = interesting_lines(lines)
        (ctx.artifact_dir / "interesting.txt").write_text("\n".join(intr))
        ctx.note(f"journal: {len(lines)} lines, {len(intr)} interesting (see {ctx.artifact_dir})")


class PgStopWithTraffic(Scenario):
    name = "pg_stop_with_traffic"
    group = "pg"
    description = "Stop PG while a reader is probing the mount; daemon stays alive, callbacks EIO cleanly."

    def run(self, ctx: RunContext) -> None:
        before = daemon_snapshot()
        ctx.expect(before.alive and not before.wedged, "daemon healthy at start")
        if not pg_running():
            start_pg()
            wait_for_pg_up()

        # Spawn a backgrounded reader that probes the mount every 0.3s.
        # We use the root readdir + a stat on a known path; both go through
        # the (now-bounded) ``_run_sync`` path.
        reader_log = ctx.artifact_dir / "reader.log"
        reader = subprocess.Popen(
            [
                "bash",
                "-c",
                (
                    "for i in $(seq 1 40); do "
                    "  out=$(timeout 2 stat -c '%s' /views/slack-split 2>&1); err=$?; "
                    "  echo \"[$i] $(date +%T) err=$err out=$out\"; "
                    "  out=$(timeout 2 ls /views/slack-split/channels 2>&1 | head -1); err=$?; "
                    "  echo \"[$i] $(date +%T) ls_err=$err out=$out\"; "
                    "  sleep 0.3; "
                    "done"
                ),
            ],
            stdout=reader_log.open("w"),
            stderr=subprocess.STDOUT,
        )

        since = now_iso()
        # Let the reader warm up
        time.sleep(2.0)
        stop_pg()
        ctx.note("PG container killed — letting reader hammer for 5s")
        time.sleep(5.0)
        start_pg()
        ctx.note("PG container restarted")
        wait_for_pg_up(15.0)
        # Let reader finish
        reader.wait(timeout=30)
        ctx.note(f"reader exited with {reader.returncode}")

        after = daemon_snapshot()
        ctx.note(f"after: pid={after.pid} nrestarts={after.nrestarts} state={after.proc_state}")
        ctx.expect(not after.wedged, "daemon not D-state after the cycle")
        ctx.expect(
            after.nrestarts == before.nrestarts,
            "NRestarts did not increment (daemon survived PG outage)",
        )

        lines = journal_lines_since(since)
        (ctx.artifact_dir / "journal.txt").write_text("\n".join(lines))
        intr = interesting_lines(lines)
        (ctx.artifact_dir / "interesting.txt").write_text("\n".join(intr))
        # Should have at least ONE EIO-shaped log line during the outage.
        eio = [ln for ln in intr if "EIO" in ln or "pg_health" in ln or "OperationalError" in ln]
        ctx.expect(
            len(eio) > 0,
            "at least one EIO / pg_health / OperationalError log line surfaced (FUSE ops actually hit dead PG)",
        )
        ctx.note(f"journal: {len(lines)} lines, {len(intr)} interesting, {len(eio)} EIO/pg_health-shaped")


class PgFlap(Scenario):
    name = "pg_flap"
    group = "pg"
    description = "Stop+start PG three times quickly; daemon stays alive each cycle."

    def run(self, ctx: RunContext) -> None:
        before = daemon_snapshot()
        ctx.expect(before.alive and not before.wedged, "daemon healthy at start")
        if not pg_running():
            start_pg()
            wait_for_pg_up()

        since = now_iso()
        for i in range(3):
            ctx.note(f"flap iteration {i + 1}: stopping PG")
            stop_pg()
            time.sleep(2)
            ctx.note(f"flap iteration {i + 1}: starting PG")
            start_pg()
            time.sleep(3)
            snap = daemon_snapshot()
            ctx.expect(not snap.wedged, f"iter {i + 1}: daemon not wedged")
            ctx.expect(snap.pid == before.pid, f"iter {i + 1}: PID stable")

        wait_for_pg_up(15.0)
        after = daemon_snapshot()
        ctx.note(f"after: pid={after.pid} nrestarts={after.nrestarts} state={after.proc_state}")

        lines = journal_lines_since(since)
        (ctx.artifact_dir / "journal.txt").write_text("\n".join(lines))


# =============================================================================
# Scenarios — Group D: process lifecycle
# =============================================================================


class CleanSigterm(Scenario):
    name = "clean_sigterm"
    group = "lifecycle"
    description = "systemctl stop the unit; verify clean exit + a clean restart."

    def run(self, ctx: RunContext) -> None:
        before = daemon_snapshot()
        ctx.expect(before.alive, "daemon alive at start")

        since = now_iso()
        subprocess.run(["systemctl", "--user", "stop", UNIT], check=False, capture_output=True, timeout=30)
        time.sleep(2)
        state = systemctl_show(UNIT, "ActiveState")
        ctx.expect(state in ("inactive", "failed"), f"unit stopped (state={state!r})")

        subprocess.run(["systemctl", "--user", "start", UNIT], check=False, capture_output=True, timeout=30)
        time.sleep(5)
        after = daemon_snapshot()
        ctx.note(f"after restart: pid={after.pid} state={after.proc_state}")
        ctx.expect(after.alive and not after.wedged, "fresh daemon healthy")
        ctx.expect(after.pid != before.pid, "PID changed (fresh process)")

        lines = journal_lines_since(since)
        (ctx.artifact_dir / "journal.txt").write_text("\n".join(lines))


class SigkillDaemon(Scenario):
    name = "sigkill_daemon"
    group = "lifecycle"
    description = "SIGKILL the daemon; systemd should respawn (Restart=on-failure)."

    def run(self, ctx: RunContext) -> None:
        before = daemon_snapshot()
        ctx.expect(before.alive, "daemon alive at start")
        if before.pid == 0:
            ctx.expect(False, "no PID to kill")
            return

        since = now_iso()
        try:
            os.kill(before.pid, signal.SIGKILL)
        except OSError as exc:
            ctx.note(f"kill failed: {exc}")
        time.sleep(6)

        after = daemon_snapshot()
        ctx.note(f"after: pid={after.pid} nrestarts={after.nrestarts} state={after.proc_state}")
        # systemd may not flag this as a Restart=on-failure if `Type=notify`
        # bookkeeping treats SIGKILL as clean. We accept either: the unit
        # must end up active again.
        ctx.expect(after.alive, "daemon respawned after SIGKILL")
        ctx.expect(after.pid != before.pid, "fresh PID")
        ctx.expect(not after.wedged, "respawn not in D-state")

        lines = journal_lines_since(since)
        (ctx.artifact_dir / "journal.txt").write_text("\n".join(lines))


# =============================================================================
# Runner
# =============================================================================


SCENARIOS: list[Scenario] = [
    PgStopIdle(),
    PgStopWithTraffic(),
    PgFlap(),
    CleanSigterm(),
    SigkillDaemon(),
]


def now_iso() -> str:
    return dt.datetime.now(dt.UTC).astimezone().isoformat(timespec="seconds")


def run_id() -> str:
    return dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%S")


def run_scenario(scenario: Scenario, root: Path) -> ScenarioResult:
    artifact_dir = root / f"{scenario.group}__{scenario.name}"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    ctx = RunContext(artifact_dir)
    print(f"\n==> [{scenario.group}] {scenario.name}")
    print(f"    {scenario.description}")
    start = time.monotonic()
    try:
        scenario.setup()
        scenario.run(ctx)
    except Exception as exc:  # noqa: BLE001
        ctx.note(f"SCENARIO RAISED: {type(exc).__name__}: {exc}")
        ctx.passed = False
    finally:
        try:
            scenario.teardown()
        except Exception as exc:  # noqa: BLE001
            ctx.note(f"teardown raised: {exc}")
    duration = time.monotonic() - start
    return ScenarioResult(
        name=scenario.name,
        group=scenario.group,
        passed=ctx.passed,
        notes=ctx.notes,
        artifacts_dir=artifact_dir,
        duration_s=duration,
    )


def restore_state() -> None:
    """Best-effort cleanup before/after the run: PG back up + daemon active.

    Handles the case where the unit is in ``failed`` state (e.g. previous
    scenario crashed the daemon): clears the failure flag with
    ``reset-failed`` and force-unmounts a stale FUSE entry that would
    otherwise make the next ``start`` race with a leftover mount."""
    if not pg_running():
        start_pg()
        wait_for_pg_up()
    state = systemctl_show(UNIT, "ActiveState")
    if state != "active":
        # ``failed`` requires reset-failed before start will retry.
        subprocess.run(["systemctl", "--user", "reset-failed", UNIT], check=False, capture_output=True, timeout=10)
        # If the previous instance left a FUSE entry behind, clean it up
        # before we ask systemd to start a fresh one.
        subprocess.run(["fusermount3", "-uz", MOUNT], check=False, capture_output=True, timeout=5)
        r = subprocess.run(
            ["systemctl", "--user", "start", UNIT], check=False, capture_output=True, timeout=30
        )
        if r.returncode != 0:
            print(f"  ! restore_state: start failed: {r.stderr.decode(errors='replace')[:200]}")
        # Give it a few seconds to actually come up.
        time.sleep(3)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--list", action="store_true", help="list scenarios + exit")
    parser.add_argument("--only", action="append", help="only run scenarios with these names (repeatable)")
    parser.add_argument("--group", action="append", help="only run scenarios in these groups (repeatable)")
    parser.add_argument("--all", action="store_true", help="run every scenario")
    parser.add_argument(
        "--root",
        default=str(ARTIFACT_ROOT),
        help=f"artifact root dir (default: {ARTIFACT_ROOT})",
    )
    args = parser.parse_args(argv)

    if args.list:
        for s in SCENARIOS:
            print(f"  [{s.group:10s}] {s.name:24s} — {s.description}")
        return 0

    if not (args.only or args.group or args.all):
        parser.error("pick --only / --group / --all (or use --list)")

    chosen: list[Scenario] = []
    for s in SCENARIOS:
        if args.all:
            chosen.append(s)
        elif args.only and s.name in args.only:
            chosen.append(s)
        elif args.group and s.group in args.group:
            chosen.append(s)
    if not chosen:
        parser.error("no scenarios matched")

    rid = run_id()
    root = Path(args.root) / rid
    root.mkdir(parents=True, exist_ok=True)
    print(f"# break-test run {rid}, artifacts under {root}")

    restore_state()  # known-good baseline

    results: list[ScenarioResult] = []
    for s in chosen:
        results.append(run_scenario(s, root))
        restore_state()  # always restore between scenarios

    # Summary
    print("\n========================================================")
    print(f"  SUMMARY ({len(results)} scenarios)")
    print("========================================================")
    width = max((len(r.name) for r in results), default=0)
    for r in results:
        flag = "PASS" if r.passed else "FAIL"
        print(f"  [{flag}] {r.name:{width}s}  ({r.duration_s:5.1f}s)  → {r.artifacts_dir}")
    failed = [r for r in results if not r.passed]
    print("========================================================")
    print(f"  {len(results) - len(failed)} passed, {len(failed)} failed")

    summary_json = root / "summary.json"
    summary_json.write_text(
        json.dumps(
            [
                {
                    "name": r.name,
                    "group": r.group,
                    "passed": r.passed,
                    "duration_s": r.duration_s,
                    "notes": r.notes,
                    "artifacts_dir": str(r.artifacts_dir),
                }
                for r in results
            ],
            indent=2,
        )
    )
    print(f"  summary: {summary_json}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
