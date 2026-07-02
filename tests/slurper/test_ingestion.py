"""IngestionContext / source-envelope composition (no DB needed)."""

from __future__ import annotations

import pytest
import trio

from slack_fuse_server.slurper.ingestion import (
    CURRENT_SPAN_ID,
    BootContext,
    IngestionContext,
    compose_source,
    current_ingestion_context,
    ingesting,
    ingesting_run,
    make_source,
    new_ulid,
    process_boot,
)


def _ctx(producer: str = "test-producer") -> IngestionContext:
    return IngestionContext(
        producer=producer,
        boot_id="boot-1",
        task_id="task-1",
        commit="abc123",
    )


def test_make_source_omits_none_fields() -> None:
    assert make_source() == {}
    assert make_source(slack_cursor="c1", page_index=0, has_more=False) == {
        "slack_cursor": "c1",
        "page_index": 0,
        "has_more": False,
    }


def test_compose_source_is_none_outside_any_scope() -> None:
    assert current_ingestion_context() is None
    assert compose_source(None) is None
    assert compose_source({}) is None


def test_compose_source_merges_ambient_and_record_fields() -> None:
    with ingesting(_ctx()):
        composed = compose_source(make_source(slack_cursor="c9", page_index=3))
    assert composed == {
        "producer": "test-producer",
        "boot_id": "boot-1",
        "task_id": "task-1",
        "commit": "abc123",
        "slack_cursor": "c9",
        "page_index": 3,
    }


def test_compose_source_record_producer_overrides_ambient() -> None:
    with ingesting(_ctx()):
        composed = compose_source(make_source(producer="backfill-corrective-parent"))
    assert composed is not None
    assert composed["producer"] == "backfill-corrective-parent"


def test_compose_source_includes_current_span_id() -> None:
    token = CURRENT_SPAN_ID.set("span-42")
    try:
        with ingesting(_ctx()):
            composed = compose_source(None)
    finally:
        CURRENT_SPAN_ID.reset(token)
    assert composed is not None
    assert composed["span_id"] == "span-42"


def test_compose_source_record_only_without_ambient_context() -> None:
    composed = compose_source({"slack_event_ts": "1700000000.000100"})
    assert composed == {"slack_event_ts": "1700000000.000100"}


def test_ingesting_scope_restores_previous_context() -> None:
    outer = _ctx()
    inner = _ctx("inner")
    with ingesting(outer):
        with ingesting(inner):
            assert current_ingestion_context() is inner
        assert current_ingestion_context() is outer
    assert current_ingestion_context() is None


def test_ingesting_run_derives_fresh_run_id_and_trigger() -> None:
    with ingesting(_ctx()):
        with ingesting_run(triggered_by="reconnect") as run_ctx:
            assert run_ctx is not None
            assert run_ctx.run_id is not None
            assert run_ctx.triggered_by == "reconnect"
            assert run_ctx.boot_id == "boot-1"
            first_run_id = run_ctx.run_id
        with ingesting_run() as second:
            assert second is not None
            assert second.run_id != first_run_id


def test_ingesting_run_is_noop_without_ambient_context() -> None:
    with ingesting_run() as run_ctx:
        assert run_ctx is None
        assert current_ingestion_context() is None


def test_context_propagates_into_worker_threads_and_child_tasks() -> None:
    """The load-bearing trio property: sync psycopg bodies run via
    `to_thread.run_sync` and tasks spawned via `nursery.start_soon` both see
    the spawning task's ingestion context."""
    ctx = _ctx()
    seen: dict[str, IngestionContext | None] = {}

    async def main() -> None:
        with ingesting(ctx):
            seen["thread"] = await trio.to_thread.run_sync(current_ingestion_context)

            async def child() -> None:
                await trio.lowlevel.checkpoint()
                seen["child"] = current_ingestion_context()

            async with trio.open_nursery() as nursery:
                nursery.start_soon(child)

    trio.run(main)
    assert seen["thread"] is ctx
    assert seen["child"] is ctx


def test_boot_context_mints_distinct_task_ids() -> None:
    boot = BootContext(boot_id="boot-x", commit=None, image_digest=None)
    a = boot.task_context("socket-mode")
    b = boot.task_context("catchup", triggered_by="startup")
    assert a.task_id != b.task_id
    assert a.boot_id == b.boot_id == "boot-x"
    assert b.triggered_by == "startup"
    # Unset commit/image stay off the envelope entirely.
    assert "commit" not in a.fields()
    assert "image_digest" not in a.fields()


def test_process_boot_reads_env_once(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GIT_COMMIT", "deadbeef")
    monkeypatch.setenv("SLACK_FUSE_SERVER_IMAGE_DIGEST", "sha256:1234")
    boot = process_boot()
    assert boot.commit == "deadbeef"
    assert boot.image_digest == "sha256:1234"
    monkeypatch.delenv("GIT_COMMIT")
    monkeypatch.delenv("SLACK_FUSE_SERVER_IMAGE_DIGEST")
    bare = process_boot()
    assert bare.commit is None
    assert bare.image_digest is None
    assert bare.boot_id != boot.boot_id


def test_new_ulid_is_sortable_and_unique() -> None:
    ids = [new_ulid() for _ in range(5)]
    assert len(set(ids)) == 5
    assert all(len(i) == 26 for i in ids)
