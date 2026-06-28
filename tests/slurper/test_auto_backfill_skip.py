# pyright: reportPrivateUsage=false
"""Auto-backfill restart skip for channels with prior completion events."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import cast

import psycopg
import pytest
import trio
from psycopg.rows import TupleRow

from slack_fuse.models import Message
from slack_fuse_render import ChannelId
from slack_fuse_server.backfill.types import BackfillResult
from slack_fuse_server.config import ServerConfig
from slack_fuse_server.slurper import __main__ as slurper_main
from slack_fuse_server.slurper.api import SlackClient, Validated
from slack_fuse_server.slurper.health import HealthEmitter
from slack_fuse_server.slurper.offsets import EventRecord, write_event
from tests.conftest import make_test_limiters, make_test_writer


class _ChannelListBackfiller:
    def __init__(self, channel_ids: list[str]) -> None:
        self._channel_ids = channel_ids

    @property
    def name(self) -> str:
        return "channel-list"

    async def channels_to_backfill(self) -> AsyncIterator[ChannelId]:
        for channel_id in self._channel_ids:
            yield ChannelId(channel_id)

    async def messages_for_channel(
        self,
        channel_id: ChannelId,
        since_ts: float | None = None,
    ) -> AsyncIterator[Validated[Message]]:
        return
        yield  # pragma: no cover - present only to make this an async generator


def _config(*, auto_backfill_skip_if_completed: bool = True) -> ServerConfig:
    return ServerConfig(
        slack_user_token="xoxp-test",
        slack_app_token="xapp-test",
        shared_secret="sek",
        auto_backfill_skip_if_completed=auto_backfill_skip_if_completed,
    )


def _seed_completion(
    conn: psycopg.Connection[TupleRow],
    channel_id: str,
    *,
    events_written: int,
    at: datetime | None = None,
) -> datetime:
    created_at = at or datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    offset = write_event(
        conn,
        EventRecord(
            stream="slurper-health",
            kind="backfill_completed",
            ts=None,
            payload={"channel_id": channel_id, "events_written": events_written},
        ),
    )
    assert offset is not None
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE events
            SET created_at = %s
            WHERE stream = 'slurper-health'
              AND offset_in_stream = %s
            """,
            (created_at, offset),
        )
        cur.execute(
            """
            SELECT created_at
            FROM events
            WHERE stream = 'slurper-health'
              AND offset_in_stream = %s
            """,
            (offset,),
        )
        row = cur.fetchone()
    assert row is not None
    stored_at = row[0]
    assert isinstance(stored_at, datetime)
    return stored_at


def _run_auto_backfill(
    conn: psycopg.Connection[TupleRow],
    monkeypatch: pytest.MonkeyPatch,
    channel_ids: list[str],
    *,
    auto_backfill_skip_if_completed: bool = True,
) -> tuple[list[str], list[float]]:
    writer = make_test_writer(conn)
    limiters = make_test_limiters()
    health = HealthEmitter(writer)
    backfiller = _ChannelListBackfiller(channel_ids)
    backfilled: list[str] = []
    sleep_calls: list[float] = []

    def _fake_make_backfiller(*_args: object, **_kwargs: object) -> _ChannelListBackfiller:
        return backfiller

    async def _fake_backfill_channel(
        _backfiller: object,
        channel_id: ChannelId,
        _ctx: object,
        *,
        since_ts: float | None = None,
    ) -> BackfillResult:
        await trio.lowlevel.checkpoint()
        backfilled.append(channel_id.value)
        return BackfillResult(channel_id=channel_id, messages=0, events_written=0, elapsed_s=0.0)

    async def _fake_sleep(seconds: float) -> None:
        await trio.lowlevel.checkpoint()
        sleep_calls.append(seconds)

    monkeypatch.setattr(slurper_main, "_make_backfiller", _fake_make_backfiller)
    monkeypatch.setattr(slurper_main, "backfill_channel", _fake_backfill_channel)
    monkeypatch.setattr(slurper_main.trio, "sleep", _fake_sleep)

    trio.run(
        slurper_main._auto_backfill,
        _config(auto_backfill_skip_if_completed=auto_backfill_skip_if_completed),
        writer,
        health,
        cast(SlackClient, object()),
        limiters,
    )
    return backfilled, sleep_calls


def test_auto_backfill_skips_channel_with_prior_completion(
    server_conn: psycopg.Connection[TupleRow],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_completion(server_conn, "C_DONE", events_written=17)

    backfilled, sleep_calls = _run_auto_backfill(server_conn, monkeypatch, ["C_DONE"])

    assert backfilled == []
    # Startup settle only; skipped channels do not pay the inter-channel API gap.
    assert sleep_calls == [30]


def test_auto_backfill_backfills_channel_without_prior_completion(
    server_conn: psycopg.Connection[TupleRow],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backfilled, sleep_calls = _run_auto_backfill(server_conn, monkeypatch, ["C_NEW"])

    assert backfilled == ["C_NEW"]
    assert sleep_calls == [30]


def test_auto_backfill_config_false_rewalks_completed_channel(
    server_conn: psycopg.Connection[TupleRow],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_completion(server_conn, "C_FORCE", events_written=9)

    backfilled, sleep_calls = _run_auto_backfill(
        server_conn,
        monkeypatch,
        ["C_FORCE"],
        auto_backfill_skip_if_completed=False,
    )

    assert backfilled == ["C_FORCE"]
    assert sleep_calls == [30]


def test_auto_backfill_skip_logs_completion_details(
    server_conn: psycopg.Connection[TupleRow],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    completed_at = _seed_completion(server_conn, "C_LOG", events_written=23)
    caplog.set_level(logging.INFO, logger=slurper_main.log.name)

    backfilled, _sleep_calls = _run_auto_backfill(server_conn, monkeypatch, ["C_LOG"])

    assert backfilled == []
    assert f"auto-backfill: skipping C_LOG — completed at {completed_at.isoformat()}, events_written=23" in caplog.text
