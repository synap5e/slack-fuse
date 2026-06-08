"""`slack-fuse tier` manual tier override command."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol

import psycopg
from psycopg import Cursor
from psycopg.errors import UndefinedColumn
from psycopg.rows import TupleRow
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)

from slack_fuse.slug import slugify

TierName = Literal["hot", "hidden", "blocked"]
_VALID_TIERS: tuple[TierName, TierName, TierName] = ("hot", "hidden", "blocked")

_ENV_PREFIX = "SLACK_FUSE_"
_DEFAULT_TOML = Path.home() / ".config" / "slack-fuse" / "config.toml"


class _TierConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix=_ENV_PREFIX,
        toml_file=_DEFAULT_TOML,
        extra="ignore",
    )

    database_url: str = "postgresql:///slack_fuse"

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (init_settings, env_settings, TomlConfigSettingsSource(settings_cls))


class TierCommandError(RuntimeError):
    """Domain error for `slack-fuse tier` failures."""

    def __init__(self, message: str, *, exit_code: int) -> None:
        super().__init__(message)
        self.exit_code = exit_code


@dataclass(frozen=True)
class TierUpdateResult:
    channel_id: str
    tier: TierName
    changed: bool


class _SubparserRegistry(Protocol):
    def add_parser(self, name: str, **kwargs: Any) -> argparse.ArgumentParser:
        ...


def register_tier_subcommand(subparsers: _SubparserRegistry) -> None:
    """Register `slack-fuse tier ...` on the top-level CLI parser."""
    parser = subparsers.add_parser(
        "tier",
        help="Set a channel tier override",
        description="Manually set channel visibility tier and mark source as manual",
    )
    parser.add_argument("slug_or_channel_id", help="Channel slug or channel ID")
    parser.add_argument("tier", choices=_VALID_TIERS, help="One of: hot, hidden, blocked")
    parser.set_defaults(func=cmd_tier)


def cmd_tier(args: argparse.Namespace) -> None:
    """Entry point used by `slack_fuse.__main__`."""
    raw_target = getattr(args, "slug_or_channel_id", None)
    raw_tier = getattr(args, "tier", None)
    if not isinstance(raw_target, str) or not isinstance(raw_tier, str):
        msg = "tier command arguments are invalid"
        raise ValueError(msg)

    desired_tier = _as_tier_name(raw_tier)
    try:
        result = set_channel_tier(
            database_url=load_database_url(),
            slug_or_channel_id=raw_target,
            desired_tier=desired_tier,
        )
    except TierCommandError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(exc.exit_code)
    except (psycopg.Error, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    if result.changed:
        print(f"{result.channel_id}: tier set to {result.tier} (tier_source=manual)")
    else:
        print(f"{result.channel_id}: tier already {result.tier} (tier_source=manual); no change")


def load_database_url() -> str:
    """Resolve the client DB URL from env/TOML/defaults."""
    return _TierConfig().database_url


def set_channel_tier(*, database_url: str, slug_or_channel_id: str, desired_tier: TierName) -> TierUpdateResult:
    """Apply a manual tier override to a channel row."""
    conn: psycopg.Connection[TupleRow] = psycopg.connect(database_url)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            channel_id = _resolve_channel_id(cur, slug_or_channel_id)
            if channel_id is None:
                msg = f"unknown channel slug or id: {slug_or_channel_id}"
                raise TierCommandError(msg, exit_code=2)

            cur.execute(
                "SELECT tier, tier_source, subscribed FROM channels WHERE channel_id = %s",
                (channel_id,),
            )
            row = cur.fetchone()
            if row is None:
                msg = f"channel row disappeared while updating tier: {channel_id}"
                raise TierCommandError(msg, exit_code=1)

            current_tier = _as_tier_name(str(row[0]))
            current_source = str(row[1])
            current_subscribed = bool(row[2])
            desired_subscribed = desired_tier != "blocked"
            if (
                current_tier == desired_tier
                and current_source == "manual"
                and current_subscribed == desired_subscribed
            ):
                return TierUpdateResult(channel_id=channel_id, tier=desired_tier, changed=False)

            cur.execute(
                "UPDATE channels "
                "SET tier = %s, tier_source = 'manual', subscribed = %s, updated_at = now() "
                "WHERE channel_id = %s",
                (desired_tier, desired_subscribed, channel_id),
            )
            if cur.rowcount != 1:
                msg = f"failed to update tier for channel: {channel_id}"
                raise TierCommandError(msg, exit_code=1)

            return TierUpdateResult(channel_id=channel_id, tier=desired_tier, changed=True)
    finally:
        conn.close()


def _resolve_channel_id(cur: Cursor[TupleRow], slug_or_channel_id: str) -> str | None:
    slug_match = _lookup_channel_id_by_slug(cur, slug_or_channel_id)
    if slug_match is not None:
        return slug_match

    cur.execute("SELECT channel_id FROM channels WHERE channel_id = %s", (slug_or_channel_id,))
    row = cur.fetchone()
    if row is None:
        return None
    return str(row[0])


def _lookup_channel_id_by_slug(cur: Cursor[TupleRow], slug: str) -> str | None:
    try:
        cur.execute("SELECT channel_id FROM channels WHERE slug = %s", (slug,))
    except UndefinedColumn:
        # Older schemas did not persist slug; replay from channel names.
        return _lookup_channel_id_by_replayed_slug(cur, slug)

    row = cur.fetchone()
    if row is None:
        return None
    return str(row[0])


def _lookup_channel_id_by_replayed_slug(cur: Cursor[TupleRow], slug: str) -> str | None:
    cur.execute("SELECT channel_id, name FROM channels ORDER BY channel_id")
    rows = cur.fetchall()
    slug_counts: dict[str, int] = {}
    for row in rows:
        channel_id = str(row[0])
        raw_name = row[1]
        name = str(raw_name) if isinstance(raw_name, str) else channel_id
        base_slug = slugify(name) or channel_id[:12].lower()
        count = slug_counts.get(base_slug, 0)
        slug_counts[base_slug] = count + 1
        candidate = base_slug if count == 0 else f"{base_slug}-{count + 1}"
        if candidate == slug:
            return channel_id
    return None


def _as_tier_name(value: str) -> TierName:
    if value not in _VALID_TIERS:
        msg = f"invalid tier value: {value!r}"
        raise ValueError(msg)
    return value
