"""`slack-fuse tier` manual tier override command."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol

import psycopg
from psycopg.rows import TupleRow
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)

from slack_fuse.fuse_v2_helpers import CONV_ROOTS, fetch_channel_by_slug
from slack_fuse.projector.apply import _default_tier  # pyright: ignore[reportPrivateUsage]

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
    tier_source: Literal["auto", "manual"] = "manual"


class _SubparserRegistry(Protocol):
    def add_parser(self, name: str, **kwargs: Any) -> argparse.ArgumentParser:
        ...


def register_tier_subcommand(subparsers: _SubparserRegistry) -> None:
    """Register `slack-fuse tier ...` on the top-level CLI parser."""
    parser = subparsers.add_parser(
        "tier",
        help="Set a channel tier override",
        description=(
            "Manually set channel visibility tier (sets tier_source='manual', "
            "sticky against automatic re-evaluation), or use --reset-to-auto "
            "to clear a manual override and let the projector recompute."
        ),
    )
    parser.add_argument(
        "slug_or_channel_id",
        help="Channel slug (e.g. 'general' or 'channels/general'), or channel ID (e.g. 'C123')",
    )
    parser.add_argument(
        "tier",
        nargs="?",
        choices=_VALID_TIERS,
        help="One of: hot, hidden, blocked. Omit when using --reset-to-auto.",
    )
    parser.add_argument(
        "--reset-to-auto",
        dest="reset_to_auto",
        action="store_true",
        help=(
            "Clear the manual tier override: flip tier_source back to 'auto' and "
            "recompute tier from the channel's is_archived/is_im/is_mpim/is_member "
            "flags (same logic the projector uses on `channel_added`). When set, "
            "the positional `tier` argument must be omitted."
        ),
    )
    parser.set_defaults(func=cmd_tier)


def cmd_tier(args: argparse.Namespace) -> None:
    """Entry point used by `slack_fuse.__main__`."""
    raw_target = getattr(args, "slug_or_channel_id", None)
    raw_tier = getattr(args, "tier", None)
    reset_to_auto = bool(getattr(args, "reset_to_auto", False))
    if not isinstance(raw_target, str):
        msg = "tier command arguments are invalid"
        raise ValueError(msg)

    if reset_to_auto and raw_tier is not None:
        print(
            "Error: --reset-to-auto cannot be combined with a tier value; omit the tier argument.",
            file=sys.stderr,
        )
        sys.exit(2)
    if not reset_to_auto and not isinstance(raw_tier, str):
        print(
            "Error: tier is required unless --reset-to-auto is set.",
            file=sys.stderr,
        )
        sys.exit(2)

    try:
        if reset_to_auto:
            result = reset_channel_tier_to_auto(
                database_url=load_database_url(),
                slug_or_channel_id=raw_target,
            )
        else:
            assert isinstance(raw_tier, str)  # narrowed by the guard above
            desired_tier = _as_tier_name(raw_tier)
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
        print(f"{result.channel_id}: tier set to {result.tier} (tier_source={result.tier_source})")
    else:
        print(f"{result.channel_id}: tier already {result.tier} (tier_source={result.tier_source}); no change")


def load_database_url() -> str:
    """Resolve the client DB URL from env/TOML/defaults."""
    return _TierConfig().database_url


def set_channel_tier(*, database_url: str, slug_or_channel_id: str, desired_tier: TierName) -> TierUpdateResult:
    """Apply a manual tier override to a channel row."""
    conn: psycopg.Connection[TupleRow] = psycopg.connect(database_url)
    conn.autocommit = True
    try:
        channel_id = _resolve_channel_id(conn, slug_or_channel_id)
        if channel_id is None:
            msg = f"unknown channel slug or id: {slug_or_channel_id}"
            raise TierCommandError(msg, exit_code=2)

        with conn.cursor() as cur:
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
                return TierUpdateResult(
                    channel_id=channel_id, tier=desired_tier, changed=False, tier_source="manual"
                )

            cur.execute(
                "UPDATE channels "
                "SET tier = %s, tier_source = 'manual', subscribed = %s, updated_at = now() "
                "WHERE channel_id = %s",
                (desired_tier, desired_subscribed, channel_id),
            )
            if cur.rowcount != 1:
                msg = f"failed to update tier for channel: {channel_id}"
                raise TierCommandError(msg, exit_code=1)

            return TierUpdateResult(
                channel_id=channel_id, tier=desired_tier, changed=True, tier_source="manual"
            )
    finally:
        conn.close()


def reset_channel_tier_to_auto(*, database_url: str, slug_or_channel_id: str) -> TierUpdateResult:
    """Clear a manual tier override: flip ``tier_source`` back to ``'auto'`` and
    recompute the tier from the channel's flags using the same
    :func:`_default_tier` logic the projector applies on ``channel_added``.

    Idempotent: if the row is already ``tier_source='auto'`` AND the current
    tier already matches the recomputed value AND ``subscribed`` is consistent,
    returns ``changed=False`` without writing.
    """
    conn: psycopg.Connection[TupleRow] = psycopg.connect(database_url)
    conn.autocommit = True
    try:
        channel_id = _resolve_channel_id(conn, slug_or_channel_id)
        if channel_id is None:
            msg = f"unknown channel slug or id: {slug_or_channel_id}"
            raise TierCommandError(msg, exit_code=2)

        with conn.cursor() as cur:
            cur.execute(
                "SELECT tier, tier_source, subscribed, is_archived, is_im, is_mpim, is_member "
                "FROM channels WHERE channel_id = %s",
                (channel_id,),
            )
            row = cur.fetchone()
            if row is None:
                msg = f"channel row disappeared while resetting tier: {channel_id}"
                raise TierCommandError(msg, exit_code=1)

            current_tier = _as_tier_name(str(row[0]))
            current_source = str(row[1])
            current_subscribed = bool(row[2])
            is_archived = bool(row[3])
            is_im = bool(row[4])
            is_mpim = bool(row[5])
            is_member = bool(row[6])

            recomputed = _as_tier_name(
                _default_tier(
                    is_archived=is_archived,
                    is_im=is_im,
                    is_mpim=is_mpim,
                    is_member=is_member,
                )
            )
            desired_subscribed = recomputed != "blocked"
            if (
                current_source == "auto"
                and current_tier == recomputed
                and current_subscribed == desired_subscribed
            ):
                return TierUpdateResult(
                    channel_id=channel_id, tier=recomputed, changed=False, tier_source="auto"
                )

            cur.execute(
                "UPDATE channels "
                "SET tier = %s, tier_source = 'auto', subscribed = %s, updated_at = now() "
                "WHERE channel_id = %s",
                (recomputed, desired_subscribed, channel_id),
            )
            if cur.rowcount != 1:
                msg = f"failed to reset tier for channel: {channel_id}"
                raise TierCommandError(msg, exit_code=1)

            return TierUpdateResult(
                channel_id=channel_id, tier=recomputed, changed=True, tier_source="auto"
            )
    finally:
        conn.close()


def _resolve_channel_id(conn: psycopg.Connection[TupleRow], slug_or_channel_id: str) -> str | None:
    """Resolve a CLI target to a channel id using the SAME slug logic as FUSE V2.

    Accepts (in priority order):

    1. ``<conv-root>/<slug>`` — fully qualified, resolved against that conv root.
    2. ``<slug>`` — resolved across all conv roots via ``fetch_channel_by_slug``
       (the exact hot-first, per-conv-root assignment FUSE V2 uses, incl. DM
       display-name slugs). A slug matching in more than one conv root is
       ambiguous and raises (the operator must qualify it).
    3. ``<channel_id>`` — a literal id, the unambiguous escape hatch.

    Reuses ``fetch_channel_by_slug`` / ``assign_conv_root_slugs`` so a slug that
    works in the mounted filesystem resolves to the same channel here, against
    the real production schema (review P1-G — the old path queried a
    non-existent ``channels.slug`` column, then fell back to a slug replay that
    didn't partition by conv root, mishandled DMs, and didn't exclude blocked).
    """
    qualified = _resolve_qualified_slug(conn, slug_or_channel_id)
    if qualified is not None:
        return qualified

    if "/" not in slug_or_channel_id:
        slug_match = _resolve_bare_slug(conn, slug_or_channel_id)
        if slug_match is not None:
            return slug_match

    return _resolve_literal_channel_id(conn, slug_or_channel_id)


def _resolve_qualified_slug(conn: psycopg.Connection[TupleRow], target: str) -> str | None:
    """Resolve a ``<conv-root>/<slug>`` target, or return ``None`` if not one."""
    if "/" not in target:
        return None
    conv_root, _, slug = target.partition("/")
    if conv_root not in CONV_ROOTS or not slug:
        return None
    row = fetch_channel_by_slug(conn, conv_root, slug, allow_hidden=True)
    return None if row is None else row.channel_id


def _resolve_bare_slug(conn: psycopg.Connection[TupleRow], slug: str) -> str | None:
    """Resolve a bare ``<slug>`` across every conv root.

    Raises ``TierCommandError`` if the slug collides across conv roots so the
    operator re-runs with a ``<conv-root>/<slug>`` qualifier rather than silently
    re-tiering the wrong channel.
    """
    matches: list[tuple[str, str]] = []
    for conv_root in CONV_ROOTS:
        row = fetch_channel_by_slug(conn, conv_root, slug, allow_hidden=True)
        if row is not None:
            matches.append((conv_root, row.channel_id))
    if not matches:
        return None
    if len(matches) > 1:
        roots = ", ".join(f"{conv_root}/{slug}" for conv_root, _ in matches)
        msg = f"slug {slug!r} is ambiguous across conv roots; qualify it as one of: {roots}"
        raise TierCommandError(msg, exit_code=2)
    return matches[0][1]


def _resolve_literal_channel_id(conn: psycopg.Connection[TupleRow], channel_id: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute("SELECT channel_id FROM channels WHERE channel_id = %s", (channel_id,))
        row = cur.fetchone()
    return None if row is None else str(row[0])


def _as_tier_name(value: str) -> TierName:
    if value not in _VALID_TIERS:
        msg = f"invalid tier value: {value!r}"
        raise ValueError(msg)
    return value
