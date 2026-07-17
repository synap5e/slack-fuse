"""Client config loader.

Per RFC §Configuration → Client. Precedence: env vars (prefix `SLACK_FUSE_`)
first, then the TOML file, then built-in defaults. The client needs no Slack
tokens (they stay server-side); only the shared secret to talk to its own
server is required.

`SLACK_FUSE_MOUNTPOINT` overrides `mountpoint` (matches the existing
single-process behaviour). Use `load_client_config(toml_path=...)` for a
non-default TOML file (tests); `ClientConfig()` reads the conventional path.
"""

from __future__ import annotations

from pathlib import Path

from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)

_ENV_PREFIX = "SLACK_FUSE_"
_DEFAULT_TOML = Path.home() / ".config" / "slack-fuse" / "config.toml"


class ClientConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix=_ENV_PREFIX,
        toml_file=_DEFAULT_TOML,
        extra="ignore",
    )

    # Server endpoint.
    server_url: str = "ws://localhost:8765"
    shared_secret: str  # required; must match the server's

    # Postgres.
    database_url: str = "postgresql:///slack_fuse"

    # Mountpoint (overridden by the SLACK_FUSE_MOUNTPOINT env var).
    mountpoint: str = "/views/slack"

    # Bounded connection pool the projector's per-stream appliers share
    # (review P0-A). Sized below a stock local Postgres max_connections=100
    # after the split mount's other fixed connections are accounted for.
    projector_pool_size: int = 8

    # Staleness-trailer behaviour.
    #
    # ``stale_trailer_enabled`` is a bake-in comparison knob, NOT a long-term
    # setting: with it off, staleness no longer appends a trailer nor gates
    # kernel-cache priming, so an operator can A/B "trailer-warned" vs "no
    # trailer" reads. The unresolved-mention fallback gate stays on regardless.
    stale_trailer_enabled: bool = True
    # WS-disconnect staleness threshold (s). No frame for this long → trailer.
    stale_after_disconnect_s: float = 60.0
    # Optional append-only JSONL log of per-read trailer decisions for bake-in
    # false-positive measurement. ``None`` disables logging. Rotation is the
    # operator's responsibility (logrotate / cron) — the writer only appends.
    trailer_log_path: Path | None = None

    # Server blocked_channels SSOT sync interval.
    block_sync_interval_s: float = 30.0

    # DEPRECATED 2026-06-27, chain fully removed 2026-07-17 (FINDING-17).
    # This field is accepted for config-file backwards compatibility but has
    # no effect: the applier-side enforcement (WSClient → StreamApplier →
    # apply_event) was removed because the parameter was never wired into
    # cmd_mount_split, and b0dcff2 removed the startup migration. Use the
    # server-side blocked_channels table via ``_control/blocked_channels``
    # or ``POST /blocked-channels``. Non-empty entries on startup log a
    # warning classifying each id vs server SSOT (see
    # ``_migrate_legacy_always_blocked``).
    always_blocked_channel_ids: list[str] = []

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # Order = priority (earlier wins): explicit init kwargs, then env,
        # then the TOML file. Defaults fill anything none of them set.
        return (init_settings, env_settings, TomlConfigSettingsSource(settings_cls))


def load_client_config(toml_path: Path | None = None) -> ClientConfig:
    """Load `ClientConfig`, optionally from a non-default TOML file."""
    # BaseSettings populates required fields from env/TOML at runtime, so the
    # no-arg construction is correct despite pyright not seeing the sources.
    if toml_path is None:
        return ClientConfig()  # pyright: ignore[reportCallIssue]

    class _Configured(ClientConfig):
        model_config = SettingsConfigDict(
            env_prefix=_ENV_PREFIX,
            toml_file=toml_path,
            extra="ignore",
        )

    return _Configured()  # pyright: ignore[reportCallIssue]
