"""Server config loader.

Per RFC §Configuration → Server. Precedence: env vars (prefix
`SLACK_FUSE_SERVER_`) first, then the TOML file, then built-in defaults.
Credentials and the shared secret are required (no default); every tunable
number carries its RFC default.

Use `load_server_config(toml_path=...)` to point at a non-default TOML file
(used by tests); `ServerConfig()` reads the conventional path.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)

_ENV_PREFIX = "SLACK_FUSE_SERVER_"
_DEFAULT_TOML = Path.home() / ".config" / "slack-fuse-server" / "config.toml"


class ServerConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix=_ENV_PREFIX,
        toml_file=_DEFAULT_TOML,
        extra="ignore",
    )

    # Slack credentials (required at runtime; the bot token need only exist).
    slack_user_token: str
    slack_app_token: str
    slack_bot_token: str = ""

    # Postgres.
    database_url: str = "postgresql:///slack_fuse_server"
    slurper_lock_timeout_s: float = 10.0
    slurper_statement_timeout_s: float = 30.0
    slurper_writer_pool_size: int = Field(default=4, ge=1)
    slurper_writer_pool_acquire_timeout_s: float = Field(default=30.0, gt=0.0)

    # WebSocket server.
    listen_addr: str = "127.0.0.1:8765"
    shared_secret: str  # required; clients send it as a header

    # Snapshot cadence.
    snapshot_every_n_events: int = 5000
    snapshot_max_age_hours: int = 24

    # Backfill thresholds.
    backfill_warn_at: int = 5000
    backfill_abort_at: int = 20000
    backfill_page_sleep_min_s: float = 15.0
    backfill_page_sleep_max_s: float = 90.0
    backfill_thread_sleep_min_s: float = 2.0
    backfill_thread_sleep_max_s: float = 8.0

    # Health-stream debouncing.
    slack_degraded_min_duration_s: float = 30.0

    # Slurper span logging slow-operation thresholds.
    span_slow_threshold_default_ms: int = 5000
    span_slow_threshold_backfill_channel_ms: int = 300000
    span_slow_threshold_snapshot_ms: int = 60000
    span_slow_threshold_socket_event_ms: int = 1000

    # Reconnect / restart catchup (slurper/catchup.py). A bounded gap-fill that
    # recovers messages Slack dropped while the slurper was down longer than its
    # ~5min event buffer — runs once at startup (the restart case) and on any
    # in-process reconnect whose downtime exceeds the gap threshold.
    catchup_enabled: bool = True
    catchup_gap_threshold_s: float = 300.0
    catchup_max_lookback_s: float = 3600.0
    catchup_channel_gap_s: float = 1.5
    catchup_startup_delay_s: float = 30.0
    catchup_page_sleep_min_s: float = 1.0
    catchup_page_sleep_max_s: float = 3.0
    catchup_thread_sleep_min_s: float = 1.0
    catchup_thread_sleep_max_s: float = 3.0

    # State-reconciliation probes (slurper/probes.py). The sweep task runs
    # hourly by default; each job has its own restart-safe cadence anchored on
    # the latest persisted raw API sample event.
    probe_sweep_interval_s: float = 3600.0
    probe_channel_older_than_oldest_cadence_s: float = 7 * 86400.0
    probe_channel_newest_message_cadence_s: float = 86400.0
    probe_channel_inventory_cadence_s: float = 86400.0
    probe_workspace_user_count_cadence_s: float = 86400.0
    probe_channel_day_presence_cadence_s: float = 7 * 86400.0

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


def load_server_config(toml_path: Path | None = None) -> ServerConfig:
    """Load `ServerConfig`, optionally from a non-default TOML file."""
    # BaseSettings populates required fields from env/TOML at runtime, so the
    # no-arg construction is correct despite pyright not seeing the sources.
    if toml_path is None:
        return ServerConfig()  # pyright: ignore[reportCallIssue]

    class _Configured(ServerConfig):
        model_config = SettingsConfigDict(
            env_prefix=_ENV_PREFIX,
            toml_file=toml_path,
            extra="ignore",
        )

    return _Configured()  # pyright: ignore[reportCallIssue]
