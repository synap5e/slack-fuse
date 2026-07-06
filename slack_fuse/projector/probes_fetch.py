"""Sync HTTP GET for the ``_control/probes`` read surface."""

from __future__ import annotations

import json
from datetime import datetime

import httpx
from pydantic import BaseModel, ConfigDict

from slack_fuse.projector.gaps_fetch import DEFAULT_CONTROL_TIMEOUT_S
from slack_fuse.projector.refresh_fetch import TRANSPORT_ERROR_CODE


class ProbeStatus(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    last_sweep_completed_at: datetime | None
    age_seconds: int | None
    channels_covered_last_sweep: int
    days_covered_last_sweep: int
    alert_threshold_seconds: int


def _auth_headers(shared_secret: str | None) -> dict[str, str]:
    if not shared_secret:
        return {}
    return {"Authorization": f"Bearer {shared_secret}"}


def fetch_probes(
    http_client: httpx.Client,
    base_http_url: str,
    *,
    shared_secret: str | None = None,
    timeout_s: float = DEFAULT_CONTROL_TIMEOUT_S,
) -> tuple[int, ProbeStatus | None]:
    """``GET {base}/probe-status`` → ``(status, body)``; status ``0`` on transport error."""
    url = f"{base_http_url.rstrip('/')}/probe-status"
    try:
        response = http_client.get(url, headers=_auth_headers(shared_secret), timeout=timeout_s)
    except httpx.HTTPError:
        return TRANSPORT_ERROR_CODE, None
    if response.status_code != 200:
        return response.status_code, None
    try:
        return response.status_code, ProbeStatus.model_validate_json(response.content)
    except ValueError:
        return 500, None


def fetch_probes_bytes(
    http_client: httpx.Client,
    base_http_url: str,
    *,
    shared_secret: str | None = None,
    timeout_s: float = DEFAULT_CONTROL_TIMEOUT_S,
) -> bytes:
    status, payload = fetch_probes(
        http_client,
        base_http_url,
        shared_secret=shared_secret,
        timeout_s=timeout_s,
    )
    if status == 200 and payload is not None:
        return (payload.model_dump_json(indent=2) + "\n").encode()
    return (json.dumps({"error": "server_unavailable" if status == 0 else f"http_{status}"}) + "\n").encode()


__all__ = ["ProbeStatus", "fetch_probes", "fetch_probes_bytes"]
