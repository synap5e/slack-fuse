"""Sync HTTP fetch for gap views.

Mirrors ``originals_fetch.py``: sync httpx because the FUSE read path
runs in a worker thread (no trio context to thread through). Two
markdown ghost-file endpoints:

- ``GET /gaps/{channel_id}`` for the per-channel ``gaps.md``
- ``GET /gaps`` for the workspace ``/_workspace/gaps.md``

Also carries the operator control-surface ``GET /gap-candidates`` helper, which
returns day-presence refill candidates as typed JSON.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import httpx
from pydantic import BaseModel, ConfigDict, TypeAdapter

from slack_fuse.projector._control_cache import TTLCache
from slack_fuse.projector.refresh_fetch import TRANSPORT_ERROR_CODE

DEFAULT_FETCH_TIMEOUT_S = 5.0
# Sized for the day-presence query's baseline at prod scale (~2s, occasionally
# longer under DB contention). Anything under this timed out even under normal
# conditions — the 0.9s value was tuned for the fast blocked-channels endpoint.
DEFAULT_CONTROL_TIMEOUT_S = 5.0

# Short client-side TTL so the FUSE getattr/lookup/read cascade shares one
# response instead of firing 5+ concurrent slow queries per `cat`. Operator
# observability is not real-time; a ~30s freshness window is fine.
_GAP_CANDIDATES_TTL_S = 30.0
_gap_candidates_cache: TTLCache[bytes] = TTLCache(ttl_s=_GAP_CANDIDATES_TTL_S)


class GapRow(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    channel_id: str
    day: date
    oldest_ts: float
    latest_ts: float
    slack_sample_ts: str
    sampled_at: datetime
    gap_type: str


_GAP_ROWS = TypeAdapter(list[GapRow])


def fetch_channel_gaps(
    http_client: httpx.Client,
    base_http_url: str,
    channel_id: str,
    *,
    timeout_s: float = DEFAULT_FETCH_TIMEOUT_S,
) -> bytes:
    """``GET {base}/gaps/{channel_id}`` → response body."""
    url = f"{base_http_url.rstrip('/')}/gaps/{channel_id}"
    response = http_client.get(url, timeout=timeout_s)
    response.raise_for_status()
    return response.content


def fetch_workspace_gaps(
    http_client: httpx.Client,
    base_http_url: str,
    *,
    timeout_s: float = DEFAULT_FETCH_TIMEOUT_S,
) -> bytes:
    """``GET {base}/gaps`` → workspace-wide gaps summary body."""
    url = f"{base_http_url.rstrip('/')}/gaps"
    response = http_client.get(url, timeout=timeout_s)
    response.raise_for_status()
    return response.content


def fetch_gap_candidates(
    http_client: httpx.Client,
    base_http_url: str,
    *,
    timeout_s: float = DEFAULT_CONTROL_TIMEOUT_S,
) -> tuple[int, list[GapRow]]:
    """``GET {base}/gap-candidates`` → ``(status, rows)``; status ``0`` on transport error."""
    url = f"{base_http_url.rstrip('/')}/gap-candidates"
    try:
        response = http_client.get(url, timeout=timeout_s)
    except httpx.HTTPError:
        return TRANSPORT_ERROR_CODE, []
    if response.status_code != 200:
        return response.status_code, []
    try:
        return response.status_code, _GAP_ROWS.validate_json(response.content)
    except ValueError:
        return 500, []


def fetch_gaps_tsv_bytes(
    http_client: httpx.Client,
    base_http_url: str,
    *,
    timeout_s: float = DEFAULT_CONTROL_TIMEOUT_S,
) -> bytes:
    cached = _gap_candidates_cache.get()
    if cached is not None:
        return cached
    status, rows = fetch_gap_candidates(http_client, base_http_url, timeout_s=timeout_s)
    if status != 200:
        error = "server_unavailable" if status == TRANSPORT_ERROR_CODE else f"http_{status}"
        return f"# error\t{error}\n".encode()
    lines = [
        "\t".join((
            row.channel_id,
            row.day.isoformat(),
            f"{row.oldest_ts:.6f}",
            f"{row.latest_ts:.6f}",
            row.slack_sample_ts,
            _format_sampled_at(row.sampled_at),
            row.gap_type,
        ))
        for row in rows
    ]
    body = ("\n".join(lines) + ("\n" if lines else "")).encode()
    _gap_candidates_cache.set(body)
    return body


def _format_sampled_at(value: datetime) -> str:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC).isoformat()
    return value.astimezone(UTC).isoformat()


__all__ = [
    "DEFAULT_CONTROL_TIMEOUT_S",
    "DEFAULT_FETCH_TIMEOUT_S",
    "GapRow",
    "fetch_channel_gaps",
    "fetch_gap_candidates",
    "fetch_gaps_tsv_bytes",
    "fetch_workspace_gaps",
]
