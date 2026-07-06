"""Sync HTTP POST helper for ``_control/refill_gap``."""

from __future__ import annotations

from typing import cast
from urllib.parse import quote

import httpx
from pydantic import BaseModel, ConfigDict

from slack_fuse.control import result_for_status
from slack_fuse.projector.gaps_fetch import DEFAULT_CONTROL_TIMEOUT_S
from slack_fuse.projector.refresh_fetch import TRANSPORT_ERROR_CODE


class RefillResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    result: str
    status_code: int
    run_id: str | None = None


def _auth_headers(shared_secret: str | None) -> dict[str, str]:
    if not shared_secret:
        return {}
    return {"Authorization": f"Bearer {shared_secret}"}


def trigger_refill(  # noqa: PLR0913 - sync HTTP helper keeps endpoint pieces explicit.
    http_client: httpx.Client,
    base_http_url: str,
    channel_id: str,
    oldest: float,
    latest: float,
    *,
    shared_secret: str | None = None,
    timeout_s: float = DEFAULT_CONTROL_TIMEOUT_S,
) -> RefillResult:
    """``POST {base}/refill-window/{channel_id}`` → status verb + optional run id."""
    url = f"{base_http_url.rstrip('/')}/refill-window/{quote(channel_id, safe='')}"
    try:
        response = http_client.post(
            url,
            headers=_auth_headers(shared_secret),
            json={"oldest": oldest, "latest": latest},
            timeout=timeout_s,
        )
    except httpx.HTTPError:
        return RefillResult(result=result_for_status(TRANSPORT_ERROR_CODE), status_code=TRANSPORT_ERROR_CODE)

    run_id = _run_id_from_response(response)
    if response.status_code == 200:
        return RefillResult(result="refilled", status_code=response.status_code, run_id=run_id)
    return RefillResult(result=result_for_status(response.status_code), status_code=response.status_code, run_id=run_id)


def _run_id_from_response(response: httpx.Response) -> str | None:
    try:
        body: object = response.json()
    except ValueError:
        return None
    if not isinstance(body, dict):
        return None
    body_dict = cast("dict[object, object]", body)
    run_id = body_dict.get("run_id")
    return run_id if isinstance(run_id, str) else None


__all__ = ["RefillResult", "trigger_refill"]
