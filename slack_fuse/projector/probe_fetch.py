"""Sync HTTP helpers for the ``_control/`` probe-sweep triggers."""

from __future__ import annotations

from typing import cast
from urllib.parse import quote

import httpx

from slack_fuse.projector.refresh_fetch import (
    DEFAULT_REFRESH_TIMEOUT_S,
    TRANSPORT_ERROR_CODE,
    _auth_headers,  # pyright: ignore[reportPrivateUsage]
)


def _probe_sweep_url(base_http_url: str, job_id: str | None, target: str | None) -> str:
    base = f"{base_http_url.rstrip('/')}/probe-sweep"
    if job_id is None:
        return base
    url = f"{base}/{quote(job_id, safe='')}"
    if target is None:
        return url
    return f"{url}/{quote(target, safe='')}"


def post_probe_sweep(  # noqa: PLR0913 - sync HTTP helper keeps endpoint pieces explicit.
    http_client: httpx.Client,
    base_http_url: str,
    *,
    job_id: str | None = None,
    target: str | None = None,
    shared_secret: str | None = None,
    timeout_s: float = DEFAULT_REFRESH_TIMEOUT_S,
) -> tuple[int, str | None]:
    """``POST {base}/probe-sweep[/job[/target]]``.

    Returns ``(status_code, status_message)``; transport failures use the same
    ``0`` sentinel as the other FUSE control HTTP helpers.
    """
    try:
        response = http_client.post(
            _probe_sweep_url(base_http_url, job_id, target),
            headers=_auth_headers(shared_secret),
            timeout=timeout_s,
        )
    except httpx.HTTPError:
        return TRANSPORT_ERROR_CODE, None
    try:
        body_raw: object = response.json()
    except ValueError:
        return response.status_code, None
    if isinstance(body_raw, dict):
        body = cast("dict[object, object]", body_raw)
        status = body.get("status")
        if isinstance(status, str):
            return response.status_code, status
        error = body.get("error")
        if isinstance(error, str):
            return response.status_code, error
    return response.status_code, None


__all__ = ["post_probe_sweep"]
