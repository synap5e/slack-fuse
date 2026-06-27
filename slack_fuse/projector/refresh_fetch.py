"""Sync HTTP POST for the ``_control/`` refresh triggers.

Mirrors ``gaps_fetch.py`` / ``originals_fetch.py``: sync httpx because the FUSE
write path fires these from a worker thread (``trio.to_thread.run_sync``), so
there is no trio context to thread through. Two endpoints on the slurper
server, both fire-and-forget (the server returns 202 immediately and runs the
``conversations.info`` sweep in the background):

- ``POST /refresh-channels`` — workspace-wide sweep
- ``POST /refresh-channels/{channel_id}`` — single channel

The timeout is deliberately tight (sub-second): the call runs inside a FUSE
``release`` callback bounded by the per-callback budget. The endpoints return
202 without doing any Slack work, so a healthy server answers well within it.
Any transport error or timeout returns the ``0`` sentinel — the caller maps it
to ``server_unavailable`` (see ``slack_fuse.control.result_for_status``).
"""

from __future__ import annotations

import httpx

#: Sub-second so the whole POST fits inside the FUSE per-callback budget.
DEFAULT_REFRESH_TIMEOUT_S = 0.9

#: Returned when the server is unreachable (transport error / timeout).
TRANSPORT_ERROR_CODE = 0


def _auth_headers(shared_secret: str | None) -> dict[str, str]:
    """Bearer header when a secret is configured; empty otherwise.

    The server treats a missing secret as "no auth required" (home-lab), so an
    empty header dict is correct when ``shared_secret`` is unset/empty.
    """
    if not shared_secret:
        return {}
    return {"Authorization": f"Bearer {shared_secret}"}


def post_refresh_channels(
    http_client: httpx.Client,
    base_http_url: str,
    *,
    shared_secret: str | None = None,
    timeout_s: float = DEFAULT_REFRESH_TIMEOUT_S,
) -> int:
    """``POST {base}/refresh-channels`` → HTTP status code (``0`` on transport error)."""
    url = f"{base_http_url.rstrip('/')}/refresh-channels"
    try:
        response = http_client.post(url, headers=_auth_headers(shared_secret), timeout=timeout_s)
    except httpx.HTTPError:
        return TRANSPORT_ERROR_CODE
    return response.status_code


def post_refresh_channel(
    http_client: httpx.Client,
    base_http_url: str,
    channel_id: str,
    *,
    shared_secret: str | None = None,
    timeout_s: float = DEFAULT_REFRESH_TIMEOUT_S,
) -> int:
    """``POST {base}/refresh-channels/{channel_id}`` → status code (``0`` on transport error)."""
    url = f"{base_http_url.rstrip('/')}/refresh-channels/{channel_id}"
    try:
        response = http_client.post(url, headers=_auth_headers(shared_secret), timeout=timeout_s)
    except httpx.HTTPError:
        return TRANSPORT_ERROR_CODE
    return response.status_code


__all__ = [
    "DEFAULT_REFRESH_TIMEOUT_S",
    "TRANSPORT_ERROR_CODE",
    "post_refresh_channel",
    "post_refresh_channels",
]
