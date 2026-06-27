"""Sync HTTP helpers for server-side channel blocks and manual backfill."""

from __future__ import annotations

import json
from typing import cast
from urllib.parse import quote

import httpx

from slack_fuse.projector.refresh_fetch import (
    DEFAULT_REFRESH_TIMEOUT_S,
    TRANSPORT_ERROR_CODE,
    _auth_headers,  # pyright: ignore[reportPrivateUsage]
)


def _channel_url(base_http_url: str, prefix: str, channel_id: str) -> str:
    return f"{base_http_url.rstrip('/')}/{prefix}/{quote(channel_id, safe='')}"


def get_blocked_channels(
    http_client: httpx.Client,
    base_http_url: str,
    *,
    shared_secret: str | None = None,
    timeout_s: float = DEFAULT_REFRESH_TIMEOUT_S,
) -> tuple[int, dict[str, object]]:
    url = f"{base_http_url.rstrip('/')}/blocked-channels"
    try:
        response = http_client.get(url, headers=_auth_headers(shared_secret), timeout=timeout_s)
    except httpx.HTTPError:
        return TRANSPORT_ERROR_CODE, {"error": "server_unavailable"}
    try:
        body_raw: object = response.json()
    except ValueError:
        body_raw = {}
    return response.status_code, cast("dict[str, object]", body_raw if isinstance(body_raw, dict) else {})


def get_blocked_channels_bytes(
    http_client: httpx.Client,
    base_http_url: str,
    *,
    shared_secret: str | None = None,
    timeout_s: float = DEFAULT_REFRESH_TIMEOUT_S,
) -> bytes:
    status, payload = get_blocked_channels(
        http_client,
        base_http_url,
        shared_secret=shared_secret,
        timeout_s=timeout_s,
    )
    if status == 200:
        return (json.dumps(payload, indent=2) + "\n").encode()
    return (json.dumps({"error": "server_unavailable" if status == 0 else f"http_{status}"}) + "\n").encode()


def blocked_channel_ids_from_payload(payload: dict[str, object]) -> set[str]:
    rows = payload.get("blocked")
    if not isinstance(rows, list):
        return set()
    out: set[str] = set()
    for row in cast("list[object]", rows):
        if not isinstance(row, dict):
            continue
        row_dict = cast("dict[object, object]", row)
        channel_id = row_dict.get("channel_id")
        if isinstance(channel_id, str):
            out.add(channel_id)
    return out


def post_block_channel(  # noqa: PLR0913 - sync HTTP helper keeps endpoint pieces explicit.
    http_client: httpx.Client,
    base_http_url: str,
    channel_id: str,
    *,
    reason: str | None = None,
    shared_secret: str | None = None,
    timeout_s: float = DEFAULT_REFRESH_TIMEOUT_S,
) -> int:
    url = f"{base_http_url.rstrip('/')}/blocked-channels"
    payload: dict[str, str] = {"channel_id": channel_id}
    if reason:
        payload["reason"] = reason
    try:
        response = http_client.post(
            url,
            headers=_auth_headers(shared_secret),
            json=payload,
            timeout=timeout_s,
        )
    except httpx.HTTPError:
        return TRANSPORT_ERROR_CODE
    return response.status_code


def delete_block_channel(
    http_client: httpx.Client,
    base_http_url: str,
    channel_id: str,
    *,
    shared_secret: str | None = None,
    timeout_s: float = DEFAULT_REFRESH_TIMEOUT_S,
) -> int:
    try:
        response = http_client.delete(
            _channel_url(base_http_url, "blocked-channels", channel_id),
            headers=_auth_headers(shared_secret),
            timeout=timeout_s,
        )
    except httpx.HTTPError:
        return TRANSPORT_ERROR_CODE
    return response.status_code


def post_backfill_channel(
    http_client: httpx.Client,
    base_http_url: str,
    channel_id: str,
    *,
    shared_secret: str | None = None,
    timeout_s: float = DEFAULT_REFRESH_TIMEOUT_S,
) -> tuple[int, str | None]:
    try:
        response = http_client.post(
            _channel_url(base_http_url, "backfill-channel", channel_id),
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
    return response.status_code, None


__all__ = [
    "blocked_channel_ids_from_payload",
    "delete_block_channel",
    "get_blocked_channels",
    "get_blocked_channels_bytes",
    "post_backfill_channel",
    "post_block_channel",
]
