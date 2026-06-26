"""Sync HTTP fetch for the gaps ghost-files.

Mirrors ``originals_fetch.py``: sync httpx because the FUSE read path
runs in a worker thread (no trio context to thread through). Two
endpoints:

- ``GET /gaps/{channel_id}`` for the per-channel ``gaps.md``
- ``GET /gaps`` for the workspace ``/_workspace/gaps.md``
"""

from __future__ import annotations

import httpx

DEFAULT_FETCH_TIMEOUT_S = 5.0


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
