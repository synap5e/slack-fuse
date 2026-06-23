"""Sync HTTP fetch for ``channel.original.md`` ghost-file rendering.

The FUSE read path runs in a worker thread (dispatched by ``_run_sync``),
so a sync httpx call is the natural fit — no trio context, no nursery
ownership question. The slurper-server replays its events table on the
other end; we just GET the bytes and hand them to the existing resolver.
"""

from __future__ import annotations

from urllib.parse import urlencode

import httpx

DEFAULT_FETCH_TIMEOUT_S = 5.0


def fetch_originals(  # noqa: PLR0913 — sync HTTP call needs the client + url + ts pair + timeout.
    http_client: httpx.Client,
    base_http_url: str,
    channel_id: str,
    *,
    from_epoch: float,
    to_epoch: float,
    timeout_s: float = DEFAULT_FETCH_TIMEOUT_S,
) -> bytes:
    """``GET {base}/originals/{channel_id}?from=&to=`` → response body.

    Raises :class:`httpx.HTTPError` on any transport / non-2xx outcome; the
    caller (FUSE read) converts to ``EIO`` via the standard ``_callback_guard``
    exception path so the user sees a normal IO error rather than a hang.
    """
    query = urlencode({"from": f"{from_epoch:.6f}", "to": f"{to_epoch:.6f}"})
    url = f"{base_http_url.rstrip('/')}/originals/{channel_id}?{query}"
    response = http_client.get(url, timeout=timeout_s)
    response.raise_for_status()
    return response.content
