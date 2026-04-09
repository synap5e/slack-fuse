"""Fetch and parse Slack canvas content (huddle notes, standalone canvases).

Uses raw httpx (not SlackClient) so a canvas fetch failure can't trip the
store's backoff machinery. The Slack file_info response is validated through
`FilesInfoResponse` so we still get a typed `SlackFile` at the boundary.
"""

from __future__ import annotations

import html as html_mod
import logging
import re
from typing import Protocol

import httpx

from .models import FilesInfoResponse

log = logging.getLogger(__name__)


class _UserResolver(Protocol):
    def get_display_name(self, user_id: str) -> str: ...


def fetch_canvas_markdown(
    token: str,
    file_id: str,
    user_resolver: _UserResolver | None = None,
) -> str | None:
    """Fetch a canvas file and convert its HTML to markdown.

    Slack serves canvas content as HTML at the url_private endpoint.
    We parse it to produce readable markdown.
    """
    try:
        info_resp = httpx.get(
            "https://slack.com/api/files.info",
            headers={"Authorization": f"Bearer {token}"},
            params={"file": file_id},
            timeout=15.0,
        )
        info_resp.raise_for_status()
    except httpx.HTTPError as e:
        log.warning("files.info failed for %s: %s", file_id, e)
        return None

    parsed = FilesInfoResponse.model_validate(info_resp.json())
    if not parsed.ok or parsed.file is None:
        log.warning("files.info ok=False for %s: %s", file_id, parsed.error)
        return None

    url = parsed.file.url_private
    if not url:
        return None

    try:
        page_resp = httpx.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            follow_redirects=True,
            timeout=30.0,
        )
    except httpx.HTTPError as e:
        log.warning("Canvas download failed for %s: %s", file_id, e)
        return None
    if page_resp.status_code != 200:
        log.warning(
            "Canvas download failed for %s: HTTP %d", file_id, page_resp.status_code,
        )
        return None

    md = _html_to_markdown(page_resp.text)
    if md and user_resolver:
        md = _resolve_users(md, user_resolver)
    return md


def _html_to_markdown(page: str) -> str | None:
    """Extract canvas content from Slack's HTML page and convert to markdown."""
    idx = page.find("<h1")
    if idx < 0:
        return None

    end_idx = page.find("</body>", idx)
    if end_idx < 0:
        end_idx = len(page)
    content = page[idx:end_idx]

    # Remove script/style
    content = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", content, flags=re.DOTALL)

    # Images: Slack emojis are <img alt="name">:name:</img> — extract just :name:
    # Handle self-closing and non-self-closing img tags with their content
    content = re.sub(
        r'<img[^>]*alt="([^"]*)"[^>]*>([^<]*)</img>',
        lambda m: f":{m.group(1)}:",
        content,
        flags=re.DOTALL,
    )
    content = re.sub(r'<img[^>]*alt="([^"]*)"[^>]*/?\s*>', r":\1:", content, flags=re.DOTALL)
    content = re.sub(r"<img[^>]*/?\s*>", "", content, flags=re.DOTALL)

    # Control tags with emoji (Slack-specific)
    content = re.sub(r"<control[^>]*>(.*?)</control>", r"\1", content, flags=re.DOTALL)

    # Headers
    for i in range(6, 0, -1):
        content = re.sub(
            f"<h{i}[^>]*>(.*?)</h{i}>",
            lambda m, n=i: "\n" + "#" * n + " " + m.group(1).strip() + "\n",
            content,
            flags=re.DOTALL,
        )

    # Lists
    content = re.sub(r"<li[^>]*>(.*?)</li>", r"\n- \1", content, flags=re.DOTALL)

    # Links
    content = re.sub(
        r'<a[^>]*href="([^"]*)"[^>]*>(.*?)</a>',
        r"[\2](\1)",
        content,
        flags=re.DOTALL,
    )

    # Bold / italic
    content = re.sub(r"<(?:b|strong)[^>]*>(.*?)</(?:b|strong)>", r"**\1**", content, flags=re.DOTALL)
    content = re.sub(r"<(?:i|em)[^>]*>(.*?)</(?:i|em)>", r"*\1*", content, flags=re.DOTALL)

    # Checkboxes
    content = re.sub(r'<input[^>]*type="checkbox"[^>]*checked[^>]*/?\s*>', "[x] ", content)
    content = re.sub(r'<input[^>]*type="checkbox"[^>]*/?\s*>', "[ ] ", content)

    # HR
    content = re.sub(r"<hr[^>]*/?\s*>", "\n---\n", content)

    # Paragraphs and breaks
    content = re.sub(r"<br[^>]*/?\s*>", "\n", content)
    content = re.sub(r"<p[^>]*>", "\n", content)
    content = content.replace("</p>", "\n")

    # Strip remaining tags
    content = re.sub(r"<[^>]+>", "", content)

    # Decode HTML entities
    content = html_mod.unescape(content)

    # Clean duplicate emoji (img alt text + adjacent text node both produce the name)
    content = re.sub(r":([a-z_0-9-]+):\1:", r":\1:", content)
    # Also handle case where emoji colon text appears right after the alt-text version
    content = re.sub(r"([a-z_0-9-]+):(\1)", r"\1", content)

    # Clean whitespace
    content = re.sub(r" +", " ", content)
    content = re.sub(r"\n{3,}", "\n\n", content)

    return content.strip()


def _resolve_users(text: str, resolver: _UserResolver) -> str:
    """Replace @U12345ABC user ID patterns with display names."""

    def _replace(m: re.Match[str]) -> str:
        name = resolver.get_display_name(m.group(1))
        return f"@{name}"

    return re.sub(r"@(U[A-Z0-9]+)", _replace, text)
