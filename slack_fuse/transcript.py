"""Fetch and render huddle transcripts from the Slack API."""

from __future__ import annotations

import logging
from typing import Any, Protocol

import httpx

log = logging.getLogger(__name__)


class _UserResolver(Protocol):
    def get_display_name(self, user_id: str) -> str: ...


def fetch_transcript_markdown(
    token: str,
    transcript_file_id: str,
    users: _UserResolver | None = None,
) -> str | None:
    """Fetch a huddle transcript and render it as markdown.

    Uses the undocumented `include_transcription=true` parameter on files.info
    which returns the transcript as Slack Blocks JSON in `huddle_transcription`.
    """
    try:
        resp = httpx.post(
            "https://slack.com/api/files.info",
            headers={"Authorization": f"Bearer {token}"},
            data={
                "file": transcript_file_id,
                "include_transcription": "true",
            },
            timeout=30.0,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        log.warning("Failed to fetch transcript %s", transcript_file_id, exc_info=True)
        return None

    if not data.get("ok"):
        log.warning("files.info failed for transcript %s: %s", transcript_file_id, data.get("error"))
        return None

    transcription = data.get("file", {}).get("huddle_transcription")
    if not transcription:
        return None

    return _render_blocks(transcription, users)


def _render_blocks(transcription: dict[str, Any], users: _UserResolver | None) -> str:
    """Convert Slack Blocks transcript JSON to markdown."""
    blocks = transcription.get("blocks", {})
    elements = blocks.get("elements", [])

    lines: list[str] = []

    for section in elements:
        if section.get("type") != "rich_text_section":
            continue

        parts: list[str] = []
        for el in section.get("elements", []):
            el_type = el.get("type", "")

            if el_type == "user":
                user_id = el.get("user_id", "")
                name = users.get_display_name(user_id) if users else user_id
                parts.append(f"**@{name}**")

            elif el_type == "text":
                text = el.get("text", "")
                style = el.get("style", {})
                if style.get("bold"):
                    parts.append(f"**{text.strip()}**")
                else:
                    parts.append(text)

        line = "".join(parts).strip()
        if line:
            lines.append(line)

    return "\n\n".join(lines)
