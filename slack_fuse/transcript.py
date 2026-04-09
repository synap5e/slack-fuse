"""Fetch and render huddle transcripts from the Slack API.

Slack returns transcripts as Slack Blocks JSON via the undocumented
`include_transcription=true` form param on `files.info`. We validate the
response into typed Pydantic models so the renderer never touches `dict[str, Any]`.
"""

from __future__ import annotations

import logging
from typing import Protocol

import httpx

from .models import FilesInfoResponse, HuddleTranscription

log = logging.getLogger(__name__)


class _UserResolver(Protocol):
    def get_display_name(self, user_id: str) -> str: ...


def fetch_transcript_markdown(
    token: str,
    transcript_file_id: str,
    users: _UserResolver | None = None,
) -> str | None:
    """Fetch a huddle transcript and render it as markdown."""
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
    except httpx.HTTPError as e:
        log.warning("Failed to fetch transcript %s: %s", transcript_file_id, e)
        return None

    try:
        parsed = FilesInfoResponse.model_validate(resp.json())
    except ValueError as e:
        log.warning("Transcript validation failed for %s: %s", transcript_file_id, e)
        return None

    if not parsed.ok or parsed.file is None:
        log.warning(
            "files.info ok=False for transcript %s: %s",
            transcript_file_id,
            parsed.error,
        )
        return None

    transcription = parsed.file.huddle_transcription
    if transcription is None:
        return None

    return _render_blocks(transcription, users)


def _render_blocks(
    transcription: HuddleTranscription,
    users: _UserResolver | None,
) -> str:
    """Convert the typed transcript blocks to markdown."""
    lines: list[str] = []

    for section in transcription.blocks.elements:
        if section.type != "rich_text_section":
            continue

        parts: list[str] = []
        for el in section.elements:
            if el.type == "user":
                name = users.get_display_name(el.user_id) if users else el.user_id
                parts.append(f"**@{name}**")
            elif el.type == "text":
                if el.style.bold:
                    parts.append(f"**{el.text.strip()}**")
                else:
                    parts.append(el.text)

        line = "".join(parts).strip()
        if line:
            lines.append(line)

    return "\n\n".join(lines)
