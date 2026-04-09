"""Slugify text for use as filenames."""

from __future__ import annotations

import re
import unicodedata

_MAX_LEN = 60


def slugify(text: str) -> str:
    """Convert text to a URL/filename-safe slug, truncated on word boundary."""
    if not text or not text.strip():
        return ""

    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = text.strip("-")

    if not text:
        return ""

    if len(text) <= _MAX_LEN:
        return text

    truncated = text[:_MAX_LEN]
    last_hyphen = truncated.rfind("-")
    if last_hyphen > 20:
        truncated = truncated[:last_hyphen]

    return truncated
