"""Slack Events API HMAC helper without transport freshness policy."""

from __future__ import annotations

import hashlib
import hmac

from slack_fuse_server.http.slack_webhook import verify_hmac_v0

_SECRET = "test-signing-secret"


def _signature(body: bytes, timestamp_text: str) -> str:
    base = b"v0:" + timestamp_text.encode("ascii") + b":" + body
    return "v0=" + hmac.new(_SECRET.encode(), base, hashlib.sha256).hexdigest()


def test_hmac_accepts_valid_signature_without_freshness_policy() -> None:
    body = b'{"type":"event_callback","event_id":"EvOld"}'
    timestamp_text = "1700000000"

    assert verify_hmac_v0(body, timestamp_text, _signature(body, timestamp_text), _SECRET) is True


def test_hmac_rejects_tampered_body() -> None:
    signed = b'{"type":"event_callback","event_id":"EvA"}'
    delivered = b'{"type":"event_callback","event_id":"EvB"}'
    timestamp_text = "1800000000"

    assert verify_hmac_v0(delivered, timestamp_text, _signature(signed, timestamp_text), _SECRET) is False


def test_hmac_rejects_malformed_or_tampered_signature() -> None:
    body = b"{}"
    timestamp_text = "1800000000"
    signature = _signature(body, timestamp_text)

    assert verify_hmac_v0(body, timestamp_text, "v1=bad", _SECRET) is False
    assert verify_hmac_v0(body, timestamp_text, signature[:-1] + "0", _SECRET) is False
