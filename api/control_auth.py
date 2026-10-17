# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared authentication for operator-only and Registry-only API capabilities."""

from __future__ import annotations

import hmac
import os

from sanic.exceptions import Forbidden


def _is_token_match(request, expected: str) -> bool:
    headers = getattr(request, "headers", {}) or {}
    auth_header = str(headers.get("Authorization", ""))
    bearer = auth_header.removeprefix("Bearer ").strip() if auth_header.startswith("Bearer ") else ""
    explicit = str(headers.get("X-HealthPorta-Control-Token", "")).strip()
    try:
        return hmac.compare_digest(bearer, expected) or hmac.compare_digest(explicit, expected)
    except TypeError:
        return False


def require_control_auth(request) -> None:
    """Require the configured control token without exposing token details."""
    expected = str(os.getenv("HLTHPRT_CONTROL_API_TOKEN") or "").strip()
    if not expected:
        raise Forbidden("control API token is required")
    if not _is_token_match(request, expected):
        raise Forbidden("control API token is invalid")


def require_registry_auth(request) -> None:
    """Admit a Registry-only service token or the existing operator credential."""
    expected = str(os.getenv("HLTHPRT_REGISTRY_CONTROL_API_TOKEN") or "").strip()
    if expected and expected.isascii() and _is_token_match(request, expected):
        return
    require_control_auth(request)
