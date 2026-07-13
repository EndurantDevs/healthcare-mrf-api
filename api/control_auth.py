# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared authentication for operator-only API capabilities."""

from __future__ import annotations

import hmac
import os

from sanic.exceptions import Forbidden


def require_control_auth(request) -> None:
    """Require the configured control token without exposing token details."""

    expected = str(os.getenv("HLTHPRT_CONTROL_API_TOKEN") or "").strip()
    if not expected:
        raise Forbidden("control API token is required")
    headers = getattr(request, "headers", {}) or {}
    auth_header = str(headers.get("Authorization", ""))
    bearer = (
        auth_header.removeprefix("Bearer ").strip()
        if auth_header.startswith("Bearer ")
        else ""
    )
    explicit = str(headers.get("X-HealthPorta-Control-Token", "")).strip()
    if not (
        hmac.compare_digest(bearer, expected)
        or hmac.compare_digest(explicit, expected)
    ):
        raise Forbidden("control API token is invalid")
