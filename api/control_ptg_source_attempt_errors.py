# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stable control-plane response for terminal PTG source attempts."""

from __future__ import annotations

from sanic import response

from process.ptg_parts.ptg_source_attempt_guard import (
    PTGSourceAttemptFencedError,
)


def _request_id(request) -> str:
    return str(
        request.headers.get("X-Request-ID", "")
        or request.headers.get("X-Request-Id", "")
    ).strip()


def register_source_attempt_error_handler(blueprint) -> None:
    """Register the cross-service 409 fence response."""

    @blueprint.exception(PTGSourceAttemptFencedError)
    async def source_attempt_fenced(request, exc):
        """Render one stable terminal-fence conflict."""

        return response.json(
            {
                "error": {
                    "code": exc.error_code,
                    "message": str(exc),
                    "detail": {},
                    "request_id": _request_id(request),
                }
            },
            status=409,
        )


__all__ = ["register_source_attempt_error_handler"]
