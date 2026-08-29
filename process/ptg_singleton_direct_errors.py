# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Private singleton-direct contract error classification."""

from __future__ import annotations

from typing import Any, Sequence


class SingletonDirectValidationError(ValueError):
    """A signed direct selector is malformed or internally inconsistent."""


def singleton_direct_failure_payload(
    error_leaves: Sequence[BaseException],
) -> dict[str, Any] | None:
    """Classify contract failures without reflecting private selectors."""

    if not any(
        isinstance(error, SingletonDirectValidationError)
        for error in error_leaves
    ):
        return None
    return {
        "code": "ptg_singleton_direct_contract_failed",
        "message": "protected singleton direct input is invalid",
        "retryable": False,
    }
