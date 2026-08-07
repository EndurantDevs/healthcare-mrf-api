# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Terminal error classification for controlled PTG imports."""

from __future__ import annotations

from typing import Any

from process.ptg_frozen_control import frozen_rate_failure_payload
from process.ptg_singleton_direct_control import (
    singleton_direct_failure_payload,
)
from process.ptg_parts.ptg2_source_witness_contract import (
    WitnessPayloadLimitError,
)


_PROVIDER_GROUP_DEFINITION_ERROR_MARKERS = (
    "conflicting provider_group_id definition:",
    "duplicate provider_group_id definition:",
)


def _exception_leaves(error: BaseException) -> tuple[BaseException, ...]:
    if isinstance(error, BaseExceptionGroup):
        return tuple(
            leaf
            for nested_error in error.exceptions
            for leaf in _exception_leaves(nested_error)
        )
    return (error,)


def ptg_failure_error(error: BaseException) -> dict[str, Any]:
    """Classify one possibly grouped PTG failure for lifecycle storage."""

    error_leaves = _exception_leaves(error)
    witness_budget_error = next(
        (
            error_leaf
            for error_leaf in error_leaves
            if isinstance(error_leaf, WitnessPayloadLimitError)
        ),
        None,
    )
    if witness_budget_error is not None:
        return {
            "code": "ptg_source_witness_payload_budget_exceeded",
            "message": str(witness_budget_error),
            "retryable": False,
        }
    provider_group_error = next(
        (
            error_leaf
            for error_leaf in error_leaves
            if any(
                marker in str(error_leaf)
                for marker in _PROVIDER_GROUP_DEFINITION_ERROR_MARKERS
            )
        ),
        None,
    )
    if provider_group_error is not None:
        return {
            "code": "ptg_provider_group_definition_conflict",
            "message": str(provider_group_error),
        }
    frozen_failure = frozen_rate_failure_payload(error_leaves)
    if frozen_failure is not None:
        return frozen_failure
    direct_failure = singleton_direct_failure_payload(error_leaves)
    if direct_failure is not None:
        return direct_failure
    leaf_messages = tuple(
        dict.fromkeys(
            str(error_leaf).strip()
            for error_leaf in error_leaves
            if str(error_leaf).strip()
        )
    )
    return {
        "code": "ptg_import_failed",
        "message": (
            "; ".join(leaf_messages) if leaf_messages else str(error)
        ),
    }


__all__ = ["ptg_failure_error"]
