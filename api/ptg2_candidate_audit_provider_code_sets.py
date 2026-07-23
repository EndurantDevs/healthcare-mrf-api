# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Load bounded provider/code dimensions without Cartesian request products."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Iterable, Mapping, Sequence

from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
    retain_unique_integer_keys,
)
from api.ptg2_candidate_audit_maps import (
    INTEGER_MAP_BUCKET_BYTES,
    INTEGER_MAP_BYTES,
    INTEGER_SET_MEMBERSHIP_BYTES,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


_DECODED_PROVIDER_CODE_BUCKET_BYTES = 160
_DECODED_PROVIDER_CODE_MEMBERSHIP_BYTES = 16
ProviderCodeIntersectionLookup = Callable[..., Awaitable[dict[int, tuple[int, ...]]]]


@dataclass(frozen=True)
class ProviderCodeBounds:
    """Bound one decoded provider/code lookup."""

    maximum_provider_keys: int
    maximum_code_keys: int
    schema_name: str


def decoded_provider_code_bytes(
    code_keys_by_provider_set: Mapping[int, Sequence[int]],
) -> int:
    """Account one decoded provider-to-tuple membership map."""

    return INTEGER_MAP_BYTES + sum(
        _DECODED_PROVIDER_CODE_BUCKET_BYTES
        + len(code_keys) * _DECODED_PROVIDER_CODE_MEMBERSHIP_BYTES
        for code_keys in code_keys_by_provider_set.values()
    )


def frozen_provider_code_bytes(
    code_keys_by_provider_set: Mapping[int, Sequence[int]],
) -> int:
    """Account one retained provider-to-frozenset membership map."""

    return INTEGER_MAP_BYTES + sum(
        INTEGER_MAP_BUCKET_BYTES
        + len(code_keys) * INTEGER_SET_MEMBERSHIP_BYTES
        for code_keys in code_keys_by_provider_set.values()
    )


def has_complete_provider_code_keys(
    code_keys_by_provider_set: Mapping[int, Sequence[int]],
    requested_provider_set_keys: Sequence[int],
) -> bool:
    """Return whether one lookup answered every requested provider key."""

    return len(code_keys_by_provider_set) == len(
        requested_provider_set_keys
    ) and all(
        provider_set_key in code_keys_by_provider_set
        for provider_set_key in requested_provider_set_keys
    )


def ensure_complete_provider_code_map(
    code_keys_by_provider_set: Mapping[int, Sequence[int]],
    requested_provider_set_keys: Sequence[int],
    retention_budget: CandidateAuditDecodedRetentionBudget | None,
) -> None:
    """Reject an incomplete decoded map and release its owned allocation."""

    if has_complete_provider_code_keys(
        code_keys_by_provider_set,
        requested_provider_set_keys,
    ):
        return
    if retention_budget is not None:
        retention_budget.release(
            decoded_provider_code_bytes(code_keys_by_provider_set)
        )
    raise PTG2ManifestArtifactError(
        "PTG2 candidate provider-code artifact is missing a referenced provider set"
    )


def freeze_nonempty_provider_code_sets(
    code_keys_by_provider_set: Mapping[int, Sequence[int]],
    retention_budget: CandidateAuditDecodedRetentionBudget | None,
) -> dict[int, frozenset[int]]:
    """Replace a complete lookup map with only its useful intersections."""

    retained_source_bytes = decoded_provider_code_bytes(
        code_keys_by_provider_set
    )
    if retention_budget is None:
        return {
            provider_set_key: frozenset(code_keys)
            for provider_set_key, code_keys in code_keys_by_provider_set.items()
            if code_keys
        }
    retained_result_bytes = 0
    frozen_code_keys_by_provider_set: dict[int, frozenset[int]] = {}
    try:
        retention_budget.claim(
            INTEGER_MAP_BYTES,
            category="the frozen provider/code membership map",
        )
        retained_result_bytes = INTEGER_MAP_BYTES
        for provider_set_key, code_keys in code_keys_by_provider_set.items():
            if not code_keys:
                continue
            retained_bucket_bytes = (
                INTEGER_MAP_BUCKET_BYTES
                + len(code_keys) * INTEGER_SET_MEMBERSHIP_BYTES
            )
            retention_budget.claim(
                retained_bucket_bytes,
                category="a frozen provider/code membership bucket",
            )
            retained_result_bytes += retained_bucket_bytes
            frozen_code_keys_by_provider_set[provider_set_key] = frozenset(
                code_keys
            )
    except BaseException:
        if retained_result_bytes:
            retention_budget.release(retained_result_bytes)
        retention_budget.release(retained_source_bytes)
        raise
    retention_budget.release(retained_source_bytes)
    return frozen_code_keys_by_provider_set


def _retained_provider_code_dimensions(
    provider_set_keys: Iterable[int],
    requested_code_keys: Iterable[int],
    retention_budget: CandidateAuditDecodedRetentionBudget | None,
    *,
    maximum_provider_keys: int,
    maximum_code_keys: int,
) -> tuple[tuple[int, ...], tuple[int, ...], int]:
    """Retain bounded provider and code dimensions for one lookup."""

    requested_keys, retained_provider_key_bytes = retain_unique_integer_keys(
        provider_set_keys,
        retention_budget,
        category="candidate provider-code provider keys",
        maximum_count=maximum_provider_keys,
        limit_error_message=(
            "PTG2 candidate provider-code scope exceeds its bounded limit"
        ),
    )
    if not requested_keys:
        return requested_keys, (), retained_provider_key_bytes
    try:
        normalized_code_keys, retained_code_key_bytes = (
            retain_unique_integer_keys(
                requested_code_keys,
                retention_budget,
                category="candidate provider-code requested codes",
                maximum_count=maximum_code_keys,
                limit_error_message=(
                    "PTG2 candidate requested code scope exceeds its bounded limit"
                ),
            )
        )
    except BaseException:
        if retention_budget is not None:
            retention_budget.release(retained_provider_key_bytes)
        raise
    return (
        requested_keys,
        normalized_code_keys,
        retained_provider_key_bytes + retained_code_key_bytes,
    )


async def load_bounded_provider_code_sets(
    lookup_intersections: ProviderCodeIntersectionLookup,
    session: Any,
    shared_snapshot_key: int,
    provider_set_keys: Iterable[int],
    requested_code_keys: Iterable[int],
    *,
    bounds: ProviderCodeBounds,
    retention_budget: CandidateAuditDecodedRetentionBudget | None = None,
) -> dict[int, frozenset[int]]:
    """Retain only requested memberships from each sealed provider code set."""

    requested_keys, normalized_code_keys, retained_dimension_bytes = (
        _retained_provider_code_dimensions(
            provider_set_keys,
            requested_code_keys,
            retention_budget,
            maximum_provider_keys=bounds.maximum_provider_keys,
            maximum_code_keys=bounds.maximum_code_keys,
        )
    )
    try:
        if not requested_keys:
            return {}
        if not normalized_code_keys:
            raise PTG2ManifestArtifactError(
                "PTG2 candidate provider-code scope has no requested codes"
            )
        code_keys_by_provider_set = await lookup_intersections(
            session,
            shared_snapshot_key,
            requested_keys,
            normalized_code_keys,
            max_retained_memberships=bounds.maximum_provider_keys,
            schema_name=bounds.schema_name,
            decoded_retention_budget=retention_budget,
            inputs_are_normalized=True,
        )
        ensure_complete_provider_code_map(
            code_keys_by_provider_set,
            requested_keys,
            retention_budget,
        )
        return freeze_nonempty_provider_code_sets(
            code_keys_by_provider_set,
            retention_budget,
        )
    finally:
        if retention_budget is not None:
            retention_budget.release(retained_dimension_bytes)


__all__ = [
    "ProviderCodeBounds",
    "decoded_provider_code_bytes",
    "ensure_complete_provider_code_map",
    "freeze_nonempty_provider_code_sets",
    "frozen_provider_code_bytes",
    "has_complete_provider_code_keys",
    "load_bounded_provider_code_sets",
]
