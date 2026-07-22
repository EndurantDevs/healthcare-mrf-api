# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""One immutable outcome summary per visible Provider Directory source."""

from __future__ import annotations

from collections.abc import Mapping
import re
from typing import Any

from process.provider_directory_projection_contribution import (
    validated_semantic_outcome_proof,
)
from process.provider_directory_projection_types import (
    ProjectionSemanticOutcomeProof,
    ProviderDirectoryProjectionError,
    SEMANTIC_OUTCOME_FIELDS,
    required_hash,
    required_text,
    stable_hash,
)


SEMANTIC_SOURCE_SUMMARY_CONTRACT_ID = (
    "healthporta.provider-directory.physical-source-semantic-summary.v1"
)
MAX_SUPPLEMENTAL_COUNT_FIELDS = 32
_SUPPLEMENTAL_COUNT_FIELD = re.compile(r"^[a-z][a-z0-9_]{0,63}$")


def _nonnegative_count(raw_value: Any, field_name: str) -> int:
    if isinstance(raw_value, bool) or not isinstance(raw_value, int) or raw_value < 0:
        raise ProviderDirectoryProjectionError(
            f"provider_directory_projection_semantic_{field_name}_invalid"
        )
    return raw_value


def normalized_supplemental_counts(raw_counts: Any) -> dict[str, int]:
    """Validate a bounded, source-adapter-neutral map of additive counters."""

    if raw_counts is None:
        return {}
    if (
        not isinstance(raw_counts, Mapping)
        or len(raw_counts) > MAX_SUPPLEMENTAL_COUNT_FIELDS
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_supplemental_counts_invalid"
        )
    normalized_count_by_name: dict[str, int] = {}
    for raw_name, raw_value in raw_counts.items():
        if not isinstance(raw_name, str) or not _SUPPLEMENTAL_COUNT_FIELD.fullmatch(
            raw_name
        ) or raw_name in SEMANTIC_OUTCOME_FIELDS:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_supplemental_count_name_invalid"
            )
        value = _nonnegative_count(raw_value, "supplemental_count")
        if value > 9_223_372_036_854_775_807:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_supplemental_count_invalid"
            )
        normalized_count_by_name[raw_name] = value
    return dict(sorted(normalized_count_by_name.items()))


def semantic_source_summary(
    *,
    semantic_adapter_contract_id: str,
    transform_contract_id: str,
    dataset_hash: str,
    outcome_proof: ProjectionSemanticOutcomeProof,
    supplemental_counts: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    """Build counts only from the reducer-derived semantic outcome proof."""

    validated_proof = validated_semantic_outcome_proof(outcome_proof)
    summary_map = {
        "contract_id": SEMANTIC_SOURCE_SUMMARY_CONTRACT_ID,
        "contract_version": 1,
        "summary_grain": "visible_source",
        "semantic_adapter_contract_id": required_text(
            semantic_adapter_contract_id,
            "semantic_adapter_contract_id",
        ),
        "transform_contract_id": required_text(
            transform_contract_id,
            "transform_contract_id",
        ),
        "dataset_hash": required_hash(dataset_hash, "dataset_hash"),
        "semantic_outcome_proof_sha256": validated_proof.proof_sha256,
        "canonical_row_sha256": validated_proof.canonical_row_sha256,
        "profile_contribution_sha256": (
            validated_proof.profile_contribution_sha256
        ),
        "distinct_npi_sha256": validated_proof.distinct_npi_sha256,
        "resource_count": validated_proof.resource_count,
        "resource_counts": dict(sorted(validated_proof.resource_counts.items())),
        "outcome_counts": {
            field_name: validated_proof.outcome_counts[field_name]
            for field_name in SEMANTIC_OUTCOME_FIELDS
        },
        "supplemental_counts": normalized_supplemental_counts(supplemental_counts),
        "complete": True,
    }
    summary_map["summary_sha256"] = stable_hash(
        summary_map,
        domain="provider-directory-projection-semantic-source-summary-v1",
    )
    return summary_map


def validated_semantic_source_summary(
    raw_summary: Any,
    *,
    expected_semantic_adapter_contract_id: str,
    expected_transform_contract_id: str,
    expected_dataset_hash: str,
    expected_outcome_proof: ProjectionSemanticOutcomeProof,
    expected_supplemental_counts: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    """Validate a stored summary against independently derived expectations."""

    if not isinstance(raw_summary, Mapping):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_semantic_summary_invalid"
        )
    expected_fields = {
        "contract_id",
        "contract_version",
        "summary_grain",
        "semantic_adapter_contract_id",
        "transform_contract_id",
        "dataset_hash",
        "semantic_outcome_proof_sha256",
        "canonical_row_sha256",
        "profile_contribution_sha256",
        "distinct_npi_sha256",
        "resource_count",
        "resource_counts",
        "outcome_counts",
        "supplemental_counts",
        "complete",
        "summary_sha256",
    }
    if set(raw_summary) != expected_fields:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_semantic_summary_fields_mismatch"
        )
    canonical = semantic_source_summary(
        semantic_adapter_contract_id=expected_semantic_adapter_contract_id,
        transform_contract_id=expected_transform_contract_id,
        dataset_hash=expected_dataset_hash,
        outcome_proof=expected_outcome_proof,
        supplemental_counts=expected_supplemental_counts,
    )
    if dict(raw_summary) != canonical:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_semantic_summary_mismatch"
        )
    return canonical


__all__ = [
    "MAX_SUPPLEMENTAL_COUNT_FIELDS",
    "SEMANTIC_OUTCOME_FIELDS",
    "SEMANTIC_SOURCE_SUMMARY_CONTRACT_ID",
    "normalized_supplemental_counts",
    "semantic_source_summary",
    "validated_semantic_source_summary",
]
