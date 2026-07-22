# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Build non-authoritative source-summary candidates from semantic aggregates.

The summary is deterministically bound to its candidate aggregate, but neither
object attests native decoding from retained bytes or authorizes publication.
"""

from __future__ import annotations

from collections.abc import Mapping
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


# Historical wire identifier retained for compatibility.  It describes a
# candidate summary envelope rather than a native-attested source summary.
SEMANTIC_SOURCE_SUMMARY_CONTRACT_ID = (
    "healthporta.provider-directory.physical-source-semantic-summary.v1"
)
def semantic_source_summary(
    *,
    semantic_adapter_contract_id: str,
    transform_contract_id: str,
    dataset_hash: str,
    outcome_proof: ProjectionSemanticOutcomeProof,
) -> dict[str, Any]:
    """Build counts only from one structurally validated candidate outcome."""

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
) -> dict[str, Any]:
    """Validate candidate-summary shape and exact expected bindings only."""

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
    )
    if dict(raw_summary) != canonical:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_semantic_summary_mismatch"
        )
    return canonical


__all__ = [
    "SEMANTIC_OUTCOME_FIELDS",
    "SEMANTIC_SOURCE_SUMMARY_CONTRACT_ID",
    "semantic_source_summary",
    "validated_semantic_source_summary",
]
