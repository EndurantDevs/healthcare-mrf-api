# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact, database-neutral proof for one current retained UHC dataset."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import json
import re
from typing import Any

from process.provider_directory_source_summary import (
    ProviderDirectorySourceSummaryError,
    SOURCE_SUMMARY_METADATA_KEY,
    validate_semantic_source_summary,
)
from process.uhc_canonical_proof import (
    UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY,
    UhcCanonicalProofError,
    validate_uhc_canonical_content_proof,
)
from process.uhc_retained_dataset import (
    UHC_RETAINED_PUBLICATION_METADATA_KEY,
    UHC_RETAINED_SUMMARY_INPUT_METADATA_KEY,
    UhcRetainedDatasetError,
    publication_identity,
    validate_uhc_summary_input,
)


PROVIDER_DIRECTORY_OUTCOME_RESOURCE_COUNTS_METADATA_KEY = "outcome_resource_counts_v1"

_PUBLISHED_STATUS = "published"
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_OUTCOME_FIELDS = {
    "complete",
    "version",
    "dataset_id",
    "endpoint_id",
    "acquisition_root_run_id",
    "dataset_hash",
    "source_ids",
    "selected_resources",
    "resource_count",
    "resource_counts",
}


class UhcFinalPublicationError(ValueError):
    """Reject an incomplete or internally divergent current-publication proof."""


@dataclass(frozen=True)
class UhcFinalPublicationExpectation:
    """External coordinates that one final publication must reproduce exactly."""

    source_id: str
    dataset_id: str
    endpoint_id: str
    acquisition_root_run_id: str
    selected_resources: tuple[str, ...]
    semantic_contract_id: str
    catalog_set_sha256: str | None = None


@dataclass(frozen=True)
class UhcFinalPublicationProof:
    """Validated final state shared by importer and catalog read models."""

    dataset_id: str
    endpoint_id: str
    acquisition_root_run_id: str
    dataset_hash: str
    resource_count: int
    resource_counts: dict[str, int]
    summary_input: dict[str, Any]
    source_summary: dict[str, Any]
    canonical_proof: dict[str, Any]
    outcome: dict[str, Any]
    publication_identity: dict[str, Any]


def _publication_error() -> UhcFinalPublicationError:
    return UhcFinalPublicationError("retained UHC current publication proof is invalid")


def _metadata_map(raw_metadata: Any) -> dict[str, Any]:
    if isinstance(raw_metadata, str):
        try:
            raw_metadata = json.loads(raw_metadata)
        except (ValueError, RecursionError) as error:
            raise _publication_error() from error
    if not isinstance(raw_metadata, Mapping):
        raise _publication_error()
    try:
        return dict(raw_metadata)
    except (KeyError, TypeError, ValueError) as error:
        raise _publication_error() from error


def _is_publication_expectation_valid(
    expectation: UhcFinalPublicationExpectation,
) -> bool:
    text_fields = (
        expectation.source_id,
        expectation.dataset_id,
        expectation.endpoint_id,
        expectation.acquisition_root_run_id,
        expectation.semantic_contract_id,
    )
    return bool(
        all(
            isinstance(identity_text, str)
            and identity_text
            and identity_text == identity_text.strip()
            and identity_text.isprintable()
            for identity_text in text_fields
        )
        and expectation.selected_resources
        and expectation.selected_resources
        == tuple(sorted(set(expectation.selected_resources)))
        and all(
            isinstance(resource_type, str)
            and resource_type
            and resource_type == resource_type.strip()
            for resource_type in expectation.selected_resources
        )
        and (
            expectation.catalog_set_sha256 is None
            or _SHA256_PATTERN.fullmatch(expectation.catalog_set_sha256) is not None
        )
    )


def _validated_state(
    state: Mapping[str, Any],
    expectation: UhcFinalPublicationExpectation,
) -> tuple[str, int]:
    dataset_hash = state.get("dataset_hash")
    resource_count = state.get("resource_count")
    state_source_id = state.get("source_id")
    if (
        not _is_publication_expectation_valid(expectation)
        or state.get("status") != _PUBLISHED_STATUS
        or state.get("is_current") is not True
        or state.get("dataset_id") != expectation.dataset_id
        or state.get("endpoint_id") != expectation.endpoint_id
        or state.get("acquisition_root_run_id") != expectation.acquisition_root_run_id
        or (state_source_id is not None and state_source_id != expectation.source_id)
        or not isinstance(dataset_hash, str)
        or _SHA256_PATTERN.fullmatch(dataset_hash) is None
        or isinstance(resource_count, bool)
        or not isinstance(resource_count, int)
        or resource_count <= 0
    ):
        raise _publication_error()
    return dataset_hash, resource_count


def _validated_metadata_proofs(
    metadata: Mapping[str, Any],
    expectation: UhcFinalPublicationExpectation,
    dataset_hash: str,
    resource_count: int,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    try:
        summary_input = validate_uhc_summary_input(
            metadata.get(UHC_RETAINED_SUMMARY_INPUT_METADATA_KEY)
        )
        source_summary = validate_semantic_source_summary(
            metadata.get(SOURCE_SUMMARY_METADATA_KEY),
            expected_by_field={
                "dataset_id": expectation.dataset_id,
                "endpoint_id": expectation.endpoint_id,
                "acquisition_root_run_id": (expectation.acquisition_root_run_id),
                "dataset_hash": dataset_hash,
                "total_resources": resource_count,
                "source_ids": [expectation.source_id],
                "selected_resources": list(expectation.selected_resources),
                "semantic_contract_id": expectation.semantic_contract_id,
            },
            expected_semantic_contract_id=expectation.semantic_contract_id,
        )
        canonical_proof = validate_uhc_canonical_content_proof(
            metadata.get(UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY),
            dataset_id=expectation.dataset_id,
            endpoint_id=expectation.endpoint_id,
            acquisition_root_run_id=expectation.acquisition_root_run_id,
        )
    except (
        ProviderDirectorySourceSummaryError,
        UhcCanonicalProofError,
        UhcRetainedDatasetError,
    ) as error:
        raise _publication_error() from error
    return summary_input, source_summary, canonical_proof


def _is_summary_proof_set_consistent(
    summary_input: Mapping[str, Any],
    source_summary: Mapping[str, Any],
    canonical_proof: Mapping[str, Any],
    expectation: UhcFinalPublicationExpectation,
    dataset_hash: str,
    resource_count: int,
) -> bool:
    summary_count_by_field = summary_input["count_by_field"]
    summary_count_by_category = summary_input["count_by_category"]
    return bool(
        summary_input["source_id"] == expectation.source_id
        and summary_input["semantic_contract_id"] == expectation.semantic_contract_id
        and (
            expectation.catalog_set_sha256 is None
            or summary_input["catalog_set_sha256"] == expectation.catalog_set_sha256
        )
        and all(
            source_summary.get(field_name) == count
            for field_name, count in summary_count_by_field.items()
        )
        and all(
            source_summary.get(field_name) == count_map
            for field_name, count_map in summary_count_by_category.items()
        )
        and all(
            source_summary.get(field_name) == summary_input[field_name]
            for field_name in (
                "semantic_contract_id",
                "input_set_sha256",
                "layout_set_sha256",
                "encoder_digest",
                "quarantine_proof_sha256",
            )
        )
        and canonical_proof["source_id"] == expectation.source_id
        and canonical_proof["dataset_hash"] == dataset_hash
        and canonical_proof["resource_count"] == resource_count
        and canonical_proof["resource_counts"] == source_summary["resource_counts"]
        and canonical_proof["resource_hashes"] == source_summary["resource_hashes"]
        and canonical_proof["catalog_set_sha256"] == summary_input["catalog_set_sha256"]
        and canonical_proof["semantic_contract_id"]
        == summary_input["semantic_contract_id"]
        and canonical_proof["semantic_contract_version"]
        == summary_input["semantic_contract_version"]
        and canonical_proof["semantic_set_sha256"]
        == summary_input["semantic_set_sha256"]
        and canonical_proof["semantic_build_ids"] == summary_input["semantic_build_ids"]
        and canonical_proof["canonical_contract_id"]
        == summary_input["canonical_contract_id"]
    )


def _expected_outcome_by_field(
    expectation: UhcFinalPublicationExpectation,
    dataset_hash: str,
    resource_count: int,
    resource_counts: Mapping[str, int],
) -> dict[str, Any]:
    return {
        "complete": True,
        "version": 1,
        "dataset_id": expectation.dataset_id,
        "endpoint_id": expectation.endpoint_id,
        "acquisition_root_run_id": expectation.acquisition_root_run_id,
        "dataset_hash": dataset_hash,
        "source_ids": [expectation.source_id],
        "selected_resources": list(expectation.selected_resources),
        "resource_count": resource_count,
        "resource_counts": dict(resource_counts),
    }


def _is_embedded_metadata_consistent(
    metadata_by_field: Mapping[str, Any],
    expectation: UhcFinalPublicationExpectation,
    outcome_by_field: Any,
    publication_identity_by_field: Mapping[str, Any],
    expected_outcome_by_field: Mapping[str, Any],
) -> bool:
    return bool(
        metadata_by_field.get("source_ids") == [expectation.source_id]
        and metadata_by_field.get("selected_resources")
        == list(expectation.selected_resources)
        and isinstance(outcome_by_field, dict)
        and set(outcome_by_field) == _OUTCOME_FIELDS
        and outcome_by_field == expected_outcome_by_field
        and metadata_by_field.get(UHC_RETAINED_PUBLICATION_METADATA_KEY)
        == publication_identity_by_field
    )


def _validated_embedded_proofs(
    state_by_field: Mapping[str, Any],
    expectation: UhcFinalPublicationExpectation,
    dataset_hash: str,
    resource_count: int,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
]:
    metadata_by_field = _metadata_map(state_by_field.get("publication_metadata_json"))
    summary_input_by_field, source_summary_by_field, canonical_proof_by_field = (
        _validated_metadata_proofs(
            metadata_by_field,
            expectation,
            dataset_hash,
            resource_count,
        )
    )
    publication_identity_by_field = publication_identity(
        summary_input_by_field,
        dataset_id=expectation.dataset_id,
        acquisition_root_run_id=expectation.acquisition_root_run_id,
    )
    expected_outcome_by_field = _expected_outcome_by_field(
        expectation,
        dataset_hash,
        resource_count,
        source_summary_by_field["resource_counts"],
    )
    outcome_by_field = metadata_by_field.get(
        PROVIDER_DIRECTORY_OUTCOME_RESOURCE_COUNTS_METADATA_KEY
    )
    if not _is_summary_proof_set_consistent(
        summary_input_by_field,
        source_summary_by_field,
        canonical_proof_by_field,
        expectation,
        dataset_hash,
        resource_count,
    ) or not _is_embedded_metadata_consistent(
        metadata_by_field,
        expectation,
        outcome_by_field,
        publication_identity_by_field,
        expected_outcome_by_field,
    ):
        raise _publication_error()
    return (
        summary_input_by_field,
        source_summary_by_field,
        canonical_proof_by_field,
        outcome_by_field,
        publication_identity_by_field,
    )


def validate_uhc_final_publication(
    raw_state: Mapping[str, Any],
    expectation: UhcFinalPublicationExpectation,
) -> UhcFinalPublicationProof:
    """Validate one exact current dataset without database or importer coupling."""

    if not isinstance(raw_state, Mapping):
        raise _publication_error()
    state_by_field = dict(raw_state)
    dataset_hash, resource_count = _validated_state(
        state_by_field,
        expectation,
    )
    (
        summary_input_by_field,
        source_summary_by_field,
        canonical_proof_by_field,
        outcome_by_field,
        publication_identity_by_field,
    ) = _validated_embedded_proofs(
        state_by_field,
        expectation,
        dataset_hash,
        resource_count,
    )
    return UhcFinalPublicationProof(
        dataset_id=expectation.dataset_id,
        endpoint_id=expectation.endpoint_id,
        acquisition_root_run_id=expectation.acquisition_root_run_id,
        dataset_hash=dataset_hash,
        resource_count=resource_count,
        resource_counts=dict(source_summary_by_field["resource_counts"]),
        summary_input=summary_input_by_field,
        source_summary=source_summary_by_field,
        canonical_proof=canonical_proof_by_field,
        outcome=dict(outcome_by_field),
        publication_identity=publication_identity_by_field,
    )


__all__ = [
    "PROVIDER_DIRECTORY_OUTCOME_RESOURCE_COUNTS_METADATA_KEY",
    "UhcFinalPublicationError",
    "UhcFinalPublicationExpectation",
    "UhcFinalPublicationProof",
    "validate_uhc_final_publication",
]
