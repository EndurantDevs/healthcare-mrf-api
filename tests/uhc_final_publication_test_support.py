# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import json
from typing import Any

from process.provider_directory_source_summary import (
    ProviderDirectorySourceSummaryBinding,
    SOURCE_SUMMARY_METADATA_KEY,
    SOURCE_SUMMARY_UHC_OUTCOME_COUNT_FIELDS,
    SOURCE_SUMMARY_UHC_SELECTED_RESOURCES,
    SOURCE_SUMMARY_UHC_SEMANTIC_CONTRACT_ID,
    build_source_summary,
)
from process.uhc_canonical_proof import (
    UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY,
    ProviderDirectoryContentProofBuilder,
    UhcCanonicalMaterializationIdentity,
    UhcCanonicalNpiProof,
    bind_uhc_canonical_content_proof,
    canonical_materialization_proof,
)
from process.uhc_final_publication_contract import (
    PROVIDER_DIRECTORY_OUTCOME_RESOURCE_COUNTS_METADATA_KEY,
    UhcFinalPublicationExpectation,
)
from process.uhc_retained_dataset import (
    UHC_RETAINED_CANONICAL_CONTRACT_ID,
    UHC_RETAINED_PUBLICATION_METADATA_KEY,
    UHC_RETAINED_SOURCE_ID,
    UHC_RETAINED_SUMMARY_INPUT_CONTRACT_ID,
    UHC_RETAINED_SUMMARY_INPUT_METADATA_KEY,
    _summary_input_hash,
    publication_identity,
)
from process.uhc_semantic_build_store import (
    UHC_SEMANTIC_CONTRACT_ID,
    UHC_SEMANTIC_CONTRACT_VERSION,
)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _canonical_row(resource_type: str) -> tuple[str, ...]:
    resource_id = f"synthetic-{resource_type.lower()}"
    payload = {"resource_id": resource_id}
    payload_json = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    return (
        resource_type,
        resource_id,
        hashlib.sha256(payload_json.encode()).hexdigest(),
        payload_json,
        "synthetic-source-rank",
    )


def _count_by_field() -> dict[str, int]:
    count_by_field = {
        field_name: 0 for field_name in SOURCE_SUMMARY_UHC_OUTCOME_COUNT_FIELDS
    }
    count_by_field.update(
        raw_provider_records=2,
        raw_plan_records=1,
        raw_individual_records=1,
        raw_facility_records=1,
        raw_address_rows=1,
        raw_provider_plan_rows=2,
        named_facility_records=1,
        facility_type_values=1,
        distinct_npis=2,
        accepting_null_records=2,
        plan_year_rows=3,
        provider_file_count=78,
        plan_file_count=24,
        membership_plan_key_count=1,
        detail_plan_key_count=1,
        matched_plan_key_count=1,
    )
    return count_by_field


def _build_summary_input(catalog_set_sha256: str) -> dict[str, Any]:
    summary_input_by_field = {
        "contract_id": UHC_RETAINED_SUMMARY_INPUT_CONTRACT_ID,
        "complete": True,
        "source_id": UHC_RETAINED_SOURCE_ID,
        "catalog_set_sha256": catalog_set_sha256,
        "semantic_contract_id": UHC_SEMANTIC_CONTRACT_ID,
        "semantic_contract_version": UHC_SEMANTIC_CONTRACT_VERSION,
        "canonical_contract_id": UHC_RETAINED_CANONICAL_CONTRACT_ID,
        "semantic_build_ids": ["b" * 64, "c" * 64],
        "semantic_set_sha256": "d" * 64,
        "input_set_sha256": "e" * 64,
        "layout_set_sha256": "f" * 64,
        "encoder_digest": "1" * 64,
        "quarantine_proof_sha256": "2" * 64,
        "count_by_field": _count_by_field(),
        "count_by_category": {
            "conflict_counts": {},
            "rejected_counts": {},
            "intentional_drop_counts": {},
            "unknown_field_counts": {},
        },
    }
    summary_input_by_field["input_sha256"] = _summary_input_hash(summary_input_by_field)
    return summary_input_by_field


def _materialization_identity(
    summary_input_by_field: dict[str, Any],
) -> UhcCanonicalMaterializationIdentity:
    return UhcCanonicalMaterializationIdentity(
        catalog_set_sha256=summary_input_by_field["catalog_set_sha256"],
        semantic_set_sha256=summary_input_by_field["semantic_set_sha256"],
        semantic_build_ids=tuple(summary_input_by_field["semantic_build_ids"]),
        source_id=UHC_RETAINED_SOURCE_ID,
        semantic_contract_id=summary_input_by_field["semantic_contract_id"],
        semantic_contract_version=summary_input_by_field["semantic_contract_version"],
        canonical_contract_id=summary_input_by_field["canonical_contract_id"],
    )


def _npi_proof(source_file_id: str, artifact_sha256: str) -> UhcCanonicalNpiProof:
    return UhcCanonicalNpiProof(
        evidence_count=1,
        distinct_npis=1,
        proof_sha256="6" * 64,
        shards=(
            {
                "source_id": UHC_RETAINED_SOURCE_ID,
                "source_file_id": source_file_id,
                "range_ordinal": 0,
                "row_count": 1,
                "input_sha256": "7" * 64,
                "artifact_sha256": artifact_sha256,
                "layout_sha256": "8" * 64,
            },
        ),
    )


def _canonical_proof(
    summary_input_by_field: dict[str, Any],
    *,
    dataset_id: str,
    endpoint_id: str,
    acquisition_root_run_id: str,
) -> dict[str, Any]:
    source_file_id = "3" * 64
    artifact_sha256 = "4" * 64
    builder = ProviderDirectoryContentProofBuilder(
        source_id=UHC_RETAINED_SOURCE_ID,
        shard_rows=2,
    )
    builder.observe_rows(
        [
            _canonical_row(resource_type)
            for resource_type in SOURCE_SUMMARY_UHC_SELECTED_RESOURCES
        ],
        input_lineage=[
            {
                "source_file_id": source_file_id,
                "range_ordinal": 0,
                "input_sha256": "5" * 64,
                "artifact_sha256": artifact_sha256,
            }
        ],
    )
    materialization = canonical_materialization_proof(
        builder.complete(),
        _materialization_identity(summary_input_by_field),
        _npi_proof(source_file_id, artifact_sha256),
    )
    return bind_uhc_canonical_content_proof(
        materialization,
        dataset_id=dataset_id,
        endpoint_id=endpoint_id,
        acquisition_root_run_id=acquisition_root_run_id,
    )


def _build_source_summary_by_field(
    summary_input_by_field: dict[str, Any],
    canonical_proof_by_field: dict[str, Any],
    *,
    dataset_id: str,
    endpoint_id: str,
    acquisition_root_run_id: str,
) -> dict[str, Any]:
    return build_source_summary(
        binding=ProviderDirectorySourceSummaryBinding(
            dataset_id=dataset_id,
            endpoint_id=endpoint_id,
            acquisition_root_run_id=acquisition_root_run_id,
            dataset_hash=canonical_proof_by_field["dataset_hash"],
        ),
        source_ids=(UHC_RETAINED_SOURCE_ID,),
        selected_resources=SOURCE_SUMMARY_UHC_SELECTED_RESOURCES,
        count_by_resource=canonical_proof_by_field["resource_counts"],
        hash_by_resource=canonical_proof_by_field["resource_hashes"],
        count_by_field=summary_input_by_field["count_by_field"],
        count_by_category=summary_input_by_field["count_by_category"],
        identity_by_field={
            "semantic_contract_id": summary_input_by_field["semantic_contract_id"],
            "input_set_sha256": summary_input_by_field["input_set_sha256"],
            "layout_set_sha256": summary_input_by_field["layout_set_sha256"],
            "encoder_digest": summary_input_by_field["encoder_digest"],
            "quarantine_proof_sha256": summary_input_by_field[
                "quarantine_proof_sha256"
            ],
        },
    )


def _build_outcome_by_field(
    canonical_proof_by_field: dict[str, Any],
    *,
    dataset_id: str,
    endpoint_id: str,
    acquisition_root_run_id: str,
) -> dict[str, Any]:
    return {
        "complete": True,
        "version": 1,
        "dataset_id": dataset_id,
        "endpoint_id": endpoint_id,
        "acquisition_root_run_id": acquisition_root_run_id,
        "dataset_hash": canonical_proof_by_field["dataset_hash"],
        "source_ids": [UHC_RETAINED_SOURCE_ID],
        "selected_resources": list(SOURCE_SUMMARY_UHC_SELECTED_RESOURCES),
        "resource_count": canonical_proof_by_field["resource_count"],
        "resource_counts": canonical_proof_by_field["resource_counts"],
    }


def _build_metadata_by_field(
    summary_input_by_field: dict[str, Any],
    canonical_proof_by_field: dict[str, Any],
    source_summary_by_field: dict[str, Any],
    outcome_by_field: dict[str, Any],
    *,
    dataset_id: str,
    acquisition_root_run_id: str,
) -> dict[str, Any]:
    return {
        "source_ids": [UHC_RETAINED_SOURCE_ID],
        "selected_resources": list(SOURCE_SUMMARY_UHC_SELECTED_RESOURCES),
        UHC_RETAINED_SUMMARY_INPUT_METADATA_KEY: summary_input_by_field,
        UHC_RETAINED_PUBLICATION_METADATA_KEY: publication_identity(
            summary_input_by_field,
            dataset_id=dataset_id,
            acquisition_root_run_id=acquisition_root_run_id,
        ),
        UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY: canonical_proof_by_field,
        SOURCE_SUMMARY_METADATA_KEY: source_summary_by_field,
        PROVIDER_DIRECTORY_OUTCOME_RESOURCE_COUNTS_METADATA_KEY: (outcome_by_field),
    }


def final_publication_fixture(
    *,
    catalog_set_sha256: str = "a" * 64,
    dataset_id: str = "synthetic-dataset",
    endpoint_id: str = "9" * 64,
    acquisition_root_run_id: str = "synthetic-root",
) -> tuple[dict[str, Any], UhcFinalPublicationExpectation]:
    """Build one internally exact current-publication state and expectation."""

    summary_input_by_field = _build_summary_input(catalog_set_sha256)
    canonical_proof_by_field = _canonical_proof(
        summary_input_by_field,
        dataset_id=dataset_id,
        endpoint_id=endpoint_id,
        acquisition_root_run_id=acquisition_root_run_id,
    )
    source_summary_by_field = _build_source_summary_by_field(
        summary_input_by_field,
        canonical_proof_by_field,
        dataset_id=dataset_id,
        endpoint_id=endpoint_id,
        acquisition_root_run_id=acquisition_root_run_id,
    )
    outcome_by_field = _build_outcome_by_field(
        canonical_proof_by_field,
        dataset_id=dataset_id,
        endpoint_id=endpoint_id,
        acquisition_root_run_id=acquisition_root_run_id,
    )
    metadata_by_field = _build_metadata_by_field(
        summary_input_by_field,
        canonical_proof_by_field,
        source_summary_by_field,
        outcome_by_field,
        dataset_id=dataset_id,
        acquisition_root_run_id=acquisition_root_run_id,
    )
    state_by_field = {
        "source_id": UHC_RETAINED_SOURCE_ID,
        "dataset_id": dataset_id,
        "endpoint_id": endpoint_id,
        "acquisition_root_run_id": acquisition_root_run_id,
        "status": "published",
        "is_current": True,
        "dataset_hash": canonical_proof_by_field["dataset_hash"],
        "resource_count": canonical_proof_by_field["resource_count"],
        "published_at": None,
        "publication_metadata_json": metadata_by_field,
    }
    return state_by_field, UhcFinalPublicationExpectation(
        source_id=UHC_RETAINED_SOURCE_ID,
        dataset_id=dataset_id,
        endpoint_id=endpoint_id,
        acquisition_root_run_id=acquisition_root_run_id,
        selected_resources=SOURCE_SUMMARY_UHC_SELECTED_RESOURCES,
        semantic_contract_id=SOURCE_SUMMARY_UHC_SEMANTIC_CONTRACT_ID,
        catalog_set_sha256=catalog_set_sha256,
    )
