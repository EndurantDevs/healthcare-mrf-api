# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Metadata sections for one admitted rooted-graph publication."""

from __future__ import annotations

from typing import Any

from process.provider_directory_fhir_root_policy import (
    REVIEWED_ROOT_POLICY_METADATA_KEY,
)
from process.provider_directory_resource_hash import (
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
)
from process.provider_directory_rooted_graph_publication_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_RESOURCES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_KIND,
)
from process.provider_directory_rooted_graph_twin_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SINGLE_ROOT_ADMISSION_CONTRACT_ID,
)


def _publication_lineage_metadata(
    identity: Any,
    admission: Any,
    previous_dataset_id: str,
) -> dict[str, Any]:
    metadata_by_field = {
        "acquisition_root_run_id": identity.acquisition_root_run_id,
        "admission_contract_id": admission.admission_contract_id,
        "admission_id": admission.admission_id,
        "attempt_id": admission.attempt_id,
        "comparison_acquisition_id": admission.comparison_acquisition_id,
        "publication_acquisition_id": admission.publication_acquisition_id,
        "publication_run_id": admission.publication_run_id,
        "publication_contract_id": identity.publication_contract_id,
        "publication_kind": PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_KIND,
        "dataset_id": identity.dataset_id,
        "previous_dataset_id": previous_dataset_id,
        "dataset_intent_id": admission.dataset_intent_id,
        "scope_id": admission.scope_id,
        "root_source_id": identity.root_source_id,
        "root_endpoint_id": identity.root_endpoint_id,
        "acquisition_source_id": identity.source_id,
        "acquisition_endpoint_id": identity.endpoint_id,
        "endpoint_id": identity.endpoint_id,
        "endpoint_signature_sha256": admission.endpoint_signature_sha256,
        "source_id": identity.source_id,
        "source_ids": [identity.source_id],
        "source_authority_id": identity.source_authority_id,
        "semantic_projection_as_of": identity.semantic_projection_as_of,
        "operation_key": identity.operation_key,
        "root_variant": identity.root_dataset_variant,
        "root_publication_contract_id": identity.root_publication_contract_id,
        "root_dataset_id": identity.root_dataset_id,
        "root_dataset_hash": identity.root_dataset_hash,
        "root_content_proof_sha256": identity.root_content_proof_sha256,
        "root_cohort_id": identity.root_cohort_id,
        "root_practitioner_resource_count": identity.root_practitioner_resource_count,
        "practitioner_origin_source_id": identity.practitioner_origin_source_id,
        "practitioner_origin_endpoint_id": identity.practitioner_origin_endpoint_id,
        "connector_id": admission.connector_id,
        "storage_contract_id": admission.storage_contract_id,
        "graph_contract_sha256": admission.graph_contract_sha256,
        "query_contract_sha256": admission.query_contract_sha256,
    }
    if (
        admission.admission_contract_id
        == PROVIDER_DIRECTORY_ROOTED_GRAPH_SINGLE_ROOT_ADMISSION_CONTRACT_ID
    ):
        metadata_by_field["acquisition_operation_key"] = (
            admission.acquisition_operation_key
        )
        metadata_by_field[REVIEWED_ROOT_POLICY_METADATA_KEY] = (
            admission.reviewed_root_policy_json
        )
    return metadata_by_field


def _publication_proof_metadata(admission: Any) -> dict[str, Any]:
    return {
        field_name: getattr(admission, field_name)
        for field_name in (
            "max_work_items",
            "max_resource_rows",
            "max_edge_rows",
            "max_payload_bytes",
            "used_work_items",
            "used_resource_rows",
            "used_edge_rows",
            "used_payload_bytes",
            "completed_count",
            "resource_count",
            "edge_count",
            "insurance_plan_count",
            "insurance_plan_page_count",
            "terminal_set_sha256",
            "resource_set_sha256",
            "edge_set_sha256",
            "rooted_graph_sha256",
        )
    }


def _publication_resource_metadata(
    resource_count_by_type: dict[str, int],
    *,
    cohort_complete: bool,
    retry_exhausted_count: int,
) -> dict[str, Any]:
    resource_types = list(PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_RESOURCES)
    metadata_by_field = {
        "resource_hash_contract": SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        "selected_resources": resource_types,
        "expected_resources": resource_types,
        "resource_counts": {
            resource_type: resource_count_by_type[resource_type]
            for resource_type in PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_RESOURCES
        },
        "cohort_complete": cohort_complete,
        "rooted_graph_complete": True,
        "endpoint_collection_complete": False,
        "endpoint_complete": False,
    }
    if not cohort_complete:
        metadata_by_field["retry_exhausted_count"] = retry_exhausted_count
    return metadata_by_field


def rooted_graph_publication_metadata_sections(
    identity: Any,
    admission: Any,
    previous_dataset_id: str,
    resource_count_by_type: dict[str, int],
) -> dict[str, Any]:
    """Assemble validated lineage, proof, and resource metadata sections."""

    return {
        **_publication_lineage_metadata(identity, admission, previous_dataset_id),
        **_publication_proof_metadata(admission),
        **_publication_resource_metadata(
            resource_count_by_type,
            cohort_complete=identity.cohort_complete,
            retry_exhausted_count=identity.retry_exhausted_count,
        ),
    }


__all__ = ("rooted_graph_publication_metadata_sections",)
