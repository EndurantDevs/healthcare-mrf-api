# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic exact dataset variants shared by Profile Flex contract tests."""

from __future__ import annotations

from types import SimpleNamespace

from process.provider_directory_dataset_scoped_publication import (
    LEGACY_PRACTITIONER_VARIANT,
    ROOTED_COMBINED_VARIANT,
)
from process.provider_directory_rooted_graph_publication import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_RESOURCES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_CONTRACT_ID,
)
from process.provider_directory_rooted_graph_source_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
)
from process.provider_directory_rooted_graph_twin_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_TWIN_ADMISSION_CONTRACT_ID,
)
from process.uhc_flex_practitioner_contract import UHC_FLEX_PRACTITIONER_SOURCE_ID
from process.uhc_flex_practitioner_twin_store_contract import (
    UHC_FLEX_PRACTITIONER_TWIN_ADMISSION_CONTRACT_ID,
)


OFFICIAL_SOURCE_ID = "pdfhir_2754e999dd691175821ec26e"
FLEX_ENDPOINT_ID = "ad53a7446514ed65b3a8ea7ab68ceb9a1ef85bf6c04fcb882219ecb50928bab5"
GRAPH_ENDPOINT_ID = PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID
OFFICIAL_ENDPOINT_ID = "b" * 64
FLEX_DATASET_ID = "pdufpd_" + "c" * 48
GRAPH_DATASET_ID = "pdrgpd_" + "9" * 48


def _flex_metadata(*, projection: str = "2026-08-09") -> dict[str, object]:
    return {
        "acquisition_root_run_id": "pdufpar_" + "d" * 48,
        "admission_contract_id": UHC_FLEX_PRACTITIONER_TWIN_ADMISSION_CONTRACT_ID,
        "admission_id": "pdufpa_" + "e" * 48,
        "baseline_acquisition_id": "pdufpa_" + "a" * 48,
        "baseline_run_id": "pdufpr_" + "b" * 48,
        "cohort_complete": True,
        "dataset_id": FLEX_DATASET_ID,
        "endpoint_collection_complete": False,
        "endpoint_complete": False,
        "endpoint_id": FLEX_ENDPOINT_ID,
        "expected_resources": ["Practitioner"],
        "operation_key": "f" * 64,
        "publication_contract_id": (
            "healthporta.provider-directory.uhc-flex-practitioner-"
            "dataset-publication.v1"
        ),
        "resource_hash_contract": "semantic_content_v3",
        "selected_resources": ["Practitioner"],
        "semantic_projection_as_of": projection,
        "source_authority_id": "unitedhealthcare",
        "source_id": UHC_FLEX_PRACTITIONER_SOURCE_ID,
        "source_ids": [UHC_FLEX_PRACTITIONER_SOURCE_ID],
    }


def _rooted_metadata(*, projection: str = "2026-08-09") -> dict[str, object]:
    resources = list(PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_RESOURCES)
    return {
        "acquisition_root_run_id": "pdrgpr_" + "8" * 48,
        "acquisition_source_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
        "acquisition_endpoint_id": GRAPH_ENDPOINT_ID,
        "admission_contract_id": (
            PROVIDER_DIRECTORY_ROOTED_GRAPH_TWIN_ADMISSION_CONTRACT_ID
        ),
        "admission_id": "pdrgad_" + "7" * 48,
        "attempt_id": "pdrgat_" + "a" * 48,
        "comparison_acquisition_id": "pdrga_" + "b" * 48,
        "publication_acquisition_id": "pdrga_" + "c" * 48,
        "cohort_complete": True,
        "dataset_id": GRAPH_DATASET_ID,
        "endpoint_collection_complete": False,
        "endpoint_complete": False,
        "endpoint_id": GRAPH_ENDPOINT_ID,
        "expected_resources": resources,
        "operation_key": "6" * 64,
        "practitioner_origin_source_id": UHC_FLEX_PRACTITIONER_SOURCE_ID,
        "practitioner_origin_endpoint_id": FLEX_ENDPOINT_ID,
        "publication_contract_id": (
            PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_CONTRACT_ID
        ),
        "publication_kind": ROOTED_COMBINED_VARIANT,
        "resource_counts": {resource_type: 1 for resource_type in resources},
        "resource_hash_contract": "semantic_content_v3",
        "root_source_id": UHC_FLEX_PRACTITIONER_SOURCE_ID,
        "root_endpoint_id": FLEX_ENDPOINT_ID,
        "root_variant": LEGACY_PRACTITIONER_VARIANT,
        "rooted_graph_complete": True,
        "selected_resources": resources,
        "semantic_projection_as_of": projection,
        "source_authority_id": "unitedhealthcare",
        "source_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
        "source_ids": [PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID],
    }


def _source_rows() -> list[dict[str, object]]:
    return [
        {
            "source_id": OFFICIAL_SOURCE_ID,
            "endpoint_id": OFFICIAL_ENDPOINT_ID,
            "canonical_api_base": "https://files.example.test",
            "org_name": "Official files",
            "plan_name": None,
        },
        {
            "source_id": UHC_FLEX_PRACTITIONER_SOURCE_ID,
            "endpoint_id": FLEX_ENDPOINT_ID,
            "canonical_api_base": "https://directory.example.test/R4",
            "org_name": "Practitioner enrichment",
            "plan_name": None,
        },
        {
            "source_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
            "endpoint_id": GRAPH_ENDPOINT_ID,
            "canonical_api_base": "https://directory.example.test/R4",
            "org_name": "Rooted graph enrichment",
            "plan_name": None,
        },
        {
            "source_id": "pdfhir_0b5cfd565c53364a73981dcb",
            "endpoint_id": "probe-endpoint",
            "canonical_api_base": "https://directory.example.test/R4",
            "org_name": "Generic probe",
            "plan_name": None,
        },
    ]


def _dataset_rows(*, ready: bool = True) -> list[dict[str, object]]:
    metadata = _flex_metadata()
    return [
        {
            "endpoint_id": FLEX_ENDPOINT_ID,
            "dataset_id": FLEX_DATASET_ID,
            "acquisition_root_run_id": metadata["acquisition_root_run_id"],
            "dataset_hash": "1" * 64,
            "status": "published",
            "is_current": True,
            "resource_count": 3,
            "validated_at": "2026-08-09T00:00:00",
            "published_at": "2026-08-10T00:00:00",
            "superseded_at": None,
            "publication_metadata_json": metadata,
            "dataset_scoped_ready": ready,
            "dataset_scoped_variant": LEGACY_PRACTITIONER_VARIANT,
            "dataset_scoped_publication_kind": LEGACY_PRACTITIONER_VARIANT,
            "dataset_scoped_admission_id": metadata["admission_id"],
            "dataset_scoped_projection_as_of": "2026-08-09",
            "dataset_scoped_authority_id": "unitedhealthcare",
            "dataset_scoped_operation_key": "f" * 64,
            "dataset_scoped_cohort_complete": True,
            "dataset_scoped_rooted_graph_complete": None,
            "dataset_scoped_endpoint_collection_complete": False,
            "dataset_scoped_endpoint_complete": False,
        }
    ]


def _rooted_dataset_rows(*, ready: bool = True) -> list[dict[str, object]]:
    metadata = _rooted_metadata()
    return [
        {
            "endpoint_id": GRAPH_ENDPOINT_ID,
            "dataset_id": GRAPH_DATASET_ID,
            "acquisition_root_run_id": metadata["acquisition_root_run_id"],
            "dataset_hash": "5" * 64,
            "status": "published",
            "is_current": True,
            "resource_count": 8,
            "validated_at": "2026-08-09T00:00:00",
            "published_at": "2026-08-10T00:00:00",
            "superseded_at": None,
            "publication_metadata_json": metadata,
            "dataset_scoped_ready": ready,
            "dataset_scoped_variant": ROOTED_COMBINED_VARIANT,
            "dataset_scoped_publication_kind": ROOTED_COMBINED_VARIANT,
            "dataset_scoped_admission_id": metadata["admission_id"],
            "dataset_scoped_projection_as_of": "2026-08-09",
            "dataset_scoped_authority_id": "unitedhealthcare",
            "dataset_scoped_operation_key": "6" * 64,
            "dataset_scoped_cohort_complete": True,
            "dataset_scoped_rooted_graph_complete": True,
            "dataset_scoped_endpoint_collection_complete": False,
            "dataset_scoped_endpoint_complete": False,
        }
    ]


def _catalog() -> dict[str, object]:
    return {
        "catalog_digest": "2" * 64,
        "items": [
            {
                "entry_id": "uhc-provider-files",
                "runnable": True,
                "profile_enabled": True,
                "source_ids": [OFFICIAL_SOURCE_ID],
            },
            {
                "entry_id": "uhc-generic-probe",
                "runnable": False,
                "profile_enabled": False,
                "source_ids": ["pdfhir_0b5cfd565c53364a73981dcb"],
            },
        ],
    }


def _readiness_record(**overrides: object) -> SimpleNamespace:
    metadata = _flex_metadata()
    readiness_by_field: dict[str, object] = {
        "dataset_id": FLEX_DATASET_ID,
        "endpoint_id": FLEX_ENDPOINT_ID,
        "source_id": UHC_FLEX_PRACTITIONER_SOURCE_ID,
        "source_authority_id": "unitedhealthcare",
        "dataset_hash": "1" * 64,
        "resource_count": 3,
        "semantic_projection_as_of": metadata["semantic_projection_as_of"],
        "admission_id": metadata["admission_id"],
        "operation_key": metadata["operation_key"],
        "cohort_complete": True,
        "endpoint_collection_complete": False,
        "endpoint_complete": False,
    }
    readiness_by_field.update(overrides)
    return SimpleNamespace(**readiness_by_field)


def _artifact_dataset_row() -> dict[str, object]:
    metadata = _flex_metadata()
    return {
        "source_id": UHC_FLEX_PRACTITIONER_SOURCE_ID,
        "dataset_id": FLEX_DATASET_ID,
        "endpoint_id": FLEX_ENDPOINT_ID,
        "acquisition_root_run_id": metadata["acquisition_root_run_id"],
        "dataset_hash": "1" * 64,
        "resource_count": 3,
        "publication_metadata_json": metadata,
        "dataset_scoped_variant": LEGACY_PRACTITIONER_VARIANT,
    }


__all__ = (
    "FLEX_DATASET_ID",
    "FLEX_ENDPOINT_ID",
    "GRAPH_DATASET_ID",
    "GRAPH_ENDPOINT_ID",
    "OFFICIAL_ENDPOINT_ID",
    "OFFICIAL_SOURCE_ID",
    "_artifact_dataset_row",
    "_catalog",
    "_dataset_rows",
    "_flex_metadata",
    "_readiness_record",
    "_rooted_dataset_rows",
    "_rooted_metadata",
    "_source_rows",
)
