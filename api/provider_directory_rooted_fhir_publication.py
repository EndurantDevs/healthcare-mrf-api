# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed read model for one cohort-rooted FHIR publication."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from process.import_status_events import isoformat_utc
from process.provider_directory_rooted_graph_publication_contract import (
    ProviderDirectoryRootedGraphDatasetReadiness,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_CONTRACT_ID,
)
from process.provider_directory_rooted_graph_source_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
)


ROOTED_FHIR_PUBLICATION_FIELD = "rooted_fhir_publication"
ROOTED_FHIR_PUBLICATION_SUMMARY_CONTRACT_ID = (
    "healthporta.provider-directory.rooted-fhir-publication-summary.v1"
)
ROOTED_FHIR_CATALOG_ENTRY_ID = "uhc"
ROOTED_FHIR_CATALOG_SOURCE_IDS = (
    "pdfhir_0b5cfd565c53364a73981dcb",
)
ROOTED_FHIR_SOURCE_ID_GROUP = (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
)


def _summary_state(state: str) -> dict[str, Any]:
    return {
        "contract_id": ROOTED_FHIR_PUBLICATION_SUMMARY_CONTRACT_ID,
        "state": state,
    }


def is_rooted_fhir_catalog_entry(entry: Mapping[str, Any]) -> bool:
    """Match only the exact direct-FHIR catalog anchor."""

    source_ids = entry.get("source_ids")
    return bool(
        entry.get("entry_id") == ROOTED_FHIR_CATALOG_ENTRY_ID
        and isinstance(source_ids, (list, tuple))
        and tuple(source_ids) == ROOTED_FHIR_CATALOG_SOURCE_IDS
    )


def unavailable_rooted_fhir_publication() -> dict[str, Any]:
    """Distinguish static fallback from database-backed publication state."""

    return _summary_state("unavailable")


def _has_current_dataset_identity(
    dataset: object,
    readiness: ProviderDirectoryRootedGraphDatasetReadiness,
) -> bool:
    return bool(
        getattr(dataset, "source_ids", None) == (readiness.source_id,)
        and getattr(dataset, "dataset_id", None) == readiness.dataset_id
        and getattr(dataset, "endpoint_id", None) == readiness.endpoint_id
        and getattr(dataset, "acquisition_root_run_id", None)
        == readiness.acquisition_root_run_id
        and getattr(dataset, "dataset_hash", None) == readiness.dataset_hash
        and getattr(dataset, "resource_count", None) == readiness.resource_count
        and getattr(dataset, "status", None) == "published"
        and getattr(dataset, "is_current", None) is True
        and getattr(dataset, "published_at", None) is not None
    )


def rooted_fhir_publication_summary(
    dataset: object | None,
    readiness: ProviderDirectoryRootedGraphDatasetReadiness | None,
) -> dict[str, Any]:
    """Project a closed summary only from exact current readiness evidence."""

    if dataset is None:
        return _summary_state("not_published")
    if (
        type(readiness) is not ProviderDirectoryRootedGraphDatasetReadiness
        or not _has_current_dataset_identity(dataset, readiness)
    ):
        return _summary_state("not_ready")
    published_at = isoformat_utc(getattr(dataset, "published_at"))
    if not isinstance(published_at, str):
        return _summary_state("not_ready")
    return {
        **_summary_state("closed"),
        "publication_contract_id": (
            PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_CONTRACT_ID
        ),
        "publication_kind": readiness.publication_kind,
        "source_id": readiness.source_id,
        "endpoint_id": readiness.endpoint_id,
        "source_authority_id": readiness.source_authority_id,
        "dataset_id": readiness.dataset_id,
        "dataset_hash": readiness.dataset_hash,
        "acquisition_root_run_id": readiness.acquisition_root_run_id,
        "admission_id": readiness.admission_id,
        "publication_acquisition_id": readiness.publication_acquisition_id,
        "root_dataset_variant": readiness.root_dataset_variant,
        "root_publication_contract_id": readiness.root_publication_contract_id,
        "root_dataset_id": readiness.root_dataset_id,
        "root_dataset_hash": readiness.root_dataset_hash,
        "root_content_proof_sha256": readiness.root_content_proof_sha256,
        "root_cohort_id": readiness.root_cohort_id,
        "semantic_projection_as_of": readiness.semantic_projection_as_of,
        "published_at": published_at,
        "total_resources": readiness.resource_count,
        "resource_counts": dict(readiness.resource_counts),
        "cohort_complete": readiness.cohort_complete,
        "rooted_graph_complete": readiness.rooted_graph_complete,
        "endpoint_collection_complete": readiness.endpoint_collection_complete,
        "endpoint_complete": readiness.endpoint_complete,
    }


__all__ = (
    "is_rooted_fhir_catalog_entry",
    "rooted_fhir_publication_summary",
    "unavailable_rooted_fhir_publication",
    "ROOTED_FHIR_CATALOG_ENTRY_ID",
    "ROOTED_FHIR_CATALOG_SOURCE_IDS",
    "ROOTED_FHIR_PUBLICATION_FIELD",
    "ROOTED_FHIR_PUBLICATION_SUMMARY_CONTRACT_ID",
    "ROOTED_FHIR_SOURCE_ID_GROUP",
)
