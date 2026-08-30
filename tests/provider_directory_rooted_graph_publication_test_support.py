# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure fixtures for rooted twin and combined-publication contract tests."""

from __future__ import annotations

from datetime import UTC, datetime

from process.provider_directory_dataset_scoped_publication import (
    ExactCurrentDataset,
    exact_uhc_dataset_pair,
    LEGACY_PRACTITIONER_VARIANT,
    ROOTED_COMBINED_VARIANT,
)
from process.provider_directory_rooted_graph_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_CONNECTOR_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ROOT_PUBLICATION_BY_VARIANT,
)
from process.provider_directory_rooted_graph_publication import (
    build_provider_directory_rooted_graph_dataset_identity,
    ProviderDirectoryRootedGraphDatasetIdentity,
)
from process.provider_directory_rooted_graph_publication_contract import (
    ProviderDirectoryRootedGraphDatasetReadiness,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_RESOURCES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_KIND,
)
from process.provider_directory_rooted_graph_source_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256,
)
from process.provider_directory_rooted_graph_store_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_SHA256,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_QUERY_CONTRACT_SHA256,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_STORAGE_CONTRACT_ID,
)
from process.provider_directory_rooted_graph_twin_contract import (
    build_provider_directory_rooted_graph_twin_admission,
    build_provider_directory_rooted_graph_twin_attempt,
    ProviderDirectoryRootedGraphSealedRoot,
    ProviderDirectoryRootedGraphTwinAdmission,
)
from process.uhc_flex_official_cohort_contract import (
    UHC_FLEX_OFFICIAL_AUTHORITY_ID,
)


def sealed_roots(
    *,
    variant: str = LEGACY_PRACTITIONER_VARIANT,
    second_resource_hash: str = "b" * 64,
) -> tuple[
    ProviderDirectoryRootedGraphSealedRoot,
    ProviderDirectoryRootedGraphSealedRoot,
]:
    """Return a baseline/candidate pair with deterministic sealed evidence."""

    fields_by_name = _sealed_root_fields(variant, second_resource_hash)
    baseline = ProviderDirectoryRootedGraphSealedRoot(
        acquisition_id="pdrga_" + "1" * 48,
        acquisition_role="baseline",
        run_id="pdrgr_" + "7" * 48,
        **fields_by_name,
    )
    candidate = ProviderDirectoryRootedGraphSealedRoot(
        acquisition_id="pdrga_" + "2" * 48,
        acquisition_role="candidate",
        run_id="pdrgr_" + "8" * 48,
        **fields_by_name,
    )
    return baseline, candidate


def _sealed_root_fields(variant: str, resource_hash: str) -> dict[str, object]:
    pair = exact_uhc_dataset_pair()
    is_rooted = variant == ROOTED_COMBINED_VARIANT
    root_source_id = pair.rooted_source_id if is_rooted else pair.legacy_source_id
    root_endpoint_id = pair.rooted_endpoint_id if is_rooted else pair.legacy_endpoint_id
    return {
        "storage_contract_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_STORAGE_CONTRACT_ID,
        "scope_id": "pdrgs_" + "3" * 48,
        "root_source_id": root_source_id,
        "root_endpoint_id": root_endpoint_id,
        "acquisition_source_id": pair.rooted_source_id,
        "acquisition_endpoint_id": pair.rooted_endpoint_id,
        "source_authority_id": UHC_FLEX_OFFICIAL_AUTHORITY_ID,
        "endpoint_signature_sha256": (
            PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256
        ),
        "root_dataset_id": ("pdrgpd_" if is_rooted else "pdufpd_") + "4" * 48,
        "root_dataset_variant": variant,
        "root_publication_contract_id": (
            PROVIDER_DIRECTORY_ROOTED_GRAPH_ROOT_PUBLICATION_BY_VARIANT[variant]
        ),
        "root_dataset_hash": "5" * 64,
        "root_content_proof_sha256": "6" * 64,
        "root_cohort_id": "synthetic-root-cohort",
        "root_resource_count": 1,
        "connector_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_CONNECTOR_ID,
        "graph_contract_sha256": PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_SHA256,
        "query_contract_sha256": PROVIDER_DIRECTORY_ROOTED_GRAPH_QUERY_CONTRACT_SHA256,
        "dataset_intent_id": "pdrgi_" + "9" * 48,
        "max_work_items": 10,
        "max_resource_rows": 10,
        "max_edge_rows": 10,
        "max_payload_bytes": 1_000_000,
        "pending_count": 0,
        "leased_count": 0,
        "completed_count": 3,
        "error_count": 0,
        "resource_count": 2,
        "edge_count": 1,
        "insurance_plan_count": 1,
        "insurance_plan_page_count": 1,
        "used_work_items": 3,
        "used_resource_rows": 2,
        "used_edge_rows": 1,
        "used_payload_bytes": 512,
        "terminal_set_sha256": "a" * 64,
        "resource_set_sha256": resource_hash,
        "edge_set_sha256": "c" * 64,
        "rooted_graph_sha256": "d" * 64,
    }


def twin_admission(
    *,
    variant: str = LEGACY_PRACTITIONER_VARIANT,
) -> ProviderDirectoryRootedGraphTwinAdmission:
    """Build a valid matched candidate publication authority."""

    baseline, candidate = sealed_roots(variant=variant)
    timestamp = datetime(2026, 8, 10, 12, tzinfo=UTC)
    attempt = build_provider_directory_rooted_graph_twin_attempt(
        baseline,
        candidate,
        attempted_at=timestamp,
    )
    return build_provider_directory_rooted_graph_twin_admission(
        attempt,
        candidate,
        admitted_at=timestamp,
    )


def exact_current(
    *,
    variant: str = LEGACY_PRACTITIONER_VARIANT,
    retry_exhausted_count: int = 0,
) -> ExactCurrentDataset:
    """Build one exact ready root capability for either closed variant."""

    pair = exact_uhc_dataset_pair()
    is_rooted = variant == ROOTED_COMBINED_VARIANT
    source_id = pair.rooted_source_id if is_rooted else pair.legacy_source_id
    endpoint_id = pair.rooted_endpoint_id if is_rooted else pair.legacy_endpoint_id
    return ExactCurrentDataset(
        dataset_id=("pdrgpd_" if is_rooted else "pdufpd_") + "4" * 48,
        endpoint_id=endpoint_id,
        source_id=source_id,
        root_source_id=source_id,
        root_endpoint_id=endpoint_id,
        acquisition_source_id=pair.rooted_source_id,
        acquisition_endpoint_id=pair.rooted_endpoint_id,
        practitioner_origin_source_id=pair.legacy_source_id,
        practitioner_origin_endpoint_id=pair.legacy_endpoint_id,
        source_authority_id=UHC_FLEX_OFFICIAL_AUTHORITY_ID,
        endpoint_signature_sha256=(
            PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256
        ),
        dataset_hash="5" * 64,
        resource_count=8 if is_rooted else 1,
        practitioner_resource_count=1,
        root_content_proof_sha256="6" * 64,
        root_cohort_id="synthetic-root-cohort",
        cohort_complete=retry_exhausted_count == 0,
        retry_exhausted_count=retry_exhausted_count,
        semantic_projection_as_of="2026-08-10",
        operation_key="e" * 64,
        acquisition_root_run_id=("pdrgpr_" if is_rooted else "pdufpar_") + "f" * 48,
        variant=variant,
        root_publication_contract_id=(
            PROVIDER_DIRECTORY_ROOTED_GRAPH_ROOT_PUBLICATION_BY_VARIANT[variant]
        ),
    )


def dataset_identity(
    *,
    variant: str = LEGACY_PRACTITIONER_VARIANT,
) -> ProviderDirectoryRootedGraphDatasetIdentity:
    return build_provider_directory_rooted_graph_dataset_identity(
        twin_admission(variant=variant),
        exact_current(variant=variant),
    )


def resource_counts() -> dict[str, int]:
    return {
        resource_type: (1 if resource_type == "Practitioner" else 0)
        for resource_type in PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_RESOURCES
    }


def readiness(
    *,
    variant: str = LEGACY_PRACTITIONER_VARIANT,
    retry_exhausted_count: int = 0,
) -> ProviderDirectoryRootedGraphDatasetReadiness:
    identity = dataset_identity(variant=variant)
    counts = resource_counts()
    return ProviderDirectoryRootedGraphDatasetReadiness(
        dataset_id=identity.dataset_id,
        previous_dataset_id=identity.root_dataset_id,
        admission_id=identity.admission_id,
        publication_acquisition_id=identity.publication_acquisition_id,
        acquisition_root_run_id=identity.acquisition_root_run_id,
        source_id=identity.source_id,
        endpoint_id=identity.endpoint_id,
        source_authority_id=identity.source_authority_id,
        root_dataset_variant=identity.root_dataset_variant,
        root_publication_contract_id=identity.root_publication_contract_id,
        root_dataset_id=identity.root_dataset_id,
        root_dataset_hash=identity.root_dataset_hash,
        root_content_proof_sha256=identity.root_content_proof_sha256,
        root_cohort_id=identity.root_cohort_id,
        practitioner_resource_count=1,
        semantic_projection_as_of=identity.semantic_projection_as_of,
        operation_key=identity.operation_key,
        dataset_hash="1" * 64,
        resource_count=sum(counts.values()),
        resource_counts=counts,
        publication_kind=PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_KIND,
        cohort_complete=retry_exhausted_count == 0,
        retry_exhausted_count=retry_exhausted_count,
        rooted_graph_complete=True,
        endpoint_collection_complete=False,
        endpoint_complete=False,
    )


__all__ = (
    "dataset_identity",
    "exact_current",
    "readiness",
    "resource_counts",
    "sealed_roots",
    "twin_admission",
)
