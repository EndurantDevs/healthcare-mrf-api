# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed contract for admitted rooted-graph combined dataset publication."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import hashlib
from typing import Any

from process.provider_directory_dataset_scoped_publication import (
    ExactCurrentDataset,
    exact_dataset_variant,
    exact_uhc_dataset_pair,
    LEGACY_PRACTITIONER_VARIANT,
    ROOTED_COMBINED_VARIANT,
)
from process.provider_directory_rooted_graph_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ROOT_PUBLICATION_BY_VARIANT,
)
from process.provider_directory_rooted_graph_publication_contract import (
    canonical_json,
    DATASET_PATTERN,
    DATASET_ROOT_PATTERN,
    HASH_PATTERN,
    ProviderDirectoryRootedGraphDatasetReadiness,
    ProviderDirectoryRootedGraphPublicationError,
    ProviderDirectoryRootedGraphPublicationResult,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_RESOURCES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_ROOT_CONTRACT_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_OUTPUT_RESOURCES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_CONTRACT_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_KIND,
)
from process.provider_directory_rooted_graph_publication_metadata import (
    rooted_graph_publication_metadata_sections,
)
from process.provider_directory_rooted_graph_store_contract import (
    ACQUISITION_PATTERN,
)
from process.uhc_flex_official_cohort_contract import (
    UHC_FLEX_OFFICIAL_AUTHORITY_ID,
)
from process.provider_directory_rooted_graph_twin_contract import (
    ADMISSION_PATTERN,
    ProviderDirectoryRootedGraphTwinAdmission,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SINGLE_ROOT_ADMISSION_CONTRACT_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_TWIN_ADMISSION_CONTRACT_ID,
)


def _identifier(prefix: str, values: tuple[object, ...]) -> str:
    content = "\x1f".join(str(value) for value in values)
    return prefix + hashlib.sha256(content.encode("utf-8")).hexdigest()[:48]


def _dataset_identity_tail(candidate: object) -> tuple[object, ...]:
    return tuple(
        getattr(candidate, field_name)
        for field_name in (
            "admission_id",
            "publication_acquisition_id",
            "source_id",
            "endpoint_id",
            "source_authority_id",
            "root_dataset_variant",
            "root_publication_contract_id",
            "root_source_id",
            "root_endpoint_id",
            "practitioner_origin_source_id",
            "practitioner_origin_endpoint_id",
            "root_dataset_id",
            "root_dataset_hash",
            "root_content_proof_sha256",
            "root_cohort_id",
            "root_practitioner_resource_count",
            "semantic_projection_as_of",
            "operation_key",
            "rooted_graph_sha256",
        )
    )


def _has_valid_dataset_identity_types(candidate: object) -> bool:
    try:
        date.fromisoformat(candidate.semantic_projection_as_of)
    except (AttributeError, TypeError, ValueError):
        return False
    return bool(
        type(candidate.dataset_id) is str
        and DATASET_PATTERN.fullmatch(candidate.dataset_id) is not None
        and type(candidate.acquisition_root_run_id) is str
        and DATASET_ROOT_PATTERN.fullmatch(candidate.acquisition_root_run_id)
        is not None
        and type(candidate.admission_id) is str
        and ADMISSION_PATTERN.fullmatch(candidate.admission_id) is not None
        and type(candidate.publication_acquisition_id) is str
        and ACQUISITION_PATTERN.fullmatch(candidate.publication_acquisition_id)
        is not None
        and type(candidate.root_practitioner_resource_count) is int
        and candidate.root_practitioner_resource_count >= 1
        and type(candidate.root_dataset_id) is str
        and 1 <= len(candidate.root_dataset_id) <= 96
        and type(candidate.root_cohort_id) is str
        and 1 <= len(candidate.root_cohort_id) <= 128
        and type(candidate.cohort_complete) is bool
        and type(candidate.retry_exhausted_count) is int
        and candidate.retry_exhausted_count >= 0
        and candidate.cohort_complete is (candidate.retry_exhausted_count == 0)
    )


def _has_valid_dataset_identity_lineage(candidate: object) -> bool:
    pair = exact_uhc_dataset_pair()
    is_rooted_variant = candidate.root_dataset_variant == ROOTED_COMBINED_VARIANT
    expected_root_source_id = (
        pair.rooted_source_id if is_rooted_variant else pair.legacy_source_id
    )
    expected_root_endpoint_id = (
        pair.rooted_endpoint_id if is_rooted_variant else pair.legacy_endpoint_id
    )
    has_same_root_pair = (
        candidate.root_source_id == candidate.source_id
        and candidate.root_endpoint_id == candidate.endpoint_id
    )
    return bool(
        candidate.root_dataset_variant
        in {LEGACY_PRACTITIONER_VARIANT, ROOTED_COMBINED_VARIANT}
        and candidate.root_publication_contract_id
        == PROVIDER_DIRECTORY_ROOTED_GRAPH_ROOT_PUBLICATION_BY_VARIANT[
            candidate.root_dataset_variant
        ]
        and has_same_root_pair == is_rooted_variant
        and candidate.source_id == pair.rooted_source_id
        and candidate.endpoint_id == pair.rooted_endpoint_id
        and candidate.source_authority_id == UHC_FLEX_OFFICIAL_AUTHORITY_ID
        and candidate.root_source_id == expected_root_source_id
        and candidate.root_endpoint_id == expected_root_endpoint_id
        and candidate.practitioner_origin_source_id == pair.legacy_source_id
        and candidate.practitioner_origin_endpoint_id == pair.legacy_endpoint_id
        and exact_dataset_variant(candidate.root_dataset_id)
        == candidate.root_dataset_variant
    )


def _has_valid_dataset_identity_content(candidate: object) -> bool:
    tail = _dataset_identity_tail(candidate)
    expected_dataset_id = _identifier(
        "pdrgpd_", (candidate.publication_contract_id, *tail)
    )
    expected_root_id = _identifier(
        "pdrgpr_", (PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_ROOT_CONTRACT_ID, *tail)
    )
    bounded_identifiers = (
        candidate.source_id,
        candidate.source_authority_id,
        candidate.root_source_id,
        candidate.practitioner_origin_source_id,
    )
    hashes = (
        candidate.endpoint_id,
        candidate.root_endpoint_id,
        candidate.practitioner_origin_endpoint_id,
        candidate.root_dataset_hash,
        candidate.root_content_proof_sha256,
        candidate.operation_key,
        candidate.rooted_graph_sha256,
    )
    return bool(
        candidate.publication_contract_id
        == PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_CONTRACT_ID
        and candidate.dataset_id == expected_dataset_id
        and candidate.acquisition_root_run_id == expected_root_id
        and all(
            type(text) is str and 1 <= len(text) <= 64 for text in bounded_identifiers
        )
        and all(
            type(digest) is str and HASH_PATTERN.fullmatch(digest) is not None
            for digest in hashes
        )
    )


@dataclass(frozen=True, slots=True, repr=False)
class ProviderDirectoryRootedGraphDatasetIdentity:
    """Deterministic generic-dataset and acquisition-root coordinates."""

    dataset_id: str
    acquisition_root_run_id: str
    admission_id: str
    publication_acquisition_id: str
    source_id: str
    endpoint_id: str
    source_authority_id: str
    root_dataset_variant: str
    root_publication_contract_id: str
    root_source_id: str
    root_endpoint_id: str
    root_dataset_id: str
    root_dataset_hash: str
    root_content_proof_sha256: str
    root_cohort_id: str
    root_practitioner_resource_count: int
    cohort_complete: bool
    retry_exhausted_count: int
    practitioner_origin_source_id: str
    practitioner_origin_endpoint_id: str
    semantic_projection_as_of: str
    operation_key: str
    rooted_graph_sha256: str
    publication_contract_id: str = (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_CONTRACT_ID
    )

    def __post_init__(self) -> None:
        """Reject forged deterministic identities before any database write."""

        if (
            not _has_valid_dataset_identity_types(self)
            or not _has_valid_dataset_identity_lineage(self)
            or not _has_valid_dataset_identity_content(self)
        ):
            raise ValueError("provider_directory_rooted_graph_dataset_identity_invalid")


def _has_matching_admission_lineage(
    admission: object,
    current_root: object,
) -> bool:
    if (
        type(admission) is not ProviderDirectoryRootedGraphTwinAdmission
        or type(current_root) is not ExactCurrentDataset
    ):
        return False
    exact_pair = exact_uhc_dataset_pair()
    has_single_root_identity = True
    if (
        admission.admission_contract_id
        == PROVIDER_DIRECTORY_ROOTED_GRAPH_SINGLE_ROOT_ADMISSION_CONTRACT_ID
    ):
        from process.provider_directory_rooted_graph_single_root_contract import (
            derive_single_root_identity,
        )

        expected = derive_single_root_identity(
            current_root,
            operation_key=admission.acquisition_operation_key,
        ).candidate
        has_single_root_identity = (
            admission.publication_acquisition_id == expected.acquisition_id
            and admission.publication_run_id == expected.run_id
            and admission.dataset_intent_id == expected.dataset_intent_id
        )
    elif current_root.cohort_complete is not True:
        has_single_root_identity = False
    return bool(
        admission.admission_contract_id
        in {
            PROVIDER_DIRECTORY_ROOTED_GRAPH_TWIN_ADMISSION_CONTRACT_ID,
            PROVIDER_DIRECTORY_ROOTED_GRAPH_SINGLE_ROOT_ADMISSION_CONTRACT_ID,
        }
        and admission.publication_authority is True
        and admission.root_dataset_id == current_root.dataset_id
        and admission.root_dataset_variant == current_root.variant
        and admission.root_publication_contract_id
        == current_root.root_publication_contract_id
        and admission.root_dataset_hash == current_root.dataset_hash
        and admission.root_content_proof_sha256
        == current_root.root_content_proof_sha256
        and admission.root_cohort_id == current_root.root_cohort_id
        and admission.root_resource_count == current_root.practitioner_resource_count
        and admission.root_source_id == current_root.root_source_id
        and admission.root_endpoint_id == current_root.root_endpoint_id
        and admission.acquisition_source_id == current_root.acquisition_source_id
        and admission.acquisition_endpoint_id == current_root.acquisition_endpoint_id
        and admission.source_authority_id == current_root.source_authority_id
        and admission.endpoint_signature_sha256
        == current_root.endpoint_signature_sha256
        and admission.acquisition_source_id == exact_pair.rooted_source_id
        and admission.acquisition_endpoint_id == exact_pair.rooted_endpoint_id
        and has_single_root_identity
    )


def _identity_tail_from_authority(
    admission: ProviderDirectoryRootedGraphTwinAdmission,
    current_root: ExactCurrentDataset,
) -> tuple[object, ...]:
    return (
        admission.admission_id,
        admission.publication_acquisition_id,
        admission.acquisition_source_id,
        admission.acquisition_endpoint_id,
        admission.source_authority_id,
        current_root.variant,
        current_root.root_publication_contract_id,
        current_root.root_source_id,
        current_root.root_endpoint_id,
        current_root.practitioner_origin_source_id,
        current_root.practitioner_origin_endpoint_id,
        current_root.dataset_id,
        current_root.dataset_hash,
        current_root.root_content_proof_sha256,
        current_root.root_cohort_id,
        current_root.practitioner_resource_count,
        current_root.semantic_projection_as_of,
        current_root.operation_key,
        admission.rooted_graph_sha256,
    )


def build_rooted_graph_dataset_identity(
    admission: ProviderDirectoryRootedGraphTwinAdmission,
    current_root: ExactCurrentDataset,
) -> ProviderDirectoryRootedGraphDatasetIdentity:
    """Bind one admitted graph to the exact current root rechecked in transaction."""

    if not _has_matching_admission_lineage(admission, current_root):
        raise ValueError("provider_directory_rooted_graph_dataset_admission_invalid")
    tail = _identity_tail_from_authority(admission, current_root)
    return ProviderDirectoryRootedGraphDatasetIdentity(
        dataset_id=_identifier(
            "pdrgpd_",
            (PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_CONTRACT_ID, *tail),
        ),
        acquisition_root_run_id=_identifier(
            "pdrgpr_",
            (PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_ROOT_CONTRACT_ID, *tail),
        ),
        admission_id=admission.admission_id,
        publication_acquisition_id=admission.publication_acquisition_id,
        source_id=admission.acquisition_source_id,
        endpoint_id=admission.acquisition_endpoint_id,
        source_authority_id=admission.source_authority_id,
        root_dataset_variant=current_root.variant,
        root_publication_contract_id=current_root.root_publication_contract_id,
        root_source_id=current_root.root_source_id,
        root_endpoint_id=current_root.root_endpoint_id,
        root_dataset_id=current_root.dataset_id,
        root_dataset_hash=current_root.dataset_hash,
        root_content_proof_sha256=current_root.root_content_proof_sha256,
        root_cohort_id=current_root.root_cohort_id,
        root_practitioner_resource_count=current_root.practitioner_resource_count,
        cohort_complete=current_root.cohort_complete,
        retry_exhausted_count=current_root.retry_exhausted_count,
        practitioner_origin_source_id=current_root.practitioner_origin_source_id,
        practitioner_origin_endpoint_id=current_root.practitioner_origin_endpoint_id,
        semantic_projection_as_of=current_root.semantic_projection_as_of,
        operation_key=current_root.operation_key,
        rooted_graph_sha256=admission.rooted_graph_sha256,
    )


build_provider_directory_rooted_graph_dataset_identity = (
    build_rooted_graph_dataset_identity
)


def _has_valid_publication_metadata_inputs(
    identity: ProviderDirectoryRootedGraphDatasetIdentity,
    admission: ProviderDirectoryRootedGraphTwinAdmission,
    previous_dataset_id: str | None,
    resource_counts: dict[str, int],
) -> bool:
    if (
        type(identity) is not ProviderDirectoryRootedGraphDatasetIdentity
        or type(admission) is not ProviderDirectoryRootedGraphTwinAdmission
        or type(resource_counts) is not dict
    ):
        return False
    return bool(
        identity.admission_id == admission.admission_id
        and identity.publication_acquisition_id == admission.publication_acquisition_id
        and identity.source_id == admission.acquisition_source_id
        and identity.endpoint_id == admission.acquisition_endpoint_id
        and identity.source_authority_id == admission.source_authority_id
        and identity.root_dataset_variant == admission.root_dataset_variant
        and identity.root_publication_contract_id
        == admission.root_publication_contract_id
        and identity.root_source_id == admission.root_source_id
        and identity.root_endpoint_id == admission.root_endpoint_id
        and identity.root_dataset_id == admission.root_dataset_id
        and identity.root_dataset_hash == admission.root_dataset_hash
        and identity.root_content_proof_sha256 == admission.root_content_proof_sha256
        and identity.root_cohort_id == admission.root_cohort_id
        and identity.root_practitioner_resource_count == admission.root_resource_count
        and identity.rooted_graph_sha256 == admission.rooted_graph_sha256
        and set(resource_counts)
        == set(PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_RESOURCES)
        and all(type(count) is int and count >= 0 for count in resource_counts.values())
        and resource_counts["Practitioner"] == identity.root_practitioner_resource_count
        and previous_dataset_id == identity.root_dataset_id
    )


def provider_directory_rooted_graph_publication_metadata(
    identity: ProviderDirectoryRootedGraphDatasetIdentity,
    admission: ProviderDirectoryRootedGraphTwinAdmission,
    *,
    previous_dataset_id: str | None,
    resource_counts: dict[str, int],
) -> dict[str, Any]:
    """Return the exact closed parent metadata object checked by PostgreSQL."""

    if not _has_valid_publication_metadata_inputs(
        identity,
        admission,
        previous_dataset_id,
        resource_counts,
    ):
        raise ProviderDirectoryRootedGraphPublicationError("content")
    assert previous_dataset_id is not None
    return rooted_graph_publication_metadata_sections(
        identity,
        admission,
        previous_dataset_id,
        resource_counts,
    )


from process.provider_directory_rooted_graph_publication_facade import (
    load_provider_directory_rooted_graph_dataset_readiness,
    publish_provider_directory_rooted_graph_dataset,
)

__all__ = tuple(
    (
        "build_provider_directory_rooted_graph_dataset_identity canonical_json "
        "load_provider_directory_rooted_graph_dataset_readiness "
        "provider_directory_rooted_graph_publication_metadata "
        "publish_provider_directory_rooted_graph_dataset ProviderDirectoryRootedGraphDatasetIdentity "
        "ProviderDirectoryRootedGraphDatasetReadiness ProviderDirectoryRootedGraphPublicationError "
        "ProviderDirectoryRootedGraphPublicationResult DATASET_PATTERN "
        "DATASET_ROOT_PATTERN PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_RESOURCES "
        "PROVIDER_DIRECTORY_ROOTED_GRAPH_OUTPUT_RESOURCES "
        "PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_CONTRACT_ID "
        "PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_KIND"
    ).split()
)
