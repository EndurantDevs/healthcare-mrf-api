# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Public constants, readiness, and result types for rooted publication."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import json
import re

from process.provider_directory_dataset_scoped_publication import (
    exact_dataset_variant,
    LEGACY_PRACTITIONER_VARIANT,
    ROOTED_COMBINED_VARIANT,
)
from process.provider_directory_rooted_graph_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ROOT_PUBLICATION_BY_VARIANT,
)
from process.provider_directory_rooted_graph_source_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
)
from process.provider_directory_rooted_graph_store_contract import (
    ACQUISITION_PATTERN,
)
from process.provider_directory_rooted_graph_twin_contract import (
    ADMISSION_PATTERN,
)
from process.uhc_flex_official_cohort_contract import (
    UHC_FLEX_OFFICIAL_AUTHORITY_ID,
)


PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_CONTRACT_ID = (
    "healthporta.provider-directory.rooted-graph-publication.v1"
)
PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_ROOT_CONTRACT_ID = (
    "healthporta.provider-directory.rooted-graph-dataset-root.v1"
)
PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_KIND = ROOTED_COMBINED_VARIANT
PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_RESOURCES = (
    "InsurancePlan",
    "PractitionerRole",
    "Practitioner",
    "Organization",
    "Location",
    "HealthcareService",
    "OrganizationAffiliation",
    "Endpoint",
)
PROVIDER_DIRECTORY_ROOTED_GRAPH_OUTPUT_RESOURCES = tuple(
    resource_type
    for resource_type in PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_RESOURCES
    if resource_type != "Practitioner"
)

DATASET_PATTERN = re.compile(r"pdrgpd_[0-9a-f]{48}\Z")
DATASET_ROOT_PATTERN = re.compile(r"pdrgpr_[0-9a-f]{48}\Z")
HASH_PATTERN = re.compile(r"[0-9a-f]{64}\Z")


def canonical_json(document: object) -> str:
    """Serialize one publication proof with stable ordering and strict numbers."""

    try:
        return json.dumps(
            document,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (MemoryError, OverflowError, RecursionError, TypeError, ValueError):
        raise ProviderDirectoryRootedGraphPublicationError("content") from None


class ProviderDirectoryRootedGraphPublicationError(RuntimeError):
    """Expose a bounded publication failure without retained FHIR payloads."""

    def __init__(self, code: str = "state") -> None:
        message_by_code = {
            "admission": "rooted graph publication admission is invalid",
            "content": "rooted graph publication content is invalid",
            "foreign_current": "rooted graph publication current dataset is invalid",
            "replay": "rooted graph publication replay is not ready",
            "source_drift": "rooted graph publication source has drifted",
            "state": "rooted graph publication state is invalid",
        }
        self.code = code if code in message_by_code else "state"
        super().__init__(message_by_code[self.code])


@dataclass(frozen=True, slots=True)
class ProviderDirectoryRootedGraphDatasetReadiness:
    dataset_id: str
    previous_dataset_id: str | None
    admission_id: str
    publication_acquisition_id: str
    acquisition_root_run_id: str
    source_id: str
    endpoint_id: str
    source_authority_id: str
    root_dataset_variant: str
    root_publication_contract_id: str
    root_dataset_id: str
    root_dataset_hash: str
    root_content_proof_sha256: str
    root_cohort_id: str
    practitioner_resource_count: int
    semantic_projection_as_of: str
    operation_key: str
    dataset_hash: str
    resource_count: int
    resource_counts: dict[str, int]
    publication_kind: str
    cohort_complete: bool
    retry_exhausted_count: int
    rooted_graph_complete: bool
    endpoint_collection_complete: bool
    endpoint_complete: bool

    def __post_init__(self) -> None:
        counts = self.resource_counts
        try:
            projection = date.fromisoformat(self.semantic_projection_as_of)
        except (TypeError, ValueError):
            projection = None
        if (
            type(self.dataset_id) is not str
            or DATASET_PATTERN.fullmatch(self.dataset_id) is None
            or self.previous_dataset_id != self.root_dataset_id
            or exact_dataset_variant(self.previous_dataset_id)
            != self.root_dataset_variant
            or type(self.admission_id) is not str
            or ADMISSION_PATTERN.fullmatch(self.admission_id) is None
            or type(self.publication_acquisition_id) is not str
            or ACQUISITION_PATTERN.fullmatch(self.publication_acquisition_id) is None
            or type(self.acquisition_root_run_id) is not str
            or DATASET_ROOT_PATTERN.fullmatch(self.acquisition_root_run_id) is None
            or self.source_id != PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID
            or self.endpoint_id != PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID
            or self.source_authority_id != UHC_FLEX_OFFICIAL_AUTHORITY_ID
            or self.root_dataset_variant
            not in {LEGACY_PRACTITIONER_VARIANT, ROOTED_COMBINED_VARIANT}
            or PROVIDER_DIRECTORY_ROOTED_GRAPH_ROOT_PUBLICATION_BY_VARIANT.get(
                self.root_dataset_variant
            )
            != self.root_publication_contract_id
            or type(self.root_dataset_hash) is not str
            or HASH_PATTERN.fullmatch(self.root_dataset_hash) is None
            or type(self.root_content_proof_sha256) is not str
            or HASH_PATTERN.fullmatch(self.root_content_proof_sha256) is None
            or type(self.root_cohort_id) is not str
            or not self.root_cohort_id
            or len(self.root_cohort_id) > 128
            or type(self.practitioner_resource_count) is not int
            or self.practitioner_resource_count < 1
            or projection is None
            or type(self.operation_key) is not str
            or HASH_PATTERN.fullmatch(self.operation_key) is None
            or type(self.dataset_hash) is not str
            or HASH_PATTERN.fullmatch(self.dataset_hash) is None
            or type(self.resource_count) is not int
            or self.resource_count < self.practitioner_resource_count
            or type(counts) is not dict
            or set(counts) != set(PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_RESOURCES)
            or any(type(count) is not int or count < 0 for count in counts.values())
            or counts.get("Practitioner") != self.practitioner_resource_count
            or sum(counts.values()) != self.resource_count
            or self.publication_kind != PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_KIND
            or type(self.retry_exhausted_count) is not int
            or self.retry_exhausted_count < 0
            or self.cohort_complete is not (self.retry_exhausted_count == 0)
            or self.rooted_graph_complete is not True
            or self.endpoint_collection_complete is not False
            or self.endpoint_complete is not False
        ):
            raise ValueError(
                "provider_directory_rooted_graph_dataset_readiness_invalid"
            )


@dataclass(frozen=True, slots=True)
class ProviderDirectoryRootedGraphPublicationResult:
    readiness: ProviderDirectoryRootedGraphDatasetReadiness
    replayed: bool

    def __post_init__(self) -> None:
        if (
            type(self.readiness) is not ProviderDirectoryRootedGraphDatasetReadiness
            or type(self.replayed) is not bool
        ):
            raise ValueError(
                "provider_directory_rooted_graph_publication_result_invalid"
            )


__all__ = (
    "canonical_json",
    "ProviderDirectoryRootedGraphDatasetReadiness",
    "ProviderDirectoryRootedGraphPublicationError",
    "ProviderDirectoryRootedGraphPublicationResult",
    "DATASET_PATTERN",
    "DATASET_ROOT_PATTERN",
    "HASH_PATTERN",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_RESOURCES",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_ROOT_CONTRACT_ID",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_OUTPUT_RESOURCES",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_CONTRACT_ID",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_KIND",
)
