# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure closed contract for exact Provider Directory current datasets."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import re

from process.provider_directory_rooted_graph_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_COMBINED_ROOT_VARIANT,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_LEGACY_ROOT_VARIANT,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ROOT_PUBLICATION_BY_VARIANT,
)
from process.provider_directory_rooted_graph_source_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
)
from process.uhc_flex_official_cohort_contract import (
    UHC_FLEX_OFFICIAL_AUTHORITY_ID,
)
from process.uhc_flex_practitioner_contract import (
    UHC_FLEX_PRACTITIONER_PUBLICATION_LOCK_IDENTITY,
    UHC_FLEX_PRACTITIONER_SOURCE_ID,
)
from process.uhc_flex_practitioner_registration import (
    uhc_flex_practitioner_endpoint_identity,
)


LEGACY_PRACTITIONER_VARIANT = PROVIDER_DIRECTORY_ROOTED_GRAPH_LEGACY_ROOT_VARIANT
ROOTED_COMBINED_VARIANT = PROVIDER_DIRECTORY_ROOTED_GRAPH_COMBINED_ROOT_VARIANT
EXACT_DATASET_VARIANTS = frozenset(
    {LEGACY_PRACTITIONER_VARIANT, ROOTED_COMBINED_VARIANT}
)
if (
    frozenset(PROVIDER_DIRECTORY_ROOTED_GRAPH_ROOT_PUBLICATION_BY_VARIANT)
    != EXACT_DATASET_VARIANTS
):
    raise RuntimeError("provider_directory_exact_variant_contract_invalid")

EXACT_DATASET_PUBLICATION_LOCK_IDENTITY = (
    UHC_FLEX_PRACTITIONER_PUBLICATION_LOCK_IDENTITY
)
LEGACY_DATASET_PATTERN = re.compile(r"pdufpd_[0-9a-f]{48}\Z")
ROOTED_DATASET_PATTERN = re.compile(r"pdrgpd_[0-9a-f]{48}\Z")
_LEGACY_ROOT_RUN_PATTERN = re.compile(r"pdufpar_[0-9a-f]{48}\Z")
_ROOTED_ROOT_RUN_PATTERN = re.compile(r"pdrgpr_[0-9a-f]{48}\Z")
_HASH_PATTERN = re.compile(r"[0-9a-f]{64}\Z")


class ProviderDirectoryDatasetScopedPublicationError(RuntimeError):
    """Expose a bounded exact-current failure without resource payloads."""

    def __init__(self, code: str = "state") -> None:
        message_by_code = {
            "foreign_current": (
                "provider directory current dataset variant is unsupported"
            ),
            "both_current": (
                "provider directory exact dataset variants are both current"
            ),
            "source_drift": "provider directory publication source has drifted",
            "state": "provider directory dataset publication state is invalid",
        }
        self.code = code if code in message_by_code else "state"
        super().__init__(message_by_code[self.code])


def exact_dataset_variant(dataset_id: object) -> str | None:
    """Return one recognized dedicated dataset family, never a generic guess."""

    if type(dataset_id) is not str:
        return None
    if LEGACY_DATASET_PATTERN.fullmatch(dataset_id) is not None:
        return LEGACY_PRACTITIONER_VARIANT
    if ROOTED_DATASET_PATTERN.fullmatch(dataset_id) is not None:
        return ROOTED_COMBINED_VARIANT
    return None


def _has_valid_exact_current_types(candidate: object) -> bool:
    text_fields = (
        "dataset_id",
        "endpoint_id",
        "source_id",
        "root_source_id",
        "root_endpoint_id",
        "acquisition_source_id",
        "acquisition_endpoint_id",
        "practitioner_origin_source_id",
        "practitioner_origin_endpoint_id",
        "source_authority_id",
        "endpoint_signature_sha256",
        "dataset_hash",
        "root_content_proof_sha256",
        "root_cohort_id",
        "semantic_projection_as_of",
        "operation_key",
        "acquisition_root_run_id",
        "variant",
        "root_publication_contract_id",
    )
    return bool(
        all(
            type(getattr(candidate, field_name, None)) is str
            for field_name in text_fields
        )
        and type(getattr(candidate, "resource_count", None)) is int
        and type(getattr(candidate, "practitioner_resource_count", None)) is int
        and getattr(candidate, "variant", None) in EXACT_DATASET_VARIANTS
    )


def _has_valid_exact_current_coordinates(candidate: object) -> bool:
    pair = exact_uhc_dataset_pair()
    is_legacy_variant = candidate.variant == LEGACY_PRACTITIONER_VARIANT
    expected_source_id = (
        pair.legacy_source_id if is_legacy_variant else pair.rooted_source_id
    )
    expected_endpoint_id = (
        pair.legacy_endpoint_id if is_legacy_variant else pair.rooted_endpoint_id
    )
    has_valid_lineage_relation = (
        candidate.root_source_id != candidate.acquisition_source_id
        and candidate.root_endpoint_id != candidate.acquisition_endpoint_id
        if is_legacy_variant
        else candidate.root_source_id == candidate.acquisition_source_id
        and candidate.root_endpoint_id == candidate.acquisition_endpoint_id
    )
    return bool(
        candidate.source_id == expected_source_id
        and candidate.endpoint_id == expected_endpoint_id
        and candidate.root_source_id == expected_source_id
        and candidate.root_endpoint_id == expected_endpoint_id
        and candidate.acquisition_source_id == pair.rooted_source_id
        and candidate.acquisition_endpoint_id == pair.rooted_endpoint_id
        and candidate.practitioner_origin_source_id == pair.legacy_source_id
        and candidate.practitioner_origin_endpoint_id == pair.legacy_endpoint_id
        and has_valid_lineage_relation
    )


def _has_valid_exact_current_content(candidate: object) -> bool:
    run_pattern = (
        _LEGACY_ROOT_RUN_PATTERN
        if candidate.variant == LEGACY_PRACTITIONER_VARIANT
        else _ROOTED_ROOT_RUN_PATTERN
    )
    identifiers = (
        candidate.endpoint_id,
        candidate.root_endpoint_id,
        candidate.acquisition_endpoint_id,
        candidate.practitioner_origin_endpoint_id,
        candidate.endpoint_signature_sha256,
        candidate.dataset_hash,
        candidate.root_content_proof_sha256,
        candidate.operation_key,
    )
    try:
        date.fromisoformat(candidate.semantic_projection_as_of)
    except ValueError:
        return False
    return bool(
        exact_dataset_variant(candidate.dataset_id) == candidate.variant
        and candidate.root_publication_contract_id
        == PROVIDER_DIRECTORY_ROOTED_GRAPH_ROOT_PUBLICATION_BY_VARIANT[
            candidate.variant
        ]
        and candidate.source_authority_id == UHC_FLEX_OFFICIAL_AUTHORITY_ID
        and candidate.endpoint_signature_sha256
        == PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256
        and all(
            _HASH_PATTERN.fullmatch(identifier) is not None
            for identifier in identifiers
        )
        and all(
            1 <= len(identifier) <= 64
            for identifier in (
                candidate.source_id,
                candidate.root_source_id,
                candidate.acquisition_source_id,
                candidate.practitioner_origin_source_id,
                candidate.source_authority_id,
            )
        )
        and 0 <= candidate.practitioner_resource_count <= candidate.resource_count
        and candidate.practitioner_resource_count >= 1
        and 1 <= len(candidate.root_cohort_id) <= 128
        and run_pattern.fullmatch(candidate.acquisition_root_run_id) is not None
    )


@dataclass(frozen=True, slots=True)
class ExactCurrentDataset:
    """One locked generic current row backed by one exact known variant."""

    dataset_id: str
    endpoint_id: str
    source_id: str
    root_source_id: str
    root_endpoint_id: str
    acquisition_source_id: str
    acquisition_endpoint_id: str
    practitioner_origin_source_id: str
    practitioner_origin_endpoint_id: str
    source_authority_id: str
    endpoint_signature_sha256: str
    dataset_hash: str
    resource_count: int
    practitioner_resource_count: int
    root_content_proof_sha256: str
    root_cohort_id: str
    semantic_projection_as_of: str
    operation_key: str
    acquisition_root_run_id: str
    variant: str
    root_publication_contract_id: str

    def __post_init__(self) -> None:
        """Reject nominal instances that do not match the closed exact pair."""

        if (
            not _has_valid_exact_current_types(self)
            or not _has_valid_exact_current_coordinates(self)
            or not _has_valid_exact_current_content(self)
        ):
            raise ValueError("provider_directory_exact_current_dataset_invalid")


@dataclass(frozen=True, slots=True)
class ExactDatasetPair:
    """The only reviewed legacy/rooted logical-current coordinate pair."""

    legacy_source_id: str
    legacy_endpoint_id: str
    rooted_source_id: str
    rooted_endpoint_id: str

    def __post_init__(self) -> None:
        if (
            any(
                type(value) is not str or not value or len(value) > 64
                for value in (self.legacy_source_id, self.rooted_source_id)
            )
            or any(
                type(value) is not str or _HASH_PATTERN.fullmatch(value) is None
                for value in (self.legacy_endpoint_id, self.rooted_endpoint_id)
            )
            or self.legacy_source_id == self.rooted_source_id
            or self.legacy_endpoint_id == self.rooted_endpoint_id
        ):
            raise ValueError("provider_directory_exact_dataset_pair_invalid")


def exact_uhc_dataset_pair() -> ExactDatasetPair:
    """Return fresh reviewed coordinates without mutable registry rows."""

    return ExactDatasetPair(
        legacy_source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID,
        legacy_endpoint_id=uhc_flex_practitioner_endpoint_identity().endpoint_id,
        rooted_source_id=PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
        rooted_endpoint_id=PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID,
    )


def is_exact_current_root_match(
    current: ExactCurrentDataset | None,
    identity: object,
) -> bool:
    """Compare every mutable-current coordinate used by a rooted acquisition."""

    return bool(
        type(current) is ExactCurrentDataset
        and current.variant == getattr(identity, "root_dataset_variant", None)
        and current.root_publication_contract_id
        == getattr(identity, "root_publication_contract_id", None)
        and current.root_source_id == getattr(identity, "root_source_id", None)
        and current.root_endpoint_id == getattr(identity, "root_endpoint_id", None)
        and current.acquisition_source_id
        == getattr(identity, "acquisition_source_id", None)
        and current.acquisition_endpoint_id
        == getattr(identity, "acquisition_endpoint_id", None)
        and current.source_authority_id
        == getattr(identity, "source_authority_id", None)
        and current.endpoint_signature_sha256
        == getattr(identity, "endpoint_signature_sha256", None)
        and current.dataset_id == getattr(identity, "root_dataset_id", None)
        and current.dataset_hash == getattr(identity, "root_dataset_hash", None)
        and current.root_content_proof_sha256
        == getattr(identity, "root_content_proof_sha256", None)
        and current.root_cohort_id == getattr(identity, "root_cohort_id", None)
        and current.practitioner_resource_count
        == getattr(identity, "root_resource_count", None)
    )


exact_current_matches_root = is_exact_current_root_match


__all__ = (
    "exact_dataset_variant",
    "exact_current_matches_root",
    "exact_uhc_dataset_pair",
    "ExactCurrentDataset",
    "ExactDatasetPair",
    "EXACT_DATASET_PUBLICATION_LOCK_IDENTITY",
    "LEGACY_DATASET_PATTERN",
    "LEGACY_PRACTITIONER_VARIANT",
    "ProviderDirectoryDatasetScopedPublicationError",
    "ROOTED_COMBINED_VARIANT",
    "ROOTED_DATASET_PATTERN",
)
