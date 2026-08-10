# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed identity and publication contract for Profile dataset variants."""

from __future__ import annotations

import json
import re
from typing import Any, Mapping

from process.provider_directory_dataset_scoped_publication import (
    exact_dataset_variant,
    LEGACY_PRACTITIONER_VARIANT,
    ROOTED_COMBINED_VARIANT,
)
from process.provider_directory_resource_hash import (
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
)
from process.provider_directory_rooted_graph_publication import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_RESOURCES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_CONTRACT_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_KIND,
)
from process.provider_directory_rooted_graph_source_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
)
from process.uhc_flex_official_cohort_contract import (
    UHC_FLEX_OFFICIAL_AUTHORITY_ID,
    UHC_FLEX_OFFICIAL_RESOURCE_TYPE,
)
from process.uhc_flex_practitioner_contract import UHC_FLEX_PRACTITIONER_SOURCE_ID
from process.uhc_flex_practitioner_publication import (
    UHC_FLEX_PRACTITIONER_DATASET_PUBLICATION_CONTRACT_ID,
)
from process.uhc_flex_practitioner_registration import (
    uhc_flex_practitioner_endpoint_identity,
)


UHC_FLEX_PROFILE_SELECTION_LOCK_RELATIONS = (
    "provider_directory_dataset_affiliation_organization",
    "provider_directory_dataset_insurance_plan",
    "provider_directory_dataset_network_plan",
    "provider_directory_dataset_resource",
    "provider_directory_rooted_graph_acquisition",
    "provider_directory_rooted_graph_dataset",
    "provider_directory_rooted_graph_dataset_resource",
    "provider_directory_rooted_graph_edge",
    "provider_directory_rooted_graph_resource",
    "provider_directory_rooted_graph_twin_admission",
    "provider_directory_rooted_graph_twin_attempt",
    "provider_directory_rooted_graph_work",
    "provider_directory_uhc_flex_npi_cohort",
    "provider_directory_uhc_flex_practitioner_acquisition",
    "provider_directory_uhc_flex_practitioner_dataset",
    "provider_directory_uhc_flex_practitioner_dataset_resource",
    "provider_directory_uhc_flex_practitioner_resource",
    "provider_directory_uhc_flex_practitioner_twin_admission",
    "provider_directory_uhc_flex_practitioner_work",
)
UHC_FLEX_LEGACY_PROFILE_VARIANT = LEGACY_PRACTITIONER_VARIANT
UHC_FLEX_ROOTED_PROFILE_VARIANT = ROOTED_COMBINED_VARIANT
UHC_FLEX_LEGACY_PROFILE_RESOURCES = (UHC_FLEX_OFFICIAL_RESOURCE_TYPE,)
UHC_FLEX_ROOTED_PROFILE_RESOURCES = PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_RESOURCES
_UHC_FLEX_PRACTITIONER_ENDPOINT_ID = (
    uhc_flex_practitioner_endpoint_identity().endpoint_id
)
_UHC_FLEX_ROOTED_PROFILE_RESOURCE_SET = frozenset(UHC_FLEX_ROOTED_PROFILE_RESOURCES)


def uhc_flex_profile_dataset_variant(dataset_id: object) -> str | None:
    """Classify only an exact legacy or rooted combined dataset identity."""

    return exact_dataset_variant(dataset_id)


def uhc_flex_profile_source_variant(source_id: object) -> str | None:
    """Classify only one reviewed source in the logical generation group."""

    if source_id == UHC_FLEX_PRACTITIONER_SOURCE_ID:
        return LEGACY_PRACTITIONER_VARIANT
    if source_id == PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID:
        return ROOTED_COMBINED_VARIANT
    return None


def is_uhc_flex_profile_source(source_id: object) -> bool:
    """Return whether a source owns one reviewed Profile dataset variant."""

    return uhc_flex_profile_source_variant(source_id) is not None


def uhc_flex_profile_expected_resources(
    dataset_id: object,
) -> tuple[str, ...] | None:
    """Return the exact retained families for a recognized dataset variant."""

    variant = uhc_flex_profile_dataset_variant(dataset_id)
    if variant == LEGACY_PRACTITIONER_VARIANT:
        return UHC_FLEX_LEGACY_PROFILE_RESOURCES
    if variant == ROOTED_COMBINED_VARIANT:
        return UHC_FLEX_ROOTED_PROFILE_RESOURCES
    return None


def is_uhc_flex_dataset_variant_matching(
    source_id: object,
    dataset_id: object,
) -> bool:
    """Bind each reviewed alternative source to exactly one dataset family."""

    variant = uhc_flex_profile_dataset_variant(dataset_id)
    return bool(
        (
            source_id == UHC_FLEX_PRACTITIONER_SOURCE_ID
            and variant == LEGACY_PRACTITIONER_VARIANT
        )
        or (
            source_id == PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID
            and variant == ROOTED_COMBINED_VARIANT
        )
    )


def _clean_text(raw_value: Any) -> str | None:
    return (
        raw_value.strip() if isinstance(raw_value, str) and raw_value.strip() else None
    )


def _json_object(raw_value: Any) -> dict[str, Any]:
    decoded_value = raw_value
    if isinstance(decoded_value, str):
        try:
            decoded_value = json.loads(decoded_value)
        except (TypeError, ValueError):
            return {}
    return dict(decoded_value) if isinstance(decoded_value, Mapping) else {}


def _is_rooted_publication_lineage_valid(
    publication_metadata: Mapping[str, Any],
) -> bool:
    root_variant = publication_metadata.get("root_variant")
    root_source_id = _clean_text(publication_metadata.get("root_source_id"))
    root_endpoint_id = _clean_text(publication_metadata.get("root_endpoint_id"))
    acquisition_source_id = _clean_text(
        publication_metadata.get("acquisition_source_id")
    )
    acquisition_endpoint_id = _clean_text(
        publication_metadata.get("acquisition_endpoint_id")
    )
    has_same_root_pair = bool(
        root_source_id
        and root_endpoint_id
        and root_source_id == acquisition_source_id
        and root_endpoint_id == acquisition_endpoint_id
    )
    exact_root_pair = (root_source_id, root_endpoint_id)
    expected_root_pair = (
        (PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID, acquisition_endpoint_id)
        if root_variant == ROOTED_COMBINED_VARIANT
        else (
            UHC_FLEX_PRACTITIONER_SOURCE_ID,
            _UHC_FLEX_PRACTITIONER_ENDPOINT_ID,
        )
    )
    return bool(
        root_variant in {LEGACY_PRACTITIONER_VARIANT, ROOTED_COMBINED_VARIANT}
        and acquisition_source_id == PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID
        and acquisition_endpoint_id == publication_metadata.get("endpoint_id")
        and has_same_root_pair == (root_variant == ROOTED_COMBINED_VARIANT)
        and exact_root_pair == expected_root_pair
        and _clean_text(publication_metadata.get("practitioner_origin_source_id"))
        == UHC_FLEX_PRACTITIONER_SOURCE_ID
        and _clean_text(publication_metadata.get("practitioner_origin_endpoint_id"))
        == _UHC_FLEX_PRACTITIONER_ENDPOINT_ID
    )


def _is_rooted_resource_counts_valid(resource_counts: object) -> bool:
    """Accept an exact family mapping independent of canonical JSON key order."""

    if not isinstance(resource_counts, Mapping):
        return False
    try:
        resource_type_set = frozenset(resource_counts)
    except TypeError:
        return False
    return bool(
        resource_type_set == _UHC_FLEX_ROOTED_PROFILE_RESOURCE_SET
        and all(
            type(resource_counts[resource_type]) is int
            and resource_counts[resource_type] >= 0
            for resource_type in UHC_FLEX_ROOTED_PROFILE_RESOURCES
        )
    )


def is_uhc_flex_publication_metadata_valid(
    publication_metadata: Mapping[str, Any],
    *,
    dataset_id: str,
    endpoint_id: str,
    evidence_run_id: str,
) -> bool:
    """Return whether closed publication metadata binds the exact Profile row."""

    source_id = publication_metadata.get("source_id")
    variant = uhc_flex_profile_dataset_variant(dataset_id)
    if not is_uhc_flex_dataset_variant_matching(source_id, dataset_id):
        return False
    expected_resources = uhc_flex_profile_expected_resources(dataset_id)
    is_common_metadata_valid = bool(
        publication_metadata.get("dataset_id") == dataset_id
        and publication_metadata.get("endpoint_id") == endpoint_id
        and publication_metadata.get("acquisition_root_run_id") == evidence_run_id
        and publication_metadata.get("source_ids") == [source_id]
        and publication_metadata.get("source_authority_id")
        == UHC_FLEX_OFFICIAL_AUTHORITY_ID
        and publication_metadata.get("selected_resources")
        == list(expected_resources or ())
        and publication_metadata.get("expected_resources")
        == list(expected_resources or ())
        and publication_metadata.get("resource_hash_contract")
        == SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
        and publication_metadata.get("cohort_complete") is True
        and publication_metadata.get("endpoint_collection_complete") is False
        and publication_metadata.get("endpoint_complete") is False
        and _clean_text(publication_metadata.get("semantic_projection_as_of"))
        is not None
        and _clean_text(publication_metadata.get("admission_id")) is not None
        and re.fullmatch(
            r"[0-9a-f]{64}",
            _clean_text(publication_metadata.get("operation_key")) or "",
        )
        is not None
    )
    if not is_common_metadata_valid:
        return False
    if variant == LEGACY_PRACTITIONER_VARIANT:
        return (
            publication_metadata.get("publication_contract_id")
            == UHC_FLEX_PRACTITIONER_DATASET_PUBLICATION_CONTRACT_ID
        )
    resource_counts = publication_metadata.get("resource_counts")
    return bool(
        variant == ROOTED_COMBINED_VARIANT
        and publication_metadata.get("publication_contract_id")
        == PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_CONTRACT_ID
        and publication_metadata.get("publication_kind")
        == PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_KIND
        and publication_metadata.get("rooted_graph_complete") is True
        and _is_rooted_resource_counts_valid(resource_counts)
        and _is_rooted_publication_lineage_valid(publication_metadata)
    )


__all__ = (
    "is_uhc_flex_dataset_variant_matching",
    "is_uhc_flex_profile_source",
    "is_uhc_flex_publication_metadata_valid",
    "uhc_flex_profile_dataset_variant",
    "uhc_flex_profile_expected_resources",
    "uhc_flex_profile_source_variant",
    "UHC_FLEX_LEGACY_PROFILE_RESOURCES",
    "UHC_FLEX_LEGACY_PROFILE_VARIANT",
    "UHC_FLEX_PROFILE_SELECTION_LOCK_RELATIONS",
    "UHC_FLEX_ROOTED_PROFILE_RESOURCES",
    "UHC_FLEX_ROOTED_PROFILE_VARIANT",
)
