# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Deterministic metadata projection for reviewed manual FHIR seeds."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from process.provider_directory_fhir_census_contract import (
    CURRENT_VERSION_CENSUS_CANONICALIZATION_VERSION_FIELD,
    CURRENT_VERSION_CENSUS_COMPLETION_SCOPES_FIELD,
    CURRENT_VERSION_CENSUS_CONTINUATION_STRATEGY_FIELD,
    CURRENT_VERSION_CENSUS_CONTRACT_VERSION_FIELD,
    CURRENT_VERSION_CENSUS_METADATA_STRATEGY_FIELD,
    CURRENT_VERSION_CENSUS_PAGE_COUNT_FIELD,
    CURRENT_VERSION_CENSUS_START_URLS_FIELD,
    CURRENT_VERSION_CENSUS_STRATEGY_VERSION_FIELD,
    CURRENT_VERSION_CENSUS_TRAVERSAL_VERSION_FIELD,
    SERVER_ISSUED_SUBSET_SEMANTICS,
)
from process.provider_directory_fhir_root_policy import (
    POLICY_PENDING_STATUS,
    REVIEWED_ROOT_POLICY_METADATA_KEY,
    ReviewedRootPolicy,
)


MANUAL_SOURCE_PENDING_STATUS = "pending_two_matching_reviewed_subset_acquisitions"
MANUAL_SOURCE_VERIFICATION_CAMPAIGN_FIELD = (
    "provider_directory_verification_campaign_id"
)


def manual_seed_metadata(
    entry: Mapping[str, Any],
    resources: tuple[str, ...],
    page_count: int,
    canonical_base: str,
    root_policy: ReviewedRootPolicy | None,
) -> dict[str, Any]:
    """Project the reviewed subset identity into dormant source metadata."""

    metadata_by_field = {
        "provider_directory_override": "reviewed_manual_current_version_census",
        "provider_directory_manual_only": True,
        "provider_directory_confirmed_base": canonical_base,
        "provider_directory_confirmed_metadata_url": f"{canonical_base}/metadata",
        "provider_directory_confirmed_catalog_url": canonical_base,
        "provider_directory_supported_resources": list(resources),
        "provider_directory_fully_enumerable_resources": [],
        "provider_directory_server_issued_subset_resources": list(resources),
        "provider_directory_expected_nonempty_resources": list(
            entry["expected_nonempty_resources"]
        ),
        "provider_directory_resource_page_count_caps": {
            resource_type: page_count for resource_type in resources
        },
        "provider_directory_coverage_mode": SERVER_ISSUED_SUBSET_SEMANTICS,
        "provider_directory_acquisition_enabled": True,
        "provider_directory_candidate_status": (
            POLICY_PENDING_STATUS
            if root_policy is not None
            else MANUAL_SOURCE_PENDING_STATUS
        ),
        MANUAL_SOURCE_VERIFICATION_CAMPAIGN_FIELD: entry[
            "verification_campaign_id"
        ],
        CURRENT_VERSION_CENSUS_METADATA_STRATEGY_FIELD: (
            SERVER_ISSUED_SUBSET_SEMANTICS
        ),
        CURRENT_VERSION_CENSUS_CONTRACT_VERSION_FIELD: entry["contract_version"],
        CURRENT_VERSION_CENSUS_PAGE_COUNT_FIELD: page_count,
        CURRENT_VERSION_CENSUS_STRATEGY_VERSION_FIELD: entry["strategy_version"],
        CURRENT_VERSION_CENSUS_TRAVERSAL_VERSION_FIELD: entry["traversal_version"],
        CURRENT_VERSION_CENSUS_CANONICALIZATION_VERSION_FIELD: entry[
            "canonicalization_version"
        ],
        CURRENT_VERSION_CENSUS_COMPLETION_SCOPES_FIELD: list(
            entry["completion_scopes"]
        ),
        CURRENT_VERSION_CENSUS_CONTINUATION_STRATEGY_FIELD: entry[
            "continuation_strategy"
        ],
        CURRENT_VERSION_CENSUS_START_URLS_FIELD: dict(entry["start_urls"]),
    }
    if root_policy is not None:
        metadata_by_field[REVIEWED_ROOT_POLICY_METADATA_KEY] = (
            root_policy.document()
        )
    return metadata_by_field
