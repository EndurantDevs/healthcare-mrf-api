# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Reviewed neutral identity for server-issued FHIR subset traversal."""

from __future__ import annotations

from typing import Any, Mapping

CURRENT_VERSION_CENSUS_CONTRACT_FIELD = (
    "_provider_directory_current_version_census_contract"
)
CURRENT_VERSION_CENSUS_METADATA_STRATEGY_FIELD = (
    "provider_directory_current_version_census_strategy"
)
CURRENT_VERSION_CENSUS_START_URLS_FIELD = (
    "provider_directory_current_version_census_start_urls"
)
CURRENT_VERSION_CENSUS_CONTINUATION_STRATEGY_FIELD = (
    "provider_directory_current_version_census_continuation_strategy"
)
CURRENT_VERSION_CENSUS_CONTRACT_VERSION_FIELD = (
    "provider_directory_current_version_census_contract_version"
)
CURRENT_VERSION_CENSUS_PAGE_COUNT_FIELD = (
    "provider_directory_current_version_census_page_count"
)
CURRENT_VERSION_CENSUS_STRATEGY_VERSION_FIELD = (
    "provider_directory_current_version_census_strategy_version"
)
CURRENT_VERSION_CENSUS_TRAVERSAL_VERSION_FIELD = (
    "provider_directory_current_version_census_traversal_version"
)
CURRENT_VERSION_CENSUS_CANONICALIZATION_VERSION_FIELD = (
    "provider_directory_current_version_census_canonicalization_version"
)
CURRENT_VERSION_CENSUS_COMPLETION_SCOPES_FIELD = (
    "provider_directory_current_version_census_completion_scopes"
)

SERVER_ISSUED_SUBSET_SMILE_CONTINUATION_STRATEGY = (
    "smile-opaque-logical-offset-v3"
)
SERVER_ISSUED_SUBSET_STRATEGY_VERSION = (
    "provider-directory-fhir-server-issued-traversal-subset-v3"
)
SERVER_ISSUED_SUBSET_TRAVERSAL_VERSION = (
    "provider-directory-fhir-smile-logical-offset-v3"
)
SERVER_ISSUED_SUBSET_CANONICALIZATION_VERSION = (
    "provider-directory-fhir-returned-resource-json-v2"
)
SERVER_ISSUED_SUBSET_COMPLETION_SCOPES = (
    "advertised-count-stability",
    "source-issued-continuation",
    "returned-resource-content",
)
SERVER_ISSUED_SUBSET_SEMANTICS = "server-issued-traversal-subset"
SERVER_ISSUED_SUBSET_CAMPAIGN_FIELD = (
    "provider_directory_verification_campaign_id"
)
SERVER_ISSUED_SUBSET_RESOURCE_TYPES = (
    "HealthcareService",
    "InsurancePlan",
    "Location",
    "Organization",
    "OrganizationAffiliation",
    "Practitioner",
    "PractitionerRole",
)
SERVER_ISSUED_SUBSET_SOURCE_SCOPE_VERSION = (
    "provider-directory-fhir-server-issued-subset-source-scope-v1"
)
SERVER_ISSUED_SUBSET_ACTIVATION_SOURCE_CONTRACT_VERSION = (
    "provider-directory-fhir-reviewed-subset-source-contract-v1"
)
SERVER_ISSUED_SUBSET_SOURCE_SCOPE_METADATA_FIELDS = (
    "provider_directory_supported_resources",
    "provider_directory_fully_enumerable_resources",
    "provider_directory_expected_nonempty_resources",
    "provider_directory_resource_page_count_caps",
    "provider_directory_acquisition_enabled",
    "provider_directory_coverage_mode",
    "provider_directory_manual_only",
    "provider_directory_server_issued_subset_resources",
    CURRENT_VERSION_CENSUS_METADATA_STRATEGY_FIELD,
    CURRENT_VERSION_CENSUS_CONTRACT_VERSION_FIELD,
    CURRENT_VERSION_CENSUS_PAGE_COUNT_FIELD,
    CURRENT_VERSION_CENSUS_STRATEGY_VERSION_FIELD,
    CURRENT_VERSION_CENSUS_TRAVERSAL_VERSION_FIELD,
    CURRENT_VERSION_CENSUS_CANONICALIZATION_VERSION_FIELD,
    CURRENT_VERSION_CENSUS_COMPLETION_SCOPES_FIELD,
    CURRENT_VERSION_CENSUS_CONTINUATION_STRATEGY_FIELD,
    CURRENT_VERSION_CENSUS_START_URLS_FIELD,
    SERVER_ISSUED_SUBSET_CAMPAIGN_FIELD,
    "provider_directory_configured_endpoint_id",
)


def subset_activation_source_contract_payload(
    source_record: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the cutoff-neutral source identity used by activation evidence."""

    metadata = source_record.get("metadata_json")
    source_id = source_record.get("source_id")
    endpoint_id = source_record.get("endpoint_id")
    canonical_api_base = source_record.get("canonical_api_base")
    if (
        not isinstance(metadata, Mapping)
        or type(source_id) is not str
        or not source_id
        or type(endpoint_id) is not str
        or not endpoint_id
        or type(canonical_api_base) is not str
        or not canonical_api_base
    ):
        raise ValueError("provider_directory_subset_activation_source_invalid")
    return {
        "identity_version": (
            SERVER_ISSUED_SUBSET_ACTIVATION_SOURCE_CONTRACT_VERSION
        ),
        "source": {
            "source_id": source_id,
            "endpoint_id": endpoint_id,
            "canonical_api_base": canonical_api_base,
            "requires_registration": source_record.get(
                "requires_registration"
            ),
            "requires_api_key": source_record.get("requires_api_key"),
            "auth_type": source_record.get("auth_type"),
        },
        "metadata_identity": [
            [field_name, field_name in metadata, metadata.get(field_name)]
            for field_name in SERVER_ISSUED_SUBSET_SOURCE_SCOPE_METADATA_FIELDS
        ],
    }


def server_issued_subset_source_scope_payload(
    source_record: Mapping[str, Any],
    source_ids: tuple[str, ...],
    cutoff: str,
    canonical_api_base: str,
) -> dict[str, Any]:
    """Return the DB-reproducible reviewed source scope for subset v3."""

    metadata = source_record.get("metadata_json")
    if not isinstance(metadata, Mapping):
        raise ValueError("provider_directory_subset_source_scope_invalid")
    source_id = source_record.get("source_id")
    endpoint_id = source_record.get("endpoint_id")
    if (
        type(source_id) is not str
        or not source_id
        or type(endpoint_id) is not str
        or not endpoint_id
        or source_ids != (source_id,)
        or not cutoff
        or not canonical_api_base
    ):
        raise ValueError("provider_directory_subset_source_scope_invalid")
    return {
        "identity_version": SERVER_ISSUED_SUBSET_SOURCE_SCOPE_VERSION,
        "source_ids": list(source_ids),
        "cutoff": cutoff,
        "source": {
            "source_id": source_id,
            "endpoint_id": endpoint_id,
            "canonical_api_base": canonical_api_base,
            "requires_registration": source_record.get(
                "requires_registration"
            ),
            "requires_api_key": source_record.get("requires_api_key"),
            "auth_type": source_record.get("auth_type"),
        },
        "metadata_identity": [
            [field_name, field_name in metadata, metadata.get(field_name)]
            for field_name in SERVER_ISSUED_SUBSET_SOURCE_SCOPE_METADATA_FIELDS
        ],
    }


def is_reviewed_subset_contract(contract: Any) -> bool:
    """Return whether every fixed field matches the reviewed v3 identity."""

    resources = getattr(contract, "resources", ())
    start_urls = getattr(contract, "start_urls", ())
    page_count = getattr(contract, "page_count", None)
    campaign_id = getattr(contract, "campaign_id", None)
    return bool(
        getattr(contract, "contract_version", None) == 3
        and getattr(contract, "semantics", None)
        == SERVER_ISSUED_SUBSET_SEMANTICS
        and getattr(contract, "strategy_version", None)
        == SERVER_ISSUED_SUBSET_STRATEGY_VERSION
        and getattr(contract, "continuation_strategy", None)
        == SERVER_ISSUED_SUBSET_SMILE_CONTINUATION_STRATEGY
        and getattr(contract, "traversal_version", None)
        == SERVER_ISSUED_SUBSET_TRAVERSAL_VERSION
        and getattr(contract, "canonicalization_version", None)
        == SERVER_ISSUED_SUBSET_CANONICALIZATION_VERSION
        and getattr(contract, "completion_scopes", None)
        == SERVER_ISSUED_SUBSET_COMPLETION_SCOPES
        and len(resources) == len(SERVER_ISSUED_SUBSET_RESOURCE_TYPES)
        and set(resources) == set(SERVER_ISSUED_SUBSET_RESOURCE_TYPES)
        and len(start_urls) == len(SERVER_ISSUED_SUBSET_RESOURCE_TYPES)
        and set(dict(start_urls)) == set(SERVER_ISSUED_SUBSET_RESOURCE_TYPES)
        and type(page_count) is int
        and 1 <= page_count <= 1000
        and type(campaign_id) is str
        and bool(campaign_id)
    )


def validated_subset_identity_values(
    metadata: Mapping[str, Any],
) -> tuple[int, int, str, str, str, tuple[str, ...], str]:
    """Return fixed v3 fields only when the reviewed metadata is exact."""

    contract_version = metadata.get(CURRENT_VERSION_CENSUS_CONTRACT_VERSION_FIELD)
    page_count = metadata.get(CURRENT_VERSION_CENSUS_PAGE_COUNT_FIELD)
    strategy_version = metadata.get(CURRENT_VERSION_CENSUS_STRATEGY_VERSION_FIELD)
    traversal_version = metadata.get(CURRENT_VERSION_CENSUS_TRAVERSAL_VERSION_FIELD)
    canonicalization_version = metadata.get(
        CURRENT_VERSION_CENSUS_CANONICALIZATION_VERSION_FIELD
    )
    completion_scopes = metadata.get(CURRENT_VERSION_CENSUS_COMPLETION_SCOPES_FIELD)
    raw_campaign_id = metadata.get(SERVER_ISSUED_SUBSET_CAMPAIGN_FIELD)
    campaign_id = raw_campaign_id.strip() if isinstance(raw_campaign_id, str) else ""
    is_valid = bool(
        type(contract_version) is int
        and contract_version == 3
        and type(page_count) is int
        and 1 <= page_count <= 1000
        and strategy_version == SERVER_ISSUED_SUBSET_STRATEGY_VERSION
        and traversal_version == SERVER_ISSUED_SUBSET_TRAVERSAL_VERSION
        and canonicalization_version
        == SERVER_ISSUED_SUBSET_CANONICALIZATION_VERSION
        and type(completion_scopes) is list
        and tuple(completion_scopes) == SERVER_ISSUED_SUBSET_COMPLETION_SCOPES
        and campaign_id
    )
    if not is_valid:
        raise ValueError(
            "provider_directory_current_version_census_v3_identity_not_reviewed"
        )
    return (
        contract_version,
        page_count,
        strategy_version,
        traversal_version,
        canonicalization_version,
        tuple(completion_scopes),
        campaign_id,
    )
