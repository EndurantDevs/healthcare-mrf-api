# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Reviewed neutral identity for server-issued FHIR subset traversal."""

from __future__ import annotations

from typing import Any, Mapping

from process.provider_directory_fhir_root_policy import (
    REVIEWED_ROOT_POLICY_METADATA_KEY,
    reviewed_root_policy_from_document,
)

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
SERVER_ISSUED_SUBSET_EXACT_STRATEGY_VERSION = (
    "provider-directory-fhir-server-issued-traversal-subset-v3"
)
SERVER_ISSUED_SUBSET_STRATEGY_VERSION = (
    "provider-directory-fhir-server-issued-traversal-subset-v4"
)
SERVER_ISSUED_SUBSET_TRAVERSAL_VERSION = (
    "provider-directory-fhir-smile-logical-offset-v3"
)
SERVER_ISSUED_SUBSET_CANONICALIZATION_VERSION = (
    "provider-directory-fhir-returned-resource-json-v2"
)
SERVER_ISSUED_SUBSET_EXACT_COMPLETION_SCOPES = (
    "advertised-count-stability",
    "source-issued-continuation",
    "returned-resource-content",
)
SERVER_ISSUED_SUBSET_COMPLETION_SCOPES = (
    "advertised-count-monotone-decrease-at-most-one",
    "source-issued-continuation",
    "returned-resource-content",
)
SERVER_ISSUED_SUBSET_MAX_ADVERTISED_COUNT_DECREASE = 1
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
SERVER_ISSUED_SUBSET_SOURCE_SCOPE_VERSION_V2 = (
    "provider-directory-fhir-server-issued-subset-source-scope-v2"
)
SERVER_ISSUED_SUBSET_ACTIVATION_SOURCE_CONTRACT_VERSION = (
    "provider-directory-fhir-reviewed-subset-source-contract-v1"
)
SERVER_ISSUED_SUBSET_ACTIVATION_SOURCE_CONTRACT_VERSION_V2 = (
    "provider-directory-fhir-reviewed-subset-source-contract-v2"
)
CONFIGURED_ENDPOINT_ID_METADATA_FIELD = (
    "provider_directory_configured_endpoint_id"
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
    CONFIGURED_ENDPOINT_ID_METADATA_FIELD,
)
SERVER_ISSUED_SUBSET_SOURCE_SCOPE_METADATA_FIELDS_V2 = (
    *SERVER_ISSUED_SUBSET_SOURCE_SCOPE_METADATA_FIELDS,
    REVIEWED_ROOT_POLICY_METADATA_KEY,
)


def _source_identity_contract(
    metadata: Mapping[str, Any],
) -> tuple[str, str, tuple[str, ...]]:
    """Select the legacy or explicit-policy identity without hash drift."""

    if REVIEWED_ROOT_POLICY_METADATA_KEY not in metadata:
        return (
            SERVER_ISSUED_SUBSET_ACTIVATION_SOURCE_CONTRACT_VERSION,
            SERVER_ISSUED_SUBSET_SOURCE_SCOPE_VERSION,
            SERVER_ISSUED_SUBSET_SOURCE_SCOPE_METADATA_FIELDS,
        )
    reviewed_root_policy_from_document(
        metadata.get(REVIEWED_ROOT_POLICY_METADATA_KEY)
    )
    return (
        SERVER_ISSUED_SUBSET_ACTIVATION_SOURCE_CONTRACT_VERSION_V2,
        SERVER_ISSUED_SUBSET_SOURCE_SCOPE_VERSION_V2,
        SERVER_ISSUED_SUBSET_SOURCE_SCOPE_METADATA_FIELDS_V2,
    )


def subset_source_endpoint_identity(
    source_record: Mapping[str, Any],
) -> tuple[str, str]:
    """Return the serving snapshot and configured acquisition endpoint."""

    metadata = source_record.get("metadata_json")
    serving_endpoint_id = source_record.get("endpoint_id")
    configured_endpoint_id = (
        metadata.get(CONFIGURED_ENDPOINT_ID_METADATA_FIELD)
        if isinstance(metadata, Mapping)
        else None
    )
    if (
        type(serving_endpoint_id) is not str
        or not serving_endpoint_id
        or serving_endpoint_id != serving_endpoint_id.strip()
        or type(configured_endpoint_id) is not str
        or not configured_endpoint_id
        or configured_endpoint_id != configured_endpoint_id.strip()
    ):
        raise ValueError("provider_directory_subset_endpoint_identity_invalid")
    return serving_endpoint_id, configured_endpoint_id


def subset_activation_source_contract_payload(
    source_record: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the cutoff-neutral source identity used by activation evidence."""

    metadata = source_record.get("metadata_json")
    source_id = source_record.get("source_id")
    canonical_api_base = source_record.get("canonical_api_base")
    try:
        _, configured_endpoint_id = subset_source_endpoint_identity(
            source_record
        )
    except ValueError:
        raise ValueError(
            "provider_directory_subset_activation_source_invalid"
        ) from None
    if (
        not isinstance(metadata, Mapping)
        or type(source_id) is not str
        or not source_id
        or type(canonical_api_base) is not str
        or not canonical_api_base
    ):
        raise ValueError("provider_directory_subset_activation_source_invalid")
    try:
        activation_version, _, metadata_fields = _source_identity_contract(
            metadata
        )
    except ValueError:
        raise ValueError(
            "provider_directory_subset_activation_source_invalid"
        ) from None
    return {
        "identity_version": activation_version,
        "source": {
            "source_id": source_id,
            "endpoint_id": configured_endpoint_id,
            "canonical_api_base": canonical_api_base,
            "requires_registration": source_record.get(
                "requires_registration"
            ),
            "requires_api_key": source_record.get("requires_api_key"),
            "auth_type": source_record.get("auth_type"),
        },
        "metadata_identity": [
            [field_name, field_name in metadata, metadata.get(field_name)]
            for field_name in metadata_fields
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
    try:
        _, configured_endpoint_id = subset_source_endpoint_identity(
            source_record
        )
    except ValueError:
        raise ValueError(
            "provider_directory_subset_source_scope_invalid"
        ) from None
    if (
        type(source_id) is not str
        or not source_id
        or source_ids != (source_id,)
        or not cutoff
        or not canonical_api_base
    ):
        raise ValueError("provider_directory_subset_source_scope_invalid")
    try:
        _, scope_version, metadata_fields = _source_identity_contract(
            metadata
        )
    except ValueError:
        raise ValueError(
            "provider_directory_subset_source_scope_invalid"
        ) from None
    return {
        "identity_version": scope_version,
        "source_ids": list(source_ids),
        "cutoff": cutoff,
        "source": {
            "source_id": source_id,
            "endpoint_id": configured_endpoint_id,
            "canonical_api_base": canonical_api_base,
            "requires_registration": source_record.get(
                "requires_registration"
            ),
            "requires_api_key": source_record.get("requires_api_key"),
            "auth_type": source_record.get("auth_type"),
        },
        "metadata_identity": [
            [field_name, field_name in metadata, metadata.get(field_name)]
            for field_name in metadata_fields
        ],
    }


def is_reviewed_subset_contract(contract: Any) -> bool:
    """Return whether every fixed field matches the reviewed v3 identity."""

    resources = getattr(contract, "resources", ())
    start_urls = getattr(contract, "start_urls", ())
    page_count = getattr(contract, "page_count", None)
    campaign_id = getattr(contract, "campaign_id", None)
    strategy_version = getattr(contract, "strategy_version", None)
    completion_scopes = getattr(contract, "completion_scopes", None)
    return bool(
        getattr(contract, "contract_version", None) == 3
        and getattr(contract, "semantics", None)
        == SERVER_ISSUED_SUBSET_SEMANTICS
        and reviewed_subset_max_advertised_count_decrease(
            strategy_version,
            completion_scopes,
        )
        is not None
        and getattr(contract, "continuation_strategy", None)
        == SERVER_ISSUED_SUBSET_SMILE_CONTINUATION_STRATEGY
        and getattr(contract, "traversal_version", None)
        == SERVER_ISSUED_SUBSET_TRAVERSAL_VERSION
        and getattr(contract, "canonicalization_version", None)
        == SERVER_ISSUED_SUBSET_CANONICALIZATION_VERSION
        and len(resources) == len(SERVER_ISSUED_SUBSET_RESOURCE_TYPES)
        and set(resources) == set(SERVER_ISSUED_SUBSET_RESOURCE_TYPES)
        and len(start_urls) == len(SERVER_ISSUED_SUBSET_RESOURCE_TYPES)
        and set(dict(start_urls)) == set(SERVER_ISSUED_SUBSET_RESOURCE_TYPES)
        and type(page_count) is int
        and 1 <= page_count <= 1000
        and type(campaign_id) is str
        and bool(campaign_id)
    )


def reviewed_subset_max_advertised_count_decrease(
    strategy_version: Any,
    completion_scopes: Any,
) -> int | None:
    """Return the exact count-decrease bound for one allowlisted profile."""

    profile = (strategy_version, completion_scopes)
    if profile == (
        SERVER_ISSUED_SUBSET_EXACT_STRATEGY_VERSION,
        SERVER_ISSUED_SUBSET_EXACT_COMPLETION_SCOPES,
    ):
        return 0
    if profile == (
        SERVER_ISSUED_SUBSET_STRATEGY_VERSION,
        SERVER_ISSUED_SUBSET_COMPLETION_SCOPES,
    ):
        return SERVER_ISSUED_SUBSET_MAX_ADVERTISED_COUNT_DECREASE
    return None


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
    max_advertised_count_decrease = (
        reviewed_subset_max_advertised_count_decrease(
            strategy_version,
            tuple(completion_scopes) if type(completion_scopes) is list else None,
        )
    )
    is_valid = bool(
        type(contract_version) is int
        and contract_version == 3
        and type(page_count) is int
        and 1 <= page_count <= 1000
        and max_advertised_count_decrease is not None
        and traversal_version == SERVER_ISSUED_SUBSET_TRAVERSAL_VERSION
        and canonicalization_version
        == SERVER_ISSUED_SUBSET_CANONICALIZATION_VERSION
        and type(completion_scopes) is list
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
