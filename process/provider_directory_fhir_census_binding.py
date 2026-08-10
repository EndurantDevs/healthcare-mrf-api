# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Reviewed source and URL binding for current-version FHIR acquisition."""

from __future__ import annotations

import hashlib
import urllib.parse
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from process.provider_directory_fhir_census_contract import (
    CURRENT_VERSION_CENSUS_CANONICALIZATION_VERSION_FIELD,
    CURRENT_VERSION_CENSUS_COMPLETION_SCOPES_FIELD,
    CURRENT_VERSION_CENSUS_CONTINUATION_STRATEGY_FIELD,
    CURRENT_VERSION_CENSUS_CONTRACT_FIELD,
    CURRENT_VERSION_CENSUS_CONTRACT_VERSION_FIELD,
    CURRENT_VERSION_CENSUS_METADATA_STRATEGY_FIELD,
    CURRENT_VERSION_CENSUS_PAGE_COUNT_FIELD,
    CURRENT_VERSION_CENSUS_SEMANTICS,
    CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY,
    CURRENT_VERSION_CENSUS_START_URLS_FIELD,
    CURRENT_VERSION_CENSUS_STRATEGY_VERSION_FIELD,
    CURRENT_VERSION_CENSUS_STRATEGY_VERSION,
    CURRENT_VERSION_CENSUS_TRAVERSAL_VERSION_FIELD,
    SERVER_ISSUED_SUBSET_CANONICALIZATION_VERSION,
    SERVER_ISSUED_SUBSET_COMPLETION_SCOPES,
    SERVER_ISSUED_SUBSET_RESOURCE_TYPES,
    SERVER_ISSUED_SUBSET_SEMANTICS,
    SERVER_ISSUED_SUBSET_SMILE_CONTINUATION_STRATEGY,
    SERVER_ISSUED_SUBSET_STRATEGY_VERSION,
    SERVER_ISSUED_SUBSET_TRAVERSAL_VERSION,
    CurrentVersionCensusRequest,
    ProviderDirectoryFHIRAcquisitionStrategy,
    _clean_text,
    _strict_text_vector,
)
from process.provider_directory_fhir_subset_identity import (
    is_reviewed_subset_contract,
    validated_subset_identity_values,
)
from process.provider_directory_fhir_subset_profiles import (
    is_reviewed_subset_terminal_window_required,
    reviewed_subset_decrease_limit,
)
from process.provider_directory_fhir_census_urls import (
    _normalized_base_url,
    _reviewed_start_urls,
    _validate_reviewed_start_url,
)


_MANUAL_ONLY_FIELD = "provider_directory_manual_only"
_SUPPORTED_RESOURCES_FIELD = "provider_directory_supported_resources"
_ENUMERABLE_RESOURCES_FIELD = "provider_directory_fully_enumerable_resources"
_SUBSET_RESOURCES_FIELD = "provider_directory_server_issued_subset_resources"
_VERIFICATION_CAMPAIGN_FIELD = "provider_directory_verification_campaign_id"
_EXPECTED_NONEMPTY_RESOURCES_FIELD = (
    "provider_directory_expected_nonempty_resources"
)
_CONTROL_QUERY_NAMES = frozenset({"_count", "_summary", "_total"})


def _strict_metadata_resources(
    metadata: Mapping[str, Any],
    field_name: str,
    request_resources: tuple[str, ...],
) -> tuple[str, ...]:
    configured_resources = _strict_text_vector(
        metadata.get(field_name),
        field_name=field_name,
        allowed_values=frozenset(request_resources),
    )
    if frozenset(configured_resources) != frozenset(request_resources):
        raise ValueError(f"{field_name}_must_match_requested_resources")
    return configured_resources


@dataclass(frozen=True)
class CurrentVersionCensusContract:
    """Reviewed source binding for one manual current-version acquisition."""

    source_id: str
    cutoff: str
    resources: tuple[str, ...]
    expected_nonempty_resources: tuple[str, ...]
    start_urls: tuple[tuple[str, str], ...]
    continuation_strategy: str
    strategy_version: str = CURRENT_VERSION_CENSUS_STRATEGY_VERSION
    contract_version: int = 2
    semantics: str = CURRENT_VERSION_CENSUS_SEMANTICS
    page_count: int | None = None
    traversal_version: str = "provider-directory-fhir-smile-logical-offset-v2"
    canonicalization_version: str = (
        "provider-directory-fhir-current-version-resource-json-v1"
    )
    completion_scopes: tuple[str, ...] = ()
    campaign_id: str | None = None

    @property
    def is_server_issued_subset_v3(self) -> bool:
        """Return whether every field matches the reviewed subset identity."""

        return is_reviewed_subset_contract(self)

    def advertised_count_decrease_limit(self, pre_count: int) -> int:
        """Return this profile's exact resource-specific decrease limit."""

        return reviewed_subset_decrease_limit(
            self.strategy_version,
            self.completion_scopes,
            pre_count=pre_count,
            page_count=self.page_count,
            invalid_error=(
                "provider_directory_current_version_census_profile_invalid"
            ),
        )

    @property
    def is_terminal_count_window_required(self) -> bool:
        """Return whether pre-count must fall in the terminal window."""

        return is_reviewed_subset_terminal_window_required(
            self.strategy_version,
            self.completion_scopes,
        )

    def start_url(self, resource_type: str, page_count: int) -> str:
        """Return the reviewed URL with source filters and cutoff preserved."""

        if (
            isinstance(page_count, bool)
            or not isinstance(page_count, int)
            or page_count <= 0
        ):
            raise ValueError(
                "provider_directory_current_version_census_page_count_invalid"
            )
        if self.page_count is not None and page_count != self.page_count:
            raise ValueError(
                "provider_directory_current_version_census_page_count_identity_mismatch"
            )
        start_url_by_resource = dict(self.start_urls)
        try:
            reviewed_url = start_url_by_resource[resource_type]
        except KeyError as exc:
            raise ValueError(
                "provider_directory_current_version_census_resource_not_bound"
            ) from exc
        parsed_url = urllib.parse.urlsplit(reviewed_url)
        query_items = [
            (query_name, query_value)
            for query_name, query_value in urllib.parse.parse_qsl(
                parsed_url.query,
                keep_blank_values=True,
            )
            if query_name.lower() != "_count"
        ]
        query_items.extend(
            (
                ("_lastUpdated", f"lt{self.cutoff}"),
                ("_count", str(page_count)),
            )
        )
        return urllib.parse.urlunsplit(
            (
                parsed_url.scheme,
                parsed_url.netloc,
                parsed_url.path,
                urllib.parse.urlencode(query_items, doseq=True),
                "",
            )
        )

    def identity(self) -> dict[str, Any]:
        """Return source, strategy, cutoff, resources, and reviewed URL hashes."""

        identity_by_field = {
            "contract_version": self.contract_version,
            "semantics": self.semantics,
            "source_id": self.source_id,
            "strategy": self.strategy_version,
            "cutoff": self.cutoff,
            "resources": list(self.resources),
            "expected_nonempty_resources": list(
                self.expected_nonempty_resources
            ),
            "continuation_strategy": self.continuation_strategy,
            "reviewed_start_url_sha256_by_resource": {
                resource_type: hashlib.sha256(
                    start_url.encode("utf-8")
                ).hexdigest()
                for resource_type, start_url in self.start_urls
            },
        }
        if self.contract_version == 3:
            identity_by_field.update(
                page_count=self.page_count,
                traversal_version=self.traversal_version,
                canonicalization_version=self.canonicalization_version,
                completion_scopes=list(self.completion_scopes),
                campaign_id=self.campaign_id,
            )
        return identity_by_field


def _reviewed_metadata(
    request: CurrentVersionCensusRequest,
    source_record: Mapping[str, Any],
) -> Mapping[str, Any]:
    if _clean_text(source_record.get("source_id")) != request.source_id:
        raise ValueError(
            "provider_directory_current_version_census_source_identity_mismatch"
        )
    metadata = source_record.get("metadata_json")
    if not isinstance(metadata, Mapping):
        raise ValueError(
            "provider_directory_current_version_census_source_metadata_required"
        )
    if metadata.get(_MANUAL_ONLY_FIELD) is not True:
        raise ValueError(
            "provider_directory_current_version_census_manual_source_required"
        )
    configured_strategy = _clean_text(
        metadata.get(CURRENT_VERSION_CENSUS_METADATA_STRATEGY_FIELD)
    )
    if configured_strategy != request.strategy.value:
        raise ValueError(
            "provider_directory_current_version_census_strategy_not_reviewed"
        )
    return metadata


def _reviewed_resource_configuration(
    request: CurrentVersionCensusRequest,
    metadata: Mapping[str, Any],
) -> tuple[tuple[str, ...], str, bool]:
    """Validate resource scope and continuation strategy for one request."""

    _strict_metadata_resources(
        metadata,
        _SUPPORTED_RESOURCES_FIELD,
        request.resources,
    )
    is_subset_v3 = (
        request.strategy
        is ProviderDirectoryFHIRAcquisitionStrategy.SERVER_ISSUED_TRAVERSAL_SUBSET
    )
    resource_scope_field = (
        _SUBSET_RESOURCES_FIELD
        if is_subset_v3
        else _ENUMERABLE_RESOURCES_FIELD
    )
    _strict_metadata_resources(metadata, resource_scope_field, request.resources)
    expected_nonempty_resources = _strict_text_vector(
        metadata.get(_EXPECTED_NONEMPTY_RESOURCES_FIELD),
        field_name=_EXPECTED_NONEMPTY_RESOURCES_FIELD,
        allowed_values=frozenset(request.resources),
    )
    continuation_strategy = _clean_text(
        metadata.get(CURRENT_VERSION_CENSUS_CONTINUATION_STRATEGY_FIELD)
    )
    expected_continuation_strategy = (
        SERVER_ISSUED_SUBSET_SMILE_CONTINUATION_STRATEGY
        if is_subset_v3
        else CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY
    )
    if continuation_strategy != expected_continuation_strategy:
        raise ValueError(
            "provider_directory_current_version_census_continuation_strategy_not_reviewed"
        )
    return expected_nonempty_resources, continuation_strategy, is_subset_v3


def bind_current_version_census_contract(
    request: CurrentVersionCensusRequest,
    source_records: Sequence[dict[str, Any]],
) -> CurrentVersionCensusContract:
    """Bind one explicit request to one manually reviewed source record."""

    if len(source_records) != 1:
        raise ValueError(
            "provider_directory_current_version_census_source_resolution_ambiguous"
        )
    source_record = source_records[0]
    metadata = _reviewed_metadata(request, source_record)
    (
        expected_nonempty_resources,
        continuation_strategy,
        is_subset_v3,
    ) = _reviewed_resource_configuration(request, metadata)
    if is_subset_v3:
        (
            contract_version,
            page_count,
            strategy_version,
            traversal_version,
            canonicalization_version,
            completion_scopes,
            campaign_id,
        ) = validated_subset_identity_values(metadata)
    else:
        contract_version = 2
        page_count = None
        strategy_version = CURRENT_VERSION_CENSUS_STRATEGY_VERSION
        traversal_version = "provider-directory-fhir-smile-logical-offset-v2"
        canonicalization_version = (
            "provider-directory-fhir-current-version-resource-json-v1"
        )
        completion_scopes = ()
        campaign_id = None
    contract = CurrentVersionCensusContract(
        source_id=request.source_id,
        cutoff=request.cutoff,
        resources=request.resources,
        expected_nonempty_resources=expected_nonempty_resources,
        start_urls=_reviewed_start_urls(request, source_record, metadata),
        continuation_strategy=continuation_strategy,
        contract_version=contract_version,
        semantics=(
            SERVER_ISSUED_SUBSET_SEMANTICS
            if is_subset_v3
            else CURRENT_VERSION_CENSUS_SEMANTICS
        ),
        page_count=page_count,
        strategy_version=strategy_version,
        traversal_version=traversal_version,
        canonicalization_version=canonicalization_version,
        completion_scopes=completion_scopes,
        campaign_id=campaign_id,
    )
    source_record[CURRENT_VERSION_CENSUS_CONTRACT_FIELD] = contract
    return contract


def current_version_census_contract(
    source_record: Mapping[str, Any],
) -> CurrentVersionCensusContract | None:
    """Return the already admitted transient contract, if present."""

    contract = source_record.get(CURRENT_VERSION_CENSUS_CONTRACT_FIELD)
    return contract if isinstance(contract, CurrentVersionCensusContract) else None


def current_version_census_count_url(start_url: str) -> str:
    """Build an exact count request without discarding source filters."""

    parsed_url = urllib.parse.urlsplit(start_url)
    query_items = [
        (query_name, query_value)
        for query_name, query_value in urllib.parse.parse_qsl(
            parsed_url.query,
            keep_blank_values=True,
        )
        if query_name.lower() not in _CONTROL_QUERY_NAMES
    ]
    query_items.extend((("_summary", "count"), ("_total", "accurate")))
    return urllib.parse.urlunsplit(
        (
            parsed_url.scheme,
            parsed_url.netloc,
            parsed_url.path,
            urllib.parse.urlencode(query_items, doseq=True),
            parsed_url.fragment,
        )
    )


def validated_current_version_census_count_map(
    contract: CurrentVersionCensusContract,
    count_by_resource: Mapping[str, Any],
) -> dict[str, int]:
    """Reject malformed, all-zero, or expected-empty exact count vectors."""

    if set(count_by_resource) != set(contract.resources):
        raise ValueError(
            "provider_directory_current_version_census_count_resources_mismatch"
        )
    normalized_count_by_resource: dict[str, int] = {}
    for resource_type in contract.resources:
        count = count_by_resource[resource_type]
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise ValueError(
                "provider_directory_current_version_census_count_invalid"
            )
        normalized_count_by_resource[resource_type] = count
    if not any(normalized_count_by_resource.values()):
        raise ValueError(
            "provider_directory_current_version_census_all_zero_rejected"
        )
    empty_expected_resources = [
        resource_type
        for resource_type in contract.expected_nonempty_resources
        if normalized_count_by_resource[resource_type] == 0
    ]
    if empty_expected_resources:
        raise ValueError(
            "provider_directory_current_version_census_expected_nonempty_zero:"
            + ",".join(empty_expected_resources)
        )
    return normalized_count_by_resource
