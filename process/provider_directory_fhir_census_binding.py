# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Reviewed source and URL binding for the current-version census contract."""

from __future__ import annotations

import hashlib
import ipaddress
import re
import urllib.parse
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from process.provider_directory_fhir_census_contract import (
    CURRENT_VERSION_CENSUS_CONTINUATION_STRATEGY_FIELD,
    CURRENT_VERSION_CENSUS_CONTRACT_FIELD,
    CURRENT_VERSION_CENSUS_METADATA_STRATEGY_FIELD,
    CURRENT_VERSION_CENSUS_SEMANTICS,
    CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY,
    CURRENT_VERSION_CENSUS_START_URLS_FIELD,
    CURRENT_VERSION_CENSUS_STRATEGY_VERSION,
    CurrentVersionCensusRequest,
    _clean_text,
    _strict_text_vector,
)


_MANUAL_ONLY_FIELD = "provider_directory_manual_only"
_SUPPORTED_RESOURCES_FIELD = "provider_directory_supported_resources"
_ENUMERABLE_RESOURCES_FIELD = "provider_directory_fully_enumerable_resources"
_EXPECTED_NONEMPTY_RESOURCES_FIELD = (
    "provider_directory_expected_nonempty_resources"
)
_CONTROL_QUERY_NAMES = frozenset({"_count", "_summary", "_total"})
_PUBLIC_HOST_LABEL_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
_CONTINUATION_QUERY_NAMES = frozenset(
    {
        "_continuationtoken",
        "_getpages",
        "_getpagesid",
        "_getpagesoffset",
        "_offset",
        "_page",
        "_page_token",
        "_searchid",
        "_skip",
        "cursor",
        "cursormark",
        "ct",
        "nexttoken",
        "page",
        "pagetoken",
    }
)


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


def _normalized_base_url(raw_value: Any) -> urllib.parse.SplitResult:
    raw_url = _clean_text(raw_value)
    if raw_url is None or any(character.isspace() for character in raw_url):
        raise ValueError(
            "provider_directory_current_version_census_base_url_invalid"
        )
    try:
        parsed_url = urllib.parse.urlsplit(raw_url)
        port = parsed_url.port
    except ValueError as exc:
        raise ValueError(
            "provider_directory_current_version_census_base_url_invalid"
        ) from exc
    hostname = (parsed_url.hostname or "").lower()
    normalized_hostname = hostname.rstrip(".")
    if (
        parsed_url.scheme.lower() != "https"
        or not normalized_hostname
        or parsed_url.username is not None
        or parsed_url.password is not None
        or parsed_url.fragment
        or port not in (None, 443)
        or hostname != normalized_hostname
        or parsed_url.netloc.lower()
        not in {normalized_hostname, f"{normalized_hostname}:443"}
    ):
        raise ValueError(
            "provider_directory_current_version_census_base_url_invalid"
        )
    try:
        literal_ip = ipaddress.ip_address(normalized_hostname)
    except ValueError:
        literal_ip = None
    if literal_ip is not None:
        raise ValueError(
            "provider_directory_current_version_census_base_url_invalid"
        )
    host_labels = normalized_hostname.split(".")
    if (
        len(host_labels) < 2
        or normalized_hostname == "localhost"
        or normalized_hostname.endswith((".localhost", ".local"))
        or host_labels[-1].isdigit()
        or len(normalized_hostname) > 253
        or any(
            _PUBLIC_HOST_LABEL_RE.fullmatch(host_label) is None
            for host_label in host_labels
        )
    ):
        raise ValueError(
            "provider_directory_current_version_census_base_url_invalid"
        )
    return parsed_url


def _https_origin(parsed_url: urllib.parse.SplitResult) -> tuple[str, int]:
    """Return the already validated public HTTPS origin."""

    return (parsed_url.hostname or "").lower(), parsed_url.port or 443


def _validate_reviewed_start_url(
    raw_url: Any,
    *,
    canonical_base: urllib.parse.SplitResult,
    resource_type: str,
) -> str:
    if not isinstance(raw_url, str) or not raw_url.strip():
        raise ValueError(
            "provider_directory_current_version_census_start_url_invalid"
        )
    reviewed_url = raw_url.strip()
    parsed_url = _normalized_base_url(reviewed_url)
    if _https_origin(parsed_url) != _https_origin(canonical_base):
        raise ValueError(
            "provider_directory_current_version_census_start_url_origin_mismatch"
        )
    expected_path = f"{canonical_base.path.rstrip('/')}/{resource_type}"
    if parsed_url.path.rstrip("/") != expected_path:
        raise ValueError(
            "provider_directory_current_version_census_start_url_path_mismatch"
        )
    query_names = {
        query_name.lower()
        for query_name, _query_value in urllib.parse.parse_qsl(
            parsed_url.query,
            keep_blank_values=True,
        )
    }
    if query_names.intersection(_CONTINUATION_QUERY_NAMES):
        raise ValueError(
            "provider_directory_current_version_census_start_url_contains_continuation"
        )
    if query_names.intersection({"_summary", "_total"}):
        raise ValueError(
            "provider_directory_current_version_census_start_url_contains_count_control"
        )
    if any(
        query_name == "_lastupdated"
        or query_name.startswith("_lastupdated:")
        for query_name in query_names
    ):
        raise ValueError(
            "provider_directory_current_version_census_start_url_contains_last_updated"
        )
    return reviewed_url


@dataclass(frozen=True)
class CurrentVersionCensusContract:
    """Reviewed source binding for one manual current-version census."""

    source_id: str
    cutoff: str
    resources: tuple[str, ...]
    expected_nonempty_resources: tuple[str, ...]
    start_urls: tuple[tuple[str, str], ...]
    continuation_strategy: str
    strategy_version: str = CURRENT_VERSION_CENSUS_STRATEGY_VERSION

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

        return {
            "contract_version": 1,
            "semantics": CURRENT_VERSION_CENSUS_SEMANTICS,
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


def _reviewed_start_urls(
    request: CurrentVersionCensusRequest,
    source_record: Mapping[str, Any],
    metadata: Mapping[str, Any],
) -> tuple[tuple[str, str], ...]:
    raw_start_url_by_resource = metadata.get(
        CURRENT_VERSION_CENSUS_START_URLS_FIELD
    )
    if not isinstance(raw_start_url_by_resource, Mapping):
        raise ValueError(
            "provider_directory_current_version_census_start_urls_required"
        )
    if set(raw_start_url_by_resource) != set(request.resources):
        raise ValueError(
            "provider_directory_current_version_census_start_urls_must_match_resources"
        )
    canonical_base = _normalized_base_url(
        source_record.get("canonical_api_base") or source_record.get("api_base")
    )
    return tuple(
        (
            resource_type,
            _validate_reviewed_start_url(
                raw_start_url_by_resource[resource_type],
                canonical_base=canonical_base,
                resource_type=resource_type,
            ),
        )
        for resource_type in request.resources
    )


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
    _strict_metadata_resources(
        metadata,
        _SUPPORTED_RESOURCES_FIELD,
        request.resources,
    )
    _strict_metadata_resources(
        metadata,
        _ENUMERABLE_RESOURCES_FIELD,
        request.resources,
    )
    expected_nonempty_resources = _strict_text_vector(
        metadata.get(_EXPECTED_NONEMPTY_RESOURCES_FIELD),
        field_name=_EXPECTED_NONEMPTY_RESOURCES_FIELD,
        allowed_values=frozenset(request.resources),
    )
    continuation_strategy = _clean_text(
        metadata.get(CURRENT_VERSION_CENSUS_CONTINUATION_STRATEGY_FIELD)
    )
    if continuation_strategy != CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY:
        raise ValueError(
            "provider_directory_current_version_census_continuation_strategy_not_reviewed"
        )
    contract = CurrentVersionCensusContract(
        source_id=request.source_id,
        cutoff=request.cutoff,
        resources=request.resources,
        expected_nonempty_resources=expected_nonempty_resources,
        start_urls=_reviewed_start_urls(request, source_record, metadata),
        continuation_strategy=continuation_strategy,
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
