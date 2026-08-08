# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Public HTTPS and reviewed start-URL fences for FHIR traversal."""

from __future__ import annotations

import ipaddress
import re
import urllib.parse
from typing import Any, Mapping

from process.provider_directory_fhir_census_contract import (
    CURRENT_VERSION_CENSUS_START_URLS_FIELD,
    _clean_text,
)


_PUBLIC_HOST_LABEL_RE = re.compile(
    r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$"
)
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


def _reviewed_start_urls(
    request: Any,
    source_record: Mapping[str, Any],
    metadata: Mapping[str, Any],
) -> tuple[tuple[str, str], ...]:
    """Return exact reviewed start URLs for every requested family."""

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
