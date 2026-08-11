# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed marker contract for one direct-v5 HTTP-410 disposition."""

from __future__ import annotations

import re
from typing import Any, Mapping

from process.provider_directory_fhir_subset_terminal_disposition_contract import (
    ReviewedSubsetTerminalDispositionError,
    canonical_evidence_sha256,
)
from process.provider_directory_fhir_subset_terminal_disposition_profile import (
    DIRECT_V5_CONTRACT_VERSION,
    DIRECT_V5_DISPOSITION_BY_RESOURCE_TYPE,
    DIRECT_V5_MAX_DECREASE_BASIS_POINTS,
    DIRECT_V5_MAX_DECREASE_PAGES,
    DIRECT_V5_PAGE_COUNT,
    DIRECT_V5_REASON_CODE,
    DIRECT_V5_TERMINAL_MARKER_FIELDS,
    EXPECTED_RESOURCE_TYPES,
    RESOURCE_DISPOSITION_FIELDS,
    TERMINAL_HTTP_410_DISPOSITION,
    VERIFIED_COMPLETE_DISPOSITION,
)
from process.provider_directory_fhir_subset_terminal_disposition_v4_contract import (
    validated_direct_lineage,
)


_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_COMMON_COUNT_FIELDS = (
    "checkpoint_pages",
    "diagnostic_pages",
    "page_delta",
    "retained_rows",
    "advertised_pre",
)
_TERMINAL_COUNT_FIELDS = (
    "advertised_post",
    "returned_unique",
    "deficit",
)
_HASH_FIELDS = (
    "diagnostic_sha256",
    "checkpoint_proof_sha256",
    "start_url_sha256",
    "recent_cursor_hashes_sha256",
)


def _nonnegative_integer(value: object) -> int:
    if type(value) is not int or value < 0:
        raise ReviewedSubsetTerminalDispositionError("evidence")
    return value


def _decrease_limit(pre_count: int, page_count: int) -> int:
    percentage_limit = (
        pre_count * DIRECT_V5_MAX_DECREASE_BASIS_POINTS + 9_999
    ) // 10_000
    return min(
        page_count * DIRECT_V5_MAX_DECREASE_PAGES,
        percentage_limit,
    )


def _validated_complete_resource(
    resource_by_field: Mapping[str, Any],
    count_by_name: Mapping[str, int],
) -> None:
    terminal_count_by_name = {
        field_name: _nonnegative_integer(resource_by_field.get(field_name))
        for field_name in _TERMINAL_COUNT_FIELDS
    }
    pre_count = count_by_name["advertised_pre"]
    post_count = terminal_count_by_name["advertised_post"]
    returned_unique = terminal_count_by_name["returned_unique"]
    if (
        resource_by_field.get("checkpoint_state") != "complete"
        or count_by_name["page_delta"] != 0
        or post_count > pre_count
        or pre_count - post_count
        > _decrease_limit(pre_count, DIRECT_V5_PAGE_COUNT)
        or returned_unique > post_count
        or terminal_count_by_name["deficit"] != pre_count - returned_unique
        or count_by_name["retained_rows"] != returned_unique
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")


def _validated_http_410_resource(
    resource_by_field: Mapping[str, Any],
    count_by_name: Mapping[str, int],
) -> None:
    if (
        resource_by_field.get("checkpoint_state") != "active"
        or count_by_name["page_delta"] != 0
        or count_by_name["retained_rows"] > count_by_name["advertised_pre"]
        or any(
            resource_by_field.get(field_name) is not None
            for field_name in _TERMINAL_COUNT_FIELDS
        )
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")


def _validated_resource(
    resource_type: str,
    resource_value: object,
) -> dict[str, Any]:
    if (
        not isinstance(resource_value, Mapping)
        or set(resource_value) != RESOURCE_DISPOSITION_FIELDS
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    resource_by_field = dict(resource_value)
    count_by_name = {
        field_name: _nonnegative_integer(resource_by_field.get(field_name))
        for field_name in _COMMON_COUNT_FIELDS
    }
    if (
        count_by_name["retained_rows"] <= 0
        or count_by_name["diagnostic_pages"]
        - count_by_name["checkpoint_pages"]
        != count_by_name["page_delta"]
        or any(
            _SHA256.fullmatch(str(resource_by_field.get(field_name) or ""))
            is None
            for field_name in _HASH_FIELDS
        )
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    expected_disposition = DIRECT_V5_DISPOSITION_BY_RESOURCE_TYPE[resource_type]
    if resource_by_field.get("disposition") != expected_disposition:
        raise ReviewedSubsetTerminalDispositionError("evidence")
    if expected_disposition == VERIFIED_COMPLETE_DISPOSITION:
        _validated_complete_resource(resource_by_field, count_by_name)
    elif expected_disposition == TERMINAL_HTTP_410_DISPOSITION:
        _validated_http_410_resource(resource_by_field, count_by_name)
    else:
        raise ReviewedSubsetTerminalDispositionError("evidence")
    return resource_by_field


def _resource_totals(
    resources_by_type: Mapping[str, Mapping[str, Any]],
) -> dict[str, int]:
    retained_rows = sum(
        resource["retained_rows"] for resource in resources_by_type.values()
    )
    return {
        "checkpoint_count": len(resources_by_type),
        "checkpoint_pages_processed": sum(
            resource["checkpoint_pages"] for resource in resources_by_type.values()
        ),
        "diagnostic_pages_processed": sum(
            resource["diagnostic_pages"] for resource in resources_by_type.values()
        ),
        "terminal_page_delta": sum(
            resource["page_delta"] for resource in resources_by_type.values()
        ),
        "checkpoint_rows_processed": retained_rows,
        "resource_count": retained_rows,
        "proof_row_count": retained_rows,
    }


def validated_direct_v5_terminal_marker(marker_value: object) -> dict[str, Any]:
    """Validate one identifier-free direct-v5 HTTP-410 marker."""

    if (
        not isinstance(marker_value, Mapping)
        or set(marker_value) != DIRECT_V5_TERMINAL_MARKER_FIELDS
        or marker_value.get("contract_version") != DIRECT_V5_CONTRACT_VERSION
        or marker_value.get("reason_code") != DIRECT_V5_REASON_CODE
        or marker_value.get("resource_types") != list(EXPECTED_RESOURCE_TYPES)
        or _SHA256.fullmatch(str(marker_value.get("source_scope_sha256") or ""))
        is None
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    raw_resources = marker_value.get("resource_dispositions")
    if not isinstance(raw_resources, Mapping) or set(raw_resources) != set(
        EXPECTED_RESOURCE_TYPES
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    resources_by_type = {
        resource_type: _validated_resource(
            resource_type,
            raw_resources[resource_type],
        )
        for resource_type in EXPECTED_RESOURCE_TYPES
    }
    if any(
        _nonnegative_integer(marker_value.get(field_name)) != expected_value
        for field_name, expected_value in _resource_totals(resources_by_type).items()
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    if _nonnegative_integer(marker_value.get("proof_shard_count")) <= 0:
        raise ReviewedSubsetTerminalDispositionError("evidence")
    if any(
        _SHA256.fullmatch(str(marker_value.get(field_name) or "")) is None
        for field_name in (
            "source_diagnostics_sha256",
            "source_import_sha256",
            "candidate_metadata_sha256",
        )
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    validated_direct_lineage(marker_value.get("direct_lineage"))
    return dict(marker_value)


def direct_v5_terminal_marker(
    *,
    source_scope_sha256: str,
    resource_dispositions: Mapping[str, Mapping[str, Any]],
    proof_shard_count: int,
    source_diagnostics: Mapping[str, Any],
    source_import: Mapping[str, Any],
    candidate_metadata: Mapping[str, Any],
    direct_lineage: Mapping[str, Any],
) -> dict[str, Any]:
    """Build the exact direct-v5 marker in the shared terminal envelope."""

    resources_by_type = {
        resource_type: dict(resource_dispositions[resource_type])
        for resource_type in EXPECTED_RESOURCE_TYPES
    }
    marker_by_field: dict[str, Any] = {
        "contract_version": DIRECT_V5_CONTRACT_VERSION,
        "reason_code": DIRECT_V5_REASON_CODE,
        "source_scope_sha256": source_scope_sha256,
        "resource_types": list(EXPECTED_RESOURCE_TYPES),
        "resource_dispositions": resources_by_type,
        **_resource_totals(resources_by_type),
        "proof_shard_count": proof_shard_count,
        "source_diagnostics_sha256": canonical_evidence_sha256(
            source_diagnostics
        ),
        "source_import_sha256": canonical_evidence_sha256(source_import),
        "candidate_metadata_sha256": canonical_evidence_sha256(
            candidate_metadata
        ),
        "direct_lineage": dict(direct_lineage),
    }
    return validated_direct_v5_terminal_marker(marker_by_field)


__all__ = (
    "direct_v5_terminal_marker",
    "validated_direct_v5_terminal_marker",
)
