# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed marker contract for one direct v4 terminal-root disposition."""

from __future__ import annotations

import re
from typing import Any, Mapping

from process.provider_directory_fhir_subset_terminal_disposition_contract import (
    ReviewedSubsetTerminalDispositionError,
    canonical_evidence_sha256,
)
from process.provider_directory_fhir_subset_terminal_disposition_profile import (
    DIRECT_V4_CONTRACT_VERSION,
    DIRECT_V4_DISPOSITION_BY_RESOURCE_TYPE,
    DIRECT_V4_DRIFT_RESOURCE_TYPES,
    DIRECT_V4_LINEAGE_FIELDS,
    DIRECT_V4_MAX_VERIFIED_DECREASE,
    DIRECT_V4_REASON_CODE,
    DIRECT_V4_RESOURCE_DISPOSITION_FIELDS,
    DIRECT_V4_TERMINAL_MARKER_FIELDS,
    EXPECTED_RESOURCE_TYPES,
    TERMINAL_CENSUS_DRIFT_DISPOSITION,
    VERIFIED_COMPLETE_DISPOSITION,
)


_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_COUNT_FIELDS = (
    "checkpoint_pages",
    "diagnostic_pages",
    "page_delta",
    "retained_rows",
    "advertised_pre",
    "advertised_post",
    "returned_unique",
    "deficit",
    "terminal_page_entry_count",
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


def _validated_resource(
    resource_type: str,
    resource_value: object,
) -> dict[str, Any]:
    if (
        not isinstance(resource_value, Mapping)
        or set(resource_value) != DIRECT_V4_RESOURCE_DISPOSITION_FIELDS
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    resource_by_field = dict(resource_value)
    count_by_name = {
        field_name: _nonnegative_integer(resource_by_field.get(field_name))
        for field_name in _COUNT_FIELDS
    }
    if any(
        _SHA256.fullmatch(str(resource_by_field.get(field_name) or "")) is None
        for field_name in _HASH_FIELDS
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    expected_disposition = DIRECT_V4_DISPOSITION_BY_RESOURCE_TYPE[resource_type]
    advertised_decrease = (
        count_by_name["advertised_pre"] - count_by_name["advertised_post"]
    )
    common_is_valid = bool(
        resource_by_field.get("disposition") == expected_disposition
        and count_by_name["diagnostic_pages"]
        - count_by_name["checkpoint_pages"]
        == count_by_name["page_delta"]
        and count_by_name["advertised_post"]
        <= count_by_name["advertised_pre"]
        and count_by_name["returned_unique"]
        <= count_by_name["advertised_post"]
        and count_by_name["deficit"]
        == count_by_name["advertised_pre"]
        - count_by_name["returned_unique"]
        and count_by_name["retained_rows"] > 0
    )
    if expected_disposition == VERIFIED_COMPLETE_DISPOSITION:
        state_is_valid = bool(
            resource_by_field.get("checkpoint_state") == "complete"
            and count_by_name["page_delta"] == 0
            and advertised_decrease <= DIRECT_V4_MAX_VERIFIED_DECREASE
            and count_by_name["retained_rows"]
            == count_by_name["returned_unique"]
        )
    else:
        state_is_valid = bool(
            expected_disposition == TERMINAL_CENSUS_DRIFT_DISPOSITION
            and resource_by_field.get("checkpoint_state") == "active"
            and count_by_name["page_delta"] == 1
            and advertised_decrease > DIRECT_V4_MAX_VERIFIED_DECREASE
            and count_by_name["returned_unique"]
            - count_by_name["retained_rows"]
            == count_by_name["terminal_page_entry_count"]
        )
    if not common_is_valid or not state_is_valid:
        raise ReviewedSubsetTerminalDispositionError("evidence")
    return resource_by_field


def _validated_lineage(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != DIRECT_V4_LINEAGE_FIELDS:
        raise ReviewedSubsetTerminalDispositionError("evidence")
    lineage_by_field = dict(value)
    count_fields = (
        "checkpoint_retry_count",
        "competing_candidate_count",
        "current_dataset_count",
        "import_run_row_count",
        "previous_reference_count",
    )
    if (
        any(
            _nonnegative_integer(lineage_by_field.get(field_name)) != 0
            for field_name in count_fields
        )
        or lineage_by_field.get("owner_equals_root") is not True
        or lineage_by_field.get("previous_dataset_present") is not False
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    return lineage_by_field


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
        "terminal_page_delta": len(DIRECT_V4_DRIFT_RESOURCE_TYPES),
        "checkpoint_rows_processed": retained_rows,
        "resource_count": retained_rows,
        "proof_row_count": retained_rows,
    }


def validated_direct_v4_terminal_marker(marker_value: object) -> dict[str, Any]:
    """Validate one identifier-free direct-v4 terminal marker."""

    if (
        not isinstance(marker_value, Mapping)
        or set(marker_value) != DIRECT_V4_TERMINAL_MARKER_FIELDS
        or marker_value.get("contract_version") != DIRECT_V4_CONTRACT_VERSION
        or marker_value.get("reason_code") != DIRECT_V4_REASON_CODE
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
    expected_total_by_name = _resource_totals(resources_by_type)
    if any(
        _nonnegative_integer(marker_value.get(field_name)) != expected_value
        for field_name, expected_value in expected_total_by_name.items()
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    if _nonnegative_integer(marker_value.get("proof_shard_count")) <= 0:
        raise ReviewedSubsetTerminalDispositionError("evidence")
    hash_fields = (
        "source_diagnostics_sha256",
        "source_import_sha256",
        "candidate_metadata_sha256",
    )
    if any(
        _SHA256.fullmatch(str(marker_value.get(field_name) or "")) is None
        for field_name in hash_fields
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    _validated_lineage(marker_value.get("direct_lineage"))
    return dict(marker_value)


def direct_v4_terminal_marker(
    *,
    source_scope_sha256: str,
    resource_dispositions: Mapping[str, Mapping[str, Any]],
    proof_shard_count: int,
    source_diagnostics: Mapping[str, Any],
    source_import: Mapping[str, Any],
    candidate_metadata: Mapping[str, Any],
    direct_lineage: Mapping[str, Any],
) -> dict[str, Any]:
    """Build the exact direct-v4 marker in the existing terminal envelope."""

    resources_by_type = {
        resource_type: dict(resource_dispositions[resource_type])
        for resource_type in EXPECTED_RESOURCE_TYPES
    }
    marker_by_field: dict[str, Any] = {
        "contract_version": DIRECT_V4_CONTRACT_VERSION,
        "reason_code": DIRECT_V4_REASON_CODE,
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
    return validated_direct_v4_terminal_marker(marker_by_field)


__all__ = (
    "direct_v4_terminal_marker",
    "validated_direct_v4_terminal_marker",
)
