# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed contract for one reviewed mixed-terminal root disposition."""

from __future__ import annotations

from dataclasses import dataclass
import os
import re
from typing import Any, Mapping, Sequence

from process.provider_directory_fhir_subset_completion import (
    canonical_payload_sha256,
)

from process.provider_directory_fhir_subset_terminal_disposition_profile import (
    COUNT_DRIFT_DISPOSITION,
    COUNT_DRIFT_RESOURCE_TYPES,
    DIRECT_V4_CONTRACT_VERSION,
    EXPECTED_DISPOSITION_BY_RESOURCE_TYPE,
    EXPECTED_RESOURCE_TYPES,
    RESOURCE_DISPOSITION_FIELDS,
    RETRYABLE_HTTP_500_DISPOSITION,
    RETRYABLE_HTTP_500_RESOURCE_TYPES,
    STABLE_COMPLETE_DISPOSITION,
    STABLE_COMPLETE_RESOURCE_TYPES,
    TERMINAL_MARKER_FIELDS,
)


TERMINAL_DISPOSITION_CONTRACT_VERSION = (
    "healthporta.provider-directory.reviewed-subset-terminal-disposition.v1"
)
TERMINAL_DISPOSITION_METADATA_KEY = (
    "provider_directory_reviewed_subset_terminal_disposition_v1"
)
TERMINAL_DISPOSITION_REASON_CODE = (
    "bounded_advertised_count_drift_with_retained_progress"
)
TERMINAL_DISPOSITION_ENABLED_ENV = (
    "HLTHPRT_PROVIDER_DIRECTORY_REVIEWED_SUBSET_TERMINAL_DISPOSITION_ENABLED"
)
TERMINAL_DISPOSITION_TIMEOUT_SECONDS = 120
TERMINAL_DISPOSITION_STATUS = "acquisition_abandoned"
TERMINAL_DISPOSITION_CHECKPOINT_STATE = "acquisition_abandoned"
TERMINAL_DISPOSITION_PRIOR_STATUS = "failed"

_HEX_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class ReviewedSubsetTerminalDispositionError(RuntimeError):
    """Fail with one stable redacted operator error code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True, slots=True)
class ReviewedSubsetTerminalDispositionResult:
    """Report whether the exact retained root changed state."""

    disposed: bool

    def __post_init__(self) -> None:
        if type(self.disposed) is not bool:
            raise ReviewedSubsetTerminalDispositionError("state")

    @property
    def is_already_applied(self) -> bool:
        """Return whether the exact terminal disposition already existed."""

        return not self.disposed


@dataclass(frozen=True, slots=True, repr=False)
class ReviewedSubsetTerminalDispositionSelection:
    """Bind one private retained root to its exact observed evidence."""

    source_id: str
    endpoint_id: str
    dataset_id: str
    acquisition_root_run_id: str
    owner_run_id: str
    canonical_api_base: str
    source_scope_sha256: str
    marker_by_field: dict[str, Any]
    prior_status: str
    observed_resource_count: int
    observed_candidate_metadata: dict[str, Any]

    def __post_init__(self) -> None:
        text_fields = (
            self.source_id,
            self.endpoint_id,
            self.dataset_id,
            self.acquisition_root_run_id,
            self.owner_run_id,
            self.canonical_api_base,
        )
        if (
            any(type(value) is not str or not value for value in text_fields)
            or self.prior_status
            not in {
                TERMINAL_DISPOSITION_PRIOR_STATUS,
                TERMINAL_DISPOSITION_STATUS,
            }
            or _HEX_SHA256.fullmatch(self.source_scope_sha256) is None
            or type(self.observed_resource_count) is not int
            or self.observed_resource_count < 0
            or type(self.observed_candidate_metadata) is not dict
        ):
            raise ReviewedSubsetTerminalDispositionError("evidence")
        marker = validated_terminal_disposition_marker(self.marker_by_field)
        if marker["source_scope_sha256"] != self.source_scope_sha256:
            raise ReviewedSubsetTerminalDispositionError("evidence")


def require_reviewed_subset_terminal_disposition_gate() -> None:
    """Require the explicit one-shot disposition gate."""

    if os.getenv(TERMINAL_DISPOSITION_ENABLED_ENV) != "true":
        raise ReviewedSubsetTerminalDispositionError("disabled")


def canonical_evidence_sha256(value: Any) -> str:
    """Hash one JSON evidence value with deterministic serialization."""

    try:
        return canonical_payload_sha256(value)
    except (TypeError, ValueError):
        raise ReviewedSubsetTerminalDispositionError("evidence") from None


def _nonnegative_integer(value: Any) -> int:
    if type(value) is not int or value < 0:
        raise ReviewedSubsetTerminalDispositionError("evidence")
    return value


def _optional_nonnegative_integer(value: Any) -> int | None:
    if value is None:
        return None
    return _nonnegative_integer(value)


def _validated_resource_counts(
    resource_by_field: Mapping[str, Any],
) -> tuple[int | None, int | None, int | None, int | None]:
    count_field_names = (
        "advertised_pre",
        "advertised_post",
        "returned_unique",
        "deficit",
    )
    return tuple(
        _optional_nonnegative_integer(resource_by_field.get(field_name))
        for field_name in count_field_names
    )


def _is_resource_disposition_state_valid(
    resource_by_field: Mapping[str, Any],
    disposition: Any,
    page_delta: int,
    completed_counts: tuple[int | None, ...],
) -> bool:
    if disposition == STABLE_COMPLETE_DISPOSITION:
        return bool(
            resource_by_field.get("checkpoint_state") == "complete"
            and page_delta == 0
            and all(type(count) is int for count in completed_counts)
            and resource_by_field["advertised_pre"]
            == resource_by_field["advertised_post"]
        )
    if disposition == COUNT_DRIFT_DISPOSITION:
        return bool(
            resource_by_field.get("checkpoint_state") == "active"
            and page_delta == 1
            and all(type(count) is int for count in completed_counts)
            and resource_by_field["advertised_pre"]
            - resource_by_field["advertised_post"]
            == 1
        )
    return bool(
        disposition == RETRYABLE_HTTP_500_DISPOSITION
        and resource_by_field.get("checkpoint_state") == "active"
        and page_delta == 0
        and resource_by_field.get("advertised_pre") is not None
        and all(count is None for count in completed_counts[1:])
    )


def _has_valid_resource_count_binding(
    resource_by_field: Mapping[str, Any],
    disposition: Any,
) -> bool:
    if disposition == RETRYABLE_HTTP_500_DISPOSITION:
        return True
    return bool(
        resource_by_field["returned_unique"]
        == resource_by_field["retained_rows"]
        and resource_by_field["deficit"]
        == resource_by_field["advertised_pre"]
        - resource_by_field["returned_unique"]
    )


def _validated_resource_disposition(
    resource_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate one closed resource-level disposition projection."""

    if (
        not isinstance(resource_by_field, Mapping)
        or set(resource_by_field) != RESOURCE_DISPOSITION_FIELDS
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    normalized_by_field = dict(resource_by_field)
    disposition = normalized_by_field.get("disposition")
    checkpoint_pages = _nonnegative_integer(
        normalized_by_field.get("checkpoint_pages")
    )
    diagnostic_pages = _nonnegative_integer(
        normalized_by_field.get("diagnostic_pages")
    )
    page_delta = _nonnegative_integer(normalized_by_field.get("page_delta"))
    _nonnegative_integer(normalized_by_field.get("retained_rows"))
    completed_counts = _validated_resource_counts(normalized_by_field)
    hash_field_names = (
        "diagnostic_sha256",
        "checkpoint_proof_sha256",
        "start_url_sha256",
        "recent_cursor_hashes_sha256",
    )
    if any(
        _HEX_SHA256.fullmatch(str(normalized_by_field.get(field_name) or ""))
        is None
        for field_name in hash_field_names
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    if diagnostic_pages - checkpoint_pages != page_delta:
        raise ReviewedSubsetTerminalDispositionError("evidence")
    if not _is_resource_disposition_state_valid(
        normalized_by_field,
        disposition,
        page_delta,
        completed_counts,
    ) or not _has_valid_resource_count_binding(
        normalized_by_field,
        disposition,
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    return normalized_by_field


def terminal_disposition_marker(
    *,
    source_scope_sha256: str,
    resource_dispositions: Mapping[str, Mapping[str, Any]],
    proof_shard_count: int,
    source_diagnostics: Mapping[str, Any],
    source_import: Mapping[str, Any],
    candidate_metadata: Mapping[str, Any],
) -> dict[str, Any]:
    """Build the closed identifier-free retained-evidence marker."""

    resources_by_type = {
        resource_type: dict(resource_dispositions[resource_type])
        for resource_type in EXPECTED_RESOURCE_TYPES
    }
    observed_disposition_by_resource_type = {
        resource_type: resource_by_field["disposition"]
        for resource_type, resource_by_field in resources_by_type.items()
    }
    if (
        observed_disposition_by_resource_type
        != EXPECTED_DISPOSITION_BY_RESOURCE_TYPE
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    retained_progress_types = (
        *STABLE_COMPLETE_RESOURCE_TYPES,
        *COUNT_DRIFT_RESOURCE_TYPES,
    )
    if any(
        resources_by_type[resource_type]["retained_rows"] <= 0
        for resource_type in retained_progress_types
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    marker_by_field = _terminal_marker_fields(
        source_scope_sha256,
        resources_by_type,
        proof_shard_count,
        source_diagnostics,
        source_import,
        candidate_metadata,
    )
    return validated_terminal_disposition_marker(marker_by_field)


def _terminal_marker_fields(
    source_scope_sha256: str,
    resources_by_type: Mapping[str, Mapping[str, Any]],
    proof_shard_count: int,
    source_diagnostics: Mapping[str, Any],
    source_import: Mapping[str, Any],
    candidate_metadata: Mapping[str, Any],
) -> dict[str, Any]:
    retained_rows = sum(
        resource["retained_rows"] for resource in resources_by_type.values()
    )
    return {
        "contract_version": TERMINAL_DISPOSITION_CONTRACT_VERSION,
        "reason_code": TERMINAL_DISPOSITION_REASON_CODE,
        "source_scope_sha256": source_scope_sha256,
        "resource_types": list(EXPECTED_RESOURCE_TYPES),
        "resource_dispositions": resources_by_type,
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
        "proof_shard_count": proof_shard_count,
        "proof_row_count": retained_rows,
        "source_diagnostics_sha256": canonical_evidence_sha256(
            source_diagnostics
        ),
        "source_import_sha256": canonical_evidence_sha256(source_import),
        "candidate_metadata_sha256": canonical_evidence_sha256(
            candidate_metadata
        ),
    }


def _validated_marker_resources(
    marker_by_field: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    raw_resources_by_type = marker_by_field.get("resource_dispositions")
    if not isinstance(raw_resources_by_type, Mapping) or set(
        raw_resources_by_type
    ) != set(EXPECTED_RESOURCE_TYPES):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    resources_by_type = {
        resource_type: _validated_resource_disposition(
            raw_resources_by_type[resource_type]
        )
        for resource_type in EXPECTED_RESOURCE_TYPES
    }
    observed_disposition_by_resource_type = {
        resource_type: resource_by_field["disposition"]
        for resource_type, resource_by_field in resources_by_type.items()
    }
    if (
        observed_disposition_by_resource_type
        != EXPECTED_DISPOSITION_BY_RESOURCE_TYPE
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    return resources_by_type


def _expected_marker_totals(
    resources_by_type: Mapping[str, Mapping[str, Any]],
) -> dict[str, int]:
    checkpoint_rows = sum(
        resource["retained_rows"] for resource in resources_by_type.values()
    )
    return {
        "checkpoint_count": len(resources_by_type),
        "checkpoint_pages_processed": sum(
            resource["checkpoint_pages"]
            for resource in resources_by_type.values()
        ),
        "diagnostic_pages_processed": sum(
            resource["diagnostic_pages"]
            for resource in resources_by_type.values()
        ),
        "terminal_page_delta": sum(
            resource["page_delta"] for resource in resources_by_type.values()
        ),
        "checkpoint_rows_processed": checkpoint_rows,
        "resource_count": checkpoint_rows,
        "proof_row_count": checkpoint_rows,
    }


def _has_valid_marker_totals_and_hashes(
    marker_by_field: Mapping[str, Any],
    resources_by_type: Mapping[str, Mapping[str, Any]],
) -> bool:
    expected_total_by_field = _expected_marker_totals(resources_by_type)
    try:
        if any(
            _nonnegative_integer(marker_by_field.get(field_name))
            != expected_value
            for field_name, expected_value in expected_total_by_field.items()
        ):
            return False
        proof_shard_count = _nonnegative_integer(
            marker_by_field.get("proof_shard_count")
        )
    except ReviewedSubsetTerminalDispositionError:
        return False
    hash_field_names = (
        "source_diagnostics_sha256",
        "source_import_sha256",
        "candidate_metadata_sha256",
    )
    return bool(
        all(
            _HEX_SHA256.fullmatch(str(marker_by_field.get(field_name) or ""))
            is not None
            for field_name in hash_field_names
        )
        and marker_by_field["terminal_page_delta"] == 1
        and marker_by_field["resource_count"] > 0
        and proof_shard_count > 0
    )


def _validated_legacy_terminal_disposition_marker(
    marker_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate exact marker fields, disposition partition, and totals."""

    if (
        not isinstance(marker_by_field, Mapping)
        or set(marker_by_field) != TERMINAL_MARKER_FIELDS
        or marker_by_field.get("contract_version")
        != TERMINAL_DISPOSITION_CONTRACT_VERSION
        or marker_by_field.get("reason_code")
        != TERMINAL_DISPOSITION_REASON_CODE
        or marker_by_field.get("resource_types")
        != list(EXPECTED_RESOURCE_TYPES)
        or _HEX_SHA256.fullmatch(
            str(marker_by_field.get("source_scope_sha256") or "")
        )
        is None
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    resources_by_type = _validated_marker_resources(marker_by_field)
    if not _has_valid_marker_totals_and_hashes(
        marker_by_field,
        resources_by_type,
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    return dict(marker_by_field)


def validated_terminal_disposition_marker(
    marker_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate either supported contract in the terminal marker envelope."""

    if (
        isinstance(marker_by_field, Mapping)
        and marker_by_field.get("contract_version") == DIRECT_V4_CONTRACT_VERSION
    ):
        from process.provider_directory_fhir_subset_terminal_disposition_v4_contract import (
            validated_direct_v4_terminal_marker,
        )

        return validated_direct_v4_terminal_marker(marker_by_field)
    return _validated_legacy_terminal_disposition_marker(marker_by_field)


__all__ = (
    "COUNT_DRIFT_DISPOSITION",
    "COUNT_DRIFT_RESOURCE_TYPES",
    "EXPECTED_RESOURCE_TYPES",
    "EXPECTED_DISPOSITION_BY_RESOURCE_TYPE",
    "RETRYABLE_HTTP_500_DISPOSITION",
    "RETRYABLE_HTTP_500_RESOURCE_TYPES",
    "ReviewedSubsetTerminalDispositionError",
    "ReviewedSubsetTerminalDispositionResult",
    "ReviewedSubsetTerminalDispositionSelection",
    "STABLE_COMPLETE_DISPOSITION",
    "STABLE_COMPLETE_RESOURCE_TYPES",
    "TERMINAL_DISPOSITION_CHECKPOINT_STATE",
    "TERMINAL_DISPOSITION_ENABLED_ENV",
    "TERMINAL_DISPOSITION_METADATA_KEY",
    "TERMINAL_DISPOSITION_STATUS",
    "TERMINAL_DISPOSITION_TIMEOUT_SECONDS",
    "require_reviewed_subset_terminal_disposition_gate",
    "terminal_disposition_marker",
    "validated_terminal_disposition_marker",
)
