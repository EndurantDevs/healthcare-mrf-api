# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed contract for reviewed subset acquisition abandonment."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
import re
from typing import Any, Mapping, Sequence

from process.provider_directory_fhir_census_execution import (
    CURRENT_VERSION_CENSUS_BLOCKED_ERROR,
    SERVER_ISSUED_SUBSET_FETCH_MODE,
)

ABANDONMENT_CONTRACT_VERSION = (
    "healthporta.provider-directory.reviewed-subset-abandonment.v1"
)
ABANDONMENT_METADATA_KEY = "provider_directory_reviewed_subset_abandonment_v1"
ABANDONMENT_REASON_CODE = "expired_server_cursor"
ABANDONMENT_ENABLED_ENV = (
    "HLTHPRT_PROVIDER_DIRECTORY_REVIEWED_SUBSET_ABANDONMENT_ENABLED"
)
ABANDONMENT_TIMEOUT_SECONDS = 120
ABANDONED_STATUS = "acquisition_abandoned"
ABANDONED_CHECKPOINT_STATE = "acquisition_abandoned"
ELIGIBLE_PRIOR_STATUSES = frozenset({"acquiring", "incomplete", "failed"})
_HEX_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_MARKER_FIELDS = frozenset(
    {
        "contract_version",
        "reason_code",
        "source_scope_sha256",
        "resource_types",
        "terminal_error_codes",
        "checkpoint_count",
        "pages_processed",
        "rows_processed",
        "resource_count",
        "proof_shard_count",
        "proof_row_count",
    }
)


class ReviewedSubsetAbandonmentError(RuntimeError):
    """Fail with a stable redacted operator error code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class ReviewedSubsetAbandonmentResult:
    """Report whether the exact retained root changed state."""

    abandoned: bool

    def __post_init__(self) -> None:
        if type(self.abandoned) is not bool:
            raise ReviewedSubsetAbandonmentError("state")

    @property
    def is_already_applied(self) -> bool:
        """Return whether the exact sealed disposition already existed."""

        return not self.abandoned


@dataclass(frozen=True)
class ReviewedSubsetAbandonmentSelection:
    """Bind one private retained root to its closed abandonment evidence."""

    source_id: str
    endpoint_id: str
    dataset_id: str
    acquisition_root_run_id: str
    owner_run_id: str
    canonical_api_base: str
    source_scope_sha256: str
    resource_types: tuple[str, ...]
    marker_by_field: dict[str, Any]
    diagnostic_by_resource: dict[str, dict[str, Any]]
    prior_status: str
    observed_resource_count: int
    observed_metadata: dict[str, Any]

    def __post_init__(self) -> None:
        private_text_fields = (
            self.source_id,
            self.endpoint_id,
            self.dataset_id,
            self.acquisition_root_run_id,
            self.owner_run_id,
            self.canonical_api_base,
        )
        if (
            any(type(value) is not str or not value for value in private_text_fields)
            or self.prior_status not in (ELIGIBLE_PRIOR_STATUSES | {ABANDONED_STATUS})
            or not self.resource_types
            or tuple(sorted(set(self.resource_types))) != self.resource_types
            or _HEX_SHA256.fullmatch(self.source_scope_sha256) is None
            or type(self.observed_resource_count) is not int
            or self.observed_resource_count < 0
            or not isinstance(self.observed_metadata, dict)
        ):
            raise ReviewedSubsetAbandonmentError("evidence")
        marker_by_field = validated_abandonment_marker(self.marker_by_field)
        if (
            marker_by_field["source_scope_sha256"] != self.source_scope_sha256
            or tuple(marker_by_field["resource_types"]) != self.resource_types
        ):
            raise ReviewedSubsetAbandonmentError("evidence")


def require_reviewed_subset_abandonment_gate() -> None:
    """Require an explicit one-shot operator gate."""

    if os.getenv(ABANDONMENT_ENABLED_ENV) != "true":
        raise ReviewedSubsetAbandonmentError("disabled")


def _text(value: object) -> str | None:
    return value if type(value) is str and value and value == value.strip() else None


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if type(value) is str:
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            raise ReviewedSubsetAbandonmentError("evidence") from None
        if isinstance(decoded, Mapping):
            return dict(decoded)
    raise ReviewedSubsetAbandonmentError("evidence")


def _json_text_tuple(value: Any) -> tuple[str, ...]:
    if type(value) is str:
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            raise ReviewedSubsetAbandonmentError("evidence") from None
    if not isinstance(value, list) or any(
        type(entry) is not str or not entry or entry != entry.strip() for entry in value
    ):
        raise ReviewedSubsetAbandonmentError("evidence")
    return tuple(value)


def _row_mapping(row: object) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    row_mapping = getattr(row, "_mapping", None)
    if isinstance(row_mapping, Mapping):
        return dict(row_mapping)
    raise ReviewedSubsetAbandonmentError("state")


def _schema_name() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise ReviewedSubsetAbandonmentError("state")
    schema_name = runtime_schema or legacy_schema or "mrf"
    if _IDENTIFIER.fullmatch(schema_name) is None:
        raise ReviewedSubsetAbandonmentError("state")
    return schema_name


def _quoted_relation(table_name: str) -> str:
    if _IDENTIFIER.fullmatch(table_name) is None:
        raise ReviewedSubsetAbandonmentError("state")
    return f'"{_schema_name()}"."{table_name}"'


def _nonnegative_integer(value: Any) -> int:
    if type(value) is not int or value < 0:
        raise ReviewedSubsetAbandonmentError("evidence")
    return value


def terminal_error_code(diagnostic: Mapping[str, Any]) -> str:
    """Return the sole admitted permanent cursor status without transport detail."""

    error_text = diagnostic.get("error")
    if type(error_text) is not str:
        raise ReviewedSubsetAbandonmentError("evidence")
    if error_text in {
        "http_410",
        f"{CURRENT_VERSION_CENSUS_BLOCKED_ERROR}:http_410",
    }:
        return "http_410"
    raise ReviewedSubsetAbandonmentError("evidence")


def validated_terminal_diagnostics(
    diagnostics: Mapping[str, Any],
    resource_types: Sequence[str],
) -> dict[str, dict[str, Any]]:
    """Require one permanent reviewed-cursor failure for every resource."""

    expected_resource_types = tuple(sorted(set(resource_types)))
    if not expected_resource_types or set(diagnostics) != set(expected_resource_types):
        raise ReviewedSubsetAbandonmentError("evidence")
    diagnostics_by_resource: dict[str, dict[str, Any]] = {}
    for resource_type in expected_resource_types:
        diagnostic = diagnostics.get(resource_type)
        if not isinstance(diagnostic, Mapping):
            raise ReviewedSubsetAbandonmentError("evidence")
        if (
            diagnostic.get("fetch_mode") != SERVER_ISSUED_SUBSET_FETCH_MODE
            or diagnostic.get("complete") is not False
            or diagnostic.get("bounded") is not False
        ):
            raise ReviewedSubsetAbandonmentError("evidence")
        terminal_error_code(diagnostic)
        diagnostics_by_resource[resource_type] = dict(diagnostic)
    return diagnostics_by_resource


def abandonment_marker(
    *,
    source_scope_sha256: str,
    resource_types: Sequence[str],
    checkpoint_count: int,
    pages_processed: int,
    rows_processed: int,
    resource_count: int,
    proof_shard_count: int,
    proof_row_count: int,
) -> dict[str, Any]:
    """Build the closed, identifier-free retained-evidence marker."""

    normalized_resources = tuple(sorted(set(resource_types)))
    marker_by_field = {
        "contract_version": ABANDONMENT_CONTRACT_VERSION,
        "reason_code": ABANDONMENT_REASON_CODE,
        "source_scope_sha256": source_scope_sha256,
        "resource_types": list(normalized_resources),
        "terminal_error_codes": dict.fromkeys(
            normalized_resources,
            "http_410",
        ),
        "checkpoint_count": checkpoint_count,
        "pages_processed": pages_processed,
        "rows_processed": rows_processed,
        "resource_count": resource_count,
        "proof_shard_count": proof_shard_count,
        "proof_row_count": proof_row_count,
    }
    return validated_abandonment_marker(marker_by_field)


def validated_abandonment_marker(
    marker_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate exact fields and neutral counts in one stored marker."""

    if (
        not isinstance(marker_by_field, Mapping)
        or set(marker_by_field) != _MARKER_FIELDS
    ):
        raise ReviewedSubsetAbandonmentError("evidence")
    resource_types = marker_by_field.get("resource_types")
    terminal_codes = marker_by_field.get("terminal_error_codes")
    if (
        marker_by_field.get("contract_version") != ABANDONMENT_CONTRACT_VERSION
        or marker_by_field.get("reason_code") != ABANDONMENT_REASON_CODE
        or _HEX_SHA256.fullmatch(str(marker_by_field.get("source_scope_sha256") or ""))
        is None
        or not isinstance(resource_types, list)
        or not resource_types
        or any(
            type(resource_type) is not str or not resource_type
            for resource_type in resource_types
        )
        or sorted(set(resource_types)) != resource_types
        or not isinstance(terminal_codes, Mapping)
        or set(terminal_codes) != set(resource_types)
        or any(code != "http_410" for code in terminal_codes.values())
    ):
        raise ReviewedSubsetAbandonmentError("evidence")
    for field_name in (
        "checkpoint_count",
        "pages_processed",
        "rows_processed",
        "resource_count",
        "proof_shard_count",
        "proof_row_count",
    ):
        _nonnegative_integer(marker_by_field.get(field_name))
    if (
        marker_by_field["checkpoint_count"] != len(resource_types)
        or marker_by_field["rows_processed"] != marker_by_field["resource_count"]
        or marker_by_field["proof_row_count"] != marker_by_field["resource_count"]
    ):
        raise ReviewedSubsetAbandonmentError("evidence")
    return dict(marker_by_field)


def abandonment_result_json(
    abandonment_result: ReviewedSubsetAbandonmentResult,
) -> str:
    """Render a closed selector-free success result."""

    if type(abandonment_result) is not ReviewedSubsetAbandonmentResult:
        raise ReviewedSubsetAbandonmentError("state")
    return json.dumps(
        {
            "abandoned": abandonment_result.abandoned,
            "already_applied": abandonment_result.is_already_applied,
            "status": "ok",
        },
        sort_keys=True,
        separators=(",", ":"),
    )
