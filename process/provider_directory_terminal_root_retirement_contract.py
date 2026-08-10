# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed contract for one exact terminal Provider Directory root retirement."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
import json
import os
import re
from typing import Any, Mapping


RETIREMENT_STATUS = "acquisition_retired"
RETIREMENT_METADATA_KEY = "provider_directory_terminal_root_retirement_v1"
RETIREMENT_CONTRACT_VERSION = (
    "healthporta.provider-directory.terminal-root-retirement.v1"
)
RETIREMENT_REASON_CODE = "terminal_retry_lineage_exhausted"
RETIREMENT_RESOURCE_HASH_CONTRACT = "transport_bound_v1"
RETIREMENT_ENABLED_ENV = (
    "HLTHPRT_PROVIDER_DIRECTORY_TERMINAL_ROOT_RETIREMENT_ENABLED"
)
RETIREMENT_TIMEOUT_SECONDS = 180
MINIMUM_TERMINAL_AGE_SECONDS = 15 * 60
ELIGIBLE_PRIOR_STATUSES = frozenset({"acquiring"})
TERMINAL_FAILURE_STATUSES = frozenset(
    {"canceled", "cancelled", "dead_letter", "failed"}
)
RETIREMENT_VALID_FUNCTION = "provider_directory_terminal_root_retirement_valid"
RETIREMENT_EVIDENCE_FUNCTION = (
    "provider_directory_terminal_root_retirement_evidence"
)
REQUIRED_CHILD_RELATIONS = frozenset(
    {
        "provider_directory_bulk_acquisition_checkpoint",
        "provider_directory_bulk_output_checkpoint",
        "provider_directory_dataset_affiliation_organization",
        "provider_directory_dataset_insurance_plan",
        "provider_directory_dataset_network_plan",
        "provider_directory_dataset_proof_shard",
        "provider_directory_dataset_rehydration_checkpoint",
        "provider_directory_dataset_resource",
        "provider_directory_endpoint_dataset_previous_reference",
        "provider_directory_pagination_checkpoint",
        "provider_directory_uhc_flex_npi_cohort",
        "provider_directory_uhc_flex_practitioner_dataset",
        "provider_directory_uhc_flex_practitioner_dataset_resource",
    }
)

_HEX_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_MARKER_FIELDS = frozenset(
    {
        "contract_version",
        "evidence",
        "minimum_terminal_age_seconds",
        "reason_code",
        "retired_at",
    }
)
_EVIDENCE_FIELDS = frozenset(
    {
        "actual_resource_count",
        "child_relations",
        "lineage_finished_at",
        "lineage_sha256",
        "parent_identity_sha256",
        "parent_resource_count",
        "predecessor_identity_sha256",
        "prior_status",
        "proof_shard_count",
        "proof_row_count",
        "resource_counts",
        "source_identity_sha256",
        "target_identity_sha256",
        "terminal_run_count",
    }
)
_RELATION_EVIDENCE_FIELDS = frozenset({"row_count", "row_sha256"})


class TerminalRootRetirementError(RuntimeError):
    """Fail with one stable identifier-free operator error code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _clean_text(value: object, *, maximum_length: int) -> str | None:
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or len(value) > maximum_length
    ):
        return None
    return value


@dataclass(frozen=True)
class TerminalRootRetirementRequest:
    """Bind every private selector needed for one exact retirement."""

    source_id: str
    endpoint_id: str
    dataset_id: str
    acquisition_root_run_id: str
    owner_run_id: str
    expected_current_dataset_id: str
    expected_evidence_sha256: str | None = None
    minimum_terminal_age_seconds: int = MINIMUM_TERMINAL_AGE_SECONDS

    def __post_init__(self) -> None:
        selector_limits = (
            (self.source_id, 64),
            (self.endpoint_id, 64),
            (self.dataset_id, 96),
            (self.acquisition_root_run_id, 64),
            (self.owner_run_id, 64),
            (self.expected_current_dataset_id, 96),
        )
        if (
            any(
                _clean_text(value, maximum_length=limit) is None
                for value, limit in selector_limits
            )
            or self.dataset_id == self.expected_current_dataset_id
            or (
                self.expected_evidence_sha256 is not None
                and _HEX_SHA256.fullmatch(self.expected_evidence_sha256) is None
            )
            or type(self.minimum_terminal_age_seconds) is not int
            or self.minimum_terminal_age_seconds < MINIMUM_TERMINAL_AGE_SECONDS
            or self.minimum_terminal_age_seconds > 7 * 24 * 60 * 60
        ):
            raise TerminalRootRetirementError("request_invalid")


@dataclass(frozen=True)
class TerminalRootRetirementSelection:
    """Seal one locked parent snapshot and its exact evidence marker."""

    request: TerminalRootRetirementRequest
    canonical_api_base: str
    prior_status: str
    observed_metadata: dict[str, Any]
    marker_by_field: dict[str, Any]

    def __post_init__(self) -> None:
        if (
            type(self.request) is not TerminalRootRetirementRequest
            or _clean_text(self.canonical_api_base, maximum_length=4096) is None
            or self.prior_status
            not in (ELIGIBLE_PRIOR_STATUSES | {RETIREMENT_STATUS})
            or not isinstance(self.observed_metadata, dict)
        ):
            raise TerminalRootRetirementError("evidence_invalid")
        validated_retirement_marker(self.marker_by_field)


@dataclass(frozen=True)
class TerminalRootRetirementResult:
    """Report whether the exact parent changed state."""

    retired: bool
    marker_sha256: str

    def __post_init__(self) -> None:
        if (
            type(self.retired) is not bool
            or _HEX_SHA256.fullmatch(self.marker_sha256) is None
        ):
            raise TerminalRootRetirementError("state_invalid")

    @property
    def is_already_applied(self) -> bool:
        """Return whether the exact sealed retirement already existed."""

        return not self.retired


def require_terminal_root_retirement_gate() -> None:
    """Require one explicit process-local operator gate."""

    if os.getenv(RETIREMENT_ENABLED_ENV) != "true":
        raise TerminalRootRetirementError("disabled")


def schema_name() -> str:
    """Return one identifier-safe runtime schema."""

    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise TerminalRootRetirementError("state_invalid")
    selected_schema = runtime_schema or legacy_schema or "mrf"
    if _IDENTIFIER.fullmatch(selected_schema) is None:
        raise TerminalRootRetirementError("state_invalid")
    return selected_schema


def quoted_relation(relation_name: str) -> str:
    """Return one schema-qualified trusted relation name."""

    if _IDENTIFIER.fullmatch(relation_name) is None:
        raise TerminalRootRetirementError("state_invalid")
    return f'"{schema_name()}"."{relation_name}"'


def row_mapping(row: object) -> dict[str, Any]:
    """Decode one supported database row representation."""

    if isinstance(row, Mapping):
        return dict(row)
    mapped_row = getattr(row, "_mapping", None)
    if isinstance(mapped_row, Mapping):
        return dict(mapped_row)
    raise TerminalRootRetirementError("state_invalid")


def json_object(value: Any) -> dict[str, Any]:
    """Decode one JSON object without accepting scalar coercions."""

    if isinstance(value, Mapping):
        return dict(value)
    if type(value) is str:
        try:
            decoded_value = json.loads(value)
        except json.JSONDecodeError:
            raise TerminalRootRetirementError("evidence_invalid") from None
        if isinstance(decoded_value, Mapping):
            return dict(decoded_value)
    raise TerminalRootRetirementError("evidence_invalid")


def retirement_resource_hash_contract(
    publication_metadata: Mapping[str, Any],
) -> str:
    """Normalize only the historic absent-or-explicit v1 marker shape."""

    metadata_by_field = dict(publication_metadata)
    if "resource_hash_contract" not in metadata_by_field:
        return RETIREMENT_RESOURCE_HASH_CONTRACT
    if (
        metadata_by_field.get("resource_hash_contract")
        == RETIREMENT_RESOURCE_HASH_CONTRACT
    ):
        return RETIREMENT_RESOURCE_HASH_CONTRACT
    raise TerminalRootRetirementError("evidence_invalid")


def canonical_json_sha256(value: Any) -> str:
    """Hash one JSON-compatible value with the repository canonical shape."""

    try:
        encoded_value = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    except (TypeError, ValueError):
        raise TerminalRootRetirementError("evidence_invalid") from None
    return sha256(encoded_value).hexdigest()


def _nonnegative_integer(value: Any) -> int:
    if type(value) is not int or value < 0:
        raise TerminalRootRetirementError("evidence_invalid")
    return value


def _validated_timestamp(value: Any) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise TerminalRootRetirementError("evidence_invalid")
    try:
        parsed_value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise TerminalRootRetirementError("evidence_invalid") from None
    if parsed_value.tzinfo is None:
        raise TerminalRootRetirementError("evidence_invalid")
    return value


def _validated_resource_counts(value: Any) -> dict[str, int]:
    if not isinstance(value, Mapping):
        raise TerminalRootRetirementError("evidence_invalid")
    count_by_resource = dict(value)
    if (
        not count_by_resource
        or any(
            _clean_text(name, maximum_length=64) is None
            for name in count_by_resource
        )
    ):
        raise TerminalRootRetirementError("evidence_invalid")
    for count in count_by_resource.values():
        _nonnegative_integer(count)
    return dict(sorted(count_by_resource.items()))


def _validated_relation_evidence(value: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(value, Mapping):
        raise TerminalRootRetirementError("evidence_invalid")
    evidence_by_relation = dict(value)
    if set(evidence_by_relation) != REQUIRED_CHILD_RELATIONS:
        raise TerminalRootRetirementError("evidence_invalid")
    for relation_name, raw_evidence in evidence_by_relation.items():
        if (
            _IDENTIFIER.fullmatch(str(relation_name)) is None
            or not isinstance(raw_evidence, Mapping)
            or set(raw_evidence) != _RELATION_EVIDENCE_FIELDS
            or _HEX_SHA256.fullmatch(str(raw_evidence.get("row_sha256") or ""))
            is None
        ):
            raise TerminalRootRetirementError("evidence_invalid")
        _nonnegative_integer(raw_evidence.get("row_count"))
    return dict(sorted({
        str(name): dict(evidence)
        for name, evidence in evidence_by_relation.items()
    }.items()))


def validated_retirement_evidence(raw_evidence: Any) -> dict[str, Any]:
    """Validate the exact identifier-free database evidence envelope."""

    evidence_by_field = json_object(raw_evidence)
    digest_fields = (
        "lineage_sha256",
        "parent_identity_sha256",
        "predecessor_identity_sha256",
        "source_identity_sha256",
        "target_identity_sha256",
    )
    if (
        set(evidence_by_field) != _EVIDENCE_FIELDS
        or any(
            _HEX_SHA256.fullmatch(str(evidence_by_field.get(field_name) or ""))
            is None
            for field_name in digest_fields
        )
    ):
        raise TerminalRootRetirementError("evidence_invalid")
    _validated_timestamp(evidence_by_field.get("lineage_finished_at"))
    terminal_run_count = _nonnegative_integer(
        evidence_by_field.get("terminal_run_count")
    )
    actual_resource_count = _nonnegative_integer(
        evidence_by_field.get("actual_resource_count")
    )
    parent_resource_count = _nonnegative_integer(
        evidence_by_field.get("parent_resource_count")
    )
    proof_shard_count = _nonnegative_integer(
        evidence_by_field.get("proof_shard_count")
    )
    proof_row_count = _nonnegative_integer(
        evidence_by_field.get("proof_row_count")
    )
    if (
        terminal_run_count == 0
        or evidence_by_field.get("prior_status") != "acquiring"
    ):
        raise TerminalRootRetirementError("evidence_invalid")
    resource_counts = _validated_resource_counts(
        evidence_by_field.get("resource_counts")
    )
    relation_evidence = _validated_relation_evidence(
        evidence_by_field.get("child_relations")
    )
    evidence_by_field["resource_counts"] = resource_counts
    evidence_by_field["child_relations"] = relation_evidence
    evidence_by_field["actual_resource_count"] = actual_resource_count
    evidence_by_field["parent_resource_count"] = parent_resource_count
    evidence_by_field["proof_shard_count"] = proof_shard_count
    evidence_by_field["proof_row_count"] = proof_row_count
    return evidence_by_field


def retirement_marker(
    evidence_by_field: Mapping[str, Any],
    *,
    minimum_terminal_age_seconds: int,
    retired_at: str,
) -> dict[str, Any]:
    """Build the closed marker inserted by the single parent CAS."""

    marker_by_field = {
        "contract_version": RETIREMENT_CONTRACT_VERSION,
        "evidence": validated_retirement_evidence(evidence_by_field),
        "minimum_terminal_age_seconds": minimum_terminal_age_seconds,
        "reason_code": RETIREMENT_REASON_CODE,
        "retired_at": retired_at,
    }
    return validated_retirement_marker(marker_by_field)


def validated_retirement_marker(value: Any) -> dict[str, Any]:
    """Validate one exact stored retirement marker."""

    marker_by_field = json_object(value)
    if (
        set(marker_by_field) != _MARKER_FIELDS
        or marker_by_field.get("contract_version")
        != RETIREMENT_CONTRACT_VERSION
        or marker_by_field.get("reason_code") != RETIREMENT_REASON_CODE
    ):
        raise TerminalRootRetirementError("evidence_invalid")
    minimum_age = _nonnegative_integer(
        marker_by_field.get("minimum_terminal_age_seconds")
    )
    if minimum_age < MINIMUM_TERMINAL_AGE_SECONDS:
        raise TerminalRootRetirementError("evidence_invalid")
    _validated_timestamp(marker_by_field.get("retired_at"))
    marker_by_field["evidence"] = validated_retirement_evidence(
        marker_by_field.get("evidence")
    )
    return marker_by_field


def retirement_result_json(result: TerminalRootRetirementResult) -> str:
    """Render a closed result without private selectors."""

    if type(result) is not TerminalRootRetirementResult:
        raise TerminalRootRetirementError("state_invalid")
    return json.dumps(
        {
            "already_applied": result.is_already_applied,
            "marker_sha256": result.marker_sha256,
            "retired": result.retired,
            "status": "ok",
        },
        sort_keys=True,
        separators=(",", ":"),
    )


__all__ = (
    "ELIGIBLE_PRIOR_STATUSES",
    "MINIMUM_TERMINAL_AGE_SECONDS",
    "RETIREMENT_CONTRACT_VERSION",
    "RETIREMENT_ENABLED_ENV",
    "RETIREMENT_EVIDENCE_FUNCTION",
    "RETIREMENT_METADATA_KEY",
    "RETIREMENT_RESOURCE_HASH_CONTRACT",
    "RETIREMENT_STATUS",
    "RETIREMENT_TIMEOUT_SECONDS",
    "RETIREMENT_VALID_FUNCTION",
    "TERMINAL_FAILURE_STATUSES",
    "TerminalRootRetirementError",
    "TerminalRootRetirementRequest",
    "TerminalRootRetirementResult",
    "TerminalRootRetirementSelection",
    "canonical_json_sha256",
    "json_object",
    "quoted_relation",
    "require_terminal_root_retirement_gate",
    "retirement_marker",
    "retirement_resource_hash_contract",
    "retirement_result_json",
    "row_mapping",
    "schema_name",
    "validated_retirement_evidence",
    "validated_retirement_marker",
)
