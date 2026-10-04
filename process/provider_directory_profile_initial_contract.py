# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit first-publication contracts; never an inferred serving predecessor."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Mapping

from process.provider_directory_profile_capacity_runtime_types import ProviderDirectoryProfileCapacityGeometryInputs
from process.provider_directory_profile_capacity_types import ProviderDirectoryProfileCapacityGeometry

MATERIALIZATION = "initial_full_swap"
REQUEST_CONTRACT = "healthporta.provider-directory-profile-capacity-preflight-request.v5"
RECEIPT_CONTRACT = "healthporta.provider-directory-profile-capacity-preflight.v5"
PROJECTION_REQUEST_CONTRACT = "healthporta.provider-directory-profile-capacity-authority-projection-request.v3"
PROJECTION_CONTRACT = "healthporta.provider-directory-profile-capacity-authority-projection.v3"
CONTROL_REQUEST_CONTRACT = "healthporta.provider-directory-profile-capacity-control-plane-preflight-request.v3"
CONTROL_RECEIPT_CONTRACT = "healthporta.provider-directory-profile-capacity-control-plane-preflight.v3"
GEOMETRY_CONTRACT = "healthporta.provider-directory-profile-initial-capacity-geometry.v1"
GEOMETRY_DOMAIN = "provider_directory_profile_initial_capacity_geometry.v1"
TARGET_CONTRACT = "healthporta.provider-directory-profile-initial-target-state.v1"
COMMIT_CONTRACT = "healthporta.provider-directory-profile-initial-receipt.v1"
CONTROL_WAL_CONTRACT = "healthporta.provider-directory-profile-initial-control-wal.v1"
FORECAST_CONTRACT = "healthporta.provider-directory-profile-initial-cutover-forecast.v1"
ACTUAL_CONTRACT = "healthporta.provider-directory-profile-initial-cutover-actual.v1"
INITIAL_CUTOVER_ATTEMPTS = 3
INITIAL_CUTOVER_STATEMENTS = 15
RECEIPT_TABLE = "provider_directory_profile_initial_receipt"
INITIAL_FIELDS = frozenset(
    {"initial_target_state_sha256", "initial_receipt_oid", "initial_receipt_storage_fingerprint"}
)
TARGET_FIELDS = frozenset(
    {
        "contract_id",
        "resolution",
        "serving_singleton_absent",
        "initial_commit_receipt_absent",
        "evidence_target_oid",
        "profile_target_oid",
        "evidence_target_storage_fingerprint",
        "profile_target_storage_fingerprint",
        "evidence_target_bytes",
        "profile_target_bytes",
        "evidence_rows",
        "profile_rows",
        "historical_publication",
    }
)
HISTORY_FIELDS = frozenset({"run_id", "result_sha256", "result", "profile_as_of", "temporal_metadata"})
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_RESULT_IDENTITIES = (
    "proof_id",
    "node_id",
    "catalog_digest",
    "selection_fingerprint",
    "authority_revision",
    "profile_schema_version",
    "profile_strategy_version",
    "source_context_digest",
    "profile_input_digest",
    "operation",
    "pairs",
)


@dataclass(frozen=True)
class InitialCapacityGeometry(ProviderDirectoryProfileCapacityGeometry):
    initial_target_state_sha256: str
    initial_receipt_oid: int
    initial_receipt_storage_fingerprint: str

    @property
    def cutover_forecast_contract_id(self):
        """Identify the initial full-swap forecast contract."""
        return FORECAST_CONTRACT

    @property
    def cutover_actual_contract_id(self):
        """Identify the initial full-swap observed receipt contract."""
        return ACTUAL_CONTRACT


@dataclass(frozen=True)
class InitialCapacityGeometryInputs(ProviderDirectoryProfileCapacityGeometryInputs):
    initial_target_state_sha256: str
    initial_receipt_oid: int
    initial_receipt_storage_fingerprint: str


@dataclass(frozen=True)
class InitialTargets:
    """Physical targets only: deliberately has no serving generation or date."""

    evidence_target_oid: int
    profile_target_oid: int
    payload: dict[str, Any]


def canonical_json(value: Any) -> str:
    """Serialize the canonical ASCII JSON used by the signed initial contracts."""
    return json.dumps(value, ensure_ascii=True, allow_nan=False, sort_keys=True, separators=(",", ":"))


def result_sha256(result: Mapping[str, Any]) -> str:
    """Hash the complete historical producer result without a domain prefix."""
    return hashlib.sha256(canonical_json(result).encode("ascii")).hexdigest()


def target_state_sha256(payload: Mapping[str, Any]) -> str:
    """Hash the closed target snapshot with its contract domain and NUL separator."""
    return hashlib.sha256(TARGET_CONTRACT.encode("ascii") + b"\0" + canonical_json(payload).encode("ascii")).hexdigest()


def _invalid(reason: str) -> None:
    raise ValueError("provider_directory_profile_initial_" + reason)


def validated_legacy_result(raw: Any) -> dict[str, Any]:
    """Validate exactly the original result-v1 producer, without inventing its date."""
    from process.provider_directory_profile_selection_contract import (
        PROFILE_SELECTION_ATTESTATION_CONTRACT_ID,
        PROFILE_SELECTION_RESULT_CONTRACT_ID,
        ProviderDirectoryProfileExecution,
        _profile_result_counts,
        validated_profile_selection_attestation,
    )

    fields = {*_RESULT_IDENTITIES, "contract_id", "status", "generation", "profile_generation_id", "row_counts"}
    if not isinstance(raw, Mapping) or set(raw) != fields:
        _invalid("legacy_result_fields_invalid")
    result_by_field = dict(raw)
    attestation = validated_profile_selection_attestation(
        {
            "contract_id": PROFILE_SELECTION_ATTESTATION_CONTRACT_ID,
            **{key: result_by_field[key] for key in _RESULT_IDENTITIES},
        }
    )
    generation = result_by_field["generation"]
    counts = result_by_field["row_counts"]
    if (
        result_by_field["contract_id"] != PROFILE_SELECTION_RESULT_CONTRACT_ID
        or result_by_field["status"] != "published"
        or result_by_field["operation"] != "publish"
        or type(generation) is not int
        or generation < 1
        or not isinstance(result_by_field["profile_generation_id"], str)
        or re.fullmatch(r"pdprofile_[0-9a-f]{32}", result_by_field["profile_generation_id"]) is None
        or not isinstance(counts, Mapping)
    ):
        _invalid("legacy_result_invalid")
    expected_counts = _profile_result_counts(
        ProviderDirectoryProfileExecution(attestation=attestation, generation=generation),
        counts.get("profile_rows"),
        counts.get("profile_source_evidence_rows"),
    )
    if counts != expected_counts or any(result_by_field[key] != attestation.payload[key] for key in _RESULT_IDENTITIES):
        _invalid("legacy_result_invalid")
    return result_by_field


def validated_target_state(raw: Any) -> dict[str, Any]:
    """Validate an empty target pair or authentic undated historical publication."""
    if not isinstance(raw, Mapping) or set(raw) != TARGET_FIELDS:
        _invalid("target_fields_invalid")
    target_by_field = dict(raw)
    if (
        target_by_field["contract_id"] != TARGET_CONTRACT
        or target_by_field["serving_singleton_absent"] is not True
        or target_by_field["initial_commit_receipt_absent"] is not True
    ):
        _invalid("target_absence_invalid")
    for key in ("evidence_target_oid", "profile_target_oid"):
        if type(target_by_field[key]) is not int or not 0 < target_by_field[key] < 2**32:
            _invalid("target_oid_invalid")
    if target_by_field["evidence_target_oid"] == target_by_field["profile_target_oid"]:
        _invalid("target_oid_collision")
    for key in ("evidence_target_storage_fingerprint", "profile_target_storage_fingerprint"):
        if not isinstance(target_by_field[key], str) or not _HASH.fullmatch(target_by_field[key]):
            _invalid("target_fingerprint_invalid")
    for key in ("evidence_target_bytes", "profile_target_bytes", "evidence_rows", "profile_rows"):
        if type(target_by_field[key]) is not int or not 0 <= target_by_field[key] < 2**63:
            _invalid("target_size_invalid")
    history = target_by_field["historical_publication"]
    if target_by_field["resolution"] == "empty":
        if history is not None or target_by_field["evidence_rows"] or target_by_field["profile_rows"]:
            _invalid("empty_history_invalid")
    elif target_by_field["resolution"] == "legacy_as_of_unknown":
        if (
            not isinstance(history, Mapping)
            or set(history) != HISTORY_FIELDS
            or history["profile_as_of"] is not None
            or history["temporal_metadata"] != "not_recorded_by_producer"
            or not isinstance(history["run_id"], str)
            or re.fullmatch(r"run_[0-9a-f]{32}", history["run_id"]) is None
        ):
            _invalid("legacy_history_invalid")
        legacy_result = validated_legacy_result(history["result"])
        if (
            target_by_field["profile_rows"] < 1
            or target_by_field["evidence_rows"] < 1
            or history["result_sha256"] != result_sha256(legacy_result)
            or target_by_field["profile_rows"] != legacy_result["row_counts"]["profile_rows"]
            or target_by_field["evidence_rows"] != legacy_result["row_counts"]["profile_source_evidence_rows"]
        ):
            _invalid("legacy_history_binding_invalid")
    else:
        _invalid("resolution_invalid")
    return target_by_field


def is_initial_request(request: Any) -> bool:
    """Recognize the explicit initial materialization field on a validated request."""
    return getattr(request, "request_payload", {}).get("profile_materialization") == MATERIALIZATION


def is_initial_profile_request(request: Any) -> bool:
    """Recognize direct initial requests or their signed paired Profile lease."""
    if is_initial_request(request):
        return True
    admission = getattr(request, "cms_nonprofile_admission", None)
    if not isinstance(admission, Mapping):
        return False
    pair = admission.get("paired_profile_lease", {})
    return execution_initial_requested(type("Pair", (), {"capacity_attestation": pair})())


def is_initial_execution_requested(execution: Any) -> bool:
    """Only a signed request selects initial mode; absence is never authority."""
    envelope = getattr(execution, "capacity_attestation", None)
    if not isinstance(envelope, Mapping):
        return False
    lease = envelope.get("lease")
    guard = lease.get("signing_preflight_guard") if isinstance(lease, Mapping) else None
    request = guard.get("healthcare_request") if isinstance(guard, Mapping) else None
    return bool(
        isinstance(request, Mapping)
        and request.get("contract_id") == REQUEST_CONTRACT
        and request.get("profile_materialization") == MATERIALIZATION
    )


initial_profile_for_request = is_initial_profile_request
execution_initial_requested = is_initial_execution_requested
