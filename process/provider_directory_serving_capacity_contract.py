# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed ordinary serving values; parsing does not grant runtime authority."""

from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from datetime import date

NATIVE_ADDRESS_TARGETS = (
    "entity_address_evidence",
    "entity_address_medication_bridge",
    "entity_address_network_bridge",
    "entity_address_plan_bridge",
    "entity_address_procedure_bridge",
    "entity_address_unified",
    "facility_anchor_npi_candidate",
)

PURPOSE = "serving_nonprofile"
GEOMETRY = "provider-directory-serving-nonprofile-capacity.v1"
SCOPE = "provider-directory-ordinary-serving-scope.v1"
ADMISSION_FIELD = "serving_nonprofile_admission"
EXECUTION_FIELD = "provider_directory_serving_nonprofile_capacity_attestation"
ADMISSION_CONTRACT = "provider-directory-serving-nonprofile-admission-request.v1"
LIMITS_CONTRACT = "provider-directory-serving-nonprofile-limits.v1"
PREFLIGHT_REQUEST = "healthporta.provider-directory-profile-capacity-preflight-request.v6"
PREFLIGHT_RECEIPT = "healthporta.provider-directory-profile-capacity-preflight.v6"
PROJECTION_REQUEST = "healthporta.provider-directory-profile-capacity-authority-projection-request.v4"
PROJECTION_RECEIPT = "healthporta.provider-directory-profile-capacity-authority-projection.v4"
CONTROL_REQUEST = "healthporta.provider-directory-profile-capacity-control-plane-preflight-request.v4"
CONTROL_RECEIPT = "healthporta.provider-directory-profile-capacity-control-plane-preflight.v4"
OPERATOR_CONTRACT = "provider-directory-profile-capacity-operator-input-v5"
BASE_GEOMETRY = "provider-directory-cms-nonprofile-capacity.v1"
HASH = re.compile(r"[0-9a-f]{64}\Z")
PLAN_FIELDS = {
    "selection_proof_id",
    "desired_profile_as_of",
    "desired_fence_hash",
    "artifact_scope_projection_hash",
    "publish_targets",
    "resource_types",
    "batch_size",
    "worker_count",
    "reservation_bytes",
    "minimum_remaining_bytes",
    "required_build_seconds",
    "native_address_targets",
    "native_address_input_hash",
    "temp_file_limit_bytes_per_backend",
    "logging_wal_upper_bound_bytes",
    "cutover_wal_upper_bound_bytes",
    "paired_profile_lease_digest",
}
INPUT_FIELDS = {
    "source_vector_sha256",
    "predecessor_sha256",
    "affected_scope_sha256",
    "retention_plan_sha256",
    "native_address_input_hash",
    "owner_oid",
    "schema_oid",
    "retention_kind",
}
SCOPE_FIELDS = INPUT_FIELDS | {
    "contract_id",
    "input_sha256",
    "max_retention_data_bytes",
    "max_retention_wal_bytes",
    "max_commit_wal_bytes",
    "resident_retention_data_bytes",
    "requested_source_ids",
}
RETENTION_LIMIT_FIELDS = frozenset({"max_retention_data_bytes", "max_retention_wal_bytes", "max_commit_wal_bytes"})


def require(condition, reason):
    """Reject incomplete ordinary authority with a stable contract error."""
    if not condition:
        raise ValueError("ordinary_serving_capacity_" + reason)


def canonical(value):
    """Encode exactly the JSON value covered by the shared capacity digest."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def is_valid_hash(value):
    """Recognize a canonical SHA-256 identity without coercion."""
    return type(value) is str and HASH.fullmatch(value) is not None


def is_positive_int(value):
    """Require a finite PostgreSQL bigint ceiling and reject booleans."""
    return type(value) is int and 0 < value < 2**63


def input_sha256(values):
    """Identify owner-captured Source, predecessor, affected rows and retention work."""
    require(type(values) is dict and set(values) == INPUT_FIELDS, "input_fields")
    for name in INPUT_FIELDS - {"owner_oid", "schema_oid", "retention_kind"}:
        require(is_valid_hash(values[name]), "input_hash")
    for name in ("owner_oid", "schema_oid"):
        require(type(values[name]) is int and 0 < values[name] < 2**32, "input_owner")
    require(values["retention_kind"] in {"snapshot", "delta_preimage"}, "retention_kind")
    return hashlib.sha256(
        b"provider-directory-ordinary-address-input.v1\0" + canonical(values).encode("ascii")
    ).hexdigest()


def validate_scope(raw):
    """Parse signed input identity and ceilings without granting capture authority."""
    require(type(raw) is dict and set(raw) == SCOPE_FIELDS, "scope_fields")
    require(raw["contract_id"] == SCOPE, "scope_contract")
    require(raw["input_sha256"] == input_sha256({name: raw[name] for name in INPUT_FIELDS}), "input_digest")
    validate_retention_limits({name: raw[name] for name in RETENTION_LIMIT_FIELDS})
    resident = raw["resident_retention_data_bytes"]
    require(type(resident) is int and 0 <= resident <= raw["max_retention_data_bytes"], "resident_retention_budget")
    require(raw["retention_kind"] == "snapshot" or resident == 0, "resident_retention_kind")
    require(type(raw["requested_source_ids"]) is list, "refresh_source_ids")
    validate_refresh_source_ids(raw["requested_source_ids"])
    return deepcopy(raw)


def validate_retention_limits(raw):
    """Accept reviewed ceilings only; these fields confer no Source or storage authority."""
    require(type(raw) is dict and set(raw) == RETENTION_LIMIT_FIELDS, "retention_limit_fields")
    require(all(is_positive_int(raw[name]) for name in RETENTION_LIMIT_FIELDS), "retention_budget")
    return deepcopy(raw)


def validate_refresh_source_ids(raw):
    """Requested work may add only named Sources; capture validates their real membership."""
    require(type(raw) in {tuple, list} and len(raw) <= 256, "refresh_source_ids")
    require(all(type(source) is str and 0 < len(source) <= 255 for source in raw), "refresh_source_ids")
    require(list(raw) == sorted(set(raw)), "refresh_source_ids")
    return list(raw)


def require_ordinary_selection(attestation):
    """Call only after the existing complete selection validator succeeds."""
    require(attestation["operation"] == "publish" and bool(attestation["pairs"]), "selection_operation")
    require(attestation.get("desired_cms_dataset") is None, "cms_scope")
    require(attestation.get("expected_cms_incumbent") is None, "cms_scope")
    require(all(pair["source_id"] != "cms-npd" for pair in attestation["pairs"]), "cms_scope")
    as_of = attestation.get("desired_profile_as_of")
    require(type(as_of) is str and date.fromisoformat(as_of).isoformat() == as_of, "explicit_date")
    return as_of


def paired_profile_wal_bytes(receipt):
    """Read the exact paired signed reservation; parsing never authenticates its issuer."""
    value = receipt["required_reservation_bytes_by_storage_class"]["wal"]
    require(is_positive_int(value), "paired_profile_wal")
    return value


def extend_plan_values(base_geometry, raw_scope, *, paired_profile_wal):
    """Reuse engine-produced plan values; add retained bytes exactly once before signing.

    The base object here is arithmetic, never a CMS lease or accepted CMS proof.
    Existing candidate/data/logging/cutover ceilings remain independently available.
    """
    require(type(base_geometry) is dict, "base_geometry")
    require(set(base_geometry) == PLAN_FIELDS | {"contract_id", "admission_purpose"}, "base_fields")
    require(base_geometry["contract_id"] == BASE_GEOMETRY, "base_contract")
    require(base_geometry["admission_purpose"] == "cms_nonprofile", "base_contract")
    scope = validate_scope(raw_scope)
    require(is_positive_int(paired_profile_wal), "paired_profile_wal")
    value = deepcopy(base_geometry)
    reservations = value["reservation_bytes"]
    require(type(reservations) is list and len(reservations) == 3, "reservations")
    require(
        all(type(entry) is list and len(entry) == 2 and is_positive_int(entry[1]) for entry in reservations),
        "reservations",
    )
    require([entry[0] for entry in reservations] == ["data", "temp", "wal"], "reservations")
    reservations[0][1] += scope["max_retention_data_bytes"] - scope["resident_retention_data_bytes"]
    reservations[2][1] += scope["max_retention_wal_bytes"] + scope["max_commit_wal_bytes"] + paired_profile_wal
    value["logging_wal_upper_bound_bytes"] += scope["max_retention_wal_bytes"]
    value["cutover_wal_upper_bound_bytes"] += scope["max_commit_wal_bytes"] + paired_profile_wal
    value.update(contract_id=GEOMETRY, admission_purpose=PURPOSE, ordinary_scope=scope)
    return validate_geometry(value)


def validate_geometry(raw):
    """Validate only the added closed scope; retain shared _validate_plan as well."""
    require(
        type(raw) is dict and set(raw) == PLAN_FIELDS | {"contract_id", "admission_purpose", "ordinary_scope"},
        "geometry_fields",
    )
    require(raw["contract_id"] == GEOMETRY and raw["admission_purpose"] == PURPOSE, "geometry_contract")
    scope = validate_scope(raw["ordinary_scope"])
    require(raw["native_address_input_hash"] == scope["native_address_input_hash"], "native_input_changed")
    require(raw["publish_targets"] == ["address_overlay"], "artifact_targets")
    require(raw["native_address_targets"] == list(NATIVE_ADDRESS_TARGETS), "address_targets")
    reservations = raw["reservation_bytes"]
    require(type(reservations) is list and len(reservations) == 3, "reservations")
    require(
        all(type(entry) is list and len(entry) == 2 and is_positive_int(entry[1]) for entry in reservations),
        "reservations",
    )
    require([entry[0] for entry in reservations] == ["data", "temp", "wal"], "reservations")
    budget_by_class = dict(reservations)
    preparation, cutover = raw["logging_wal_upper_bound_bytes"], raw["cutover_wal_upper_bound_bytes"]
    require(is_positive_int(preparation) and is_positive_int(cutover), "phase_budget")
    require(
        budget_by_class["data"] > scope["max_retention_data_bytes"] - scope["resident_retention_data_bytes"],
        "candidate_data_budget",
    )
    require(preparation > scope["max_retention_wal_bytes"], "preparation_budget")
    require(
        cutover > scope["max_commit_wal_bytes"] and preparation + cutover <= budget_by_class["wal"], "cutover_budget"
    )
    return deepcopy(raw)


def source_vector_sha256(pairs):
    """Bind the complete already-validated Profile Source vector without dropping fields."""
    require(type(pairs) is list and bool(pairs), "source_vector")
    return hashlib.sha256(
        b"provider-directory-ordinary-address-source-vector.v1\0" + canonical(pairs).encode("ascii")
    ).hexdigest()


def require_plan_selection(geometry, attestation, captured_scope):
    """Compare signed values with the coordinator's server-issued input capture."""
    value = validate_geometry(geometry)
    require(value["desired_profile_as_of"] == require_ordinary_selection(attestation), "date_changed")
    require(value["selection_proof_id"] == attestation["proof_id"], "selection_changed")
    require(
        value["ordinary_scope"]["source_vector_sha256"] == source_vector_sha256(attestation["pairs"]),
        "source_vector_changed",
    )
    require(value["ordinary_scope"] == validate_scope(captured_scope), "captured_input_changed")


def require_materialization_scope(scope, verified_profile_geometry):
    """Keep the retention recipe coupled to the separately verified Profile mode."""
    expected_by_materialization = {"full_swap": "snapshot", "source_delta": "delta_preimage"}
    materialization = verified_profile_geometry.get("materialization_mode")
    require(materialization in expected_by_materialization, "profile_materialization")
    require(
        validate_scope(scope)["retention_kind"] == expected_by_materialization[materialization],
        "retention_materialization",
    )
