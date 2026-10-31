# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed standalone inverse values; parsing grants no spending authority."""

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from uuid import UUID

CONTROL_QUIESCENCE = "provider-directory-serving-inverse-control-quiescence.v1"
CONTROL_RECEIPT = "healthporta.provider-directory-serving-inverse-capacity-control-plane-preflight.v1"
CONTROL_REQUEST = "healthporta.provider-directory-serving-inverse-capacity-control-plane-preflight-request.v1"
EVENT_FIELD = "provider_directory_serving_published_event_sha256"
EXECUTION = "provider-directory-serving-inverse-execution.v1"
EXECUTION_FIELD = "provider_directory_serving_inverse_capacity_attestation"
GEOMETRY = "provider-directory-serving-inverse-capacity.v1"
HEALTHCARE_QUIESCENCE = "provider-directory-serving-inverse-engine-quiescence.v1"
HEALTHCARE_RECEIPT = "healthporta.provider-directory-serving-inverse-capacity-preflight.v1"
HEALTHCARE_REQUEST = "healthporta.provider-directory-serving-inverse-capacity-preflight-request.v1"
INPUT_DOMAIN = "provider-directory-serving-inverse-input.v1"
LIMITS = "provider-directory-serving-inverse-limits.v1"
LIMITS_DOMAIN = "provider-directory-serving-inverse-limits.v1"
OPERATION_DOMAIN = "provider-directory-serving-inverse-operation.v1"
OPERATOR_INPUT = "provider-directory-serving-inverse-capacity-operator-input-v1"
PROJECTION_RECEIPT = "healthporta.provider-directory-serving-inverse-capacity-authority-projection.v1"
PROJECTION_REQUEST = "healthporta.provider-directory-serving-inverse-capacity-authority-projection-request.v1"
PURPOSE = "serving_inverse"
RESULT = "provider-directory-common-serving-inverse-result.v1"
SCOPE = "provider-directory-serving-inverse-scope.v1"
SIGNING_GUARD = "healthporta.provider-directory-profile-capacity-signing-preflight-guard.v2"

FIELDS = {
    "control_quiescence": frozenset(
        {
            "active_held_dispatch_count",
            "contract_id",
            "active_inverse_run_count",
            "operation_sha256",
            "active_profile_run_count",
        }
    ),
    "control_receipt": frozenset(
        {
            "quiescence",
            "contract_id",
            "receipt_sha256",
            "recovery_execution_identity",
            "max_build_deadline",
            "operation_sha256",
            "quiescence_sha256",
            "request_sha256",
            "request_nonce",
            "capacity_limits_sha256",
            "request_contract_id",
            "storage_observation_sha256",
            "issued_at",
            "expires_at",
        }
    ),
    "control_request": frozenset(
        {"recovery_execution", "contract_id", "limits", "signing_intent", "storage_observation"}
    ),
    "execution": frozenset(
        {"node_id", "generation", "contract_id", "source_ids", "operation", "importer", "published_event_sha256"}
    ),
    "family": frozenset(
        {
            "live_logical_bytes",
            "insert_logical_bytes",
            "expected_postimage",
            "restore_mode",
            "retained_rows",
            "retained_name",
            "live_rows",
            "native_sha256",
            "retained_data_sha256",
            "retained_oid",
            "insert_rows",
            "retained_layout_sha256",
            "root_oid",
            "family",
        }
    ),
    "family_limit": frozenset(
        {"max_delete_rows", "max_insert_rows", "max_insert_logical_bytes", "max_delete_logical_bytes", "family"}
    ),
    "geometry": frozenset(
        {
            "required_build_seconds",
            "temp_file_limit_bytes_per_backend",
            "worker_count",
            "postgres_block_size_bytes",
            "cutover_wal_upper_bound_bytes",
            "input_sha256",
            "reservation_bytes",
            "contract_id",
            "minimum_remaining_bytes",
            "commit_wal_upper_bound_bytes",
            "data_tablespace_oid",
            "data_tablespace_name",
            "limits_sha256",
            "preparation_wal_upper_bound_bytes",
            "scope",
            "temp_tablespace_oid",
            "temp_tablespace_name",
            "admission_purpose",
            "wal_segment_size_bytes",
        }
    ),
    "healthcare_quiescence": frozenset(
        {
            "unexpired_capacity_consumption_count",
            "contract_id",
            "active_inverse_run_count",
            "capacity_consumption_boundary",
            "active_profile_run_statuses",
            "claimed_profile_checkpoint_count",
            "outstanding_preflight_receipt_count",
            "preflight_receipt_boundary",
            "claimed_checkpoint_states",
            "active_profile_run_count",
        }
    ),
    "healthcare_receipt": frozenset(
        {
            "preflight_receipt_storage",
            "capacity_geometry",
            "quiescence",
            "required_reservation_bytes_by_storage_class",
            "contract_id",
            "receipt_sha256",
            "recovery_execution_identity",
            "runtime_observation",
            "capacity_geometry_hash",
            "control_plane_receipt_sha256",
            "capacity_limits",
            "quiescence_sha256",
            "request_sha256",
            "request_nonce",
            "capacity_limits_sha256",
            "request_contract_id",
            "issued_at",
            "expires_at",
        }
    ),
    "healthcare_request": frozenset({"recovery_execution", "contract_id", "signing_guard", "limits"}),
    "limits": frozenset(
        {
            "temp_file_limit_bytes_per_backend",
            "max_temp_bytes",
            "max_preparation_wal_bytes",
            "max_data_bytes",
            "max_cutover_seconds",
            "max_wal_bytes",
            "contract_id",
            "statement_timeout_ms",
            "max_commit_wal_bytes",
            "max_prepare_seconds",
            "minimum_remaining_bytes",
            "lock_timeout_ms",
            "families",
            "max_cutover_wal_bytes",
        }
    ),
    "profile_provenance": frozenset(
        {
            "profile_input_digest",
            "selection_proof_id",
            "source_vector_hash",
            "source_context_vector_hash",
            "profile_schema_version",
            "selection_fingerprint",
            "generation_id",
            "control_generation",
            "profile_strategy_version",
            "profile_as_of",
        }
    ),
    "projection_receipt": frozenset(
        {
            "authority_projection_sha256",
            "capacity_geometry",
            "required_reservation_bytes_by_storage_class",
            "contract_id",
            "recovery_execution_identity",
            "runtime_observation",
            "capacity_geometry_hash",
            "operation_sha256",
            "capacity_limits",
            "request_sha256",
            "capacity_limits_sha256",
            "request_contract_id",
        }
    ),
    "projection_request": frozenset({"recovery_execution", "contract_id", "limits"}),
    "recovered_inverse": frozenset({"lease_digest", "run_id", "operation_sha256"}),
    "relation": frozenset({"baseline_total_bytes", "oid", "native_sha256", "schema", "allowed_names", "name"}),
    "result": frozenset(
        {
            "outcome",
            "inverse",
            "phase",
            "generation",
            "journal_sha256",
            "contract_id",
            "restored_profile_generation_id",
            "capacity",
            "qualified_source_ids",
            "published_event_sha256",
            "address",
        }
    ),
    "result_address": frozenset({"local_generation", "local_lineage_id"}),
    "result_capacity": frozenset({"lease_digest", "capacity_geometry_hash", "attestation_id", "reservation_id"}),
    "retained_scope": frozenset({"rows", "layout_sha256", "data_sha256", "oid", "name", "family"}),
    "scope": frozenset(
        {
            "scopes",
            "node_id",
            "input_sha256",
            "owner_oid",
            "retention_payload_sha256",
            "database_name",
            "contract_id",
            "database_system_identifier",
            "operation_sha256",
            "profile_provenance",
            "qualified_source_ids",
            "published_event_sha256",
            "generation",
            "schema_oid",
            "database_oid",
            "relations",
            "families",
            "forward_state_sha256",
            "initial_receipt",
        }
    ),
    "signing_guard": frozenset(
        {
            "healthcare_request",
            "control_plane_preflight_path",
            "contract_id",
            "storage_observation_sha256",
            "control_plane_receipt",
            "healthcare_preflight_path",
            "operation_preimage_sha256",
            "control_plane_receipt_sha256",
            "control_plane_request",
            "healthcare_receipt_sha256",
            "healthcare_request_sha256",
            "bundle_validation_path",
            "capacity_limits_sha256",
            "healthcare_receipt",
            "control_plane_request_sha256",
        }
    ),
}
FAMILIES = (
    "address_evidence",
    "evidence",
    "facility_candidate",
    "medication_bridge",
    "network_bridge",
    "overlay",
    "plan_bridge",
    "procedure_bridge",
    "profile",
    "unified",
)
HARD_BOUNDS = {
    "lock_timeout_ms": 500,
    "max_cutover_seconds": 2,
    "max_prepare_seconds": 7200,
    "statement_timeout_ms": 1000,
    "worker_count": 1,
}

_HASH = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,62}\Z")


def require(condition, reason):
    """Reject incomplete standalone values with a stable failure code."""
    if not condition:
        raise ValueError("provider_directory_serving_inverse_" + reason)


def canonical(value):
    """Use the same finite canonical JSON encoding as existing signing guards."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def digest(domain, value):
    """Bind one closed document with the established ASCII/NUL digest encoding."""
    return hashlib.sha256(domain.encode("ascii") + b"\0" + canonical(value).encode("ascii")).hexdigest()


def exact(raw, kind):
    """Copy an exact shared field set, refusing extensions and mixed purposes."""
    require(type(raw) is dict and set(raw) == FIELDS[kind], kind + "_fields")
    return json.loads(canonical(raw))


def integer(value, *, positive=False):
    """Reject bools, negative values and PostgreSQL signed-bigint overflow."""
    require(type(value) is int and (0 < value if positive else 0 <= value) and value < 2**63, "integer")
    return value


def oid(value):
    """Recognize a real finite native OID without coercion."""
    require(type(value) is int and 0 < value < 2**32, "oid")
    return value


def hash64(value):
    """Require a canonical lowercase SHA-256 digest."""
    require(type(value) is str and _HASH.fullmatch(value) is not None, "hash")
    return value


def text(value, *, maximum=255):
    """Require bounded canonical nonblank text."""
    require(type(value) is str and bool(value) and value == value.strip() and len(value) <= maximum, "text")
    return value


def identifier(value):
    """Keep relation coordinates confined to native SQL identifiers."""
    require(type(value) is str and _IDENTIFIER.fullmatch(value) is not None, "identifier")
    return value


def canonical_uuid(value):
    """Require the actual non-nil existing common generation identifier."""
    try:
        parsed = UUID(value) if type(value) is str else None
    except ValueError:
        parsed = None
    require(parsed is not None and parsed.int != 0 and str(parsed) == value, "generation")
    return value


def run_handle(value):
    """Bind the actual engine handle independently from the old common generation."""
    require(type(value) is str and re.fullmatch(r"run_[0-9a-f]{32}", value) is not None, "run_id")
    return value


def utc_second(value):
    """Parse the existing canonical UTC-second signing timestamp."""
    require(type(value) is str, "timestamp")
    try:
        result = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError as error:
        raise ValueError("provider_directory_serving_inverse_timestamp") from error
    require(result.strftime("%Y-%m-%dT%H:%M:%SZ") == value, "timestamp")
    return result


def validated_inverse_execution(raw):
    """Accept only an explicit existing recover operation, never a Profile execution."""
    result = exact(raw, "execution")
    require(
        result["contract_id"] == EXECUTION
        and result["importer"] == "provider-directory-fhir"
        and result["operation"] == "recover",
        "execution",
    )
    text(result["node_id"], maximum=64)
    canonical_uuid(result["generation"])
    hash64(result["published_event_sha256"])
    sources = result["source_ids"]
    require(type(sources) is list and len(sources) == 1, "source_ids")
    text(sources[0])
    return result


def operation_sha256(raw):
    """Bind the requested existing operation while the real run is assigned separately."""
    return digest(OPERATION_DOMAIN, validated_inverse_execution(raw))


def validated_inverse_limits(raw):
    """Keep each finite signed ceiling within the existing native recovery hard walls."""
    result = exact(raw, "limits")
    require(result["contract_id"] == LIMITS, "limits")
    for key in FIELDS["limits"] - {"contract_id", "families"}:
        integer(result[key], positive=True)
    for key in HARD_BOUNDS.keys() - {"worker_count"}:
        require(result[key] <= HARD_BOUNDS[key], "limits_wall")
    require(result["statement_timeout_ms"] <= result["max_cutover_seconds"] * 1000, "statement_wall")
    require(result["lock_timeout_ms"] <= result["statement_timeout_ms"], "lock_wall")
    require(result["temp_file_limit_bytes_per_backend"] <= result["max_temp_bytes"], "temp_ceiling")
    require(result["temp_file_limit_bytes_per_backend"] % 1024 == 0, "temp_alignment")
    require(
        sum(result[key] for key in ("max_preparation_wal_bytes", "max_cutover_wal_bytes", "max_commit_wal_bytes"))
        <= result["max_wal_bytes"],
        "wal_partition",
    )
    families = result["families"]
    require(type(families) is list and [entry.get("family") for entry in families] == list(FAMILIES), "family_limits")
    for entry in families:
        entry = exact(entry, "family_limit")
        for key in FIELDS["family_limit"] - {"family"}:
            integer(entry[key])
    return result


def limits_sha256(raw):
    """Identify actual reviewed inverse ceilings without converting a forward budget."""
    return digest(LIMITS_DOMAIN, validated_inverse_limits(raw))


def _profile_provenance(raw):
    result = exact(raw, "profile_provenance")
    require(re.fullmatch(r"pdprofile_[0-9a-f]{32}", result["generation_id"]) is not None, "profile_provenance")
    for key in ("control_generation", "profile_schema_version"):
        integer(result[key], positive=True)
    for key in (
        "selection_proof_id",
        "selection_fingerprint",
        "profile_input_digest",
        "source_vector_hash",
        "source_context_vector_hash",
    ):
        hash64(result[key])
    require(
        type(result["profile_as_of"]) is str
        and date.fromisoformat(result["profile_as_of"]).isoformat() == result["profile_as_of"],
        "profile_date",
    )
    text(result["profile_strategy_version"], maximum=128)
    return result


def _initial_receipt(raw, profile):
    if raw is None:
        return
    require(
        type(raw) is dict and set(raw) == {"build_id", "generation_id", "run_id", "payload_sha256"}, "initial_receipt"
    )
    require(re.fullmatch(r"pdpb_[0-9a-f]{32}", raw["build_id"]) is not None, "initial_receipt")
    require(raw["generation_id"] == profile["generation_id"], "initial_receipt")
    run_handle(raw["run_id"])
    hash64(raw["payload_sha256"])


def _family_observation(raw):
    result = exact(raw, "family")
    require(result["family"] in FAMILIES and result["restore_mode"] in {"rows", "initial_heap"}, "family")
    for key in ("root_oid", "retained_oid"):
        oid(result[key])
    for key in ("native_sha256", "retained_layout_sha256"):
        hash64(result[key])
    identifier(result["retained_name"])
    for key in ("retained_rows", "live_rows", "live_logical_bytes", "insert_rows", "insert_logical_bytes"):
        integer(result[key])
    expected = result["expected_postimage"]
    require(type(expected) is dict and set(expected) == {"rows", "data_sha256"}, "postimage")
    integer(expected["rows"])
    hash64(expected["data_sha256"])
    if result["restore_mode"] == "initial_heap":
        require(
            result["family"] in {"profile", "evidence"} and result["retained_data_sha256"] is None, "initial_family"
        )
        require(result["insert_rows"] == result["insert_logical_bytes"] == 0, "initial_rows")
    else:
        hash64(result["retained_data_sha256"])
        require(
            result["insert_rows"] == result["retained_rows"] and result["root_oid"] != result["retained_oid"],
            "retained_rows",
        )
    return result


def _relations(raw):
    require(type(raw) is list and bool(raw), "relations")
    require([entry.get("oid") for entry in raw] == sorted({entry.get("oid") for entry in raw}), "relation_order")
    for entry in raw:
        entry = exact(entry, "relation")
        identifier(entry["schema"])
        identifier(entry["name"])
        oid(entry["oid"])
        hash64(entry["native_sha256"])
        integer(entry["baseline_total_bytes"])
        names = entry["allowed_names"]
        require(
            type(names) is list and bool(names) and names == sorted(set(names)) and entry["name"] in names,
            "relation_names",
        )
        for name in names:
            identifier(name)
    return {entry["oid"]: entry for entry in raw}


def validated_inverse_scope(raw):
    """Parse actual captured identities; JSON alone never establishes native observation authority."""
    scope_by_field = exact(raw, "scope")
    require(scope_by_field["contract_id"] == SCOPE, "scope")
    for key in (
        "operation_sha256",
        "published_event_sha256",
        "forward_state_sha256",
        "retention_payload_sha256",
        "input_sha256",
    ):
        hash64(scope_by_field[key])
    canonical_uuid(scope_by_field["generation"])
    text(scope_by_field["node_id"], maximum=64)
    for key in ("owner_oid", "schema_oid", "database_oid"):
        oid(scope_by_field[key])
    require(
        type(scope_by_field["database_system_identifier"]) is str
        and scope_by_field["database_system_identifier"].isdigit(),
        "database_system",
    )
    text(scope_by_field["database_name"], maximum=63)
    profile = _profile_provenance(scope_by_field["profile_provenance"])
    _initial_receipt(scope_by_field["initial_receipt"], profile)
    relations = _relations(scope_by_field["relations"])
    _scope_families(scope_by_field, relations)
    _scope_keys(scope_by_field, relations)
    qualified = scope_by_field["qualified_source_ids"]
    require(type(qualified) is list and bool(qualified) and qualified == sorted(set(qualified)), "qualified_sources")
    for source_id in qualified:
        text(source_id)
    require(
        scope_by_field["input_sha256"]
        == digest(
            INPUT_DOMAIN, {key: scope_value for key, scope_value in scope_by_field.items() if key != "input_sha256"}
        ),
        "scope_hash",
    )
    return scope_by_field


def _scope_families(result, relations):
    families = result["families"]
    require(type(families) is list and [entry.get("family") for entry in families] == list(FAMILIES), "family_order")
    for entry in families:
        entry = _family_observation(entry)
        require({entry["root_oid"], entry["retained_oid"]} <= relations.keys(), "family_relation")
        require(entry["native_sha256"] == relations[entry["root_oid"]]["native_sha256"], "family_native")
        require(entry["retained_name"] in relations[entry["retained_oid"]]["allowed_names"], "retained_name")
    initial_entries = {entry["family"] for entry in families if entry["restore_mode"] == "initial_heap"}
    require(initial_entries in (set(), {"profile", "evidence"}), "initial_pair")
    require((result["initial_receipt"] is not None) == bool(initial_entries), "initial_pair")


def _scope_keys(result, relations):
    scopes = result["scopes"]
    require(
        type(scopes) is list
        and [entry.get("family") for entry in scopes] == sorted({entry.get("family") for entry in scopes}),
        "scope_order",
    )
    require(
        {entry["family"] for entry in scopes}
        in ({"location_scope", "group_scope"}, {"location_scope", "group_scope", "npi_scope"}),
        "scope_keys",
    )
    for entry in scopes:
        entry = exact(entry, "retained_scope")
        identifier(entry["name"])
        oid(entry["oid"])
        integer(entry["rows"])
        hash64(entry["layout_sha256"])
        hash64(entry["data_sha256"])
        require(
            entry["oid"] in relations and entry["name"] in relations[entry["oid"]]["allowed_names"], "scope_relation"
        )


def validated_inverse_geometry(raw, *, limits=None, execution=None):
    """Bind finite measured aggregate ceilings to one exact original inverse scope."""
    geometry_by_field = exact(raw, "geometry")
    require(
        geometry_by_field["contract_id"] == GEOMETRY and geometry_by_field["admission_purpose"] == PURPOSE, "geometry"
    )
    scope = validated_inverse_scope(geometry_by_field["scope"])
    require(
        geometry_by_field["input_sha256"] == scope["input_sha256"] and geometry_by_field["worker_count"] == 1,
        "geometry_scope",
    )
    hash64(geometry_by_field["limits_sha256"])
    for key in FIELDS["geometry"] - {
        "contract_id",
        "admission_purpose",
        "scope",
        "input_sha256",
        "limits_sha256",
        "reservation_bytes",
        "data_tablespace_name",
        "temp_tablespace_name",
    }:
        integer(geometry_by_field[key], positive=True)
    for key in ("data_tablespace_oid", "temp_tablespace_oid"):
        oid(geometry_by_field[key])
    for key in ("data_tablespace_name", "temp_tablespace_name"):
        text(geometry_by_field[key], maximum=63)
    reserve = geometry_by_field["reservation_bytes"]
    require(
        type(reserve) is list
        and [entry[0] for entry in reserve] == ["data", "temp", "wal"]
        and all(len(entry) == 2 for entry in reserve),
        "reservations",
    )
    for _kind, amount in reserve:
        integer(amount, positive=True)
    if limits is not None:
        _geometry_limits(geometry_by_field, validated_inverse_limits(limits))
    if execution is not None:
        execution = validated_inverse_execution(execution)
        require(scope["operation_sha256"] == operation_sha256(execution), "geometry_operation")
        require(
            all(scope[key] == execution[key] for key in ("generation", "published_event_sha256", "node_id")),
            "geometry_operation",
        )
    return geometry_by_field


def _geometry_limits(result, limits):
    require(result["limits_sha256"] == limits_sha256(limits), "geometry_limits")
    require(
        dict(result["reservation_bytes"])
        == {kind: limits["max_" + kind + "_bytes"] for kind in ("data", "temp", "wal")},
        "geometry_reserve",
    )
    equal_by_field = {
        "minimum_remaining_bytes": "minimum_remaining_bytes",
        "required_build_seconds": "max_prepare_seconds",
        "temp_file_limit_bytes_per_backend": "temp_file_limit_bytes_per_backend",
        "preparation_wal_upper_bound_bytes": "max_preparation_wal_bytes",
        "cutover_wal_upper_bound_bytes": "max_cutover_wal_bytes",
        "commit_wal_upper_bound_bytes": "max_commit_wal_bytes",
    }
    require(all(result[key] == limits[field] for key, field in equal_by_field.items()), "geometry_limits")
    for observed, ceiling in zip(result["scope"]["families"], limits["families"], strict=True):
        require(observed["family"] == ceiling["family"], "geometry_family")
        for actual, maximum in (
            ("live_rows", "max_delete_rows"),
            ("live_logical_bytes", "max_delete_logical_bytes"),
            ("insert_rows", "max_insert_rows"),
            ("insert_logical_bytes", "max_insert_logical_bytes"),
        ):
            require(observed[actual] <= ceiling[maximum], "family_ceiling")


@dataclass(frozen=True)
class InverseCapacityPreflightRequest:
    """One explicit inverse request; contains no Profile execution or paired lease."""

    recovery_execution: dict
    limits_payload: dict
    limits_sha256: str
    request_payload: dict
    request_sha256: str
    request_nonce: str | None = None
    control_plane_receipt_sha256: str | None = None
    expires_at: datetime | None = None


def _request(raw, *, projection):
    kind = "projection_request" if projection else "healthcare_request"
    result = exact(raw, kind)
    require(result["contract_id"] == (PROJECTION_REQUEST if projection else HEALTHCARE_REQUEST), "request_contract")
    execution = validated_inverse_execution(result["recovery_execution"])
    limits = validated_inverse_limits(result["limits"])
    values_by_field = {}
    if not projection:
        from process.provider_directory_profile_capacity_preflight_contract import _validated_signing_guard

        challenge = result["signing_guard"]
        _validated_signing_guard(challenge)
        values_by_field = dict(
            request_nonce=challenge["request_nonce"],
            control_plane_receipt_sha256=challenge["control_plane_receipt_sha256"],
            expires_at=utc_second(challenge["expires_at"]),
        )
    return InverseCapacityPreflightRequest(
        execution, limits, limits_sha256(limits), result, digest(result["contract_id"], result), **values_by_field
    )


def validated_inverse_preflight_request(raw):
    """Parse the standalone authenticated preflight without creating authority."""
    return _request(raw, projection=False)


def validated_inverse_projection_request(raw):
    """Parse the existing operator's standalone read-only authority projection."""
    return _request(raw, projection=True)
