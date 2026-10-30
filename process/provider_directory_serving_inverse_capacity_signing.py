# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact inverse guard-v2 replay links inside the existing lease-v3 envelope."""

from process import provider_directory_serving_inverse_capacity_contract as contract
from process.provider_directory_profile_capacity_signing_guard_contract import (
    CAPACITY_BUNDLE_VALIDATION_PATH,
    CONTROL_PLANE_PREFLIGHT_PATH,
    CONTROL_PLANE_SIGNING_INTENT_CONTRACT_ID,
    CONTROL_PLANE_STORAGE_OBSERVATION_DIGEST_DOMAIN,
    HEALTHCARE_PREFLIGHT_PATH,
)
from process.provider_directory_profile_capacity_signing_receipts import _validated_storage_observation


def _receipt_digest(value):
    return contract.digest(value["contract_id"], {key: item for key, item in value.items() if key != "receipt_sha256"})


def _quiescence(raw, kind):
    value = contract.exact(raw, kind)
    expected = contract.CONTROL_QUIESCENCE if kind == "control_quiescence" else contract.HEALTHCARE_QUIESCENCE
    contract.require(value["contract_id"] == expected, "quiescence_contract")
    for key, item in value.items():
        if key.endswith("_count"):
            contract.require(type(item) is int and item == 0, "quiescence")
    if kind == "control_quiescence":
        contract.hash64(value["operation_sha256"])
    else:
        contract.require(
            value["capacity_consumption_boundary"] == "unexpired"
            and value["preflight_receipt_boundary"] == "unconsumed_and_unexpired",
            "quiescence_boundaries",
        )
        contract.require(
            type(value["active_profile_run_statuses"]) is list and bool(value["active_profile_run_statuses"]),
            "quiescence_statuses",
        )
        contract.require(
            type(value["claimed_checkpoint_states"]) is list and bool(value["claimed_checkpoint_states"]),
            "quiescence_states",
        )
    return value


def validated_inverse_control_request(raw, healthcare_request):
    """Bind one exact operation and fresh storage observation without a Profile follow-up."""
    value = contract.exact(raw, "control_request")
    contract.require(value["contract_id"] == contract.CONTROL_REQUEST, "control_request")
    execution = contract.validated_inverse_execution(value["recovery_execution"])
    limits = contract.validated_inverse_limits(value["limits"])
    contract.require(
        execution == healthcare_request.recovery_execution and limits == healthcare_request.limits_payload,
        "control_inputs",
    )
    intent = value["signing_intent"]
    contract.require(type(intent) is dict and set(intent) == {"contract_id", "request_nonce"}, "signing_intent")
    contract.require(intent["contract_id"] == CONTROL_PLANE_SIGNING_INTENT_CONTRACT_ID, "signing_intent")
    contract.hash64(intent["request_nonce"])
    return value, _validated_storage_observation(value["storage_observation"])


def validated_inverse_control_receipt(raw, request, storage, healthcare_request):
    """Require exact Control operation/quiescence/hash and unchanged signing interval."""
    value = contract.exact(raw, "control_receipt")
    operation = contract.operation_sha256(healthcare_request.recovery_execution)
    quiescence = _quiescence(value["quiescence"], "control_quiescence")
    expected_by_field = dict(
        contract_id=contract.CONTROL_RECEIPT,
        request_contract_id=contract.CONTROL_REQUEST,
        request_sha256=contract.digest(contract.CONTROL_REQUEST, request),
        request_nonce=request["signing_intent"]["request_nonce"],
        recovery_execution_identity=healthcare_request.recovery_execution,
        operation_sha256=operation,
        capacity_limits_sha256=healthcare_request.limits_sha256,
        storage_observation_sha256=contract.digest(CONTROL_PLANE_STORAGE_OBSERVATION_DIGEST_DOMAIN, storage),
        quiescence_sha256=contract.digest(contract.CONTROL_QUIESCENCE, quiescence),
        receipt_sha256=_receipt_digest(value),
    )
    expected_by_field.update({key: storage[key] for key in ("issued_at", "expires_at", "max_build_deadline")})
    contract.require(
        all(value[key] == item for key, item in expected_by_field.items())
        and quiescence["operation_sha256"] == operation,
        "control_receipt",
    )
    return value


def validated_inverse_healthcare_receipt(raw, request, control_receipt):
    """Bind a genuine engine-produced aggregate budget and exact inverse input scope."""
    receipt_by_field = contract.exact(raw, "healthcare_receipt")
    quiescence = _quiescence(receipt_by_field["quiescence"], "healthcare_quiescence")
    geometry = contract.validated_inverse_geometry(
        receipt_by_field["capacity_geometry"], limits=request.limits_payload, execution=request.recovery_execution
    )
    expected_by_field = dict(
        contract_id=contract.HEALTHCARE_RECEIPT,
        request_contract_id=contract.HEALTHCARE_REQUEST,
        request_sha256=request.request_sha256,
        request_nonce=request.request_nonce,
        control_plane_receipt_sha256=control_receipt["receipt_sha256"],
        recovery_execution_identity=request.recovery_execution,
        capacity_limits=request.limits_payload,
        capacity_limits_sha256=request.limits_sha256,
        capacity_geometry_hash=geometry_sha256(geometry),
        required_reservation_bytes_by_storage_class=dict(geometry["reservation_bytes"]),
        expires_at=control_receipt["expires_at"],
        quiescence_sha256=contract.digest(contract.HEALTHCARE_QUIESCENCE, quiescence),
        receipt_sha256=_receipt_digest(receipt_by_field),
    )
    contract.require(
        all(receipt_by_field[key] == expected_value for key, expected_value in expected_by_field.items()),
        "healthcare_receipt",
    )
    contract.require(
        contract.utc_second(receipt_by_field["issued_at"]) < contract.utc_second(receipt_by_field["expires_at"]),
        "receipt_interval",
    )
    contract.require(
        type(receipt_by_field["runtime_observation"]) is dict
        and type(receipt_by_field["preflight_receipt_storage"]) is dict,
        "receipt_observation",
    )
    return receipt_by_field


def geometry_sha256(raw):
    """Use the reviewed standalone geometry digest, preserving all legacy domains."""
    import hashlib

    return hashlib.sha256(contract.canonical(contract.validated_inverse_geometry(raw)).encode("ascii")).hexdigest()


def validated_inverse_signing_preflight_guard(
    raw_guard,
    supplied_sha256,
    *,
    lease_nonce,
    lease_capacity_geometry_hash,
    lease_observed_at,
    lease_issued_at,
    lease_expires_at,
    lease_max_build_deadline,
):
    """Verify complete replay material; this parser does not issue resource authority."""
    guard = contract.exact(raw_guard, "signing_guard")
    contract.require(guard["contract_id"] == contract.SIGNING_GUARD, "guard_contract")
    contract.hash64(supplied_sha256)
    contract.require(supplied_sha256 == contract.digest(contract.SIGNING_GUARD, guard), "guard_hash")
    expected_paths_by_field = dict(
        bundle_validation_path=CAPACITY_BUNDLE_VALIDATION_PATH,
        control_plane_preflight_path=CONTROL_PLANE_PREFLIGHT_PATH,
        healthcare_preflight_path=HEALTHCARE_PREFLIGHT_PATH,
    )
    contract.require(
        all(guard[key] == expected_value for key, expected_value in expected_paths_by_field.items()), "guard_paths"
    )
    request = contract.validated_inverse_preflight_request(guard["healthcare_request"])
    control_request, storage = validated_inverse_control_request(guard["control_plane_request"], request)
    control_receipt = validated_inverse_control_receipt(
        guard["control_plane_receipt"], control_request, storage, request
    )
    receipt = validated_inverse_healthcare_receipt(guard["healthcare_receipt"], request, control_receipt)
    expected_by_field = dict(
        control_plane_request_sha256=control_receipt["request_sha256"],
        control_plane_receipt_sha256=control_receipt["receipt_sha256"],
        healthcare_request_sha256=request.request_sha256,
        healthcare_receipt_sha256=receipt["receipt_sha256"],
        capacity_limits_sha256=request.limits_sha256,
        storage_observation_sha256=control_receipt["storage_observation_sha256"],
        operation_preimage_sha256=contract.operation_sha256(request.recovery_execution),
    )
    contract.require(
        all(guard[key] == expected_value for key, expected_value in expected_by_field.items()), "guard_links"
    )
    contract.require(
        request.control_plane_receipt_sha256 == control_receipt["receipt_sha256"]
        and request.request_nonce == control_receipt["request_nonce"]
        and receipt["receipt_sha256"] == lease_nonce
        and receipt["capacity_geometry_hash"] == lease_capacity_geometry_hash,
        "lease_links",
    )
    for key, expected_value in dict(
        observed_at=lease_observed_at,
        issued_at=lease_issued_at,
        expires_at=lease_expires_at,
        max_build_deadline=lease_max_build_deadline,
    ).items():
        contract.require(contract.utc_second(storage[key]) == expected_value, "lease_interval")
    contract.require(request.expires_at == lease_expires_at, "request_interval")
    return guard, supplied_sha256


def validated_inverse_signing_guard_fields(lease_body, parsed):
    """Dispatch only the exact guard-v2 contract while retaining lease-v3 identity checks."""
    guard, guard_sha256 = validated_inverse_signing_preflight_guard(
        lease_body["signing_preflight_guard"],
        lease_body["signing_preflight_guard_sha256"],
        lease_nonce=parsed["nonce"],
        lease_capacity_geometry_hash=parsed["capacity_geometry_hash"],
        lease_observed_at=parsed["observed_at"],
        lease_issued_at=parsed["issued_at"],
        lease_expires_at=parsed["expires_at"],
        lease_max_build_deadline=parsed["max_build_deadline"],
    )
    geometry = guard["healthcare_receipt"]["capacity_geometry"]
    scope = geometry["scope"]
    contract.require(
        all(scope[key] == parsed[key] for key in ("database_system_identifier", "database_oid", "database_name")),
        "lease_database",
    )
    tablespaces_by_field = {entry.usage: entry for entry in parsed["tablespaces"]}
    contract.require(set(tablespaces_by_field) == {"data", "temp"}, "lease_tablespaces")
    contract.require(
        all(
            (tablespaces_by_field[key].tablespace_oid, tablespaces_by_field[key].tablespace_name)
            == (geometry[key + "_tablespace_oid"], geometry[key + "_tablespace_name"])
            for key in ("data", "temp")
        ),
        "lease_tablespaces",
    )
    return {**parsed, "signing_preflight_guard": guard, "signing_preflight_guard_sha256": guard_sha256}
