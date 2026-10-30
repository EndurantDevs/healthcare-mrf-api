# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Standalone inverse values cannot be confused with forward Profile authority."""

from copy import deepcopy

import pytest

from process import provider_directory_serving_inverse_capacity_contract as inverse
from tests.provider_directory_serving_inverse_capacity_support import execution, geometry, limits, scope


def test_closed_standalone_operation_scope_limits_and_geometry():
    operation, ceiling, observed, plan = execution(), limits(), scope(), geometry()
    assert inverse.validated_inverse_execution(operation) == operation
    assert inverse.validated_inverse_limits(ceiling) == ceiling
    assert inverse.validated_inverse_scope(observed) == observed
    assert inverse.validated_inverse_geometry(plan, limits=ceiling, execution=operation) == plan
    assert "paired_profile_lease" not in inverse.canonical(plan)
    assert inverse.run_handle("run_" + "a" * 32) != operation["generation"]


@pytest.mark.parametrize("damage", ["mixed", "node", "event", "uuid", "source"])
def test_execution_refuses_mixed_or_substituted_identity(damage):
    value = execution()
    if damage == "mixed":
        value["profile_execution"] = {}
    elif damage == "node":
        value["node_id"] = " "
    elif damage == "event":
        value["published_event_sha256"] = "bad"
    elif damage == "uuid":
        value["generation"] = "run_" + "a" * 32
    else:
        value["source_ids"] *= 2
    with pytest.raises(ValueError):
        inverse.validated_inverse_execution(value)


@pytest.mark.parametrize(
    "field,value",
    [
        ("max_cutover_seconds", 3),
        ("statement_timeout_ms", 1001),
        ("max_data_bytes", True),
        ("max_wal_bytes", 1),
        ("minimum_remaining_bytes", 0),
        ("max_prepare_seconds", 7201),
    ],
)
def test_limits_preserve_native_hard_walls_and_wal_partitions(field, value):
    ceiling = limits()
    ceiling[field] = value
    with pytest.raises(ValueError):
        inverse.validated_inverse_limits(ceiling)


@pytest.mark.parametrize("damage", ["hash", "scope_oid", "family", "row_ceiling", "purpose", "reservation"])
def test_geometry_refuses_stale_scope_or_changed_spending(damage):
    plan = geometry()
    match damage:
        case "hash":
            plan["scope"]["forward_state_sha256"] = "0" * 64
        case "scope_oid":
            plan["scope"]["scopes"][0]["oid"] = 999
        case "family":
            plan["scope"]["families"].pop()
        case "row_ceiling":
            ceiling = limits()
            ceiling["families"][0]["max_delete_rows"] = 0
            plan["limits_sha256"] = inverse.limits_sha256(ceiling)
            with pytest.raises(ValueError):
                inverse.validated_inverse_geometry(plan, limits=ceiling, execution=execution())
            return
        case "purpose":
            plan["admission_purpose"] = "serving_nonprofile"
        case _:
            plan["reservation_bytes"][0][1] -= 1
    with pytest.raises(ValueError):
        inverse.validated_inverse_geometry(plan, limits=limits(), execution=execution())


def test_preflight_request_uses_existing_signing_challenge_with_no_profile_dto():
    challenge_by_field = dict(
        contract_id="healthporta.provider-directory-profile-capacity-signing-guard-request.v1",
        request_nonce="1" * 64,
        control_plane_receipt_sha256="2" * 64,
        expires_at="2026-07-01T01:00:00Z",
    )
    raw_by_field = dict(
        contract_id=inverse.HEALTHCARE_REQUEST,
        recovery_execution=execution(),
        limits=limits(),
        signing_guard=challenge_by_field,
    )
    request = inverse.validated_inverse_preflight_request(raw_by_field)
    assert request.recovery_execution == execution() and not hasattr(request, "execution")
    assert request.request_nonce == challenge_by_field["request_nonce"]
    projection = deepcopy(raw_by_field)
    projection["contract_id"] = inverse.PROJECTION_REQUEST
    projection.pop("signing_guard")
    assert inverse.validated_inverse_projection_request(projection).request_nonce is None
