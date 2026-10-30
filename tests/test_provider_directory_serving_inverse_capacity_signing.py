# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real synthetic signatures bind inverse purpose without accepting a Profile pair."""

from copy import deepcopy

import pytest

from process import provider_directory_profile_capacity_attestation as leases
from process import provider_directory_serving_inverse_capacity_contract as inverse
from process import provider_directory_serving_inverse_capacity_signing as signing
from tests.provider_directory_profile_capacity_trust_fixtures import capacity_trust_from_envelope
from tests.provider_directory_serving_inverse_capacity_support import signed_envelope, signing_guard


def verify(envelope):
    body = envelope["lease"]
    return leases.verify_database_capacity_lease(
        envelope,
        trust=capacity_trust_from_envelope(envelope),
        now=inverse.utc_second(body["issued_at"]),
        expected_capacity_geometry_hash=body["capacity_geometry_hash"],
        expected_database_system_identifier=body["database_system_identifier"],
        expected_database_oid=body["database_oid"],
        expected_database_name=body["database_name"],
    )


def test_genuine_signature_keeps_standalone_operation_and_database_volume_identity():
    envelope = signed_envelope()
    lease = verify(envelope)
    assert lease.signing_preflight_guard["contract_id"] == inverse.SIGNING_GUARD
    assert "profile_execution" not in inverse.canonical(lease.signing_preflight_guard)
    assert lease.nonce == envelope["lease"]["signing_preflight_guard"]["healthcare_receipt_sha256"]


@pytest.mark.parametrize("damage", ["operation", "node", "scope", "tablespace", "pair", "legacy_domain"])
def test_signed_but_mixed_replay_material_is_refused(damage):
    guard = signing_guard()
    match damage:
        case "operation":
            guard["control_plane_request"]["recovery_execution"]["generation"] = "00000000-0000-4000-8000-000000000002"
        case "node":
            guard["healthcare_receipt"]["recovery_execution_identity"]["node_id"] = "other-node"
        case "scope":
            guard["healthcare_receipt"]["capacity_geometry"]["scope"]["forward_state_sha256"] = "0" * 64
        case "tablespace":
            guard["healthcare_receipt"]["capacity_geometry"]["data_tablespace_name"] = "other_space"
        case "pair":
            guard["control_plane_request"]["paired_profile_lease"] = {}
        case _:
            guard["contract_id"] = "healthporta.provider-directory-profile-capacity-signing-preflight-guard.v1"
    with pytest.raises(ValueError):
        verify(signed_envelope(guard))


def test_standalone_guard_parser_does_not_accept_nonzero_native_writer_counts():
    guard = signing_guard()
    receipt = deepcopy(guard["control_plane_receipt"])
    receipt["quiescence"]["active_inverse_run_count"] = 1
    request = inverse.validated_inverse_preflight_request(guard["healthcare_request"])
    with pytest.raises(ValueError, match="quiescence"):
        signing.validated_inverse_control_receipt(
            receipt, guard["control_plane_request"], guard["control_plane_request"]["storage_observation"], request
        )


@pytest.mark.parametrize("field", ["limits", "relations", "families", "scopes"])
def test_signed_inverse_guard_translates_nonobject_list_entries(field):
    guard = signing_guard()
    if field == "limits":
        guard["healthcare_request"]["limits"]["families"][0] = 1
    else:
        guard["healthcare_receipt"]["capacity_geometry"]["scope"][field][0] = 1
    with pytest.raises(ValueError, match="^binding_mismatch: signing_preflight_guard$"):
        verify(signed_envelope(guard))
