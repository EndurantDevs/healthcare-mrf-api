# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic signatures over exact native initial-publication identities."""

from datetime import timedelta

from process import provider_directory_profile_capacity as capacity
from process import provider_directory_profile_capacity_preflight_contract as preflight
from process import provider_directory_profile_initial_contract as initial
from tests import provider_directory_profile_capacity_signing_guard_test_support as support
from tests.provider_directory_profile_replay_test_support import _capacity_storage_rows
from tests.test_provider_directory_profile_capacity_attestation import _signed_envelope


def _signed_preflight_inputs(execution, execution_payload, limits, storage, timing):
    control_request = support._control_plane_request(execution_payload, limits, storage, timing)
    control_request.update(
        contract_id=initial.CONTROL_REQUEST_CONTRACT, profile_materialization=initial.MATERIALIZATION
    )
    healthcare_request = support._healthcare_request(execution_payload, limits, {"receipt_sha256": "00" * 32}, timing)
    healthcare_request.update(contract_id=initial.REQUEST_CONTRACT, profile_materialization=initial.MATERIALIZATION)
    validated = preflight.validated_capacity_preflight_request(healthcare_request)
    identity = preflight.profile_execution_identity_payload(validated)
    followup = support._held_followup(execution, timing)
    control = support._control_plane_receipt(control_request, validated, identity, storage, followup, timing)
    control.update(
        contract_id=initial.CONTROL_RECEIPT_CONTRACT,
        request_contract_id=initial.CONTROL_REQUEST_CONTRACT,
        profile_materialization=initial.MATERIALIZATION,
    )
    control.pop("receipt_sha256")
    control["receipt_sha256"] = preflight.preflight_domain_sha256(initial.CONTROL_RECEIPT_CONTRACT, control)
    healthcare_request["signing_guard"]["control_plane_receipt_sha256"] = control["receipt_sha256"]
    validated = preflight.validated_capacity_preflight_request(healthcare_request)
    return control_request, control, healthcare_request, validated, identity, followup


def signed_initial_envelope(geometry, database_identity, target, execution, accepted_at):
    """Sign a test lease; this does not issue a runtime reservation or attest Linux admission."""
    timing = _initial_signing_timing(accepted_at, geometry)
    execution_payload = support._execution_by_field(execution)
    limits = support._limits_payload()
    storage = support._storage_observation(timing)
    control_request, control, healthcare_request, validated, identity, followup = _signed_preflight_inputs(
        execution, execution_payload, limits, storage, timing
    )
    geometry_hash = capacity.capacity_geometry_hash(geometry)
    receipt = support._healthcare_receipt(validated, control, identity, limits, geometry_hash, timing)
    receipt.update(
        contract_id=initial.RECEIPT_CONTRACT,
        request_contract_id=initial.REQUEST_CONTRACT,
        profile_materialization=initial.MATERIALIZATION,
        capacity_geometry=capacity.capacity_geometry_payload(geometry),
        serving_generation_preflight=target,
        serving_generation_preflight_sha256=initial.target_state_sha256(target),
        required_reservation_bytes_by_storage_class=geometry.reservation_bytes_by_storage_class,
    )
    receipt.pop("receipt_sha256")
    receipt["receipt_sha256"] = preflight.preflight_domain_sha256(initial.RECEIPT_CONTRACT, receipt)
    guard = support._guard_payload(control_request, control, healthcare_request, validated, receipt, followup)
    return _signed_initial_result(guard, geometry_hash, database_identity, timing, receipt)


def _signed_initial_result(guard, geometry_hash, database_identity, timing, receipt):
    """Bind the original synthetic signed body to the exact initial receipt and storage."""
    guard_hash = preflight.preflight_domain_sha256(
        support.guard_contract.CAPACITY_SIGNING_PREFLIGHT_GUARD_DIGEST_DOMAIN, guard
    )
    tablespaces, volumes = _capacity_storage_rows(database_identity)

    def body(body):
        body.update(
            capacity_geometry_hash=geometry_hash,
            database_name=database_identity.database_name,
            database_oid=database_identity.database_oid,
            database_system_identifier=database_identity.database_system_identifier,
            observed_at=support._utc(timing.observed_at),
            issued_at=support._utc(timing.issued_at),
            max_build_deadline=support._utc(timing.max_build_deadline),
            expires_at=support._utc(timing.expires_at),
            signing_preflight_guard=guard,
            signing_preflight_guard_sha256=guard_hash,
            nonce=receipt["receipt_sha256"],
            tablespaces=tablespaces,
            volumes=volumes,
        )

    return _signed_envelope(body_mutator=body), receipt


def _initial_signing_timing(accepted_at, geometry):
    """Construct the unchanged bounded test lease timestamps."""
    return support._GuardTiming(
        accepted_at - timedelta(seconds=2),
        accepted_at - timedelta(seconds=1),
        accepted_at + timedelta(seconds=geometry.max_build_seconds + 30),
        accepted_at + timedelta(seconds=geometry.max_build_seconds + 10),
        "22" * 32,
    )
