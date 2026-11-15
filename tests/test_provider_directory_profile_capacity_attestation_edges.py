# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed capacity attestation and consumption edge coverage."""

from __future__ import annotations

import datetime
from unittest.mock import MagicMock

import pytest

from process import provider_directory_profile_capacity_attestation as attestation
from process import (
    provider_directory_profile_capacity_attestation_contract as attestation_contract,
)
from process import (
    provider_directory_profile_capacity_consumption as consumption,
)
from process import (
    provider_directory_profile_capacity_trust_config as trust_config,
)
from process import (
    provider_directory_profile_capacity_trust_validation as trust_validation,
)
from tests.provider_directory_profile_capacity_trust_fixtures import capacity_trust
from tests.test_provider_directory_profile_capacity_attestation import (
    GOLDEN_SIGNATURE,
    VALIDATION_TIME,
    _golden_body,
    _signed_envelope,
    _verify,
)


def _assert_lease_errors(failure_operations):
    for failure_operation in failure_operations:
        with pytest.raises(attestation.ProviderDirectoryCapacityLeaseError):
            failure_operation()


def test_attestation_contract_rejects_container_and_time_edges():
    operations = (
        lambda: attestation_contract._exact_mapping([], frozenset(), field="lease"),
        lambda: attestation_contract._exact_sequence("not rows", field="storage_classes"),
        lambda: attestation_contract._validation_time(VALIDATION_TIME.replace(tzinfo=None)),
        lambda: attestation_contract._parse_capacity_storage_classes([]),
        lambda: attestation_contract._integer(True, field="bytes", minimum=0, maximum=100),
    )
    _assert_lease_errors(operations)


@pytest.mark.parametrize(
    ("field_name", "field_value", "error"),
    (
        ("contract_id", "unsupported", "unsupported_contract"),
        ("signature_algorithm", "unsupported", "unsupported_algorithm"),
    ),
)
def test_attestation_rejects_unsupported_signed_identity(
    field_name,
    field_value,
    error,
):
    envelope_by_field = _signed_envelope(body_mutator=lambda body: body.update({field_name: field_value}))

    with pytest.raises(
        attestation.ProviderDirectoryCapacityLeaseError,
        match=error,
    ):
        _verify(envelope_by_field)


def test_attestation_rejects_invalid_colocated_accounting():
    envelope_by_field = _signed_envelope(
        body_mutator=lambda body: body["storage_classes"][0].update(
            {"available_after_all_reservations_bytes": (body["storage_classes"][0]["available_bytes"] + 1)}
        )
    )

    with pytest.raises(
        attestation.ProviderDirectoryCapacityLeaseError,
        match="invalid_accounting",
    ):
        _verify(envelope_by_field)


def test_attestation_rejects_signature_decode_and_identity_drift(monkeypatch):
    decode_failure = MagicMock(side_effect=ValueError("bad signature"))
    with monkeypatch.context() as signature_context:
        signature_context.setattr(
            attestation.base64,
            "b64decode",
            decode_failure,
        )
        with pytest.raises(
            attestation.ProviderDirectoryCapacityLeaseError,
            match="invalid_value",
        ):
            attestation._decode_signature(GOLDEN_SIGNATURE)

    invalid_identity_by_field = {
        "lease": _golden_body(),
        "signature": GOLDEN_SIGNATURE,
    }
    invalid_identity_by_field["lease"]["attestation_id"] = "00" * 32
    with pytest.raises(
        attestation.ProviderDirectoryCapacityLeaseError,
        match="identity_mismatch",
    ):
        _verify(invalid_identity_by_field)


def test_attestation_rejects_noncanonical_signature_pad_bits():
    with pytest.raises(
        attestation.ProviderDirectoryCapacityLeaseError,
        match="invalid_value",
    ):
        attestation._decode_signature(("A" * 85) + "B")


def test_reservation_rejects_type_and_storage_class_drift():
    required_bytes_by_class = {"data": 1, "temp": 1, "wal": 1}
    with pytest.raises(
        attestation.ProviderDirectoryCapacityLeaseError,
        match="invalid_type",
    ):
        attestation.assert_database_capacity_lease_reservation(
            object(),
            required_bytes_by_storage_class=required_bytes_by_class,
            minimum_remaining_bytes=1,
            required_build_seconds=1,
        )
    with pytest.raises(
        attestation.ProviderDirectoryCapacityLeaseError,
        match="invalid_fields",
    ):
        attestation.assert_database_capacity_lease_reservation(
            _verify(),
            required_bytes_by_storage_class={"data": 1},
            minimum_remaining_bytes=1,
            required_build_seconds=1,
        )


def _capacity_binding(**overrides):
    binding_by_field = {
        "run_id": "run_" + "6" * 32,
        "build_id": "pdpb_" + "7" * 32,
        "executable_plan_hash": "88" * 32,
        "selection_proof_id": "99" * 32,
        "source_vector_hash": "aa" * 32,
        "source_context_vector_hash": "bb" * 32,
        "profile_as_of": "2026-07-30",
    }
    binding_by_field.update(overrides)
    return attestation.CapacityLeaseConsumptionBinding(**binding_by_field)


def test_consumption_rejects_invalid_profile_dates():
    for candidate in (None, "2026-02-30"):
        with pytest.raises(attestation.ProviderDirectoryCapacityLeaseError):
            consumption._profile_as_of(candidate)


def test_consumption_rejects_noncanonical_compact_profile_date():
    with pytest.raises(attestation.ProviderDirectoryCapacityLeaseError):
        consumption._profile_as_of("20260730")


def test_consumption_rejects_invalid_types_and_expired_acceptance():
    verified_lease = _verify()
    valid_binding = _capacity_binding()
    failure_operations = (
        lambda: consumption.capacity_lease_consumption_values(
            object(),
            valid_binding,
            accepted_at=VALIDATION_TIME,
        ),
        lambda: consumption.capacity_lease_consumption_values(
            verified_lease,
            object(),
            accepted_at=VALIDATION_TIME,
        ),
        lambda: consumption.capacity_lease_consumption_values(
            verified_lease,
            valid_binding,
            accepted_at=VALIDATION_TIME.replace(tzinfo=None),
        ),
        lambda: consumption.capacity_lease_consumption_values(
            verified_lease,
            valid_binding,
            accepted_at=verified_lease.max_build_deadline,
        ),
    )
    _assert_lease_errors(failure_operations)


def test_retired_trust_rotation_returns_one_bounded_window():
    retired_key_by_field = {
        "status": "retired",
        "retired_at": "2026-07-30T12:00:00Z",
        "verify_until": "2026-07-30T13:00:00Z",
    }

    status, retired_at, verify_until = trust_config._trust_rotation(retired_key_by_field)

    assert status == "retired"
    assert retired_at < verify_until


def test_runtime_trust_timestamp_requires_exact_utc_second():
    with pytest.raises(attestation.ProviderDirectoryCapacityLeaseError):
        trust_validation._utc_second(None, field="validated_at")


@pytest.mark.parametrize("observed_date", ["2026-07-30", "2026-07-29"])
def test_delta_capacity_geometry_binds_explicit_desired_date(observed_date):
    from types import SimpleNamespace

    from process import provider_directory_profile_capacity as capacity
    from process.provider_directory_profile_capacity_signing_receipts import _assert_capacity_receipt_geometry
    from tests.test_provider_directory_profile_capacity import _geometry_payload
    from tests.test_provider_directory_profile_initial_replay import _selection

    execution = _selection("2026-07-30", current=True)
    geometry = capacity.validated_capacity_geometry(
        _geometry_payload(
            selection_proof_id=execution.attestation.proof_id,
            profile_input_digest=execution.attestation.profile_input_digest,
            profile_as_of=observed_date,
        )
    )
    receipt_by_field = {
        "capacity_geometry": capacity.capacity_geometry_payload(geometry),
        "capacity_geometry_hash": capacity.capacity_geometry_hash(geometry),
        "required_reservation_bytes_by_storage_class": geometry.reservation_bytes_by_storage_class,
    }
    request = SimpleNamespace(execution=execution, cms_nonprofile_admission=None)
    if observed_date != "2026-07-30":
        with pytest.raises(ValueError, match="profile_geometry_binding_invalid"):
            _assert_capacity_receipt_geometry(receipt_by_field, request)
    else:
        _assert_capacity_receipt_geometry(receipt_by_field, request)


def test_original_signed_history_without_retained_geometry_cannot_grant_replay():
    from types import SimpleNamespace

    from process.provider_directory_profile_capacity_signing_guard import validated_historical_replay_guard
    from tests.test_provider_directory_profile_capacity_attestation import _historical_envelope

    historical = attestation.authenticate_historical_capacity_lease(
        _historical_envelope(), trust=capacity_trust(), now=VALIDATION_TIME
    )
    with pytest.raises(ValueError, match="legacy_replay"):
        validated_historical_replay_guard(historical, SimpleNamespace(legacy_payload={"contract_id": "missing"}))


@pytest.mark.parametrize("undercharged_class", [None, "data", "temp", "wal"])
def test_current_cms_lease_requires_the_authenticated_profile_pair_in_outstanding_charges(undercharged_class):
    from tests.provider_directory_cms_capacity_test_support import cms_guard, rehash_guard, sign_guard

    guard = cms_guard()
    if undercharged_class is not None:
        pair = guard["healthcare_request"]["cms_nonprofile_admission"]["paired_profile_lease"]["lease"]
        required_by_class = {entry["storage_class"]: entry["reserved_bytes"] for entry in pair["storage_classes"]}
        own = guard["healthcare_receipt"]["required_reservation_bytes_by_storage_class"]
        for entry in guard["control_plane_request"]["storage_observation"]["storage_classes"]:
            if entry["storage_class"] == undercharged_class:
                entry["available_after_all_reservations_bytes"] = (
                    entry["available_bytes"] - own[undercharged_class] - required_by_class[undercharged_class] + 1
                )
        from process.provider_directory_profile_capacity_signing_guard_contract import (
            CONTROL_PLANE_STORAGE_OBSERVATION_DIGEST_DOMAIN,
        )

        guard["control_plane_receipt"]["storage_observation_sha256"] = attestation._domain_hash(
            CONTROL_PLANE_STORAGE_OBSERVATION_DIGEST_DOMAIN,
            guard["control_plane_request"]["storage_observation"],
        )
        rehash_guard(guard)
    envelope = sign_guard(guard, cms=True)
    if undercharged_class is None:
        assert _verify(envelope, expected_capacity_geometry_hash=envelope["lease"]["capacity_geometry_hash"])
    else:
        with pytest.raises(attestation.ProviderDirectoryCapacityLeaseError, match="paired_profile_reservation_missing"):
            _verify(envelope, expected_capacity_geometry_hash=envelope["lease"]["capacity_geometry_hash"])


def _signed_retained_semantic_geometry():
    """Sign synthetic old-format replay evidence; leave original golden bytes untouched."""
    import base64

    from process.provider_directory_profile_capacity_geometry import project_legacy_capacity_geometry
    from process.provider_directory_profile_capacity_preflight_contract import preflight_domain_sha256
    from tests.test_provider_directory_profile_capacity import _geometry_payload, _legacy_geometry_projection_inputs
    from tests.test_provider_directory_profile_capacity_attestation import PRIVATE_KEY, _historical_envelope

    original, geometry_hash, retained = _legacy_geometry_projection_inputs()
    geometry = project_legacy_capacity_geometry(
        original, geometry_hash, relations=_geometry_payload()["relations"], retained_evidence=retained
    )
    envelope = _historical_envelope()
    body = envelope["lease"]
    guard = body["signing_preflight_guard"]
    receipt = guard["healthcare_receipt"]
    receipt.update(capacity_geometry=original, capacity_geometry_hash=geometry_hash)
    receipt["receipt_sha256"] = preflight_domain_sha256(
        receipt["contract_id"], {name: field_value for name, field_value in receipt.items() if name != "receipt_sha256"}
    )
    guard["healthcare_receipt_sha256"] = receipt["receipt_sha256"]
    body.update(
        capacity_geometry_hash=geometry_hash,
        nonce=receipt["receipt_sha256"],
        signing_preflight_guard_sha256=preflight_domain_sha256(guard["contract_id"], guard),
    )
    body["attestation_id"] = attestation._domain_hash(
        "healthporta.provider-directory.database-capacity-attestation-id.v3",
        {name: field_value for name, field_value in body.items() if name != "attestation_id"},
    )
    message = (
        b"healthporta.provider-directory.database-capacity-lease.v3\0"
        + attestation.canonical_capacity_lease_json(body).encode("ascii")
    )
    envelope["signature"] = base64.urlsafe_b64encode(PRIVATE_KEY.sign(message)).rstrip(b"=").decode("ascii")
    return envelope, geometry


@pytest.mark.parametrize("changed_field", [None, "signature", "profile_as_of", "reservation_id"])
def test_retained_v3_replay_requires_original_signature_and_semantic_consumption(monkeypatch, changed_field):
    import importlib

    from process.provider_directory_profile_capacity_consumption import retained_capacity_consumption_values

    fhir = importlib.import_module("process.provider_directory_fhir")
    envelope, geometry = _signed_retained_semantic_geometry()
    historical = attestation.authenticate_historical_capacity_lease(
        envelope, trust=capacity_trust(), now=VALIDATION_TIME
    )
    binding = attestation_contract.CapacityLeaseConsumptionBinding(
        "run_" + "1" * 32,
        "pdpb_" + "2" * 32,
        geometry.executable_plan_hash,
        geometry.selection_proof_id,
        geometry.desired_source_vector_hash,
        geometry.desired_context_vector_hash,
        geometry.profile_as_of,
    )
    stored = retained_capacity_consumption_values(historical, binding, accepted_at=VALIDATION_TIME)
    receipt_by_field = {
        "build_id": binding.build_id,
        "executable_plan_hash": binding.executable_plan_hash,
        "selection_proof_id": binding.selection_proof_id,
        "to_source_vector_hash": binding.source_vector_hash,
        "to_source_context_vector_hash": binding.source_context_vector_hash,
        "profile_as_of": binding.profile_as_of,
    }
    monkeypatch.setattr(fhir.profile_capacity_runtime, "configured_capacity_lease_trust", capacity_trust)
    if changed_field is not None:
        stored[changed_field] = "A" * 86 if changed_field == "signature" else "changed"
        with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="replay_capacity"):
            fhir._verified_provider_directory_profile_replay_lease(stored, receipt_by_field, geometry)
    else:
        replayed = fhir._verified_provider_directory_profile_replay_lease(stored, receipt_by_field, geometry)
        assert replayed.canonical_lease_json == stored["canonical_lease_json"]
        assert replayed.signature == envelope["signature"]
        assert replayed.reservation_bytes_by_storage_class == historical.reservation_bytes_by_storage_class
        with pytest.raises(attestation.ProviderDirectoryCapacityLeaseError, match="unsupported_contract"):
            _verify(envelope)
