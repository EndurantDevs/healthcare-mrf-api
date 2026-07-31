# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed capacity attestation and consumption edge coverage."""

from __future__ import annotations

import datetime
from unittest.mock import MagicMock

import pytest

from process import provider_directory_profile_capacity_attestation as attestation
from process import (
    provider_directory_profile_capacity_attestation_contract
    as attestation_contract,
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
    failure_operations = (
        lambda: attestation_contract.canonical_capacity_lease_json(
            {"invalid": float("nan")}
        ),
        lambda: attestation_contract._exact_sequence(
            "not-a-sequence",
            field="tablespaces",
        ),
        lambda: attestation_contract._timestamp(
            "2026-02-30T12:00:00Z",
            field="issued_at",
        ),
        lambda: attestation_contract._validation_time(None),
        lambda: attestation_contract._parse_capacity_tablespace_list([]),
        lambda: attestation_contract._parse_capacity_volume_list([]),
    )
    _assert_lease_errors(failure_operations)


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
    envelope_by_field = _signed_envelope(
        body_mutator=lambda body: body.update({field_name: field_value})
    )

    with pytest.raises(
        attestation.ProviderDirectoryCapacityLeaseError,
        match=error,
    ):
        _verify(envelope_by_field)


def test_attestation_rejects_invalid_colocated_accounting():
    envelope_by_field = _signed_envelope(
        body_mutator=lambda body: body["volumes"][0].update(
            {
                "available_after_all_reservations_bytes": (
                    body["volumes"][0]["available_bytes"] + 1
                )
            }
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

    status, retired_at, verify_until = trust_config._trust_rotation(
        retired_key_by_field
    )

    assert status == "retired"
    assert retired_at < verify_until


def test_runtime_trust_timestamp_requires_exact_utc_second():
    with pytest.raises(attestation.ProviderDirectoryCapacityLeaseError):
        trust_validation._utc_second(None, field="validated_at")
