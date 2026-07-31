# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable consumption values for a signed database-capacity lease."""

from __future__ import annotations

import hashlib

import pytest

from process import provider_directory_profile_capacity_attestation as lease
from tests.test_provider_directory_profile_capacity_attestation import (
    GOLDEN_ATTESTATION_ID,
    GOLDEN_SIGNATURE,
    VALIDATION_TIME,
    _golden_body,
    _signed_envelope,
    _verify,
)


def test_consumption_values_bind_full_build_and_source_identity():
    verified = _verify()
    binding = lease.CapacityLeaseConsumptionBinding(
        run_id="run_" + "6" * 32,
        build_id="pdpb_" + "7" * 32,
        executable_plan_hash="88" * 32,
        selection_proof_id="99" * 32,
        source_vector_hash="aa" * 32,
        source_context_vector_hash="bb" * 32,
        profile_as_of="2026-07-30",
    )

    consumption_by_field = lease.capacity_lease_consumption_values(
        verified,
        binding,
        accepted_at=VALIDATION_TIME,
    )

    assert consumption_by_field["attestation_id"] == GOLDEN_ATTESTATION_ID
    assert (
        consumption_by_field["reservation_id"]
        == "pd-capacity-reservation-7"
    )
    assert consumption_by_field["capacity_geometry_hash"] == "55" * 32
    assert consumption_by_field["executable_plan_hash"] == "88" * 32
    assert consumption_by_field["selection_proof_id"] == "99" * 32
    assert consumption_by_field["source_vector_hash"] == "aa" * 32
    assert consumption_by_field["source_context_vector_hash"] == "bb" * 32
    assert consumption_by_field["profile_as_of"] == "2026-07-30"
    assert consumption_by_field["accepted_at"] == VALIDATION_TIME
    assert (
        consumption_by_field["expires_at"]
        > consumption_by_field["accepted_at"]
    )
    canonical_digest = hashlib.sha256(
        consumption_by_field["canonical_lease_json"].encode("ascii")
    ).hexdigest()
    assert canonical_digest != consumption_by_field["lease_digest"]


@pytest.mark.parametrize(
    ("field_name", "invalid_identifier"),
    [
        ("run_id", "run_not-a-digest"),
        ("build_id", "pdpb_not-a-digest"),
    ],
)
def test_consumption_binding_rejects_noncanonical_lineage_identifiers(
    field_name,
    invalid_identifier,
):
    binding_by_field = {
        "run_id": "run_" + "6" * 32,
        "build_id": "pdpb_" + "7" * 32,
        "executable_plan_hash": "88" * 32,
        "selection_proof_id": "99" * 32,
        "source_vector_hash": "aa" * 32,
        "source_context_vector_hash": "bb" * 32,
        "profile_as_of": "2026-07-30",
    }
    binding_by_field[field_name] = invalid_identifier

    with pytest.raises(
        lease.ProviderDirectoryCapacityLeaseError,
        match="invalid_value",
    ):
        lease.capacity_lease_consumption_values(
            _verify(),
            lease.CapacityLeaseConsumptionBinding(**binding_by_field),
            accepted_at=VALIDATION_TIME,
        )


def test_wire_contract_contains_no_host_path_or_key_material_fields():
    envelope_map = {"lease": _golden_body(), "signature": GOLDEN_SIGNATURE}
    canonical = lease.canonical_capacity_lease_json(envelope_map)

    assert "hostname" not in canonical
    assert "host_path" not in canonical
    assert "mount_path" not in canonical
    assert "private_key" not in canonical
    assert "/var/" not in canonical


@pytest.mark.parametrize(
    ("field_name", "path_value"),
    [
        ("database_name", "/var/lib/postgresql"),
        ("attestor_id", "capacity/host/path"),
    ],
)
def test_wire_text_fields_reject_host_or_path_shaped_content(
    field_name,
    path_value,
):
    def insert_path(lease_body):
        lease_body[field_name] = path_value

    with pytest.raises(
        lease.ProviderDirectoryCapacityLeaseError,
        match="invalid_value",
    ):
        _verify(_signed_envelope(body_mutator=insert_path))


@pytest.mark.parametrize(
    "body_mutator",
    [
        lambda body: body.update(
            {"database_system_identifier": "18446744073709551616"}
        ),
        lambda body: body.update({"database_oid": True}),
        lambda body: body.update({"nonce": "A" * 64}),
        lambda body: body["volumes"][0].update({"reserved_bytes": 0}),
        lambda body: body["volumes"][0].update({"available_bytes": -1}),
    ],
)
def test_wire_numeric_and_digest_bounds_reject_signed_invalid_content(
    body_mutator,
):
    with pytest.raises(
        lease.ProviderDirectoryCapacityLeaseError,
        match="invalid_value",
    ):
        _verify(_signed_envelope(body_mutator=body_mutator))


def test_signature_encoding_must_be_unpadded_canonical_base64url():
    envelope_map = {
        "lease": _golden_body(),
        "signature": GOLDEN_SIGNATURE + "=",
    }

    with pytest.raises(
        lease.ProviderDirectoryCapacityLeaseError,
        match="invalid_value",
    ):
        _verify(envelope_map)
