# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Signed database-capacity lease contracts for Provider Directory builds."""

from __future__ import annotations

import base64
import datetime
import hashlib
import os

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from process import provider_directory_profile_capacity_attestation as lease
from process import provider_directory_profile_capacity_preflight_contract as preflight
from process import provider_directory_profile_capacity_signing_guard as guard_contract
from tests.provider_directory_profile_capacity_runtime_test_support import (
    PROFILE_RUNTIME_WITNESS_MIGRATION_REVISION,
    golden_capacity_storage,
    golden_runtime_witnesses,
)
from tests.provider_directory_profile_capacity_signing_guard_test_support import (
    capacity_signing_guard,
)
from tests.provider_directory_profile_capacity_trust_fixtures import (
    PUBLIC_KEY_HEX,
    capacity_trust as _trust,
)


UTC = datetime.timezone.utc
VALIDATION_TIME = datetime.datetime(2026, 7, 30, 12, 0, 2, tzinfo=UTC)
PRIVATE_KEY = Ed25519PrivateKey.from_private_bytes(bytes(range(32)))
GOLDEN_ATTESTATION_ID = "078288bd96e22ff76f4563db162f31c57f925b2814fc5a8d153211d1dd421b87"
GOLDEN_SIGNATURE = (
    "EN1UiDHsMVmG_DszvzV-0wWsqEp8MKoy2Aey4T-zKj9IWQK8N2E_sm540ZJ0_Ggd"
    "SPja0BuQ9FHCHuB3eI3SDQ"
)
GOLDEN_CANONICAL_BODY_SHA256 = "9b8a27ae4c32c2e2635d4cfcb55332f14f419959da9fde384e862b1f15b87602"
GOLDEN_SIGNING_PREFLIGHT_GUARD_SHA256 = (
    "936d1324312013cd91c1b0a1a6e5179" "b152d3a3671c65c204775f7cb4abeb4f6"
)
GOLDEN_HEALTHCARE_PREFLIGHT_RECEIPT_SHA256 = (
    "79b15f27b2ff3b1d6fddb7b2e2b5d20" "5ba58cbc3f0ae4e3c6d12b11f1e0d7be1"
)


def _golden_body() -> dict[str, object]:
    runtime_witness_by_field, deployment_witness_by_field = golden_runtime_witnesses()
    tablespace_rows, volume_rows = golden_capacity_storage()
    observed_at = datetime.datetime(2026, 7, 30, 12, 0, tzinfo=UTC)
    issued_at = datetime.datetime(2026, 7, 30, 12, 0, 1, tzinfo=UTC)
    expires_at = datetime.datetime(2026, 7, 30, 13, 0, tzinfo=UTC)
    max_build_deadline = datetime.datetime(2026, 7, 30, 12, 55, tzinfo=UTC)
    signing_guard, signing_guard_sha256, receipt_sha256 = capacity_signing_guard(
        capacity_geometry_hash="55" * 32,
        observed_at=observed_at,
        issued_at=issued_at,
        expires_at=expires_at,
        max_build_deadline=max_build_deadline,
    )
    return {
        "attestation_id": GOLDEN_ATTESTATION_ID,
        "attestor_id": "capacity-authority-dev",
        "attestor_release_digest": "11" * 32,
        "capacity_geometry_hash": "55" * 32,
        "contract_id": lease.CAPACITY_LEASE_CONTRACT_ID,
        "database_name": "healthporta_test",
        "database_oid": 16401,
        "database_system_identifier": "7527713908662902214",
        "environment_id": "dev-us",
        "expires_at": "2026-07-30T13:00:00Z",
        "issued_at": "2026-07-30T12:00:01Z",
        "key_id": "capacity-key-2026-07",
        "max_build_deadline": "2026-07-30T12:55:00Z",
        "nonce": receipt_sha256,
        "observed_at": "2026-07-30T12:00:00Z",
        "reservation_id": "pd-capacity-reservation-7",
        "signing_preflight_guard": signing_guard,
        "signing_preflight_guard_sha256": signing_guard_sha256,
        "runtime_witness": runtime_witness_by_field,
        "runtime_witness_sha256": (
            lease.capacity_runtime_witness_sha256(
                runtime_witness_by_field,
                deployment_witness_by_field,
            )
        ),
        "deployment_witness": deployment_witness_by_field,
        "signature_algorithm": lease.CAPACITY_LEASE_SIGNATURE_ALGORITHM,
        "tablespaces": tablespace_rows,
        "volumes": volume_rows,
    }


def _signed_envelope(
    *,
    body_mutator=None,
) -> dict[str, object]:
    body = _golden_body()
    body.pop("attestation_id")
    if body_mutator is not None:
        body_mutator(body)
    body["attestation_id"] = lease.capacity_attestation_id(body)
    canonical_body = lease.canonical_capacity_lease_json(body).encode("ascii")
    message = (
        lease.CAPACITY_LEASE_SIGNATURE_DOMAIN.encode("ascii") + b"\x00" + canonical_body
    )
    signature = (
        base64.urlsafe_b64encode(PRIVATE_KEY.sign(message)).rstrip(b"=").decode("ascii")
    )
    return {"lease": body, "signature": signature}


def _verify(
    envelope: dict[str, object] | None = None,
    **overrides: object,
) -> lease.VerifiedDatabaseCapacityLease:
    verification_by_option: dict[str, object] = {
        "trust": _trust(),
        "now": VALIDATION_TIME,
        "expected_capacity_geometry_hash": "55" * 32,
        "expected_database_system_identifier": "7527713908662902214",
        "expected_database_oid": 16401,
        "expected_database_name": "healthporta_test",
    }
    verification_by_option.update(overrides)
    previous_node_id = os.environ.get("HLTHPRT_IMPORT_NODE_ID")
    os.environ["HLTHPRT_IMPORT_NODE_ID"] = "dev-node"
    try:
        return lease.verify_database_capacity_lease(
            envelope
            or {
                "lease": _golden_body(),
                "signature": GOLDEN_SIGNATURE,
            },
            **verification_by_option,
        )
    finally:
        if previous_node_id is None:
            os.environ.pop("HLTHPRT_IMPORT_NODE_ID", None)
        else:
            os.environ["HLTHPRT_IMPORT_NODE_ID"] = previous_node_id


def test_golden_vector_verifies_exact_canonical_schema_and_signature():
    verified = _verify()

    assert verified.attestation_id == GOLDEN_ATTESTATION_ID
    assert verified.signature == GOLDEN_SIGNATURE
    assert (
        hashlib.sha256(verified.canonical_lease_json.encode("ascii")).hexdigest()
        == GOLDEN_CANONICAL_BODY_SHA256
    )
    assert verified.capacity_geometry_hash == "55" * 32
    assert verified.signing_preflight_guard_sha256 == (
        GOLDEN_SIGNING_PREFLIGHT_GUARD_SHA256
    )
    assert verified.nonce == GOLDEN_HEALTHCARE_PREFLIGHT_RECEIPT_SHA256
    assert verified.database_system_identifier == "7527713908662902214"
    assert verified.runtime_witness.healthcare_source_commit == "12" * 20
    assert verified.runtime_witness.profile_migration_revision == (
        PROFILE_RUNTIME_WITNESS_MIGRATION_REVISION
    )
    assert verified.runtime_witness_sha256 == (
        "b302efd7b80efd2a2f6e4fc772fb9f32a6be50cf55a69f2d48149e0588b9f15f"
    )
    assert verified.deployment_witness.preflight_transport == (
        "kubectl_exec_loopback_8080"
    )
    assert verified.reservation_bytes_by_storage_class == {
        "data": 180_000_000_000,
        "temp": 20_000_000_000,
        "wal": 150_000_000_000,
    }
    assert verified.lease_digest == (
        "ad3c07d4fdb96964dfe56afd57b1c1ab7001d844e67a271a81dc609023d1cb1d"
    )
    assert verified.public_key_fingerprint == (
        "05549452c2988321a6d9e7daa9a7704b" "f150aa556ea2ddb9c45c8fe92dc7f643"
    )
    assert verified.tablespace_identity_hash == (
        "4c53f2792f1198c75a1e6a7ca1d03621" "924d19c72bd39f8997ede9c312371f0e"
    )
    assert verified.volume_identity_hash == (
        "fd8a7e7f2a446dac51955276d6865c16" "954b4526c3b6cc0bd5d66320a798d975"
    )


def test_attestation_id_golden_vector_is_independent_of_mapping_order():
    body = _golden_body()
    expected = body.pop("attestation_id")

    assert (
        lease.capacity_attestation_id(dict(reversed(tuple(body.items())))) == expected
    )


def test_legacy_v2_body_is_recognized_but_rejected_for_profile_admission():
    body = _golden_body()
    body["contract_id"] = lease.LEGACY_CAPACITY_LEASE_V2_CONTRACT_ID
    body.pop("signing_preflight_guard")
    body.pop("signing_preflight_guard_sha256")

    with pytest.raises(
        lease.ProviderDirectoryCapacityLeaseError,
        match="unsupported_contract: contract_id",
    ):
        _verify({"lease": body, "signature": GOLDEN_SIGNATURE})


def test_signed_guard_is_closed_and_binds_one_identical_limits_document():
    def add_unsigned_extension(body):
        guard = body["signing_preflight_guard"]
        guard["not_in_contract"] = "rejected"
        body["signing_preflight_guard_sha256"] = preflight.preflight_domain_sha256(
            guard_contract.CAPACITY_SIGNING_PREFLIGHT_GUARD_DIGEST_DOMAIN,
            guard,
        )

    with pytest.raises(
        lease.ProviderDirectoryCapacityLeaseError,
        match="binding_mismatch: signing_preflight_guard",
    ):
        _verify(_signed_envelope(body_mutator=add_unsigned_extension))

    def drift_healthcare_limits(body):
        guard = body["signing_preflight_guard"]
        limits = guard["healthcare_request"][
            "provider_directory_profile_capacity_limits"
        ]
        limits["max_build_seconds"] += 1
        body["signing_preflight_guard_sha256"] = preflight.preflight_domain_sha256(
            guard_contract.CAPACITY_SIGNING_PREFLIGHT_GUARD_DIGEST_DOMAIN,
            guard,
        )

    with pytest.raises(
        lease.ProviderDirectoryCapacityLeaseError,
        match="binding_mismatch: signing_preflight_guard",
    ):
        _verify(_signed_envelope(body_mutator=drift_healthcare_limits))


@pytest.mark.parametrize(
    ("target", "mutation"),
    [
        ("envelope", "missing"),
        ("envelope", "extra"),
        ("lease", "missing"),
        ("lease", "extra"),
        ("tablespace", "missing"),
        ("tablespace", "extra"),
        ("volume", "missing"),
        ("volume", "extra"),
        ("runtime_witness", "missing"),
        ("runtime_witness", "extra"),
        ("deployment_witness", "missing"),
        ("deployment_witness", "extra"),
    ],
)
def test_closed_wire_schema_rejects_missing_and_extra_fields(target, mutation):
    envelope = _signed_envelope()
    selected = envelope if target == "envelope" else envelope["lease"]
    if target == "tablespace":
        selected = selected["tablespaces"][0]
    elif target == "volume":
        selected = selected["volumes"][0]
    elif target in {"runtime_witness", "deployment_witness"}:
        selected = selected[target]
    if mutation == "missing":
        selected.pop(next(iter(selected)))
    else:
        selected["not_in_contract"] = "rejected"

    with pytest.raises(
        lease.ProviderDirectoryCapacityLeaseError,
        match="invalid_fields",
    ):
        _verify(envelope)


def test_body_without_capacity_geometry_hash_is_not_an_old_valid_contract():
    envelope = _signed_envelope()
    envelope["lease"].pop("capacity_geometry_hash")

    with pytest.raises(
        lease.ProviderDirectoryCapacityLeaseError,
        match="invalid_fields",
    ):
        _verify(envelope)


def test_signature_and_all_trust_pins_fail_closed():
    envelope = _signed_envelope()
    first_character = "A" if envelope["signature"][0] != "A" else "B"
    envelope["signature"] = first_character + envelope["signature"][1:]
    with pytest.raises(
        lease.ProviderDirectoryCapacityLeaseError,
        match="invalid_signature",
    ):
        _verify(envelope)

    trust_overrides = (
        {"public_key": b"\0" * 32},
        {"key_id": "other-key"},
        {"environment_id": "other-env"},
        {"attestor_id": "other-attestor"},
        {"attestor_release_digest": "99" * 32},
    )
    for override in trust_overrides:
        with pytest.raises(lease.ProviderDirectoryCapacityLeaseError):
            _verify(trust=_trust(**override))


@pytest.mark.parametrize(
    ("expected_field", "expected_value"),
    [
        ("expected_capacity_geometry_hash", "99" * 32),
        ("expected_database_system_identifier", "7527713908662902215"),
        ("expected_database_oid", 16402),
    ],
)
def test_runtime_database_and_geometry_pins_are_mandatory(
    expected_field,
    expected_value,
):
    with pytest.raises(
        lease.ProviderDirectoryCapacityLeaseError,
        match="pin_mismatch",
    ):
        _verify(**{expected_field: expected_value})


def test_verifier_requires_observed_database_identity_arguments():
    with pytest.raises(TypeError):
        lease.verify_database_capacity_lease(
            {"lease": _golden_body(), "signature": GOLDEN_SIGNATURE},
            trust=_trust(),
            now=VALIDATION_TIME,
            expected_capacity_geometry_hash="55" * 32,
        )


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("observed_at", "2026-07-30T12:00:02Z", "invalid_interval"),
        ("observed_at", "2026-07-30T11:55:00Z", "invalid_interval"),
        ("issued_at", "2026-07-30T12:00:08Z", "issued_in_future"),
        ("expires_at", "2026-07-30T12:00:01Z", "invalid_interval"),
        ("expires_at", "2026-08-01T12:00:01Z", "invalid_interval"),
        ("max_build_deadline", "2026-07-30T12:00:01Z", "invalid_interval"),
        ("max_build_deadline", "2026-07-30T13:00:01Z", "invalid_interval"),
    ],
)
def test_temporal_contract_rejects_stale_future_or_unbounded_lease(
    field,
    value,
    error,
):
    envelope = _signed_envelope(body_mutator=lambda body: body.update({field: value}))

    with pytest.raises(
        lease.ProviderDirectoryCapacityLeaseError,
        match=error,
    ):
        _verify(envelope)


def test_expiry_and_build_deadline_are_enforced_at_validation_time():
    with pytest.raises(
        lease.ProviderDirectoryCapacityLeaseError,
        match="stale",
    ):
        _verify(now=datetime.datetime(2026, 7, 30, 12, 5, 6, tzinfo=UTC))
    with pytest.raises(
        lease.ProviderDirectoryCapacityLeaseError,
        match="expired",
    ):
        _verify(now=datetime.datetime(2026, 7, 30, 13, 0, tzinfo=UTC))
    with pytest.raises(
        lease.ProviderDirectoryCapacityLeaseError,
        match="deadline_reached",
    ):
        _verify(now=datetime.datetime(2026, 7, 30, 12, 55, tzinfo=UTC))


def test_tablespace_roles_and_storage_classes_require_canonical_order():
    def reverse_tablespaces(body):
        body["tablespaces"].reverse()

    def reverse_volumes(body):
        body["volumes"].reverse()

    for mutator, field in (
        (reverse_tablespaces, "tablespaces"),
        (reverse_volumes, "volumes"),
    ):
        with pytest.raises(
            lease.ProviderDirectoryCapacityLeaseError,
            match=field,
        ):
            _verify(_signed_envelope(body_mutator=mutator))


def test_tablespace_role_must_map_to_the_signed_class_volume():
    def mutate(body):
        body["tablespaces"][0]["volume_digest"] = "77" * 32

    with pytest.raises(
        lease.ProviderDirectoryCapacityLeaseError,
        match="binding_mismatch",
    ):
        _verify(_signed_envelope(body_mutator=mutate))


def test_identical_tablespace_oid_must_share_name_and_volume_digest():
    def split_one_physical_tablespace(body):
        body["tablespaces"][1]["volume_digest"] = "66" * 32
        body["volumes"][1]["volume_digest"] = "66" * 32

    with pytest.raises(
        lease.ProviderDirectoryCapacityLeaseError,
        match="colocation_mismatch",
    ):
        _verify(_signed_envelope(body_mutator=split_one_physical_tablespace))


@pytest.mark.parametrize(
    "mutator",
    [
        lambda body: body["volumes"][1].update({"available_bytes": 999_999_999_999}),
        lambda body: body["volumes"][1].update(
            {"available_after_all_reservations_bytes": 699_999_999_999}
        ),
        lambda body: body["volumes"][1].update({"reserved_bytes": 200_000_000_001}),
    ],
)
def test_colocated_storage_is_counted_once_and_must_share_physical_facts(
    mutator,
):
    with pytest.raises(
        lease.ProviderDirectoryCapacityLeaseError,
        match="colocation_mismatch|reservation_unaccounted",
    ):
        _verify(_signed_envelope(body_mutator=mutator))
