# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Signed database-capacity lease contracts for Provider Directory builds."""

from __future__ import annotations

import base64
import dataclasses
import datetime
import hashlib
import json
import os
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from process import provider_directory_profile_capacity_attestation as lease
from process import provider_directory_profile_capacity_preflight_contract as preflight
from process import provider_directory_profile_capacity_signing_guard as guard_contract
from tests.provider_directory_profile_capacity_runtime_test_support import (
    PROFILE_RUNTIME_WITNESS_MIGRATION_REVISION,
    golden_runtime_witnesses,
    golden_storage_classes,
)
from tests.provider_directory_profile_capacity_signing_guard_test_support import (
    capacity_signing_guard,
)
from tests.provider_directory_profile_capacity_trust_fixtures import (
    PUBLIC_KEY_HEX,
)
from tests.provider_directory_profile_capacity_trust_fixtures import (
    capacity_trust as _trust,
)

UTC = datetime.timezone.utc
VALIDATION_TIME = datetime.datetime(2026, 7, 30, 12, 0, 2, tzinfo=UTC)
PRIVATE_KEY = Ed25519PrivateKey.from_private_bytes(bytes(range(32)))
GOLDEN_ATTESTATION_ID = "e9c0258ca2afd155596c59e58b61f794def418c19092143efc89a7d5d9a653a6"
GOLDEN_SIGNATURE = "E3ISlocFKT8RLzF5m4xpNjeZxnP7y4IX5Nro9Yr7GC9uXoFLjGYjoHaI2FuIOea3tUCVYPoQpUd_I7I1aizXDg"
GOLDEN_CANONICAL_BODY_SHA256 = "62f5358a35cd8ff942bd97a90766e051bee2d95053002194809c25710e719a2f"
GOLDEN_SIGNING_PREFLIGHT_GUARD_SHA256 = "4d464989005fbd497ea86b50e62cae94a98cf8d548e24f4c276c620f346ca2f2"
GOLDEN_HEALTHCARE_PREFLIGHT_RECEIPT_SHA256 = "7e1471d615cea386c0b00d75ec40d1dc954b34ae614ce60efe5a0d6f0014bbbf"


def _golden_body() -> dict[str, object]:
    runtime, _legacy_deployment = golden_runtime_witnesses()
    signing_guard, guard_hash, receipt_hash = capacity_signing_guard(
        capacity_geometry_hash=None,
        observed_at=datetime.datetime(2026, 7, 30, 12, 0, tzinfo=UTC),
        issued_at=datetime.datetime(2026, 7, 30, 12, 0, 1, tzinfo=UTC),
        expires_at=datetime.datetime(2026, 7, 30, 13, 0, tzinfo=UTC),
        max_build_deadline=datetime.datetime(2026, 7, 30, 12, 55, tzinfo=UTC),
    )
    body_by_field = {
        "attestor_id": "capacity-authority-dev",
        "attestor_release_digest": "11" * 32,
        "capacity_geometry_hash": signing_guard["healthcare_receipt"]["capacity_geometry_hash"],
        "contract_id": lease.CAPACITY_LEASE_CONTRACT_ID,
        "environment_id": "dev-us",
        "expires_at": "2026-07-30T13:00:00Z",
        "issued_at": "2026-07-30T12:00:01Z",
        "key_id": "capacity-key-2026-07",
        "max_build_deadline": "2026-07-30T12:55:00Z",
        "nonce": receipt_hash,
        "observed_at": "2026-07-30T12:00:00Z",
        "reservation_id": "pd-capacity-reservation-7",
        "signing_preflight_guard": signing_guard,
        "signing_preflight_guard_sha256": guard_hash,
        "runtime": runtime,
        "signature_algorithm": lease.CAPACITY_LEASE_SIGNATURE_ALGORITHM,
        "storage_classes": golden_storage_classes(),
    }
    body_by_field["attestation_id"] = lease.capacity_attestation_id(body_by_field)
    return body_by_field


def _historical_envelope() -> dict[str, object]:
    """Read the unchanged original signed v3 fixture without rebuilding its guard."""
    path = Path(__file__).resolve().parent / "fixtures/provider_directory_profile_execution_v2_golden.json"
    return json.loads(path.read_text())["provider_directory_profile_capacity_attestation"]


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
    message = lease.CAPACITY_LEASE_SIGNATURE_DOMAIN.encode("ascii") + b"\x00" + canonical_body
    signature = base64.urlsafe_b64encode(PRIVATE_KEY.sign(message)).rstrip(b"=").decode("ascii")
    return {"lease": body, "signature": signature}


def _verify(
    envelope: dict[str, object] | None = None,
    **overrides: object,
) -> lease.VerifiedDatabaseCapacityLease:
    verification_by_option: dict[str, object] = {
        "trust": _trust(),
        "now": VALIDATION_TIME,
        "expected_capacity_geometry_hash": _golden_body()["capacity_geometry_hash"],
    }
    verification_by_option.update(overrides)
    previous_node_id = os.environ.get("HLTHPRT_IMPORT_NODE_ID")
    os.environ["HLTHPRT_IMPORT_NODE_ID"] = "dev-node"
    try:
        return lease.verify_database_capacity_lease(
            envelope or _signed_envelope(),
            **verification_by_option,
        )
    finally:
        if previous_node_id is None:
            os.environ.pop("HLTHPRT_IMPORT_NODE_ID", None)
        else:
            os.environ["HLTHPRT_IMPORT_NODE_ID"] = previous_node_id


def test_current_lease_authenticates_logical_runtime_and_one_pool():
    verified = _verify()
    assert verified.contract_id == "provider-directory-database-capacity-lease-v4"
    assert verified.runtime.healthcare_source_commit == "12" * 20
    assert verified.runtime.profile_migration_revision == PROFILE_RUNTIME_WITNESS_MIGRATION_REVISION
    assert verified.reservation_bytes_by_storage_class == {
        "data": 180_000_000_000,
        "temp": 20_000_000_000,
        "wal": 150_000_000_000,
    }
    assert verified.outstanding_reserved_bytes_by_storage_class == verified.reservation_bytes_by_storage_class
    assert verified.pool_remaining_bytes == 300_000_000_000
    assert not hasattr(verified, "database_oid") and not hasattr(verified, "deployment_witness")
    assert verified.public_key_fingerprint == "05549452c2988321a6d9e7daa9a7704bf150aa556ea2ddb9c45c8fe92dc7f643"


def test_original_v3_golden_authenticates_unchanged_and_cannot_fresh_admit():
    envelope = _historical_envelope()
    verified = lease.authenticate_historical_capacity_lease(envelope, trust=_trust(), now=VALIDATION_TIME)
    assert verified.attestation_id == GOLDEN_ATTESTATION_ID and verified.signature == GOLDEN_SIGNATURE
    assert hashlib.sha256(verified.canonical_lease_json.encode("ascii")).hexdigest() == GOLDEN_CANONICAL_BODY_SHA256
    assert verified.lease_digest == "5fc245d6d887416d00e65b6bc843ec334cdb402a2044b3268ead3ee14dc45f74"
    assert verified.signing_preflight_guard_sha256 == GOLDEN_SIGNING_PREFLIGHT_GUARD_SHA256
    assert verified.nonce == GOLDEN_HEALTHCARE_PREFLIGHT_RECEIPT_SHA256
    assert verified.reservation_bytes_by_storage_class == {
        "data": 180_000_000_000,
        "temp": 20_000_000_000,
        "wal": 150_000_000_000,
    }
    assert verified.has_semantic_admission_proof is False
    with pytest.raises(lease.ProviderDirectoryCapacityLeaseError, match="unsupported_contract"):
        _verify(envelope)


def test_attestation_id_golden_vector_is_independent_of_mapping_order():
    body = _golden_body()
    expected = body.pop("attestation_id")

    assert lease.capacity_attestation_id(dict(reversed(tuple(body.items())))) == expected


def test_historical_deadline_does_not_erase_outstanding_ceiling():
    envelope = _historical_envelope()
    authenticated = lease.authenticate_historical_capacity_lease(
        envelope, trust=_trust(), now=VALIDATION_TIME.replace(minute=56)
    )
    assert authenticated.reservation_bytes_by_storage_class["wal"] == 150_000_000_000
    assert authenticated.max_build_deadline < authenticated.validated_at < authenticated.expires_at


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
        limits = guard["healthcare_request"]["provider_directory_profile_capacity_limits"]
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


@pytest.mark.parametrize("target", ["envelope", "lease", "storage_class", "runtime"])
@pytest.mark.parametrize("mutation", ["missing", "extra"])
def test_closed_wire_schema_rejects_missing_and_extra_fields(target, mutation):
    envelope = _signed_envelope()
    selected = envelope if target == "envelope" else envelope["lease"]
    if target == "storage_class":
        selected = selected["storage_classes"][0]
    elif target == "runtime":
        selected = selected["runtime"]
    if mutation == "missing":
        selected.pop(next(iter(selected)))
    else:
        selected["not_in_contract"] = "rejected"
    with pytest.raises(lease.ProviderDirectoryCapacityLeaseError, match="invalid_fields"):
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


def test_expected_logical_geometry_is_mandatory():
    with pytest.raises(lease.ProviderDirectoryCapacityLeaseError, match="pin_mismatch"):
        _verify(expected_capacity_geometry_hash="99" * 32)


@pytest.mark.parametrize(
    "field", ["expected_database_name", "expected_database_oid", "expected_database_system_identifier"]
)
def test_current_verifier_rejects_physical_identity_arguments(field):
    with pytest.raises(TypeError):
        _verify(**{field: "unused"})


@pytest.mark.parametrize("require_fresh_observation", [None, 0, 1, "false"])
def test_freshness_control_requires_a_boolean(require_fresh_observation):
    with pytest.raises(lease.ProviderDirectoryCapacityLeaseError, match="invalid_type: require_fresh_observation"):
        _verify(_signed_envelope(), require_fresh_observation=require_fresh_observation)


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


def test_storage_classes_require_canonical_order():
    with pytest.raises(lease.ProviderDirectoryCapacityLeaseError, match="invalid_order"):
        _verify(_signed_envelope(body_mutator=lambda body: body["storage_classes"].reverse()))


@pytest.mark.parametrize(
    "field", ["tablespaces", "volumes", "database_oid", "deployment_witness", "runtime_witness_sha256"]
)
def test_current_lease_refuses_removed_physical_fields(field):
    envelope = _signed_envelope()
    envelope["lease"][field] = None
    with pytest.raises(lease.ProviderDirectoryCapacityLeaseError, match="invalid_fields"):
        _verify(envelope)


def test_aggregate_pool_refuses_when_each_class_alone_would_fit():
    verified = dataclasses.replace(
        _verify(),
        storage_classes=tuple(
            lease.DatabaseCapacityStorageClass(
                storage_class=name, available_bytes=100, reserved_bytes=40, available_after_all_reservations_bytes=60
            )
            for name in ("data", "temp", "wal")
        ),
    )
    assert verified.pool_remaining_bytes == -20
    assert all(entry.reserved_bytes < entry.available_bytes for entry in verified.storage_classes)
    with pytest.raises(lease.ProviderDirectoryCapacityLeaseError, match="remaining_capacity_too_small"):
        lease.assert_database_capacity_lease_reservation(
            verified,
            required_bytes_by_storage_class={"data": 1, "temp": 1, "wal": 1},
            minimum_remaining_bytes=1,
            required_build_seconds=1,
        )


@pytest.mark.parametrize("remaining", [-1, True, 1_000_000_000_001])
def test_storage_numeric_accounting_is_closed(remaining):
    envelope = _signed_envelope(
        body_mutator=lambda body: body["storage_classes"][0].update(available_after_all_reservations_bytes=remaining)
    )
    with pytest.raises(lease.ProviderDirectoryCapacityLeaseError):
        _verify(envelope)


def test_fresh_verifier_refuses_v2_before_any_replay_projection():
    lease_by_field = {name: None for name in lease._LEGACY_V2_SIGNED_BODY_FIELDS}
    lease_by_field["contract_id"] = lease.LEGACY_CAPACITY_LEASE_V2_CONTRACT_ID
    with pytest.raises(lease.ProviderDirectoryCapacityLeaseError, match="unsupported_contract"):
        _verify({"lease": lease_by_field, "signature": GOLDEN_SIGNATURE})


@pytest.mark.parametrize("historical", [False, True])
@pytest.mark.parametrize("contract", [None, [], {}])
def test_unsupported_contract_container_fails_closed(historical, contract):
    envelope = _historical_envelope() if historical else _signed_envelope()
    envelope["lease"]["contract_id"] = contract
    with pytest.raises(lease.ProviderDirectoryCapacityLeaseError):
        if historical:
            lease.authenticate_historical_capacity_lease(envelope, trust=_trust(), now=VALIDATION_TIME)
        else:
            _verify(envelope)


def test_original_v3_signature_cannot_use_the_new_domain():
    envelope = _historical_envelope()
    body = lease.canonical_capacity_lease_json(envelope["lease"]).encode("ascii")
    envelope["signature"] = (
        base64.urlsafe_b64encode(
            PRIVATE_KEY.sign(lease.CAPACITY_LEASE_SIGNATURE_DOMAIN.encode("ascii") + b"\x00" + body)
        )
        .rstrip(b"=")
        .decode("ascii")
    )
    with pytest.raises(lease.ProviderDirectoryCapacityLeaseError, match="invalid_signature"):
        lease.authenticate_historical_capacity_lease(envelope, trust=_trust(), now=VALIDATION_TIME)


def test_historical_remaining_bytes_are_never_reinterpreted_as_pool_evidence():
    envelope = _historical_envelope()
    envelope["lease"]["volumes"][0]["available_after_all_reservations_bytes"] = "opaque history"
    body = envelope["lease"]
    body["attestation_id"] = lease._domain_hash(
        "healthporta.provider-directory.database-capacity-attestation-id.v3",
        {key: value for key, value in body.items() if key != "attestation_id"},
    )
    envelope["signature"] = (
        base64.urlsafe_b64encode(
            PRIVATE_KEY.sign(
                b"healthporta.provider-directory.database-capacity-lease.v3\x00"
                + lease.canonical_capacity_lease_json(body).encode("ascii")
            )
        )
        .rstrip(b"=")
        .decode("ascii")
    )
    authenticated = lease.authenticate_historical_capacity_lease(envelope, trust=_trust(), now=VALIDATION_TIME)
    assert authenticated.reservation_bytes_by_storage_class["data"] == 180_000_000_000
    assert not hasattr(authenticated, "pool_remaining_bytes")
    assert authenticated.has_semantic_admission_proof is False
