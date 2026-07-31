# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Signed database-capacity lease contracts for Provider Directory builds."""

from __future__ import annotations

import base64
import copy
import datetime

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from process import provider_directory_profile_capacity_attestation as lease
from tests.provider_directory_profile_capacity_trust_fixtures import (
    PUBLIC_KEY_HEX,
    capacity_trust as _trust,
)


UTC = datetime.timezone.utc
VALIDATION_TIME = datetime.datetime(2026, 7, 30, 12, 0, 2, tzinfo=UTC)
PRIVATE_KEY = Ed25519PrivateKey.from_private_bytes(bytes(range(32)))
GOLDEN_ATTESTATION_ID = (
    "7ef089ca3c1e2e28d62d93e92c13b609"
    "42cd8305a7bbd30dcd7715c1d362177d"
)
GOLDEN_SIGNATURE = (
    "RxmvWgphpLXZPJaLybyCd8b8uVEDcbIo5aT6PbcYkzMndX8afD3rXtG1"
    "RUdDf8Tnh8BzRUHm-J9ekEoCmaerCQ"
)
GOLDEN_CANONICAL_BODY = (
    '{"attestation_id":"7ef089ca3c1e2e28d62d93e92c13b60942cd8305'
    'a7bbd30dcd7715c1d362177d","attestor_id":"capacity-authority-dev"'
    ',"attestor_release_digest":"111111111111111111111111111111111111'
    '1111111111111111111111111111","capacity_geometry_hash":"5555555555'
    '555555555555555555555555555555555555555555555555555555","contract_id"'
    ':"provider-directory-database-capacity-lease-v1","database_name":'
    '"healthporta_test","database_oid":16401,"database_system_identifier":'
    '"7527713908662902214","environment_id":"dev-us","expires_at":'
    '"2026-07-30T13:00:00Z","issued_at":"2026-07-30T12:00:01Z",'
    '"key_id":"capacity-key-2026-07","max_build_deadline":'
    '"2026-07-30T12:55:00Z","nonce":"22222222222222222222222222222222'
    '22222222222222222222222222222222","observed_at":"2026-07-30T12:'
    '00:00Z","reservation_id":"pd-capacity-reservation-7",'
    '"signature_algorithm":"Ed25519","tablespaces":[{"tablespace_name":'
    '"pg_default","tablespace_oid":1663,"usage":"data","volume_digest":'
    '"3333333333333333333333333333333333333333333333333333333333333333"'
    '},{"tablespace_name":"pg_default","tablespace_oid":1663,"usage":"temp",'
    '"volume_digest":"333333333333333333333333333333333333333333333333333333'
    '3333333333"}],"volumes":[{"available_after_all_reservations_bytes":'
    '700000000000,"available_bytes":1000000000000,"reserved_bytes":'
    '180000000000,"volume_class":"data","volume_digest":"333333333333333333'
    '3333333333333333333333333333333333333333333333"},{"available_after_all'
    '_reservations_bytes":700000000000,"available_bytes":1000000000000,'
    '"reserved_bytes":20000000000,"volume_class":"temp","volume_digest":'
    '"3333333333333333333333333333333333333333333333333333333333333333"'
    '},{"available_after_all_reservations_bytes":300000000000,'
    '"available_bytes":500000000000,"reserved_bytes":150000000000,'
    '"volume_class":"wal","volume_digest":"4444444444444444444444444444444444'
    '444444444444444444444444444444"}]}'
)


def _golden_body() -> dict[str, object]:
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
        "nonce": "22" * 32,
        "observed_at": "2026-07-30T12:00:00Z",
        "reservation_id": "pd-capacity-reservation-7",
        "signature_algorithm": lease.CAPACITY_LEASE_SIGNATURE_ALGORITHM,
        "tablespaces": [
            {
                "tablespace_name": "pg_default",
                "tablespace_oid": 1663,
                "usage": "data",
                "volume_digest": "33" * 32,
            },
            {
                "tablespace_name": "pg_default",
                "tablespace_oid": 1663,
                "usage": "temp",
                "volume_digest": "33" * 32,
            },
        ],
        "volumes": [
            {
                "available_after_all_reservations_bytes": 700_000_000_000,
                "available_bytes": 1_000_000_000_000,
                "reserved_bytes": 180_000_000_000,
                "volume_class": "data",
                "volume_digest": "33" * 32,
            },
            {
                "available_after_all_reservations_bytes": 700_000_000_000,
                "available_bytes": 1_000_000_000_000,
                "reserved_bytes": 20_000_000_000,
                "volume_class": "temp",
                "volume_digest": "33" * 32,
            },
            {
                "available_after_all_reservations_bytes": 300_000_000_000,
                "available_bytes": 500_000_000_000,
                "reserved_bytes": 150_000_000_000,
                "volume_class": "wal",
                "volume_digest": "44" * 32,
            },
        ],
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
        lease.CAPACITY_LEASE_SIGNATURE_DOMAIN.encode("ascii")
        + b"\x00"
        + canonical_body
    )
    signature = base64.urlsafe_b64encode(
        PRIVATE_KEY.sign(message)
    ).rstrip(b"=").decode("ascii")
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
    return lease.verify_database_capacity_lease(
        envelope or {
            "lease": _golden_body(),
            "signature": GOLDEN_SIGNATURE,
        },
        **verification_by_option,
    )


def test_golden_vector_verifies_exact_canonical_schema_and_signature():
    verified = _verify()

    assert verified.attestation_id == GOLDEN_ATTESTATION_ID
    assert verified.signature == GOLDEN_SIGNATURE
    assert verified.canonical_lease_json == GOLDEN_CANONICAL_BODY
    assert verified.capacity_geometry_hash == "55" * 32
    assert verified.database_system_identifier == "7527713908662902214"
    assert verified.reservation_bytes_by_storage_class == {
        "data": 180_000_000_000,
        "temp": 20_000_000_000,
        "wal": 150_000_000_000,
    }
    assert verified.lease_digest == (
        "f0e53a34fabd3b8e4a0c1657a984c68e"
        "070977b5f7133314a201c0ef1e10b1af"
    )
    assert verified.public_key_fingerprint == (
        "05549452c2988321a6d9e7daa9a7704b"
        "f150aa556ea2ddb9c45c8fe92dc7f643"
    )
    assert verified.tablespace_identity_hash == (
        "4c53f2792f1198c75a1e6a7ca1d03621"
        "924d19c72bd39f8997ede9c312371f0e"
    )
    assert verified.volume_identity_hash == (
        "fd8a7e7f2a446dac51955276d6865c16"
        "954b4526c3b6cc0bd5d66320a798d975"
    )


def test_attestation_id_golden_vector_is_independent_of_mapping_order():
    body = _golden_body()
    expected = body.pop("attestation_id")

    assert lease.capacity_attestation_id(
        dict(reversed(tuple(body.items())))
    ) == expected


@pytest.mark.parametrize(("target", "mutation"), [
    ("envelope", "missing"),
    ("envelope", "extra"),
    ("lease", "missing"),
    ("lease", "extra"),
    ("tablespace", "missing"),
    ("tablespace", "extra"),
    ("volume", "missing"),
    ("volume", "extra"),
])
def test_closed_wire_schema_rejects_missing_and_extra_fields(target, mutation):
    envelope = _signed_envelope()
    selected = envelope if target == "envelope" else envelope["lease"]
    if target == "tablespace":
        selected = selected["tablespaces"][0]
    elif target == "volume":
        selected = selected["volumes"][0]
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
    envelope["signature"] = "A" + envelope["signature"][1:]
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
    envelope = _signed_envelope(
        body_mutator=lambda body: body.update({field: value})
    )

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
        _verify(
            now=datetime.datetime(2026, 7, 30, 12, 5, 6, tzinfo=UTC)
        )
    with pytest.raises(
        lease.ProviderDirectoryCapacityLeaseError,
        match="expired",
    ):
        _verify(
            now=datetime.datetime(2026, 7, 30, 13, 0, tzinfo=UTC)
        )
    with pytest.raises(
        lease.ProviderDirectoryCapacityLeaseError,
        match="deadline_reached",
    ):
        _verify(
            now=datetime.datetime(2026, 7, 30, 12, 55, tzinfo=UTC)
        )


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
        lambda body: body["volumes"][1].update(
            {"available_bytes": 999_999_999_999}
        ),
        lambda body: body["volumes"][1].update(
            {"available_after_all_reservations_bytes": 699_999_999_999}
        ),
        lambda body: body["volumes"][1].update(
            {"reserved_bytes": 200_000_000_001}
        ),
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


def _assert_capacity_reservation_refused(
    verified_lease,
    *,
    required_bytes_by_storage_class: dict[str, int],
    minimum_remaining_bytes: int,
    required_build_seconds: int,
    reason: str,
) -> None:
    """Assert one exact physical-capacity or deadline refusal."""
    with pytest.raises(
        lease.ProviderDirectoryCapacityLeaseError,
        match=reason,
    ):
        lease.assert_database_capacity_lease_reservation(
            verified_lease,
            required_bytes_by_storage_class=required_bytes_by_storage_class,
            minimum_remaining_bytes=minimum_remaining_bytes,
            required_build_seconds=required_build_seconds,
        )


def test_reservation_gate_checks_every_class_and_physical_remaining_space():
    """Require every signed storage reservation to fit its physical volume."""
    verified = _verify()
    lease.assert_database_capacity_lease_reservation(
        verified,
        required_bytes_by_storage_class={
            "data": 175_000_000_000,
            "temp": 20_000_000_000,
            "wal": 140_000_000_000,
        },
        minimum_remaining_bytes=250_000_000_000,
        required_build_seconds=3_000,
    )
    _assert_capacity_reservation_refused(
        verified,
        required_bytes_by_storage_class={
            "data": 180_000_000_001,
            "temp": 20_000_000_000,
            "wal": 140_000_000_000,
        },
        minimum_remaining_bytes=1,
        required_build_seconds=3_000,
        reason="reservation_too_small",
    )
    _assert_capacity_reservation_refused(
        verified,
        required_bytes_by_storage_class={"data": 1, "temp": 1, "wal": 1},
        minimum_remaining_bytes=300_000_000_001,
        required_build_seconds=3_000,
        reason="remaining_capacity_too_small",
    )
    _assert_capacity_reservation_refused(
        verified,
        required_bytes_by_storage_class={"data": 1, "temp": 1, "wal": 1},
        minimum_remaining_bytes=1,
        required_build_seconds=3_301,
        reason="deadline_too_short",
    )
    skewed_lease = _verify(
        now=datetime.datetime(2026, 7, 30, 11, 59, 58, tzinfo=UTC)
    )
    _assert_capacity_reservation_refused(
        skewed_lease,
        required_bytes_by_storage_class={"data": 1, "temp": 1, "wal": 1},
        minimum_remaining_bytes=1,
        required_build_seconds=3_300,
        reason="deadline_too_short",
    )
