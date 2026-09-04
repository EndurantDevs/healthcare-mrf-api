# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed trust-set parsing and key-rotation coverage."""

from __future__ import annotations

import dataclasses
import datetime
import hashlib
import json
from pathlib import Path

import pytest

from process import provider_directory_profile_capacity_attestation as lease
from process import provider_directory_profile_capacity_runtime as runtime
from process import provider_directory_profile_selection_contract as selection
from process.provider_directory_profile_capacity_trust import (
    CAPACITY_TRUST_MAX_DOCUMENT_BYTES,
    CAPACITY_TRUST_MAX_KEYS,
)
from tests.provider_directory_profile_capacity_trust_fixtures import (
    PUBLIC_KEY_HEX,
    capacity_trust,
)
from tests.provider_directory_profile_capacity_signing_guard_test_support import (
    synthetic_profile_task,
)
from tests.test_provider_directory_profile_capacity_attestation import (
    VALIDATION_TIME,
    _signed_envelope,
    _verify,
)

UTC = datetime.timezone.utc
EXECUTION_V2_FIXTURE = (
    Path(__file__).resolve().parent
    / "fixtures/provider_directory_profile_execution_v2_golden.json"
)
EXECUTION_V2_CANONICAL_SHA256 = "7f9c37b833c60592e4e0fc014c24d167edcb06d2cde45bbce32c8caf5851e016"
EXECUTION_V2_FILE_SHA256 = "2a19f484bad2cb43a52f11e7d3bdf911fc598dde03c45f68f506dd011d33cc10"


def _active_key(
    key_id: str = "capacity-key-2026-07",
) -> dict[str, object]:
    return {
        "key_id": key_id,
        "public_key_hex": PUBLIC_KEY_HEX,
        "attestor_release_digest": "11" * 32,
        "status": "active",
        "retired_at": None,
        "verify_until": None,
    }


def _trust_payload(**overrides: object) -> dict[str, object]:
    trust_by_field: dict[str, object] = {
        "contract_id": runtime.CAPACITY_TRUST_CONTRACT_ID,
        "signature_algorithm": lease.CAPACITY_LEASE_SIGNATURE_ALGORITHM,
        "environment_id": "dev-us",
        "attestor_id": "capacity-authority-dev",
        "active_key_id": "capacity-key-2026-07",
        "keys": [_active_key()],
        "database_system_identifier": "7527713908662902214",
        "database_oid": 16401,
        "database_name": "healthporta_test",
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
            {"volume_class": "data", "volume_digest": "33" * 32},
            {"volume_class": "temp", "volume_digest": "33" * 32},
            {"volume_class": "wal", "volume_digest": "44" * 32},
        ],
    }
    trust_by_field.update(overrides)
    return trust_by_field


def _retired_trust(
    *,
    retired_at: datetime.datetime | None = None,
    verify_until: datetime.datetime | None = None,
) -> lease.CapacityLeaseTrust:
    retired_key = lease.CapacityLeaseTrustKey(
        public_key=bytes.fromhex(PUBLIC_KEY_HEX),
        key_id="capacity-key-2026-07",
        attestor_release_digest="11" * 32,
        status="retired",
        retired_at=retired_at or datetime.datetime(2026, 7, 30, 12, 0, 2, tzinfo=UTC),
        verify_until=verify_until or datetime.datetime(2026, 7, 30, 13, 0, tzinfo=UTC),
    )
    active_key = lease.CapacityLeaseTrustKey(
        public_key=b"\x08" * 32,
        key_id="capacity-key-2026-08",
        attestor_release_digest="88" * 32,
        status="active",
        retired_at=None,
        verify_until=None,
    )
    return dataclasses.replace(
        capacity_trust(),
        active_key_id=active_key.key_id,
        keys=(retired_key, active_key),
    )


def test_trust_v2_is_mandatory_closed_public_configuration(monkeypatch):
    monkeypatch.delenv(runtime.CAPACITY_TRUST_ENV, raising=False)
    with pytest.raises(
        runtime.ProviderDirectoryProfileCapacityConfigurationError,
        match="trust_missing",
    ):
        runtime.configured_capacity_lease_trust()

    trust = runtime.configured_capacity_lease_trust(json.dumps(_trust_payload()))
    assert trust.environment_id == "dev-us"
    assert trust.active_key_id == "capacity-key-2026-07"
    assert trust.keys[0].public_key == bytes.fromhex(PUBLIC_KEY_HEX)
    assert trust.database_name == "healthporta_test"
    assert not hasattr(trust, "private_key")


def test_execution_v2_golden_freezes_neutral_cross_repository_envelope(
    monkeypatch,
):
    """Freeze bytes consumers must refresh when the signed runtime head moves."""
    fixture_bytes = EXECUTION_V2_FIXTURE.read_bytes()
    regenerated_bytes = (
        json.dumps(
            synthetic_profile_task(_signed_envelope()),
            indent=2,
            sort_keys=True,
            ensure_ascii=True,
        )
        + "\n"
    ).encode("ascii")
    assert fixture_bytes == regenerated_bytes
    assert len(fixture_bytes) == 27_814
    assert fixture_bytes.endswith(b"\n")
    assert not fixture_bytes.endswith(b"\n\n")
    assert hashlib.sha256(fixture_bytes).hexdigest() == (EXECUTION_V2_FILE_SHA256)
    task_map = json.loads(fixture_bytes)
    canonical = json.dumps(
        task_map,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    assert hashlib.sha256(canonical).hexdigest() == (EXECUTION_V2_CANONICAL_SHA256)
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    execution = selection.validated_profile_execution(task_map)
    assert execution.generation == 11
    assert (
        execution.capacity_attestation
        == task_map["provider_directory_profile_capacity_attestation"]
    )
    verified = _verify(dict(execution.capacity_attestation))
    assert (
        verified.attestation_id
        == task_map["provider_directory_profile_capacity_attestation"]["lease"][
            "attestation_id"
        ]
    )


@pytest.mark.parametrize(
    ("target", "field_name"),
    (
        ("trust", "private_key_hex"),
        ("trust", "unexpected"),
        ("key", "private_key_hex"),
        ("key", "unexpected"),
    ),
)
def test_trust_rejects_unknown_and_private_fields(target, field_name):
    payload = _trust_payload()
    selected = payload if target == "trust" else payload["keys"][0]
    selected[field_name] = "00" * 32

    with pytest.raises(runtime.ProviderDirectoryProfileCapacityConfigurationError):
        runtime.validated_capacity_lease_trust(payload)


def test_trust_rejects_legacy_shape_duplicate_json_and_oversize():
    legacy_trust_by_field = {
        "public_key_hex": PUBLIC_KEY_HEX,
        "key_id": "capacity-key-2026-07",
    }
    with pytest.raises(runtime.ProviderDirectoryProfileCapacityConfigurationError):
        runtime.validated_capacity_lease_trust(legacy_trust_by_field)
    with pytest.raises(
        runtime.ProviderDirectoryProfileCapacityConfigurationError,
        match="trust_json_invalid",
    ):
        runtime.configured_capacity_lease_trust(
            '{"contract_id":"one","contract_id":"two"}'
        )
    with pytest.raises(
        runtime.ProviderDirectoryProfileCapacityConfigurationError,
        match="trust_document_too_large",
    ):
        runtime.configured_capacity_lease_trust(
            "x" * (CAPACITY_TRUST_MAX_DOCUMENT_BYTES + 1)
        )


@pytest.mark.parametrize(
    "mutation",
    ("tablespace_binding", "volume_order", "database_oid_bool"),
)
def test_trust_storage_identity_is_closed_and_physically_bound(mutation):
    trust_by_field = json.loads(json.dumps(_trust_payload()))
    if mutation == "tablespace_binding":
        trust_by_field["tablespaces"][0]["volume_digest"] = "99" * 32
    elif mutation == "volume_order":
        trust_by_field["volumes"].reverse()
    else:
        trust_by_field["database_oid"] = True

    with pytest.raises(runtime.ProviderDirectoryProfileCapacityConfigurationError):
        runtime.validated_capacity_lease_trust(trust_by_field)


@pytest.mark.parametrize(
    "keys",
    (
        [_active_key("key-b"), _active_key("key-a")],
        [_active_key(), _active_key()],
        [{**_active_key(), "status": "retired"}],
        [_active_key(), _active_key("capacity-key-2026-08")],
    ),
)
def test_trust_requires_sorted_unique_keys_and_exactly_one_active(keys):
    payload = _trust_payload(keys=keys)

    with pytest.raises(runtime.ProviderDirectoryProfileCapacityConfigurationError):
        runtime.validated_capacity_lease_trust(payload)


def test_trust_key_count_and_active_identity_are_bounded():
    too_many_keys = [
        {
            **_active_key(f"capacity-key-{index:02d}"),
            "status": "retired",
            "retired_at": "2026-07-30T12:00:00Z",
            "verify_until": "2026-07-31T12:00:00Z",
        }
        for index in range(CAPACITY_TRUST_MAX_KEYS + 1)
    ]
    too_many_keys[-1].update(
        {"status": "active", "retired_at": None, "verify_until": None}
    )
    with pytest.raises(runtime.ProviderDirectoryProfileCapacityConfigurationError):
        runtime.validated_capacity_lease_trust(
            _trust_payload(
                active_key_id=too_many_keys[-1]["key_id"],
                keys=too_many_keys,
            )
        )
    with pytest.raises(runtime.ProviderDirectoryProfileCapacityConfigurationError):
        runtime.validated_capacity_lease_trust(
            _trust_payload(active_key_id="unknown-key")
        )


@pytest.mark.parametrize(
    ("retired_at", "verify_until"),
    (
        (None, "2026-07-30T13:00:00Z"),
        ("2026-07-30T12:00:00+00:00", "2026-07-30T13:00:00Z"),
        ("2026-07-30T12:00:00Z", "2026-07-30T12:00:00Z"),
        ("2026-07-30T12:00:00Z", "2026-07-31T12:00:01Z"),
    ),
)
def test_retired_key_metadata_is_canonical_and_bounded(
    retired_at,
    verify_until,
):
    retired_key_by_field = {
        **_active_key(),
        "status": "retired",
        "retired_at": retired_at,
        "verify_until": verify_until,
    }
    active_key = _active_key("capacity-key-2026-08")
    with pytest.raises(runtime.ProviderDirectoryProfileCapacityConfigurationError):
        runtime.validated_capacity_lease_trust(
            _trust_payload(
                active_key_id=active_key["key_id"],
                keys=[retired_key_by_field, active_key],
            )
        )


def test_assigned_lease_selects_active_or_still_valid_retired_key():
    active_verified = _verify()
    retired_verified = _verify(trust=_retired_trust())

    assert active_verified.key_id == "capacity-key-2026-07"
    assert retired_verified.key_id == active_verified.key_id
    assert (
        retired_verified.public_key_fingerprint
        == active_verified.public_key_fingerprint
    )


def test_retired_key_cannot_verify_post_retirement_or_overlong_lease():
    post_retirement = _signed_envelope(
        body_mutator=lambda body: body.update(
            {
                "issued_at": "2026-07-30T12:00:03Z",
                "observed_at": "2026-07-30T12:00:02Z",
            }
        )
    )
    with pytest.raises(
        lease.ProviderDirectoryCapacityLeaseError,
        match="retired_key_outside_window",
    ):
        _verify(post_retirement, trust=_retired_trust())
    with pytest.raises(
        lease.ProviderDirectoryCapacityLeaseError,
        match="retired_key_outside_window",
    ):
        _verify(
            trust=_retired_trust(
                verify_until=datetime.datetime(
                    2026,
                    7,
                    30,
                    12,
                    59,
                    tzinfo=UTC,
                )
            )
        )


def test_unknown_key_release_database_and_storage_pins_fail_closed():
    mismatched_trusts = (
        capacity_trust(key_id="other-key"),
        capacity_trust(attestor_release_digest="99" * 32),
        dataclasses.replace(capacity_trust(), database_oid=16402),
        dataclasses.replace(
            capacity_trust(),
            volumes=(
                *capacity_trust().volumes[:2],
                dataclasses.replace(
                    capacity_trust().volumes[2],
                    volume_digest="99" * 32,
                ),
            ),
        ),
    )
    for trust in mismatched_trusts:
        with pytest.raises(lease.ProviderDirectoryCapacityLeaseError):
            _verify(trust=trust)


def test_runtime_database_name_is_an_exact_observed_pin():
    with pytest.raises(
        lease.ProviderDirectoryCapacityLeaseError,
        match="pin_mismatch",
    ):
        _verify(expected_database_name="other_database")


def test_retired_key_verification_window_must_still_be_open():
    with pytest.raises(
        lease.ProviderDirectoryCapacityLeaseError,
        match="retired_key_outside_window",
    ):
        _verify(
            trust=_retired_trust(),
            now=datetime.datetime(2026, 7, 30, 13, 0, tzinfo=UTC),
        )
