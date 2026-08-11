# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed edge coverage for the Profile capacity signing contracts."""

from __future__ import annotations

import copy
import datetime
from types import SimpleNamespace

import pytest

from process import provider_directory_profile_capacity_preflight_contract as preflight
from process import provider_directory_profile_capacity_signing_guard as signing_guard
from process import (
    provider_directory_profile_capacity_signing_guard_contract as guard_fields,
)
from process import provider_directory_profile_capacity_signing_receipts as receipts
from tests import provider_directory_profile_capacity_signing_guard_builder as builder


UTC = datetime.timezone.utc
OBSERVED_AT = datetime.datetime(2026, 8, 10, 8, 50, tzinfo=UTC)
ISSUED_AT = datetime.datetime(2026, 8, 10, 9, 0, tzinfo=UTC)
MAX_BUILD_DEADLINE = datetime.datetime(2026, 8, 10, 9, 30, tzinfo=UTC)
EXPIRES_AT = datetime.datetime(2026, 8, 10, 10, 0, tzinfo=UTC)
CAPACITY_GEOMETRY_HASH = "aa" * 32
REQUEST_NONCE = "22" * 32


@pytest.fixture
def receipt_material() -> tuple[dict[str, object], ...]:
    return builder._capacity_receipt_material(
        capacity_geometry_hash=CAPACITY_GEOMETRY_HASH,
        observed_at=OBSERVED_AT,
        issued_at=ISSUED_AT,
        expires_at=EXPIRES_AT,
        max_build_deadline=MAX_BUILD_DEADLINE,
        request_nonce=REQUEST_NONCE,
    )


@pytest.fixture
def guard_material() -> tuple[dict[str, object], str, str]:
    return builder.build_capacity_signing_guard(
        capacity_geometry_hash=CAPACITY_GEOMETRY_HASH,
        observed_at=OBSERVED_AT,
        issued_at=ISSUED_AT,
        expires_at=EXPIRES_AT,
        max_build_deadline=MAX_BUILD_DEADLINE,
        request_nonce=REQUEST_NONCE,
    )


def test_preflight_primitives_reject_noncanonical_values() -> None:
    with pytest.raises(
        preflight.ProviderDirectoryProfileCapacityPreflightError,
        match="preflight_json_invalid",
    ):
        preflight.canonical_preflight_json({"not-json"})
    with pytest.raises(
        preflight.ProviderDirectoryProfileCapacityPreflightError,
        match="preflight_limits_invalid",
    ):
        preflight.canonical_capacity_limits_payload({})
    with pytest.raises(
        preflight.ProviderDirectoryProfileCapacityPreflightError,
        match="request_nonce_invalid",
    ):
        preflight._hex_digest("A" * 64, reason="request_nonce_invalid")
    with pytest.raises(
        preflight.ProviderDirectoryProfileCapacityPreflightError,
        match="expires_at_invalid",
    ):
        preflight._utc_timestamp("2026-02-30T00:00:00Z")


def test_preflight_timestamp_rejects_noncanonical_render(monkeypatch) -> None:
    class _ParsedTimestamp:
        def replace(self, **_kwargs):
            return self

        def strftime(self, _format: str) -> str:
            return "2026-08-10T09:00:01Z"

    class _NonCanonicalDatetime:
        @staticmethod
        def strptime(_value: str, _format: str) -> _ParsedTimestamp:
            return _ParsedTimestamp()

    monkeypatch.setattr(
        preflight,
        "datetime",
        SimpleNamespace(datetime=_NonCanonicalDatetime, timezone=datetime.timezone),
    )

    with pytest.raises(
        preflight.ProviderDirectoryProfileCapacityPreflightError,
        match="expires_at_invalid",
    ):
        preflight._utc_timestamp("2026-08-10T09:00:00Z")


def test_preflight_rejects_nonmapping_and_noncanonical_limits(
    monkeypatch,
    receipt_material,
) -> None:
    validated_request = receipt_material[3]
    with pytest.raises(
        preflight.ProviderDirectoryProfileCapacityPreflightError,
        match="preflight_limits_invalid",
    ):
        preflight._validated_limits_payload([])

    monkeypatch.setattr(
        preflight,
        "validated_capacity_limits",
        lambda _raw_limits: validated_request.limits,
    )
    with pytest.raises(
        preflight.ProviderDirectoryProfileCapacityPreflightError,
        match="limits_not_canonical",
    ):
        preflight._validated_limits_payload({"noncanonical": True})


def test_preflight_rejects_wrong_nested_and_outer_contracts(receipt_material) -> None:
    raw_request = copy.deepcopy(receipt_material[2])
    raw_request["contract_id"] = "synthetic.invalid-request.v1"
    with pytest.raises(
        preflight.ProviderDirectoryProfileCapacityPreflightError,
        match="request_contract_invalid",
    ):
        preflight.validated_capacity_preflight_request(raw_request)

    raw_guard = copy.deepcopy(receipt_material[2]["signing_guard"])
    raw_guard["contract_id"] = "synthetic.invalid-guard.v1"
    with pytest.raises(
        preflight.ProviderDirectoryProfileCapacityPreflightError,
        match="signing_guard_contract_invalid",
    ):
        preflight._validated_signing_guard(raw_guard)


def test_preflight_rejects_normalization_drift(
    monkeypatch,
    receipt_material,
) -> None:
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    raw_request = copy.deepcopy(receipt_material[2])
    validated_request = receipt_material[3]
    monkeypatch.setattr(
        preflight,
        "_validated_signing_guard",
        lambda _raw_guard: (
            "ff" * 32,
            validated_request.control_plane_receipt_sha256,
            validated_request.expires_at,
        ),
    )

    with pytest.raises(
        preflight.ProviderDirectoryProfileCapacityPreflightError,
        match="request_not_canonical",
    ):
        preflight.validated_capacity_preflight_request(raw_request)


def test_preflight_expiry_and_clock_are_strict(receipt_material) -> None:
    validated_request = receipt_material[3]
    with pytest.raises(
        preflight.ProviderDirectoryProfileCapacityPreflightError,
        match="expiry_invalid",
    ):
        preflight.assert_preflight_expiry(
            validated_request,
            issued_at=validated_request.expires_at,
        )
    with pytest.raises(
        preflight.ProviderDirectoryProfileCapacityPreflightError,
        match="clock_invalid",
    ):
        preflight.utc_second_text(datetime.datetime(2026, 8, 10, 9, 0))
    with pytest.raises(
        preflight.ProviderDirectoryProfileCapacityPreflightError,
        match="clock_invalid",
    ):
        preflight.utc_second_text(
            datetime.datetime(2026, 8, 10, 9, 0, 0, 1, tzinfo=UTC)
        )


def test_signing_guard_primitives_reject_invalid_scalars(monkeypatch) -> None:
    with pytest.raises(guard_fields.ProfileCapacitySigningGuardError):
        guard_fields._hex(None, "digest_invalid")
    with pytest.raises(guard_fields.ProfileCapacitySigningGuardError):
        guard_fields._integer(True, "integer_invalid")
    with pytest.raises(guard_fields.ProfileCapacitySigningGuardError):
        guard_fields._timestamp(None, "timestamp_invalid")
    with pytest.raises(guard_fields.ProfileCapacitySigningGuardError):
        guard_fields._timestamp("2026-02-30T00:00:00Z", "timestamp_invalid")
    monkeypatch.setattr(guard_fields, "utc_second_text", lambda _value: "different")
    with pytest.raises(guard_fields.ProfileCapacitySigningGuardError):
        guard_fields._timestamp("2026-08-10T09:00:00Z", "timestamp_invalid")
    with pytest.raises(guard_fields.ProfileCapacitySigningGuardError):
        guard_fields._plain_json({"not-json"}, "json_invalid")


@pytest.mark.parametrize(
    ("mutation", "error"),
    (
        ("interval", "storage_interval_invalid"),
        ("tablespace", "temp_tablespace_invalid"),
        ("volume_shape", "volumes_invalid"),
        ("volume_order", "volume_order_invalid"),
        ("volume_accounting", "volume_accounting_invalid"),
    ),
)
def test_storage_receipt_rejects_invalid_capacity_coordinates(
    receipt_material,
    mutation,
    error,
) -> None:
    storage = copy.deepcopy(receipt_material[0]["storage_observation"])
    if mutation == "interval":
        storage["observed_at"] = "2026-08-10T09:01:00Z"
    elif mutation == "tablespace":
        storage["temp_tablespace"]["tablespace_name"] = " invalid"
    elif mutation == "volume_shape":
        storage["volumes"] = []
    elif mutation == "volume_order":
        storage["volumes"][0]["volume_class"] = "temp"
    else:
        storage["volumes"][0]["available_after_all_reservations_bytes"] = (
            storage["volumes"][0]["available_bytes"] + 1
        )

    with pytest.raises(guard_fields.ProfileCapacitySigningGuardError, match=error):
        receipts._validated_storage_observation(storage)


@pytest.mark.parametrize(
    ("mutation", "error"),
    (
        ("contract", "control_plane_request_contract_invalid"),
        ("execution", "execution_or_limits_mismatch"),
    ),
)
def test_control_plane_request_rejects_contract_or_execution_drift(
    receipt_material,
    mutation,
    error,
) -> None:
    import_request = copy.deepcopy(receipt_material[0])
    if mutation == "contract":
        import_request["contract_id"] = "synthetic.invalid-import-request.v1"
    else:
        import_request["profile_execution"] = {}

    with pytest.raises(guard_fields.ProfileCapacitySigningGuardError, match=error):
        receipts._validated_control_plane_request(import_request, receipt_material[3])


def test_held_followup_rejects_binding_drift(receipt_material) -> None:
    followup = copy.deepcopy(receipt_material[5])
    followup["status"] = "running"

    with pytest.raises(
        guard_fields.ProfileCapacitySigningGuardError,
        match="held_followup_binding_invalid",
    ):
        receipts._validated_held_followup(
            followup,
            receipt_material[3],
            EXPIRES_AT,
        )


def test_control_plane_receipt_rejects_nonquiescent_state(receipt_material) -> None:
    import_receipt = copy.deepcopy(receipt_material[1])
    import_receipt["quiescence"]["active_profile_run_count"] = 1

    with pytest.raises(
        guard_fields.ProfileCapacitySigningGuardError,
        match="control_plane_quiescence_invalid",
    ):
        receipts._validated_control_plane_receipt(
            import_receipt,
            receipt_material[0],
            receipt_material[0]["storage_observation"],
            receipt_material[3],
        )


@pytest.mark.parametrize(
    ("mutation", "error"),
    (
        ("quiescence", "healthcare_quiescence_invalid"),
        ("serving", "healthcare_serving_preflight_invalid"),
        ("interval", "healthcare_receipt_binding_invalid"),
    ),
)
def test_healthcare_receipt_rejects_invalid_state(
    receipt_material,
    mutation,
    error,
) -> None:
    healthcare_receipt = copy.deepcopy(receipt_material[4])
    if mutation == "quiescence":
        healthcare_receipt["quiescence"]["active_profile_run_count"] = 1
    elif mutation == "serving":
        healthcare_receipt["serving_generation_preflight"] = None
    else:
        healthcare_receipt["issued_at"] = healthcare_receipt["expires_at"]

    with pytest.raises(guard_fields.ProfileCapacitySigningGuardError, match=error):
        receipts._validated_healthcare_receipt(
            healthcare_receipt,
            receipt_material[3],
            receipt_material[1],
        )


@pytest.mark.parametrize(
    ("mutation", "error"),
    (
        ("path", "contract_or_path_invalid"),
        ("digest", "sha256_mismatch"),
        ("healthcare_request", "healthcare_request_invalid"),
        ("hash_link", "capacity_limits_sha256_mismatch"),
        ("lease", "lease_binding_invalid"),
    ),
)
def test_capacity_signing_guard_rejects_every_replay_boundary(
    monkeypatch,
    guard_material,
    mutation,
    error,
) -> None:
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    raw_guard, guard_sha256, lease_nonce = guard_material
    raw_guard = copy.deepcopy(raw_guard)
    lease_capacity_geometry_hash = CAPACITY_GEOMETRY_HASH
    if mutation == "path":
        raw_guard["healthcare_preflight_path"] = "/synthetic/invalid"
    elif mutation == "digest":
        guard_sha256 = "00" * 32
    elif mutation == "healthcare_request":
        raw_guard["healthcare_request"] = {}
        guard_sha256 = preflight.preflight_domain_sha256(
            guard_fields.CAPACITY_SIGNING_PREFLIGHT_GUARD_DIGEST_DOMAIN,
            raw_guard,
        )
    elif mutation == "hash_link":
        raw_guard["capacity_limits_sha256"] = "ff" * 32
        guard_sha256 = preflight.preflight_domain_sha256(
            guard_fields.CAPACITY_SIGNING_PREFLIGHT_GUARD_DIGEST_DOMAIN,
            raw_guard,
        )
    else:
        lease_capacity_geometry_hash = "ff" * 32

    with pytest.raises(guard_fields.ProfileCapacitySigningGuardError, match=error):
        signing_guard.validated_capacity_signing_preflight_guard(
            raw_guard,
            guard_sha256,
            lease_nonce=lease_nonce,
            lease_capacity_geometry_hash=lease_capacity_geometry_hash,
            lease_observed_at=OBSERVED_AT,
            lease_issued_at=ISSUED_AT,
            lease_expires_at=EXPIRES_AT,
            lease_max_build_deadline=MAX_BUILD_DEADLINE,
        )
