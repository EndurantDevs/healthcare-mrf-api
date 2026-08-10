# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed control_plane -> healthcare -> control_plane replay material signed into lease v3."""

from __future__ import annotations

import datetime
import hmac
from collections.abc import Mapping
from typing import Any

from process.provider_directory_profile_capacity_preflight_contract import (
    CAPACITY_CONTROL_PLANE_RECEIPT_SHA256_FIELD,
    ProviderDirectoryProfileCapacityPreflightError,
    preflight_domain_sha256,
    validated_capacity_preflight_request,
)
from process.provider_directory_profile_capacity_signing_receipts import (
    _validated_healthcare_receipt,
    _validated_control_plane_receipt,
    _validated_control_plane_request,
)
from process.provider_directory_profile_capacity_signing_guard_contract import (
    CAPACITY_BUNDLE_VALIDATION_PATH,
    CAPACITY_SIGNING_PREFLIGHT_GUARD_CONTRACT_ID,
    CAPACITY_SIGNING_PREFLIGHT_GUARD_DIGEST_DOMAIN,
    HEALTHCARE_PREFLIGHT_PATH,
    CONTROL_PLANE_PREFLIGHT_CONTRACT_ID,
    CONTROL_PLANE_PREFLIGHT_PATH,
    CONTROL_PLANE_PREFLIGHT_PATH_FIELD,
    CONTROL_PLANE_PREFLIGHT_REQUEST_CONTRACT_ID,
    CONTROL_PLANE_QUIESCENCE_CONTRACT_ID,
    CONTROL_PLANE_QUIESCENCE_DIGEST_DOMAIN,
    CONTROL_PLANE_RECEIPT_FIELD,
    CONTROL_PLANE_REQUEST_DIGEST_DOMAIN,
    CONTROL_PLANE_REQUEST_FIELD,
    CONTROL_PLANE_REQUEST_SHA256_FIELD,
    CONTROL_PLANE_SIGNING_INTENT_CONTRACT_ID,
    CONTROL_PLANE_STORAGE_OBSERVATION_DIGEST_DOMAIN,
    ProfileCapacitySigningGuardError,
    _GUARD_FIELDS,
    _exact,
    _fail,
    _hex,
    _plain_json,
    _timestamp,
)


def _validated_guard_header(
    raw_guard: Any,
    supplied_sha256: Any,
) -> tuple[dict[str, Any], str]:
    """Validate the closed guard fields, paths, and top-level digest."""

    guard_by_field = _exact(raw_guard, _GUARD_FIELDS, "fields_invalid")
    guard_sha256 = _hex(supplied_sha256, "sha256_invalid")
    expected_path_by_field = {
        "contract_id": CAPACITY_SIGNING_PREFLIGHT_GUARD_CONTRACT_ID,
        "bundle_validation_path": CAPACITY_BUNDLE_VALIDATION_PATH,
        CONTROL_PLANE_PREFLIGHT_PATH_FIELD: CONTROL_PLANE_PREFLIGHT_PATH,
        "healthcare_preflight_path": HEALTHCARE_PREFLIGHT_PATH,
    }
    if any(
        guard_by_field.get(field_name) != expected_value
        for field_name, expected_value in expected_path_by_field.items()
    ):
        _fail("contract_or_path_invalid")
    expected_sha256 = preflight_domain_sha256(
        CAPACITY_SIGNING_PREFLIGHT_GUARD_DIGEST_DOMAIN,
        guard_by_field,
    )
    if not hmac.compare_digest(guard_sha256, expected_sha256):
        _fail("sha256_mismatch")
    return guard_by_field, guard_sha256


def _assert_guard_hash_links(
    guard_by_field: Mapping[str, Any],
    healthcare_request: Any,
    control_plane_receipt_by_field: Mapping[str, Any],
    healthcare_receipt_by_field: Mapping[str, Any],
) -> None:
    """Require every redundant guard digest to match its closed object."""

    expected_hash_by_field = {
        CONTROL_PLANE_REQUEST_SHA256_FIELD: control_plane_receipt_by_field[
            "request_sha256"
        ],
        CAPACITY_CONTROL_PLANE_RECEIPT_SHA256_FIELD: control_plane_receipt_by_field[
            "receipt_sha256"
        ],
        "healthcare_request_sha256": healthcare_request.request_sha256,
        "healthcare_receipt_sha256": healthcare_receipt_by_field["receipt_sha256"],
        "capacity_limits_sha256": healthcare_request.limits_sha256,
        "storage_observation_sha256": control_plane_receipt_by_field[
            "storage_observation_sha256"
        ],
        "held_followup_preimage_sha256": control_plane_receipt_by_field[
            "held_followup"
        ]["followup_preimage_sha256"],
    }
    for field_name, expected_hash in expected_hash_by_field.items():
        observed_hash = _hex(guard_by_field.get(field_name), f"{field_name}_invalid")
        if observed_hash != expected_hash:
            _fail(f"{field_name}_mismatch")


def _assert_guard_lease_links(
    healthcare_request: Any,
    control_plane_receipt_by_field: Mapping[str, Any],
    healthcare_receipt_by_field: Mapping[str, Any],
    storage_by_field: Mapping[str, Any],
    lease_by_field: Mapping[str, Any],
) -> None:
    """Require the replay chain to bind the exact signed lease interval."""

    storage_time_by_name = {
        field_name: _timestamp(
            storage_by_field[field_name], f"storage_{field_name}_invalid"
        )
        for field_name in (
            "observed_at",
            "issued_at",
            "expires_at",
            "max_build_deadline",
        )
    }
    if (
        healthcare_request.control_plane_receipt_sha256
        != control_plane_receipt_by_field["receipt_sha256"]
        or healthcare_request.request_nonce
        != control_plane_receipt_by_field["request_nonce"]
        or healthcare_receipt_by_field["receipt_sha256"] != lease_by_field["nonce"]
        or healthcare_receipt_by_field["capacity_geometry_hash"]
        != lease_by_field["capacity_geometry_hash"]
        or any(
            storage_time_by_name[field_name] != lease_by_field[field_name]
            for field_name in storage_time_by_name
        )
        or healthcare_request.expires_at != lease_by_field["expires_at"]
    ):
        _fail("lease_binding_invalid")


def validated_capacity_signing_preflight_guard(
    raw_guard: Any,
    supplied_sha256: Any,
    *,
    lease_nonce: str,
    lease_capacity_geometry_hash: str,
    lease_observed_at: datetime.datetime,
    lease_issued_at: datetime.datetime,
    lease_expires_at: datetime.datetime,
    lease_max_build_deadline: datetime.datetime,
) -> tuple[dict[str, Any], str]:
    """Validate every closed replay request, receipt, digest, and lease link."""

    guard_by_field, guard_sha256 = _validated_guard_header(
        raw_guard,
        supplied_sha256,
    )
    try:
        healthcare_request = validated_capacity_preflight_request(
            guard_by_field.get("healthcare_request")
        )
    except ProviderDirectoryProfileCapacityPreflightError:
        _fail("healthcare_request_invalid")
    control_plane_request, storage_by_field = _validated_control_plane_request(
        guard_by_field.get(CONTROL_PLANE_REQUEST_FIELD), healthcare_request
    )
    control_plane_receipt_by_field = _validated_control_plane_receipt(
        guard_by_field.get(CONTROL_PLANE_RECEIPT_FIELD),
        control_plane_request,
        storage_by_field,
        healthcare_request,
    )
    healthcare_receipt_by_field = _validated_healthcare_receipt(
        guard_by_field.get("healthcare_receipt"),
        healthcare_request,
        control_plane_receipt_by_field,
    )
    _assert_guard_hash_links(
        guard_by_field,
        healthcare_request,
        control_plane_receipt_by_field,
        healthcare_receipt_by_field,
    )
    _assert_guard_lease_links(
        healthcare_request,
        control_plane_receipt_by_field,
        healthcare_receipt_by_field,
        storage_by_field,
        {
            "nonce": lease_nonce,
            "capacity_geometry_hash": lease_capacity_geometry_hash,
            "observed_at": lease_observed_at,
            "issued_at": lease_issued_at,
            "expires_at": lease_expires_at,
            "max_build_deadline": lease_max_build_deadline,
        },
    )
    normalized_guard = _plain_json(guard_by_field, "json_invalid")
    return normalized_guard, guard_sha256


def validated_capacity_signing_guard_fields(
    lease_body: Mapping[str, Any],
    parsed_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    """Add the validated replay guard to parsed lease-v3 fields."""

    signing_guard, signing_guard_sha256 = validated_capacity_signing_preflight_guard(
        lease_body["signing_preflight_guard"],
        lease_body["signing_preflight_guard_sha256"],
        lease_nonce=parsed_by_field["nonce"],
        lease_capacity_geometry_hash=parsed_by_field["capacity_geometry_hash"],
        lease_observed_at=parsed_by_field["observed_at"],
        lease_issued_at=parsed_by_field["issued_at"],
        lease_expires_at=parsed_by_field["expires_at"],
        lease_max_build_deadline=parsed_by_field["max_build_deadline"],
    )
    return {
        **parsed_by_field,
        "signing_preflight_guard": signing_guard,
        "signing_preflight_guard_sha256": signing_guard_sha256,
    }


__all__ = (
    "CAPACITY_BUNDLE_VALIDATION_PATH",
    "CAPACITY_SIGNING_PREFLIGHT_GUARD_CONTRACT_ID",
    "CAPACITY_SIGNING_PREFLIGHT_GUARD_DIGEST_DOMAIN",
    "HEALTHCARE_PREFLIGHT_PATH",
    "CONTROL_PLANE_PREFLIGHT_PATH",
    "ProfileCapacitySigningGuardError",
    "validated_capacity_signing_guard_fields",
    "validated_capacity_signing_preflight_guard",
)
