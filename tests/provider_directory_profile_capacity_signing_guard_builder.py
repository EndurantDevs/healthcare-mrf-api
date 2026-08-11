# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Assembly of the synthetic closed Profile capacity signing chain."""

from __future__ import annotations

import datetime
from typing import Any

from process import provider_directory_profile_capacity_preflight_contract as preflight
from tests import (
    provider_directory_profile_capacity_signing_guard_test_support as support,
)


def _capacity_receipt_material(
    *,
    capacity_geometry_hash: str,
    observed_at: datetime.datetime,
    issued_at: datetime.datetime,
    expires_at: datetime.datetime,
    max_build_deadline: datetime.datetime,
    request_nonce: str,
) -> tuple[dict[str, object], ...]:
    timing = support._GuardTiming(
        observed_at, issued_at, expires_at, max_build_deadline, request_nonce
    )
    execution = support.synthetic_profile_execution()
    execution_by_field = support._execution_by_field(execution)
    limits_by_field = support._limits_payload()
    storage_by_field = support._storage_observation(timing)
    import_request_by_field = support._control_plane_request(
        execution_by_field, limits_by_field, storage_by_field, timing
    )
    execution_request = support._execution_request(
        execution_by_field, limits_by_field, timing
    )
    execution_identity_by_field = preflight.profile_execution_identity_payload(
        execution_request
    )
    followup_by_field = support._held_followup(execution, timing)
    import_receipt_by_field = support._control_plane_receipt(
        import_request_by_field,
        execution_request,
        execution_identity_by_field,
        storage_by_field,
        followup_by_field,
        timing,
    )
    healthcare_request_by_field = support._healthcare_request(
        execution_by_field, limits_by_field, import_receipt_by_field, timing
    )
    validated_healthcare_request = support._validated_request(
        healthcare_request_by_field
    )
    healthcare_receipt_by_field = support._healthcare_receipt(
        validated_healthcare_request,
        import_receipt_by_field,
        execution_identity_by_field,
        limits_by_field,
        capacity_geometry_hash,
        timing,
    )
    return (
        import_request_by_field,
        import_receipt_by_field,
        healthcare_request_by_field,
        validated_healthcare_request,
        healthcare_receipt_by_field,
        followup_by_field,
    )


def build_capacity_signing_guard(
    *,
    capacity_geometry_hash: str,
    observed_at: datetime.datetime,
    issued_at: datetime.datetime,
    expires_at: datetime.datetime,
    max_build_deadline: datetime.datetime,
    request_nonce: str,
) -> tuple[dict[str, object], str, str]:
    """Return guard payload, guard digest, and its healthcare receipt ID."""

    (
        import_request_by_field,
        import_receipt_by_field,
        healthcare_request_by_field,
        validated_healthcare_request,
        healthcare_receipt_by_field,
        followup_by_field,
    ) = _capacity_receipt_material(
        capacity_geometry_hash=capacity_geometry_hash,
        observed_at=observed_at,
        issued_at=issued_at,
        expires_at=expires_at,
        max_build_deadline=max_build_deadline,
        request_nonce=request_nonce,
    )
    guard_by_field = support._guard_payload(
        import_request_by_field,
        import_receipt_by_field,
        healthcare_request_by_field,
        validated_healthcare_request,
        healthcare_receipt_by_field,
        followup_by_field,
    )
    guard_sha256 = preflight.preflight_domain_sha256(
        support.guard_contract.CAPACITY_SIGNING_PREFLIGHT_GUARD_DIGEST_DOMAIN,
        guard_by_field,
    )
    return (
        guard_by_field,
        guard_sha256,
        str(healthcare_receipt_by_field["receipt_sha256"]),
    )


__all__ = ("build_capacity_signing_guard",)
