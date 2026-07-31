# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable ledger values for a consumed Provider Directory capacity lease."""

from __future__ import annotations

import datetime
from typing import Any

from process.provider_directory_profile_capacity_attestation_contract import (
    CAPACITY_LEASE_CONTRACT_ID,
    CapacityLeaseConsumptionBinding,
    VerifiedDatabaseCapacityLease,
    _BUILD_ID,
    _RUN_ID,
    _error,
    _hex_digest,
    _opaque_id,
)


def _profile_as_of(candidate: Any) -> str:
    if not isinstance(candidate, str):
        raise _error("invalid_value", "profile_as_of")
    try:
        parsed_date = datetime.date.fromisoformat(candidate)
    except ValueError as exc:
        raise _error("invalid_value", "profile_as_of") from exc
    if parsed_date.isoformat() != candidate:
        raise _error("invalid_value", "profile_as_of")
    return candidate


def _capacity_consumption_build_fields(
    binding: CapacityLeaseConsumptionBinding,
) -> dict[str, Any]:
    run_id = _opaque_id(binding.run_id, field="run_id", maximum_length=64)
    build_id = _opaque_id(
        binding.build_id, field="build_id", maximum_length=64
    )
    if not _RUN_ID.fullmatch(run_id):
        raise _error("invalid_value", "run_id")
    if not _BUILD_ID.fullmatch(build_id):
        raise _error("invalid_value", "build_id")
    return {
        "run_id": run_id,
        "build_id": build_id,
        "executable_plan_hash": _hex_digest(
            binding.executable_plan_hash, field="executable_plan_hash"
        ),
        "selection_proof_id": _hex_digest(
            binding.selection_proof_id, field="selection_proof_id"
        ),
        "source_vector_hash": _hex_digest(
            binding.source_vector_hash, field="source_vector_hash"
        ),
        "source_context_vector_hash": _hex_digest(
            binding.source_context_vector_hash,
            field="source_context_vector_hash",
        ),
        "profile_as_of": _profile_as_of(binding.profile_as_of),
    }


def capacity_lease_consumption_values(
    capacity_lease: VerifiedDatabaseCapacityLease,
    binding: CapacityLeaseConsumptionBinding,
    *,
    accepted_at: datetime.datetime,
) -> dict[str, Any]:
    """Return exact values for one immutable lease-consumption insert."""

    if not isinstance(capacity_lease, VerifiedDatabaseCapacityLease):
        raise _error("invalid_type", "lease")
    if capacity_lease.contract_id != CAPACITY_LEASE_CONTRACT_ID:
        raise _error("unsupported_contract", "contract_id")
    if not isinstance(binding, CapacityLeaseConsumptionBinding):
        raise _error("invalid_type", "binding")
    if (
        not isinstance(accepted_at, datetime.datetime)
        or accepted_at.tzinfo is None
        or accepted_at.utcoffset() is None
    ):
        raise _error("invalid_type", "accepted_at")
    accepted_at = accepted_at.astimezone(datetime.timezone.utc)
    if (
        accepted_at >= capacity_lease.expires_at
        or accepted_at >= capacity_lease.max_build_deadline
    ):
        raise _error("expired", "accepted_at")
    consumption_by_field = {
        "attestation_id": capacity_lease.attestation_id,
        "reservation_id": capacity_lease.reservation_id,
        "lease_digest": capacity_lease.lease_digest,
        "capacity_geometry_hash": capacity_lease.capacity_geometry_hash,
        **_capacity_consumption_build_fields(binding),
        "contract_id": capacity_lease.contract_id,
        "key_id": capacity_lease.key_id,
        "environment_id": capacity_lease.environment_id,
        "attestor_id": capacity_lease.attestor_id,
        "attestor_release_digest": (
            capacity_lease.attestor_release_digest
        ),
        "public_key_fingerprint": capacity_lease.public_key_fingerprint,
        "database_system_identifier": (
            capacity_lease.database_system_identifier
        ),
        "database_oid": capacity_lease.database_oid,
        "database_name": capacity_lease.database_name,
        "tablespace_identity_hash": (
            capacity_lease.tablespace_identity_hash
        ),
        "volume_identity_hash": capacity_lease.volume_identity_hash,
        "canonical_lease_json": capacity_lease.canonical_lease_json,
        "signature": capacity_lease.signature,
        "observed_at": capacity_lease.observed_at,
        "issued_at": capacity_lease.issued_at,
        "accepted_at": accepted_at,
        "expires_at": capacity_lease.expires_at,
        "max_build_deadline": capacity_lease.max_build_deadline,
        "recorded_at": accepted_at,
    }
    return consumption_by_field


__all__ = ("capacity_lease_consumption_values",)
