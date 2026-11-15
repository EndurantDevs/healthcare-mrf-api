# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared public trust-set fixtures for capacity-lease tests."""

from __future__ import annotations

from collections.abc import Mapping

from process import provider_directory_profile_capacity_attestation as lease

PUBLIC_KEY_HEX = "03a107bff3ce10be1d70dd18e74bc09967e4d6309ba50d5f1ddc8664125531b8"


def capacity_trust(**overrides: object) -> lease.CapacityLeaseTrust:
    """Build the exact logical active-key trust used by current and historical leases."""
    key_id = str(overrides.pop("key_id", "capacity-key-2026-07"))
    key = lease.CapacityLeaseTrustKey(
        public_key=overrides.pop("public_key", bytes.fromhex(PUBLIC_KEY_HEX)),
        key_id=key_id,
        attestor_release_digest=overrides.pop("attestor_release_digest", "11" * 32),
        status="active",
        retired_at=None,
        verify_until=None,
    )
    trust_by_field = {
        "environment_id": "dev-us",
        "attestor_id": "capacity-authority-dev",
        "active_key_id": key_id,
        "keys": (key,),
    }
    trust_by_field.update(overrides)
    return lease.CapacityLeaseTrust(**trust_by_field)


def capacity_trust_from_envelope(envelope: Mapping[str, object]) -> lease.CapacityLeaseTrust:
    """Build public key and environment policy from one synthetic signed fixture."""
    body = envelope["lease"]
    return capacity_trust(
        key_id=body["key_id"],
        environment_id=body["environment_id"],
        attestor_id=body["attestor_id"],
        attestor_release_digest=body["attestor_release_digest"],
    )


__all__ = (
    "PUBLIC_KEY_HEX",
    "capacity_trust",
    "capacity_trust_from_envelope",
)
