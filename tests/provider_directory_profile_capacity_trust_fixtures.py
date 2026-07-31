# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared public trust-set fixtures for capacity-lease tests."""

from __future__ import annotations

from collections.abc import Mapping

from process import provider_directory_profile_capacity_attestation as lease


PUBLIC_KEY_HEX = (
    "03a107bff3ce10be1d70dd18e74bc0996"
    "7e4d6309ba50d5f1ddc8664125531b8"
)


def capacity_trust(**overrides: object) -> lease.CapacityLeaseTrust:
    """Build the exact active-key trust used by the golden lease."""

    key_id = str(overrides.pop("key_id", "capacity-key-2026-07"))
    trust_key = lease.CapacityLeaseTrustKey(
        public_key=overrides.pop(
            "public_key",
            bytes.fromhex(PUBLIC_KEY_HEX),
        ),
        key_id=key_id,
        attestor_release_digest=overrides.pop(
            "attestor_release_digest",
            "11" * 32,
        ),
        status="active",
        retired_at=None,
        verify_until=None,
    )
    trust_by_field: dict[str, object] = {
        "environment_id": "dev-us",
        "attestor_id": "capacity-authority-dev",
        "active_key_id": key_id,
        "keys": (trust_key,),
        "database_system_identifier": "7527713908662902214",
        "database_oid": 16401,
        "database_name": "healthporta_test",
        "tablespaces": (
            lease.CapacityLeaseTrustTablespace(
                tablespace_name="pg_default",
                tablespace_oid=1663,
                usage="data",
                volume_digest="33" * 32,
            ),
            lease.CapacityLeaseTrustTablespace(
                tablespace_name="pg_default",
                tablespace_oid=1663,
                usage="temp",
                volume_digest="33" * 32,
            ),
        ),
        "volumes": (
            lease.CapacityLeaseTrustVolume(
                volume_class="data",
                volume_digest="33" * 32,
            ),
            lease.CapacityLeaseTrustVolume(
                volume_class="temp",
                volume_digest="33" * 32,
            ),
            lease.CapacityLeaseTrustVolume(
                volume_class="wal",
                volume_digest="44" * 32,
            ),
        ),
    }
    trust_by_field.update(overrides)
    return lease.CapacityLeaseTrust(**trust_by_field)


def capacity_trust_from_envelope(
    envelope: Mapping[str, object],
) -> lease.CapacityLeaseTrust:
    """Build public trust pins from one synthetic signed lease fixture."""

    lease_by_field = envelope["lease"]
    tablespaces = tuple(
        lease.CapacityLeaseTrustTablespace(
            tablespace_name=entry["tablespace_name"],
            tablespace_oid=entry["tablespace_oid"],
            usage=entry["usage"],
            volume_digest=entry["volume_digest"],
        )
        for entry in lease_by_field["tablespaces"]
    )
    volumes = tuple(
        lease.CapacityLeaseTrustVolume(
            volume_class=entry["volume_class"],
            volume_digest=entry["volume_digest"],
        )
        for entry in lease_by_field["volumes"]
    )
    return capacity_trust(
        key_id=lease_by_field["key_id"],
        environment_id=lease_by_field["environment_id"],
        attestor_id=lease_by_field["attestor_id"],
        attestor_release_digest=lease_by_field[
            "attestor_release_digest"
        ],
        database_system_identifier=lease_by_field[
            "database_system_identifier"
        ],
        database_oid=lease_by_field["database_oid"],
        database_name=lease_by_field["database_name"],
        tablespaces=tablespaces,
        volumes=volumes,
    )


__all__ = (
    "PUBLIC_KEY_HEX",
    "capacity_trust",
    "capacity_trust_from_envelope",
)
