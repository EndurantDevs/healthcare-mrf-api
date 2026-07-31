# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Public verification-key policy for Provider Directory capacity leases."""

from __future__ import annotations

import datetime
from dataclasses import dataclass


CAPACITY_TRUST_CONTRACT_ID = (
    "provider-directory-database-capacity-trust-v2"
)
CAPACITY_TRUST_MAX_DOCUMENT_BYTES = 16 * 1024
CAPACITY_TRUST_MAX_KEYS = 16
CAPACITY_TRUST_KEY_STATUSES = ("active", "retired")


@dataclass(frozen=True)
class CapacityLeaseTrustKey:
    """One public verification key and its bounded rotation state."""

    public_key: bytes
    key_id: str
    attestor_release_digest: str
    status: str
    retired_at: datetime.datetime | None
    verify_until: datetime.datetime | None


@dataclass(frozen=True)
class CapacityLeaseTrustTablespace:
    """One PostgreSQL tablespace pinned to an opaque physical volume."""

    tablespace_name: str
    tablespace_oid: int
    usage: str
    volume_digest: str


@dataclass(frozen=True)
class CapacityLeaseTrustVolume:
    """One storage-class-to-volume identity pin."""

    volume_class: str
    volume_digest: str


@dataclass(frozen=True)
class CapacityLeaseTrust:
    """Closed public trust set for one stable capacity authority."""

    environment_id: str
    attestor_id: str
    active_key_id: str
    keys: tuple[CapacityLeaseTrustKey, ...]
    database_system_identifier: str
    database_oid: int
    database_name: str
    tablespaces: tuple[CapacityLeaseTrustTablespace, ...]
    volumes: tuple[CapacityLeaseTrustVolume, ...]


__all__ = (
    "CAPACITY_TRUST_CONTRACT_ID",
    "CAPACITY_TRUST_KEY_STATUSES",
    "CAPACITY_TRUST_MAX_DOCUMENT_BYTES",
    "CAPACITY_TRUST_MAX_KEYS",
    "CapacityLeaseTrust",
    "CapacityLeaseTrustKey",
    "CapacityLeaseTrustTablespace",
    "CapacityLeaseTrustVolume",
)
