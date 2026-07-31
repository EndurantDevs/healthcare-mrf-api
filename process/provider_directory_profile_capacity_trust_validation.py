# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed selection of capacity-lease public verification keys."""

from __future__ import annotations

import datetime
import hmac
from collections.abc import Mapping
from typing import Any

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from process.provider_directory_profile_capacity_attestation_contract import (
    CAPACITY_LEASE_MAX_VALIDITY_SECONDS,
    CapacityLeaseTrust,
    CapacityLeaseTrustKey,
    CapacityLeaseTrustTablespace,
    CapacityLeaseTrustVolume,
    _MAX_OID,
    _database_name_value,
    _database_system_identifier,
    _error,
    _hex_digest,
    _integer,
    _opaque_id,
)
from process.provider_directory_profile_capacity_trust import (
    CAPACITY_TRUST_KEY_STATUSES,
    CAPACITY_TRUST_MAX_KEYS,
)


def _utc_second(value: Any, *, field: str) -> datetime.datetime:
    if (
        not isinstance(value, datetime.datetime)
        or value.tzinfo is None
        or value.utcoffset() != datetime.timedelta(0)
        or value.microsecond
    ):
        raise _error("invalid_value", field)
    return value


def _trust_key_entry(
    trust_key: CapacityLeaseTrustKey,
) -> Ed25519PublicKey:
    if not isinstance(trust_key, CapacityLeaseTrustKey):
        raise _error("invalid_type", "trust_key")
    _opaque_id(trust_key.key_id, field="trusted_key_id")
    _hex_digest(
        trust_key.attestor_release_digest,
        field="trusted_attestor_release_digest",
    )
    if trust_key.status not in CAPACITY_TRUST_KEY_STATUSES:
        raise _error("invalid_value", "trusted_key_status")
    if (
        not isinstance(trust_key.public_key, bytes)
        or len(trust_key.public_key) != 32
    ):
        raise _error("invalid_value", "public_key")
    try:
        public_key = Ed25519PublicKey.from_public_bytes(trust_key.public_key)
    except ValueError as exc:
        raise _error("invalid_value", "public_key") from exc
    _assert_rotation_metadata(trust_key)
    return public_key


def _assert_rotation_metadata(trust_key: CapacityLeaseTrustKey) -> None:
    if trust_key.status == "active":
        if trust_key.retired_at is not None or trust_key.verify_until is not None:
            raise _error("invalid_value", "active_key_retirement")
        return
    retired_at = _utc_second(
        trust_key.retired_at,
        field="trusted_key_retired_at",
    )
    verify_until = _utc_second(
        trust_key.verify_until,
        field="trusted_key_verify_until",
    )
    maximum_window = datetime.timedelta(
        seconds=CAPACITY_LEASE_MAX_VALIDITY_SECONDS
    )
    if not retired_at < verify_until <= retired_at + maximum_window:
        raise _error("invalid_interval", "retired_key_verification")


def _validated_trust_keys(
    trust: CapacityLeaseTrust,
) -> tuple[dict[str, Ed25519PublicKey], tuple[str, ...]]:
    if (
        not isinstance(trust.keys, tuple)
        or not 1 <= len(trust.keys) <= CAPACITY_TRUST_MAX_KEYS
    ):
        raise _error("invalid_count", "trust_keys")
    if any(
        not isinstance(trust_key, CapacityLeaseTrustKey)
        for trust_key in trust.keys
    ):
        raise _error("invalid_type", "trust_key")
    public_keys_by_id: dict[str, Ed25519PublicKey] = {}
    key_ids = tuple(trust_key.key_id for trust_key in trust.keys)
    if key_ids != tuple(sorted(set(key_ids))):
        raise _error("invalid_order", "trust_keys")
    for trust_key in trust.keys:
        public_keys_by_id[trust_key.key_id] = _trust_key_entry(trust_key)
    return public_keys_by_id, key_ids


def _assert_active_key(trust: CapacityLeaseTrust) -> None:
    active_keys = tuple(
        trust_key
        for trust_key in trust.keys
        if trust_key.status == "active"
    )
    if (
        len(active_keys) != 1
        or not hmac.compare_digest(
            active_keys[0].key_id,
            trust.active_key_id,
        )
    ):
        raise _error("active_key_mismatch", "active_key_id")


def _validated_trust_tablespace(
    tablespace: CapacityLeaseTrustTablespace,
    *,
    expected_usage: str,
) -> tuple[int, str, str]:
    if not isinstance(tablespace, CapacityLeaseTrustTablespace):
        raise _error("invalid_type", "trust_tablespace")
    if tablespace.usage != expected_usage:
        raise _error("invalid_order", "trust_tablespaces")
    return (
        _integer(
            tablespace.tablespace_oid,
            field="trusted_tablespace_oid",
            minimum=1,
            maximum=_MAX_OID,
        ),
        _database_name_value(
            tablespace.tablespace_name,
            field="trusted_tablespace_name",
        ),
        _hex_digest(
            tablespace.volume_digest,
            field="trusted_tablespace_volume_digest",
        ),
    )


def _validated_trust_volume(
    volume: CapacityLeaseTrustVolume,
    *,
    expected_class: str,
) -> str:
    if (
        not isinstance(volume, CapacityLeaseTrustVolume)
        or volume.volume_class != expected_class
    ):
        raise _error("invalid_order", "trust_volumes")
    return _hex_digest(
        volume.volume_digest,
        field="trusted_volume_digest",
    )


def _assert_trust_storage(trust: CapacityLeaseTrust) -> None:
    _database_system_identifier(trust.database_system_identifier)
    _integer(
        trust.database_oid,
        field="trusted_database_oid",
        minimum=1,
        maximum=_MAX_OID,
    )
    _database_name_value(
        trust.database_name,
        field="trusted_database_name",
    )
    if not isinstance(trust.tablespaces, tuple) or len(trust.tablespaces) != 2:
        raise _error("invalid_count", "trust_tablespaces")
    if not isinstance(trust.volumes, tuple) or len(trust.volumes) != 3:
        raise _error("invalid_count", "trust_volumes")
    tablespace_identities = tuple(
        _validated_trust_tablespace(entry, expected_usage=usage)
        for entry, usage in zip(
            trust.tablespaces,
            ("data", "temp"),
            strict=True,
        )
    )
    volume_digests = tuple(
        _validated_trust_volume(entry, expected_class=volume_class)
        for entry, volume_class in zip(
            trust.volumes,
            ("data", "temp", "wal"),
            strict=True,
        )
    )
    identity_by_oid: dict[int, tuple[str, str]] = {}
    for tablespace_oid, tablespace_name, volume_digest in tablespace_identities:
        identity = (tablespace_name, volume_digest)
        if identity_by_oid.setdefault(tablespace_oid, identity) != identity:
            raise _error("colocation_mismatch", "trust_tablespaces")
    if tuple(entry[2] for entry in tablespace_identities) != volume_digests[:2]:
        raise _error("binding_mismatch", "trust_tablespace_volume")


def _assert_pinned_text(actual: str, expected: str, *, field: str) -> None:
    if not hmac.compare_digest(actual, expected):
        raise _error("pin_mismatch", field)


def _storage_pins(
    entries: tuple[Any, ...],
    *,
    is_tablespace: bool,
) -> tuple[tuple[Any, ...], ...]:
    if is_tablespace:
        return tuple(
            (
                entry.tablespace_name,
                entry.tablespace_oid,
                entry.usage,
                entry.volume_digest,
            )
            for entry in entries
        )
    return tuple(
        (entry.volume_class, entry.volume_digest)
        for entry in entries
    )


def _assert_capacity_trust_scalar_pins(
    lease_fields: Mapping[str, Any],
    *,
    trust: CapacityLeaseTrust,
    trust_key: CapacityLeaseTrustKey,
    expected_capacity_geometry_hash: str,
    expected_database_system_identifier: str,
    expected_database_oid: int,
    expected_database_name: str,
) -> None:
    pinned_text_fields = (
        ("key_id", trust_key.key_id),
        ("environment_id", trust.environment_id),
        ("attestor_id", trust.attestor_id),
        ("attestor_release_digest", trust_key.attestor_release_digest),
        ("database_name", trust.database_name),
        (
            "capacity_geometry_hash",
            _hex_digest(
                expected_capacity_geometry_hash,
                field="expected_capacity_geometry_hash",
            ),
        ),
    )
    for field_name, expected_text in pinned_text_fields:
        _assert_pinned_text(
            lease_fields[field_name],
            expected_text,
            field=field_name,
        )
    expected_system = _database_system_identifier(expected_database_system_identifier)
    _assert_pinned_text(
        lease_fields["database_system_identifier"],
        expected_system,
        field="database_system_identifier",
    )
    expected_oid = _integer(
        expected_database_oid,
        field="expected_database_oid",
        minimum=1,
        maximum=_MAX_OID,
    )
    if lease_fields["database_oid"] != expected_oid:
        raise _error("pin_mismatch", "database_oid")
    expected_name = _database_name_value(
        expected_database_name,
        field="expected_database_name",
    )
    _assert_pinned_text(
        lease_fields["database_name"],
        expected_name,
        field="database_name",
    )
    if (
        lease_fields["database_system_identifier"]
        != trust.database_system_identifier
        or lease_fields["database_oid"] != trust.database_oid
    ):
        raise _error("pin_mismatch", "trusted_database")


def _assert_capacity_trust_storage_pins(
    lease_fields: Mapping[str, Any],
    trust: CapacityLeaseTrust,
) -> None:
    if (
        _storage_pins(
            lease_fields["tablespaces"],
            is_tablespace=True,
        )
        != _storage_pins(trust.tablespaces, is_tablespace=True)
        or _storage_pins(
            lease_fields["volumes"],
            is_tablespace=False,
        )
        != _storage_pins(trust.volumes, is_tablespace=False)
    ):
        raise _error("pin_mismatch", "trusted_storage")


def assert_capacity_trust_binding(
    lease_fields: Mapping[str, Any],
    *,
    trust: CapacityLeaseTrust,
    trust_key: CapacityLeaseTrustKey,
    expected_capacity_geometry_hash: str,
    expected_database_system_identifier: str,
    expected_database_oid: int,
    expected_database_name: str,
) -> None:
    """Bind an assigned lease to public trust and observed PostgreSQL."""

    _assert_capacity_trust_scalar_pins(
        lease_fields,
        trust=trust,
        trust_key=trust_key,
        expected_capacity_geometry_hash=expected_capacity_geometry_hash,
        expected_database_system_identifier=(
            expected_database_system_identifier
        ),
        expected_database_oid=expected_database_oid,
        expected_database_name=expected_database_name,
    )
    _assert_capacity_trust_storage_pins(lease_fields, trust)


def _assert_retired_lease_window(
    trust_key: CapacityLeaseTrustKey,
    lease_fields: Mapping[str, Any],
    *,
    now: datetime.datetime,
) -> None:
    retired_at = _utc_second(
        trust_key.retired_at,
        field="trusted_key_retired_at",
    )
    verify_until = _utc_second(
        trust_key.verify_until,
        field="trusted_key_verify_until",
    )
    if (
        now >= verify_until
        or lease_fields["issued_at"] > retired_at
        or lease_fields["expires_at"] > verify_until
        or lease_fields["max_build_deadline"] > verify_until
    ):
        raise _error("retired_key_outside_window", "key_id")


def capacity_trust_key_for_assigned_lease(
    trust: CapacityLeaseTrust,
    lease_fields: Mapping[str, Any],
    *,
    now: datetime.datetime,
) -> tuple[CapacityLeaseTrustKey, Ed25519PublicKey]:
    """Select the exact public key for one already-assigned lease envelope."""

    if not isinstance(trust, CapacityLeaseTrust):
        raise _error("invalid_type", "trust")
    _opaque_id(trust.environment_id, field="trusted_environment_id")
    _opaque_id(trust.attestor_id, field="trusted_attestor_id")
    _opaque_id(trust.active_key_id, field="active_key_id")
    public_keys_by_id, _key_ids = _validated_trust_keys(trust)
    _assert_active_key(trust)
    _assert_trust_storage(trust)
    lease_key_id = _opaque_id(lease_fields["key_id"], field="key_id")
    selected_key = next(
        (
            trust_key
            for trust_key in trust.keys
            if hmac.compare_digest(trust_key.key_id, lease_key_id)
        ),
        None,
    )
    if selected_key is None:
        raise _error("unknown_key", "key_id")
    if selected_key.status == "retired":
        _assert_retired_lease_window(selected_key, lease_fields, now=now)
    return selected_key, public_keys_by_id[selected_key.key_id]


__all__ = (
    "assert_capacity_trust_binding",
    "capacity_trust_key_for_assigned_lease",
)
