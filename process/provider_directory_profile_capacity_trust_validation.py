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
    _error,
    _hex_digest,
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
    if not isinstance(trust_key.public_key, bytes) or len(trust_key.public_key) != 32:
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
    maximum_window = datetime.timedelta(seconds=CAPACITY_LEASE_MAX_VALIDITY_SECONDS)
    if not retired_at < verify_until <= retired_at + maximum_window:
        raise _error("invalid_interval", "retired_key_verification")


def _validated_trust_keys(
    trust: CapacityLeaseTrust,
) -> tuple[dict[str, Ed25519PublicKey], tuple[str, ...]]:
    if not isinstance(trust.keys, tuple) or not 1 <= len(trust.keys) <= CAPACITY_TRUST_MAX_KEYS:
        raise _error("invalid_count", "trust_keys")
    if any(not isinstance(trust_key, CapacityLeaseTrustKey) for trust_key in trust.keys):
        raise _error("invalid_type", "trust_key")
    public_keys_by_id: dict[str, Ed25519PublicKey] = {}
    key_ids = tuple(trust_key.key_id for trust_key in trust.keys)
    if key_ids != tuple(sorted(set(key_ids))):
        raise _error("invalid_order", "trust_keys")
    for trust_key in trust.keys:
        public_keys_by_id[trust_key.key_id] = _trust_key_entry(trust_key)
    return public_keys_by_id, key_ids


def _assert_active_key(trust: CapacityLeaseTrust) -> None:
    active_keys = tuple(trust_key for trust_key in trust.keys if trust_key.status == "active")
    if len(active_keys) != 1 or not hmac.compare_digest(
        active_keys[0].key_id,
        trust.active_key_id,
    ):
        raise _error("active_key_mismatch", "active_key_id")


def _assert_pinned_text(actual: str, expected: str, *, field: str) -> None:
    if not hmac.compare_digest(actual, expected):
        raise _error("pin_mismatch", field)


def assert_capacity_trust_binding(
    lease_fields: Mapping[str, Any],
    *,
    trust: CapacityLeaseTrust,
    trust_key: CapacityLeaseTrustKey,
    expected_capacity_geometry_hash: str,
) -> None:
    """Bind the signed source, client environment, key release and logical geometry."""
    expected_by_field = {
        "key_id": trust_key.key_id,
        "environment_id": trust.environment_id,
        "attestor_id": trust.attestor_id,
        "attestor_release_digest": trust_key.attestor_release_digest,
        "capacity_geometry_hash": _hex_digest(expected_capacity_geometry_hash, field="expected_capacity_geometry_hash"),
    }
    for name, value in expected_by_field.items():
        _assert_pinned_text(lease_fields[name], value, field=name)


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
    lease_key_id = _opaque_id(lease_fields["key_id"], field="key_id")
    selected_key = next(
        (trust_key for trust_key in trust.keys if hmac.compare_digest(trust_key.key_id, lease_key_id)),
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
