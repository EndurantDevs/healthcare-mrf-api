# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Private signed capacity leases for Provider Directory database builds."""

from __future__ import annotations

import base64
import datetime
import hashlib
import hmac
from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Any

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from process.provider_directory_profile_capacity_attestation_contract import (
    CAPACITY_ATTESTATION_ID_DOMAIN,
    CAPACITY_LEASE_CONTRACT_ID,
    CAPACITY_LEASE_DIGEST_DOMAIN,
    CAPACITY_LEASE_MAX_FUTURE_SKEW_SECONDS,
    CAPACITY_LEASE_MAX_OBSERVATION_AGE_SECONDS,
    CAPACITY_LEASE_MAX_VALIDITY_SECONDS,
    CAPACITY_LEASE_PUBLIC_KEY_DOMAIN,
    CAPACITY_LEASE_SIGNATURE_ALGORITHM,
    CAPACITY_LEASE_SIGNATURE_DOMAIN,
    CAPACITY_RUNTIME_WITNESS_DOMAIN,
    CAPACITY_TABLESPACE_IDENTITY_DOMAIN,
    CAPACITY_VOLUME_IDENTITY_DOMAIN,
    LEGACY_CAPACITY_LEASE_V2_CONTRACT_ID,
    CapacityLeaseConsumptionBinding,
    CapacityLeaseDeploymentWitness,
    CapacityLeaseRuntimeWitness,
    CapacityLeaseTrust,
    CapacityLeaseTrustKey,
    CapacityLeaseTrustTablespace,
    CapacityLeaseTrustVolume,
    DatabaseCapacityTablespace,
    DatabaseCapacityVolume,
    ProviderDirectoryCapacityLeaseError,
    VerifiedDatabaseCapacityLease,
    _ATTESTATION_ID_FIELDS,
    _BASE64URL_SIGNATURE,
    _ENVELOPE_FIELDS,
    _MAX_OID,
    _MAX_SIGNED_BIGINT,
    _LEGACY_V2_SIGNED_BODY_FIELDS,
    _SIGNED_BODY_FIELDS,
    _STORAGE_CLASSES,
    _database_name_value,
    _database_system_identifier,
    _domain_bytes,
    _domain_hash,
    _error,
    _assert_tablespace_volume_binding,
    _exact_mapping,
    _hex_digest,
    _integer,
    _opaque_id,
    _parse_capacity_tablespace_list,
    _parse_capacity_volume_list,
    _parse_capacity_deployment_witness,
    _parse_capacity_runtime_witness,
    _text,
    _timestamp,
    _validation_time,
    canonical_capacity_lease_json,
    capacity_runtime_witness_sha256,
)
from process.provider_directory_profile_capacity_signing_guard import (
    ProfileCapacitySigningGuardError,
    validated_capacity_signing_guard_fields,
)
from process.provider_directory_profile_capacity_consumption import (
    capacity_lease_consumption_values,
)
from process.provider_directory_profile_capacity_trust_validation import (
    assert_capacity_trust_binding,
    capacity_trust_key_for_assigned_lease,
)


def _assert_colocated_volume_accounting(
    volumes: tuple[DatabaseCapacityVolume, ...],
) -> None:
    volume_groups: dict[str, list[DatabaseCapacityVolume]] = defaultdict(list)
    for volume in volumes:
        if volume.available_after_all_reservations_bytes > volume.available_bytes:
            raise _error("invalid_accounting", "available_bytes")
        volume_groups[volume.volume_digest].append(volume)
    for colocated_volumes in volume_groups.values():
        available_bytes = {entry.available_bytes for entry in colocated_volumes}
        remaining_bytes = {
            entry.available_after_all_reservations_bytes
            for entry in colocated_volumes
        }
        if len(available_bytes) != 1 or len(remaining_bytes) != 1:
            raise _error("colocation_mismatch", "volumes")
        physical_delta = (
            colocated_volumes[0].available_bytes
            - colocated_volumes[0].available_after_all_reservations_bytes
        )
        class_reservations = sum(
            entry.reserved_bytes for entry in colocated_volumes
        )
        if class_reservations > physical_delta:
            raise _error("reservation_unaccounted", "volumes")


def _parse_capacity_lease_identity(
    lease_body: Mapping[str, Any],
) -> dict[str, Any]:
    contract_id = lease_body["contract_id"]
    if contract_id != CAPACITY_LEASE_CONTRACT_ID:
        raise _error("unsupported_contract", "contract_id")
    signature_algorithm = lease_body["signature_algorithm"]
    if signature_algorithm != CAPACITY_LEASE_SIGNATURE_ALGORITHM:
        raise _error("unsupported_algorithm", "signature_algorithm")
    return {
        "attestation_id": _hex_digest(
            lease_body["attestation_id"], field="attestation_id"
        ),
        "attestor_id": _opaque_id(
            lease_body["attestor_id"], field="attestor_id"
        ),
        "attestor_release_digest": _hex_digest(
            lease_body["attestor_release_digest"],
            field="attestor_release_digest",
        ),
        "capacity_geometry_hash": _hex_digest(
            lease_body["capacity_geometry_hash"],
            field="capacity_geometry_hash",
        ),
        "contract_id": contract_id,
        "database_name": _database_name_value(
            lease_body["database_name"], field="database_name"
        ),
        "database_oid": _integer(
            lease_body["database_oid"],
            field="database_oid",
            minimum=1,
            maximum=_MAX_OID,
        ),
        "database_system_identifier": _database_system_identifier(
            lease_body["database_system_identifier"]
        ),
        "environment_id": _opaque_id(
            lease_body["environment_id"], field="environment_id"
        ),
        "key_id": _opaque_id(lease_body["key_id"], field="key_id"),
        "nonce": _hex_digest(lease_body["nonce"], field="nonce"),
        "reservation_id": _opaque_id(
            lease_body["reservation_id"],
            field="reservation_id",
            maximum_length=128,
        ),
        "signature_algorithm": signature_algorithm,
    }


def _parse_capacity_lease_fields(
    lease_body: Mapping[str, Any],
) -> dict[str, Any]:
    """Parse every exact signed field into a typed internal representation."""

    parsed_by_field = {
        **_parse_capacity_lease_identity(lease_body),
        **_parse_capacity_runtime_evidence(lease_body),
        "expires_at": _timestamp(
            lease_body["expires_at"], field="expires_at"
        ),
        "issued_at": _timestamp(
            lease_body["issued_at"], field="issued_at"
        ),
        "max_build_deadline": _timestamp(
            lease_body["max_build_deadline"],
            field="max_build_deadline",
        ),
        "observed_at": _timestamp(
            lease_body["observed_at"], field="observed_at"
        ),
        "tablespaces": _parse_capacity_tablespace_list(
            lease_body["tablespaces"]
        ),
        "volumes": _parse_capacity_volume_list(lease_body["volumes"]),
    }
    return parsed_by_field


def _parse_capacity_signing_guard_fields(
    lease_body: Mapping[str, Any],
    parsed_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate the signed replay chain after basic time bounds pass."""

    try:
        return validated_capacity_signing_guard_fields(
            lease_body,
            parsed_by_field,
        )
    except ProfileCapacitySigningGuardError as exc:
        raise _error("binding_mismatch", "signing_preflight_guard") from exc


def _parse_capacity_runtime_evidence(
    lease_body: Mapping[str, Any],
) -> dict[str, Any]:
    runtime_witness = _parse_capacity_runtime_witness(
        lease_body["runtime_witness"]
    )
    deployment_witness = _parse_capacity_deployment_witness(
        lease_body["deployment_witness"]
    )
    supplied_digest = _hex_digest(
        lease_body["runtime_witness_sha256"],
        field="runtime_witness_sha256",
    )
    expected_digest = capacity_runtime_witness_sha256(
        lease_body["runtime_witness"],
        lease_body["deployment_witness"],
    )
    if not hmac.compare_digest(supplied_digest, expected_digest):
        raise _error("identity_mismatch", "runtime_witness_sha256")
    return {
        "runtime_witness": runtime_witness,
        "runtime_witness_sha256": supplied_digest,
        "deployment_witness": deployment_witness,
    }


def _assert_temporal_validity(
    lease_fields: Mapping[str, Any],
    *,
    now: datetime.datetime,
) -> None:
    observed_at = lease_fields["observed_at"]
    issued_at = lease_fields["issued_at"]
    expires_at = lease_fields["expires_at"]
    build_deadline = lease_fields["max_build_deadline"]
    future_skew = datetime.timedelta(
        seconds=CAPACITY_LEASE_MAX_FUTURE_SKEW_SECONDS
    )
    observation_age = datetime.timedelta(
        seconds=CAPACITY_LEASE_MAX_OBSERVATION_AGE_SECONDS
    )
    maximum_validity = datetime.timedelta(
        seconds=CAPACITY_LEASE_MAX_VALIDITY_SECONDS
    )
    if observed_at > issued_at or issued_at - observed_at > observation_age:
        raise _error("invalid_interval", "observed_at")
    if issued_at > now + future_skew:
        raise _error("issued_in_future", "issued_at")
    if expires_at <= issued_at or expires_at - issued_at > maximum_validity:
        raise _error("invalid_interval", "expires_at")
    if now >= expires_at:
        raise _error("expired", "expires_at")
    if not issued_at < build_deadline <= expires_at:
        raise _error("invalid_interval", "max_build_deadline")
    if now >= build_deadline:
        raise _error("deadline_reached", "max_build_deadline")
    if now - observed_at > observation_age + future_skew:
        raise _error("stale", "observed_at")


def _decode_signature(candidate: Any) -> tuple[str, bytes]:
    signature_text = _text(
        candidate,
        field="signature",
        maximum_length=86,
        pattern=_BASE64URL_SIGNATURE,
    )
    try:
        signature_bytes = base64.b64decode(
            signature_text + "==",
            altchars=b"-_",
            validate=True,
        )
    except (ValueError, TypeError) as exc:
        raise _error("invalid_value", "signature") from exc
    canonical_signature = (
        base64.urlsafe_b64encode(signature_bytes)
        .rstrip(b"=")
        .decode("ascii")
    )
    if len(signature_bytes) != 64 or canonical_signature != signature_text:
        raise _error("invalid_value", "signature")
    return signature_text, signature_bytes


def capacity_attestation_id(
    lease_body_without_id: Mapping[str, Any],
) -> str:
    """Return the deterministic identifier for one exact unsigned lease body."""

    identity_map = _exact_mapping(
        lease_body_without_id,
        _ATTESTATION_ID_FIELDS,
        field="attestation_identity",
    )
    return _domain_hash(CAPACITY_ATTESTATION_ID_DOMAIN, identity_map)


def _assert_attestation_id(lease_body: Mapping[str, Any]) -> None:
    attestation_identity_map = {
        field_name: field_content
        for field_name, field_content in lease_body.items()
        if field_name != "attestation_id"
    }
    expected_attestation_id = capacity_attestation_id(
        attestation_identity_map
    )
    if not hmac.compare_digest(
        lease_body["attestation_id"], expected_attestation_id
    ):
        raise _error("identity_mismatch", "attestation_id")


def _verify_signature(
    lease_body: Mapping[str, Any],
    signature_bytes: bytes,
    public_key: Ed25519PublicKey,
) -> str:
    canonical_body = canonical_capacity_lease_json(lease_body)
    signature_message = _domain_bytes(
        CAPACITY_LEASE_SIGNATURE_DOMAIN,
        canonical_body.encode("ascii"),
    )
    try:
        public_key.verify(signature_bytes, signature_message)
    except InvalidSignature as exc:
        raise _error("invalid_signature", "signature") from exc
    return canonical_body


def _identity_hash(domain: str, identity_entries: Sequence[Any]) -> str:
    return _domain_hash(domain, list(identity_entries))


def _build_verified_capacity_lease(
    *,
    lease_fields: Mapping[str, Any],
    lease_body: Mapping[str, Any],
    envelope: Mapping[str, Any],
    signature_text: str,
    canonical_body: str,
    trust_key: CapacityLeaseTrustKey,
    validated_at: datetime.datetime,
) -> VerifiedDatabaseCapacityLease:
    return VerifiedDatabaseCapacityLease(
        **lease_fields,
        signature=signature_text,
        canonical_lease_json=canonical_body,
        lease_digest=_domain_hash(CAPACITY_LEASE_DIGEST_DOMAIN, envelope),
        public_key_fingerprint=hashlib.sha256(
            _domain_bytes(
                CAPACITY_LEASE_PUBLIC_KEY_DOMAIN,
                trust_key.public_key,
            )
        ).hexdigest(),
        tablespace_identity_hash=_identity_hash(
            CAPACITY_TABLESPACE_IDENTITY_DOMAIN, lease_body["tablespaces"]
        ),
        volume_identity_hash=_identity_hash(
            CAPACITY_VOLUME_IDENTITY_DOMAIN, lease_body["volumes"]
        ),
        validated_at=validated_at,
    )


def verify_database_capacity_lease(
    envelope: Mapping[str, Any],
    *,
    trust: CapacityLeaseTrust,
    now: datetime.datetime,
    expected_capacity_geometry_hash: str,
    expected_database_system_identifier: str,
    expected_database_oid: int,
    expected_database_name: str,
) -> VerifiedDatabaseCapacityLease:
    """Verify one private, exclusive, proof-bound PostgreSQL capacity lease."""

    closed_envelope = _exact_mapping(envelope, _ENVELOPE_FIELDS, field="envelope")
    raw_lease_body = closed_envelope["lease"]
    is_legacy_v2 = (
        isinstance(raw_lease_body, Mapping)
        and raw_lease_body.get("contract_id") == LEGACY_CAPACITY_LEASE_V2_CONTRACT_ID
    )
    if is_legacy_v2:
        _exact_mapping(
            raw_lease_body,
            _LEGACY_V2_SIGNED_BODY_FIELDS,
            field="legacy_v2_lease",
        )
        raise _error("unsupported_contract", "contract_id")
    lease_body = _exact_mapping(raw_lease_body, _SIGNED_BODY_FIELDS, field="lease")
    signature_text, signature_bytes = _decode_signature(closed_envelope["signature"])
    lease_fields = _parse_capacity_lease_fields(lease_body)
    validated_at = _validation_time(now)
    trust_key, public_key = capacity_trust_key_for_assigned_lease(
        trust,
        lease_fields,
        now=validated_at,
    )
    _assert_attestation_id(lease_body)
    _assert_tablespace_volume_binding(lease_fields["tablespaces"], lease_fields["volumes"])
    _assert_colocated_volume_accounting(lease_fields["volumes"])
    _assert_temporal_validity(lease_fields, now=validated_at)
    assert_capacity_trust_binding(
        lease_fields,
        trust=trust,
        trust_key=trust_key,
        expected_capacity_geometry_hash=expected_capacity_geometry_hash,
        expected_database_system_identifier=expected_database_system_identifier,
        expected_database_oid=expected_database_oid,
        expected_database_name=expected_database_name,
    )
    canonical_body = _verify_signature(lease_body, signature_bytes, public_key)
    lease_fields = _parse_capacity_signing_guard_fields(lease_body, lease_fields)
    return _build_verified_capacity_lease(
        lease_fields=lease_fields,
        lease_body=lease_body,
        envelope=closed_envelope,
        signature_text=signature_text,
        canonical_body=canonical_body,
        trust_key=trust_key,
        validated_at=validated_at,
    )


def assert_database_capacity_lease_reservation(
    lease: VerifiedDatabaseCapacityLease,
    *,
    required_bytes_by_storage_class: Mapping[str, int],
    minimum_remaining_bytes: int,
    required_build_seconds: int,
) -> None:
    """Require signed per-class reservations and physical remaining capacity."""

    if not isinstance(lease, VerifiedDatabaseCapacityLease):
        raise _error("invalid_type", "lease")
    if set(required_bytes_by_storage_class) != set(_STORAGE_CLASSES):
        raise _error("invalid_fields", "required_bytes_by_storage_class")
    signed_reservation_by_class = (
        lease.reservation_bytes_by_storage_class
    )
    for storage_class in _STORAGE_CLASSES:
        required_bytes = _integer(
            required_bytes_by_storage_class[storage_class],
            field=f"required_{storage_class}_bytes",
            minimum=1,
            maximum=_MAX_SIGNED_BIGINT,
        )
        if signed_reservation_by_class[storage_class] < required_bytes:
            raise _error("reservation_too_small", storage_class)
    minimum_remaining = _integer(
        minimum_remaining_bytes,
        field="minimum_remaining_bytes",
        minimum=1,
        maximum=_MAX_SIGNED_BIGINT,
    )
    remaining_by_volume = {
        volume.volume_digest: volume.available_after_all_reservations_bytes
        for volume in lease.volumes
    }
    if any(
        remaining_bytes < minimum_remaining
        for remaining_bytes in remaining_by_volume.values()
    ):
        raise _error("remaining_capacity_too_small", "volumes")
    build_seconds = _integer(
        required_build_seconds,
        field="required_build_seconds",
        minimum=1,
        maximum=CAPACITY_LEASE_MAX_VALIDITY_SECONDS,
    )
    projected_completion = max(lease.validated_at, lease.issued_at) + (
        datetime.timedelta(seconds=build_seconds)
    )
    if projected_completion > lease.max_build_deadline:
        raise _error("deadline_too_short", "max_build_deadline")
__all__ = (
    "CAPACITY_ATTESTATION_ID_DOMAIN", "CAPACITY_LEASE_CONTRACT_ID",
    "CAPACITY_LEASE_DIGEST_DOMAIN", "CAPACITY_LEASE_MAX_FUTURE_SKEW_SECONDS",
    "CAPACITY_LEASE_MAX_OBSERVATION_AGE_SECONDS",
    "CAPACITY_LEASE_MAX_VALIDITY_SECONDS", "CAPACITY_LEASE_PUBLIC_KEY_DOMAIN",
    "CAPACITY_LEASE_SIGNATURE_ALGORITHM", "CAPACITY_LEASE_SIGNATURE_DOMAIN",
    "CAPACITY_RUNTIME_WITNESS_DOMAIN",
    "CAPACITY_TABLESPACE_IDENTITY_DOMAIN", "CAPACITY_VOLUME_IDENTITY_DOMAIN",
    "CapacityLeaseConsumptionBinding", "CapacityLeaseTrust",
    "CapacityLeaseDeploymentWitness", "CapacityLeaseRuntimeWitness",
    "CapacityLeaseTrustKey",
    "CapacityLeaseTrustTablespace", "CapacityLeaseTrustVolume",
    "DatabaseCapacityTablespace", "DatabaseCapacityVolume",
    "ProviderDirectoryCapacityLeaseError", "VerifiedDatabaseCapacityLease",
    "assert_database_capacity_lease_reservation",
    "canonical_capacity_lease_json", "capacity_attestation_id",
    "capacity_runtime_witness_sha256",
    "capacity_lease_consumption_values", "verify_database_capacity_lease",
)
