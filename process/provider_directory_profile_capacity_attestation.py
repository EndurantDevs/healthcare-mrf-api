# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Private signed capacity leases for Provider Directory database builds."""

from __future__ import annotations

import base64
import datetime
import hashlib
import hmac
from collections.abc import Mapping, Sequence
from typing import Any

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from process.provider_directory_profile_capacity_attestation_contract import (
    _ATTESTATION_ID_FIELDS,
    _BASE64URL_SIGNATURE,
    _ENVELOPE_FIELDS,
    _LEGACY_V2_SIGNED_BODY_FIELDS,
    _LEGACY_V3_SIGNED_BODY_FIELDS,
    _MAX_SIGNED_BIGINT,
    _SIGNED_BODY_FIELDS,
    _STORAGE_CLASSES,
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
    LEGACY_CAPACITY_LEASE_V2_CONTRACT_ID,
    LEGACY_CAPACITY_LEASE_V3_CONTRACT_ID,
    AuthenticatedHistoricalCapacityLease,
    CapacityLeaseConsumptionBinding,
    CapacityLeaseRuntimeWitness,
    CapacityLeaseTrust,
    CapacityLeaseTrustKey,
    DatabaseCapacityStorageClass,
    ProviderDirectoryCapacityLeaseError,
    VerifiedDatabaseCapacityLease,
    _domain_bytes,
    _domain_hash,
    _error,
    _exact_mapping,
    _exact_sequence,
    _hex_digest,
    _historical_lease_body,
    _historical_lease_runtime,
    _historical_reservation_ceilings,
    _integer,
    _opaque_id,
    _parse_capacity_lease_identity,
    _parse_capacity_runtime_witness,
    _parse_capacity_storage_classes,
    _text,
    _timestamp,
    _validation_time,
    canonical_capacity_lease_json,
)
from process.provider_directory_profile_capacity_consumption import (
    capacity_lease_consumption_values,
)
from process.provider_directory_profile_capacity_signing_guard import (
    ProfileCapacitySigningGuardError,
    validated_capacity_signing_guard_fields,
)
from process.provider_directory_profile_capacity_trust_validation import (
    assert_capacity_trust_binding,
    capacity_trust_key_for_assigned_lease,
)


def _parse_capacity_lease_fields(
    lease_body: Mapping[str, Any],
) -> dict[str, Any]:
    """Parse every exact signed field into a typed internal representation."""

    parsed_by_field = {
        **_parse_capacity_lease_identity(lease_body),
        **_parse_capacity_runtime_evidence(lease_body),
        "expires_at": _timestamp(lease_body["expires_at"], field="expires_at"),
        "issued_at": _timestamp(lease_body["issued_at"], field="issued_at"),
        "max_build_deadline": _timestamp(
            lease_body["max_build_deadline"],
            field="max_build_deadline",
        ),
        "observed_at": _timestamp(lease_body["observed_at"], field="observed_at"),
        "storage_classes": _parse_capacity_storage_classes(lease_body["storage_classes"]),
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


def _parse_capacity_runtime_evidence(lease_body: Mapping[str, Any]) -> dict[str, Any]:
    """The complete signed lease directly authenticates logical runtime fields."""
    return {"runtime": _parse_capacity_runtime_witness(lease_body["runtime"])}


def _assert_temporal_validity(
    lease_fields: Mapping[str, Any],
    *,
    now: datetime.datetime,
    require_fresh_observation: bool = True,
    accounting_only: bool = False,
) -> None:
    if type(require_fresh_observation) is not bool:
        raise _error("invalid_type", "require_fresh_observation")
    observed_at = lease_fields["observed_at"]
    issued_at = lease_fields["issued_at"]
    expires_at = lease_fields["expires_at"]
    build_deadline = lease_fields["max_build_deadline"]
    future_skew = datetime.timedelta(seconds=CAPACITY_LEASE_MAX_FUTURE_SKEW_SECONDS)
    observation_age = datetime.timedelta(seconds=CAPACITY_LEASE_MAX_OBSERVATION_AGE_SECONDS)
    maximum_validity = datetime.timedelta(seconds=CAPACITY_LEASE_MAX_VALIDITY_SECONDS)
    if observed_at > issued_at or issued_at - observed_at > observation_age:
        raise _error("invalid_interval", "observed_at")
    if issued_at > now + future_skew:
        raise _error("issued_in_future", "issued_at")
    if expires_at <= issued_at or expires_at - issued_at > maximum_validity:
        raise _error("invalid_interval", "expires_at")
    if not accounting_only and now >= expires_at:
        raise _error("expired", "expires_at")
    if not issued_at < build_deadline <= expires_at:
        raise _error("invalid_interval", "max_build_deadline")
    if not accounting_only and now >= build_deadline:
        raise _error("deadline_reached", "max_build_deadline")
    if require_fresh_observation and now - observed_at > observation_age + future_skew:
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
    canonical_signature = base64.urlsafe_b64encode(signature_bytes).rstrip(b"=").decode("ascii")
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
        field_name: field_content for field_name, field_content in lease_body.items() if field_name != "attestation_id"
    }
    expected_attestation_id = capacity_attestation_id(attestation_identity_map)
    if not hmac.compare_digest(lease_body["attestation_id"], expected_attestation_id):
        raise _error("identity_mismatch", "attestation_id")


def _verify_signature(
    lease_body: Mapping[str, Any],
    signature_bytes: bytes,
    public_key: Ed25519PublicKey,
    domain: str = CAPACITY_LEASE_SIGNATURE_DOMAIN,
) -> str:
    canonical_body = canonical_capacity_lease_json(lease_body)
    signature_message = _domain_bytes(
        domain,
        canonical_body.encode("ascii"),
    )
    try:
        public_key.verify(signature_bytes, signature_message)
    except InvalidSignature as exc:
        raise _error("invalid_signature", "signature") from exc
    return canonical_body


def _build_verified_capacity_lease(
    *,
    lease_fields: Mapping[str, Any],
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
            _domain_bytes(CAPACITY_LEASE_PUBLIC_KEY_DOMAIN, trust_key.public_key)
        ).hexdigest(),
        validated_at=validated_at,
    )


def verify_database_capacity_lease(
    envelope: Mapping[str, Any],
    *,
    trust: CapacityLeaseTrust,
    now: datetime.datetime,
    expected_capacity_geometry_hash: str,
    require_fresh_observation: bool = True,
) -> VerifiedDatabaseCapacityLease:
    """Verify current logical admission; historical envelopes are accounting only."""
    closed = _exact_mapping(envelope, _ENVELOPE_FIELDS, field="envelope")
    raw = closed["lease"]
    if (
        isinstance(raw, Mapping)
        and isinstance(raw.get("contract_id"), str)
        and raw.get("contract_id")
        in {
            LEGACY_CAPACITY_LEASE_V2_CONTRACT_ID,
            LEGACY_CAPACITY_LEASE_V3_CONTRACT_ID,
        }
    ):
        fields = (
            _LEGACY_V2_SIGNED_BODY_FIELDS
            if raw["contract_id"] == LEGACY_CAPACITY_LEASE_V2_CONTRACT_ID
            else _LEGACY_V3_SIGNED_BODY_FIELDS
        )
        _exact_mapping(raw, fields, field="historical_lease")
        raise _error("unsupported_contract", "contract_id")
    body = _exact_mapping(raw, _SIGNED_BODY_FIELDS, field="lease")
    signature_text, signature_bytes = _decode_signature(closed["signature"])
    fields = _parse_capacity_lease_fields(body)
    validated_at = _validation_time(now)
    trust_key, public_key = capacity_trust_key_for_assigned_lease(trust, fields, now=validated_at)
    _assert_attestation_id(body)
    _assert_temporal_validity(fields, now=validated_at, require_fresh_observation=require_fresh_observation)
    assert_capacity_trust_binding(
        fields, trust=trust, trust_key=trust_key, expected_capacity_geometry_hash=expected_capacity_geometry_hash
    )
    canonical = _verify_signature(body, signature_bytes, public_key)
    fields = _parse_capacity_signing_guard_fields(body, fields)
    _assert_paired_profile_reservations(fields, trust, validated_at)
    return _build_verified_capacity_lease(
        lease_fields=fields,
        envelope=closed,
        signature_text=signature_text,
        canonical_body=canonical,
        trust_key=trust_key,
        validated_at=validated_at,
    )


def _assert_paired_profile_reservations(fields, trust, validated_at):
    from process.provider_directory_cms_capacity_contract import verified_cms_paired_profile_lease
    from process.provider_directory_profile_capacity_preflight_contract import validated_capacity_preflight_request

    request = validated_capacity_preflight_request(fields["signing_preflight_guard"]["healthcare_request"])
    if request.cms_nonprofile_admission is None:
        return
    paired = verified_cms_paired_profile_lease(request, trust=trust, now=validated_at)
    for item in fields["storage_classes"]:
        outstanding = item.available_bytes - item.reserved_bytes - item.available_after_all_reservations_bytes
        if outstanding < paired.reservation_bytes_by_storage_class[item.storage_class]:
            raise _error("paired_profile_reservation_missing", item.storage_class)


def assert_database_capacity_lease_reservation(
    lease: VerifiedDatabaseCapacityLease,
    *,
    required_bytes_by_storage_class: Mapping[str, int],
    minimum_remaining_bytes: int,
    required_build_seconds: int,
) -> None:
    """Require every class ceiling, shared-pool headroom and the original deadline."""

    if not isinstance(lease, VerifiedDatabaseCapacityLease):
        raise _error("invalid_type", "lease")
    if set(required_bytes_by_storage_class) != set(_STORAGE_CLASSES):
        raise _error("invalid_fields", "required_bytes_by_storage_class")
    signed_reservation_by_class = lease.reservation_bytes_by_storage_class
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
    if lease.pool_remaining_bytes < minimum_remaining:
        raise _error("remaining_capacity_too_small", "storage_classes")
    build_seconds = _integer(
        required_build_seconds,
        field="required_build_seconds",
        minimum=1,
        maximum=CAPACITY_LEASE_MAX_VALIDITY_SECONDS,
    )
    projected_completion = max(lease.validated_at, lease.issued_at) + (datetime.timedelta(seconds=build_seconds))
    if projected_completion > lease.max_build_deadline:
        raise _error("deadline_too_short", "max_build_deadline")


def authenticate_historical_capacity_lease(
    envelope: Mapping[str, Any],
    *,
    trust: CapacityLeaseTrust,
    now: datetime.datetime,
) -> AuthenticatedHistoricalCapacityLease:
    """Authenticate original bytes and ceilings without granting admission or replay."""
    closed = _exact_mapping(envelope, _ENVELOPE_FIELDS, field="envelope")
    version, body, identity = _historical_lease_body(closed["lease"])
    validated_at = _validation_time(now)
    trust_key, key = capacity_trust_key_for_assigned_lease(trust, identity, now=validated_at)
    assert_capacity_trust_binding(
        identity, trust=trust, trust_key=trust_key, expected_capacity_geometry_hash=identity["capacity_geometry_hash"]
    )
    _assert_temporal_validity(identity, now=validated_at, require_fresh_observation=False, accounting_only=True)
    signature_text, signature_bytes = _decode_signature(closed["signature"])
    canonical = _authenticate_historical_body(body, version, signature_bytes, key)
    guard = body.get("signing_preflight_guard")
    if version > 2 and not isinstance(guard, Mapping):
        raise _error("invalid_fields", "signing_preflight_guard")
    return AuthenticatedHistoricalCapacityLease(
        **identity,
        reserved_ceilings=_historical_reservation_ceilings(body, version),
        runtime=_historical_lease_runtime(body, version),
        signing_preflight_guard=None if guard is None else dict(guard),
        signing_preflight_guard_sha256=body.get("signing_preflight_guard_sha256"),
        signature=signature_text,
        canonical_lease_json=canonical,
        lease_digest=_domain_hash(f"healthporta.provider-directory.database-capacity-lease-digest.v{version}", closed),
        public_key_fingerprint=hashlib.sha256(
            _domain_bytes(CAPACITY_LEASE_PUBLIC_KEY_DOMAIN, trust_key.public_key)
        ).hexdigest(),
        validated_at=validated_at,
    )


def _authenticate_historical_body(
    body: Mapping[str, Any], version: int, signature: bytes, key: Ed25519PublicKey
) -> str:
    """Use each original signature and nested-runtime domain before semantic projection."""
    expected_id = _domain_hash(
        f"healthporta.provider-directory.database-capacity-attestation-id.v{version}",
        {name: signed_value for name, signed_value in body.items() if name != "attestation_id"},
    )
    if not hmac.compare_digest(body["attestation_id"], expected_id):
        raise _error("identity_mismatch", "attestation_id")
    canonical = _verify_signature(
        body, signature, key, f"healthporta.provider-directory.database-capacity-lease.v{version}"
    )
    if version < 4:
        runtime_digest = _domain_hash(
            CAPACITY_RUNTIME_WITNESS_DOMAIN,
            {
                "runtime_witness": body["runtime_witness"],
                "deployment_witness": body["deployment_witness"],
            },
        )
        if runtime_digest != body["runtime_witness_sha256"]:
            raise _error("identity_mismatch", "runtime_witness_sha256")
    return canonical


__all__ = (
    "CAPACITY_ATTESTATION_ID_DOMAIN",
    "CAPACITY_LEASE_CONTRACT_ID",
    "CAPACITY_LEASE_DIGEST_DOMAIN",
    "CAPACITY_LEASE_MAX_FUTURE_SKEW_SECONDS",
    "CAPACITY_LEASE_MAX_OBSERVATION_AGE_SECONDS",
    "CAPACITY_LEASE_MAX_VALIDITY_SECONDS",
    "CAPACITY_LEASE_PUBLIC_KEY_DOMAIN",
    "CAPACITY_LEASE_SIGNATURE_ALGORITHM",
    "CAPACITY_LEASE_SIGNATURE_DOMAIN",
    "CAPACITY_RUNTIME_WITNESS_DOMAIN",
    "LEGACY_CAPACITY_LEASE_V2_CONTRACT_ID",
    "LEGACY_CAPACITY_LEASE_V3_CONTRACT_ID",
    "AuthenticatedHistoricalCapacityLease",
    "CapacityLeaseConsumptionBinding",
    "CapacityLeaseRuntimeWitness",
    "CapacityLeaseTrust",
    "CapacityLeaseTrustKey",
    "DatabaseCapacityStorageClass",
    "ProviderDirectoryCapacityLeaseError",
    "VerifiedDatabaseCapacityLease",
    "assert_database_capacity_lease_reservation",
    "authenticate_historical_capacity_lease",
    "canonical_capacity_lease_json",
    "capacity_attestation_id",
    "capacity_lease_consumption_values",
    "verify_database_capacity_lease",
)
