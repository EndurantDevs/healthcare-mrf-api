# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed data types and parsing rules for private database-capacity leases."""

from __future__ import annotations

import datetime
import hashlib
import hmac
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from process.provider_directory_profile_capacity_runtime_witness import (
    CAPACITY_RUNTIME_WITNESS_DOMAIN,
    CapacityLeaseRuntimeWitness,
    ProviderDirectoryCapacityLeaseError,
    _parse_capacity_runtime_witness,
    capacity_runtime_witness_sha256,
)
from process.provider_directory_profile_capacity_trust import (
    CapacityLeaseTrust,
    CapacityLeaseTrustKey,
)

LEGACY_CAPACITY_LEASE_V2_CONTRACT_ID = "provider-directory-database-capacity-lease-v2"
LEGACY_CAPACITY_LEASE_V3_CONTRACT_ID = "provider-directory-database-capacity-lease-v3"
CAPACITY_LEASE_CONTRACT_ID = "provider-directory-database-capacity-lease-v4"
CAPACITY_LEASE_SIGNATURE_ALGORITHM = "Ed25519"
CAPACITY_LEASE_SIGNATURE_DOMAIN = "healthporta.provider-directory.database-capacity-lease.v4"
CAPACITY_ATTESTATION_ID_DOMAIN = "healthporta.provider-directory.database-capacity-attestation-id.v4"
CAPACITY_LEASE_DIGEST_DOMAIN = "healthporta.provider-directory.database-capacity-lease-digest.v4"
CAPACITY_LEASE_PUBLIC_KEY_DOMAIN = "healthporta.provider-directory.database-capacity-public-key.v1"
CAPACITY_TABLESPACE_IDENTITY_DOMAIN = "healthporta.provider-directory.database-capacity-tablespaces.v1"
CAPACITY_VOLUME_IDENTITY_DOMAIN = "healthporta.provider-directory.database-capacity-volumes.v1"

CAPACITY_LEASE_MAX_FUTURE_SKEW_SECONDS = 5
CAPACITY_LEASE_MAX_OBSERVATION_AGE_SECONDS = 300
CAPACITY_LEASE_MAX_VALIDITY_SECONDS = 24 * 60 * 60

_LEGACY_V2_SIGNED_BODY_FIELDS = frozenset(
    {
        "attestation_id",
        "attestor_id",
        "attestor_release_digest",
        "capacity_geometry_hash",
        "contract_id",
        "database_name",
        "database_oid",
        "database_system_identifier",
        "environment_id",
        "expires_at",
        "issued_at",
        "key_id",
        "max_build_deadline",
        "nonce",
        "observed_at",
        "reservation_id",
        "runtime_witness",
        "runtime_witness_sha256",
        "deployment_witness",
        "signature_algorithm",
        "tablespaces",
        "volumes",
    }
)
_LEGACY_V3_SIGNED_BODY_FIELDS = _LEGACY_V2_SIGNED_BODY_FIELDS | {
    "signing_preflight_guard",
    "signing_preflight_guard_sha256",
}
_SIGNED_BODY_FIELDS = frozenset(
    {
        *(
            _LEGACY_V2_SIGNED_BODY_FIELDS
            - {
                "database_name",
                "database_oid",
                "database_system_identifier",
                "runtime_witness",
                "runtime_witness_sha256",
                "deployment_witness",
                "tablespaces",
                "volumes",
            }
        ),
        "runtime",
        "storage_classes",
        "signing_preflight_guard",
        "signing_preflight_guard_sha256",
    }
)
_ATTESTATION_ID_FIELDS = _SIGNED_BODY_FIELDS - {"attestation_id"}
_ENVELOPE_FIELDS = frozenset({"lease", "signature"})
_STORAGE_CLASS_FIELDS = frozenset(
    {
        "available_after_all_reservations_bytes",
        "available_bytes",
        "reserved_bytes",
        "storage_class",
    }
)
_STORAGE_CLASSES = ("data", "temp", "wal")

_LOWER_HEX_64 = re.compile(r"[0-9a-f]{64}\Z")
_OPAQUE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
_UTC_TIMESTAMP = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T"
    r"[0-9]{2}:[0-9]{2}:[0-9]{2}Z\Z"
)
_BASE64URL_SIGNATURE = re.compile(r"[A-Za-z0-9_-]{86}\Z")
_RUN_ID = re.compile(r"run_[0-9a-f]{32}\Z")
_BUILD_ID = re.compile(r"pdpb_[0-9a-f]{32}\Z")

_MAX_SIGNED_BIGINT = (1 << 63) - 1
_UTC = datetime.timezone.utc


@dataclass(frozen=True)
class DatabaseCapacityStorageClass:
    """One signed numeric reservation in the shared capacity pool."""

    available_after_all_reservations_bytes: int
    available_bytes: int
    reserved_bytes: int
    storage_class: str


@dataclass(frozen=True)
class VerifiedDatabaseCapacityLease:
    """A current lease with authenticated logical runtime and bounded capacity."""

    attestation_id: str
    attestor_id: str
    attestor_release_digest: str
    capacity_geometry_hash: str
    contract_id: str
    environment_id: str
    expires_at: datetime.datetime
    issued_at: datetime.datetime
    key_id: str
    max_build_deadline: datetime.datetime
    nonce: str
    observed_at: datetime.datetime
    reservation_id: str
    signing_preflight_guard: dict[str, Any]
    signing_preflight_guard_sha256: str
    runtime: CapacityLeaseRuntimeWitness
    signature_algorithm: str
    storage_classes: tuple[DatabaseCapacityStorageClass, ...]
    signature: str
    canonical_lease_json: str
    lease_digest: str
    public_key_fingerprint: str
    validated_at: datetime.datetime

    @property
    def reservation_bytes_by_storage_class(self) -> dict[str, int]:
        """Return every independently authorized class ceiling."""
        return {entry.storage_class: entry.reserved_bytes for entry in self.storage_classes}

    @property
    def outstanding_reserved_bytes_by_storage_class(self) -> dict[str, int]:
        """Return the authenticated outstanding class charges in this observation."""
        return {
            entry.storage_class: entry.available_bytes
            - entry.reserved_bytes
            - entry.available_after_all_reservations_bytes
            for entry in self.storage_classes
        }

    @property
    def pool_remaining_bytes(self) -> int:
        """Charge all classes and outstanding ceilings against the least free observation."""
        return (
            min(entry.available_bytes for entry in self.storage_classes)
            - sum(self.reservation_bytes_by_storage_class.values())
            - sum(self.outstanding_reserved_bytes_by_storage_class.values())
        )


@dataclass(frozen=True)
class AuthenticatedHistoricalCapacityLease:
    """Original signed history for full-ceiling accounting, never admission proof."""

    attestation_id: str
    attestor_id: str
    attestor_release_digest: str
    capacity_geometry_hash: str
    contract_id: str
    environment_id: str
    expires_at: datetime.datetime
    issued_at: datetime.datetime
    key_id: str
    max_build_deadline: datetime.datetime
    nonce: str
    observed_at: datetime.datetime
    reservation_id: str
    reserved_ceilings: tuple[tuple[str, int], ...]
    runtime: CapacityLeaseRuntimeWitness | None
    signing_preflight_guard: dict[str, Any] | None
    signing_preflight_guard_sha256: str | None
    signature_algorithm: str
    signature: str
    canonical_lease_json: str
    lease_digest: str
    public_key_fingerprint: str
    validated_at: datetime.datetime

    @property
    def reservation_bytes_by_storage_class(self) -> dict[str, int]:
        """Count the complete original ceilings without interpreting old remaining bytes."""
        return dict(self.reserved_ceilings)

    @property
    def has_semantic_admission_proof(self) -> bool:
        """Authentication alone supplies no retained semantic replay evidence."""
        return False


@dataclass(frozen=True)
class CapacityLeaseConsumptionBinding:
    """Provider Profile identity bound to one consumed database lease."""

    run_id: str
    build_id: str
    executable_plan_hash: str
    selection_proof_id: str
    source_vector_hash: str
    source_context_vector_hash: str
    profile_as_of: str


def _error(code: str, field: str) -> ProviderDirectoryCapacityLeaseError:
    return ProviderDirectoryCapacityLeaseError(code, field)


def canonical_capacity_lease_json(value: Any) -> str:
    """Serialize the closed lease schema deterministically."""

    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as exc:
        raise _error("invalid_canonical_json", "lease") from exc


def _domain_bytes(domain: str, encoded_content: bytes) -> bytes:
    return domain.encode("ascii") + b"\x00" + encoded_content


def _domain_hash(domain: str, json_content: Any) -> str:
    encoded_content = canonical_capacity_lease_json(json_content).encode("ascii")
    return hashlib.sha256(_domain_bytes(domain, encoded_content)).hexdigest()


def _exact_mapping(
    candidate: Any,
    fields: frozenset[str],
    *,
    field: str,
) -> Mapping[str, Any]:
    if not isinstance(candidate, Mapping) or set(candidate) != fields:
        raise _error("invalid_fields", field)
    return candidate


def _exact_sequence(candidate: Any, *, field: str) -> Sequence[Any]:
    if not isinstance(candidate, Sequence) or isinstance(candidate, (str, bytes, bytearray)):
        raise _error("invalid_type", field)
    return candidate


def _text(
    candidate: Any,
    *,
    field: str,
    maximum_length: int,
    pattern: re.Pattern[str],
) -> str:
    if (
        not isinstance(candidate, str)
        or not candidate
        or candidate != candidate.strip()
        or len(candidate) > maximum_length
        or not pattern.fullmatch(candidate)
    ):
        raise _error("invalid_value", field)
    return candidate


def _opaque_id(
    candidate: Any,
    *,
    field: str,
    maximum_length: int = 64,
) -> str:
    return _text(
        candidate,
        field=field,
        maximum_length=maximum_length,
        pattern=_OPAQUE_ID,
    )


def _hex_digest(candidate: Any, *, field: str) -> str:
    return _text(
        candidate,
        field=field,
        maximum_length=64,
        pattern=_LOWER_HEX_64,
    )


def _integer(
    candidate: Any,
    *,
    field: str,
    minimum: int,
    maximum: int,
) -> int:
    if isinstance(candidate, bool) or not isinstance(candidate, int) or not minimum <= candidate <= maximum:
        raise _error("invalid_value", field)
    return candidate


def _timestamp(candidate: Any, *, field: str) -> datetime.datetime:
    timestamp_text = _text(
        candidate,
        field=field,
        maximum_length=20,
        pattern=_UTC_TIMESTAMP,
    )
    try:
        parsed_timestamp = datetime.datetime.strptime(
            timestamp_text,
            "%Y-%m-%dT%H:%M:%SZ",
        )
    except ValueError as exc:
        raise _error("invalid_value", field) from exc
    if parsed_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ") != timestamp_text:
        raise _error("invalid_value", field)
    return parsed_timestamp.replace(tzinfo=_UTC)


def _validation_time(candidate: datetime.datetime) -> datetime.datetime:
    if not isinstance(candidate, datetime.datetime) or candidate.tzinfo is None:
        raise _error("invalid_value", "validated_at")
    return candidate.astimezone(_UTC)


def _parse_capacity_storage_classes(candidate: Any) -> tuple[DatabaseCapacityStorageClass, ...]:
    entries = _exact_sequence(candidate, field="storage_classes")
    if len(entries) != len(_STORAGE_CLASSES):
        raise _error("invalid_count", "storage_classes")
    storage_classes = []
    for entry, storage_class in zip(entries, _STORAGE_CLASSES, strict=True):
        row = _exact_mapping(entry, _STORAGE_CLASS_FIELDS, field="storage_class")
        if row["storage_class"] != storage_class:
            raise _error("invalid_order", "storage_classes")
        capacity_by_field = {
            name: _integer(
                row[name], field=name, minimum=1 if name == "reserved_bytes" else 0, maximum=_MAX_SIGNED_BIGINT
            )
            for name in _STORAGE_CLASS_FIELDS - {"storage_class"}
        }
        if (
            capacity_by_field["available_bytes"]
            - capacity_by_field["reserved_bytes"]
            - capacity_by_field["available_after_all_reservations_bytes"]
            < 0
        ):
            raise _error("invalid_accounting", "storage_classes")
        storage_classes.append(DatabaseCapacityStorageClass(storage_class=storage_class, **capacity_by_field))
    return tuple(storage_classes)


def _parse_capacity_lease_identity(
    lease_body: Mapping[str, Any],
    *,
    allowed_contracts: tuple[str, ...] = (CAPACITY_LEASE_CONTRACT_ID,),
) -> dict[str, Any]:
    contract_id = lease_body["contract_id"]
    if contract_id not in allowed_contracts:
        raise _error("unsupported_contract", "contract_id")
    if lease_body["signature_algorithm"] != CAPACITY_LEASE_SIGNATURE_ALGORITHM:
        raise _error("unsupported_algorithm", "signature_algorithm")
    return {
        "attestation_id": _hex_digest(lease_body["attestation_id"], field="attestation_id"),
        "attestor_id": _opaque_id(lease_body["attestor_id"], field="attestor_id"),
        "attestor_release_digest": _hex_digest(lease_body["attestor_release_digest"], field="attestor_release_digest"),
        "capacity_geometry_hash": _hex_digest(lease_body["capacity_geometry_hash"], field="capacity_geometry_hash"),
        "contract_id": contract_id,
        "environment_id": _opaque_id(lease_body["environment_id"], field="environment_id"),
        "key_id": _opaque_id(lease_body["key_id"], field="key_id"),
        "nonce": _hex_digest(lease_body["nonce"], field="nonce"),
        "reservation_id": _opaque_id(lease_body["reservation_id"], field="reservation_id", maximum_length=128),
        "signature_algorithm": lease_body["signature_algorithm"],
    }


def _historical_lease_body(raw_body: Any) -> tuple[int, Mapping[str, Any], dict[str, Any]]:
    """Parse original identities and intervals without changing their serialized version."""
    contract_by_id = {
        LEGACY_CAPACITY_LEASE_V2_CONTRACT_ID: (2, _LEGACY_V2_SIGNED_BODY_FIELDS),
        LEGACY_CAPACITY_LEASE_V3_CONTRACT_ID: (3, _LEGACY_V3_SIGNED_BODY_FIELDS),
        CAPACITY_LEASE_CONTRACT_ID: (4, _SIGNED_BODY_FIELDS),
    }
    if (
        not isinstance(raw_body, Mapping)
        or not isinstance(raw_body.get("contract_id"), str)
        or raw_body["contract_id"] not in contract_by_id
    ):
        raise _error("unsupported_contract", "contract_id")
    version, body_fields = contract_by_id[raw_body["contract_id"]]
    body = _exact_mapping(raw_body, body_fields, field="historical_lease")
    identity = _parse_capacity_lease_identity(body, allowed_contracts=tuple(contract_by_id))
    identity.update(
        {
            name: _timestamp(body[name], field=name)
            for name in (
                "issued_at",
                "observed_at",
                "expires_at",
                "max_build_deadline",
            )
        }
    )
    return version, body, identity


def _historical_lease_runtime(body: Mapping[str, Any], version: int) -> CapacityLeaseRuntimeWitness | None:
    """Keep v2 nested runtime opaque; only known v3/v4 schemas provide typed facts."""
    if version == 2:
        return None
    return _parse_capacity_runtime_witness(body["runtime" if version == 4 else "runtime_witness"], historical=True)


def _historical_reservation_ceilings(body: Mapping[str, Any], version: int) -> tuple[tuple[str, int], ...]:
    """Read full signed ceilings; old physical capacity and remaining fields are opaque."""
    entries = _exact_sequence(body["storage_classes" if version == 4 else "volumes"], field="reserved_ceilings")
    if len(entries) != 3 or any(not isinstance(entry, Mapping) for entry in entries):
        raise _error("invalid_count", "reserved_ceilings")
    class_field = "storage_class" if version == 4 else "volume_class"
    if tuple(entry.get(class_field) for entry in entries) != _STORAGE_CLASSES:
        raise _error("invalid_order", "reserved_ceilings")
    return tuple(
        (
            entry[class_field],
            _integer(entry.get("reserved_bytes"), field="reserved_bytes", minimum=1, maximum=_MAX_SIGNED_BIGINT),
        )
        for entry in entries
    )
