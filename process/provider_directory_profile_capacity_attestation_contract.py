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

from process.provider_directory_profile_capacity_trust import (
    CapacityLeaseTrust,
    CapacityLeaseTrustKey,
    CapacityLeaseTrustTablespace,
    CapacityLeaseTrustVolume,
)
from process.provider_directory_profile_capacity_runtime_witness import (
    CAPACITY_RUNTIME_WITNESS_DOMAIN,
    CapacityLeaseDeploymentWitness,
    CapacityLeaseRuntimeWitness,
    ProviderDirectoryCapacityLeaseError,
    _parse_capacity_deployment_witness,
    _parse_capacity_runtime_witness,
    capacity_runtime_witness_sha256,
)

LEGACY_CAPACITY_LEASE_V2_CONTRACT_ID = "provider-directory-database-capacity-lease-v2"
CAPACITY_LEASE_CONTRACT_ID = "provider-directory-database-capacity-lease-v3"
CAPACITY_LEASE_SIGNATURE_ALGORITHM = "Ed25519"
CAPACITY_LEASE_SIGNATURE_DOMAIN = "healthporta.provider-directory.database-capacity-lease.v3"
CAPACITY_ATTESTATION_ID_DOMAIN = "healthporta.provider-directory.database-capacity-attestation-id.v3"
CAPACITY_LEASE_DIGEST_DOMAIN = "healthporta.provider-directory.database-capacity-lease-digest.v3"
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
_SIGNED_BODY_FIELDS = frozenset(
    {
        *_LEGACY_V2_SIGNED_BODY_FIELDS,
        "signing_preflight_guard",
        "signing_preflight_guard_sha256",
    }
)
_ATTESTATION_ID_FIELDS = _SIGNED_BODY_FIELDS - {"attestation_id"}
_ENVELOPE_FIELDS = frozenset({"lease", "signature"})
_TABLESPACE_FIELDS = frozenset(
    {"tablespace_name", "tablespace_oid", "usage", "volume_digest"}
)
_VOLUME_FIELDS = frozenset(
    {
        "available_after_all_reservations_bytes",
        "available_bytes",
        "reserved_bytes",
        "volume_class",
        "volume_digest",
    }
)
_STORAGE_CLASSES = ("data", "temp", "wal")
_TABLESPACE_USAGES = ("data", "temp")

_LOWER_HEX_64 = re.compile(r"[0-9a-f]{64}\Z")
_OPAQUE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
_DATABASE_NAME = re.compile(r"[A-Za-z0-9_$][A-Za-z0-9_$.-]*\Z")
_SYSTEM_IDENTIFIER = re.compile(r"[1-9][0-9]{0,19}\Z")
_UTC_TIMESTAMP = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T"
    r"[0-9]{2}:[0-9]{2}:[0-9]{2}Z\Z"
)
_BASE64URL_SIGNATURE = re.compile(r"[A-Za-z0-9_-]{86}\Z")
_RUN_ID = re.compile(r"run_[0-9a-f]{32}\Z")
_BUILD_ID = re.compile(r"pdpb_[0-9a-f]{32}\Z")

_MAX_SIGNED_BIGINT = (1 << 63) - 1
_MAX_UNSIGNED_BIGINT = (1 << 64) - 1
_MAX_OID = (1 << 32) - 1
_UTC = datetime.timezone.utc


@dataclass(frozen=True)
class DatabaseCapacityTablespace:
    """One effective database tablespace and its opaque physical volume."""

    tablespace_name: str
    tablespace_oid: int
    usage: str
    volume_digest: str


@dataclass(frozen=True)
class DatabaseCapacityVolume:
    """One storage-class reservation on an opaque physical volume."""

    available_after_all_reservations_bytes: int
    available_bytes: int
    reserved_bytes: int
    volume_class: str
    volume_digest: str


@dataclass(frozen=True)
class VerifiedDatabaseCapacityLease:
    """A closed lease whose identity, signature, and validity were verified."""

    attestation_id: str
    attestor_id: str
    attestor_release_digest: str
    capacity_geometry_hash: str
    contract_id: str
    database_name: str
    database_oid: int
    database_system_identifier: str
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
    runtime_witness: CapacityLeaseRuntimeWitness
    runtime_witness_sha256: str
    deployment_witness: CapacityLeaseDeploymentWitness
    signature_algorithm: str
    tablespaces: tuple[DatabaseCapacityTablespace, ...]
    volumes: tuple[DatabaseCapacityVolume, ...]
    signature: str
    canonical_lease_json: str
    lease_digest: str
    public_key_fingerprint: str
    tablespace_identity_hash: str
    volume_identity_hash: str
    validated_at: datetime.datetime

    @property
    def reservation_bytes_by_storage_class(self) -> dict[str, int]:
        """Return exact independently authorized storage-class reservations."""

        return {
            volume.volume_class: volume.reserved_bytes
            for volume in self.volumes
        }


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


def _assert_tablespace_volume_binding(
    tablespaces: tuple[DatabaseCapacityTablespace, ...],
    volumes: tuple[DatabaseCapacityVolume, ...],
) -> None:
    volume_by_class = {entry.volume_class: entry for entry in volumes}
    tablespace_identity_by_oid: dict[int, tuple[str, str]] = {}
    for tablespace in tablespaces:
        matching_volume = volume_by_class.get(tablespace.usage)
        if matching_volume is None or not hmac.compare_digest(
            tablespace.volume_digest,
            matching_volume.volume_digest,
        ):
            raise _error("binding_mismatch", "tablespace_volume")
        tablespace_identity = (
            tablespace.tablespace_name, tablespace.volume_digest
        )
        prior_identity = tablespace_identity_by_oid.get(
            tablespace.tablespace_oid
        )
        if prior_identity is not None and prior_identity != tablespace_identity:
            raise _error("colocation_mismatch", "tablespaces")
        tablespace_identity_by_oid[tablespace.tablespace_oid] = (
            tablespace_identity
        )


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
    encoded_content = canonical_capacity_lease_json(json_content).encode(
        "ascii"
    )
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
    if (
        not isinstance(candidate, Sequence)
        or isinstance(candidate, (str, bytes, bytearray))
    ):
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
    if (
        isinstance(candidate, bool)
        or not isinstance(candidate, int)
        or not minimum <= candidate <= maximum
    ):
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


def _database_system_identifier(candidate: Any) -> str:
    identifier = _text(
        candidate,
        field="database_system_identifier",
        maximum_length=20,
        pattern=_SYSTEM_IDENTIFIER,
    )
    if int(identifier) > _MAX_UNSIGNED_BIGINT:
        raise _error("invalid_value", "database_system_identifier")
    return identifier


def _database_name_value(candidate: Any, *, field: str) -> str:
    return _text(
        candidate,
        field=field,
        maximum_length=63,
        pattern=_DATABASE_NAME,
    )


def _parse_capacity_tablespace_entry(
    candidate: Any,
) -> DatabaseCapacityTablespace:
    tablespace_map = _exact_mapping(
        candidate,
        _TABLESPACE_FIELDS,
        field="tablespace",
    )
    return DatabaseCapacityTablespace(
        tablespace_name=_database_name_value(
            tablespace_map["tablespace_name"],
            field="tablespace_name",
        ),
        tablespace_oid=_integer(
            tablespace_map["tablespace_oid"],
            field="tablespace_oid",
            minimum=1,
            maximum=_MAX_OID,
        ),
        usage=_opaque_id(
            tablespace_map["usage"],
            field="tablespace_usage",
        ),
        volume_digest=_hex_digest(
            tablespace_map["volume_digest"],
            field="tablespace_volume_digest",
        ),
    )


def _parse_capacity_tablespace_list(
    candidate: Any,
) -> tuple[DatabaseCapacityTablespace, ...]:
    tablespace_entries = _exact_sequence(candidate, field="tablespaces")
    if len(tablespace_entries) != len(_TABLESPACE_USAGES):
        raise _error("invalid_count", "tablespaces")
    parsed_tablespaces = tuple(
        _parse_capacity_tablespace_entry(entry)
        for entry in tablespace_entries
    )
    if (
        tuple(entry.usage for entry in parsed_tablespaces)
        != _TABLESPACE_USAGES
    ):
        raise _error("invalid_order", "tablespaces")
    return parsed_tablespaces


def _parse_capacity_volume_entry(candidate: Any) -> DatabaseCapacityVolume:
    volume_map = _exact_mapping(candidate, _VOLUME_FIELDS, field="volume")
    return DatabaseCapacityVolume(
        available_after_all_reservations_bytes=_integer(
            volume_map["available_after_all_reservations_bytes"],
            field="available_after_all_reservations_bytes",
            minimum=0,
            maximum=_MAX_SIGNED_BIGINT,
        ),
        available_bytes=_integer(
            volume_map["available_bytes"],
            field="available_bytes",
            minimum=0,
            maximum=_MAX_SIGNED_BIGINT,
        ),
        reserved_bytes=_integer(
            volume_map["reserved_bytes"],
            field="reserved_bytes",
            minimum=1,
            maximum=_MAX_SIGNED_BIGINT,
        ),
        volume_class=_opaque_id(
            volume_map["volume_class"],
            field="volume_class",
        ),
        volume_digest=_hex_digest(
            volume_map["volume_digest"],
            field="volume_digest",
        ),
    )


def _parse_capacity_volume_list(
    candidate: Any,
) -> tuple[DatabaseCapacityVolume, ...]:
    volume_entries = _exact_sequence(candidate, field="volumes")
    if len(volume_entries) != len(_STORAGE_CLASSES):
        raise _error("invalid_count", "volumes")
    parsed_volumes = tuple(
        _parse_capacity_volume_entry(entry) for entry in volume_entries
    )
    if (
        tuple(entry.volume_class for entry in parsed_volumes)
        != _STORAGE_CLASSES
    ):
        raise _error("invalid_order", "volumes")
    return parsed_volumes


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
    "CAPACITY_TABLESPACE_IDENTITY_DOMAIN",
    "CAPACITY_VOLUME_IDENTITY_DOMAIN",
    "LEGACY_CAPACITY_LEASE_V2_CONTRACT_ID",
    "CapacityLeaseConsumptionBinding",
    "CapacityLeaseDeploymentWitness",
    "CapacityLeaseRuntimeWitness",
    "CapacityLeaseTrust",
    "CapacityLeaseTrustKey",
    "CapacityLeaseTrustTablespace",
    "CapacityLeaseTrustVolume",
    "DatabaseCapacityTablespace",
    "DatabaseCapacityVolume",
    "ProviderDirectoryCapacityLeaseError",
    "VerifiedDatabaseCapacityLease",
    "canonical_capacity_lease_json",
    "capacity_runtime_witness_sha256",
)
