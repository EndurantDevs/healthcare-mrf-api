# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Parse the closed public capacity trust-set document."""

from __future__ import annotations

import datetime
import json
import os
import re
from typing import Any, Mapping

from process.provider_directory_profile_capacity_attestation_contract import (
    CAPACITY_LEASE_MAX_VALIDITY_SECONDS,
    CAPACITY_LEASE_SIGNATURE_ALGORITHM,
)
from process.provider_directory_profile_capacity_runtime_types import (
    CAPACITY_TRUST_CONTRACT_ID,
    CAPACITY_TRUST_ENV,
    ProviderDirectoryProfileCapacityConfigurationError,
    _TRUST_FIELDS,
    _TRUST_KEY_FIELDS,
    _TRUST_TABLESPACE_FIELDS,
    _TRUST_VOLUME_FIELDS,
)
from process.provider_directory_profile_capacity_trust import (
    CAPACITY_TRUST_MAX_DOCUMENT_BYTES,
    CAPACITY_TRUST_MAX_KEYS,
    CapacityLeaseTrust,
    CapacityLeaseTrustKey,
    CapacityLeaseTrustTablespace,
    CapacityLeaseTrustVolume,
)

_TRUST_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
_TRUST_HEX = re.compile(r"[0-9a-f]{64}\Z")
_TRUST_DATABASE_NAME = re.compile(
    r"[A-Za-z0-9_$][A-Za-z0-9_$.-]*\Z"
)
_TRUST_SYSTEM_IDENTIFIER = re.compile(r"[1-9][0-9]{0,19}\Z")
_TRUST_TIMESTAMP = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T"
    r"[0-9]{2}:[0-9]{2}:[0-9]{2}Z\Z"
)
_UTC = datetime.timezone.utc
_MAX_OID = (1 << 32) - 1
_MAX_SYSTEM_IDENTIFIER = (1 << 64) - 1


def _configuration_error(
    reason: str,
) -> ProviderDirectoryProfileCapacityConfigurationError:
    return ProviderDirectoryProfileCapacityConfigurationError(
        "provider_directory_profile_capacity_configuration_" + reason
    )


def configured_capacity_lease_trust(
    raw_json: str | None = None,
) -> CapacityLeaseTrust:
    """Load pinned public verification policy without permissive defaults."""

    encoded_trust = (
        raw_json if raw_json is not None else os.getenv(CAPACITY_TRUST_ENV)
    )
    if not isinstance(encoded_trust, str) or not encoded_trust.strip():
        raise _configuration_error("trust_missing")
    if (
        len(encoded_trust.encode("utf-8"))
        > CAPACITY_TRUST_MAX_DOCUMENT_BYTES
    ):
        raise _configuration_error("trust_document_too_large")
    try:
        trust_map = json.loads(
            encoded_trust,
            object_pairs_hook=_unique_json_object,
        )
    except (TypeError, ValueError) as exc:
        raise _configuration_error("trust_json_invalid") from exc
    return validated_capacity_lease_trust(trust_map)


def _unique_json_object(
    field_pairs: list[tuple[str, Any]],
) -> dict[str, Any]:
    fields_by_name: dict[str, Any] = {}
    for field_name, field_value in field_pairs:
        if field_name in fields_by_name:
            raise ValueError("duplicate trust field")
        fields_by_name[field_name] = field_value
    return fields_by_name


def _trust_text(value: Any, *, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) > 64
        or _TRUST_ID.fullmatch(value) is None
    ):
        raise _configuration_error("trust_" + field_name + "_invalid")
    return value


def _trust_digest(value: Any, *, field_name: str) -> str:
    if not isinstance(value, str) or _TRUST_HEX.fullmatch(value) is None:
        raise _configuration_error("trust_" + field_name + "_invalid")
    return value


def _trust_database_name(value: Any) -> str:
    if (
        not isinstance(value, str)
        or len(value) > 63
        or _TRUST_DATABASE_NAME.fullmatch(value) is None
    ):
        raise _configuration_error("trust_database_name_invalid")
    return value


def _trust_database_oid(value: Any, *, field_name: str) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 1 <= value <= _MAX_OID
    ):
        raise _configuration_error("trust_" + field_name + "_invalid")
    return value


def _trust_system_identifier(value: Any) -> str:
    if (
        not isinstance(value, str)
        or _TRUST_SYSTEM_IDENTIFIER.fullmatch(value) is None
        or int(value) > _MAX_SYSTEM_IDENTIFIER
    ):
        raise _configuration_error(
            "trust_database_system_identifier_invalid"
        )
    return value


def _trust_public_key(value: Any) -> bytes:
    if not isinstance(value, str) or _TRUST_HEX.fullmatch(value) is None:
        raise _configuration_error("trust_public_key_invalid")
    try:
        return bytes.fromhex(value)
    except ValueError as exc:
        raise _configuration_error("trust_public_key_invalid") from exc


def _trust_timestamp(value: Any, *, field_name: str) -> datetime.datetime:
    if not isinstance(value, str) or _TRUST_TIMESTAMP.fullmatch(value) is None:
        raise _configuration_error("trust_" + field_name + "_invalid")
    try:
        parsed = datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as exc:
        raise _configuration_error(
            "trust_" + field_name + "_invalid"
        ) from exc
    if parsed.strftime("%Y-%m-%dT%H:%M:%SZ") != value:
        raise _configuration_error("trust_" + field_name + "_invalid")
    return parsed.replace(tzinfo=_UTC)


def _trust_rotation(
    trust_map: Mapping[str, Any],
) -> tuple[str, datetime.datetime | None, datetime.datetime | None]:
    status = trust_map["status"]
    if status == "active":
        if (
            trust_map["retired_at"] is not None
            or trust_map["verify_until"] is not None
        ):
            raise _configuration_error("trust_active_retirement_invalid")
        return status, None, None
    if status != "retired":
        raise _configuration_error("trust_status_invalid")
    retired_at = _trust_timestamp(
        trust_map["retired_at"],
        field_name="retired_at",
    )
    verify_until = _trust_timestamp(
        trust_map["verify_until"],
        field_name="verify_until",
    )
    maximum_window = datetime.timedelta(
        seconds=CAPACITY_LEASE_MAX_VALIDITY_SECONDS
    )
    if not retired_at < verify_until <= retired_at + maximum_window:
        raise _configuration_error("trust_retirement_window_invalid")
    return status, retired_at, verify_until


def _trust_key_entry(raw_key: Any) -> CapacityLeaseTrustKey:
    if not isinstance(raw_key, Mapping) or set(raw_key) != _TRUST_KEY_FIELDS:
        raise _configuration_error("trust_key_fields_invalid")
    status, retired_at, verify_until = _trust_rotation(raw_key)
    return CapacityLeaseTrustKey(
        public_key=_trust_public_key(raw_key["public_key_hex"]),
        key_id=_trust_text(raw_key["key_id"], field_name="key_id"),
        attestor_release_digest=_trust_digest(
            raw_key["attestor_release_digest"],
            field_name="attestor_release_digest",
        ),
        status=status,
        retired_at=retired_at,
        verify_until=verify_until,
    )


def _trust_tablespace_entry(
    raw_tablespace: Any,
    *,
    expected_usage: str,
) -> CapacityLeaseTrustTablespace:
    if (
        not isinstance(raw_tablespace, Mapping)
        or set(raw_tablespace) != _TRUST_TABLESPACE_FIELDS
        or raw_tablespace["usage"] != expected_usage
    ):
        raise _configuration_error("trust_tablespace_fields_invalid")
    return CapacityLeaseTrustTablespace(
        tablespace_name=_trust_database_name(
            raw_tablespace["tablespace_name"]
        ),
        tablespace_oid=_trust_database_oid(
            raw_tablespace["tablespace_oid"],
            field_name="tablespace_oid",
        ),
        usage=expected_usage,
        volume_digest=_trust_digest(
            raw_tablespace["volume_digest"],
            field_name="tablespace_volume_digest",
        ),
    )


def _validated_trust_tablespaces(
    raw_tablespaces: Any,
) -> tuple[CapacityLeaseTrustTablespace, ...]:
    if not isinstance(raw_tablespaces, list) or len(raw_tablespaces) != 2:
        raise _configuration_error("trust_tablespaces_invalid")
    tablespaces = tuple(
        _trust_tablespace_entry(raw_tablespace, expected_usage=usage)
        for raw_tablespace, usage in zip(
            raw_tablespaces,
            ("data", "temp"),
            strict=True,
        )
    )
    identity_by_oid: dict[int, tuple[str, str]] = {}
    for tablespace in tablespaces:
        identity = (tablespace.tablespace_name, tablespace.volume_digest)
        prior_identity = identity_by_oid.setdefault(
            tablespace.tablespace_oid,
            identity,
        )
        if prior_identity != identity:
            raise _configuration_error(
                "trust_tablespace_identity_invalid"
            )
    return tablespaces


def _validated_trust_volumes(
    raw_volumes: Any,
) -> tuple[CapacityLeaseTrustVolume, ...]:
    if not isinstance(raw_volumes, list) or len(raw_volumes) != 3:
        raise _configuration_error("trust_volumes_invalid")
    volumes = []
    for raw_volume, volume_class in zip(
        raw_volumes,
        ("data", "temp", "wal"),
        strict=True,
    ):
        if (
            not isinstance(raw_volume, Mapping)
            or set(raw_volume) != _TRUST_VOLUME_FIELDS
            or raw_volume["volume_class"] != volume_class
        ):
            raise _configuration_error("trust_volume_fields_invalid")
        volumes.append(
            CapacityLeaseTrustVolume(
                volume_class=volume_class,
                volume_digest=_trust_digest(
                    raw_volume["volume_digest"],
                    field_name="volume_digest",
                ),
            )
        )
    return tuple(volumes)


def _assert_trust_storage_binding(
    tablespaces: tuple[CapacityLeaseTrustTablespace, ...],
    volumes: tuple[CapacityLeaseTrustVolume, ...],
) -> None:
    volume_by_class = {
        volume.volume_class: volume for volume in volumes
    }
    if any(
        tablespace.volume_digest
        != volume_by_class[tablespace.usage].volume_digest
        for tablespace in tablespaces
    ):
        raise _configuration_error("trust_storage_binding_invalid")


def validated_capacity_lease_trust(
    raw_trust: Any,
) -> CapacityLeaseTrust:
    """Validate the mandatory, closed v2 public trust-set document."""

    if (
        not isinstance(raw_trust, Mapping)
        or set(raw_trust) != _TRUST_FIELDS
        or raw_trust["contract_id"] != CAPACITY_TRUST_CONTRACT_ID
        or raw_trust["signature_algorithm"]
        != CAPACITY_LEASE_SIGNATURE_ALGORITHM
    ):
        raise _configuration_error("trust_fields_invalid")
    raw_keys = raw_trust["keys"]
    if (
        not isinstance(raw_keys, list)
        or not 1 <= len(raw_keys) <= CAPACITY_TRUST_MAX_KEYS
    ):
        raise _configuration_error("trust_keys_invalid")
    trust_keys = tuple(_trust_key_entry(key) for key in raw_keys)
    tablespaces = _validated_trust_tablespaces(raw_trust["tablespaces"])
    volumes = _validated_trust_volumes(raw_trust["volumes"])
    _assert_trust_storage_binding(tablespaces, volumes)
    key_ids = tuple(key.key_id for key in trust_keys)
    active_keys = tuple(key for key in trust_keys if key.status == "active")
    active_key_id = _trust_text(
        raw_trust["active_key_id"],
        field_name="active_key_id",
    )
    if (
        key_ids != tuple(sorted(set(key_ids)))
        or len(active_keys) != 1
        or active_keys[0].key_id != active_key_id
    ):
        raise _configuration_error("trust_key_order_or_active_invalid")
    return CapacityLeaseTrust(
        environment_id=_trust_text(
            raw_trust["environment_id"],
            field_name="environment_id",
        ),
        attestor_id=_trust_text(
            raw_trust["attestor_id"],
            field_name="attestor_id",
        ),
        active_key_id=active_key_id,
        keys=trust_keys,
        database_system_identifier=_trust_system_identifier(
            raw_trust["database_system_identifier"]
        ),
        database_oid=_trust_database_oid(
            raw_trust["database_oid"],
            field_name="database_oid",
        ),
        database_name=_trust_database_name(raw_trust["database_name"]),
        tablespaces=tablespaces,
        volumes=volumes,
    )


__all__ = (
    "configured_capacity_lease_trust",
    "validated_capacity_lease_trust",
)
