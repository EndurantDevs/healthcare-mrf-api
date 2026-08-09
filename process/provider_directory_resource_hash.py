# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Stable content hashing for mapped Provider Directory resources."""

from __future__ import annotations

import datetime
import hashlib
import json
from typing import Any, Mapping


RESOURCE_TRANSPORT_PAYLOAD_FIELDS = frozenset(
    {
        "resource_url",
        "fhir_self_url",
        "fhir_fetch_url",
        "fhir_fetch_mode",
    }
)

RESOURCE_HASH_CONTRACT_METADATA_KEY = "resource_hash_contract"
LEGACY_RESOURCE_HASH_CONTRACT = "transport_bound_v1"
TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT = "transport_neutral_v2"
DEFAULT_RESOURCE_HASH_CONTRACT = TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT
RESOURCE_HASH_CONTRACTS = frozenset(
    {
        LEGACY_RESOURCE_HASH_CONTRACT,
        TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    }
)


def _json_default(value: Any) -> Any:
    """Match the importer encoding for non-JSON scalar values."""

    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return str(value)


def resource_content_hash_payload(
    payload_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    """Remove transport coordinates from the immutable content-hash view."""

    return {
        key: value
        for key, value in payload_by_field.items()
        if key not in RESOURCE_TRANSPORT_PAYLOAD_FIELDS
    }


def _payload_sha256(payload_by_field: Mapping[str, Any]) -> str:
    """Hash one mapped payload with the importer's stable JSON encoding."""

    encoded_payload = json.dumps(
        payload_by_field,
        sort_keys=True,
        default=_json_default,
    ).encode("utf-8")
    return hashlib.sha256(encoded_payload).hexdigest()


def resource_payload_sha256(payload_by_field: Mapping[str, Any]) -> str:
    """Hash semantic content while retaining transport in stored payloads."""

    return _payload_sha256(resource_content_hash_payload(payload_by_field))


def legacy_resource_payload_sha256(payload_by_field: Mapping[str, Any]) -> str:
    """Reproduce the historical transport-inclusive payload hash."""

    return _payload_sha256(payload_by_field)


def resource_payload_sha256_for_contract(
    payload_by_field: Mapping[str, Any],
    resource_hash_contract: str,
) -> str:
    """Hash one payload under its persisted dataset-root contract."""

    if resource_hash_contract == LEGACY_RESOURCE_HASH_CONTRACT:
        return legacy_resource_payload_sha256(payload_by_field)
    if resource_hash_contract == TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT:
        return resource_payload_sha256(payload_by_field)
    raise ValueError("provider_directory_resource_hash_contract_invalid")


def persisted_resource_hash_contract(
    metadata: Mapping[str, Any] | None,
) -> str:
    """Read one stored contract, treating pre-contract datasets as legacy."""

    if metadata is None or RESOURCE_HASH_CONTRACT_METADATA_KEY not in metadata:
        return LEGACY_RESOURCE_HASH_CONTRACT
    resource_hash_contract = metadata.get(RESOURCE_HASH_CONTRACT_METADATA_KEY)
    if resource_hash_contract not in RESOURCE_HASH_CONTRACTS:
        raise ValueError("provider_directory_resource_hash_contract_invalid")
    return resource_hash_contract


def is_resource_payload_hash_match(
    payload_by_field: Mapping[str, Any],
    stored_hash: str,
) -> bool:
    """Accept new semantic hashes and exact historical ordinary-row hashes."""

    return stored_hash in {
        resource_payload_sha256(payload_by_field),
        legacy_resource_payload_sha256(payload_by_field),
    }
