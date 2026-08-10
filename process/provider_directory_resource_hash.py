# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Stable content hashing for mapped Provider Directory resources."""

from __future__ import annotations

import copy
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
SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT = "semantic_content_v3"
DEFAULT_RESOURCE_HASH_CONTRACT = SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
RESOURCE_HASH_CONTRACTS = frozenset(
    {
        LEGACY_RESOURCE_HASH_CONTRACT,
        TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
        SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    }
)

PRACTITIONER_NAME_PAYLOAD_FIELDS = frozenset(
    {
        "names",
        "family_name",
        "given_names",
        "full_name",
    }
)


def _json_default(value: Any) -> Any:
    """Match the importer encoding for non-JSON scalar values."""

    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return str(value)


def _stable_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    )


def _is_mapped_practitioner_payload(
    payload_by_field: Mapping[str, Any],
) -> bool:
    return PRACTITIONER_NAME_PAYLOAD_FIELDS.issubset(payload_by_field)


def _human_name_sort_key(name_by_field: Mapping[str, Any]) -> str:
    return _stable_json(name_by_field)


def canonical_practitioner_names(value: Any) -> list[dict[str, Any]]:
    """Return an exact, permutation-stable mapped HumanName set."""

    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("provider_directory_practitioner_names_invalid")
    names_by_identity: dict[str, dict[str, Any]] = {}
    for raw_name in value:
        if not isinstance(raw_name, Mapping):
            raise ValueError("provider_directory_practitioner_names_invalid")
        name_by_field = copy.deepcopy(dict(raw_name))
        identity = _stable_json(name_by_field)
        names_by_identity[identity] = name_by_field
    return sorted(names_by_identity.values(), key=_human_name_sort_key)


def _practitioner_primary_name_projection(
    names: list[dict[str, Any]],
) -> dict[str, Any]:
    if not names:
        return {
            "family_name": None,
            "given_names": [],
            "full_name": None,
        }
    primary = names[0]
    family_name = primary.get("family")
    given_names = primary.get("given")
    if not isinstance(given_names, list):
        given_names = []
    full_name = primary.get("text")
    if not full_name:
        parts = [*given_names, *([family_name] if family_name else [])]
        full_name = " ".join(str(part) for part in parts) or None
    return {
        "family_name": family_name,
        "given_names": copy.deepcopy(given_names),
        "full_name": full_name,
    }


def canonical_practitioner_payload(
    payload_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    """Canonicalize repeating names and their searchable projection."""

    canonical = copy.deepcopy(dict(payload_by_field))
    if "names" not in canonical:
        return canonical
    names = canonical_practitioner_names(canonical.get("names"))
    canonical["names"] = names
    canonical.update(_practitioner_primary_name_projection(names))
    return canonical


def _without_volatile_fhir_time(
    payload_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    content_by_field = dict(payload_by_field)
    fhir_meta = content_by_field.get("fhir_meta")
    if isinstance(fhir_meta, Mapping):
        stable_fhir_metadata_by_field = {
            key: value
            for key, value in fhir_meta.items()
            if key != "lastUpdated"
        }
        content_by_field["fhir_meta"] = (
            stable_fhir_metadata_by_field or None
        )
    return content_by_field


def resource_content_hash_payload(
    payload_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    """Reproduce the v2 hash view without top-level transport coordinates."""

    return {
        key: value
        for key, value in payload_by_field.items()
        if key not in RESOURCE_TRANSPORT_PAYLOAD_FIELDS
    }


def semantic_resource_content_hash_payload(
    payload_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    """Remove transport coordinates and volatile FHIR observation time."""

    content_by_field = _without_volatile_fhir_time(
        resource_content_hash_payload(payload_by_field)
    )
    if _is_mapped_practitioner_payload(content_by_field):
        return canonical_practitioner_payload(content_by_field)
    return content_by_field


def _payload_sha256(payload_by_field: Mapping[str, Any]) -> str:
    """Hash one mapped payload with the importer's stable JSON encoding."""

    encoded_payload = json.dumps(
        payload_by_field,
        sort_keys=True,
        default=_json_default,
    ).encode("utf-8")
    return hashlib.sha256(encoded_payload).hexdigest()


def practitioner_semantic_base_payload(
    payload_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    """Return all stable Practitioner content except repeating names."""

    semantic_payload = semantic_resource_content_hash_payload(payload_by_field)
    if not _is_mapped_practitioner_payload(semantic_payload):
        raise ValueError("provider_directory_practitioner_payload_invalid")
    return {
        key: value
        for key, value in semantic_payload.items()
        if key not in PRACTITIONER_NAME_PAYLOAD_FIELDS
    }


def practitioner_semantic_base_sha256(
    payload_by_field: Mapping[str, Any],
) -> str:
    """Hash stable non-name Practitioner content."""

    return _payload_sha256(practitioner_semantic_base_payload(payload_by_field))


def practitioner_name_hashes(
    payload_by_field: Mapping[str, Any],
) -> tuple[str, ...]:
    """Return the ordered exact mapped HumanName digest set."""

    canonical = canonical_practitioner_payload(payload_by_field)
    return tuple(
        sorted({_payload_sha256(name_by_field) for name_by_field in canonical["names"]})
    )


def composed_practitioner_semantic_sha256(
    base_hash: str,
    name_hashes: tuple[str, ...] | list[str],
) -> str:
    """Compose a lossless cross-page Practitioner content commitment."""

    if (
        len(base_hash) != 64
        or any(character not in "0123456789abcdef" for character in base_hash)
        or any(
            len(name_hash) != 64
            or any(character not in "0123456789abcdef" for character in name_hash)
            for name_hash in name_hashes
        )
    ):
        raise ValueError("provider_directory_practitioner_hash_invalid")
    canonical_name_hashes = sorted(set(name_hashes))
    return _payload_sha256(
        {
            "base_hash": base_hash,
            "name_hashes": canonical_name_hashes,
        }
    )


def practitioner_semantic_payload_sha256(
    payload_by_field: Mapping[str, Any],
) -> str:
    """Hash one canonical mapped Practitioner under the v3 contract."""

    canonical = canonical_practitioner_payload(payload_by_field)
    if any(
        _stable_json(payload_by_field.get(field_name))
        != _stable_json(canonical.get(field_name))
        for field_name in PRACTITIONER_NAME_PAYLOAD_FIELDS
    ):
        raise ValueError(
            "provider_directory_practitioner_name_projection_invalid"
        )
    return composed_practitioner_semantic_sha256(
        practitioner_semantic_base_sha256(canonical),
        practitioner_name_hashes(canonical),
    )


def _observation_provenance(payload_by_field: Mapping[str, Any]) -> dict[str, Any]:
    """Return the complete volatile provenance from one observed payload."""

    provenance_by_field = {
        field_name: copy.deepcopy(payload_by_field[field_name])
        for field_name in RESOURCE_TRANSPORT_PAYLOAD_FIELDS
        if field_name in payload_by_field
    }
    fhir_meta = payload_by_field.get("fhir_meta")
    if isinstance(fhir_meta, Mapping) and "lastUpdated" in fhir_meta:
        provenance_by_field["fhir_meta.lastUpdated"] = copy.deepcopy(
            fhir_meta["lastUpdated"]
        )
    return provenance_by_field


def _preferred_observation_payload(
    first_payload: Mapping[str, Any],
    second_payload: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Choose one whole provenance observation without synthesizing fields."""

    return max(
        (first_payload, second_payload),
        key=lambda payload_by_field: _stable_json(
            _observation_provenance(payload_by_field)
        ),
    )


def merge_practitioner_semantic_payloads(
    first_payload: Mapping[str, Any],
    second_payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Losslessly union names only when every non-name field agrees."""

    first = canonical_practitioner_payload(first_payload)
    second = canonical_practitioner_payload(second_payload)
    if practitioner_semantic_base_sha256(first) != (
        practitioner_semantic_base_sha256(second)
    ):
        raise ValueError(
            "provider_directory_practitioner_identity_payload_conflict"
        )
    merged = copy.deepcopy(first)
    merged_names = canonical_practitioner_names(
        [*first["names"], *second["names"]]
    )
    merged["names"] = merged_names
    merged.update(_practitioner_primary_name_projection(merged_names))
    preferred_observation = _preferred_observation_payload(first, second)
    for field_name in RESOURCE_TRANSPORT_PAYLOAD_FIELDS:
        if field_name in preferred_observation:
            merged[field_name] = copy.deepcopy(
                preferred_observation[field_name]
            )
        else:
            merged.pop(field_name, None)
    first_meta = first.get("fhir_meta")
    stable_meta = _without_volatile_fhir_time({"fhir_meta": first_meta}).get(
        "fhir_meta"
    )
    preferred_meta = preferred_observation.get("fhir_meta")
    has_last_updated = (
        isinstance(preferred_meta, Mapping)
        and "lastUpdated" in preferred_meta
    )
    if isinstance(stable_meta, Mapping):
        merged_meta = copy.deepcopy(dict(stable_meta))
        if has_last_updated:
            merged_meta["lastUpdated"] = copy.deepcopy(
                preferred_meta["lastUpdated"]
            )
        merged["fhir_meta"] = merged_meta
    elif has_last_updated:
        merged["fhir_meta"] = {
            "lastUpdated": copy.deepcopy(preferred_meta["lastUpdated"])
        }
    else:
        merged["fhir_meta"] = None
    return merged


def resource_payload_sha256(payload_by_field: Mapping[str, Any]) -> str:
    """Reproduce the v2 top-level transport-neutral payload hash."""

    return _payload_sha256(resource_content_hash_payload(payload_by_field))


def semantic_resource_payload_sha256(
    payload_by_field: Mapping[str, Any],
) -> str:
    """Hash semantic content while retaining all provenance in stored payloads."""

    if _is_mapped_practitioner_payload(payload_by_field):
        return practitioner_semantic_payload_sha256(payload_by_field)
    return _payload_sha256(
        semantic_resource_content_hash_payload(payload_by_field)
    )


def merge_semantic_resource_payloads(
    first_payload: Mapping[str, Any],
    second_payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Choose deterministic provenance only for equal v3 content."""

    first_hash = semantic_resource_payload_sha256(first_payload)
    second_hash = semantic_resource_payload_sha256(second_payload)
    if first_hash != second_hash:
        raise ValueError("provider_directory_resource_payload_conflict")
    return copy.deepcopy(
        max((dict(first_payload), dict(second_payload)), key=_stable_json)
    )


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
    if resource_hash_contract == SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT:
        return semantic_resource_payload_sha256(payload_by_field)
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
    """Accept v3 semantic hashes and exact historical v1/v2 row hashes."""

    accepted_hashes = {
        resource_payload_sha256(payload_by_field),
        legacy_resource_payload_sha256(payload_by_field),
    }
    try:
        semantic_hash = semantic_resource_payload_sha256(payload_by_field)
    except ValueError:
        semantic_hash = None
    if semantic_hash is not None:
        accepted_hashes.add(semantic_hash)
    return stored_hash in accepted_hashes
