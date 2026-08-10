# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Lossless semantic hashing for repeated Provider Directory Organizations."""

from __future__ import annotations

import copy
from typing import Any, Mapping

from process.provider_directory_resource_hash import (
    RESOURCE_TRANSPORT_PAYLOAD_FIELDS,
    SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
    _payload_sha256,
    _stable_json,
    semantic_resource_content_hash_payload,
)


ORGANIZATION_NAME_PAYLOAD_FIELDS = frozenset(
    {"name", "aliases", "name_variants"}
)


def _canonical_text_values(value: Any) -> list[str]:
    """Return one exact, sorted, nonempty text set."""

    if value is None:
        return []
    if type(value) is not list or any(
        type(item) is not str or not item.strip() for item in value
    ):
        raise ValueError("provider_directory_organization_names_invalid")
    return sorted(set(value))


def _organization_name_sort_key(value: str) -> tuple[str, int, str]:
    """Prefer case-insensitive lexical order, then display-like casing."""

    return value.casefold(), sum(character.isupper() for character in value), value


def _organization_name_state(
    payload_by_field: Mapping[str, Any],
) -> tuple[list[str], list[str]]:
    """Return observed primary variants and source-provided aliases."""

    name = payload_by_field.get("name")
    if name is not None and (type(name) is not str or not name.strip()):
        raise ValueError("provider_directory_organization_names_invalid")
    raw_variants = payload_by_field.get("name_variants")
    name_variants = sorted(
        _canonical_text_values(raw_variants),
        key=_organization_name_sort_key,
    )
    if raw_variants is None and name is not None:
        name_variants = [name]
    elif name is not None and name not in name_variants:
        name_variants = sorted(
            {*name_variants, name},
            key=_organization_name_sort_key,
        )
    aliases = _canonical_text_values(payload_by_field.get("aliases"))
    source_aliases = sorted(set(aliases) - set(name_variants))
    return name_variants, source_aliases


def _organization_name_projection(
    name_variants: list[str],
    source_aliases: list[str],
) -> dict[str, Any]:
    """Project a deterministic primary and every alternate exact label."""

    primary_name = name_variants[0] if name_variants else None
    aliases = sorted(
        (set(name_variants) | set(source_aliases))
        - ({primary_name} if primary_name is not None else set())
    )
    return {
        "name": primary_name,
        "aliases": aliases,
        "name_variants": name_variants,
    }


def canonical_organization_payload(
    payload_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    """Canonicalize one Organization's symmetric name and alias set."""

    canonical = copy.deepcopy(dict(payload_by_field))
    name_variants, source_aliases = _organization_name_state(canonical)
    canonical.update(
        _organization_name_projection(name_variants, source_aliases)
    )
    return canonical


def organization_semantic_base_payload(
    payload_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    """Return stable Organization content except symmetric names."""

    semantic_payload = semantic_resource_content_hash_payload(
        canonical_organization_payload(payload_by_field)
    )
    return {
        field_name: field_value
        for field_name, field_value in semantic_payload.items()
        if field_name not in ORGANIZATION_NAME_PAYLOAD_FIELDS
    }


def organization_semantic_base_sha256(
    payload_by_field: Mapping[str, Any],
) -> str:
    """Hash stable non-name Organization content."""

    return _payload_sha256(
        organization_semantic_base_payload(payload_by_field)
    )


def organization_label_hashes(
    payload_by_field: Mapping[str, Any],
) -> tuple[str, ...]:
    """Return ordered domain-separated primary and alias digests."""

    canonical = canonical_organization_payload(payload_by_field)
    name_variants, source_aliases = _organization_name_state(canonical)
    all_labels = sorted(set(name_variants) | set(source_aliases))
    components = [
        *(
            {"kind": "name", "value": name}
            for name in name_variants
        ),
        *(
            {"kind": "label", "value": label}
            for label in all_labels
        ),
    ]
    return tuple(sorted(_payload_sha256(component) for component in components))


def organization_primary_name_hashes(
    payload_by_field: Mapping[str, Any],
) -> tuple[str, ...]:
    """Return ordered primary-name digests for union diagnostics."""

    canonical = canonical_organization_payload(payload_by_field)
    name_variants, _source_aliases = _organization_name_state(canonical)
    return tuple(
        sorted(
            _payload_sha256({"kind": "name", "value": name})
            for name in name_variants
        )
    )


def composed_organization_semantic_sha256(
    base_hash: str,
    name_hashes: tuple[str, ...] | list[str],
) -> str:
    """Compose a lossless cross-page Organization content commitment."""

    hashes = [base_hash, *name_hashes]
    if any(
        type(hash_value) is not str
        or len(hash_value) != 64
        or any(character not in "0123456789abcdef" for character in hash_value)
        for hash_value in hashes
    ):
        raise ValueError("provider_directory_organization_hash_invalid")
    return _payload_sha256(
        {
            "base_hash": base_hash,
            "name_hashes": sorted(set(name_hashes)),
        }
    )


def organization_semantic_payload_sha256(
    payload_by_field: Mapping[str, Any],
) -> str:
    """Hash one canonical mapped Organization under the v4 contract."""

    canonical = canonical_organization_payload(payload_by_field)
    if any(
        _stable_json(payload_by_field.get(field_name))
        != _stable_json(canonical.get(field_name))
        for field_name in ORGANIZATION_NAME_PAYLOAD_FIELDS
    ):
        raise ValueError(
            "provider_directory_organization_name_projection_invalid"
        )
    return composed_organization_semantic_sha256(
        organization_semantic_base_sha256(canonical),
        organization_label_hashes(canonical),
    )


def _observation_provenance(
    payload_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    """Return complete volatile provenance from one Organization observation."""

    provenance_by_field = {
        field_name: copy.deepcopy(payload_by_field[field_name])
        for field_name in RESOURCE_TRANSPORT_PAYLOAD_FIELDS
        if field_name in payload_by_field
    }
    fhir_metadata_by_field = payload_by_field.get("fhir_meta")
    if (
        isinstance(fhir_metadata_by_field, Mapping)
        and "lastUpdated" in fhir_metadata_by_field
    ):
        provenance_by_field["fhir_meta.lastUpdated"] = copy.deepcopy(
            fhir_metadata_by_field["lastUpdated"]
        )
    return provenance_by_field


def _preferred_observation_payload(
    first_payload: Mapping[str, Any],
    second_payload: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Choose one whole volatile provenance observation deterministically."""

    return max(
        (first_payload, second_payload),
        key=lambda payload_by_field: _stable_json(
            _observation_provenance(payload_by_field)
        ),
    )


def _merged_observation_provenance(
    merged_payload: dict[str, Any],
    stable_payload: Mapping[str, Any],
    preferred_payload: Mapping[str, Any],
) -> None:
    """Attach one observed transport/time tuple without synthesizing fields."""

    for field_name in RESOURCE_TRANSPORT_PAYLOAD_FIELDS:
        if field_name in preferred_payload:
            merged_payload[field_name] = copy.deepcopy(
                preferred_payload[field_name]
            )
        else:
            merged_payload.pop(field_name, None)
    stable_metadata = stable_payload.get("fhir_meta")
    merged_metadata = (
        {
            field_name: copy.deepcopy(field_value)
            for field_name, field_value in stable_metadata.items()
            if field_name != "lastUpdated"
        }
        if isinstance(stable_metadata, Mapping)
        else {}
    )
    preferred_metadata = preferred_payload.get("fhir_meta")
    if (
        isinstance(preferred_metadata, Mapping)
        and "lastUpdated" in preferred_metadata
    ):
        merged_metadata["lastUpdated"] = copy.deepcopy(
            preferred_metadata["lastUpdated"]
        )
    merged_payload["fhir_meta"] = merged_metadata or None


def merge_organization_semantic_payloads(
    first_payload: Mapping[str, Any],
    second_payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Union Organization names only when every non-name field agrees."""

    first = canonical_organization_payload(first_payload)
    second = canonical_organization_payload(second_payload)
    if organization_semantic_base_sha256(first) != (
        organization_semantic_base_sha256(second)
    ):
        raise ValueError(
            "provider_directory_organization_identity_payload_conflict"
        )
    merged = copy.deepcopy(first)
    first_variants, first_aliases = _organization_name_state(first)
    second_variants, second_aliases = _organization_name_state(second)
    merged.update(
        _organization_name_projection(
            sorted(
                set(first_variants + second_variants),
                key=_organization_name_sort_key,
            ),
            sorted(set(first_aliases + second_aliases)),
        )
    )
    _merged_observation_provenance(
        merged,
        first,
        _preferred_observation_payload(first, second),
    )
    return merged
