# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Canonical geocode and relationship evidence for semantic summaries."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from process.provider_directory_projection_types import (
    ProviderDirectoryProjectionError,
    stable_hash,
)


SEMANTIC_TYPED_EVIDENCE_CONTRACT_ID = "healthporta.provider-directory.semantic-typed-evidence.v2"
SEMANTIC_GEOCODE_EVIDENCE_KEY = "geocode_evidence"
SEMANTIC_RELATIONSHIP_EVIDENCE_KEY = "relationship_evidence"
SEMANTIC_EVIDENCE_HASH_FIELD = "summary_semantic_evidence_sha256"
_PROFILE_EVIDENCE_FIELDS = frozenset(
    {"names", "specialties", "contacts", "addresses", "references"}
)
_MAX_INT32 = 2**31 - 1
_LATITUDE_MICRODEGREE_LIMIT = 90_000_000
_LONGITUDE_MICRODEGREE_LIMIT = 180_000_000
_GEOCODE_FIELDS = frozenset({"latitude_microdegrees", "longitude_microdegrees"})
_FHIR_ADDRESS_FIELDS = frozenset(
    {
        "use",
        "type",
        "text",
        "line",
        "city",
        "district",
        "state",
        "postalCode",
        "country",
        "period",
    }
)
_PROFILE_ENTRY_FIELDS = {
    "names": frozenset(
        {"use", "text", "family", "given", "prefix", "suffix", "period"}
    ),
    "specialties": frozenset(
        {"system", "version", "code", "display", "text", "coding"}
    ),
    "contacts": frozenset({"system", "value", "use", "rank", "period"}),
    "addresses": _FHIR_ADDRESS_FIELDS | {SEMANTIC_GEOCODE_EVIDENCE_KEY},
    "references": frozenset(
        {"reference", "type", "identifier", "display", "id"}
    )
    | {SEMANTIC_RELATIONSHIP_EVIDENCE_KEY},
}
_PROFILE_MEANINGFUL_FIELDS = {
    "names": frozenset({"text", "family", "given"}),
    "specialties": frozenset({"code", "display", "text", "coding"}),
    "contacts": frozenset({"value"}),
    "addresses": frozenset(
        {"text", "line", "city", "district", "state", "postalCode", "country"}
    ),
    "references": frozenset(
        {"reference", "identifier", "id", SEMANTIC_RELATIONSHIP_EVIDENCE_KEY}
    ),
}
_PROFILE_TEXT_FIELDS = {
    "names": frozenset({"use", "text", "family"}),
    "specialties": frozenset({"system", "version", "code", "display", "text"}),
    "contacts": frozenset({"system", "value", "use"}),
    "addresses": frozenset(
        {"use", "type", "text", "city", "district", "state", "postalCode", "country"}
    ),
    "references": frozenset({"reference", "type", "display", "id"}),
}
_PROFILE_TEXT_ARRAY_FIELDS = {
    "names": frozenset({"given", "prefix", "suffix"}),
    "addresses": frozenset({"line"}),
}
_RELATIONSHIP_FIELDS = frozenset({"kind", "target_resource_type", "target_resource_id"})
_RELATIONSHIP_KIND_BY_RESOURCE_TYPE = {
    "InsurancePlan": frozenset({"network"}),
    "OrganizationAffiliation": frozenset(
        {
            "organization",
            "participating_organization",
            "network",
            "location",
            "healthcare_service",
            "endpoint",
        }
    ),
}
_RELATIONSHIP_TARGET_TYPES = {
    "organization": frozenset({"Organization"}),
    "participating_organization": frozenset({"Organization"}),
    "network": frozenset({"Organization"}),
    "location": frozenset({"Location"}),
    "healthcare_service": frozenset({"HealthcareService"}),
    "endpoint": frozenset({"Endpoint"}),
}


def _strict_text(raw_text: Any, field_name: str, *, limit: int) -> str:
    """Normalize one bounded non-whitespace semantic identifier."""

    if (
        not isinstance(raw_text, str)
        or not raw_text
        or raw_text != raw_text.strip()
        or len(raw_text) > limit
    ):
        raise ProviderDirectoryProjectionError(
            f"provider_directory_projection_{field_name}_invalid"
        )
    return raw_text


def _bounded_count(raw_count: Any, field_name: str) -> int:
    """Validate one non-negative signed 32-bit semantic count."""

    if type(raw_count) is not int or raw_count < 0 or raw_count > _MAX_INT32:
        raise ProviderDirectoryProjectionError(
            f"provider_directory_projection_{field_name}_invalid"
        )
    return raw_count


def _invalid_profile_entry(evidence_name: str) -> None:
    raise ProviderDirectoryProjectionError(
        f"provider_directory_projection_{evidence_name}_evidence_invalid"
    )


def _is_valid_period(raw_period: Any) -> bool:
    return (
        isinstance(raw_period, dict)
        and bool(raw_period)
        and set(raw_period) <= {"start", "end"}
        and all(
            isinstance(value, str)
            and bool(value)
            and value == value.strip()
            and len(value) <= 64
            for value in raw_period.values()
        )
    )


def _is_valid_coding_list(raw_codings: Any) -> bool:
    allowed_fields = {"system", "version", "code", "display", "userSelected"}
    if not isinstance(raw_codings, list) or not raw_codings:
        return False
    for coding_map in raw_codings:
        if (
            not isinstance(coding_map, dict)
            or not coding_map
            or set(coding_map) - allowed_fields
            or not set(coding_map).intersection({"code", "display"})
        ):
            return False
        for field_name, field_value in coding_map.items():
            if field_name == "userSelected":
                if type(field_value) is not bool:
                    return False
            elif not isinstance(field_value, str) or not field_value.strip():
                return False
    return True


def _is_valid_identifier(raw_identifier: Any) -> bool:
    return (
        isinstance(raw_identifier, dict)
        and set(raw_identifier) <= {"system", "value"}
        and "value" in raw_identifier
        and all(
            isinstance(value, str) and bool(value.strip())
            for value in raw_identifier.values()
        )
    )


def _assert_profile_entry(evidence_name: str, raw_entry: Any) -> None:
    invalid_evidence_name = (
        "geocode"
        if evidence_name == "addresses"
        and isinstance(raw_entry, dict)
        and SEMANTIC_GEOCODE_EVIDENCE_KEY in raw_entry
        else evidence_name
    )
    if (
        not isinstance(raw_entry, dict)
        or not raw_entry
        or set(raw_entry) - _PROFILE_ENTRY_FIELDS[evidence_name]
        or not set(raw_entry).intersection(_PROFILE_MEANINGFUL_FIELDS[evidence_name])
    ):
        _invalid_profile_entry(invalid_evidence_name)
    if evidence_name == "references" and not (
        SEMANTIC_RELATIONSHIP_EVIDENCE_KEY in raw_entry
        or "reference" in raw_entry
        or "identifier" in raw_entry
        or {"type", "id"} <= set(raw_entry)
    ):
        _invalid_profile_entry(evidence_name)
    for field_name in set(raw_entry).intersection(_PROFILE_TEXT_FIELDS[evidence_name]):
        field_value = raw_entry[field_name]
        if not isinstance(field_value, str) or not field_value.strip():
            _invalid_profile_entry(evidence_name)
    for field_name in set(raw_entry).intersection(
        _PROFILE_TEXT_ARRAY_FIELDS.get(evidence_name, ())
    ):
        field_texts = raw_entry[field_name]
        if not isinstance(field_texts, list) or not field_texts or any(
            not isinstance(field_text, str) or not field_text.strip()
            for field_text in field_texts
        ):
            _invalid_profile_entry(evidence_name)
    if "period" in raw_entry and not _is_valid_period(raw_entry["period"]):
        _invalid_profile_entry(evidence_name)
    if "coding" in raw_entry and not _is_valid_coding_list(raw_entry["coding"]):
        _invalid_profile_entry(evidence_name)
    if "identifier" in raw_entry and not _is_valid_identifier(raw_entry["identifier"]):
        _invalid_profile_entry(evidence_name)
    if "rank" in raw_entry and (
        type(raw_entry["rank"]) is not int or raw_entry["rank"] < 1
    ):
        _invalid_profile_entry(evidence_name)


def _assert_all_profile_entries(profile_evidence: Mapping[str, list[Any]]) -> None:
    for evidence_name in _PROFILE_EVIDENCE_FIELDS:
        for raw_entry in profile_evidence[evidence_name]:
            _assert_profile_entry(evidence_name, raw_entry)


def _normalize_geocode_entry(
    resource_type: str,
    address_ordinal: int,
    address_map: Mapping[str, Any],
) -> dict[str, int]:
    """Validate one coordinate envelope embedded in a FHIR address."""

    raw_geocode = address_map[SEMANTIC_GEOCODE_EVIDENCE_KEY]
    address_fields = set(address_map) - {SEMANTIC_GEOCODE_EVIDENCE_KEY}
    if (
        resource_type != "Location"
        or not isinstance(raw_geocode, dict)
        or set(raw_geocode) != _GEOCODE_FIELDS
        or not address_fields.intersection(_FHIR_ADDRESS_FIELDS)
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_geocode_evidence_invalid"
        )
    latitude = raw_geocode.get("latitude_microdegrees")
    longitude = raw_geocode.get("longitude_microdegrees")
    if (
        type(latitude) is not int
        or not -_LATITUDE_MICRODEGREE_LIMIT
        <= latitude
        <= _LATITUDE_MICRODEGREE_LIMIT
        or type(longitude) is not int
        or not -_LONGITUDE_MICRODEGREE_LIMIT
        <= longitude
        <= _LONGITUDE_MICRODEGREE_LIMIT
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_geocode_evidence_invalid"
        )
    return {
        "address_ordinal": address_ordinal,
        "latitude_microdegrees": latitude,
        "longitude_microdegrees": longitude,
    }


def _collect_geocode_evidence(
    resource_type: str,
    addresses: Iterable[Any],
) -> list[dict[str, int]]:
    """Extract every canonical coordinate envelope in address order."""

    normalized_geocodes: list[dict[str, int]] = []
    for address_ordinal, address_map in enumerate(addresses):
        if (
            not isinstance(address_map, dict)
            or SEMANTIC_GEOCODE_EVIDENCE_KEY not in address_map
        ):
            continue
        normalized_geocodes.append(
            _normalize_geocode_entry(resource_type, address_ordinal, address_map)
        )
    return normalized_geocodes


def _normalize_relationship_entry(
    resource_type: str,
    reference_ordinal: int,
    reference_map: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate one canonical FHIR relationship envelope."""

    raw_relationship = reference_map[SEMANTIC_RELATIONSHIP_EVIDENCE_KEY]
    allowed_kinds = _RELATIONSHIP_KIND_BY_RESOURCE_TYPE.get(resource_type)
    if (
        set(reference_map) != {SEMANTIC_RELATIONSHIP_EVIDENCE_KEY}
        or not isinstance(raw_relationship, dict)
        or set(raw_relationship) != _RELATIONSHIP_FIELDS
        or allowed_kinds is None
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_relationship_evidence_invalid"
        )
    kind = _strict_text(
        raw_relationship.get("kind"),
        "relationship_kind",
        limit=64,
    )
    target_resource_type = _strict_text(
        raw_relationship.get("target_resource_type"),
        "relationship_target_resource_type",
        limit=64,
    )
    target_resource_id = _strict_text(
        raw_relationship.get("target_resource_id"),
        "relationship_target_resource_id",
        limit=256,
    )
    if (
        kind not in allowed_kinds
        or target_resource_type not in _RELATIONSHIP_TARGET_TYPES[kind]
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_relationship_evidence_invalid"
        )
    return {
        "reference_ordinal": reference_ordinal,
        "kind": kind,
        "target_resource_type": target_resource_type,
        "target_resource_id": target_resource_id,
    }


def _collect_relationship_evidence(
    resource_type: str,
    references: Iterable[Any],
) -> list[dict[str, Any]]:
    """Extract unique canonical relationship envelopes in reference order."""

    normalized_relationships: list[dict[str, Any]] = []
    seen_relationships: set[tuple[str, str, str]] = set()
    for reference_ordinal, reference_map in enumerate(references):
        if (
            not isinstance(reference_map, dict)
            or SEMANTIC_RELATIONSHIP_EVIDENCE_KEY not in reference_map
        ):
            continue
        normalized_relationship = _normalize_relationship_entry(
            resource_type,
            reference_ordinal,
            reference_map,
        )
        relationship_identity = (
            normalized_relationship["kind"],
            normalized_relationship["target_resource_type"],
            normalized_relationship["target_resource_id"],
        )
        if relationship_identity in seen_relationships:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_relationship_evidence_invalid"
            )
        normalized_relationships.append(normalized_relationship)
        seen_relationships.add(relationship_identity)
    return normalized_relationships


def normalized_semantic_evidence(
    resource_type: str,
    profile_evidence: Mapping[str, list[Any]],
) -> dict[str, Any]:
    """Return strict source-neutral evidence for derived semantic outcomes."""

    normalized_type = _strict_text(resource_type, "resource_type", limit=64)
    if (
        not isinstance(profile_evidence, Mapping)
        or set(profile_evidence) != _PROFILE_EVIDENCE_FIELDS
        or any(
            not isinstance(profile_evidence[evidence_name], list)
            for evidence_name in _PROFILE_EVIDENCE_FIELDS
        )
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_semantic_evidence_fields_invalid"
        )
    _assert_all_profile_entries(profile_evidence)
    return {
        "contract_id": SEMANTIC_TYPED_EVIDENCE_CONTRACT_ID,
        "geocodes": _collect_geocode_evidence(
            normalized_type,
            profile_evidence["addresses"],
        ),
        "relationships": _collect_relationship_evidence(
            normalized_type,
            profile_evidence["references"],
        ),
    }


def _supplied_semantic_summary(
    resource_map: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate caller-carried summary fields before comparison."""

    supplied_is_addressed = resource_map.get("summary_addressed_location")
    supplied_is_geocoded = resource_map.get("summary_geocoded_location")
    if (
        type(supplied_is_addressed) is not bool
        or type(supplied_is_geocoded) is not bool
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_summary_location_flag_invalid"
        )
    return {
        "summary_address_count": _bounded_count(
            resource_map.get("summary_address_count"),
            "summary_address_count",
        ),
        "summary_addressed_location": supplied_is_addressed,
        "summary_geocoded_location": supplied_is_geocoded,
        "summary_network_link_count": _bounded_count(
            resource_map.get("summary_network_link_count"),
            "summary_network_link_count",
        ),
        "summary_affiliation_link_count": _bounded_count(
            resource_map.get("summary_affiliation_link_count"),
            "summary_affiliation_link_count",
        ),
    }


def _derived_semantic_summary(
    resource_type: str,
    profile_evidence: Mapping[str, list[Any]],
    typed_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    """Build the summary fields solely from normalized typed evidence."""

    address_count = len(profile_evidence["addresses"])
    relationship_count = len(typed_evidence["relationships"])
    return {
        "summary_address_count": address_count,
        "summary_addressed_location": (
            resource_type == "Location" and address_count > 0
        ),
        "summary_geocoded_location": bool(typed_evidence["geocodes"]),
        "summary_network_link_count": (
            relationship_count if resource_type == "InsurancePlan" else 0
        ),
        "summary_affiliation_link_count": (
            relationship_count
            if resource_type == "OrganizationAffiliation"
            else 0
        ),
    }


def evidence_derived_semantic_summary(
    resource_type: str,
    resource_map: Mapping[str, Any],
    profile_evidence: Mapping[str, list[Any]],
) -> dict[str, Any]:
    """Derive summary facts and reject independent caller assertions."""

    typed_evidence = normalized_semantic_evidence(resource_type, profile_evidence)
    supplied_summary = _supplied_semantic_summary(resource_map)
    derived_summary = _derived_semantic_summary(
        resource_type,
        profile_evidence,
        typed_evidence,
    )
    if supplied_summary != derived_summary:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_semantic_contribution_inconsistent"
        )
    return {
        **derived_summary,
        SEMANTIC_EVIDENCE_HASH_FIELD: stable_hash(
            typed_evidence,
            domain="provider-directory-projection-semantic-typed-evidence-v2",
        ),
    }


__all__ = [
    "SEMANTIC_EVIDENCE_HASH_FIELD",
    "SEMANTIC_GEOCODE_EVIDENCE_KEY",
    "SEMANTIC_RELATIONSHIP_EVIDENCE_KEY",
    "SEMANTIC_TYPED_EVIDENCE_CONTRACT_ID",
    "normalized_semantic_evidence",
]
