# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared display and classification policy for PTG provider addresses."""

from __future__ import annotations

import re
from typing import Any, Mapping


PTG_ADDRESS_KIND_PHYSICAL = "physical"
PTG_ADDRESS_KIND_POSTAL_BOX = "postal_box"
PTG_ADDRESS_KIND_UNKNOWN = "unknown"

_POSTAL_BOX_NORMALIZED_PATTERN = (
    r"^(P O BOX|PO BOX|POST OFFICE BOX|P O B|POB)( |$)"
)
_POSTAL_BOX_NORMALIZED = re.compile(_POSTAL_BOX_NORMALIZED_PATTERN)
_SQL_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


def _address_lines(address_payload: Mapping[str, Any]) -> tuple[str, ...]:
    """Return every explicit display line without joining distinct fields."""

    lines: list[str] = []
    for field_name in (
        "first_line",
        "address_line_1",
        "street",
        "street_address",
        "second_line",
        "address_line_2",
    ):
        line = str(address_payload.get(field_name) or "").strip()
        if line and line not in lines:
            lines.append(line)
    return tuple(lines)


def _normalized_address_line(address_line: str) -> str:
    return re.sub(r"[^A-Z0-9]+", " ", address_line.upper()).strip()


def _validated_sql_alias(alias: str) -> str:
    normalized_alias = str(alias or "")
    if not _SQL_IDENTIFIER.fullmatch(normalized_alias):
        raise ValueError("address alias must be a simple PostgreSQL identifier")
    return normalized_alias


def _normalized_address_line_sql(alias: str, column: str) -> str:
    return (
        "UPPER(BTRIM(REGEXP_REPLACE(COALESCE("
        f"{alias}.{column}, ''), '[^A-Za-z0-9]+', ' ', 'g')))"
    )


def classify_ptg_address_kind(address_payload: Mapping[str, Any] | Any) -> str:
    """Classify explicit postal boxes without guessing from city/ZIP data."""

    if not isinstance(address_payload, Mapping):
        return PTG_ADDRESS_KIND_UNKNOWN
    address_lines = _address_lines(address_payload)
    if not address_lines:
        return PTG_ADDRESS_KIND_UNKNOWN
    if any(
        _POSTAL_BOX_NORMALIZED.match(_normalized_address_line(address_line))
        for address_line in address_lines
    ):
        return PTG_ADDRESS_KIND_POSTAL_BOX
    return PTG_ADDRESS_KIND_PHYSICAL


def postal_box_address_sql(alias: str) -> str:
    """Return a safe SQL predicate matching the public classifier."""

    normalized_alias = _validated_sql_alias(alias)
    line_predicates = [
        f"({_normalized_address_line_sql(normalized_alias, column)} "
        f"~ '{_POSTAL_BOX_NORMALIZED_PATTERN}')"
        for column in ("first_line", "second_line")
    ]
    return f"({' OR '.join(line_predicates)})"


def address_display_rank_sql(alias: str) -> str:
    """Rank physical, postal, and blank addresses for display selection."""

    normalized_alias = _validated_sql_alias(alias)
    has_display_line_sql = (
        "COALESCE(NULLIF(BTRIM("
        f"{normalized_alias}.first_line), ''), NULLIF(BTRIM("
        f"{normalized_alias}.second_line), '')) IS NOT NULL"
    )
    return (
        f"CASE WHEN NOT ({has_display_line_sql}) THEN 2 "
        f"WHEN {postal_box_address_sql(normalized_alias)} THEN 1 ELSE 0 END"
    )


PTG_CONTACT_DETAIL_FIELDS = (
    "fax_number_digits",
    "phone_extension",
    "fax_extension",
)

PTG_POSTAL_BOX_GEO_FIELDS = {
    "anchor_zip5",
    "coordinates",
    "distance",
    "distance_bucket",
    "distance_miles",
    "google_map_url",
    "google_maps_url",
    "geo_evidence_level",
    "lat",
    "latitude",
    "long",
    "longitude",
    "location_key",
    "location_hash",
    "maps_url",
    "address_site_key",
    "premise_key",
    "zip_match_type",
    "zip_radius_miles",
}

PTG_POSTAL_BOX_LOCATION_LABEL_FIELDS = {
    "address_precision",
    "location_confidence_code",
    "location_confidence_id",
    "location_source",
}

PTG2_LEGACY_ADDRESS_COLUMNS = {
    "npi",
    "type",
    "checksum",
    "address_key",
    "state_name",
    "city_name",
    "postal_code",
    "country_code",
    "lat",
    "long",
    "first_line",
    "second_line",
    "telephone_number",
    "fax_number",
    "phone_number",
    "phone_extension",
    "fax_number_digits",
    "fax_extension",
}
PTG2_UNIFIED_ADDRESS_COLUMNS = PTG2_LEGACY_ADDRESS_COLUMNS | {
    "address_precision",
    "zip5",
    "state_code",
    "location_key",
    "premise_key",
    "address_sources",
    "source_record_ids",
    "source_count",
    "multi_source_confirmed",
    "source_mask",
    "address_source_mask",
    "location_confidence_id",
    "geo_evidence_source_id",
    "geo_identity_coherent",
    "geo_point_coherent",
    "geo_assurance_version",
}

PTG_NO_DISPLAY_ADDRESS_FIELDS = {
    "address",
    "address_kind",
    "formatted_address",
    "address_key",
    "city",
    "state",
    "zip5",
    "zip_code",
    "postal_code",
    "lat",
    "long",
    "latitude",
    "longitude",
    "distance",
    "distance_miles",
    "zip_match_type",
    "coordinates",
    "google_maps_url",
    "google_map_url",
    "maps_url",
    "phone",
    "telephone",
    "telephone_number",
    "phone_number",
    "fax",
    "fax_number",
    *PTG_CONTACT_DETAIL_FIELDS,
    "location_hash",
    "location_source",
    "location_confidence_code",
    "address_sources",
    "address_precision",
    "source_count",
    "multi_source_confirmed",
    "source_mask",
    "address_source_mask",
}

PTG_NO_DISPLAY_VERIFICATION_FIELDS = {
    "address_kind",
    "location_source",
    "location_confidence_code",
    "address_precision",
    "address_sources",
    "source_count",
    "multi_source_confirmed",
    "source_mask",
    "address_source_mask",
    "provider_directory_source_id",
    "provider_directory_org_name",
    "provider_directory_plan_name",
    "provider_directory_location_resource_id",
    "provider_directory_location_name",
    "provider_directory_plan_context_matched",
    "provider_directory_network_name_matched",
    "provider_directory_network_context_present",
    "provider_directory_network_refs",
    "provider_directory_network_names",
    "provider_directory_network_matches",
    "provider_directory_insurance_plan_refs",
    "provider_directory_insurance_plan_matches",
    "provider_directory_match_type",
    "address_verification_evidence",
    "address_provenance",
    "geo_evidence_level",
}


__all__ = [
    "PTG_ADDRESS_KIND_PHYSICAL",
    "PTG_ADDRESS_KIND_POSTAL_BOX",
    "PTG_ADDRESS_KIND_UNKNOWN",
    "PTG2_LEGACY_ADDRESS_COLUMNS",
    "PTG2_UNIFIED_ADDRESS_COLUMNS",
    "PTG_CONTACT_DETAIL_FIELDS",
    "PTG_NO_DISPLAY_ADDRESS_FIELDS",
    "PTG_NO_DISPLAY_VERIFICATION_FIELDS",
    "PTG_POSTAL_BOX_GEO_FIELDS",
    "PTG_POSTAL_BOX_LOCATION_LABEL_FIELDS",
    "address_display_rank_sql",
    "classify_ptg_address_kind",
    "postal_box_address_sql",
]
