# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Strict source-neutral normalization for common FHIR value shapes."""

from __future__ import annotations

from decimal import Decimal
import math
from typing import Any, Mapping

from process.provider_directory_projection_types import (
    ProviderDirectoryProjectionError,
)


def invalid_fhir_field(field_name: str) -> ProviderDirectoryProjectionError:
    """Return the stable field-shape error used by the transform boundary."""

    return ProviderDirectoryProjectionError(
        f"provider_directory_projection_fhir_{field_name}_invalid"
    )


def normalized_text(field_content: Any, *, limit: int = 2048) -> str | None:
    """Return one bounded nonblank FHIR string or no contribution."""

    if not isinstance(field_content, str):
        return None
    normalized = field_content.strip()
    return normalized if normalized and len(normalized) <= limit else None


def optional_field_text(
    container: Mapping[str, Any],
    field_name: str,
    *,
    limit: int = 2048,
) -> str | None:
    """Normalize a present string field and reject malformed presence."""

    if field_name not in container:
        return None
    normalized = normalized_text(container[field_name], limit=limit)
    if normalized is None:
        raise invalid_fhir_field(field_name)
    return normalized


def fhir_list(container: Mapping[str, Any], field_name: str) -> list[Any]:
    """Return an absent list as empty and reject malformed presence."""

    if field_name not in container:
        return []
    field_entries = container[field_name]
    if not isinstance(field_entries, list):
        raise invalid_fhir_field(field_name)
    return field_entries


def normalized_period_map(period_data: Any) -> dict[str, str] | None:
    """Normalize a present FHIR Period without accepting an empty object."""

    if period_data is None:
        return None
    if not isinstance(period_data, Mapping):
        raise invalid_fhir_field("period")
    period_map = {
        field_name: field_text
        for field_name in ("start", "end")
        if (
            field_text := optional_field_text(period_data, field_name, limit=64)
        )
        is not None
    }
    if not period_map:
        raise invalid_fhir_field("period")
    return period_map


def normalized_coding_map(coding_data: Any) -> dict[str, Any]:
    """Normalize one coding while preserving its useful display identity."""

    if not isinstance(coding_data, Mapping):
        raise invalid_fhir_field("coding")
    coding_map: dict[str, Any] = {}
    for field_name in ("system", "version", "code", "display"):
        field_text = optional_field_text(coding_data, field_name)
        if field_text is not None:
            coding_map[field_name] = field_text
    if "userSelected" in coding_data and type(coding_data["userSelected"]) is not bool:
        raise invalid_fhir_field("coding_user_selected")
    if type(coding_data.get("userSelected")) is bool:
        coding_map["userSelected"] = coding_data["userSelected"]
    if not set(coding_map).intersection({"code", "display"}):
        raise invalid_fhir_field("coding")
    return coding_map


def normalized_concept_map(concept_data: Any) -> dict[str, Any]:
    """Normalize one CodeableConcept, including all strict coding entries."""

    if not isinstance(concept_data, Mapping):
        raise invalid_fhir_field("codeable_concept")
    concept_map: dict[str, Any] = {}
    for field_name in ("system", "version", "code", "display", "text"):
        field_text = optional_field_text(concept_data, field_name)
        if field_text is not None:
            concept_map[field_name] = field_text
    coding_maps = [
        normalized_coding_map(coding_data)
        for coding_data in fhir_list(concept_data, "coding")
    ]
    if coding_maps:
        concept_map["coding"] = coding_maps
    if not set(concept_map).intersection({"code", "display", "text", "coding"}):
        raise invalid_fhir_field("codeable_concept")
    return concept_map


def profile_concept_maps(concept_data: Any) -> list[dict[str, Any]]:
    """Normalize one or many CodeableConcept shapes into a list."""

    if concept_data is None:
        return []
    concept_entries = concept_data if isinstance(concept_data, list) else [concept_data]
    if not all(isinstance(concept_entry, Mapping) for concept_entry in concept_entries):
        raise invalid_fhir_field("codeable_concept")
    return [normalized_concept_map(concept_entry) for concept_entry in concept_entries]


def normalized_human_name_map(name_data: Any) -> dict[str, Any]:
    """Normalize one person or organization HumanName."""

    if not isinstance(name_data, Mapping):
        raise invalid_fhir_field("name")
    name_map: dict[str, Any] = {}
    for field_name in ("use", "text", "family"):
        field_text = optional_field_text(name_data, field_name)
        if field_text is not None:
            name_map[field_name] = field_text
    for field_name in ("given", "prefix", "suffix"):
        name_parts = []
        for name_part in fhir_list(name_data, field_name):
            part_text = normalized_text(name_part)
            if part_text is None:
                raise invalid_fhir_field(f"name_{field_name}")
            name_parts.append(part_text)
        if name_parts:
            name_map[field_name] = name_parts
    period_map = normalized_period_map(name_data.get("period"))
    if period_map is not None:
        name_map["period"] = period_map
    if not set(name_map).intersection({"text", "family", "given"}):
        raise invalid_fhir_field("name")
    return name_map


def profile_name_maps(fhir_resource_map: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Normalize resource names and aliases into profile evidence."""

    raw_name = fhir_resource_map.get("name")
    name_maps: list[dict[str, Any]] = []
    if isinstance(raw_name, str):
        name_text = normalized_text(raw_name)
        if name_text is None:
            raise invalid_fhir_field("name")
        name_maps.append({"text": name_text})
    elif isinstance(raw_name, Mapping):
        name_maps.append(normalized_human_name_map(raw_name))
    elif isinstance(raw_name, list):
        name_maps.extend(normalized_human_name_map(name_data) for name_data in raw_name)
    elif raw_name is not None:
        raise invalid_fhir_field("name")
    for alias in fhir_list(fhir_resource_map, "alias"):
        alias_text = normalized_text(alias)
        if alias_text is None:
            raise invalid_fhir_field("alias")
        name_maps.append({"text": alias_text})
    return name_maps


def profile_contact_maps(
    fhir_resource_map: Mapping[str, Any],
    resource_type: str,
) -> list[dict[str, Any]]:
    """Normalize telecom and Endpoint contact entries."""

    contact_maps = []
    contact_entries = list(fhir_list(fhir_resource_map, "telecom"))
    if resource_type == "Endpoint":
        contact_entries.extend(fhir_list(fhir_resource_map, "contact"))
    for contact_data in contact_entries:
        if not isinstance(contact_data, Mapping):
            raise invalid_fhir_field("telecom")
        contact_text = optional_field_text(contact_data, "value")
        if contact_text is None:
            raise invalid_fhir_field("telecom_value")
        contact_map: dict[str, Any] = {"value": contact_text}
        for field_name in ("system", "use"):
            field_text = optional_field_text(contact_data, field_name, limit=64)
            if field_text is not None:
                contact_map[field_name] = field_text
        rank = contact_data.get("rank")
        if "rank" in contact_data and (type(rank) is not int or rank < 1):
            raise invalid_fhir_field("telecom_rank")
        if type(rank) is int:
            contact_map["rank"] = rank
        period_map = normalized_period_map(contact_data.get("period"))
        if period_map is not None:
            contact_map["period"] = period_map
        contact_maps.append(contact_map)
    return contact_maps


def normalized_address_map(address_data: Any) -> dict[str, Any]:
    """Normalize one meaningful FHIR Address."""

    if not isinstance(address_data, Mapping):
        raise invalid_fhir_field("address")
    address_map: dict[str, Any] = {}
    for field_name in (
        "use", "type", "text", "city", "district", "state", "postalCode", "country"
    ):
        field_text = optional_field_text(address_data, field_name)
        if field_text is not None:
            address_map[field_name] = field_text
    if "line" in address_data:
        address_lines = []
        for address_line in fhir_list(address_data, "line"):
            line_text = normalized_text(address_line)
            if line_text is None:
                raise invalid_fhir_field("address_line")
            address_lines.append(line_text)
        if address_lines:
            address_map["line"] = address_lines
    period_map = normalized_period_map(address_data.get("period"))
    if period_map is not None:
        address_map["period"] = period_map
    meaningful_fields = {
        "text", "line", "city", "district", "state", "postalCode", "country"
    }
    if not set(address_map).intersection(meaningful_fields):
        raise invalid_fhir_field("address")
    return address_map


def normalized_position_map(position_data: Any) -> dict[str, int] | None:
    """Normalize finite WGS84 coordinates to exact microdegrees."""

    if position_data is None:
        return None
    if not isinstance(position_data, Mapping):
        raise invalid_fhir_field("position")
    latitude = position_data.get("latitude")
    longitude = position_data.get("longitude")
    numeric_types = (int, float, Decimal)
    if (
        isinstance(latitude, bool)
        or isinstance(longitude, bool)
        or not isinstance(latitude, numeric_types)
        or not isinstance(longitude, numeric_types)
        or not math.isfinite(latitude)
        or not math.isfinite(longitude)
        or not -90 <= latitude <= 90
        or not -180 <= longitude <= 180
    ):
        raise invalid_fhir_field("position")
    return {
        "latitude_microdegrees": round(latitude * 1_000_000),
        "longitude_microdegrees": round(longitude * 1_000_000),
    }


__all__ = (
    "fhir_list",
    "invalid_fhir_field",
    "normalized_address_map",
    "normalized_period_map",
    "normalized_position_map",
    "normalized_text",
    "optional_field_text",
    "profile_concept_maps",
    "profile_contact_maps",
    "profile_name_maps",
)
