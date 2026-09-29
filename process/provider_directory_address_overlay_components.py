# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed component selection for the Provider Directory address overlay."""

from __future__ import annotations

from typing import Any

ADDRESS_OVERLAY_COMPONENT_SCOPE_TYPES = {
    "organization_address": "organization",
    "practitioner_address": "practitioner",
    "practitioner_role": "role",
    "organization_affiliation": "affiliation",
}
ADDRESS_OVERLAY_COMPONENT_RESOURCE_TYPES = {
    "organization_address": "Organization",
    "practitioner_address": "Practitioner",
    "practitioner_role": "PractitionerRole",
    "organization_affiliation": "OrganizationAffiliation",
}
ADDRESS_OVERLAY_COMPONENTS = tuple(ADDRESS_OVERLAY_COMPONENT_SCOPE_TYPES)
ADDRESS_OVERLAY_DUPLICATE_ORDER = "source_updated_at DESC NULLS LAST, published_at DESC, npi, address_key"


def address_overlay_component_select_sql(template: str, context: dict[str, str]) -> str:
    """Share the exact component input between physical inserts and virtual projection."""
    prefix = "INSERT INTO {stage_ref} ({columns})"
    statement = template.strip()
    if not statement.startswith(prefix):
        raise ValueError("provider_directory_overlay_component_template_changed")
    return statement[len(prefix) :].strip().removesuffix(";").format(**context)


def address_overlay_existing_select_sql(target_ref: str, columns: str, refresh_filter: str) -> str:
    """Copy precisely the incumbent rows outside the requested refresh scope."""
    return f"SELECT {columns} FROM {target_ref} WHERE NOT ({refresh_filter})"


def address_overlay_alias_columns() -> dict[str, str]:
    """Return the native alias rewrite assignments, after alias integrity validation."""
    return {
        "address_key": "target.address_key",
        "premise_key": "target.premise_key",
        "lat": "COALESCE(stage_row.lat, target.lat)",
        "long": "COALESCE(stage_row.long, target.long)",
    }


def address_overlay_archive_coordinate_predicate() -> str:
    """Retain the full builder's paired archive-coordinate eligibility rule."""
    return """stage_row.address_key IS NOT NULL
        AND archive.address_key = stage_row.address_key AND archive.merged_into IS NULL
        AND archive.lat IS NOT NULL AND archive.long IS NOT NULL
        AND NOT (ABS(archive.lat) < 0.0000001 AND ABS(archive.long) < 0.0000001)
        AND (stage_row.lat IS NULL OR stage_row.long IS NULL)"""


def address_overlay_formatted_columns(renderer: str, version: int, source: str) -> dict[str, str]:
    """Render the same structured label and metadata in UPDATE and SELECT paths."""
    return {
        "formatted_address": f"""{renderer}(stage_row.first_line, stage_row.second_line,
            stage_row.city_name, stage_row.state_name, stage_row.postal_code, stage_row.country_code)""",
        "formatted_address_version": str(version),
        "formatted_address_source": "'" + source.replace("'", "''") + "'",
    }


def clean_address_overlay_components(raw_components: Any) -> tuple[str, ...]:
    """Keep only named components, in requested order, without duplicates."""

    if raw_components in (None, "", ()):
        return ADDRESS_OVERLAY_COMPONENTS
    if isinstance(raw_components, str):
        component_values = raw_components.split(",")
    elif isinstance(raw_components, (bytes, bytearray, dict)) or not hasattr(raw_components, "__iter__"):
        component_values = (raw_components,)
    else:
        component_values = raw_components
    cleaned_components: list[str] = []
    seen_components: set[str] = set()
    for component in component_values:
        component_text = str(component).strip() if component is not None else ""
        if not component_text:
            continue
        if component_text not in ADDRESS_OVERLAY_COMPONENT_SCOPE_TYPES:
            raise ValueError(f"unknown Provider Directory address overlay component: {component_text}")
        if component_text in seen_components:
            continue
        seen_components.add(component_text)
        cleaned_components.append(component_text)
    return tuple(cleaned_components or ADDRESS_OVERLAY_COMPONENTS)
