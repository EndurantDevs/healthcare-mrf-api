# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""FHIR identifier selection with explicit CMS NPI and pseudo-EIN guards."""

from __future__ import annotations

import re
from typing import Any

from process.provider_directory_projection_fhir_values import is_valid_npi

CMS_NPD_SOURCE_ID = "cms-npd"
CMS_NPD_PSEUDO_EIN_SYSTEM = "https://npd.cms.gov/fhir/sid/us-pseudo-ein"


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def identifier_descriptor(identifier: dict[str, Any]) -> str:
    """Describe the declared identifier system and type."""
    identifier_type = identifier.get("type") if isinstance(identifier.get("type"), dict) else {}
    descriptor_parts = [identifier.get("system"), identifier_type.get("text")]
    for coding in identifier_type.get("coding") or []:
        if isinstance(coding, dict):
            descriptor_parts.extend((coding.get("system"), coding.get("code"), coding.get("display")))
    return " ".join(str(part).lower() for part in descriptor_parts if part)


def identifier_value(resource: dict[str, Any], *tokens: str, allow_systemless: bool = False) -> str | None:
    """Select the first typed value, optionally retaining a systemless fallback."""
    lowered_tokens = tuple(token.lower() for token in tokens)
    systemless_value = None
    for identifier in resource.get("identifier") or []:
        if not isinstance(identifier, dict):
            continue
        value = _clean_text(identifier.get("value"))
        if not value:
            continue
        descriptor = identifier_descriptor(identifier)
        if any(token in descriptor for token in lowered_tokens):
            return value
        if allow_systemless and not identifier.get("system") and not identifier.get("type"):
            systemless_value = systemless_value or value
    return systemless_value


def explicit_npi(resource: dict[str, Any], *, valid_only: bool = False) -> int | None:
    """Prefer the declared NPI system; CMS additionally requires checksum validity."""
    recognized_system = "http://hl7.org/fhir/sid/us-npi"
    best_candidate: tuple[int, int, int] | None = None
    for index, identifier in enumerate(resource.get("identifier") or []):
        if not isinstance(identifier, dict):
            continue
        value = _clean_text(identifier.get("value"))
        if not value or not any(
            token in identifier_descriptor(identifier) for token in ("us-npi", "npi", "national provider")
        ):
            continue
        if valid_only and any(ch.isnumeric() and not ch.isascii() for ch in value):
            continue
        digits = "".join(ch for ch in value if ch.isdigit())
        if len(digits) != 10 or (valid_only and not is_valid_npi(digits)):
            continue
        try:
            numeric_npi = int(digits)
        except ValueError:
            continue
        priority = int((_clean_text(identifier.get("system")) or "").lower() != recognized_system)
        candidate = (priority, index, numeric_npi)
        if best_candidate is None or candidate < best_candidate:
            best_candidate = candidate
    return best_candidate[2] if best_candidate is not None else None


def npi_from_resource_id(resource_id: str | None) -> int | None:
    """Keep legacy source fallback separate from the CMS policy."""
    text = _clean_text(resource_id)
    if not text or not re.fullmatch(r"[0-9]{10}", text):
        return None
    return int(text)


def resource_npi(resource: dict[str, Any], *, source_id: str, resource_id: str | None) -> int | None:
    """Never infer a CMS NPI from a numeric FHIR resource ID."""
    if source_id == CMS_NPD_SOURCE_ID:
        return explicit_npi(resource, valid_only=True)
    return explicit_npi(resource) or npi_from_resource_id(resource_id)


def tax_id(resource: dict[str, Any], *, source_id: str = "") -> str | None:
    """Leave CMS tax identity empty until an EIN mapping is reviewed."""
    if source_id == CMS_NPD_SOURCE_ID:
        return None
    for identifier in resource.get("identifier") or []:
        if not isinstance(identifier, dict):
            continue
        system = (_clean_text(identifier.get("system")) or "").lower()
        if system == CMS_NPD_PSEUDO_EIN_SYSTEM:
            continue
        value = _clean_text(identifier.get("value"))
        if value and any(token in identifier_descriptor(identifier) for token in ("tax", "tin", "ein")):
            return value[:64]
    return None
