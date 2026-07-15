# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""TOC body-file validation, classification, and flat-layout adapters."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlsplit

from process.ptg_parts.canonical import canonicalize_url, normalize_tic_source_url
from process.ptg_parts.domain import (
    PTG2_DOMAIN_ALLOWED_AMOUNT,
    PTG2_DOMAIN_DRUG,
    PTG2_DOMAIN_IN_NETWORK,
    PTG2SourceCatalogEntry,
)

_TOC_BODY_FILE_PLACEHOLDERS = {
    "missing file",
    "n/a",
    "na",
    "none",
    "not available",
    "null",
}
_TOC_BODY_FILE_SUFFIXES = (".json", ".json.gz", ".gz", ".zip", ".csv", ".txt")
_TOC_BODY_FILE_TOKENS = (
    "allowed",
    "download",
    "filetype",
    "innetwork",
    "machine-readable",
    "machine readable",
    "mrf",
    "ndc",
    "outnetwork",
    "outofnetwork",
    "rate",
    "rates",
    "rx",
    "transparency",
)
_TOC_BODY_ALLOWED_TOKENS = (
    "allowed-amount",
    "allowed amount",
    "allowedamount",
    "out-of-network",
    "out of network",
    "outnetwork",
    "outofnetwork",
    "oon",
)
_TOC_BODY_IN_NETWORK_TOKENS = ("in-network", "in network", "innetwork")
_TOC_BODY_DRUG_TOKENS = ("drug", "ndc", "pharmacy", "rx")
_FLAT_TOC_ALLOWED_SECTION_TOKENS = ("allowed amount", "out-of-network")
_FLAT_TOC_IN_NETWORK_SECTION_TOKENS = (
    "in-network",
    "negotiated rate",
    "out-of-area",
)


def _is_toc_body_file_location(value: Any) -> bool:
    location = str(value or "").strip()
    if not location or location.lower() in _TOC_BODY_FILE_PLACEHOLDERS:
        return False
    parsed_location = urlsplit(location)
    if parsed_location.scheme not in {"http", "https"} or not parsed_location.netloc:
        return False
    if parsed_location.path in {"", "/"} and not parsed_location.query:
        return False
    path = parsed_location.path.lower()
    query = parsed_location.query.lower()
    if any(path.endswith(suffix) for suffix in _TOC_BODY_FILE_SUFFIXES):
        return True
    if any(suffix in query for suffix in _TOC_BODY_FILE_SUFFIXES):
        return True
    searchable = f"{path} {query}".replace("_", "-").replace("%20", " ")
    compact = "".join(character for character in searchable if character.isalnum())
    return any(token in searchable or token in compact for token in _TOC_BODY_FILE_TOKENS)


def _toc_body_search_text(location: Any, description: Any = None) -> tuple[str, str]:
    parsed_location = urlsplit(str(location or ""))
    searchable = (
        f"{parsed_location.path} {parsed_location.query} {description or ''}".lower()
        .replace("_", "-")
        .replace("%20", " ")
    )
    compact = "".join(character for character in searchable if character.isalnum())
    return searchable, compact


def _toc_body_source_type(
    default_source_type: str, location: Any, description: Any = None
) -> tuple[str, str]:
    searchable, compact = _toc_body_search_text(location, description)
    if any(token in searchable or token in compact for token in _TOC_BODY_DRUG_TOKENS):
        return "payer-drug", PTG2_DOMAIN_DRUG
    if any(token in searchable or token in compact for token in _TOC_BODY_ALLOWED_TOKENS):
        return "allowed-amounts", PTG2_DOMAIN_ALLOWED_AMOUNT
    if any(token in searchable or token in compact for token in _TOC_BODY_IN_NETWORK_TOKENS):
        return "in-network", PTG2_DOMAIN_IN_NETWORK
    if default_source_type == "allowed-amounts":
        return default_source_type, PTG2_DOMAIN_ALLOWED_AMOUNT
    if default_source_type == "payer-drug":
        return default_source_type, PTG2_DOMAIN_DRUG
    return default_source_type, PTG2_DOMAIN_IN_NETWORK


def _flat_toc_section_type(section_name: str) -> tuple[str, str] | None:
    normalized_section_name = " ".join(str(section_name or "").lower().split())
    if any(token in normalized_section_name for token in _FLAT_TOC_ALLOWED_SECTION_TOKENS):
        return "allowed-amounts", PTG2_DOMAIN_ALLOWED_AMOUNT
    if any(token in normalized_section_name for token in _FLAT_TOC_IN_NETWORK_SECTION_TOKENS):
        return "in-network", PTG2_DOMAIN_IN_NETWORK
    return None


def _flat_toc_catalog_entry(
    file_item: dict[str, Any],
    section_type: tuple[str, str],
    toc_url: str,
    toc_meta: dict[str, Any],
) -> PTG2SourceCatalogEntry | None:
    location = file_item.get("location") or file_item.get("url")
    description = file_item.get("description") or file_item.get("displayname")
    if not _is_toc_body_file_location(location):
        return None
    source_type, domain = section_type
    normalized_location = normalize_tic_source_url(location)
    return PTG2SourceCatalogEntry(
        source_type=source_type,
        domain=domain,
        original_url=normalized_location,
        canonical_url=canonicalize_url(normalized_location),
        from_index_url=toc_url,
        description=description,
        reporting_entity_name=toc_meta["reporting_entity_name"],
        reporting_entity_type=toc_meta["reporting_entity_type"],
        plan_info=(),
    )


def flat_toc_catalog_entries(
    toc_content: dict[str, Any],
    toc_url: str,
    toc_meta: dict[str, Any],
) -> list[PTG2SourceCatalogEntry]:
    """Parse non-standard top-level file lists without binding private plan data."""
    catalog_entries: list[PTG2SourceCatalogEntry] = []
    for section_name, section_items in toc_content.items():
        section_type = _flat_toc_section_type(section_name)
        if section_type is None or not isinstance(section_items, list):
            continue
        for file_item in section_items:
            if not isinstance(file_item, dict):
                continue
            catalog_entry = _flat_toc_catalog_entry(
                file_item,
                section_type,
                toc_url,
                toc_meta,
            )
            if catalog_entry is not None:
                catalog_entries.append(catalog_entry)
    return catalog_entries
