# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
# pylint: disable=too-many-lines

from __future__ import annotations

import math
import os
import re
import time
from pathlib import Path
from typing import Any, Mapping

from sqlalchemy import bindparam, text

from api.code_systems import EQUIVALENT_PROCEDURE_CODE_SYSTEMS, canonical_catalog_code
from api.endpoint.pagination import PaginationParams
from api.ptg2_code_filters import (
    INFERRED_PROVIDER_TAXONOMY_RULES,
    INTERNAL_PROCEDURE_CODE_SYSTEM,
    PROCEDURE_CODE_SYSTEMS,
    InferredProviderTaxonomyRule,
    _append_code_filter,
    _append_resolved_code_filter,
    _inferred_provider_taxonomy_sql,
    _is_external_procedure_code_text,
    _is_signed_int_text,
    _normalize_code,
    _normalize_code_system,
    _normalize_npi,
    _normalize_taxonomy_code,
    _ptg2_code_query_fields,
    _qualify_compact_filters,
)
from api.ptg2_index_cache import (
    PTG2_ARTIFACT_KIND_SNAPSHOT_INDEX,
    PTG2_INDEX_CACHE_TTL_SECONDS,
    PTG2_RESPONSE_CACHE_MAX_KEYS,
    PTG2_RESPONSE_CACHE_TTL_SECONDS,
    _CACHE_MISS,
    _PTG2_INDEX_CACHE,
    _artifact_root,
    _path_from_uri,
    _ptg2_response_cache_get,
    _ptg2_response_cache_key,
    _ptg2_response_cache_set,
    clear_ptg2_index_cache,
    load_ptg2_index_from_path,
)
from api.ptg2_code_details import _enrich_ptg2_code_details
from api.ptg2_code_context import (
    _query_ptg2_code_crosswalk_edges,
    _resolve_ptg2_code_search_context,
)
from api.ptg2_snapshot import (
    current_snapshot_id,
    current_source_snapshot_id_for_plan,
    current_source_snapshot_ids_for_plan,
    load_current_ptg2_index,
    resolve_current_ptg2_snapshot_id,
    snapshot_artifact_uri,
)
from api.ptg2_response import (
    PTG2_ITEM_DIAGNOSTIC_FIELDS,
    PTG2_ITEM_SOURCE_FIELDS,
    PTG2_QUERY_DIAGNOSTIC_FIELDS,
    PTG2_QUERY_SOURCE_FIELDS,
    _canonical_catalog_code,
    _canonical_price_row,
    _catalog_key,
    _coerce_json_payload,
    _coerce_numeric_rate,
    _include_ptg2_details,
    _include_ptg2_sources,
    _normalize_catalog_code_system,
    _normalize_filter_string_list,
    _normalize_price_payload,
    _normalize_string_list,
    _optional_decimal,
    _optional_float,
    _price_component,
    _price_response_fields,
    _price_row_key,
    _request_bool,
    _shape_ptg2_response,
    _summarize_price_payload,
)
from api.ptg2_price_sql import (
    _empty_price_array_sql,
    _normalized_price_join_sql,
    _normalized_price_json_sql,
    _price_atom_payload_sql,
    _scalar_price_json_sql,
    _typed_price_json_sql,
)
from api.ptg2_tables import (
    _gin_index_available_for_column,
    _index_available,
    _is_compact_serving_table,
    _ordered_serving_table_candidates,
    _safe_table_name,
    _serving_table_candidates,
    _serving_table_available,
    _serving_table_name,
    snapshot_serving_table,
    snapshot_serving_tables,
)
from api.ptg2_types import PTG2ServingIndex, PTG2ServingTables
from api.ptg2_manifest_artifacts import search_ptg2_manifest_serving_snapshot
from process.ptg_parts.ptg2_manifest_artifacts import lookup_global_sidecar_members, lookup_global_sidecar_members_many
from api.ptg2_serving_utils import (
    _normalize_zip5,
    _price_filter_clauses,
    _provider_payload,
    _row_mapping,
    _uuid_to_hex,
)
from api.provider_specialty_filters import (
    provider_specialty_taxonomy_exists_sql,
    resolve_provider_specialty_filter,
)

PTG2_MODE_EXACT_SOURCE = "exact_source"
PTG2_MODE_PRODUCT_SEARCH = "product_search"
PTG2_SCHEMA = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
PTG2_WARM_P95_MAX_MS = max(float(os.getenv("HLTHPRT_PTG2_WARM_P95_MAX_MS", "50")), 1.0)
PTG2_JSON_FALLBACK_ENV = "HLTHPRT_PTG2_ENABLE_JSON_FALLBACK"
PTG2_SERVING_TABLE_ENV = "HLTHPRT_PTG2_SERVING_TABLE"
PTG2_FAST_COMPACT_COUNTS_ENV = "HLTHPRT_PTG2_FAST_COMPACT_COUNTS"
ADDRESS_SERVING_SOURCE_ENV = "HLTHPRT_ADDRESS_SERVING_SOURCE"
ADDRESS_SERVING_SOURCE_LEGACY = "legacy"
ADDRESS_SERVING_SOURCE_UNIFIED = "entity_address_unified"
_PTG2_MANIFEST_SIDECAR_CACHE: dict[tuple[str, str, str], tuple[str, ...]] = {}
_PTG2_MANIFEST_TAXONOMY_RATE_CANDIDATE_LIMIT = 25
_PTG2_LEGACY_ADDRESS_COLUMNS = {
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
}
_PTG2_UNIFIED_ADDRESS_COLUMNS = _PTG2_LEGACY_ADDRESS_COLUMNS | {"address_precision"}
PTG_PROVIDER_DIRECTORY_ADDRESS_CORROBORATION_VIEW = (
    f"{PTG2_SCHEMA}.ptg_provider_directory_address_corroboration"
)
PTG_NO_DISPLAY_ADDRESS_FIELDS = {
    "address",
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
    "location_source",
    "location_confidence_code",
    "address_precision",
    "address_sources",
    "source_count",
    "multi_source_confirmed",
    "source_mask",
    "address_source_mask",
    "provider_directory_source_id",
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
}
PTG_DIRECT_PAYER_LOCATION_RECORD_KEYS = {
    "source_record_id",
    "source_record_key",
    "source_provider_reference_id",
    "provider_reference_id",
    "provider_reference",
    "provider_group_id",
    "provider_group_global_id_128",
    "provider_group_hash",
    "provider_group_location_hash",
    "raw_provider_location_key",
    "json_pointer",
}


def _ptg2_geo_distance_miles_sql(lat_sql: str, long_sql: str) -> str:
    """Return a PostgreSQL expression for great-circle distance in miles."""
    request_lat = "CAST(:geo_lat AS double precision)"
    request_long = "CAST(:geo_long AS double precision)"
    return (
        "(2 * 3958.7613 * asin(least(1.0, sqrt("
        f"power(sin(radians(({lat_sql}) - {request_lat}) / 2), 2) + "
        f"cos(radians({request_lat})) * cos(radians({lat_sql})) * "
        f"power(sin(radians(({long_sql}) - {request_long}) / 2), 2)"
        "))))"
    )


def normalize_ptg2_mode(value: str | None) -> str:
    mode = str(value or PTG2_MODE_PRODUCT_SEARCH).strip().lower()
    if mode not in {PTG2_MODE_EXACT_SOURCE, PTG2_MODE_PRODUCT_SEARCH}:
        raise ValueError("mode must be exact_source or product_search")
    return mode


def _address_serving_unified_requested() -> bool:
    return os.getenv(ADDRESS_SERVING_SOURCE_ENV, ADDRESS_SERVING_SOURCE_UNIFIED).strip().lower() == ADDRESS_SERVING_SOURCE_UNIFIED


def _is_unified_address_table(table_name: str | None) -> bool:
    return bool(table_name and table_name.endswith(".entity_address_unified"))


def _ptg2_address_location_source(address_table: str | None) -> str:
    return "entity_address_unified" if _is_unified_address_table(address_table) else "npi_address"


def _ptg2_address_location_hash_sql(alias: str, address_table: str | None) -> str:
    source = _ptg2_address_location_source(address_table)
    return f"CONCAT('{source}:', {alias}.npi, ':', {alias}.type, ':', {alias}.checksum)"


def _ptg2_provider_name_sql(alias: str = "n") -> str:
    return (
        "COALESCE("
        f"NULLIF(BTRIM({alias}.provider_organization_name), ''), "
        f"NULLIF(BTRIM(CONCAT_WS(' ', {alias}.provider_first_name, {alias}.provider_middle_name, {alias}.provider_last_name)), ''), "
        "'TiC provider')"
    )


def _add_location_phone_fields(item: dict[str, Any], data: dict[str, Any], address_payload: dict[str, Any]) -> None:
    phone = (
        data.get("telephone_number")
        or data.get("phone_number")
        or address_payload.get("telephone_number")
        or address_payload.get("phone_number")
        or address_payload.get("phone")
    )
    if phone not in (None, "", "null"):
        item["telephone_number"] = phone
        item["phone_number"] = phone
        item["phone"] = phone
    fax = data.get("fax_number") or address_payload.get("fax_number")
    if fax not in (None, "", "null"):
        item["fax_number"] = fax


def _coerce_bool_payload(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return bool(value)
    normalized = str(value or "").strip().lower()
    return normalized in {"1", "true", "t", "yes", "y"}


def _optional_bool_payload(value: Any) -> bool | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return bool(value)
    normalized = str(value or "").strip().lower()
    if normalized in {"1", "true", "t", "yes", "y"}:
        return True
    if normalized in {"0", "false", "f", "no", "n"}:
        return False
    return None


def _coerce_int_payload(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_str_list_payload(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    payload = _coerce_json_payload(value, None) if isinstance(value, str) else _coerce_json_payload(value, [])
    if payload in (None, ""):
        payload = value
    if isinstance(payload, str):
        payload = [payload]
    if not isinstance(payload, list):
        return []
    values: list[str] = []
    for item in payload:
        text = str(item or "").strip()
        if text and text not in values:
            values.append(text)
    return values


def _first_payload_value(*values: Any) -> Any:
    for value in values:
        if value not in (None, "", []):
            return value
    return None


def _nonempty_payload_value(source: dict[str, Any], *keys: str) -> bool:
    return any(source.get(key) not in (None, "", [], {}) for key in keys)


def _has_displayable_address_fields(source: dict[str, Any]) -> bool:
    if _nonempty_payload_value(source, "first_line", "address_line_1", "street", "street_address"):
        return True
    has_city = _nonempty_payload_value(source, "city", "city_name")
    has_region = _nonempty_payload_value(source, "state", "state_name", "postal_code", "zip5")
    return has_city and has_region


def _has_displayed_address_payload(item: dict[str, Any], address_payload: dict[str, Any]) -> bool:
    for source in (address_payload, item):
        if _has_displayable_address_fields(source):
            return True
        nested_address = source.get("address")
        if isinstance(nested_address, dict) and _has_displayable_address_fields(nested_address):
            return True
    return False


def _provider_directory_plan_context_matched(address_payload: dict[str, Any]) -> bool:
    if _coerce_bool_payload(address_payload.get("provider_directory_plan_context_matched")):
        return True
    evidence = _coerce_json_payload(address_payload.get("address_verification_evidence"), {})
    if isinstance(evidence, dict):
        matched_on = str(evidence.get("matched_on") or "").strip().lower()
        return matched_on.endswith("_plan")
    return False


def _canonical_network_name_payload(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _provider_directory_network_name_matches(
    item: dict[str, Any],
    address_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    ptg_names = _coerce_str_list_payload(
        _first_payload_value(item.get("network_names"), address_payload.get("network_names"))
    )
    ptg_by_key = {
        _canonical_network_name_payload(value): value
        for value in ptg_names
        if _canonical_network_name_payload(value)
    }
    if not ptg_by_key:
        return []

    raw_directory_networks = _coerce_json_payload(address_payload.get("provider_directory_network_matches"), [])
    directory_networks = list(raw_directory_networks) if isinstance(raw_directory_networks, list) else []
    for name in _coerce_str_list_payload(address_payload.get("provider_directory_network_names")):
        directory_networks.append({"name": name})

    matches: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str | None]] = set()
    seen_candidate_keys: set[str] = set()
    for network in directory_networks:
        if not isinstance(network, dict):
            continue
        candidate_values = [network.get("name"), network.get("provider_directory_network_name")]
        aliases = _coerce_json_payload(network.get("aliases"), [])
        if isinstance(aliases, list):
            candidate_values.extend(aliases)
        for candidate in candidate_values:
            candidate_key = _canonical_network_name_payload(candidate)
            if not candidate_key or candidate_key not in ptg_by_key:
                continue
            if candidate_key in seen_candidate_keys:
                continue
            match_key = (candidate_key, str(network.get("resource_id") or ""), str(network.get("ref") or ""))
            if match_key in seen:
                continue
            seen.add(match_key)
            seen_candidate_keys.add(candidate_key)
            matches.append(
                {
                    "ptg_network_name": ptg_by_key[candidate_key],
                    "provider_directory_network_name": str(candidate or ""),
                    "provider_directory_network_resource_id": (
                        network.get("resource_id") or network.get("provider_directory_network_resource_id")
                    ),
                    "provider_directory_network_ref": network.get("ref") or network.get("provider_directory_network_ref"),
                }
            )
    return matches


def _provider_directory_network_context_matched(
    item: dict[str, Any],
    address_payload: dict[str, Any],
) -> bool:
    if _provider_directory_plan_context_matched(address_payload):
        return True
    return bool(_provider_directory_network_name_matches(item, address_payload))


def _has_direct_payer_location_record_evidence(address_payload: dict[str, Any]) -> bool:
    evidence = _coerce_json_payload(address_payload.get("address_verification_evidence"), {})
    if not isinstance(evidence, dict):
        return False
    return any(evidence.get(key) not in (None, "", [], {}) for key in PTG_DIRECT_PAYER_LOCATION_RECORD_KEYS)


def _provider_directory_address_verification_evidence(
    address_payload: dict[str, Any],
    provider_directory_network_name_matches: list[dict[str, Any]],
) -> dict[str, Any] | None:
    evidence = _coerce_json_payload(address_payload.get("address_verification_evidence"), {})
    if not isinstance(evidence, dict):
        evidence = {}
    if not evidence and not provider_directory_network_name_matches:
        return None
    updated = dict(evidence)
    if provider_directory_network_name_matches and not _provider_directory_plan_context_matched(address_payload):
        matched_on = str(updated.get("matched_on") or "").strip()
        if matched_on and not matched_on.endswith("_network_name"):
            updated["matched_on"] = f"{matched_on}_network_name"
        elif not matched_on:
            updated["matched_on"] = "npi_address_key_role_location_network_name"
        updated["network_name_matches"] = provider_directory_network_name_matches
        updated["network_name_context_matched"] = True
    elif not provider_directory_network_name_matches and not _provider_directory_plan_context_matched(address_payload):
        matched_on = str(updated.get("matched_on") or "").strip()
        if matched_on.endswith("_network_name"):
            updated["matched_on"] = matched_on.removesuffix("_network_name") or "npi_address_key_role_location"
        updated.pop("network_name_matches", None)
        updated.pop("network_name_context_matched", None)
    return updated


def _has_source_file_version_trace(item: dict[str, Any]) -> bool:
    source_trace = item.get("source_trace")
    if not isinstance(source_trace, list):
        return False
    return any(
        isinstance(entry, dict) and str(entry.get("source_file_version_id") or "").strip()
        for entry in source_trace
    )


def _promote_address_provenance_fields(item: dict[str, Any], address_payload: dict[str, Any]) -> None:
    address_key = address_payload.get("address_key")
    if address_key not in (None, "") and item.get("address_key") in (None, ""):
        item["address_key"] = address_key
    for key in (
        "address_precision",
        "source_count",
        "multi_source_confirmed",
        "source_mask",
        "address_source_mask",
        "location_confidence_id",
    ):
        value = address_payload.get(key)
        if value not in (None, "") and item.get(key) in (None, ""):
            item[key] = value


def _address_verification_payload(
    item: dict[str, Any],
    data: dict[str, Any],
    address_payload: dict[str, Any],
) -> dict[str, Any]:
    displayed_address_present = _has_displayed_address_payload(item, address_payload)
    if displayed_address_present is False:
        return {
            "rate_network_binding": "tic_provider_group_npi_tin",
            "address_network_binding": "inferred_from_provider_identity",
            "address_evidence_level": "unknown",
            "requires_location_confirmation": True,
            "reason": "PTG proves the provider identity is in network, but no displayable address is available.",
            "displayed_address_present": False,
            "network_bound_address": False,
        }

    location_source = item.get("location_source") or data.get("location_source")
    location_confidence_code = item.get("location_confidence_code") or data.get("location_confidence_code")
    address_sources = _coerce_str_list_payload(
        _first_payload_value(item.get("address_sources"), address_payload.get("address_sources"))
    )
    address_precision = (
        item.get("address_precision")
        or address_payload.get("address_precision")
        or ("street" if address_payload.get("first_line") else None)
    )
    source_count = _coerce_int_payload(
        _first_payload_value(item.get("source_count"), address_payload.get("source_count"))
    )
    source_mask = (
        _coerce_int_payload(_first_payload_value(item.get("source_mask"), address_payload.get("source_mask")))
        or 0
    )
    address_source_mask = (
        _coerce_int_payload(
            _first_payload_value(item.get("address_source_mask"), address_payload.get("address_source_mask"))
        )
        or 0
    )
    multi_source_confirmed = _coerce_bool_payload(
        _first_payload_value(item.get("multi_source_confirmed"), address_payload.get("multi_source_confirmed"))
    )
    if not multi_source_confirmed and source_count is not None:
        multi_source_confirmed = source_count > 1

    source_markers = {
        str(value or "").strip().lower()
        for value in (
            location_source,
            location_confidence_code,
            item.get("address_network_binding"),
            data.get("address_network_binding"),
            address_payload.get("address_network_binding"),
            address_payload.get("location_source"),
            address_payload.get("location_confidence_code"),
        )
    }
    direct_ptg_location = bool(
        source_markers
        & {
            "payer_confirmed_location",
            "payer_provider_group_location",
            "ptg_provider_group_location",
            "tic_provider_group_location",
        }
    ) and _has_direct_payer_location_record_evidence(address_payload) and _has_source_file_version_trace(item)
    normalized_sources = {value.lower().replace("-", "_") for value in address_sources}
    provider_directory_address = bool(
        source_markers
        & {
            "provider_directory_fhir",
            "provider_directory_address",
            "payer_provider_directory",
            "payer_directory_corroborated_location",
        }
    ) or bool(normalized_sources & {"provider_directory", "provider_directory_fhir", "payer_provider_directory"})
    provider_directory_network_name_matches = _provider_directory_network_name_matches(item, address_payload)
    provider_directory_evidence = _provider_directory_address_verification_evidence(
        address_payload,
        provider_directory_network_name_matches,
    )
    provider_directory_network_location = bool(
        provider_directory_address
        or source_markers
        & {
            "payer_directory_corroborated_location",
            "provider_directory_network_location",
            "provider_directory_plan_network_location",
        }
    ) and _provider_directory_network_context_matched(item, address_payload)
    direct_mrf_address = bool(address_source_mask & 2) or "mrf" in normalized_sources
    nppes_address = bool(address_source_mask & 1) or "nppes" in normalized_sources
    if direct_ptg_location:
        address_evidence_level = "payer_confirmed_location"
        address_network_binding = "payer_confirmed_location"
        requires_location_confirmation = False
        reason = "The payer/PTG source supplied the provider location used for this result."
    elif provider_directory_network_location:
        address_evidence_level = "payer_directory_network_location"
        address_network_binding = "payer_directory_corroborated_location"
        requires_location_confirmation = False
        reason = "A payer Provider Directory record links this provider, network or plan context, and displayed address."
    elif provider_directory_address:
        address_evidence_level = "provider_directory_address"
        address_network_binding = "inferred_from_provider_identity"
        requires_location_confirmation = True
        reason = "A payer Provider Directory record corroborates the displayed provider address, but the PTG rate file did not supply it."
    elif address_precision == "city_zip":
        address_evidence_level = "city_zip_fallback"
        address_network_binding = "inferred_from_provider_identity"
        requires_location_confirmation = True
        reason = "PTG proves the provider identity is in network, but only city/ZIP address evidence is available."
    elif direct_mrf_address and multi_source_confirmed:
        address_evidence_level = "multi_source_direct_mrf_address"
        address_network_binding = "inferred_from_provider_identity"
        requires_location_confirmation = True
        reason = "The street address is corroborated by MRF and another source, but not by this PTG rate file."
    elif direct_mrf_address:
        address_evidence_level = "direct_mrf_address"
        address_network_binding = "inferred_from_provider_identity"
        requires_location_confirmation = True
        reason = "The street address came from MRF provider-address evidence, but not from this PTG rate file."
    elif multi_source_confirmed:
        address_evidence_level = "multi_source_provider_address"
        address_network_binding = "inferred_from_provider_identity"
        requires_location_confirmation = True
        reason = "The street address is corroborated by multiple provider-address sources, but not by this PTG rate file."
    elif nppes_address or str(location_source or "").strip().lower() == "npi_address":
        address_evidence_level = "nppes_provider_address"
        address_network_binding = "inferred_from_provider_identity"
        requires_location_confirmation = True
        reason = "PTG proves the NPI/TIN is in network; the displayed address comes from NPPES/provider enrichment."
    elif str(location_source or "").strip().lower() == "entity_address_unified":
        address_evidence_level = "unified_provider_address"
        address_network_binding = "inferred_from_provider_identity"
        requires_location_confirmation = True
        reason = "PTG proves the provider identity is in network; the displayed address comes from unified address evidence."
    else:
        address_evidence_level = "unknown"
        address_network_binding = "inferred_from_provider_identity"
        requires_location_confirmation = True
        reason = "PTG proves the provider identity is in network, but address provenance is weak or unavailable."

    response_address_sources = list(address_sources)
    if address_network_binding != "payer_confirmed_location":
        response_address_sources = [
            value
            for value in response_address_sources
            if value.lower().replace("-", "_") not in {"ptg", "tic", "tic_provider_group"}
        ]
    provider_directory_plan_context = (
        True
        if _provider_directory_plan_context_matched(address_payload)
        else _optional_bool_payload(address_payload.get("provider_directory_plan_context_matched"))
    )
    provider_directory_network_context_present = _optional_bool_payload(
        address_payload.get("provider_directory_network_context_present")
    )

    payload = {
        "rate_network_binding": "tic_provider_group_npi_tin",
        "address_network_binding": address_network_binding,
        "address_evidence_level": address_evidence_level,
        "requires_location_confirmation": requires_location_confirmation,
        "reason": reason,
        "network_bound_address": address_network_binding in {
            "payer_confirmed_location",
            "payer_directory_corroborated_location",
        },
    }
    optional_fields = {
        "displayed_address_present": displayed_address_present,
        "location_source": location_source,
        "location_confidence_code": location_confidence_code,
        "address_precision": address_precision,
        "address_sources": response_address_sources,
        "source_count": source_count,
        "multi_source_confirmed": multi_source_confirmed,
        "source_mask": source_mask or None,
        "address_source_mask": address_source_mask or None,
        "provider_directory_source_id": address_payload.get("provider_directory_source_id"),
        "provider_directory_location_resource_id": address_payload.get("provider_directory_location_resource_id"),
        "provider_directory_location_name": address_payload.get("provider_directory_location_name"),
        "provider_directory_plan_context_matched": provider_directory_plan_context,
        "provider_directory_network_name_matched": bool(provider_directory_network_name_matches) or None,
        "provider_directory_network_context_present": provider_directory_network_context_present,
        "provider_directory_network_refs": _coerce_str_list_payload(address_payload.get("provider_directory_network_refs")),
        "provider_directory_network_names": _coerce_str_list_payload(address_payload.get("provider_directory_network_names")),
        "provider_directory_network_matches": provider_directory_network_name_matches,
        "provider_directory_insurance_plan_refs": _coerce_str_list_payload(
            address_payload.get("provider_directory_insurance_plan_refs")
        ),
        "provider_directory_insurance_plan_matches": _coerce_str_list_payload(
            address_payload.get("provider_directory_insurance_plan_matches")
        ),
        "provider_directory_match_type": address_payload.get("provider_directory_match_type"),
        "address_verification_evidence": provider_directory_evidence,
    }
    for key, value in optional_fields.items():
        if value not in (None, "", []):
            payload[key] = value
    return payload


def _strip_no_display_address_fields(item: dict[str, Any]) -> None:
    verification = item.get("address_verification")
    if not isinstance(verification, dict) or verification.get("displayed_address_present") is not False:
        return
    for key in PTG_NO_DISPLAY_ADDRESS_FIELDS:
        item.pop(key, None)
    for key in PTG_NO_DISPLAY_VERIFICATION_FIELDS:
        verification.pop(key, None)


def _include_unverified_ptg_addresses(args: Mapping[str, Any] | dict[str, Any]) -> bool:
    return _request_bool(args.get("include_unverified_addresses"), default=True)


def _is_plan_scoped_ptg_request(args: Mapping[str, Any] | dict[str, Any]) -> bool:
    return bool(
        str(
            args.get("plan_id")
            or args.get("plan_external_id")
            or args.get("plan_market_type")
            or args.get("market_type")
            or ""
        ).strip()
    )


def _apply_ptg_address_display_policy(item: dict[str, Any], args: Mapping[str, Any] | dict[str, Any]) -> None:
    """PTG responses display inferred addresses by default, unless the caller asks to suppress them."""
    verification = item.get("address_verification")
    if not isinstance(verification, dict):
        _strip_no_display_address_fields(item)
        return
    if (
        verification.get("displayed_address_present") is True
        and verification.get("network_bound_address") is not True
        and _is_plan_scoped_ptg_request(args)
        and not _include_unverified_ptg_addresses(args)
    ):
        verification["displayed_address_present"] = False
        verification["network_bound_address"] = False
        verification["address_network_binding"] = "inferred_from_provider_identity"
        verification["requires_location_confirmation"] = True
        verification["reason"] = (
            "PTG proves the provider identity is in network, but the displayed address is not tied "
            "to the priced plan or network; address and phone fields are suppressed by request."
        )
    _strip_no_display_address_fields(item)


def _ptg2_individual_npi_exists_sql(npi_sql: str) -> str:
    return (
        f"EXISTS (SELECT 1 FROM {PTG2_SCHEMA}.npi n_entity "
        f"WHERE n_entity.npi = {npi_sql} AND COALESCE(n_entity.entity_type_code, 0) = 1)"
    )


async def _ptg2_table_has_columns(session, table_name: str, required_columns: set[str]) -> bool:
    safe_table_name = _safe_table_name(table_name)
    if not safe_table_name:
        return False
    schema_name, bare_table_name = safe_table_name.split(".", 1)
    try:
        result = await session.execute(
            text(
                """
                SELECT column_name
                  FROM information_schema.columns
                 WHERE table_schema = :schema_name
                   AND table_name = :table_name
                """
            ),
            {"schema_name": schema_name, "table_name": bare_table_name},
        )
        columns: set[str] = set()
        for row in result:
            mapping = getattr(row, "_mapping", None)
            if mapping is not None:
                value = mapping.get("column_name")
            else:
                value = row[0] if row else None
            if value:
                columns.add(str(value))
        return bool(columns) and set(required_columns).issubset(columns)
    except Exception:
        await _rollback_optional_ptg2_query(session)
        return False


async def _rollback_optional_ptg2_query(session) -> None:
    rollback = getattr(session, "rollback", None)
    if rollback is None:
        return
    try:
        result = rollback()
        if hasattr(result, "__await__"):
            await result
    except Exception:
        return


def _ptg2_row_address_key(row: dict[str, Any]) -> str | None:
    address_key = row.get("address_key")
    if address_key not in (None, ""):
        return str(address_key)
    address_payload = _coerce_json_payload(row.get("address_payload") or row.get("address"), {})
    if isinstance(address_payload, dict):
        address_key = address_payload.get("address_key")
        if address_key not in (None, ""):
            return str(address_key)
    return None


async def _ptg2_provider_directory_corroboration_table(session) -> str | None:
    if await _serving_table_available(session, PTG_PROVIDER_DIRECTORY_ADDRESS_CORROBORATION_VIEW):
        return PTG_PROVIDER_DIRECTORY_ADDRESS_CORROBORATION_VIEW
    return None


async def _overlay_provider_directory_corroboration(
    session,
    rows: list[dict[str, Any]],
    *,
    plan_id: str | None = None,
    snapshot_id: str | None = None,
    source_key: str | None = None,
) -> list[dict[str, Any]]:
    if not rows or not (plan_id or snapshot_id or source_key):
        return rows
    lookup_pairs: list[tuple[int, str]] = []
    for row in rows:
        npi = row.get("npi")
        address_key = _ptg2_row_address_key(row)
        if npi in (None, "") or not address_key:
            continue
        try:
            lookup_pairs.append((int(npi), address_key))
        except (TypeError, ValueError):
            continue
    if not lookup_pairs:
        return rows
    corroboration_table = await _ptg2_provider_directory_corroboration_table(session)
    if not corroboration_table:
        return rows
    npis = sorted({npi for npi, _address_key in lookup_pairs})
    address_keys = sorted({address_key for _npi, address_key in lookup_pairs})
    try:
        result = await session.execute(
            text(
                f"""
                SELECT DISTINCT ON (npi, address_key::text)
                    npi,
                    address_key::text AS address_key,
                    source_key,
                    snapshot_id,
                    plan_id,
                    ptg_plan_id,
                    provider_directory_source_id,
                    provider_directory_org_name,
                    provider_directory_plan_name,
                    provider_directory_provider_resource_id,
                    provider_directory_provider_name,
                    provider_directory_role_resource_id,
                    provider_directory_location_resource_id,
                    provider_directory_location_name,
                    provider_directory_telephone_number,
                    provider_directory_fax_number,
                    provider_directory_network_refs,
                    provider_directory_insurance_plan_refs,
                    provider_directory_network_names,
                    provider_directory_network_matches,
                    provider_directory_plan_context_matched,
                    provider_directory_network_context_present,
                    provider_directory_insurance_plan_matches,
                    provider_directory_match_type,
                    provider_directory_observed_at,
                    address_network_binding,
                    address_verification_evidence
                FROM {corroboration_table}
                WHERE provider_directory_active_match IS TRUE
                  AND npi = ANY(CAST(:npis AS bigint[]))
                  AND address_key::text = ANY(CAST(:address_keys AS text[]))
                  AND (:source_key IS NULL OR source_key = :source_key)
                  AND (:snapshot_id IS NULL OR snapshot_id = :snapshot_id)
                  AND (
                        :plan_id IS NULL
                     OR plan_id = :plan_id
                     OR ptg_plan_id = :plan_id
                  )
                ORDER BY npi,
                         address_key::text,
                         provider_directory_observed_at DESC NULLS LAST,
                         provider_directory_source_id
                """
            ),
            {
                "npis": npis,
                "address_keys": address_keys,
                "source_key": source_key,
                "snapshot_id": snapshot_id,
                "plan_id": plan_id,
            },
        )
    except Exception:
        await _rollback_optional_ptg2_query(session)
        return rows
    matches = {
        (int(data.get("npi")), str(data.get("address_key"))): data
        for data in (_row_mapping(row) for row in result)
        if data.get("npi") is not None and data.get("address_key")
    }
    if not matches:
        return rows
    overlaid: list[dict[str, Any]] = []
    for row in rows:
        try:
            match_key = (int(row.get("npi")), str(_ptg2_row_address_key(row)))
        except (TypeError, ValueError):
            overlaid.append(row)
            continue
        corroboration = matches.get(match_key)
        if not corroboration:
            overlaid.append(row)
            continue
        address_network_binding = (
            str(corroboration.get("address_network_binding") or "payer_directory_corroborated_location").strip()
        )
        if (
            address_network_binding == "payer_directory_corroborated_location"
            and not _coerce_bool_payload(corroboration.get("provider_directory_plan_context_matched"))
        ):
            address_network_binding = "provider_directory_address"
        updated = dict(row)
        updated["location_source"] = "provider_directory_fhir"
        updated["location_confidence_code"] = address_network_binding
        if corroboration.get("provider_directory_telephone_number"):
            updated["telephone_number"] = corroboration.get("provider_directory_telephone_number")
        if corroboration.get("provider_directory_fax_number"):
            updated["fax_number"] = corroboration.get("provider_directory_fax_number")
        address_payload = _coerce_json_payload(updated.get("address_payload") or updated.get("address"), {})
        if not isinstance(address_payload, dict):
            address_payload = {}
        address_sources = _coerce_str_list_payload(address_payload.get("address_sources"))
        if "provider_directory_fhir" not in address_sources:
            address_sources.append("provider_directory_fhir")
        address_payload.update(
            {
                "address_sources": address_sources,
                "address_network_binding": address_network_binding,
                "provider_directory_source_id": corroboration.get("provider_directory_source_id"),
                "provider_directory_org_name": corroboration.get("provider_directory_org_name"),
                "provider_directory_plan_name": corroboration.get("provider_directory_plan_name"),
                "provider_directory_provider_resource_id": corroboration.get("provider_directory_provider_resource_id"),
                "provider_directory_provider_name": corroboration.get("provider_directory_provider_name"),
                "provider_directory_role_resource_id": corroboration.get("provider_directory_role_resource_id"),
                "provider_directory_location_resource_id": corroboration.get("provider_directory_location_resource_id"),
                "provider_directory_location_name": corroboration.get("provider_directory_location_name"),
                "provider_directory_network_refs": corroboration.get("provider_directory_network_refs"),
                "provider_directory_insurance_plan_refs": corroboration.get("provider_directory_insurance_plan_refs"),
                "provider_directory_network_names": corroboration.get("provider_directory_network_names"),
                "provider_directory_network_matches": corroboration.get("provider_directory_network_matches"),
                "provider_directory_plan_context_matched": corroboration.get("provider_directory_plan_context_matched"),
                "provider_directory_network_context_present": corroboration.get("provider_directory_network_context_present"),
                "provider_directory_insurance_plan_matches": corroboration.get("provider_directory_insurance_plan_matches"),
                "provider_directory_match_type": corroboration.get("provider_directory_match_type"),
                "address_verification_evidence": corroboration.get("address_verification_evidence"),
            }
        )
        if updated.get("telephone_number"):
            address_payload["telephone_number"] = updated.get("telephone_number")
        if updated.get("fax_number"):
            address_payload["fax_number"] = updated.get("fax_number")
        updated["address_payload"] = address_payload
        overlaid.append(updated)
    return overlaid


async def _ptg2_address_serving_table(
    session,
    required_columns: set[str],
    *,
    require_legacy_available: bool = False,
) -> str | None:
    legacy_table = f"{PTG2_SCHEMA}.npi_address"
    if _address_serving_unified_requested():
        unified_table = f"{PTG2_SCHEMA}.entity_address_unified"
        if await _ptg2_table_has_columns(session, unified_table, required_columns):
            return unified_table
    if require_legacy_available and not await _serving_table_available(session, legacy_table):
        return None
    return legacy_table


def _inferred_provider_taxonomy_rule(args: dict[str, Any]) -> InferredProviderTaxonomyRule | None:
    requested_system = _normalize_code_system(args.get("code_system") or args.get("reported_code_system"))
    requested_code = _normalize_code(args.get("code") or args.get("reported_code"))
    # api-layer/OpenAPI compatibility paths can arrive with only a 5-digit CPT code.
    # Keep the default narrow so short numeric revenue codes such as "450" do not
    # accidentally inherit CPT taxonomy inference.
    if not requested_system and requested_code and requested_code.isdigit() and len(requested_code) == 5:
        requested_system = "CPT"
    if requested_system not in EQUIVALENT_PROCEDURE_CODE_SYSTEMS or not requested_code or not requested_code.isdigit():
        return None
    code_value = int(requested_code)
    return next((rule for rule in INFERRED_PROVIDER_TAXONOMY_RULES if rule.matches(code_value)), None)


def _inferred_provider_taxonomy_code_sql(
    args: dict[str, Any],
    *,
    nt_alias: str,
    schema: str,
    params: dict[str, Any],
    param_prefix: str,
) -> str:
    rule = _inferred_provider_taxonomy_rule(args)
    if rule is None:
        return ""
    code_placeholders = []
    for idx, taxonomy_code in enumerate(rule.taxonomy_codes):
        key = f"{param_prefix}_code_{idx}"
        params[key] = taxonomy_code
        code_placeholders.append(f":{key}")
    clauses = []
    if code_placeholders:
        clauses.append(f"{nt_alias}.healthcare_provider_taxonomy_code IN ({', '.join(code_placeholders)})")
    return "(" + " OR ".join(clauses) + ")" if clauses else ""


def _ptg2_manifest_storage_enabled(serving_tables: PTG2ServingTables) -> bool:
    return (serving_tables.storage or "").strip().lower() == "manifest_snapshot"


def _shape_ptg2_manifest_response(payload: dict[str, Any], args: dict[str, Any]) -> dict[str, Any]:
    manifest_payload = dict(payload)
    manifest_payload["query"] = {
        key: value for key, value in dict(payload.get("query") or {}).items() if key != "result_granularity"
    }
    manifest_payload["items"] = []
    for item in payload.get("items", []):
        shaped_item = dict(item)
        shaped_item.pop("service_code", None)
        shaped_item.pop("service_code_system", None)
        shaped_item.pop("tic_prices", None)
        manifest_payload["items"].append(shaped_item)
    return _shape_ptg2_response(manifest_payload, args)


def _ptg2_manifest_id_array_cast(serving_tables: PTG2ServingTables) -> str:
    return "uuid[]" if serving_tables.uses_uuid_ids else "char(32)[]"


def _ptg2_manifest_id(value: Any) -> str:
    return _uuid_to_hex(value)


def _ptg2_manifest_ids(values: list[Any] | tuple[Any, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(hex_value for value in values if (hex_value := _ptg2_manifest_id(value))))


def _append_manifest_reported_code_filter(
    filters: list[str],
    params: dict[str, Any],
    *,
    code: Any,
    code_system: Any,
    code_context: dict[str, Any] | None = None,
) -> None:
    requested_system = _normalize_code_system(code_system)
    requested_code = canonical_catalog_code(requested_system, code) if requested_system else _normalize_code(code)
    if not requested_code:
        return
    external_pairs: set[tuple[str, str]] = set()
    if code_context:
        for resolved_code in code_context.get("resolved_codes") or []:
            system = _normalize_code_system(resolved_code.get("code_system"))
            value = canonical_catalog_code(system, resolved_code.get("code")) if system else _normalize_code(resolved_code.get("code"))
            if system and value and system != INTERNAL_PROCEDURE_CODE_SYSTEM:
                external_pairs.add((system, value))
    if not external_pairs and requested_system and requested_system != INTERNAL_PROCEDURE_CODE_SYSTEM:
        external_pairs.add((requested_system, requested_code))
    if not external_pairs and not requested_system:
        params["reported_code"] = requested_code
        filters.append("reported_code = :reported_code")
        return
    if not external_pairs:
        filters.append("FALSE")
        return
    clauses: list[str] = []
    for idx, (system, value) in enumerate(sorted(external_pairs)):
        system_key = f"reported_code_system_{idx}"
        code_key = f"reported_code_{idx}"
        params[system_key] = system
        params[code_key] = value
        clauses.append(f"(reported_code_system = :{system_key} AND reported_code = :{code_key})")
    filters.append("(" + " OR ".join(clauses) + ")")


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def search_ptg2_index(
    index: PTG2ServingIndex,
    *,
    plan_id: str | None = None,
    plan_external_id: str | None = None,
    code: str | None = None,
    state: str | None = None,
    city: str | None = None,
    zip5: str | None = None,
    npi: int | str | None = None,
) -> dict[str, Any]:
    requested_plan = str(plan_id or plan_external_id or "").strip()
    requested_code = str(code or "").strip()
    requested_state = str(state or "").strip().upper()
    requested_city = str(city or "").strip().upper()
    requested_zip = _normalize_zip5(zip5)
    requested_npi = _normalize_npi(npi)
    plan_rates = dict(index.rates.get(requested_plan) or {}) if requested_plan else {}
    rate_rows = list(plan_rates.get(requested_code) or []) if requested_code else []
    items: list[dict[str, Any]] = []
    for rate in rate_rows:
        provider = _provider_payload(index, rate.get("provider_ordinal"))
        if requested_state and str(provider.get("state") or "").upper() != requested_state:
            continue
        if requested_city and str(provider.get("city") or "").upper() != requested_city:
            continue
        if requested_zip and _normalize_zip5(provider.get("zip5")) != requested_zip:
            continue
        if requested_npi is not None and _normalize_npi(provider.get("npi")) != requested_npi:
            continue
        procedure = dict(index.procedures.get(requested_code) or {})
        prices = _normalize_price_payload(rate.get("prices") or [])
        item = {
            **provider,
            "procedure_code": procedure.get("procedure_code") or procedure.get("code") or requested_code,
            "service_code": procedure.get("billing_code") or requested_code,
            "reported_code": procedure.get("billing_code") or requested_code,
            "reported_code_system": procedure.get("billing_code_type"),
            "billing_code": procedure.get("billing_code") or requested_code,
            "billing_code_type": procedure.get("billing_code_type"),
            "procedure_name": procedure.get("name") or procedure.get("procedure_name"),
            "procedure_description": procedure.get("description") or procedure.get("procedure_description"),
            **_price_response_fields(prices),
            "source_trace": _coerce_json_payload(rate.get("source_trace"), []),
            "network_names": _coerce_str_list_payload(rate.get("network_names")),
            "confidence": rate.get("confidence") or {"network": "tic_rate_npi_tin"},
        }
        address_payload = _coerce_json_payload(
            _first_payload_value(provider.get("address_payload"), provider.get("address")),
            {},
        )
        if not isinstance(address_payload, dict):
            address_payload = {}
        if address_payload and not isinstance(item.get("address"), dict):
            item["address"] = address_payload
        _add_location_phone_fields(item, provider, address_payload)
        _promote_address_provenance_fields(item, address_payload)
        item["address_verification"] = _address_verification_payload(item, provider, address_payload)
        _strip_no_display_address_fields(item)
        items.append(item)
    return _shape_ptg2_response(
        {
            "items": items,
            "pagination": {"total": len(items), "limit": len(items), "offset": 0, "page": 1},
            "query": {
                "plan_id": requested_plan or None,
                "plan_external_id": plan_external_id or None,
                "code": requested_code or None,
                "state": state or None,
                "city": city or None,
                "zip5": zip5 or None,
                "npi": requested_npi,
                "snapshot_id": index.snapshot_id,
                "source": "ptg2",
            },
        },
        {"include_sources": "true", "include_details": "true"},
    )


def warm_cache_benchmark(index: PTG2ServingIndex, request_count: int = 100) -> dict[str, Any]:
    request_count = max(int(request_count or 1), 1)
    plan_id = next(iter(index.rates.keys()), "")
    code = next(iter(dict(index.rates.get(plan_id) or {}).keys()), "")
    timings: list[float] = []
    for _ in range(request_count):
        started = time.perf_counter()
        search_ptg2_index(index, plan_id=plan_id, code=code)
        timings.append((time.perf_counter() - started) * 1000.0)
    sorted_timings = sorted(timings)
    p95_index = min(max(math.ceil(0.95 * len(sorted_timings)) - 1, 0), len(sorted_timings) - 1)
    p95_ms = sorted_timings[p95_index]
    return {"request_count": request_count, "p95_ms": p95_ms, "passed": p95_ms <= PTG2_WARM_P95_MAX_MS}


def _ptg2_manifest_artifact_entries(serving_tables: PTG2ServingTables, name: str) -> list[dict[str, Any]]:
    artifacts = serving_tables.artifacts or {}
    entries: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    def add_entry(value: Any) -> None:
        if not isinstance(value, dict):
            return
        path = str(value.get("path") or "").strip()
        sha = str(value.get("sha256") or "").strip()
        key = (path, sha)
        if key in seen:
            return
        seen.add(key)
        entries.append(dict(value))

    value = artifacts.get(name)
    add_entry(value)
    for sidecar in artifacts.get("sidecars") or []:
        if isinstance(sidecar, dict) and sidecar.get("name") == name:
            add_entry(sidecar)
    return entries


def _ptg2_manifest_artifact_entry(serving_tables: PTG2ServingTables, name: str) -> dict[str, Any] | None:
    entries = _ptg2_manifest_artifact_entries(serving_tables, name)
    return entries[0] if entries else None


def _resolve_ptg2_manifest_sidecar_path(raw_path: str) -> Path:
    path = Path(raw_path)
    if path.exists():
        return path
    artifact_root = _artifact_root()
    if not path.is_absolute():
        candidate = artifact_root / path
        return candidate if candidate.exists() else path

    marker = "healthporta-ptg2-artifacts"
    try:
        marker_index = path.parts.index(marker)
    except ValueError:
        return path
    suffix_parts = path.parts[marker_index + 1 :]
    if not suffix_parts:
        return path
    candidate = artifact_root.joinpath(*suffix_parts)
    if candidate.exists():
        return candidate

    if len(suffix_parts) >= 3 and suffix_parts[0] == "serving":
        filename = suffix_parts[-1]
        serving_root = artifact_root / "serving"
        if serving_root.exists():
            matches = [item for item in serving_root.glob(f"*/{filename}") if item.exists()]
            if len(matches) == 1:
                return matches[0]
            prefix = filename.rsplit("_", 1)[0]
            if prefix and prefix != filename:
                prefix_matches = [
                    item for item in serving_root.glob(f"*/{prefix}_*.ptg2sc") if item.exists()
                ]
                if len(prefix_matches) == 1:
                    return prefix_matches[0]
    return path


def _ptg2_manifest_sidecar_members(
    serving_tables: PTG2ServingTables,
    name: str,
    owner_id: str,
    *,
    max_members: int | None = None,
) -> tuple[str, ...]:
    owner_id = _ptg2_manifest_id(owner_id)
    if not owner_id:
        return ()
    members: set[str] = set()
    for entry in _ptg2_manifest_artifact_entries(serving_tables, name):
        path = str(entry.get("path") or "").strip()
        if not path:
            continue
        sidecar_path = _resolve_ptg2_manifest_sidecar_path(path)
        sidecar_metadata = entry if sidecar_path == Path(path) else None
        cache_owner_id = owner_id if max_members is None else f"{owner_id}:{max_members}"
        cache_key = (str(sidecar_path), str(entry.get("sha256") or ""), cache_owner_id)
        cached = _PTG2_MANIFEST_SIDECAR_CACHE.get(cache_key)
        if cached is None:
            cached = tuple(
                member.hex()
                for member in lookup_global_sidecar_members(
                    sidecar_path,
                    owner_id,
                    metadata=sidecar_metadata,
                    max_members=max_members,
                )
            )
            _PTG2_MANIFEST_SIDECAR_CACHE[cache_key] = cached
        members.update(cached)
    return tuple(sorted(members))


def _ptg2_manifest_sidecar_members_many(
    serving_tables: PTG2ServingTables,
    name: str,
    owner_ids: list[str] | tuple[str, ...],
    *,
    max_members: int | None = None,
) -> dict[str, tuple[str, ...]]:
    owner_id_list = list(_ptg2_manifest_ids(tuple(owner_ids)))
    result_sets: dict[str, set[str]] = {owner_id: set() for owner_id in owner_id_list}
    for entry in _ptg2_manifest_artifact_entries(serving_tables, name):
        path = str(entry.get("path") or "").strip()
        if not path:
            continue
        sidecar_path = _resolve_ptg2_manifest_sidecar_path(path)
        sidecar_metadata = entry if sidecar_path == Path(path) else None
        sha = str(entry.get("sha256") or "")
        missing: list[str] = []
        for owner_id in owner_id_list:
            cache_owner_id = owner_id if max_members is None else f"{owner_id}:{max_members}"
            cache_key = (str(sidecar_path), sha, cache_owner_id)
            cached = _PTG2_MANIFEST_SIDECAR_CACHE.get(cache_key)
            if cached is None:
                missing.append(owner_id)
            else:
                result_sets[owner_id].update(cached)
        if missing:
            members_by_owner = lookup_global_sidecar_members_many(
                sidecar_path,
                missing,
                metadata=sidecar_metadata,
                max_members=max_members,
            )
            for owner_id in missing:
                try:
                    owner_bytes = bytes.fromhex(owner_id)
                except ValueError:
                    owner_bytes = b""
                members = tuple(member.hex() for member in members_by_owner.get(owner_bytes, ()))
                cache_owner_id = owner_id if max_members is None else f"{owner_id}:{max_members}"
                _PTG2_MANIFEST_SIDECAR_CACHE[(str(sidecar_path), sha, cache_owner_id)] = members
                result_sets[owner_id].update(members)
    return {owner_id: tuple(sorted(members)) for owner_id, members in result_sets.items()}


def _ptg2_manifest_provider_npis_for_provider_set(
    serving_tables: PTG2ServingTables,
    provider_set_global_id: str,
) -> tuple[int, ...]:
    members = _ptg2_manifest_sidecar_members(serving_tables, "provider_npi", provider_set_global_id)
    npis: list[int] = []
    for member in members:
        try:
            raw = bytes.fromhex(member)
        except ValueError:
            continue
        if len(raw) != 16:
            continue
        npi = int.from_bytes(raw[8:16], "big", signed=False)
        if npi > 0:
            npis.append(npi)
    return tuple(sorted(set(npis)))


def _ptg2_manifest_provider_npis_for_provider_sets(
    serving_tables: PTG2ServingTables,
    provider_set_global_ids: list[str] | tuple[str, ...],
    *,
    limit_per_set: int | None = None,
) -> dict[str, tuple[int, ...]]:
    provider_set_ids = _ptg2_manifest_ids(tuple(provider_set_global_ids))
    members_by_set = _ptg2_manifest_sidecar_members_many(
        serving_tables,
        "provider_npi",
        provider_set_ids,
        max_members=limit_per_set,
    )
    result: dict[str, tuple[int, ...]] = {}
    for provider_set_id in provider_set_ids:
        npis: list[int] = []
        for member in members_by_set.get(provider_set_id, ()):
            try:
                raw = bytes.fromhex(member)
            except ValueError:
                continue
            if len(raw) != 16:
                continue
            npi = int.from_bytes(raw[8:16], "big", signed=False)
            if npi > 0:
                npis.append(npi)
        sorted_npis = tuple(sorted(set(npis)))
        result[provider_set_id] = sorted_npis[:limit_per_set] if limit_per_set is not None else sorted_npis
    return result


def _ptg2_manifest_price_matches_filter(price: dict[str, Any], args: dict[str, Any]) -> bool:
    service_codes = _normalize_filter_string_list(
        args.get("pos") or args.get("place_of_service") or args.get("service_code"),
        code_system="POS",
    )
    if service_codes:
        price_service_codes = {
            _canonical_catalog_code("POS", code)
            for code in _normalize_string_list(price.get("service_code"))
            if code not in (None, "", "null")
        }
        if not price_service_codes.intersection(service_codes):
            return False

    modifier_codes = _normalize_filter_string_list(
        args.get("modifier") or args.get("modifiers") or args.get("billing_code_modifier"),
        upper=True,
    )
    if modifier_codes:
        price_modifiers = {modifier.upper() for modifier in _normalize_string_list(price.get("billing_code_modifier"))}
        if price_modifiers != set(modifier_codes):
            return False

    requested_rate = _optional_decimal(args.get("rate") or args.get("negotiated_rate"))
    if requested_rate is not None:
        price_rate = _optional_decimal(price.get("negotiated_rate"))
        if price_rate is None:
            return False
        tolerance = _optional_decimal(args.get("rate_tolerance") or args.get("negotiated_rate_tolerance"))
        if tolerance is None:
            tolerance = _optional_decimal("0.01")
        if tolerance is None or abs(price_rate - requested_rate) > tolerance:
            return False

    return True


def _ptg2_manifest_filter_prices(prices: list[dict[str, Any]], args: dict[str, Any]) -> list[dict[str, Any]]:
    if not (
        args.get("pos")
        or args.get("place_of_service")
        or args.get("service_code")
        or args.get("modifier")
        or args.get("modifiers")
        or args.get("billing_code_modifier")
        or args.get("rate")
        or args.get("negotiated_rate")
    ):
        return prices
    return [price for price in prices if _ptg2_manifest_price_matches_filter(price, args)]


async def _ptg2_manifest_prices_for_price_set(
    session,
    serving_tables: PTG2ServingTables,
    price_set_global_id: str,
) -> list[dict[str, Any]]:
    price_atom_table = _safe_table_name(serving_tables.price_atom_table)
    price_set_global_id = _ptg2_manifest_id(price_set_global_id)
    if not price_atom_table or not price_set_global_id:
        return []
    price_members = _ptg2_manifest_sidecar_members(serving_tables, "price_forward", price_set_global_id)
    if not price_members:
        return []
    atom_ids = list(price_members)
    atom_array_cast = _ptg2_manifest_id_array_cast(serving_tables)
    stmt = text(
        f"""
            SELECT
                price_atom_global_id_128,
                negotiated_type,
                negotiated_rate,
                expiration_date,
                service_code,
                billing_class,
                setting,
                billing_code_modifier,
                additional_information
            FROM {price_atom_table}
            WHERE price_atom_global_id_128 = ANY(CAST(:atom_ids AS {atom_array_cast}))
            """
    )
    result = await session.execute(stmt, {"atom_ids": atom_ids})
    rows_by_id = {_ptg2_manifest_id(_row_mapping(row).get("price_atom_global_id_128")): _row_mapping(row) for row in result}
    prices: list[dict[str, Any]] = []
    for atom_id in atom_ids:
        row = rows_by_id.get(atom_id)
        if not row:
            continue
        prices.append(
            {
                "negotiated_type": row.get("negotiated_type"),
                "negotiated_rate": row.get("negotiated_rate"),
                "expiration_date": row.get("expiration_date"),
                "service_code": row.get("service_code") or [],
                "billing_class": row.get("billing_class"),
                "setting": row.get("setting"),
                "billing_code_modifier": row.get("billing_code_modifier") or [],
                "additional_information": row.get("additional_information"),
            }
        )
    return prices


async def _ptg2_manifest_prices_for_price_sets(
    session,
    serving_tables: PTG2ServingTables,
    price_set_global_ids: list[str] | tuple[str, ...],
) -> dict[str, list[dict[str, Any]]]:
    price_atom_table = _safe_table_name(serving_tables.price_atom_table)
    price_set_ids = _ptg2_manifest_ids(tuple(price_set_global_ids))
    if not price_atom_table or not price_set_ids:
        return {price_set_id: [] for price_set_id in price_set_ids}
    members_by_price_set = _ptg2_manifest_sidecar_members_many(serving_tables, "price_forward", price_set_ids)
    atom_ids = tuple(dict.fromkeys(atom_id for atoms in members_by_price_set.values() for atom_id in atoms))
    if not atom_ids:
        return {price_set_id: [] for price_set_id in price_set_ids}
    atom_array_cast = _ptg2_manifest_id_array_cast(serving_tables)
    stmt = text(
        f"""
            SELECT
                price_atom_global_id_128,
                negotiated_type,
                negotiated_rate,
                expiration_date,
                service_code,
                billing_class,
                setting,
                billing_code_modifier,
                additional_information
            FROM {price_atom_table}
            WHERE price_atom_global_id_128 = ANY(CAST(:atom_ids AS {atom_array_cast}))
            """
    )
    result = await session.execute(stmt, {"atom_ids": atom_ids})
    rows_by_id = {_ptg2_manifest_id(_row_mapping(row).get("price_atom_global_id_128")): _row_mapping(row) for row in result}
    prices_by_set: dict[str, list[dict[str, Any]]] = {}
    for price_set_id in price_set_ids:
        prices: list[dict[str, Any]] = []
        for atom_id in members_by_price_set.get(price_set_id, ()):
            row = rows_by_id.get(atom_id)
            if not row:
                continue
            prices.append(
                {
                    "negotiated_type": row.get("negotiated_type"),
                    "negotiated_rate": row.get("negotiated_rate"),
                    "expiration_date": row.get("expiration_date"),
                    "service_code": row.get("service_code") or [],
                    "billing_class": row.get("billing_class"),
                    "setting": row.get("setting"),
                    "billing_code_modifier": row.get("billing_code_modifier") or [],
                    "additional_information": row.get("additional_information"),
                }
            )
        prices_by_set[price_set_id] = prices
    return prices_by_set


async def _ptg2_manifest_taxonomy_rows_for_npis(
    session,
    npis: list[int] | tuple[int, ...],
) -> dict[int, dict[str, Any]]:
    npis = tuple(sorted({int(npi) for npi in npis if int(npi) > 0}))
    if not npis:
        return {}
    result = await session.execute(
        text(
            f"""
            SELECT
                source_npis.npi,
                COALESCE(tax.taxonomy_codes, ARRAY[]::varchar[]) AS taxonomy_codes,
                COALESCE(tax.specialties, ARRAY[]::varchar[]) AS specialties,
                COALESCE(tax.classifications, ARRAY[]::varchar[]) AS classifications,
                COALESCE(tax.specializations, ARRAY[]::varchar[]) AS specializations,
                tax.primary_specialty,
                tax.primary_specialization
            FROM (SELECT UNNEST(CAST(:npis AS bigint[])) AS npi) source_npis
            {_provider_taxonomy_summary_lateral_sql("source_npis.npi")}
            """
        ),
        {"npis": list(npis)},
    )
    taxonomy_by_npi: dict[int, dict[str, Any]] = {}
    for row in result:
        data = _row_mapping(row)
        npi = data.get("npi")
        if npi is None:
            continue
        taxonomy_by_npi[int(npi)] = {
            "taxonomy_codes": data.get("taxonomy_codes") or [],
            "specialties": data.get("specialties") or [],
            "classifications": data.get("classifications") or [],
            "specializations": data.get("specializations") or [],
            "primary_specialty": data.get("primary_specialty"),
            "primary_specialization": data.get("primary_specialization"),
        }
    return taxonomy_by_npi


async def _ptg2_manifest_enriched_provider_rows_for_npis(
    session,
    *,
    npis: list[int] | tuple[int, ...],
    limit: int,
    plan_id: str | None = None,
    snapshot_id: str | None = None,
    source_key: str | None = None,
) -> list[dict[str, Any]] | None:
    npis = tuple(sorted({int(npi) for npi in npis if int(npi) > 0}))[:limit]
    if not npis:
        return []
    npi_data_table = f"{PTG2_SCHEMA}.npi"
    npi_address_table = await _ptg2_address_serving_table(
        session,
        _PTG2_LEGACY_ADDRESS_COLUMNS,
        require_legacy_available=True,
    )
    if not await _serving_table_available(session, npi_data_table) or not npi_address_table:
        taxonomy_by_npi = await _ptg2_manifest_taxonomy_rows_for_npis(session, npis)
        return [
            {
                "npi": npi,
                "provider_name": "TiC provider",
                "taxonomy_codes": taxonomy_by_npi.get(npi, {}).get("taxonomy_codes", []),
                "specialties": taxonomy_by_npi.get(npi, {}).get("specialties", []),
                "classifications": taxonomy_by_npi.get(npi, {}).get("classifications", []),
                "specializations": taxonomy_by_npi.get(npi, {}).get("specializations", []),
                "primary_specialty": taxonomy_by_npi.get(npi, {}).get("primary_specialty"),
                "primary_specialization": taxonomy_by_npi.get(npi, {}).get("primary_specialization"),
            }
            for npi in npis
        ]
    address_location_source = _ptg2_address_location_source(npi_address_table)
    address_location_hash_sql = _ptg2_address_location_hash_sql("addr", npi_address_table)
    using_unified_enrich = _is_unified_address_table(npi_address_table)
    # Same street-fill as the location search: when the unified row has no
    # first_line (or no row at all), fall back to the NPPES npi_address row.
    if using_unified_enrich:
        enrich_address_fallback_cte = f"""
            , fallback_addresses AS MATERIALIZED (
                SELECT DISTINCT ON (na.npi)
                       na.npi, na.first_line, na.second_line, na.city_name, na.state_name,
                       na.postal_code, na.country_code, na.telephone_number, na.fax_number,
                       na.lat, na.long
                  FROM {PTG2_SCHEMA}.npi_address na
                  JOIN source_npis source_filter ON source_filter.npi = na.npi
                 WHERE NULLIF(BTRIM(na.first_line), '') IS NOT NULL
                 ORDER BY na.npi,
                          CASE na.type WHEN 'primary' THEN 0 WHEN 'practice' THEN 1
                                       WHEN 'secondary' THEN 2 ELSE 3 END,
                          na.checksum
            )"""
        enrich_address_fallback_join = """
            LEFT JOIN fallback_addresses na
              ON na.npi = source_npis.npi
             AND NULLIF(BTRIM(addr.first_line), '') IS NULL"""

        def _eff_enrich(column: str) -> str:
            cast_suffix = "::numeric" if column in {"lat", "long"} else ""
            return (
                "CASE WHEN NULLIF(BTRIM(addr.first_line), '') IS NULL "
                f"AND na.first_line IS NOT NULL THEN na.{column}{cast_suffix} ELSE addr.{column}{cast_suffix} END"
            )
    else:
        enrich_address_fallback_cte = ""
        enrich_address_fallback_join = ""

        def _eff_enrich(column: str) -> str:
            return f"addr.{column}"
    enrich_stmt = (
        text(
            f"""
            WITH source_npis AS MATERIALIZED (
                SELECT UNNEST(CAST(:npis AS bigint[])) AS npi
            )
            {enrich_address_fallback_cte}
            SELECT
                source_npis.npi,
                {address_location_hash_sql} AS location_hash,
                {_eff_enrich('state_name')} AS state,
                {_eff_enrich('city_name')} AS city,
                LEFT(COALESCE({_eff_enrich('postal_code')}, ''), 5) AS zip5,
                '{address_location_source}' AS location_source,
                '{address_location_source}' AS location_confidence_code,
                json_build_object(
                    'first_line', {_eff_enrich('first_line')},
                    'second_line', {_eff_enrich('second_line')},
                    'city', {_eff_enrich('city_name')},
                    'state', {_eff_enrich('state_name')},
                    'postal_code', {_eff_enrich('postal_code')},
                    'country_code', {_eff_enrich('country_code')},
                    'telephone_number', {_eff_enrich('telephone_number')},
                    'fax_number', {_eff_enrich('fax_number')},
                    'address_key', addr.address_key::text,
                    'lat', {_eff_enrich('lat')},
                    'long', {_eff_enrich('long')}
                )::text AS address_payload,
                {_eff_enrich('telephone_number')} AS telephone_number,
                {_eff_enrich('fax_number')} AS fax_number,
                COALESCE(tax.taxonomy_codes, ARRAY[]::varchar[]) AS taxonomy_codes,
                COALESCE(tax.specialties, ARRAY[]::varchar[]) AS specialties,
                COALESCE(tax.classifications, ARRAY[]::varchar[]) AS classifications,
                COALESCE(tax.specializations, ARRAY[]::varchar[]) AS specializations,
                tax.primary_specialty,
                tax.primary_specialization,
                {_ptg2_provider_name_sql("n")} AS provider_name
            FROM source_npis
            LEFT JOIN {npi_data_table} n ON n.npi = source_npis.npi
            LEFT JOIN LATERAL (
                SELECT addr.*
                  FROM {npi_address_table} addr
                 WHERE addr.npi = source_npis.npi
                 ORDER BY (addr.type = 'primary') DESC, addr.type, addr.checksum
                 LIMIT 1
            ) addr ON TRUE
            {enrich_address_fallback_join}
            {_provider_taxonomy_summary_lateral_sql("source_npis.npi")}
            ORDER BY provider_name, source_npis.npi
            """
        )
    )
    result = await session.execute(enrich_stmt, {"npis": npis})
    rows = [_row_mapping(row) for row in result]
    return await _overlay_provider_directory_corroboration(
        session,
        rows,
        plan_id=plan_id,
        snapshot_id=snapshot_id,
        source_key=source_key,
    )


def _ptg2_provider_taxonomy_filter_requested(args: dict[str, Any]) -> bool:
    if resolve_provider_specialty_filter(args).active:
        return True
    return _inferred_provider_taxonomy_rule(args) is not None


def _ptg2_manifest_rate_candidate_limit(
    args: dict[str, Any],
    pagination,
    *,
    expand_providers: bool,
    location_filter_requested: bool,
) -> int:
    requested_limit = max(int(pagination.limit), 1)
    requested_offset = max(int(getattr(pagination, "offset", 0) or 0), 0)
    if expand_providers and location_filter_requested:
        # Bound the nearby-candidate pool the location expansion materializes.
        # The downstream provider_group_member fan-out + per-row enrichment cost
        # scales with this pool, so a dense metro (thousands of in-radius
        # in-network providers) used to blow past the request timeout at the old
        # limit*200 (=2000-4000) fetch. limit*40 (floored at 500, env-capped)
        # keeps a generous nearest-first pool -- results stay "cheapest among the
        # N nearest" -- while staying well under the wall in the densest metros.
        return min(
            _ptg2_manifest_location_match_limit(),
            max(requested_limit * 40, requested_offset + requested_limit, 500),
        )
    if expand_providers and not location_filter_requested and _ptg2_provider_taxonomy_filter_requested(args):
        return min(
            _PTG2_MANIFEST_TAXONOMY_RATE_CANDIDATE_LIMIT,
            max(requested_limit, requested_offset + requested_limit, requested_limit * 5, 5),
        )
    return requested_limit


def _ptg2_provider_price_sort_value(item: dict[str, Any]) -> float:
    rates: list[float] = []
    for price in _coerce_json_payload(item.get("prices"), []):
        if not isinstance(price, dict):
            continue
        rate = _coerce_numeric_rate(price.get("negotiated_rate"))
        if rate is not None:
            rates.append(float(rate))
    for summary in _coerce_json_payload(item.get("price_summary"), []):
        if not isinstance(summary, dict):
            continue
        rate = _coerce_numeric_rate(summary.get("rate"))
        if rate is not None:
            rates.append(float(rate))
    return min(rates) if rates else math.inf


def _ptg2_provider_distance_sort_value(item: dict[str, Any]) -> float:
    value = _optional_float(item.get("distance_miles"))
    return value if value is not None else math.inf


def _ptg2_address_verification_sort_value(item: dict[str, Any]) -> int:
    verification = item.get("address_verification")
    if isinstance(verification, dict):
        if verification.get("network_bound_address") is True:
            return 0
        if verification.get("displayed_address_present") is True:
            return 1
    return 2


def _sort_ptg2_manifest_provider_items(
    items: list[dict[str, Any]],
    args: dict[str, Any],
    *,
    location_filter_requested: bool,
) -> list[dict[str, Any]]:
    order_by = str(args.get("order_by") or "").strip().lower()
    order = str(args.get("order") or "").strip().lower()
    descending = order == "desc"
    cost_order_fields = {
        "total_allowed_amount",
        "total_drug_cost",
        "cost",
        "price",
        "rate",
        "negotiated_rate",
        "amount",
    }
    distance_order_fields = {"distance", "distance_miles"}
    if order_by in cost_order_fields:
        return sorted(
            items,
            key=lambda item: (
                _ptg2_provider_price_sort_value(item),
                _ptg2_provider_distance_sort_value(item),
                str(item.get("provider_name") or ""),
                str(item.get("npi") or ""),
            ),
            reverse=descending,
        )
    if order_by in distance_order_fields or (location_filter_requested and not order_by):
        return sorted(
            items,
            key=lambda item: (
                _ptg2_provider_distance_sort_value(item),
                _ptg2_provider_price_sort_value(item),
                str(item.get("provider_name") or ""),
                str(item.get("npi") or ""),
            ),
            reverse=descending,
        )
    return sorted(
        items,
        key=lambda item: (
            _ptg2_address_verification_sort_value(item),
            _ptg2_provider_price_sort_value(item),
            _ptg2_provider_distance_sort_value(item),
            str(item.get("provider_name") or ""),
            str(item.get("npi") or ""),
        ),
    )


def _ptg2_manifest_provider_procedure_item(
    *,
    npi: int,
    data: dict[str, Any],
    prices: list[dict[str, Any]],
    procedure_detail: dict[str, Any],
    provider_context: dict[str, Any] | None,
    args: dict[str, Any],
) -> dict[str, Any]:
    reported_code = data.get("reported_code")
    reported_system = data.get("reported_code_system")
    provider_set_hash = _ptg2_manifest_id(data.get("provider_set_global_id_128"))
    price_set_hash = _ptg2_manifest_id(data.get("price_set_global_id_128"))
    rate_pack_hash = _ptg2_manifest_id(data.get("serving_content_hash_128"))
    item_data = dict(provider_context or {})
    item_data.update(
        {
            "npi": npi,
            "provider_set_hash": provider_set_hash,
            "provider_count": data.get("provider_count") or 0,
            "provider_set_count": 1 if provider_set_hash else 0,
            "network_names": _coerce_str_list_payload(data.get("network_names")),
            "procedure_code": reported_code,
            "procedure_name": procedure_detail.get("procedure_name"),
            "procedure_description": procedure_detail.get("procedure_description"),
            "reported_code": reported_code,
            "reported_code_system": reported_system,
            "billing_code": reported_code,
            "billing_code_type": reported_system,
            "prices": prices,
            "price_set_hash": price_set_hash,
            "rate_pack_hash": rate_pack_hash,
        }
    )
    return _compact_item_from_row(item_data, args)


def _ptg2_provider_rate_group_key(item: dict[str, Any]) -> tuple[str, str, str, str] | None:
    npi = item.get("npi")
    if npi in (None, ""):
        return None
    address_payload = _coerce_json_payload(item.get("address"), {})
    if not isinstance(address_payload, dict):
        address_payload = {}
    location_key = (
        item.get("location_hash")
        or address_payload.get("address_key")
        or address_payload.get("premise_key")
    )
    if not location_key:
        location_key = "|".join(
            str(
                address_payload.get(key)
                or item.get(key)
                or ""
            ).strip().upper()
            for key in ("first_line", "second_line", "city", "state", "zip5")
        )
    reported_system = (
        item.get("reported_code_system")
        or item.get("service_code_system")
        or item.get("billing_code_type")
        or ""
    )
    reported_code = (
        item.get("reported_code")
        or item.get("service_code")
        or item.get("billing_code")
        or ""
    )
    return str(npi), str(location_key), str(reported_system), str(reported_code)


def _append_unique_value(values: list[Any], value: Any) -> None:
    if value in (None, ""):
        return
    if value not in values:
        values.append(value)


def _merge_unique_payload_list(target: dict[str, Any], field: str, value: Any) -> None:
    payload = _coerce_json_payload(value, [])
    if payload in (None, ""):
        return
    if not isinstance(payload, list):
        payload = [payload]
    target_values = target.setdefault(field, [])
    if not isinstance(target_values, list):
        target_values = [target_values]
        target[field] = target_values
    seen = {_price_row_key(item) if isinstance(item, dict) else str(item) for item in target_values}
    for item in payload:
        if item in (None, ""):
            continue
        key = _price_row_key(item) if isinstance(item, dict) else str(item)
        if key in seen:
            continue
        seen.add(key)
        target_values.append(item)


def _merge_ptg2_provider_rate_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse duplicate provider/location rows while preserving every rate option."""
    merged: list[dict[str, Any]] = []
    grouped: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for item in items:
        group_key = _ptg2_provider_rate_group_key(item)
        if group_key is None:
            merged.append(item)
            continue
        existing = grouped.get(group_key)
        if existing is None:
            existing = dict(item)
            existing["prices"] = _normalize_price_payload(existing.get("prices"))
            existing["tic_prices"] = list(existing["prices"])
            existing["price_summary"] = _price_response_fields(existing["prices"])["price_summary"]
            existing.setdefault("price_set_hashes", [])
            existing.setdefault("rate_pack_hashes", [])
            existing.setdefault("provider_set_hashes", [])
            _append_unique_value(existing["price_set_hashes"], item.get("price_set_hash"))
            _append_unique_value(existing["rate_pack_hashes"], item.get("rate_pack_hash"))
            _append_unique_value(existing["provider_set_hashes"], item.get("provider_set_hash"))
            _merge_unique_payload_list(existing, "price_set_hashes", item.get("price_set_hashes"))
            _merge_unique_payload_list(existing, "rate_pack_hashes", item.get("rate_pack_hashes"))
            _merge_unique_payload_list(existing, "provider_set_hashes", item.get("provider_set_hashes"))
            grouped[group_key] = existing
            merged.append(existing)
            continue

        combined_prices = (
            _normalize_price_payload(existing.get("prices"))
            + _normalize_price_payload(item.get("prices"))
        )
        price_fields = _price_response_fields(combined_prices)
        existing.update(price_fields)
        _append_unique_value(existing.setdefault("price_set_hashes", []), item.get("price_set_hash"))
        _append_unique_value(existing.setdefault("rate_pack_hashes", []), item.get("rate_pack_hash"))
        _append_unique_value(existing.setdefault("provider_set_hashes", []), item.get("provider_set_hash"))
        _merge_unique_payload_list(existing, "price_set_hashes", item.get("price_set_hashes"))
        _merge_unique_payload_list(existing, "rate_pack_hashes", item.get("rate_pack_hashes"))
        _merge_unique_payload_list(existing, "provider_set_hashes", item.get("provider_set_hashes"))
        _merge_unique_payload_list(existing, "source_trace", item.get("source_trace"))
        existing["price_set_count"] = len(existing.get("price_set_hashes") or [])
        existing["rate_pack_count"] = len(existing.get("rate_pack_hashes") or [])
    return merged


async def _ptg2_manifest_filter_npis_by_provider_taxonomy(
    session,
    args: dict[str, Any],
    npis: list[int] | tuple[int, ...],
    *,
    limit: int,
) -> tuple[int, ...]:
    candidate_npis = tuple(sorted({int(npi) for npi in npis if int(npi) > 0}))
    if not candidate_npis:
        return ()
    specialty_filter = resolve_provider_specialty_filter(args)
    params: dict[str, Any] = {"npis": list(candidate_npis), "limit": max(int(limit), 1)}
    predicates: list[str] = []
    if specialty_filter.active:
        predicates.append(
            provider_specialty_taxonomy_exists_sql(
                "source_npis.npi",
                params,
                "manifest_provider_specialty",
                specialty_filter,
                schema=PTG2_SCHEMA,
            )
        )
    inferred_sql = _inferred_provider_taxonomy_code_sql(
        args,
        nt_alias="nt",
        schema=PTG2_SCHEMA,
        params=params,
        param_prefix="manifest_provider_inferred_taxonomy",
    )
    if inferred_sql:
        predicates.append(
            f"EXISTS (SELECT 1 FROM {PTG2_SCHEMA}.npi_taxonomy nt WHERE nt.npi = source_npis.npi AND {inferred_sql})"
        )
        predicates.append(_ptg2_individual_npi_exists_sql("source_npis.npi"))
    if not predicates:
        return candidate_npis[: max(int(limit), 1)]
    result = await session.execute(
        text(
            f"""
            SELECT source_npis.npi
            FROM (SELECT UNNEST(CAST(:npis AS bigint[])) AS npi) source_npis
            WHERE {" AND ".join(predicates)}
            ORDER BY source_npis.npi
            LIMIT :limit
            """
        ),
        params,
    )
    return tuple(int(_row_mapping(row).get("npi")) for row in result if _row_mapping(row).get("npi") is not None)


def _ptg2_manifest_location_match_limit() -> int:
    raw_value = os.getenv("HLTHPRT_PTG2_MANIFEST_LOCATION_MATCH_LIMIT", "5000")
    try:
        return max(int(raw_value), 1)
    except ValueError:
        return 5000


async def _ptg2_manifest_location_provider_matches(
    session,
    serving_tables: PTG2ServingTables,
    args: dict[str, Any],
    *,
    candidate_limit: int | None = None,
    plan_id: str | None = None,
    snapshot_id: str | None = None,
    source_key: str | None = None,
) -> tuple[set[str], dict[str, list[dict[str, Any]]]] | None:
    provider_group_member_table = _safe_table_name(serving_tables.provider_group_member_table)
    if not provider_group_member_table or not _ptg2_manifest_artifact_entry(serving_tables, "provider_inverted"):
        return None

    state_value = str(args.get("state") or "").strip().upper()
    city_value = str(args.get("city") or "").strip().lower()
    zip_value = str(args.get("zip5") or args.get("zip") or "").strip()[:5]
    geo_lat: float | None = None
    geo_long: float | None = None
    geo_radius_miles: float | None = None
    if args.get("lat") not in (None, "", "null") or args.get("long") not in (None, "", "null"):
        try:
            radius_raw = args.get("radius_miles")
            geo_lat = float(args.get("lat"))
            geo_long = float(args.get("long"))
            geo_radius_miles = max(float(radius_raw if radius_raw not in (None, "", "null") else 25.0), 0.0)
        except (TypeError, ValueError):
            return None
    provider_npi = args.get("npi")
    npi_address_table = await _ptg2_address_serving_table(
        session,
        _PTG2_UNIFIED_ADDRESS_COLUMNS if geo_lat is not None else _PTG2_LEGACY_ADDRESS_COLUMNS,
        require_legacy_available=True,
    )
    npi_data_table = f"{PTG2_SCHEMA}.npi"
    if not npi_address_table:
        return None
    using_unified_address_table = _is_unified_address_table(npi_address_table)

    filters = ["addr.npi IS NOT NULL"]
    configured_limit = _ptg2_manifest_location_match_limit()
    if candidate_limit is not None:
        configured_limit = min(configured_limit, max(int(candidate_limit), 1))
    params: dict[str, Any] = {"limit": configured_limit}
    if state_value:
        filters.append("addr.state_name = :state_value")
        params["state_value"] = state_value
    if city_value:
        filters.append("addr.city_name = :city_value")
        params["city_value"] = city_value.upper()
    if zip_value:
        params["zip5"] = zip_value
    geo_filters: list[str] = []
    if geo_lat is not None and geo_long is not None and geo_radius_miles is not None:
        params.update(
            geo_lat=geo_lat,
            geo_long=geo_long,
            geo_radius_miles=geo_radius_miles,
            geo_min_lat=geo_lat - geo_radius_miles / 69.0,
            geo_max_lat=geo_lat + geo_radius_miles / 69.0,
            geo_min_long=geo_long - geo_radius_miles / 69.0,
            geo_max_long=geo_long + geo_radius_miles / 69.0,
        )
        geo_filters.append("addr.lat::float8 BETWEEN :geo_min_lat AND :geo_max_lat")
        geo_filters.append("addr.long::float8 BETWEEN :geo_min_long AND :geo_max_long")
        geo_filters.append(
            f"{_ptg2_geo_distance_miles_sql('addr.lat::float8', 'addr.long::float8')} <= CAST(:geo_radius_miles AS double precision)"
        )
        if using_unified_address_table:
            geo_filters.append("COALESCE(addr.address_precision, '') <> 'city_zip'")
    if zip_value and geo_filters:
        filters.append(f"(left(coalesce(addr.postal_code, ''), 5) = :zip5 OR ({' AND '.join(geo_filters)}))")
    elif zip_value:
        filters.append("left(coalesce(addr.postal_code, ''), 5) = :zip5")
    elif geo_filters:
        filters.extend(geo_filters)
    if provider_npi is not None:
        try:
            params["provider_npi"] = int(provider_npi)
            filters.append("addr.npi = :provider_npi")
        except (TypeError, ValueError):
            return None
    if len(filters) == 1:
        return None

    # Scope location matches to the requested clinical specialty/taxonomy, mirroring
    # the non-location taxonomy filter (_ptg2_manifest_filter_npis_by_provider_taxonomy)
    # and the compact provider filter. Without this, a procedure+ZIP search returns
    # every NPI that merely carries a negotiated rate at that address (e.g. an
    # optometry practice or a hospital for an arthroscopic ACL repair) instead of the
    # clinically appropriate specialists (orthopedic surgeons).
    location_specialty_filter = resolve_provider_specialty_filter(args)
    if location_specialty_filter.active:
        filters.append(
            provider_specialty_taxonomy_exists_sql(
                "addr.npi",
                params,
                "manifest_location_specialty",
                location_specialty_filter,
                schema=PTG2_SCHEMA,
            )
        )
    location_inferred_taxonomy_sql = _inferred_provider_taxonomy_code_sql(
        args,
        nt_alias="nt",
        schema=PTG2_SCHEMA,
        params=params,
        param_prefix="manifest_location_inferred_taxonomy",
    )
    if location_inferred_taxonomy_sql:
        filters.append(
            f"EXISTS (SELECT 1 FROM {PTG2_SCHEMA}.npi_taxonomy nt "
            f"WHERE nt.npi = addr.npi AND {location_inferred_taxonomy_sql})"
        )
        filters.append(_ptg2_individual_npi_exists_sql("addr.npi"))

    address_location_source = _ptg2_address_location_source(npi_address_table)
    address_location_hash_sql = _ptg2_address_location_hash_sql("addr", npi_address_table)
    has_npi_data = await _serving_table_available(session, npi_data_table)
    provider_join = f"LEFT JOIN {npi_data_table} n ON n.npi = addr.npi" if has_npi_data else ""
    provider_name_sql = (
        _ptg2_provider_name_sql("n")
        if has_npi_data
        else "'TiC provider'"
    )
    distance_sql = ""
    location_select_sql = (
        "NULL::double precision AS distance_miles, "
        "NULL::varchar AS zip_match_type, "
        "NULL::varchar AS anchor_zip5, "
        "NULL::double precision AS zip_radius_miles, "
        "0 AS zip_rank"
    )
    location_order_sql = "addr.npi"
    limited_location_order_sql = "npi"
    if geo_lat is not None and geo_long is not None and geo_radius_miles is not None:
        distance_sql = _ptg2_geo_distance_miles_sql("addr.lat::float8", "addr.long::float8")
        if zip_value:
            same_zip_sql = "LEFT(COALESCE(addr.postal_code, ''), 5) = :zip5"
            location_select_sql = (
                f"CASE WHEN {same_zip_sql} THEN 0.0 ELSE {distance_sql} END AS distance_miles, "
                f"CASE WHEN {same_zip_sql} THEN 'same_zip' ELSE 'radius' END AS zip_match_type, "
                ":zip5 AS anchor_zip5, CAST(:geo_radius_miles AS double precision) AS zip_radius_miles, "
                f"CASE WHEN {same_zip_sql} THEN 0 ELSE 1 END AS zip_rank"
            )
        else:
            location_select_sql = (
                f"{distance_sql} AS distance_miles, "
                "'radius' AS zip_match_type, "
                "NULL::varchar AS anchor_zip5, CAST(:geo_radius_miles AS double precision) AS zip_radius_miles, "
                "0 AS zip_rank"
            )
        location_order_sql = "addr.npi, zip_rank, distance_miles ASC NULLS LAST"
        limited_location_order_sql = "zip_rank, distance_miles ASC NULLS LAST, npi"

    # entity_address_unified can resolve a NPI to a city/zip-only row with no
    # street (first_line). When that happens, fall back to the NPPES npi_address
    # practice/primary row WHOLESALE (every field), so the displayed address stays
    # internally consistent instead of mixing one source's street with another's
    # city. Only applies when the unified table is the primary source.
    if using_unified_address_table:
        premise_key_select_sql = "addr.premise_key,"
        address_fallback_cte = f"""
            , fallback_addresses AS MATERIALIZED (
                SELECT DISTINCT ON (na.npi)
                       na.npi, na.first_line, na.second_line, na.city_name, na.state_name,
                       na.postal_code, na.country_code, na.telephone_number, na.fax_number,
                       na.lat, na.long
                  FROM {PTG2_SCHEMA}.npi_address na
                  JOIN location_npis loc ON loc.npi = na.npi
                 WHERE NULLIF(BTRIM(na.first_line), '') IS NOT NULL
                 ORDER BY na.npi,
                          CASE na.type WHEN 'primary' THEN 0 WHEN 'practice' THEN 1
                                       WHEN 'secondary' THEN 2 ELSE 3 END,
                          na.checksum
            )"""
        address_fallback_join = """
            LEFT JOIN fallback_addresses na
              ON na.npi = addr.npi
             AND NULLIF(BTRIM(addr.first_line), '') IS NULL"""

        def _eff(column: str) -> str:
            cast_suffix = "::numeric" if column in {"lat", "long"} else ""
            return (
                "CASE WHEN NULLIF(BTRIM(addr.first_line), '') IS NULL "
                f"AND na.first_line IS NOT NULL THEN na.{column}{cast_suffix} ELSE addr.{column}{cast_suffix} END"
            )
    else:
        premise_key_select_sql = "NULL::uuid AS premise_key,"
        address_fallback_cte = ""
        address_fallback_join = ""

        def _eff(column: str) -> str:
            return f"addr.{column}"

    async def _query_location_provider_rows(address_types: tuple[str, ...]) -> list[dict[str, Any]]:
        address_filters = [*filters]
        address_params = {**params, "address_types": list(address_types)}
        result = await session.execute(
            text(
                f"""
            WITH raw_location_npis AS (
                SELECT DISTINCT ON (addr.npi)
                    addr.npi,
                    addr.type,
                    addr.checksum,
                    addr.state_name,
                    addr.city_name,
                    addr.postal_code,
                    addr.country_code,
                    addr.address_key,
                    {premise_key_select_sql}
                    addr.lat,
                    addr.long,
                    addr.first_line,
                    addr.second_line,
                    addr.telephone_number,
                    addr.fax_number,
                    {location_select_sql}
                FROM {npi_address_table} addr
                WHERE {" AND ".join(address_filters)}
                  AND addr.type = ANY(CAST(:address_types AS varchar[]))
                ORDER BY {location_order_sql},
                    CASE addr.type
                        WHEN 'practice' THEN 0
                        WHEN 'primary' THEN 1
                        WHEN 'secondary' THEN 2
                        ELSE 3
                    END,
                    addr.checksum
            ),
            location_npis AS MATERIALIZED (
                SELECT *
                FROM raw_location_npis
                ORDER BY {limited_location_order_sql}
                LIMIT :limit
            )
            {address_fallback_cte}
            -- Resolve each candidate's taxonomy/specialty summary ONCE per NPI,
            -- before the provider_group_member fan-out below. Previously this
            -- LATERAL hung off the final SELECT, so it was re-evaluated for every
            -- (npi, provider_group) pair -- a provider in many groups paid the
            -- taxonomy lookup dozens of times. In dense metros that fan-out is
            -- what pushed the query past the request timeout. Computing it here,
            -- over the already-bounded location_npis set, makes it O(candidates)
            -- instead of O(candidates x groups).
            , located_with_tax AS MATERIALIZED (
                SELECT
                    loc.*,
                    tax.taxonomy_codes,
                    tax.specialties,
                    tax.classifications,
                    tax.specializations,
                    tax.primary_specialty,
                    tax.primary_specialization
                FROM location_npis loc
                {_provider_taxonomy_summary_lateral_sql("loc.npi")}
            )
            SELECT DISTINCT ON (pgm.provider_group_global_id_128, addr.npi)
                pgm.provider_group_global_id_128,
                addr.npi,
                addr.address_key::text AS address_key,
                addr.premise_key::text AS premise_key,
                {address_location_hash_sql} AS location_hash,
                {_eff('state_name')} AS state,
                {_eff('city_name')} AS city,
                LEFT(COALESCE({_eff('postal_code')}, ''), 5) AS zip5,
                addr.distance_miles,
                addr.zip_match_type,
                addr.anchor_zip5,
                addr.zip_radius_miles,
                '{address_location_source}' AS location_source,
                '{address_location_source}' AS location_confidence_code,
                json_build_object(
                    'first_line', {_eff('first_line')},
                    'second_line', {_eff('second_line')},
                    'city', {_eff('city_name')},
                    'state', {_eff('state_name')},
                    'postal_code', {_eff('postal_code')},
                    'country_code', {_eff('country_code')},
                    'telephone_number', {_eff('telephone_number')},
                    'fax_number', {_eff('fax_number')},
                    'address_key', addr.address_key::text,
                    'lat', {_eff('lat')},
                    'long', {_eff('long')}
                )::text AS address_payload,
                {_eff('telephone_number')} AS telephone_number,
                {_eff('fax_number')} AS fax_number,
                COALESCE(addr.taxonomy_codes, ARRAY[]::varchar[]) AS taxonomy_codes,
                COALESCE(addr.specialties, ARRAY[]::varchar[]) AS specialties,
                COALESCE(addr.classifications, ARRAY[]::varchar[]) AS classifications,
                COALESCE(addr.specializations, ARRAY[]::varchar[]) AS specializations,
                addr.primary_specialty,
                addr.primary_specialization,
                {provider_name_sql} AS provider_name
            FROM located_with_tax addr
            JOIN {provider_group_member_table} pgm ON pgm.npi = addr.npi
            {provider_join}
            {address_fallback_join}
            ORDER BY pgm.provider_group_global_id_128,
                addr.npi,
                CASE addr.type
                    WHEN 'practice' THEN 0
                    WHEN 'primary' THEN 1
                    WHEN 'secondary' THEN 2
                    ELSE 3
                END,
                addr.checksum
            LIMIT :limit
            """
            ),
            address_params,
        )
        return [_row_mapping(row) for row in result]

    async def _fill_location_phone_fallbacks(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not using_unified_address_table:
            return rows
        missing_rows = [row for row in rows if not row.get("telephone_number")]
        if not missing_rows:
            return rows
        address_keys = sorted({str(row.get("address_key")) for row in missing_rows if row.get("address_key")})
        premise_keys = sorted({str(row.get("premise_key")) for row in missing_rows if row.get("premise_key")})
        if not address_keys and not premise_keys:
            return rows
        fallback_result = await session.execute(
            text(
                f"""
                SELECT npi,
                       address_key::text AS address_key,
                       premise_key::text AS premise_key,
                       telephone_number,
                       fax_number,
                       type,
                       checksum
                  FROM {npi_address_table}
                 WHERE NULLIF(BTRIM(telephone_number), '') IS NOT NULL
                   AND (
                        address_key::text = ANY(CAST(:address_keys AS text[]))
                        OR premise_key::text = ANY(CAST(:premise_keys AS text[]))
                   )
                """
            ),
            {"address_keys": address_keys, "premise_keys": premise_keys},
        )
        candidates = [_row_mapping(row) for row in fallback_result]
        if not candidates:
            return rows

        def _fallback_rank(candidate: dict[str, Any], row: dict[str, Any]) -> tuple[int, int, int, str]:
            type_rank = {"primary": 0, "practice": 1, "secondary": 2}.get(str(candidate.get("type") or ""), 3)
            return (
                0 if candidate.get("npi") == row.get("npi") else 1,
                0 if str(candidate.get("address_key") or "") == str(row.get("address_key") or "") else 1,
                type_rank,
                str(candidate.get("checksum") or ""),
            )

        filled_rows: list[dict[str, Any]] = []
        for row in rows:
            if row.get("telephone_number"):
                filled_rows.append(row)
                continue
            row_address_key = str(row.get("address_key") or "")
            row_premise_key = str(row.get("premise_key") or "")
            matches = [
                candidate
                for candidate in candidates
                if (row_address_key and str(candidate.get("address_key") or "") == row_address_key)
                or (row_premise_key and str(candidate.get("premise_key") or "") == row_premise_key)
            ]
            if not matches:
                filled_rows.append(row)
                continue
            same_npi_matches = [candidate for candidate in matches if candidate.get("npi") == row.get("npi")]
            if same_npi_matches:
                best = sorted(same_npi_matches, key=lambda candidate: _fallback_rank(candidate, row))[0]
            else:
                distinct_phones = {
                    str(candidate.get("telephone_number") or "").strip()
                    for candidate in matches
                    if str(candidate.get("telephone_number") or "").strip()
                }
                if len(distinct_phones) != 1:
                    filled_rows.append(row)
                    continue
                best = sorted(matches, key=lambda candidate: _fallback_rank(candidate, row))[0]
            row = dict(row)
            row["telephone_number"] = best.get("telephone_number")
            row["fax_number"] = row.get("fax_number") or best.get("fax_number")
            address_payload = _coerce_json_payload(row.get("address_payload"), {})
            if isinstance(address_payload, dict):
                address_payload["telephone_number"] = row["telephone_number"]
                if row.get("fax_number"):
                    address_payload["fax_number"] = row["fax_number"]
                row["address_payload"] = address_payload
            filled_rows.append(row)
        return filled_rows

    # Primary addresses map to the existing (type, state, city, npi) and
    # (type, zip5) indexes. The broad primary+secondary form forces a large
    # sort in dense cities, so use primary practice locations for the hot path
    # and fall back to secondary only when primary has no candidates.
    if using_unified_address_table:
        primary_address_types = ("practice", "primary") if provider_npi is None else ("practice", "primary", "secondary")
        fallback_address_types = ("secondary",)
    else:
        primary_address_types = ("primary",) if provider_npi is None else ("primary", "secondary")
        fallback_address_types = ("secondary",)
    rows = await _query_location_provider_rows(primary_address_types)
    if not rows and provider_npi is None:
        rows = await _query_location_provider_rows(fallback_address_types)
    if not rows:
        return set(), {}
    rows = await _fill_location_phone_fallbacks(rows)
    rows = await _overlay_provider_directory_corroboration(
        session,
        rows,
        plan_id=plan_id,
        snapshot_id=snapshot_id,
        source_key=source_key,
    )
    group_ids = tuple(
        sorted({_ptg2_manifest_id(row.get("provider_group_global_id_128")) for row in rows if row.get("provider_group_global_id_128")})
    )
    if not group_ids:
        return set(), {}
    sets_by_group = _ptg2_manifest_sidecar_members_many(serving_tables, "provider_inverted", group_ids)
    provider_set_ids: set[str] = set()
    providers_by_set: dict[str, list[dict[str, Any]]] = {}
    seen_provider_rows: dict[str, set[int]] = {}
    for row in rows:
        group_id = _ptg2_manifest_id(row.get("provider_group_global_id_128"))
        npi = row.get("npi")
        if not group_id or npi is None:
            continue
        provider_row = {
            "npi": int(npi),
            "provider_name": row.get("provider_name") or "TiC provider",
            "state": row.get("state"),
            "city": row.get("city"),
            "zip5": row.get("zip5"),
            "distance_miles": row.get("distance_miles"),
            "zip_match_type": row.get("zip_match_type"),
            "anchor_zip5": row.get("anchor_zip5"),
            "zip_radius_miles": row.get("zip_radius_miles"),
            "telephone_number": row.get("telephone_number"),
            "fax_number": row.get("fax_number"),
            "location_hash": row.get("location_hash"),
            "location_source": row.get("location_source"),
            "location_confidence_code": row.get("location_confidence_code"),
            "address_payload": row.get("address_payload"),
            "taxonomy_codes": row.get("taxonomy_codes") or [],
            "specialties": row.get("specialties") or [],
            "classifications": row.get("classifications") or [],
            "specializations": row.get("specializations") or [],
            "primary_specialty": row.get("primary_specialty"),
            "primary_specialization": row.get("primary_specialization"),
        }
        for provider_set_id in sets_by_group.get(group_id, ()):
            provider_set_ids.add(provider_set_id)
            seen = seen_provider_rows.setdefault(provider_set_id, set())
            provider_npi_value = int(npi)
            if provider_npi_value in seen:
                continue
            seen.add(provider_npi_value)
            providers_by_set.setdefault(provider_set_id, []).append(provider_row)
    return provider_set_ids, providers_by_set


async def _ptg2_manifest_provider_rows_for_provider_set(
    session,
    serving_tables: PTG2ServingTables,
    provider_set_global_id: str,
    *,
    limit: int,
) -> list[dict[str, Any]] | None:
    provider_set_global_id = _ptg2_manifest_id(provider_set_global_id)
    if not provider_set_global_id:
        return None
    provider_npis = _ptg2_manifest_provider_npis_for_provider_set(serving_tables, provider_set_global_id)
    if provider_npis:
        return await _ptg2_manifest_enriched_provider_rows_for_npis(session, npis=provider_npis, limit=limit)

    provider_group_member_table = _safe_table_name(serving_tables.provider_group_member_table)
    if not provider_group_member_table:
        return None
    provider_groups = _ptg2_manifest_sidecar_members(serving_tables, "provider_forward", provider_set_global_id)
    if not provider_groups:
        return None
    group_ids = list(provider_groups)
    group_array_cast = _ptg2_manifest_id_array_cast(serving_tables)
    npi_stmt = text(
        f"""
        SELECT DISTINCT pgm.npi
        FROM {provider_group_member_table} pgm
        WHERE pgm.provider_group_global_id_128 = ANY(CAST(:group_ids AS {group_array_cast}))
          AND pgm.npi > 0
        ORDER BY pgm.npi
        LIMIT :limit
        """
    )
    npi_result = await session.execute(npi_stmt, {"group_ids": group_ids, "limit": limit})
    npis = [int(_row_mapping(row).get("npi")) for row in npi_result if _row_mapping(row).get("npi") is not None]
    return await _ptg2_manifest_enriched_provider_rows_for_npis(session, npis=npis, limit=limit)


async def _ptg2_manifest_provider_rows_for_provider_sets(
    session,
    serving_tables: PTG2ServingTables,
    provider_set_global_ids: list[str] | tuple[str, ...],
    *,
    limit_per_set: int,
    args: dict[str, Any] | None = None,
) -> dict[str, list[dict[str, Any]]] | None:
    provider_set_ids = _ptg2_manifest_ids(tuple(provider_set_global_ids))
    if not provider_set_ids:
        return {}
    args = args or {}
    provider_taxonomy_filter_requested = _ptg2_provider_taxonomy_filter_requested(args)
    candidate_limit_per_set = max(int(limit_per_set), 1)
    if provider_taxonomy_filter_requested:
        candidate_limit_per_set = max(candidate_limit_per_set * 200, 1000)

    npis_by_set = _ptg2_manifest_provider_npis_for_provider_sets(
        serving_tables,
        provider_set_ids,
        limit_per_set=candidate_limit_per_set,
    )
    missing_provider_set_ids = [provider_set_id for provider_set_id in provider_set_ids if not npis_by_set.get(provider_set_id)]
    if missing_provider_set_ids:
        provider_group_member_table = _safe_table_name(serving_tables.provider_group_member_table)
        if not provider_group_member_table:
            return None
        groups_by_set = _ptg2_manifest_sidecar_members_many(serving_tables, "provider_forward", missing_provider_set_ids)
        group_ids = tuple(dict.fromkeys(group_id for group_ids in groups_by_set.values() for group_id in group_ids))
        if not group_ids:
            return None
        group_array_cast = _ptg2_manifest_id_array_cast(serving_tables)
        npi_stmt = text(
            f"""
                SELECT DISTINCT pgm.provider_group_global_id_128, pgm.npi
                FROM {provider_group_member_table} pgm
                WHERE pgm.provider_group_global_id_128 = ANY(CAST(:group_ids AS {group_array_cast}))
                  AND pgm.npi > 0
                ORDER BY pgm.provider_group_global_id_128, pgm.npi
                """
        )
        npi_result = await session.execute(npi_stmt, {"group_ids": group_ids})
        npis_by_group: dict[str, set[int]] = {}
        for row in npi_result:
            data = _row_mapping(row)
            group_id = _ptg2_manifest_id(data.get("provider_group_global_id_128"))
            npi = data.get("npi")
            if group_id and npi is not None:
                npis_by_group.setdefault(group_id, set()).add(int(npi))
        for provider_set_id in missing_provider_set_ids:
            npis: set[int] = set()
            for group_id in groups_by_set.get(provider_set_id, ()):
                npis.update(npis_by_group.get(group_id, set()))
            npis_by_set[provider_set_id] = tuple(sorted(npis))

    if provider_taxonomy_filter_requested:
        filtered_npis_by_set: dict[str, tuple[int, ...]] = {}
        for provider_set_id in provider_set_ids:
            filtered_npis_by_set[provider_set_id] = await _ptg2_manifest_filter_npis_by_provider_taxonomy(
                session,
                args,
                npis_by_set.get(provider_set_id, ()),
                limit=max(int(limit_per_set), 1),
            )
        npis_by_set = filtered_npis_by_set

    all_npis = tuple(
        dict.fromkeys(
            npi
            for provider_set_id in provider_set_ids
            for npi in npis_by_set.get(provider_set_id, ())[: max(int(limit_per_set), 1)]
        )
    )
    provider_rows = await _ptg2_manifest_enriched_provider_rows_for_npis(
        session,
        npis=all_npis,
        limit=max(len(all_npis), 1),
        plan_id=str(args.get("plan_id") or args.get("plan_external_id") or "").strip() or None,
        snapshot_id=str(args.get("snapshot_id") or "").strip() or None,
        source_key=serving_tables.source_key or str(args.get("source_key") or "").strip() or None,
    )
    if provider_rows is None:
        return None
    providers_by_npi = {int(row.get("npi")): row for row in provider_rows if row.get("npi") is not None}
    return {
        provider_set_id: [
            providers_by_npi.get(npi) or {"npi": npi, "provider_name": "TiC provider"}
            for npi in npis_by_set.get(provider_set_id, ())[: max(int(limit_per_set), 1)]
        ]
        for provider_set_id in provider_set_ids
    }


async def _ptg2_manifest_provider_sets_for_npi(
    session,
    serving_tables: PTG2ServingTables,
    npi: int,
) -> tuple[str, ...] | None:
    provider_group_member_table = _safe_table_name(serving_tables.provider_group_member_table)
    if not provider_group_member_table or not _ptg2_manifest_artifact_entry(serving_tables, "provider_inverted"):
        return None
    group_result = await session.execute(
        text(
            f"""
            SELECT DISTINCT provider_group_global_id_128
            FROM {provider_group_member_table}
            WHERE npi = :provider_npi
            ORDER BY provider_group_global_id_128
            """
        ),
        {"provider_npi": npi},
    )
    group_ids = [
        _ptg2_manifest_id(_row_mapping(row).get("provider_group_global_id_128"))
        for row in group_result
        if _row_mapping(row).get("provider_group_global_id_128")
    ]
    if not group_ids:
        return ()
    sets_by_group = _ptg2_manifest_sidecar_members_many(serving_tables, "provider_inverted", tuple(group_ids))
    provider_set_ids = tuple(
        sorted({provider_set_id for provider_set_ids in sets_by_group.values() for provider_set_id in provider_set_ids})
    )
    return provider_set_ids


async def _ptg2_manifest_procedure_details_for_rows(
    session,
    rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> dict[tuple[str, str], dict[str, Any]]:
    lookup_keys = sorted(
        key
        for key in {
            _catalog_key(row.get("reported_code_system"), row.get("reported_code"))
            for row in rows
            if row.get("reported_code_system") and row.get("reported_code")
        }
        if key is not None
    )
    if not lookup_keys:
        return {}
    clauses: list[str] = []
    params: dict[str, Any] = {}
    for idx, (code_system, code) in enumerate(lookup_keys):
        clauses.append(f"(code_system = :code_system_{idx} AND code = :code_{idx})")
        params[f"code_system_{idx}"] = code_system
        params[f"code_{idx}"] = code
    try:
        result = await session.execute(
            text(
                f"""
                SELECT code_system, code, display_name, short_description
                FROM {PTG2_SCHEMA}.code_catalog
                WHERE {" OR ".join(clauses)}
                """
            ),
            params,
        )
    except Exception:
        await _rollback_optional_ptg2_query(session)
        return {}
    return {
        (str(data.get("code_system") or ""), str(data.get("code") or "")): {
            "procedure_name": data.get("display_name"),
            "procedure_description": data.get("short_description") or data.get("display_name"),
        }
        for data in (_row_mapping(row) for row in result)
    }


async def _search_ptg2_manifest_db_serving_table(
    session,
    snapshot_id: str,
    args: dict[str, Any],
    pagination,
    serving_tables: PTG2ServingTables,
    mode_value: str,
) -> dict[str, Any] | None:
    """Serve the additive manifest skinny table for exact plan/code lookups.

    Provider, geography, and text expansion still require manifest sidecar readers; keep
    this path intentionally bounded until those readers are wired into the API.
    """
    table_name = _safe_table_name(serving_tables.serving_table)
    if not table_name or not await _serving_table_available(session, table_name):
        return None
    requested_plan = str(args.get("plan_id") or args.get("plan_external_id") or "").strip()
    requested_system = _normalize_code_system(args.get("code_system") or args.get("reported_code_system"))
    requested_code = (
        canonical_catalog_code(requested_system, args.get("code") or args.get("reported_code"))
        if requested_system
        else str(args.get("code") or args.get("reported_code") or "").strip()
    )
    expand_providers = _request_bool(args.get("include_providers"))
    location_filter_requested = bool(
        args.get("state")
        or args.get("city")
        or args.get("zip5")
        or args.get("zip")
        or args.get("npi")
        or args.get("lat") is not None
        or args.get("long") is not None
        or args.get("radius_miles") is not None
    )
    unsupported_filters = args.get("q")
    if unsupported_filters or not requested_plan or not requested_code:
        return None
    has_provider_npi_sidecar = bool(_ptg2_manifest_artifact_entry(serving_tables, "provider_npi"))
    if expand_providers and not serving_tables.provider_group_member_table and not has_provider_npi_sidecar:
        return None

    rate_candidate_limit = _ptg2_manifest_rate_candidate_limit(
        args,
        pagination,
        expand_providers=expand_providers,
        location_filter_requested=location_filter_requested,
    )
    filters = ["plan_id = :plan_id", "reported_code = :reported_code"]
    params: dict[str, Any] = {
        "plan_id": requested_plan,
        "reported_code": requested_code,
        "limit": int(pagination.limit),
        "rate_candidate_limit": rate_candidate_limit,
        "rate_candidate_offset": 0 if expand_providers else int(pagination.offset),
        "offset": int(pagination.offset),
    }
    if requested_system:
        filters.append("reported_code_system = :reported_code_system")
        params["reported_code_system"] = requested_system
    location_providers_by_set: dict[str, list[dict[str, Any]]] = {}
    if location_filter_requested:
        location_matches = await _ptg2_manifest_location_provider_matches(
            session,
            serving_tables,
            args,
            candidate_limit=rate_candidate_limit,
            plan_id=requested_plan,
            snapshot_id=snapshot_id,
            source_key=serving_tables.source_key or args.get("source_key"),
        )
        if location_matches is None:
            return None
        provider_set_ids, location_providers_by_set = location_matches
        if not provider_set_ids:
            return _shape_ptg2_manifest_response(
                {
                    "items": [],
                    "pagination": {
                        "total": 0,
                        "limit": pagination.limit,
                        "offset": pagination.offset,
                        "page": (pagination.offset // pagination.limit) + 1 if pagination.limit else 1,
                    },
                    "query": {
                        "plan_id": args.get("plan_id"),
                        "plan_external_id": args.get("plan_external_id"),
                        "plan_market_type": args.get("plan_market_type") or args.get("market_type") or None,
                        "source_key": args.get("source_key") or None,
                        "snapshot_id": snapshot_id,
                        "mode": mode_value,
                        "code": args.get("code") or None,
                        "code_system": args.get("code_system") or None,
                        "state": args.get("state") or None,
                        "city": args.get("city") or None,
                        "zip5": args.get("zip5") or None,
                        "lat": args.get("lat") or None,
                        "long": args.get("long") or None,
                        "radius_miles": args.get("radius_miles") or None,
                        "npi": args.get("npi") or None,
                        "source": "ptg2_db",
                        "serving_table": table_name,
                        "include_providers": expand_providers,
                        "procedure_consolidation": "REPORTED_CODE",
                        "status": "no_match",
                    },
                },
                args,
            )
        filters.append(f"provider_set_global_id_128 = ANY(CAST(:provider_set_ids AS {_ptg2_manifest_id_array_cast(serving_tables)}))")
        params["provider_set_ids"] = sorted(provider_set_ids)
    where_sql = " AND ".join(filters)
    total: int | None = None
    code_count_table = _safe_table_name(serving_tables.code_count_table)
    if code_count_table and requested_system and not location_filter_requested:
        count_result = await session.execute(
            text(
                f"""
                SELECT rate_count
                FROM {code_count_table}
                WHERE plan_id = :plan_id
                  AND reported_code = :reported_code
                  AND reported_code_system IS NOT DISTINCT FROM :reported_code_system
                """
            ),
            {
                "plan_id": requested_plan,
                "reported_code": requested_code,
                "reported_code_system": requested_system or None,
            },
        )
        total = int(count_result.scalar() or 0)
        if total <= 0:
            return None
    elif not location_filter_requested:
        count_result = await session.execute(text(f"SELECT COUNT(*) FROM {table_name} WHERE {where_sql}"), params)
        total = int(count_result.scalar() or 0)
        if total <= 0:
            return None

    network_names_select_sql = (
        "network_names"
        if await _ptg2_table_has_columns(session, table_name, {"network_names"})
        else "NULL::varchar[] AS network_names"
    )
    row_result = await session.execute(
        text(
            f"""
            SELECT
                serving_content_hash_128,
                plan_id,
                reported_code_system,
                reported_code,
                procedure_global_id_128,
                provider_set_global_id_128,
                provider_count,
                price_set_global_id_128,
                source_trace_set_hash,
                {network_names_select_sql}
            FROM {table_name}
            WHERE {where_sql}
            ORDER BY provider_count DESC NULLS LAST, serving_content_hash_128
            LIMIT :rate_candidate_limit OFFSET :rate_candidate_offset
            """
        ),
        params,
    )
    items: list[dict[str, Any]] = []
    row_data = [_row_mapping(row) for row in row_result]
    if not row_data:
        return None
    source_traces_by_set = await _ptg2_source_traces_for_trace_sets(
        session,
        [data.get("source_trace_set_hash") for data in row_data],
    )
    prices_by_price_set = await _ptg2_manifest_prices_for_price_sets(
        session,
        serving_tables,
        [_ptg2_manifest_id(data.get("price_set_global_id_128")) for data in row_data],
    )
    providers_by_set: dict[str, list[dict[str, Any]]] = {}
    if expand_providers:
        if location_filter_requested:
            providers_by_set = location_providers_by_set
        else:
            provider_set_ids = [_ptg2_manifest_id(data.get("provider_set_global_id_128")) for data in row_data]
            provider_rows_by_set = await _ptg2_manifest_provider_rows_for_provider_sets(
                session,
                serving_tables,
                provider_set_ids,
                limit_per_set=max(int(pagination.limit), 1),
                args={**args, "snapshot_id": snapshot_id, "source_key": serving_tables.source_key or args.get("source_key")},
            )
            if provider_rows_by_set is None:
                return None
            providers_by_set = provider_rows_by_set
    procedure_details = await _ptg2_manifest_procedure_details_for_rows(session, row_data)
    for data in row_data:
        if not expand_providers and len(items) >= int(pagination.limit):
            break
        reported_code = data.get("reported_code")
        reported_system = data.get("reported_code_system")
        provider_set_hash = _ptg2_manifest_id(data.get("provider_set_global_id_128"))
        price_set_hash = _ptg2_manifest_id(data.get("price_set_global_id_128"))
        rate_pack_hash = _ptg2_manifest_id(data.get("serving_content_hash_128"))
        prices = prices_by_price_set.get(price_set_hash, [])
        procedure_detail = procedure_details.get(_catalog_key(reported_system, reported_code) or ("", ""), {})
        base_item = {
            "provider_ordinal": provider_set_hash,
            "provider_set_hash": provider_set_hash,
            "provider_set_hashes": [provider_set_hash] if provider_set_hash else [],
            "provider_name": "TiC provider set",
            "source_key": serving_tables.source_key or args.get("source_key"),
            "snapshot_id": snapshot_id,
            "network_names": _coerce_str_list_payload(data.get("network_names")),
            "provider_count": data.get("provider_count") or 0,
            "provider_set_count": 1 if provider_set_hash else 0,
            "procedure_code": reported_code,
            "hp_procedure_code": reported_code,
            "procedure_name": procedure_detail.get("procedure_name"),
            "procedure_description": procedure_detail.get("procedure_description"),
            "service_code": reported_code,
            "service_code_system": reported_system or requested_system or "CPT",
            "reported_code": reported_code,
            "reported_code_system": reported_system,
            "billing_code": reported_code,
            "billing_code_type": reported_system,
            **_price_response_fields(prices),
            "price_set_hash": price_set_hash,
            "rate_pack_hash": rate_pack_hash,
            "source_trace": source_traces_by_set.get(str(data.get("source_trace_set_hash") or ""), []),
            "confidence": {"network": "tic_rate_npi_tin", "location": "nppes_practice_location"},
        }
        base_item["address_verification"] = _address_verification_payload(base_item, {}, {})
        _apply_ptg_address_display_policy(base_item, args)
        if not expand_providers:
            items.append(base_item)
            continue
        for provider in providers_by_set.get(_ptg2_manifest_id(data.get("provider_set_global_id_128")), []):
            item = dict(base_item)
            address_payload = _coerce_json_payload(provider.get("address_payload"), {})
            item.update(
                {
                    "provider_ordinal": provider.get("npi") or provider_set_hash,
                    "npi": provider.get("npi"),
                    "provider_name": provider.get("provider_name") or base_item["provider_name"],
                    "state": provider.get("state"),
                    "city": provider.get("city"),
                    "zip5": provider.get("zip5"),
                    "location_hash": provider.get("location_hash"),
                    "location_source": provider.get("location_source"),
                    "location_confidence_code": provider.get("location_confidence_code"),
                    "address": address_payload,
                    "taxonomy_codes": _coerce_json_payload(provider.get("taxonomy_codes"), []),
                    "specialties": _coerce_json_payload(provider.get("specialties"), []),
                    "primary_specialty": provider.get("primary_specialty"),
                    "classification": (_coerce_json_payload(provider.get("classifications"), []) or [None])[0],
                    "classifications": _coerce_json_payload(provider.get("classifications"), []),
                    "specialization": provider.get("primary_specialization")
                    or ((_coerce_json_payload(provider.get("specializations"), []) or [None])[0]),
                    "primary_specialization": provider.get("primary_specialization"),
                    "specializations": _coerce_json_payload(provider.get("specializations"), []),
                    "distance_miles": provider.get("distance_miles"),
                    "zip_match_type": provider.get("zip_match_type"),
                    "anchor_zip5": provider.get("anchor_zip5"),
                    "zip_radius_miles": provider.get("zip_radius_miles"),
                }
            )
            _add_location_phone_fields(item, provider, address_payload)
            _promote_address_provenance_fields(item, address_payload)
            item["address_verification"] = _address_verification_payload(item, provider, address_payload)
            _apply_ptg_address_display_policy(item, args)
            items.append(item)
    if not items:
        return None
    if expand_providers:
        items = _merge_ptg2_provider_rate_items(items)
    items = _sort_ptg2_manifest_provider_items(
        items,
        args,
        location_filter_requested=location_filter_requested and expand_providers,
    )
    total_items = len(items)
    if expand_providers:
        start = max(int(pagination.offset), 0)
        end = start + max(int(pagination.limit), 0)
        items = items[start:end]
    return _shape_ptg2_manifest_response(
        {
            "items": items,
            "pagination": {
                "total": (
                    total_items
                    if expand_providers
                    else total
                    if total is not None
                    else int(pagination.offset) + len(items)
                ),
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": (pagination.offset // pagination.limit) + 1 if pagination.limit else 1,
            },
            "query": {
                "plan_id": args.get("plan_id"),
                "plan_external_id": args.get("plan_external_id"),
                "plan_market_type": args.get("plan_market_type") or args.get("market_type") or None,
                "source_key": args.get("source_key") or None,
                "snapshot_id": snapshot_id,
                "mode": mode_value,
                "code": args.get("code") or None,
                "code_system": args.get("code_system") or None,
                "state": args.get("state") or None,
                "city": args.get("city") or None,
                "zip5": args.get("zip5") or None,
                "lat": args.get("lat") or None,
                "long": args.get("long") or None,
                "radius_miles": args.get("radius_miles") or None,
                "npi": args.get("npi") or None,
                "source": "ptg2_db",
                "serving_table": table_name,
                "include_providers": expand_providers,
                "procedure_consolidation": "REPORTED_CODE",
            },
        },
        args,
    )


def _compact_required_tables(serving_tables: PTG2ServingTables) -> bool:
    return bool(
        serving_tables.price_code_set_table
        and serving_tables.price_atom_table
        and serving_tables.price_set_entry_table
        and serving_tables.procedure_table
    )


def _compact_price_payload_sql(serving_tables: PTG2ServingTables, params: dict[str, Any], args: dict[str, Any]) -> str:
    price_filters, _price_filter_payload = _price_filter_clauses(args, params)
    filter_sql = ""
    if price_filters:
        filter_sql = "WHERE " + " AND ".join(price_filters)
    return f"""
        SELECT jsonb_agg(pa.payload ORDER BY pa.negotiated_rate NULLS LAST) AS prices
        FROM {serving_tables.price_set_entry_table} pse
        JOIN {serving_tables.price_atom_table} pa
          ON pa.price_atom_hash = pse.price_atom_hash
        LEFT JOIN {serving_tables.price_code_set_table} service_set
          ON service_set.price_code_set_hash = pa.service_code_set_hash
        LEFT JOIN {serving_tables.price_code_set_table} modifier_set
          ON modifier_set.price_code_set_hash = pa.modifier_code_set_hash
        {filter_sql}
          {"AND" if filter_sql else "WHERE"} pse.price_set_hash = r.price_set_hash
    """


def _source_trace_payload_sql(row_alias: str = "r") -> str:
    return f"""
        COALESCE(
            NULLIF({row_alias}.source_trace::jsonb, '[]'::jsonb),
            source_trace_payload.source_trace,
            '[]'::jsonb
        ) AS source_trace
    """


def _source_trace_payload_lateral_sql(row_alias: str = "r") -> str:
    return f"""
        LEFT JOIN LATERAL (
            SELECT jsonb_agg(
                       jsonb_strip_nulls(
                           jsonb_build_object(
                               'source_file_version_id', st.source_file_version_id,
                               'original_url', st.original_url,
                               'canonical_url', st.canonical_url,
                               'json_pointer', st.json_pointer,
                               'line_number', st.line_number
                           )
                       )
                       ORDER BY st.source_trace_hash
                   ) AS source_trace
            FROM {PTG2_SCHEMA}.ptg2_source_trace_set sts
            JOIN {PTG2_SCHEMA}.ptg2_source_trace st
              ON st.source_trace_hash = ANY(sts.source_trace_hashes)
            WHERE sts.source_trace_set_hash = {row_alias}.source_trace_set_hash
        ) source_trace_payload ON TRUE
    """


async def _ptg2_source_traces_for_trace_sets(
    session,
    source_trace_set_hashes: list[Any] | tuple[Any, ...] | set[Any],
) -> dict[str, list[dict[str, Any]]]:
    trace_set_hashes = sorted({str(value).strip() for value in source_trace_set_hashes if str(value or "").strip()})
    if not trace_set_hashes:
        return {}
    result = await session.execute(
        text(
            f"""
            SELECT
                sts.source_trace_set_hash,
                COALESCE(
                    jsonb_agg(
                        jsonb_strip_nulls(
                            jsonb_build_object(
                                'source_file_version_id', st.source_file_version_id,
                                'original_url', st.original_url,
                                'canonical_url', st.canonical_url,
                                'json_pointer', st.json_pointer,
                                'line_number', st.line_number
                            )
                        )
                        ORDER BY st.source_trace_hash
                    ),
                    '[]'::jsonb
                ) AS source_trace
            FROM {PTG2_SCHEMA}.ptg2_source_trace_set sts
            JOIN {PTG2_SCHEMA}.ptg2_source_trace st
              ON st.source_trace_hash = ANY(sts.source_trace_hashes)
            WHERE sts.source_trace_set_hash = ANY(CAST(:source_trace_set_hashes AS varchar[]))
            GROUP BY sts.source_trace_set_hash
            """
        ),
        {"source_trace_set_hashes": trace_set_hashes},
    )
    return {
        str(data.get("source_trace_set_hash")): _coerce_json_payload(data.get("source_trace"), [])
        for data in (_row_mapping(row) for row in result)
        if data.get("source_trace_set_hash")
    }


def _compact_provider_filter_sql(
    serving_tables: PTG2ServingTables,
    args: dict[str, Any],
    params: dict[str, Any],
    *,
    address_table: str | None = None,
) -> tuple[str, bool]:
    has_geo = bool(args.get("zip5") or args.get("zip") or args.get("city") or args.get("state") or args.get("lat") or args.get("long") or args.get("radius_miles"))
    specialty_filter = resolve_provider_specialty_filter(args)
    inferred_sql = _inferred_provider_taxonomy_code_sql(
        args,
        nt_alias="nt",
        schema=PTG2_SCHEMA,
        params=params,
        param_prefix="inferred_taxonomy",
    )
    has_provider_filter = has_geo or specialty_filter.active or bool(inferred_sql)
    if not has_provider_filter:
        return "", False
    if not serving_tables.provider_group_member_table:
        return "", False
    if args.get("zip5") or args.get("zip"):
        params["zip5"] = _normalize_zip5(args.get("zip5") or args.get("zip"))
    if args.get("city"):
        params["city_exact"] = str(args.get("city") or "").strip().upper()
    if args.get("state"):
        params["state_exact"] = str(args.get("state") or "").strip().upper()
    if args.get("lat") and args.get("long"):
        lat = float(args.get("lat"))
        lon = float(args.get("long"))
        radius = float(args.get("radius_miles") or 25.0)
        params.update(
            geo_lat=lat,
            geo_long=lon,
            geo_radius_miles=radius,
            geo_min_lat=lat - radius / 69.0,
            geo_max_lat=lat + radius / 69.0,
            geo_min_long=lon - radius / 69.0,
            geo_max_long=lon + radius / 69.0,
        )
    if serving_tables.provider_group_location_table and has_geo:
        params.setdefault("provider_match_limit", max(int(params.get("limit") or 25) * 8, 64))
        params.setdefault("location_rate_candidate_limit", max(int(params.get("limit") or 25) * 200, 4096))
        clauses = []
        geo_clauses: list[str] = []
        if params.get("geo_lat") is not None:
            geo_clauses.append("loc.lat::float8 BETWEEN :geo_min_lat AND :geo_max_lat")
            geo_clauses.append("loc.lon::float8 BETWEEN :geo_min_long AND :geo_max_long")
            geo_clauses.append(
                f"{_ptg2_geo_distance_miles_sql('loc.lat::float8', 'loc.lon::float8')} <= CAST(:geo_radius_miles AS double precision)"
            )
        if params.get("zip5") and geo_clauses:
            clauses.append(f"(LEFT(COALESCE(loc.zip5, ''), 5) = :zip5 OR ({' AND '.join(geo_clauses)}))")
        elif params.get("zip5"):
            clauses.append("LEFT(COALESCE(loc.zip5, ''), 5) = :zip5")
        elif geo_clauses:
            clauses.extend(geo_clauses)
        if params.get("city_exact"):
            clauses.append("UPPER(COALESCE(loc.city, '')) = :city_exact")
        if params.get("state_exact"):
            clauses.append("UPPER(COALESCE(loc.state, '')) = :state_exact")
        if inferred_sql:
            clauses.append(f"EXISTS (SELECT 1 FROM {PTG2_SCHEMA}.npi_taxonomy nt WHERE nt.npi = loc.npi AND {inferred_sql})")
            clauses.append(_ptg2_individual_npi_exists_sql("loc.npi"))
        if specialty_filter.active:
            clauses.append(
                provider_specialty_taxonomy_exists_sql(
                    "loc.npi",
                    params,
                    "provider_specialty_loc",
                    specialty_filter,
                    schema=PTG2_SCHEMA,
                )
            )
        where = " AND ".join(clauses) or "TRUE"
        component_join = ""
        provider_match_predicate = "FALSE"
        if serving_tables.provider_set_component_table:
            component_join = f"""
                JOIN {serving_tables.provider_set_component_table} psc_filter
                  ON psc_filter.provider_set_hash = r.provider_set_hash"""
            provider_match_predicate = "pgm_filter.provider_group_hash = psc_filter.provider_group_hash"
        elif serving_tables.provider_group_member_table:
            provider_match_predicate = "pgm_filter.provider_group_hash = r.provider_set_hash"
        return (
            f"""
            , filtered_locations AS MATERIALIZED (
                SELECT loc.*
                FROM {serving_tables.provider_group_location_table} loc
                WHERE {where}
                LIMIT :location_rate_candidate_limit
            )
            , provider_filtered_rates AS MATERIALIZED (
                SELECT DISTINCT r.*
                FROM rate_candidates r
                {component_join}
                JOIN {serving_tables.provider_group_member_table} pgm_filter
                  ON {provider_match_predicate}
                JOIN filtered_locations loc ON loc.npi = pgm_filter.npi
                WHERE loc.npi IS NOT NULL
            )
            """,
            True,
        )
    clauses = []
    joins = []
    provider_set_predicate = ""
    if serving_tables.provider_set_component_table:
        joins.append(f"FROM {serving_tables.provider_set_component_table} psc_filter")
        joins.append(f"JOIN {serving_tables.provider_group_member_table} pgm_filter ON pgm_filter.provider_group_hash = psc_filter.provider_group_hash")
        provider_set_predicate = "psc_filter.provider_set_hash = r.provider_set_hash"
    elif serving_tables.provider_group_member_table:
        joins.append(f"FROM {serving_tables.provider_group_member_table} pgm_filter")
        provider_set_predicate = "pgm_filter.provider_group_hash = r.provider_set_hash"
    else:
        return "", False
    if has_geo:
        joins.append(f"JOIN {address_table or f'{PTG2_SCHEMA}.npi_address'} addr_filter ON addr_filter.npi = pgm_filter.npi")
        geo_clauses = []
        if params.get("geo_lat") is not None:
            geo_clauses.append("addr_filter.lat::float8 BETWEEN :geo_min_lat AND :geo_max_lat")
            geo_clauses.append("addr_filter.long::float8 BETWEEN :geo_min_long AND :geo_max_long")
            geo_clauses.append(
                f"{_ptg2_geo_distance_miles_sql('addr_filter.lat::float8', 'addr_filter.long::float8')} <= CAST(:geo_radius_miles AS double precision)"
            )
            if _is_unified_address_table(address_table):
                geo_clauses.append("COALESCE(addr_filter.address_precision, '') <> 'city_zip'")
        if params.get("zip5") and geo_clauses:
            clauses.append(f"(LEFT(COALESCE(addr_filter.postal_code, ''), 5) = :zip5 OR ({' AND '.join(geo_clauses)}))")
        elif params.get("zip5"):
            clauses.append("LEFT(COALESCE(addr_filter.postal_code, ''), 5) = :zip5")
        elif geo_clauses:
            clauses.extend(geo_clauses)
        if params.get("city_exact"):
            clauses.append("UPPER(COALESCE(addr_filter.city_name, '')) = :city_exact")
        if params.get("state_exact"):
            clauses.append("UPPER(COALESCE(addr_filter.state_name, '')) = :state_exact")
    if specialty_filter.active:
        clauses.append(
            provider_specialty_taxonomy_exists_sql(
                "pgm_filter.npi",
                params,
                "provider_specialty",
                specialty_filter,
                schema=PTG2_SCHEMA,
            )
        )
    if inferred_sql:
        clauses.append(f"EXISTS (SELECT 1 FROM {PTG2_SCHEMA}.npi_taxonomy nt WHERE nt.npi = pgm_filter.npi AND {inferred_sql})")
        clauses.append(_ptg2_individual_npi_exists_sql("pgm_filter.npi"))
    where = " AND ".join(clauses) or "TRUE"
    return (
        f"""
        , provider_filtered_rates AS MATERIALIZED (
            SELECT DISTINCT r.*
            FROM rate_candidates r
            WHERE EXISTS (
                SELECT 1
                {' '.join(joins)}
                WHERE {provider_set_predicate}
                  AND {where}
            )
        )
        """,
        True,
    )


def _provider_taxonomy_summary_lateral_sql(npi_sql: str, alias: str = "tax") -> str:
    taxonomy_order_sql = (
        "(UPPER(COALESCE(nt.healthcare_provider_primary_taxonomy_switch, '')) = 'Y') DESC, "
        "nt.checksum"
    )
    return f"""
        LEFT JOIN LATERAL (
            SELECT
                array_agg(nt.healthcare_provider_taxonomy_code ORDER BY {taxonomy_order_sql}) AS taxonomy_codes,
                array_agg(COALESCE(nucc.display_name, nucc.classification) ORDER BY {taxonomy_order_sql}) AS specialties,
                array_remove(array_agg(NULLIF(nucc.classification, '') ORDER BY {taxonomy_order_sql}), NULL) AS classifications,
                array_remove(array_agg(NULLIF(nucc.specialization, '') ORDER BY {taxonomy_order_sql}), NULL) AS specializations,
                (array_agg(COALESCE(nucc.display_name, nucc.classification) ORDER BY {taxonomy_order_sql}))[1] AS primary_specialty,
                (array_remove(array_agg(NULLIF(nucc.specialization, '') ORDER BY {taxonomy_order_sql}), NULL))[1] AS primary_specialization
            FROM {PTG2_SCHEMA}.npi_taxonomy nt
            LEFT JOIN {PTG2_SCHEMA}.nucc_taxonomy nucc
              ON nucc.code = nt.healthcare_provider_taxonomy_code
            WHERE nt.npi = {npi_sql}
        ) {alias} ON TRUE
    """


def _compact_provider_expansion_sql(
    serving_tables: PTG2ServingTables,
    args: dict[str, Any],
    params: dict[str, Any],
    *,
    address_table: str | None = None,
    provider_name_table: str | None = None,
) -> str:
    if not _request_bool(args.get("include_providers")):
        return ""
    resolved_address_table = address_table or f"{PTG2_SCHEMA}.npi_address"
    address_location_hash_sql = _ptg2_address_location_hash_sql("addr", resolved_address_table)
    params.setdefault("provider_match_limit", max(int(params.get("limit") or 25) * 8, 64))
    member_predicates: list[str] = []
    specialty_filter = resolve_provider_specialty_filter(args)
    if specialty_filter.active:
        member_predicates.append(
            provider_specialty_taxonomy_exists_sql(
                "pgm.npi",
                params,
                "provider_expansion_specialty",
                specialty_filter,
                schema=PTG2_SCHEMA,
            )
        )
    inferred_sql = _inferred_provider_taxonomy_code_sql(
        args,
        nt_alias="nt",
        schema=PTG2_SCHEMA,
        params=params,
        param_prefix="provider_expansion_inferred_taxonomy",
    )
    if inferred_sql:
        member_predicates.append(
            f"EXISTS (SELECT 1 FROM {PTG2_SCHEMA}.npi_taxonomy nt WHERE nt.npi = pgm.npi AND {inferred_sql})"
        )
        member_predicates.append(_ptg2_individual_npi_exists_sql("pgm.npi"))
    member_filter_sql = "".join(f"\n          AND {predicate}" for predicate in member_predicates)
    component_join = ""
    member_join = ""
    if serving_tables.provider_set_component_table:
        component_join = f"""
        JOIN {serving_tables.provider_set_component_table} psc
          ON psc.provider_set_hash = r.provider_set_hash"""
        member_join = f"""
        JOIN {serving_tables.provider_group_member_table} pgm
          ON pgm.provider_group_hash = psc.provider_group_hash{member_filter_sql}"""
    elif serving_tables.provider_group_member_table:
        member_join = f"""
        JOIN {serving_tables.provider_group_member_table} pgm
          ON pgm.provider_group_hash = r.provider_set_hash{member_filter_sql}"""
    else:
        return ""
    if serving_tables.provider_group_location_table and (args.get("zip5") or args.get("zip") or args.get("city") or args.get("state") or args.get("lat") or args.get("long")):
        location_order_sql = ""
        if params.get("geo_lat") is not None:
            zip_rank_sql = "0"
            if params.get("zip5"):
                zip_rank_sql = "CASE WHEN LEFT(COALESCE(loc.zip5, ''), 5) = :zip5 THEN 0 ELSE 1 END"
            location_order_sql = f"""
            ORDER BY {zip_rank_sql},
                     {_ptg2_geo_distance_miles_sql('loc.lat::float8', 'loc.lon::float8')} ASC NULLS LAST,
                     loc.location_hash
            """
        return f"""
        {component_join}
        {member_join}
        JOIN LATERAL (
            SELECT loc.*
            FROM {serving_tables.provider_group_location_table} loc
            WHERE loc.npi = pgm.npi
              AND EXISTS (
                  SELECT 1 FROM filtered_locations filtered
                  WHERE filtered.npi = loc.npi
              )
            {location_order_sql}
            OFFSET 0
            LIMIT 1
        ) loc ON TRUE
        {_provider_taxonomy_summary_lateral_sql("pgm.npi")}
        """
    provider_name_join = f"LEFT JOIN {provider_name_table} n ON n.npi = pgm.npi" if provider_name_table else ""
    address_distance_select_sql = (
        "NULL::double precision AS distance_miles, "
        "NULL::varchar AS zip_match_type, "
        "NULL::varchar AS anchor_zip5, "
        "NULL::double precision AS zip_radius_miles"
    )
    address_order_sql = """
            ORDER BY (NULLIF(BTRIM(addr.first_line), '') IS NULL),
                     (addr.type = 'primary') DESC, addr.type, addr.checksum
    """
    if params.get("geo_lat") is not None:
        address_distance_sql = _ptg2_geo_distance_miles_sql("addr.lat::float8", "addr.long::float8")
        if params.get("zip5"):
            same_zip_sql = "LEFT(COALESCE(addr.postal_code, ''), 5) = :zip5"
            address_distance_select_sql = (
                f"CASE WHEN {same_zip_sql} THEN 0.0 ELSE {address_distance_sql} END AS distance_miles, "
                f"CASE WHEN {same_zip_sql} THEN 'same_zip' ELSE 'radius' END AS zip_match_type, "
                ":zip5 AS anchor_zip5, CAST(:geo_radius_miles AS double precision) AS zip_radius_miles"
            )
            zip_rank_sql = f"CASE WHEN {same_zip_sql} THEN 0 ELSE 1 END"
        else:
            address_distance_select_sql = (
                f"{address_distance_sql} AS distance_miles, "
                "'radius' AS zip_match_type, "
                "NULL::varchar AS anchor_zip5, CAST(:geo_radius_miles AS double precision) AS zip_radius_miles"
            )
            zip_rank_sql = "0"
        address_order_sql = f"""
            ORDER BY {zip_rank_sql},
                     distance_miles ASC NULLS LAST,
                     (NULLIF(BTRIM(addr.first_line), '') IS NULL),
                     (addr.type = 'primary') DESC, addr.type, addr.checksum
        """
    return f"""
        {component_join}
        {member_join}
        {provider_name_join}
        LEFT JOIN LATERAL (
            SELECT
                addr.*,
                {address_location_hash_sql} AS location_hash,
                addr.state_name AS state,
                addr.city_name AS city,
                {address_distance_select_sql}
            FROM {resolved_address_table} addr
            WHERE addr.npi = pgm.npi
            {address_order_sql}
            LIMIT 1
        ) addr ON TRUE
        {_provider_taxonomy_summary_lateral_sql("pgm.npi")}
    """


def _compact_item_from_row(data: dict[str, Any], args: dict[str, Any]) -> dict[str, Any]:
    prices = _normalize_price_payload(data.get("prices") or [])
    provider_set_hashes = _coerce_json_payload(data.get("provider_set_hashes"), [])
    provider_set_hash = data.get("provider_set_hash") or (provider_set_hashes[0] if provider_set_hashes else None)
    specialties = _coerce_json_payload(data.get("specialties"), [])
    specializations = _coerce_json_payload(data.get("specializations"), [])
    classifications = _coerce_json_payload(data.get("classifications"), [])
    primary_specialty = data.get("primary_specialty") or (specialties[0] if specialties else None)
    primary_specialization = data.get("primary_specialization") or (specializations[0] if specializations else None)
    address_payload = _coerce_json_payload(data.get("address_payload"), {})
    item = {
        "npi": data.get("npi") or args.get("npi"),
        "provider_ordinal": data.get("provider_ordinal") or data.get("npi") or provider_set_hash,
        "provider_name": data.get("provider_name"),
        "state": data.get("state"),
        "city": data.get("city"),
        "zip5": data.get("zip5"),
        "location_hash": data.get("location_hash"),
        "location_source": data.get("location_source"),
        "location_confidence_code": data.get("location_confidence_code"),
        "address": address_payload,
        "taxonomy_codes": _coerce_json_payload(data.get("taxonomy_codes"), []),
        "specialties": specialties,
        "primary_specialty": primary_specialty,
        "classification": classifications[0] if classifications else None,
        "classifications": classifications,
        "specialization": primary_specialization,
        "primary_specialization": primary_specialization,
        "specializations": specializations,
        "procedure_code": data.get("procedure_code"),
        "hp_procedure_code": data.get("procedure_code"),
        "procedure_name": data.get("procedure_name") or data.get("procedure_display_name"),
        "procedure_description": data.get("procedure_description"),
        "service_code": data.get("billing_code") or data.get("reported_code"),
        "service_code_system": data.get("billing_code_type") or data.get("reported_code_system"),
        "reported_code": data.get("reported_code"),
        "reported_code_system": data.get("reported_code_system"),
        "billing_code": data.get("billing_code") or data.get("reported_code"),
        "billing_code_type": data.get("billing_code_type") or data.get("reported_code_system"),
        "provider_set_hash": provider_set_hash,
        "provider_set_hashes": provider_set_hashes or ([provider_set_hash] if provider_set_hash else []),
        "provider_count": data.get("provider_count"),
        "provider_set_count": data.get("provider_set_count"),
        "price_set_hash": data.get("price_set_hash"),
        "rate_pack_hash": data.get("rate_pack_hash") or data.get("serving_rate_id"),
        "source_key": data.get("source_key") or args.get("source_key"),
        "snapshot_id": data.get("snapshot_id") or args.get("snapshot_id"),
        "network_names": _coerce_str_list_payload(data.get("network_names")),
        **_price_response_fields(prices),
        "source_trace": _coerce_json_payload(
            _first_payload_value(data.get("hydrated_source_trace"), data.get("source_trace")),
            [],
        ),
        "confidence": data.get("confidence") or {"network": "tic_rate_npi_tin"},
    }
    if data.get("distance_miles") is not None:
        item["distance_miles"] = data.get("distance_miles")
    if data.get("zip_match_type") is not None:
        item["zip_match_type"] = data.get("zip_match_type")
    if data.get("anchor_zip5") is not None:
        item["anchor_zip5"] = data.get("anchor_zip5")
    if data.get("zip_radius_miles") is not None:
        item["zip_radius_miles"] = data.get("zip_radius_miles")
    _add_location_phone_fields(item, data, address_payload)
    _promote_address_provenance_fields(item, address_payload)
    item["address_verification"] = _address_verification_payload(item, data, address_payload)
    _apply_ptg_address_display_policy(item, args)
    return {key: value for key, value in item.items() if value is not None}


async def _search_compact_serving_table(
    session,
    table_name: str,
    serving_tables: PTG2ServingTables,
    snapshot_id: str,
    args: dict[str, Any],
    pagination,
    filters: list[str],
    params: dict[str, Any],
    mode_value: str,
) -> dict[str, Any] | None:
    if not _compact_required_tables(serving_tables):
        return None
    params = dict(params)
    params.setdefault("limit", int(pagination.limit))
    params.setdefault("offset", int(pagination.offset))
    price_filter_params: dict[str, Any] = {}
    price_filter_clauses, price_filter_query = _price_filter_clauses(args, price_filter_params)
    params.update(price_filter_params)
    expand_providers = _request_bool(args.get("include_providers"))
    has_geo_filter = bool(
        args.get("zip5")
        or args.get("zip")
        or args.get("city")
        or args.get("state")
        or args.get("lat")
        or args.get("long")
        or args.get("radius_miles")
    )
    uses_location_table = bool(serving_tables.provider_group_location_table and has_geo_filter)
    address_table_sql = None
    if (has_geo_filter and not uses_location_table) or (expand_providers and not uses_location_table):
        address_table_sql = await _ptg2_address_serving_table(
            session,
            _PTG2_UNIFIED_ADDRESS_COLUMNS if has_geo_filter else _PTG2_LEGACY_ADDRESS_COLUMNS,
        )
    provider_name_table = None
    if expand_providers and not uses_location_table:
        candidate_provider_name_table = f"{PTG2_SCHEMA}.npi"
        if await _serving_table_available(session, candidate_provider_name_table):
            provider_name_table = candidate_provider_name_table
    provider_filter_sql, has_provider_filter = _compact_provider_filter_sql(
        serving_tables,
        args,
        params,
        address_table=address_table_sql,
    )
    if has_provider_filter:
        public_limit = max(int(params.get("limit") or 25), 1)
        public_offset = max(int(params.get("offset") or 0), 0)
        params.setdefault(
            "rate_candidate_limit",
            max(
                public_limit * 200,
                public_offset + public_limit,
                int(params.get("location_rate_candidate_limit") or 0),
                4096,
            ),
        )
    source_cte = "provider_filtered_rates" if has_provider_filter else "rate_candidates"
    provider_expansion_sql = _compact_provider_expansion_sql(
        serving_tables,
        args,
        params,
        address_table=address_table_sql,
        provider_name_table=provider_name_table,
    )
    provider_select_sql = ""
    provider_distance_order_sql = ""
    if expand_providers and serving_tables.provider_group_location_table and provider_expansion_sql:
        provider_distance_select_sql = ""
        if params.get("geo_lat") is not None:
            loc_distance_sql = _ptg2_geo_distance_miles_sql("loc.lat::float8", "loc.lon::float8")
            if params.get("zip5"):
                same_zip_sql = "LEFT(COALESCE(loc.zip5, ''), 5) = :zip5"
                provider_distance_select_sql = (
                    f"CASE WHEN {same_zip_sql} THEN 0.0 ELSE {loc_distance_sql} END AS distance_miles, "
                    f"CASE WHEN {same_zip_sql} THEN 'same_zip' ELSE 'radius' END AS zip_match_type, "
                    ":zip5 AS anchor_zip5, CAST(:geo_radius_miles AS double precision) AS zip_radius_miles, "
                )
            else:
                provider_distance_select_sql = (
                    f"{loc_distance_sql} AS distance_miles, "
                    "'radius' AS zip_match_type, NULL::varchar AS anchor_zip5, CAST(:geo_radius_miles AS double precision) AS zip_radius_miles, "
                )
            provider_distance_order_sql = "ORDER BY distance_miles ASC NULLS LAST, r.reported_code_system, r.reported_code"
        provider_select_sql = (
            "loc.npi, loc.location_hash, loc.state, loc.city, loc.zip5, "
            "loc.location_source, loc.location_confidence_code, loc.address_payload, "
            "COALESCE(tax.taxonomy_codes, loc.taxonomy_codes, ARRAY[]::varchar[]) AS taxonomy_codes, "
            "COALESCE(tax.specialties, loc.specialties, ARRAY[]::varchar[]) AS specialties, "
            "COALESCE(tax.classifications, ARRAY[]::varchar[]) AS classifications, "
            "COALESCE(tax.specializations, ARRAY[]::varchar[]) AS specializations, "
            "tax.primary_specialty, tax.primary_specialization, loc.provider_name,"
            f"{provider_distance_select_sql}"
        )
    elif expand_providers:
        address_location_source = _ptg2_address_location_source(address_table_sql)
        provider_name_sql = (
            f"{_ptg2_provider_name_sql('n')} AS provider_name,"
            if provider_name_table
            else "'TiC provider' AS provider_name,"
        )
        provider_select_sql = (
            "pgm.npi, addr.location_hash, addr.state, addr.city, "
            "LEFT(COALESCE(addr.postal_code, ''), 5) AS zip5, "
            f"'{address_location_source}' AS location_source, "
            f"'{address_location_source}' AS location_confidence_code, "
            "(to_jsonb(addr.*) - 'premise_key') AS address_payload, "
            "COALESCE(tax.taxonomy_codes, ARRAY[]::varchar[]) AS taxonomy_codes, "
            "COALESCE(tax.specialties, ARRAY[]::varchar[]) AS specialties, "
            "COALESCE(tax.classifications, ARRAY[]::varchar[]) AS classifications, "
            "COALESCE(tax.specializations, ARRAY[]::varchar[]) AS specializations, "
            "tax.primary_specialty, tax.primary_specialization, "
            "addr.distance_miles, addr.zip_match_type, addr.anchor_zip5, addr.zip_radius_miles, "
            f"{provider_name_sql}"
        )
        if params.get("geo_lat") is not None:
            provider_distance_order_sql = "ORDER BY distance_miles ASC NULLS LAST, r.reported_code_system, r.reported_code"
    price_exists_sql = ""
    if price_filter_clauses:
        price_exists_sql = f"""
        AND EXISTS (
            SELECT 1
            FROM {serving_tables.price_set_entry_table} pse_filter
            JOIN {serving_tables.price_atom_table} pa
              ON pa.price_atom_hash = pse_filter.price_atom_hash
            LEFT JOIN {serving_tables.price_code_set_table} service_set
              ON service_set.price_code_set_hash = pa.service_code_set_hash
            LEFT JOIN {serving_tables.price_code_set_table} modifier_set
              ON modifier_set.price_code_set_hash = pa.modifier_code_set_hash
            WHERE pse_filter.price_set_hash = r.price_set_hash
              AND {' AND '.join(price_filter_clauses)}
        )
        """
    rate_candidate_limit_sql = ":rate_candidate_limit" if has_provider_filter else ":limit"
    rate_candidate_offset_sql = "" if has_provider_filter else "OFFSET :offset"
    final_order_sql = provider_distance_order_sql
    final_pagination_sql = ""
    if has_provider_filter:
        if not final_order_sql:
            final_order_sql = "ORDER BY r.reported_code_system, r.reported_code, r.provider_count DESC NULLS LAST"
        if not expand_providers:
            final_pagination_sql = "LIMIT :limit OFFSET :offset"
    row_stmt = text(
        f"""
        WITH rate_candidates AS MATERIALIZED (
            SELECT r.*
            FROM {table_name} r
            WHERE {' AND '.join(filters)}
              {price_exists_sql}
            ORDER BY r.reported_code_system, r.reported_code, r.provider_count DESC NULLS LAST
            LIMIT {rate_candidate_limit_sql} {rate_candidate_offset_sql}
        )
        {provider_filter_sql}
        SELECT
            {provider_select_sql}
            r.serving_rate_id,
            r.snapshot_id,
            r.plan_id,
            r.plan_name,
            r.plan_id_type,
            COALESCE(r.plan_market_type, NULL::varchar) AS plan_market_type,
            r.issuer_name,
            r.plan_sponsor_name,
            r.procedure_code,
            r.reported_code_system,
            r.reported_code,
            COALESCE(proc.billing_code, r.reported_code) AS billing_code,
            COALESCE(proc.billing_code_type, r.reported_code_system) AS billing_code_type,
            COALESCE(proc.display_name, proc.name) AS procedure_name,
            proc.description AS procedure_description,
            r.provider_set_hash,
            r.provider_count,
            COALESCE(r.provider_set_count, NULL::integer) AS provider_set_count,
            r.price_set_hash,
            price_payload.prices AS prices,
            {_source_trace_payload_sql("r")}
        FROM {source_cte} r
        LEFT JOIN LATERAL (
            SELECT *
            FROM {serving_tables.procedure_table} proc
            WHERE proc.procedure_code = r.procedure_code
            LIMIT 1
        ) proc ON TRUE
        LEFT JOIN LATERAL (
            {_compact_price_payload_sql(serving_tables, params, args)}
        ) price_payload ON TRUE
        {_source_trace_payload_lateral_sql("r")}
        {provider_expansion_sql}
        WHERE TRUE
          {"AND price_payload.prices IS NOT NULL" if price_filter_clauses else ""}
        {final_order_sql}
        {final_pagination_sql}
        """
    )
    result = await session.execute(row_stmt, params)
    rows = [_row_mapping(row) for row in result]
    if not rows:
        return None
    if expand_providers:
        rows = await _overlay_provider_directory_corroboration(
            session,
            rows,
            plan_id=str(args.get("plan_id") or args.get("plan_external_id") or "").strip() or None,
            snapshot_id=snapshot_id,
            source_key=serving_tables.source_key or args.get("source_key"),
        )
    items = [_compact_item_from_row(row, args) for row in rows]
    total_items = int(pagination.offset) + len(items)
    if expand_providers:
        items = _merge_ptg2_provider_rate_items(items)
        items = _sort_ptg2_manifest_provider_items(
            items,
            args,
            location_filter_requested=has_geo_filter,
        )
        total_items = len(items)
        start = max(int(pagination.offset), 0)
        end = start + max(int(pagination.limit), 0)
        items = items[start:end]
    return _shape_ptg2_response(
        {
            "items": items,
            "pagination": {
                "total": total_items,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": (pagination.offset // pagination.limit) + 1 if pagination.limit else 1,
            },
            "query": {
                "plan_id": args.get("plan_id") or None,
                "plan_external_id": args.get("plan_external_id") or None,
                "plan_market_type": str(args.get("plan_market_type") or "").strip().lower() or None,
                "source_key": args.get("source_key") or None,
                "snapshot_id": snapshot_id,
                "mode": mode_value,
                "code": args.get("code") or args.get("reported_code") or None,
                "code_system": args.get("code_system") or None,
                "q": args.get("q") or args.get("service_name") or None,
                "price_filter": price_filter_query or None,
                "state": args.get("state") or None,
                "city": args.get("city") or None,
                "zip5": args.get("zip5") or args.get("zip") or None,
                "zip_radius_miles": args.get("zip_radius_miles") or args.get("radius_miles") or None,
                "source": "ptg2_db",
                "serving_table": table_name,
                "include_providers": expand_providers,
                "result_granularity": "provider" if expand_providers else "provider_set",
                "procedure_consolidation": "REPORTED_CODE",
            },
        },
        args,
    )


async def _search_legacy_serving_table(
    session,
    table_name: str,
    snapshot_id: str,
    args: dict[str, Any],
    pagination,
    filters: list[str],
    params: dict[str, Any],
    mode_value: str,
) -> dict[str, Any] | None:
    count_result = await session.execute(text(f"SELECT COUNT(*) FROM {table_name} r WHERE {' AND '.join(filters)}"), params)
    total = int(count_result.scalar() or 0)
    if total <= 0:
        return None
    row_result = await session.execute(
        text(
            f"""
            SELECT
                r.*,
                {_source_trace_payload_sql("r").replace(" AS source_trace", " AS hydrated_source_trace")}
            FROM {table_name} r
            {_source_trace_payload_lateral_sql("r")}
            WHERE {' AND '.join(filters)}
            ORDER BY r.provider_count DESC NULLS LAST, r.serving_rate_id
            LIMIT :limit OFFSET :offset
            """
        ),
        params,
    )
    items = [_compact_item_from_row(_row_mapping(row), args) for row in row_result]
    if not items:
        return None
    return _shape_ptg2_response(
        {
            "items": items,
            "pagination": {
                "total": total,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": (pagination.offset // pagination.limit) + 1 if pagination.limit else 1,
            },
            "query": {
                "plan_id": args.get("plan_id") or None,
                "plan_external_id": args.get("plan_external_id") or None,
                "plan_market_type": str(args.get("plan_market_type") or "").strip().lower() or None,
                "source_key": args.get("source_key") or None,
                "snapshot_id": snapshot_id,
                "mode": mode_value,
                "code": args.get("code") or args.get("reported_code") or None,
                "code_system": args.get("code_system") or None,
                "source": "ptg2_db",
                "serving_table": table_name,
                "procedure_consolidation": "HP_PROCEDURE_CODE",
            },
        },
        args,
    )


async def search_ptg2_serving_table(
    session,
    snapshot_id: str,
    args: dict[str, Any],
    pagination,
    *,
    serving_tables: PTG2ServingTables | None = None,
) -> dict[str, Any] | None:
    mode_value = normalize_ptg2_mode(args.get("mode"))
    serving_tables = serving_tables or PTG2ServingTables()
    table_name = _safe_table_name(serving_tables.serving_table)
    if _ptg2_manifest_storage_enabled(serving_tables):
        if serving_tables.serving_table:
            return await _search_ptg2_manifest_db_serving_table(
                session,
                snapshot_id,
                args,
                pagination,
                serving_tables,
                mode_value,
            )
        payload = await search_ptg2_manifest_serving_snapshot(
            snapshot_id,
            args,
            pagination,
            serving_tables=serving_tables,
            mode_value=mode_value,
        )
        if payload is None:
            return None
        return _shape_ptg2_manifest_response(payload, args)
    code_context = await _resolve_ptg2_code_search_context(
        session,
        code=args.get("code") or args.get("reported_code"),
        code_system=args.get("code_system") or args.get("reported_code_system"),
    )
    if table_name and await _serving_table_available(session, table_name):
        requested_plan = str(args.get("plan_id") or args.get("plan_external_id") or "").strip()
        filters = ["snapshot_id = :snapshot_id"]
        params: dict[str, Any] = {
            "snapshot_id": snapshot_id,
            "limit": int(pagination.limit),
            "offset": int(pagination.offset),
        }
        if requested_plan:
            filters.append("plan_id = :plan_id")
            params["plan_id"] = requested_plan
        _append_resolved_code_filter(
            filters,
            params,
            code=args.get("code") or args.get("reported_code"),
            code_system=args.get("code_system") or args.get("reported_code_system"),
            code_context=code_context,
        )
        filters = _qualify_compact_filters(filters)
        if _is_compact_serving_table(table_name) or _compact_required_tables(serving_tables):
            return await _search_compact_serving_table(
                session,
                table_name,
                serving_tables,
                snapshot_id,
                args,
                pagination,
                filters,
                params,
                mode_value,
            )
        return await _search_legacy_serving_table(
            session,
            table_name,
            snapshot_id,
            args,
            pagination,
            filters,
            params,
            mode_value,
        )
    return None


async def _search_ptg2_manifest_provider_procedures(
    session,
    npi: int,
    args: dict[str, Any],
    pagination,
    *,
    snapshot_id: str,
    serving_tables: PTG2ServingTables,
) -> dict[str, Any] | None:
    table_name = _safe_table_name(serving_tables.serving_table)
    if not table_name or not await _serving_table_available(session, table_name):
        return None
    provider_set_ids = await _ptg2_manifest_provider_sets_for_npi(session, serving_tables, npi)
    if provider_set_ids is None:
        return None
    if not provider_set_ids:
        return _shape_ptg2_response(
            {
                "items": [],
                "pagination": {
                    "total": 0,
                    "limit": pagination.limit,
                    "offset": pagination.offset,
                    "page": (pagination.offset // pagination.limit) + 1 if pagination.limit else 1,
                },
                "query": {
                    "npi": npi,
                    "plan_id": args.get("plan_id") or None,
                    "plan_external_id": args.get("plan_external_id") or None,
                    "plan_market_type": str(args.get("plan_market_type") or "").strip().lower() or None,
                    "source_key": args.get("source_key") or None,
                    "snapshot_id": snapshot_id,
                    "mode": normalize_ptg2_mode(args.get("mode")),
                    "code": args.get("code") or args.get("reported_code") or None,
                    "code_system": args.get("code_system") or None,
                    "q": args.get("q") or args.get("service_name") or None,
                    "source": "ptg2_db",
                    "serving_table": table_name,
                    "provider_reverse_index": True,
                    "status": "no_match",
                },
            },
            args,
        )

    requested_plan = str(args.get("plan_id") or args.get("plan_external_id") or "").strip()
    code_value = str(args.get("code") or args.get("reported_code") or "").strip()
    q_text = str(args.get("q") or args.get("service_name") or "").strip().lower()
    market_type = str(args.get("plan_market_type") or "").strip().lower()
    params: dict[str, Any] = {
        "provider_set_ids": list(_ptg2_manifest_ids(tuple(provider_set_ids))),
        "limit": int(pagination.limit),
        "offset": int(pagination.offset),
    }
    code_context = await _resolve_ptg2_code_search_context(
        session,
        code=code_value,
        code_system=args.get("code_system"),
    )
    filters = [f"provider_set_global_id_128 = ANY(CAST(:provider_set_ids AS {_ptg2_manifest_id_array_cast(serving_tables)}))"]
    if requested_plan:
        filters.append("plan_id = :plan_id")
        params["plan_id"] = requested_plan
    _append_manifest_reported_code_filter(
        filters,
        params,
        code=code_value,
        code_system=args.get("code_system"),
        code_context=code_context,
    )
    if q_text:
        filters.append(
            """
            (
                LOWER(COALESCE(reported_code, '')) LIKE :q_like
             OR LOWER(COALESCE(reported_code_system, '')) LIKE :q_like
            )
            """
        )
        params["q_like"] = f"%{q_text}%"

    price_filter_params: dict[str, Any] = {}
    _, price_filter_query = _price_filter_clauses(args, price_filter_params)
    has_price_filter = bool(price_filter_query)
    if has_price_filter:
        params["candidate_limit"] = max(int(getattr(pagination, "limit", 25) or 25) * 200, 500)
        limit_sql = "LIMIT :candidate_limit"
        offset_sql = ""
    else:
        limit_sql = "LIMIT :limit"
        offset_sql = "OFFSET :offset"
    where_sql = " AND ".join(filters)
    network_names_select_sql = (
        "network_names"
        if await _ptg2_table_has_columns(session, table_name, {"network_names"})
        else "NULL::varchar[] AS network_names"
    )
    row_stmt = text(
        f"""
            SELECT
                serving_content_hash_128,
                plan_id,
                reported_code_system,
                reported_code,
                procedure_global_id_128,
                provider_set_global_id_128,
                provider_count,
                price_set_global_id_128,
                source_trace_set_hash,
                {network_names_select_sql}
            FROM {table_name}
            WHERE {where_sql}
            ORDER BY reported_code_system, reported_code, provider_count DESC NULLS LAST, serving_content_hash_128
            {limit_sql} {offset_sql}
            """
    )
    row_result = await session.execute(row_stmt, params)
    row_data = [_row_mapping(row) for row in row_result]
    source_traces_by_set = await _ptg2_source_traces_for_trace_sets(
        session,
        [data.get("source_trace_set_hash") for data in row_data],
    )
    prices_by_price_set = await _ptg2_manifest_prices_for_price_sets(
        session,
        serving_tables,
        [_ptg2_manifest_id(data.get("price_set_global_id_128")) for data in row_data],
    )
    procedure_details = await _ptg2_manifest_procedure_details_for_rows(session, row_data)
    provider_context_rows = await _ptg2_manifest_enriched_provider_rows_for_npis(
        session,
        npis=[npi],
        limit=1,
        plan_id=requested_plan or None,
        snapshot_id=snapshot_id,
        source_key=args.get("source_key") or None,
    )
    provider_context = provider_context_rows[0] if provider_context_rows else {"npi": npi}
    item_args = {**args, "snapshot_id": snapshot_id}

    items: list[dict[str, Any]] = []
    skipped_for_offset = 0
    for data in row_data:
        prices = _ptg2_manifest_filter_prices(
            prices_by_price_set.get(_ptg2_manifest_id(data.get("price_set_global_id_128")), []),
            args,
        )
        if has_price_filter and not prices:
            continue
        if has_price_filter and skipped_for_offset < int(pagination.offset):
            skipped_for_offset += 1
            continue
        if len(items) >= int(pagination.limit):
            break
        reported_code = data.get("reported_code")
        reported_system = data.get("reported_code_system")
        procedure_detail = procedure_details.get(_catalog_key(reported_system, reported_code) or ("", ""), {})
        data_with_trace = {
            **data,
            "source_trace": source_traces_by_set.get(str(data.get("source_trace_set_hash") or ""), []),
        }
        items.append(
            _ptg2_manifest_provider_procedure_item(
                npi=npi,
                data=data_with_trace,
                prices=prices,
                procedure_detail=procedure_detail,
                provider_context=provider_context,
                args=item_args,
            )
        )
    total = int(pagination.offset) + len(items)
    if not items:
        total = 0 if not row_data else total

    return _shape_ptg2_response(
        {
            "items": items,
            "pagination": {
                "total": total,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": (pagination.offset // pagination.limit) + 1 if pagination.limit else 1,
            },
            "query": {
                "npi": npi,
                "plan_id": args.get("plan_id") or None,
                "plan_external_id": args.get("plan_external_id") or None,
                "plan_market_type": market_type or None,
                "source_key": args.get("source_key") or None,
                "snapshot_id": snapshot_id,
                "mode": normalize_ptg2_mode(args.get("mode")),
                "code": code_value or None,
                "code_system": args.get("code_system") or None,
                "q": q_text or None,
                "price_filter": price_filter_query or None,
                "source": "ptg2_db",
                "serving_table": table_name,
                "provider_reverse_index": True,
                "status": None if items else "no_match",
                **_ptg2_code_query_fields(code_context, args),
            },
        },
        args,
    )


async def _search_compact_provider_procedures(
    session,
    npi: int,
    args: dict[str, Any],
    pagination,
    *,
    snapshot_id: str,
    serving_tables: PTG2ServingTables,
) -> dict[str, Any] | None:
    table_name = _safe_table_name(serving_tables.serving_table)
    if not table_name or not await _serving_table_available(session, table_name):
        return None
    if not serving_tables.provider_set_component_table or not serving_tables.provider_group_member_table:
        return None
    params: dict[str, Any] = {
        "npi": int(npi),
        "snapshot_id": snapshot_id,
        "limit": int(pagination.limit),
        "offset": int(pagination.offset),
    }
    requested_plan = str(args.get("plan_id") or args.get("plan_external_id") or "").strip()
    market_type = str(args.get("plan_market_type") or "").strip().lower()
    filters = ["r.snapshot_id = :snapshot_id"]
    if requested_plan:
        filters.append("r.plan_id = :plan_id")
        params["plan_id"] = requested_plan
    code_context = await _resolve_ptg2_code_search_context(
        session,
        code=args.get("code") or args.get("reported_code"),
        code_system=args.get("code_system") or args.get("reported_code_system"),
    )
    code_filters: list[str] = []
    _append_resolved_code_filter(
        code_filters,
        params,
        code=args.get("code") or args.get("reported_code"),
        code_system=args.get("code_system") or args.get("reported_code_system"),
        code_context=code_context,
    )
    filters.extend(_qualify_compact_filters(code_filters))
    price_filter_params: dict[str, Any] = {}
    price_filter_clauses, price_filter_query = _price_filter_clauses(args, price_filter_params)
    params.update(price_filter_params)
    price_exists_sql = ""
    if price_filter_clauses and _compact_required_tables(serving_tables):
        price_exists_sql = f"""
          AND EXISTS (
              SELECT 1
              FROM {serving_tables.price_set_entry_table} pse_filter
              JOIN {serving_tables.price_atom_table} pa
                ON pa.price_atom_hash = pse_filter.price_atom_hash
              LEFT JOIN {serving_tables.price_code_set_table} service_set
                ON service_set.price_code_set_hash = pa.service_code_set_hash
              LEFT JOIN {serving_tables.price_code_set_table} modifier_set
                ON modifier_set.price_code_set_hash = pa.modifier_code_set_hash
              WHERE pse_filter.price_set_hash = r.price_set_hash
                AND {' AND '.join(price_filter_clauses)}
          )
        """
    row_stmt = text(
        f"""
        WITH provider_sets AS MATERIALIZED (
            SELECT DISTINCT psc.provider_set_hash
            FROM {serving_tables.provider_set_component_table} psc
            JOIN {serving_tables.provider_group_member_table} pgm
              ON pgm.provider_group_hash = psc.provider_group_hash
            WHERE pgm.npi = :npi
        )
        SELECT
            r.serving_rate_id,
            r.snapshot_id,
            r.plan_id,
            r.plan_name,
            r.plan_id_type,
            NULL::varchar AS plan_market_type,
            r.issuer_name,
            r.plan_sponsor_name,
            r.procedure_code,
            r.reported_code_system,
            r.reported_code,
            COALESCE(proc.billing_code, r.reported_code) AS billing_code,
            COALESCE(proc.billing_code_type, r.reported_code_system) AS billing_code_type,
            COALESCE(proc.display_name, proc.name) AS procedure_name,
            proc.description AS procedure_description,
            r.provider_set_hash,
            r.provider_count,
            NULL::integer AS provider_set_count,
            r.price_set_hash,
            price_payload.prices AS prices,
            {_source_trace_payload_sql("r")}
        FROM {table_name} r
        JOIN provider_sets ps ON ps.provider_set_hash = r.provider_set_hash
        LEFT JOIN {serving_tables.procedure_table or f'{PTG2_SCHEMA}.ptg2_procedure'} proc
          ON proc.procedure_code = r.procedure_code
        LEFT JOIN LATERAL (
            {_compact_price_payload_sql(serving_tables, params, args) if _compact_required_tables(serving_tables) else 'SELECT r.prices AS prices'}
        ) price_payload ON TRUE
        {_source_trace_payload_lateral_sql("r")}
        WHERE {' AND '.join(filters)}
          {price_exists_sql}
          {"AND price_payload.prices IS NOT NULL" if price_filter_clauses else ""}
        ORDER BY r.reported_code_system, r.reported_code, r.provider_count DESC NULLS LAST
        LIMIT :limit OFFSET :offset
        """
    )
    row_result = await session.execute(row_stmt, params)
    rows = [_row_mapping(row) for row in row_result]
    items = []
    for row in rows:
        item = _compact_item_from_row(row, args)
        item["npi"] = int(npi)
        items.append(item)
    if not items:
        return _shape_ptg2_response(
            {
                "items": [],
                "pagination": {"total": 0, "limit": pagination.limit, "offset": pagination.offset, "page": (pagination.offset // pagination.limit) + 1 if pagination.limit else 1},
                "query": {
                    "npi": int(npi),
                    "plan_id": requested_plan or None,
                    "plan_external_id": args.get("plan_external_id") or None,
                    "plan_market_type": market_type or None,
                    "source_key": args.get("source_key") or None,
                    "snapshot_id": snapshot_id,
                    "mode": normalize_ptg2_mode(args.get("mode")),
                    "code": args.get("code") or args.get("reported_code") or None,
                    "code_system": args.get("code_system") or None,
                    "price_filter": price_filter_query or None,
                    "source": "ptg2_db",
                    "serving_table": table_name,
                    "provider_reverse_index": True,
                    "status": "no_match",
                },
            },
            args,
        )
    return _shape_ptg2_response(
        {
            "items": items,
            "pagination": {"total": int(pagination.offset) + len(items), "limit": pagination.limit, "offset": pagination.offset, "page": (pagination.offset // pagination.limit) + 1 if pagination.limit else 1},
            "query": {
                "npi": int(npi),
                "plan_id": requested_plan or None,
                "plan_external_id": args.get("plan_external_id") or None,
                "plan_market_type": market_type or None,
                "source_key": args.get("source_key") or None,
                "snapshot_id": snapshot_id,
                "mode": normalize_ptg2_mode(args.get("mode")),
                "code": args.get("code") or args.get("reported_code") or None,
                "code_system": args.get("code_system") or None,
                "price_filter": price_filter_query or None,
                "source": "ptg2_db",
                "serving_table": table_name,
                "provider_reverse_index": True,
                **_ptg2_code_query_fields(code_context, args),
            },
        },
        args,
    )


async def search_ptg2_provider_procedures(session, npi: int, args: dict[str, Any], pagination) -> dict[str, Any] | None:
    snapshot_id = await resolve_current_ptg2_snapshot_id(session, args)
    if not snapshot_id:
        return None
    serving_tables = await snapshot_serving_tables(session, snapshot_id)
    table_name = _safe_table_name(serving_tables.serving_table)
    if table_name and (_is_compact_serving_table(table_name) or serving_tables.provider_set_component_table):
        return await _search_compact_provider_procedures(
            session,
            npi,
            args,
            pagination,
            snapshot_id=snapshot_id,
            serving_tables=serving_tables,
        )
    if not _ptg2_manifest_storage_enabled(serving_tables):
        return None
    return await _search_ptg2_manifest_provider_procedures(
        session,
        npi,
        args,
        pagination,
        snapshot_id=snapshot_id,
        serving_tables=serving_tables,
    )


def _ptg2_location_filter_requested(args: dict[str, Any]) -> bool:
    return bool(
        args.get("state")
        or args.get("city")
        or args.get("zip5")
        or args.get("zip")
        or args.get("npi")
        or args.get("lat") is not None
        or args.get("long") is not None
        or args.get("radius_miles") is not None
    )


async def _search_one_ptg2_snapshot(
    session, snapshot_id: str, args: dict[str, Any], pagination
) -> dict[str, Any] | None:
    cache_key = _ptg2_response_cache_key(snapshot_id, args, pagination)
    cached_payload = _ptg2_response_cache_get(cache_key)
    if cached_payload is not _CACHE_MISS:
        return cached_payload  # type: ignore[return-value]
    serving_tables = await snapshot_serving_tables(session, snapshot_id)
    db_payload = await search_ptg2_serving_table(
        session,
        snapshot_id,
        args,
        pagination,
        serving_tables=serving_tables,
    )
    if db_payload is not None:
        db_payload = await _enrich_ptg2_code_details(session, db_payload, args)
        return _ptg2_response_cache_set(cache_key, _shape_ptg2_response(db_payload, args))
    return None


async def _search_multi_ptg2_snapshots(
    session,
    network_snapshots: list[tuple[str, str]],
    args: dict[str, Any],
    pagination,
) -> dict[str, Any] | None:
    """Search every network's snapshot for a plan and combine the results.

    A plan can be served by multiple networks/sources at once, each with its own
    snapshot. We query each independently with the caller's filters/sort, then
    re-sort the union on the same key (so the merged page is globally ordered,
    not network-blocked) and slice to the requested window. Items are *unioned,
    not deduplicated*: a provider present in two networks is genuinely two priced
    options (rates are network-specific), and a procedure priced in only one
    network has no overlap to collapse. Each item is tagged with the originating
    network (``source_key``) so a combined result stays attributable.
    """
    # Pull enough from each network to fill the requested page after the merge:
    # the global window [offset, offset+limit) could be satisfied entirely by a
    # single network, so fetch (offset+limit) rows from each, merge, then slice.
    fetch_count = max(1, int(pagination.offset) + int(pagination.limit))
    sub_pagination = PaginationParams(
        page=1,
        limit=fetch_count,
        offset=0,
        source=getattr(pagination, "source", "page"),
    )

    combined: list[dict[str, Any]] = []
    total = 0
    base_query: dict[str, Any] | None = None
    matched_networks: list[dict[str, str]] = []
    for source_key, snapshot_id in network_snapshots:
        payload = await _search_one_ptg2_snapshot(session, snapshot_id, args, sub_pagination)
        if not payload:
            continue
        if base_query is None:
            base_query = dict(payload.get("query") or {})
        page_info = payload.get("pagination") or {}
        try:
            total += int(page_info.get("total") or 0)
        except (TypeError, ValueError):
            pass
        items = payload.get("items") or []
        if items:
            matched_networks.append({"source_key": source_key, "snapshot_id": snapshot_id})
        for item in items:
            # Copy before tagging -- payload items may come from the response
            # cache and must not be mutated in place.
            tagged = dict(item)
            if source_key:
                tagged.setdefault("network", source_key)
            combined.append(tagged)

    if base_query is None:
        # No network produced a payload (e.g. none materialized) -- behave like
        # the single-snapshot path returning no match.
        return None

    combined = _sort_ptg2_manifest_provider_items(
        combined,
        args,
        location_filter_requested=_ptg2_location_filter_requested(args),
    )
    start = max(int(pagination.offset), 0)
    end = start + max(int(pagination.limit), 0)
    page_items = combined[start:end]

    query = dict(base_query)
    query["snapshot_id"] = None
    query["snapshots"] = [snapshot_id for _, snapshot_id in network_snapshots]
    query["networks"] = matched_networks
    query["combined"] = True

    return {
        "items": page_items,
        "pagination": {
            "total": total,
            "limit": pagination.limit,
            "offset": pagination.offset,
            "page": (pagination.offset // pagination.limit) + 1 if pagination.limit else 1,
        },
        "query": query,
    }


async def search_current_ptg2_index(session, args: dict[str, Any], pagination) -> dict[str, Any] | None:
    explicit_snapshot = str(args.get("snapshot_id") or "").strip()
    explicit_source = str(args.get("source_key") or "").strip()
    plan_scoped = bool(str(args.get("plan_id") or args.get("plan_external_id") or "").strip())
    # Plan-scoped queries with no pinned snapshot/network fan out across every
    # network in the plan (a plan can be served by multiple networks, each with
    # its own snapshot) and combine results. A pinned snapshot_id or source_key,
    # or a non-plan query, stays on the original single-snapshot path untouched.
    if plan_scoped and not explicit_snapshot and not explicit_source:
        network_snapshots = await current_source_snapshot_ids_for_plan(session, args)
        if len(network_snapshots) > 1:
            return await _search_multi_ptg2_snapshots(session, network_snapshots, args, pagination)
        if len(network_snapshots) == 1:
            return await _search_one_ptg2_snapshot(
                session, network_snapshots[0][1], args, pagination
            )
        # No per-network snapshot resolved -> fall through to global current.
    snapshot_id = await resolve_current_ptg2_snapshot_id(session, args)
    if not snapshot_id:
        return None
    return await _search_one_ptg2_snapshot(session, snapshot_id, args, pagination)
