# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import math
import hashlib
import json
import os
import re
from collections import OrderedDict, defaultdict
from copy import deepcopy
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Awaitable, Callable, Iterable, Mapping, Sequence

from sqlalchemy import text

from db.connection import db as sa_db

from api.ptg2_address_policy import (
    PTG2_LEGACY_ADDRESS_COLUMNS as _PTG2_LEGACY_ADDRESS_COLUMNS,
    PTG2_UNIFIED_ADDRESS_COLUMNS as _PTG2_UNIFIED_ADDRESS_COLUMNS,
    PTG_CONTACT_DETAIL_FIELDS,
    PTG_NO_DISPLAY_ADDRESS_FIELDS,
    PTG_NO_DISPLAY_VERIFICATION_FIELDS,
)
from api.code_systems import (
    EQUIVALENT_PROCEDURE_CODE_SYSTEMS,
    canonical_catalog_code,
    catalog_code_lookup_values,
    catalog_code_system_lookup_values,
)
from api.endpoint.pagination import PaginationParams
from api.ptg2_code_filters import (
    INFERRED_PROVIDER_TAXONOMY_RULES,
    INTERNAL_PROCEDURE_CODE_SYSTEM,
    InferredProviderTaxonomyRule,
    _normalize_code,
    _normalize_code_system,
    _normalize_npi,
    _ptg2_code_query_fields,
)
from api.ptg2_code_details import _enrich_ptg2_code_details
from api.ptg2_candidate_audit import candidate_audit_access_from_args
from api.ptg2_code_context import (
    _resolve_ptg2_code_search_context,
)
from api.ptg2_snapshot import (
    current_source_snapshot_ids_for_plan,
    resolve_current_ptg2_snapshot_id,
)
from api.ptg2_response import (
    _canonical_catalog_code,
    _catalog_key,
    _coerce_json_payload,
    _coerce_numeric_rate,
    _include_ptg2_sources,
    _normalize_filter_string_list,
    _normalize_price_payload,
    _normalize_string_list,
    _optional_decimal,
    _optional_float,
    _price_response_fields,
    _price_row_key,
    _request_bool,
    _shape_ptg2_response,
)
from api.ptg2_tables import (
    PTG2_V3_ARCH_VERSION,
    snapshot_serving_tables,
)
from api.ptg2_types import PTG2ServingTables
from api.ptg2_db_sidecars import (
    lookup_serving_binary_by_code_from_db,
    lookup_serving_binary_by_code_prefix_from_db,
    lookup_binary_code_batch_from_db,
    lookup_price_ids_from_db,
    serving_binary_code_block_exists,
    has_shared_provider_pages_in_db,
    lookup_shared_code_page_from_db,
    lookup_shared_graph_members_from_db,
    lookup_shared_price_atom_memberships_from_db,
    lookup_shared_price_atoms_from_db,
    lookup_shared_provider_code_keys_from_db,
    lookup_shared_provider_pages_from_db,
)
from api.ptg2_db_serving_v3_pages import (
    PTG2_SERVING_BINARY_V3_PAGE_ROWS,
    PTG2V3PageRecord,
    PTG2V3ProviderPage,
)
from api.ptg2_shared_blocks import (
    PTG2SharedBlockError,
    PTG2_V3_GRAPH_GROUP_TO_NPI,
    PTG2_V3_GRAPH_GROUP_TO_PROVIDER_SET,
    PTG2_V3_GRAPH_NPI_TO_GROUP,
    PTG2_V3_GRAPH_PROVIDER_SET_TO_GROUP,
    fetch_snapshot_source_provenance,
)
from process.ptg_parts.ptg2_shared_blocks import PTG2_V3_SHARED_GENERATION
from process.ptg_parts.ptg2_candidate_attestation import (
    PTG2_CANDIDATE_ATTESTATION_CONTRACT,
)
from process.ext.contact_canon import canonicalize_one
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from api.ptg2_serving_utils import (
    _normalize_zip5,
    _price_filter_clauses,
    _row_mapping,
    _uuid_to_hex,
)
from api.provider_specialty_filters import (
    provider_specialty_taxonomy_exists_sql,
    provider_specialty_taxonomy_semijoin_sql,
    resolve_provider_specialty_filter,
)
from api.provider_demographic_filters import provider_sex_exists_sql

PTG2_MODE_EXACT_SOURCE = "exact_source"
PTG2_MODE_PRODUCT_SEARCH = "product_search"
PTG2_SCHEMA = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
ADDRESS_SERVING_SOURCE_ENV = "HLTHPRT_ADDRESS_SERVING_SOURCE"
ADDRESS_SERVING_SOURCE_UNIFIED = "entity_address_unified"
_PTG2_MANIFEST_TAXONOMY_RATE_CANDIDATE_LIMIT = 25
_PTG2_MULTI_NETWORK_CONCURRENCY_ENV = "HLTHPRT_PTG2_MULTI_NETWORK_CONCURRENCY"
_PTG2_MULTI_NETWORK_CONCURRENCY_DEFAULT = 8
_PTG2_NETWORK_SERVING_TABLES_CACHE_MAX_ENTRIES = 512
_PTG2_NETWORK_SERVING_TABLES_CACHE: OrderedDict[
    str,
    PTG2ServingTables,
] = OrderedDict()
_PTG2_PROVIDER_NPI_PREFIX_CACHE_MAX_ENTRIES = 4096
_PTG2_PROVIDER_NPI_PREFIX_CACHE: OrderedDict[
    tuple[int, str],
    tuple[int, tuple[int, ...], bool],
] = OrderedDict()
_PTG2_FILTERED_PROVIDER_PREFIX_CACHE: OrderedDict[
    tuple[int, str, int, str],
    tuple[int, ...],
] = OrderedDict()
_PTG2_PROVIDER_SET_IDS_BY_NPI_CACHE: OrderedDict[
    tuple[int, int],
    tuple[str, ...],
] = OrderedDict()
_PTG2_NETWORK_SERVING_TABLES_REVALIDATION_SQL = f"""
    SELECT snapshot.snapshot_id,
           binding.snapshot_key,
           layout.layout_manifest->'serving_index'->>'shared_snapshot_key'
               AS layout_snapshot_key,
           layout.layout_manifest->'serving_index'->>'coverage_scope_id'
               AS layout_coverage_scope_id,
           layout.layout_manifest->'serving_index'->>'code_count'
               AS layout_code_count,
           layout.layout_manifest->'serving_index'->>'source_count'
               AS layout_source_count,
           snapshot_scope.plan_id AS snapshot_plan_id,
           snapshot_scope.plan_market_type AS snapshot_plan_market_type,
           encode(snapshot_scope.coverage_scope_id, 'hex')
               AS snapshot_coverage_scope_id,
           attestation.source_key AS attested_source_key,
           encode(attestation.coverage_scope_id, 'hex')
               AS attested_coverage_scope_id,
           encode(attestation.source_set_digest, 'hex')
               AS attested_source_set_digest,
           encode(attestation.audit_sample_digest, 'hex')
               AS attested_audit_sample_digest
      FROM {PTG2_SCHEMA}.ptg2_snapshot snapshot
      JOIN {PTG2_SCHEMA}.ptg2_v3_snapshot_binding binding
        ON binding.snapshot_id = snapshot.snapshot_id
      JOIN {PTG2_SCHEMA}.ptg2_v3_snapshot_layout layout
        ON layout.snapshot_key = binding.snapshot_key
      JOIN {PTG2_SCHEMA}.ptg2_v3_snapshot_scope snapshot_scope
        ON snapshot_scope.snapshot_id = snapshot.snapshot_id
      JOIN {PTG2_SCHEMA}.ptg2_v3_candidate_audit_attestation attestation
        ON attestation.snapshot_id = snapshot.snapshot_id
       AND attestation.snapshot_key = binding.snapshot_key
       AND attestation.coverage_scope_id = snapshot_scope.coverage_scope_id
     WHERE snapshot.snapshot_id = ANY(CAST(:snapshot_ids AS text[]))
       AND snapshot.status = 'published'
       AND layout.state = 'sealed'
       AND layout.generation = :storage_generation
       AND attestation.contract = :attestation_contract
       AND attestation.activated_at IS NOT NULL
       AND attestation.plan_id = snapshot_scope.plan_id
       AND attestation.plan_market_type = snapshot_scope.plan_market_type
"""


def _safe_table_name(value: Any, *, default_schema: str = PTG2_SCHEMA) -> str | None:
    """Validate an auxiliary PostgreSQL relation name before interpolation."""

    if not value:
        return None
    parts = str(value).strip().split(".", 1)
    schema_name, table_name = (default_schema, parts[0]) if len(parts) == 1 else parts
    identifier = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
    if not identifier.fullmatch(schema_name) or not identifier.fullmatch(table_name):
        return None
    return f"{schema_name}.{table_name}"


async def _is_relation_available(session: Any, table_name: str) -> bool:
    """Probe optional reference-data relations, never snapshot serving layouts."""

    try:
        result = await session.execute(
            text("SELECT to_regclass(:table_name)"),
            {"table_name": table_name},
        )
    except Exception:
        return False
    return bool(result.scalar())


@dataclass(frozen=True)
class _ManifestRateScope:
    group_ids: tuple[str, ...]
    group_id_bytes: frozenset[bytes]
    id_count: int


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


def _ptg2_geo_dwithin_sql(lat_sql: str, long_sql: str) -> str:
    """Return an indexable PostGIS radius predicate for address-table geo scans."""
    request_lat = "CAST(:geo_lat AS double precision)"
    request_long = "CAST(:geo_long AS double precision)"
    return (
        "ST_DWithin("
        f"Geography(ST_MakePoint(({long_sql})::double precision, ({lat_sql})::double precision)), "
        f"Geography(ST_MakePoint({request_long}, {request_lat})), "
        "CAST(:geo_radius_miles AS double precision) * 1609.34"
        ")"
    )


def _ptg2_geo_knn_meters_sql(lat_sql: str, long_sql: str) -> str:
    """Return the PostGIS KNN expression matching the unified-address GiST index."""
    request_lat = "CAST(:geo_lat AS double precision)"
    request_long = "CAST(:geo_long AS double precision)"
    return (
        f"Geography(ST_MakePoint(({long_sql})::double precision, ({lat_sql})::double precision)) "
        f"<-> Geography(ST_MakePoint({request_long}, {request_lat}))"
    )


def _ptg2_address_zip5_sql(alias: str, *, unified: bool) -> str:
    """Return the zip5 expression that matches the hot address-serving index."""
    if unified:
        return f"COALESCE({alias}.zip5, LEFT((COALESCE({alias}.postal_code, ''::varchar))::text, 5)::varchar)"
    return f"LEFT(COALESCE({alias}.postal_code, ''), 5)"


def normalize_ptg2_mode(value: str | None) -> str:
    """Normalize and validate the requested PTG2 serving mode."""

    mode = str(value or PTG2_MODE_PRODUCT_SEARCH).strip().lower()
    if mode not in {PTG2_MODE_EXACT_SOURCE, PTG2_MODE_PRODUCT_SEARCH}:
        raise ValueError("mode must be exact_source or product_search")
    return mode


def _is_unified_address_requested() -> bool:
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


def _fallback_contact_fields(
    canonical_phone: Any,
    display_phone: Any,
    canonical_fax: Any,
    display_fax: Any,
    location_data_by_field: Mapping[str, Any],
    address_payload: Mapping[str, Any],
) -> dict[str, str | bool | None]:
    is_phone_fallback_needed = canonical_phone in (None, "", "null") and display_phone not in (
        None,
        "",
        "null",
    )
    is_fax_fallback_needed = canonical_fax in (None, "", "null") and display_fax not in (
        None,
        "",
        "null",
    )
    if not is_phone_fallback_needed and not is_fax_fallback_needed:
        return {}
    return canonicalize_one(
        (
            display_phone,
            display_fax,
            _first_payload_value(
                location_data_by_field.get("country_code"),
                address_payload.get("country_code"),
                "US",
            ),
        )
    )


def _add_location_phone_fields(
    provider_item_by_field: dict[str, Any],
    location_data_by_field: dict[str, Any],
    address_payload: dict[str, Any],
) -> None:
    """Merge display and canonical contact fields into a provider result."""

    display_phone = _first_payload_value(
        location_data_by_field.get("telephone_number"),
        address_payload.get("telephone_number"),
        address_payload.get("telephone"),
        address_payload.get("phone"),
        location_data_by_field.get("phone"),
        location_data_by_field.get("phone_number"),
        address_payload.get("phone_number"),
    )
    canonical_phone = _first_payload_value(
        location_data_by_field.get("phone_number"),
        address_payload.get("phone_number"),
    )
    display_fax = _first_payload_value(
        location_data_by_field.get("fax_number"),
        address_payload.get("fax_number"),
        address_payload.get("fax"),
    )
    canonical_fax = _first_payload_value(
        location_data_by_field.get("fax_number_digits"),
        address_payload.get("fax_number_digits"),
    )
    fallback_contact_by_field = _fallback_contact_fields(
        canonical_phone,
        display_phone,
        canonical_fax,
        display_fax,
        location_data_by_field,
        address_payload,
    )
    if display_phone not in (None, "", "null"):
        provider_item_by_field["telephone_number"] = display_phone
        provider_item_by_field["phone"] = display_phone
    canonical_phone = _first_payload_value(
        canonical_phone,
        fallback_contact_by_field.get("phone_number"),
    )
    if canonical_phone not in (None, "", "null"):
        provider_item_by_field["phone_number"] = canonical_phone
    if display_fax not in (None, "", "null"):
        provider_item_by_field["fax_number"] = display_fax
    for field in PTG_CONTACT_DETAIL_FIELDS:
        contact_value = _first_payload_value(
            location_data_by_field.get(field),
            address_payload.get(field),
            fallback_contact_by_field.get(field),
        )
        if contact_value not in (None, "", "null"):
            provider_item_by_field[field] = contact_value


def _is_truthy_payload(value: Any) -> bool:
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


def _has_payload_value(source: dict[str, Any], *keys: str) -> bool:
    return any(source.get(key) not in (None, "", [], {}) for key in keys)


def _has_displayable_address_fields(source: dict[str, Any]) -> bool:
    if _has_payload_value(source, "first_line", "address_line_1", "street", "street_address"):
        return True
    has_city = _has_payload_value(source, "city", "city_name")
    has_region = _has_payload_value(source, "state", "state_name", "postal_code", "zip5")
    return has_city and has_region


def _has_displayed_address_payload(item: dict[str, Any], address_payload: dict[str, Any]) -> bool:
    for source in (address_payload, item):
        if _has_displayable_address_fields(source):
            return True
        nested_address = source.get("address")
        if isinstance(nested_address, dict) and _has_displayable_address_fields(nested_address):
            return True
    return False


def _has_plan_context_match(address_payload: dict[str, Any]) -> bool:
    if _is_truthy_payload(address_payload.get("provider_directory_plan_context_matched")):
        return True
    evidence = _coerce_json_payload(address_payload.get("address_verification_evidence"), {})
    if isinstance(evidence, dict):
        matched_on = str(evidence.get("matched_on") or "").strip().lower()
        return matched_on.endswith("_plan")
    return False


def _canonical_network_name_payload(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _pre_shaped_network_match_payload(
    network: dict[str, Any],
    ptg_by_key: dict[str, str],
) -> dict[str, Any] | None:
    ptg_name = str(network.get("ptg_network_name") or "").strip()
    provider_directory_name = str(network.get("provider_directory_network_name") or "").strip()
    candidate_key = _canonical_network_name_payload(ptg_name)
    if not candidate_key or candidate_key not in ptg_by_key or not provider_directory_name:
        return None
    network_match_by_field = dict(network)
    network_match_by_field["ptg_network_name"] = ptg_by_key[candidate_key]
    return network_match_by_field


def _provider_directory_network_match_context_payload(
    address_payload: dict[str, Any],
) -> dict[str, Any]:
    evidence_by_field = _coerce_json_payload(
        address_payload.get("address_verification_evidence"),
        {},
    )
    if not isinstance(evidence_by_field, dict):
        evidence_by_field = {}
    address_sources = {
        str(address_source or "").strip().lower().replace("-", "_")
        for address_source in _coerce_str_list_payload(
            address_payload.get("address_sources")
        )
    }
    directory_source = evidence_by_field.get("source") or address_payload.get(
        "provider_directory_source"
    )
    if not directory_source and "provider_directory_fhir" in address_sources:
        directory_source = "provider_directory_fhir"
    context_by_field = {
        "provider_directory_source": directory_source,
        "provider_directory_source_id": (
            address_payload.get("provider_directory_source_id")
            or evidence_by_field.get("source_id")
            or evidence_by_field.get("provider_directory_source_id")
        ),
        "provider_directory_org_name": (
            address_payload.get("provider_directory_org_name")
            or evidence_by_field.get("org_name")
            or evidence_by_field.get("provider_directory_org_name")
        ),
        "provider_directory_plan_name": (
            address_payload.get("provider_directory_plan_name")
            or evidence_by_field.get("plan_name")
            or evidence_by_field.get("provider_directory_plan_name")
        ),
    }
    return {
        field_name: field_value
        for field_name, field_value in context_by_field.items()
        if field_value not in (None, "", [])
    }


def _issuer_network_key_payload(
    context: dict[str, Any],
    candidate_key: str,
) -> dict[str, str]:
    issuer_key = str(context.get("provider_directory_issuer_key") or "").strip() or _canonical_network_name_payload(
        context.get("provider_directory_org_name") or context.get("provider_directory_plan_name")
    )
    if not issuer_key or not candidate_key:
        return {}
    return {
        "provider_directory_issuer_key": issuer_key,
        "provider_directory_issuer_network_match_key": f"{issuer_key}:{candidate_key}",
    }


def _candidate_network_match_payload(
    *,
    ptg_network_name: str,
    provider_directory_network_name: str,
    candidate_key: str,
    network: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    network_context_by_field = {
        key: network.get(key)
        for key in (
            "provider_directory_source",
            "provider_directory_source_id",
            "provider_directory_org_name",
            "provider_directory_plan_name",
            "provider_directory_issuer_key",
        )
        if network.get(key) not in (None, "", [])
    }
    effective_context_by_field = {**network_context_by_field, **context}
    return {
        "ptg_network_name": ptg_network_name,
        "provider_directory_network_name": provider_directory_network_name,
        "provider_directory_network_key": network.get("provider_directory_network_key") or candidate_key,
        "provider_directory_network_resource_id": (
            network.get("resource_id") or network.get("provider_directory_network_resource_id")
        ),
        "provider_directory_network_ref": network.get("ref") or network.get("provider_directory_network_ref"),
        "provider_directory_network_match_method": "canonical_network_name",
        "provider_directory_network_match_confidence": "candidate",
        "provider_directory_network_match_key": candidate_key,
        **effective_context_by_field,
        **_issuer_network_key_payload(
            effective_context_by_field,
            candidate_key,
        ),
    }


def _directory_network_candidates(
    address_payload: Mapping[str, Any],
) -> list[dict[str, Any]]:
    raw_networks = _coerce_json_payload(
        address_payload.get("provider_directory_network_matches"),
        [],
    )
    network_rows = list(raw_networks) if isinstance(raw_networks, list) else []
    network_rows.extend(
        {"name": name}
        for name in _coerce_str_list_payload(
            address_payload.get("provider_directory_network_names")
        )
    )
    return network_rows


def _provider_directory_network_name_matches(
    provider_item_by_field: dict[str, Any],
    address_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    """Return directory network names that corroborate one provider row."""

    ptg_names = _coerce_str_list_payload(
        _first_payload_value(
            provider_item_by_field.get("network_names"),
            address_payload.get("network_names"),
        )
    )
    ptg_by_key = {
        _canonical_network_name_payload(network_name): network_name
        for network_name in ptg_names
        if _canonical_network_name_payload(network_name)
    }
    if not ptg_by_key:
        return []

    network_match_rows: list[dict[str, Any]] = []
    seen_match_keys: set[tuple[str, str, str | None]] = set()
    seen_candidate_keys: set[str] = set()
    context = _provider_directory_network_match_context_payload(address_payload)
    for network in _directory_network_candidates(address_payload):
        if not isinstance(network, dict):
            continue
        pre_shaped_match = _pre_shaped_network_match_payload(network, ptg_by_key)
        if pre_shaped_match:
            candidate_key = _canonical_network_name_payload(pre_shaped_match.get("ptg_network_name"))
            if candidate_key and candidate_key not in seen_candidate_keys:
                seen_candidate_keys.add(candidate_key)
                network_match_rows.append(pre_shaped_match)
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
            if match_key in seen_match_keys:
                continue
            seen_match_keys.add(match_key)
            seen_candidate_keys.add(candidate_key)
            network_match_rows.append(
                _candidate_network_match_payload(
                    ptg_network_name=ptg_by_key[candidate_key],
                    provider_directory_network_name=str(candidate or ""),
                    candidate_key=candidate_key,
                    network=network,
                    context=context,
                )
            )
    return network_match_rows


def _has_network_context_match(
    item: dict[str, Any],
    address_payload: dict[str, Any],
) -> bool:
    if _has_plan_context_match(address_payload):
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
    evidence_by_field = _coerce_json_payload(
        address_payload.get("address_verification_evidence"),
        {},
    )
    if not isinstance(evidence_by_field, dict):
        evidence_by_field = {}
    if not evidence_by_field and not provider_directory_network_name_matches:
        return None
    updated_evidence_by_field = dict(evidence_by_field)
    if provider_directory_network_name_matches and not _has_plan_context_match(address_payload):
        matched_on = str(updated_evidence_by_field.get("matched_on") or "").strip()
        if matched_on and not matched_on.endswith("_network_name"):
            updated_evidence_by_field["matched_on"] = f"{matched_on}_network_name"
        elif not matched_on:
            updated_evidence_by_field["matched_on"] = (
                "npi_address_key_role_location_network_name"
            )
        updated_evidence_by_field["network_name_matches"] = (
            provider_directory_network_name_matches
        )
        updated_evidence_by_field["network_name_context_matched"] = True
    elif not provider_directory_network_name_matches and not _has_plan_context_match(address_payload):
        matched_on = str(updated_evidence_by_field.get("matched_on") or "").strip()
        if matched_on.endswith("_network_name"):
            updated_evidence_by_field["matched_on"] = (
                matched_on.removesuffix("_network_name")
                or "npi_address_key_role_location"
            )
        updated_evidence_by_field.pop("network_name_matches", None)
        updated_evidence_by_field.pop("network_name_context_matched", None)
    return updated_evidence_by_field


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
    provider_item_by_field: dict[str, Any],
    location_data_by_field: dict[str, Any],
    address_payload: dict[str, Any],
) -> dict[str, Any]:
    """Describe the network and address evidence behind one provider row."""

    has_displayed_address = _has_displayed_address_payload(
        provider_item_by_field,
        address_payload,
    )
    if has_displayed_address is False:
        return {
            "rate_network_binding": "tic_provider_group_npi_tin",
            "address_network_binding": "inferred_from_provider_identity",
            "address_evidence_level": "unknown",
            "requires_location_confirmation": True,
            "reason": "PTG proves the provider identity is in network, but no displayable address is available.",
            "displayed_address_present": False,
            "network_bound_address": False,
        }

    location_source = provider_item_by_field.get(
        "location_source"
    ) or location_data_by_field.get("location_source")
    location_confidence_code = provider_item_by_field.get(
        "location_confidence_code"
    ) or location_data_by_field.get("location_confidence_code")
    address_sources = _coerce_str_list_payload(
        _first_payload_value(
            provider_item_by_field.get("address_sources"),
            address_payload.get("address_sources"),
        )
    )
    address_precision = (
        provider_item_by_field.get("address_precision")
        or address_payload.get("address_precision")
        or ("street" if address_payload.get("first_line") else None)
    )
    source_count = _coerce_int_payload(
        _first_payload_value(
            provider_item_by_field.get("source_count"),
            address_payload.get("source_count"),
        )
    )
    source_mask = (
        _coerce_int_payload(
            _first_payload_value(
                provider_item_by_field.get("source_mask"),
                address_payload.get("source_mask"),
            )
        )
        or 0
    )
    address_source_mask = (
        _coerce_int_payload(
            _first_payload_value(
                provider_item_by_field.get("address_source_mask"),
                address_payload.get("address_source_mask"),
            )
        )
        or 0
    )
    is_multi_source_confirmed = _is_truthy_payload(
        _first_payload_value(
            provider_item_by_field.get("multi_source_confirmed"),
            address_payload.get("multi_source_confirmed"),
        )
    )
    if not is_multi_source_confirmed and source_count is not None:
        is_multi_source_confirmed = source_count > 1

    source_markers = {
        str(source_marker or "").strip().lower()
        for source_marker in (
            location_source,
            location_confidence_code,
            provider_item_by_field.get("address_network_binding"),
            location_data_by_field.get("address_network_binding"),
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
    ) and _has_direct_payer_location_record_evidence(
        address_payload
    ) and _has_source_file_version_trace(provider_item_by_field)
    normalized_sources = {
        address_source.lower().replace("-", "_")
        for address_source in address_sources
    }
    provider_directory_address = bool(
        source_markers
        & {
            "provider_directory_fhir",
            "provider_directory_address",
            "payer_provider_directory",
            "payer_directory_corroborated_location",
        }
    ) or bool(normalized_sources & {"provider_directory", "provider_directory_fhir", "payer_provider_directory"})
    provider_directory_network_name_matches = (
        _provider_directory_network_name_matches(
            provider_item_by_field,
            address_payload,
        )
    )
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
    ) and _has_network_context_match(
        provider_item_by_field,
        address_payload,
    )
    direct_mrf_address = bool(address_source_mask & 2) or "mrf" in normalized_sources
    nppes_address = bool(address_source_mask & 1) or "nppes" in normalized_sources
    if direct_ptg_location:
        address_evidence_level = "payer_confirmed_location"
        address_network_binding = "payer_confirmed_location"
        is_location_confirmation_required = False
        reason = "The payer/PTG source supplied the provider location used for this result."
    elif provider_directory_network_location:
        address_evidence_level = "payer_directory_network_location"
        address_network_binding = "payer_directory_corroborated_location"
        is_location_confirmation_required = False
        reason = "A payer Provider Directory record links this provider, network or plan context, and displayed address."
    elif provider_directory_address:
        address_evidence_level = "provider_directory_address"
        address_network_binding = "inferred_from_provider_identity"
        is_location_confirmation_required = True
        reason = "A payer Provider Directory record corroborates the displayed provider address, but the PTG rate file did not supply it."
    elif address_precision == "city_zip":
        address_evidence_level = "city_zip_fallback"
        address_network_binding = "inferred_from_provider_identity"
        is_location_confirmation_required = True
        reason = "PTG proves the provider identity is in network, but only city/ZIP address evidence is available."
    elif direct_mrf_address and is_multi_source_confirmed:
        address_evidence_level = "multi_source_direct_mrf_address"
        address_network_binding = "inferred_from_provider_identity"
        is_location_confirmation_required = True
        reason = "The street address is corroborated by MRF and another source, but not by this PTG rate file."
    elif direct_mrf_address:
        address_evidence_level = "direct_mrf_address"
        address_network_binding = "inferred_from_provider_identity"
        is_location_confirmation_required = True
        reason = "The street address came from MRF provider-address evidence, but not from this PTG rate file."
    elif is_multi_source_confirmed:
        address_evidence_level = "multi_source_provider_address"
        address_network_binding = "inferred_from_provider_identity"
        is_location_confirmation_required = True
        reason = "The street address is corroborated by multiple provider-address sources, but not by this PTG rate file."
    elif nppes_address or str(location_source or "").strip().lower() == "npi_address":
        address_evidence_level = "nppes_provider_address"
        address_network_binding = "inferred_from_provider_identity"
        is_location_confirmation_required = True
        reason = "PTG proves the NPI/TIN is in network; the displayed address comes from NPPES/provider enrichment."
    elif str(location_source or "").strip().lower() == "entity_address_unified":
        address_evidence_level = "unified_provider_address"
        address_network_binding = "inferred_from_provider_identity"
        is_location_confirmation_required = True
        reason = "PTG proves the provider identity is in network; the displayed address comes from unified address evidence."
    else:
        address_evidence_level = "unknown"
        address_network_binding = "inferred_from_provider_identity"
        is_location_confirmation_required = True
        reason = "PTG proves the provider identity is in network, but address provenance is weak or unavailable."

    response_address_sources = list(address_sources)
    if address_network_binding != "payer_confirmed_location":
        response_address_sources = [
            address_source
            for address_source in response_address_sources
            if address_source.lower().replace("-", "_")
            not in {"ptg", "tic", "tic_provider_group"}
        ]
    provider_directory_plan_context = (
        True
        if _has_plan_context_match(address_payload)
        else _optional_bool_payload(address_payload.get("provider_directory_plan_context_matched"))
    )
    provider_directory_network_context_present = _optional_bool_payload(
        address_payload.get("provider_directory_network_context_present")
    )

    verification_by_field = {
        "rate_network_binding": "tic_provider_group_npi_tin",
        "address_network_binding": address_network_binding,
        "address_evidence_level": address_evidence_level,
        "requires_location_confirmation": is_location_confirmation_required,
        "reason": reason,
        "network_bound_address": address_network_binding in {
            "payer_confirmed_location",
            "payer_directory_corroborated_location",
        },
    }
    optional_values_by_field = {
        "displayed_address_present": has_displayed_address,
        "location_source": location_source,
        "location_confidence_code": location_confidence_code,
        "address_precision": address_precision,
        "address_sources": response_address_sources,
        "source_count": source_count,
        "multi_source_confirmed": is_multi_source_confirmed,
        "source_mask": source_mask or None,
        "address_source_mask": address_source_mask or None,
        "provider_directory_source_id": address_payload.get("provider_directory_source_id"),
        "provider_directory_org_name": address_payload.get("provider_directory_org_name"),
        "provider_directory_plan_name": address_payload.get("provider_directory_plan_name"),
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
    for field_name, field_value in optional_values_by_field.items():
        if field_value not in (None, "", []):
            verification_by_field[field_name] = field_value
    return verification_by_field


def _strip_no_display_address_fields(item: dict[str, Any]) -> None:
    verification = item.get("address_verification")
    if not isinstance(verification, dict) or verification.get("displayed_address_present") is not False:
        return
    for key in PTG_NO_DISPLAY_ADDRESS_FIELDS:
        item.pop(key, None)
    for key in PTG_NO_DISPLAY_VERIFICATION_FIELDS:
        verification.pop(key, None)


def _include_unverified_addresses(args: Mapping[str, Any] | dict[str, Any]) -> bool:
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


def _apply_address_display_policy(item: dict[str, Any], args: Mapping[str, Any] | dict[str, Any]) -> None:
    """PTG responses display inferred addresses by default, unless the caller asks to suppress them."""
    verification = item.get("address_verification")
    if not isinstance(verification, dict):
        _strip_no_display_address_fields(item)
        return
    if (
        verification.get("displayed_address_present") is True
        and verification.get("network_bound_address") is not True
        and _is_plan_scoped_ptg_request(args)
        and not _include_unverified_addresses(args)
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


async def _ptg2_table_columns(session, table_name: str) -> frozenset[str]:
    safe_table_name = _safe_table_name(table_name)
    if not safe_table_name:
        return frozenset()
    schema_name, bare_table_name = safe_table_name.split(".", 1)
    try:
        column_result = await session.execute(
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
        column_names: set[str] = set()
        for column_row in column_result:
            column_mapping = getattr(column_row, "_mapping", None)
            if column_mapping is not None:
                column_name = column_mapping.get("column_name")
            else:
                column_name = column_row[0] if column_row else None
            if column_name:
                column_names.add(str(column_name))
        return frozenset(column_names)
    except Exception:
        await _rollback_optional_ptg2_query(session)
        return frozenset()


async def _has_table_columns(session, table_name: str, required_columns: set[str]) -> bool:
    columns = await _ptg2_table_columns(session, table_name)
    return bool(columns) and set(required_columns).issubset(columns)


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
    relation = f"{PTG2_SCHEMA}.provider_directory_address_corroboration"
    if await _is_relation_available(session, relation):
        return relation
    return None


async def _overlay_provider_directory_corroboration(
    session,
    provider_rows: list[dict[str, Any]],
    *,
    plan_id: str | None = None,
    snapshot_id: str | None = None,
    source_key: str | None = None,
) -> list[dict[str, Any]]:
    """Overlay plan-scoped directory corroboration onto provider rows."""

    if not provider_rows or not (plan_id or snapshot_id or source_key):
        return provider_rows
    lookup_pairs: list[tuple[int, str]] = []
    for provider_row in provider_rows:
        npi = provider_row.get("npi")
        address_key = _ptg2_row_address_key(provider_row)
        if npi in (None, "") or not address_key:
            continue
        try:
            lookup_pairs.append((int(npi), address_key))
        except (TypeError, ValueError):
            continue
    if not lookup_pairs:
        return provider_rows
    corroboration_table = await _ptg2_provider_directory_corroboration_table(session)
    if not corroboration_table:
        return provider_rows
    npis = sorted({npi for npi, _address_key in lookup_pairs})
    address_keys = sorted({address_key for _npi, address_key in lookup_pairs})
    try:
        corroboration_query = await session.execute(
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
                    provider_directory_phone_number,
                    provider_directory_phone_extension,
                    provider_directory_fax_number,
                    provider_directory_fax_number_digits,
                    provider_directory_fax_extension,
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
                  AND address_key = ANY(CAST(:address_keys AS uuid[]))
                  AND (:source_key IS NULL OR source_key IS NULL OR source_key = :source_key)
                  AND (:snapshot_id IS NULL OR snapshot_id IS NULL OR snapshot_id = :snapshot_id)
                  AND (
                        :plan_id IS NULL
                     OR (plan_id IS NULL AND ptg_plan_id IS NULL)
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
        return provider_rows
    corroboration_by_key = {
        (
            int(corroboration_by_field.get("npi")),
            str(corroboration_by_field.get("address_key")),
        ): corroboration_by_field
        for corroboration_by_field in (
            _row_mapping(corroboration_record)
            for corroboration_record in corroboration_query
        )
        if corroboration_by_field.get("npi") is not None
        and corroboration_by_field.get("address_key")
    }
    if not corroboration_by_key:
        return provider_rows
    overlaid_provider_rows: list[dict[str, Any]] = []
    for provider_row in provider_rows:
        try:
            match_key = (
                int(provider_row.get("npi")),
                str(_ptg2_row_address_key(provider_row)),
            )
        except (TypeError, ValueError):
            overlaid_provider_rows.append(provider_row)
            continue
        corroboration = corroboration_by_key.get(match_key)
        if not corroboration:
            overlaid_provider_rows.append(provider_row)
            continue
        address_network_binding = (
            str(corroboration.get("address_network_binding") or "payer_directory_corroborated_location").strip()
        )
        if (
            address_network_binding == "payer_directory_corroborated_location"
            and not _is_truthy_payload(corroboration.get("provider_directory_plan_context_matched"))
        ):
            address_network_binding = "provider_directory_address"
        updated_provider_by_field = dict(provider_row)
        updated_provider_by_field["location_source"] = "provider_directory_fhir"
        updated_provider_by_field["location_confidence_code"] = address_network_binding
        if corroboration.get("provider_directory_telephone_number"):
            updated_provider_by_field["telephone_number"] = corroboration.get(
                "provider_directory_telephone_number"
            )
        if corroboration.get("provider_directory_phone_number"):
            updated_provider_by_field["phone_number"] = corroboration.get(
                "provider_directory_phone_number"
            )
        if corroboration.get("provider_directory_phone_extension"):
            updated_provider_by_field["phone_extension"] = corroboration.get(
                "provider_directory_phone_extension"
            )
        if corroboration.get("provider_directory_fax_number"):
            updated_provider_by_field["fax_number"] = corroboration.get(
                "provider_directory_fax_number"
            )
        if corroboration.get("provider_directory_fax_number_digits"):
            updated_provider_by_field["fax_number_digits"] = corroboration.get(
                "provider_directory_fax_number_digits"
            )
        if corroboration.get("provider_directory_fax_extension"):
            updated_provider_by_field["fax_extension"] = corroboration.get(
                "provider_directory_fax_extension"
            )
        address_by_field = _coerce_json_payload(
            updated_provider_by_field.get("address_payload")
            or updated_provider_by_field.get("address"),
            {},
        )
        if not isinstance(address_by_field, dict):
            address_by_field = {}
        address_sources = _coerce_str_list_payload(
            address_by_field.get("address_sources")
        )
        if "provider_directory_fhir" not in address_sources:
            address_sources.append("provider_directory_fhir")
        address_by_field.update(
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
        if updated_provider_by_field.get("telephone_number"):
            address_by_field["telephone_number"] = updated_provider_by_field.get(
                "telephone_number"
            )
        if updated_provider_by_field.get("phone_number"):
            address_by_field["phone_number"] = updated_provider_by_field.get(
                "phone_number"
            )
        if updated_provider_by_field.get("phone_extension"):
            address_by_field["phone_extension"] = updated_provider_by_field.get(
                "phone_extension"
            )
        if updated_provider_by_field.get("fax_number"):
            address_by_field["fax_number"] = updated_provider_by_field.get(
                "fax_number"
            )
        if updated_provider_by_field.get("fax_number_digits"):
            address_by_field["fax_number_digits"] = updated_provider_by_field.get(
                "fax_number_digits"
            )
        if updated_provider_by_field.get("fax_extension"):
            address_by_field["fax_extension"] = updated_provider_by_field.get(
                "fax_extension"
            )
        updated_provider_by_field["address_payload"] = address_by_field
        overlaid_provider_rows.append(updated_provider_by_field)
    return overlaid_provider_rows


async def _ptg2_address_serving_table(
    session,
    required_columns: set[str],
    *,
    require_legacy_available: bool = False,
) -> str | None:
    legacy_table = f"{PTG2_SCHEMA}.npi_address"
    if _is_unified_address_requested():
        unified_table = f"{PTG2_SCHEMA}.entity_address_unified"
        if await _has_table_columns(session, unified_table, required_columns):
            return unified_table
    if require_legacy_available and not await _is_relation_available(session, legacy_table):
        return None
    return legacy_table


def _inferred_provider_taxonomy_rule(args: dict[str, Any]) -> InferredProviderTaxonomyRule | None:
    # A source-scoped occurrence or explicit NPI is already an exact provider
    # selection. Do not replace that source evidence with a code-family guess.
    if (
        str(args.get("mode") or "").strip().lower() == "exact_source"
        or _normalize_npi(args.get("npi")) is not None
    ):
        return None
    requested_system = _normalize_code_system(args.get("code_system") or args.get("reported_code_system"))
    requested_code = _normalize_code(args.get("code") or args.get("reported_code"))
    # OpenAPI compatibility paths can arrive with only a 5-digit CPT code.
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


def _shape_ptg2_manifest_response(
    response_by_field: dict[str, Any],
    args: dict[str, Any],
    *,
    database_evidence: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    manifest_response_by_field = dict(response_by_field)
    manifest_response_by_field["query"] = {
        key: field_value
        for key, field_value in dict(
            response_by_field.get("query") or {}
        ).items()
        if key != "result_granularity"
    }
    query_payload = manifest_response_by_field["query"]
    manifest_response_by_field["provenance"] = {
        "arch_version": PTG2_V3_ARCH_VERSION,
        "storage_generation": PTG2_V3_SHARED_GENERATION,
        "database_backend": "postgresql",
        "plan_id": str(
            query_payload.get("plan_id")
            or args.get("plan_id")
            or args.get("plan_external_id")
            or ""
        ).strip(),
        "snapshot_id": str(
            query_payload.get("snapshot_id")
            or args.get("snapshot_id")
            or ""
        ).strip(),
        "source_key": str(
            query_payload.get("source_key")
            or args.get("source_key")
            or ""
        ).strip(),
        "mode": normalize_ptg2_mode(
            query_payload.get("mode") or args.get("mode")
        ),
        "pricing_scope": "plan_scoped_ptg",
    }
    if isinstance(database_evidence, Mapping):
        manifest_response_by_field["provenance"]["database_evidence"] = dict(
            database_evidence
        )
    manifest_response_by_field["items"] = []
    for response_item_by_field in response_by_field.get("items", []):
        shaped_item_by_field = dict(response_item_by_field)
        shaped_item_by_field.pop("service_code", None)
        shaped_item_by_field.pop("service_code_system", None)
        shaped_item_by_field.pop("tic_prices", None)
        manifest_response_by_field["items"].append(shaped_item_by_field)
    return _shape_ptg2_response(manifest_response_by_field, args)


def _ptg2_manifest_id(value: Any) -> str:
    return _uuid_to_hex(value)


def _ptg2_manifest_id_bytes(value: Any) -> bytes:
    manifest_id = _ptg2_manifest_id(value)
    if len(manifest_id) != 32:
        return b""
    try:
        return bytes.fromhex(manifest_id)
    except ValueError:
        return b""


def _ptg2_manifest_ids(values: list[Any] | tuple[Any, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(hex_value for value in values if (hex_value := _ptg2_manifest_id(value))))


def _ptg2_reported_code_lookup_values(code_system: Any, code: Any) -> tuple[str, ...]:
    if code_system:
        return catalog_code_lookup_values(code_system, code)
    value = str(code or "").strip()
    return (value,) if value else ()


def _append_reported_code_value_filter(
    filters: list[str],
    params: dict[str, Any],
    *,
    column: str,
    param_name: str,
    values: tuple[str, ...],
) -> None:
    if not values:
        return
    params[param_name] = values[0]
    if len(values) == 1:
        filters.append(f"{column} = :{param_name}")
        return
    placeholders = [f":{param_name}"]
    for idx, value in enumerate(values[1:], start=1):
        key = f"{param_name}_{idx}"
        params[key] = value
        placeholders.append(f":{key}")
    filters.append(f"{column} IN ({', '.join(placeholders)})")


def _append_reported_code_system_filter(
    filters: list[str],
    params: dict[str, Any],
    *,
    column: str,
    code_system: Any,
) -> None:
    if not code_system:
        filters.append(f"{column} IS NULL")
        return
    _append_reported_code_value_filter(
        filters,
        params,
        column=column,
        param_name="reported_code_system",
        values=catalog_code_system_lookup_values(code_system),
    )


def _canonical_code_metadata_row(row: Any) -> dict[str, Any]:
    """Normalize persisted code-system aliases before response shaping."""

    code_metadata = _row_mapping(row)
    code_metadata["reported_code_system"] = _normalize_code_system(
        code_metadata.get("reported_code_system")
    )
    return code_metadata


def _external_catalog_lookup_pairs(code_context: Mapping[str, Any] | None) -> set[tuple[str, str]]:
    """Expand resolved external codes into compatible persisted lookup pairs."""

    external_pairs: set[tuple[str, str]] = set()
    for resolved_code in (code_context or {}).get("resolved_codes") or []:
        resolved_system = _normalize_code_system(resolved_code.get("code_system"))
        if not resolved_system or resolved_system == INTERNAL_PROCEDURE_CODE_SYSTEM:
            continue
        lookup_systems = catalog_code_system_lookup_values(resolved_system)
        lookup_values = catalog_code_lookup_values(resolved_system, resolved_code.get("code"))
        external_pairs.update(
            (lookup_system, lookup_value)
            for lookup_system in lookup_systems
            for lookup_value in lookup_values
            if lookup_value
        )
    return external_pairs


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
    external_pairs = _external_catalog_lookup_pairs(code_context)
    if not external_pairs and requested_system and requested_system != INTERNAL_PROCEDURE_CODE_SYSTEM:
        external_pairs.update(
            (reported_system, reported_value)
            for reported_system in catalog_code_system_lookup_values(requested_system)
            for reported_value in catalog_code_lookup_values(requested_system, code)
        )
    if not external_pairs and not requested_system:
        params["reported_code"] = requested_code
        filters.append("reported_code = :reported_code")
        return
    if not external_pairs:
        filters.append("FALSE")
        return
    clauses: list[str] = []
    ordered_pairs = sorted(
        external_pairs,
        key=lambda pair: (
            _normalize_code_system(pair[0]) != pair[0],
            pair,
        ),
    )
    for idx, (reported_system, reported_value) in enumerate(ordered_pairs):
        system_key = f"reported_code_system_{idx}"
        code_key = f"reported_code_{idx}"
        params[system_key] = reported_system
        params[code_key] = reported_value
        clauses.append(f"(reported_code_system = :{system_key} AND reported_code = :{code_key})")
    filters.append("(" + " OR ".join(clauses) + ")")


def _require_strict_shared_v3(serving_tables: PTG2ServingTables) -> None:
    if not serving_tables.uses_shared_blocks:
        raise PTG2ManifestArtifactError(
            "only postgres_binary_v3 with the strict shared-block contract is supported; "
            "reimport the snapshot"
        )


def _required_shared_snapshot_key(serving_tables: PTG2ServingTables) -> int:
    if not serving_tables.uses_shared_blocks or serving_tables.shared_snapshot_key is None:
        raise PTG2ManifestArtifactError(
            "PTG2 postgres_binary_v3 snapshot is missing the strict shared-block contract; reimport the snapshot"
        )
    return int(serving_tables.shared_snapshot_key)


def _required_source_count(serving_tables: PTG2ServingTables) -> int:
    _require_strict_shared_v3(serving_tables)
    if serving_tables.source_count is None:
        raise PTG2ManifestArtifactError(
            "PTG2 postgres_binary_v3 snapshot is missing source_count; reimport the snapshot"
        )
    return int(serving_tables.source_count)


def _required_logical_snapshot_id(serving_tables: PTG2ServingTables) -> str:
    snapshot_id = str(serving_tables.snapshot_id or "").strip()
    if not snapshot_id:
        raise PTG2ManifestArtifactError(
            "PTG2 postgres_binary_v3 snapshot is missing its logical snapshot id; reimport the snapshot"
        )
    return snapshot_id


def _shared_v3_code_table() -> str:
    return f"{PTG2_SCHEMA}.ptg2_v3_code"


def _shared_v3_snapshot_scope_table() -> str:
    return f"{PTG2_SCHEMA}.ptg2_v3_snapshot_scope"


def _shared_plan_scope_table() -> str:
    return f"{PTG2_SCHEMA}.ptg2_v3_snapshot_plan_scope"


def _normalized_plan_market_type(value: Any) -> str:
    return str(value or "").strip().lower()


def _shared_v3_code_scope_sql(
    serving_tables: PTG2ServingTables,
    *,
    requested_plan: str = "",
    plan_market_type: str = "",
    code_alias: str = "code_metadata",
    scope_alias: str = "logical_scope",
) -> tuple[str, list[str], dict[str, Any], str]:
    """Join physical code metadata to this snapshot's logical plan mappings."""

    query_params_by_name: dict[str, Any] = {
        "logical_snapshot_id": _required_logical_snapshot_id(serving_tables),
    }
    plan_filters: list[str] = []
    normalized_plan = str(requested_plan or "").strip()
    normalized_market_type = _normalized_plan_market_type(plan_market_type)
    if normalized_plan:
        plan_filters.append("plan_scope.plan_id = :plan_id")
        query_params_by_name["plan_id"] = normalized_plan
    if normalized_market_type:
        plan_filters.append(
            "plan_scope.plan_market_type = :plan_market_type"
        )
        query_params_by_name["plan_market_type"] = normalized_market_type
    plan_filter_sql = (
        " AND " + " AND ".join(plan_filters)
        if plan_filters
        else ""
    )
    join_sql = f"""
        JOIN {_shared_v3_snapshot_scope_table()} physical_scope
          ON physical_scope.snapshot_id = :logical_snapshot_id
         AND physical_scope.coverage_scope_id = {code_alias}.coverage_scope_id
        JOIN LATERAL (
            SELECT plan_scope.plan_id, plan_scope.plan_market_type
              FROM {_shared_plan_scope_table()} plan_scope
             WHERE plan_scope.snapshot_id = :logical_snapshot_id
               {plan_filter_sql}
             ORDER BY plan_scope.plan_id, plan_scope.plan_market_type
             LIMIT 1
        ) {scope_alias} ON TRUE
    """
    filters: list[str] = []
    order_sql = f"{scope_alias}.plan_id, {scope_alias}.plan_market_type"
    return join_sql, filters, query_params_by_name, order_sql


def _shared_v3_provider_set_table() -> str:
    return f"{PTG2_SCHEMA}.ptg2_v3_provider_set"


def _shared_v3_provider_group_table() -> str:
    return f"{PTG2_SCHEMA}.ptg2_v3_provider_group"


def _shared_v3_price_attr_table() -> str:
    return f"{PTG2_SCHEMA}.ptg2_v3_price_attr"


def _append_shared_snapshot_filter(
    serving_tables: PTG2ServingTables,
    filters: list[str],
    params: dict[str, Any],
    *,
    column: str = "snapshot_key",
) -> None:
    filters.append(f"{column} = :shared_snapshot_key")
    params["shared_snapshot_key"] = _required_shared_snapshot_key(serving_tables)


def _ptg2_manifest_serving_content_hash(
    code_key: Any,
    provider_set_key: Any,
    price_set_global_id: Any,
) -> str:
    price_set_id = _ptg2_manifest_id(price_set_global_id)
    return hashlib.md5(f"{int(code_key)}:{int(provider_set_key)}:{price_set_id}".encode("utf-8")).hexdigest()


async def _provider_set_ids_for_keys(
    session,
    serving_tables: PTG2ServingTables,
    provider_set_keys: Iterable[int],
) -> dict[int, str]:
    keys = sorted({int(value) for value in provider_set_keys if value is not None})
    if not keys:
        return {}
    _require_strict_shared_v3(serving_tables)
    query_parameters_by_name = {
        "shared_snapshot_key": _required_shared_snapshot_key(serving_tables),
        "provider_set_keys": keys,
    }
    result = await session.execute(
        text(
            f"""
            SELECT provider_set_key, provider_set_global_id_128
            FROM {_shared_v3_provider_set_table()}
            WHERE snapshot_key = :shared_snapshot_key
              AND provider_set_key = ANY(CAST(:provider_set_keys AS integer[]))
            """
        ),
        query_parameters_by_name,
    )
    return {
        int(data.get("provider_set_key")): _ptg2_manifest_id(data.get("provider_set_global_id_128"))
        for data in (_row_mapping(row) for row in result)
        if data.get("provider_set_key") is not None and _ptg2_manifest_id(data.get("provider_set_global_id_128"))
    }


async def _hydrate_provider_set_network_names(
    session,
    serving_tables: PTG2ServingTables,
    provider_rows: Iterable[dict[str, Any]],
) -> None:
    provider_entries = list(provider_rows)
    provider_set_ids = sorted(
        {
            provider_set_id
            for provider_entry in provider_entries
            if (
                provider_set_id := _ptg2_manifest_id(
                    provider_entry.get("provider_set_global_id_128")
                )
            )
        }
    )
    if not provider_set_ids:
        return
    network_metadata_query = await session.execute(
        text(
            f"""
            SELECT encode(provider_set_global_id_128, 'hex') AS provider_set_id,
                   network_names
              FROM {_shared_v3_provider_set_table()}
             WHERE snapshot_key = :shared_snapshot_key
               AND provider_set_global_id_128 = ANY(CAST(:provider_set_ids AS bytea[]))
            """
        ),
        {
            "shared_snapshot_key": _required_shared_snapshot_key(serving_tables),
            "provider_set_ids": [
                bytes.fromhex(provider_set_id)
                for provider_set_id in provider_set_ids
            ],
        },
    )
    network_names_by_id = {
        str(network_metadata.get("provider_set_id") or ""): _coerce_str_list_payload(
            network_metadata.get("network_names")
        )
        for network_metadata in (
            _row_mapping(network_metadata_record)
            for network_metadata_record in network_metadata_query
        )
    }
    if set(network_names_by_id) != set(provider_set_ids):
        raise PTG2ManifestArtifactError(
            "PTG2 v3 provider-set network metadata is incomplete"
        )
    for provider_entry in provider_entries:
        provider_set_id = _ptg2_manifest_id(
            provider_entry.get("provider_set_global_id_128")
        )
        if provider_set_id:
            provider_entry["network_names"] = network_names_by_id[provider_set_id]


async def _provider_set_keys_for_ids(
    session,
    serving_tables: PTG2ServingTables,
    provider_set_ids: Iterable[str],
) -> dict[str, int]:
    normalized_ids = list(_ptg2_manifest_ids(tuple(provider_set_ids)))
    if not normalized_ids:
        return {}
    _require_strict_shared_v3(serving_tables)
    query_parameters_by_name = {
        "shared_snapshot_key": _required_shared_snapshot_key(serving_tables),
        "provider_set_ids": [bytes.fromhex(provider_set_id) for provider_set_id in normalized_ids],
    }
    result = await session.execute(
        text(
            f"""
            SELECT provider_set_key, provider_set_global_id_128
            FROM {_shared_v3_provider_set_table()}
            WHERE snapshot_key = :shared_snapshot_key
              AND provider_set_global_id_128 = ANY(CAST(:provider_set_ids AS bytea[]))
            """
        ),
        query_parameters_by_name,
    )
    return {
        _ptg2_manifest_id(data.get("provider_set_global_id_128")): int(data.get("provider_set_key"))
        for data in (_row_mapping(row) for row in result)
        if data.get("provider_set_key") is not None and _ptg2_manifest_id(data.get("provider_set_global_id_128"))
    }


async def _version_three_provider_counts_for_keys(
    session,
    serving_tables: PTG2ServingTables,
    provider_set_keys: Iterable[int] | None,
) -> dict[int, int] | None:
    """Read sparse provider counts from existing v3 provider-page blocks."""

    provider_pages = await _version_three_provider_pages_for_keys(
        session,
        serving_tables,
        provider_set_keys,
    )
    if provider_pages is None:
        return None
    return {
        provider_set_key: provider_page.provider_count
        for provider_set_key, provider_page in provider_pages.items()
    }


async def _version_three_provider_pages_for_keys(
    session,
    serving_tables: PTG2ServingTables,
    provider_set_keys: Iterable[int] | None,
) -> Mapping[int, PTG2V3ProviderPage] | None:
    """Read and validate requested provider-page projections once per query."""

    _require_strict_shared_v3(serving_tables)
    if provider_set_keys is None:
        return None
    normalized_keys = tuple(sorted({int(provider_set_key) for provider_set_key in provider_set_keys}))
    if not normalized_keys:
        return {}
    provider_pages = await lookup_shared_provider_pages_from_db(
        session,
        _required_shared_snapshot_key(serving_tables),
        normalized_keys,
        source_count=_required_source_count(serving_tables),
        schema_name=PTG2_SCHEMA,
    )
    if provider_pages is None:
        return None
    if set(provider_pages) != set(normalized_keys):
        raise PTG2ManifestArtifactError(
            "PTG2 v3 provider-page projection is missing a referenced provider set"
        )
    return provider_pages


async def _lookup_shared_forward_rows(
    session,
    serving_tables: PTG2ServingTables,
    code_key: int,
    *,
    provider_set_keys: Iterable[int] | None = None,
    provider_counts_by_key: Mapping[int, int] | None = None,
):
    _require_strict_shared_v3(serving_tables)
    sparse_count_kwargs = (
        {"provider_counts_by_key": provider_counts_by_key}
        if provider_counts_by_key is not None
        else {}
    )
    dictionary_hints = _version_three_page_price_lookup_hints(serving_tables)
    return await lookup_serving_binary_by_code_from_db(
        session,
        int(code_key),
        shared_snapshot_key=_required_shared_snapshot_key(serving_tables),
        source_count=_required_source_count(serving_tables),
        provider_set_keys=provider_set_keys,
        **sparse_count_kwargs,
        **dictionary_hints,
        schema_name=PTG2_SCHEMA,
    )


def _shared_forward_row_window(
    forward_rows: Iterable[Any],
    provider_set_ids_by_key: Mapping[int, str],
    *,
    limit: int | None,
    offset: int,
    descending: bool,
) -> list[Any]:
    """Return the requested ordered serving-row window before response materialization."""
    eligible_rows = [
        forward_row
        for forward_row in forward_rows
        if provider_set_ids_by_key.get(forward_row.provider_set_key)
    ]
    if any(forward_row.price_key is None for forward_row in eligible_rows):
        raise PTG2ManifestArtifactError(
            "PTG2 strict V3 forward row is missing its dense price key"
        )
    ordered_rows = sorted(
        eligible_rows,
        key=lambda forward_row: (
            -int(forward_row.price_key)
            if descending
            else int(forward_row.price_key),
            int(forward_row.provider_set_key),
            int(forward_row.source_key),
            int(forward_row.provider_count or 0),
        ),
    )
    if limit is None:
        return ordered_rows
    start = max(int(offset), 0)
    return ordered_rows[start : start + max(int(limit), 0)]


def _version_three_page_price_lookup_hints(serving_tables: PTG2ServingTables) -> dict[str, Any]:
    item_count = serving_tables.price_dictionary_item_count
    block_bytes = serving_tables.price_dictionary_block_bytes
    if item_count is None or block_bytes is None:
        raise PTG2ManifestArtifactError(
            "PTG2 strict V3 price dictionary metadata is missing; reimport the snapshot"
        )
    return {
        "price_dictionary_item_count": int(item_count),
        "price_dictionary_block_bytes": int(block_bytes),
    }


def _version_three_forward_page_payloads(
    page_entries: tuple[PTG2V3PageRecord, ...],
    code_metadata: Mapping[str, Any],
    provider_ids_by_key: Mapping[int, str],
    price_ids_by_key: Mapping[int, str],
    network_names: list[str],
    limit: int,
    offset: int,
) -> list[dict[str, Any]]:
    """Shape a validated forward projection window as serving rows."""

    start = max(int(offset), 0)
    return [
        {
            "serving_content_hash_128": _ptg2_manifest_serving_content_hash(
                page_entry.code_key,
                page_entry.provider_set_key,
                price_ids_by_key[page_entry.price_key],
            ),
            "code_key": page_entry.code_key,
            "plan_id": code_metadata.get("plan_id"),
            "plan_market_type": code_metadata.get("plan_market_type"),
            "reported_code_system": code_metadata.get("reported_code_system"),
            "reported_code": code_metadata.get("reported_code"),
            "negotiation_arrangement": code_metadata.get(
                "negotiation_arrangement"
            ),
            "billing_code_type_version": code_metadata.get("billing_code_type_version"),
            "source_procedure_name": code_metadata.get("source_name"),
            "source_procedure_description": code_metadata.get("source_description"),
            "procedure_global_id_128": None,
            "provider_set_global_id_128": provider_ids_by_key[page_entry.provider_set_key],
            "_ptg_provider_set_key": page_entry.provider_set_key,
            "provider_count": page_entry.provider_count,
            "price_set_global_id_128": price_ids_by_key[page_entry.price_key],
            "price_key": page_entry.price_key,
            "source_key": page_entry.source_key,
            "network_names": network_names,
        }
        for page_entry in page_entries[start : start + max(int(limit), 0)]
    ]


async def _version_three_forward_page_ids(
    session,
    serving_tables: PTG2ServingTables,
    selected_entries: Sequence[Any],
) -> tuple[dict[int, str], dict[int, str]]:
    provider_keys = {
        page_entry.provider_set_key for page_entry in selected_entries
    }
    provider_ids_by_key = await _provider_set_ids_for_keys(
        session,
        serving_tables,
        provider_keys,
    )
    if set(provider_ids_by_key) != provider_keys:
        raise PTG2ManifestArtifactError(
            "PTG2 v3 forward page references an unknown provider set"
        )
    price_keys = {page_entry.price_key for page_entry in selected_entries}
    price_ids_by_key = await lookup_price_ids_from_db(
        session,
        price_keys,
        **_version_three_page_price_lookup_hints(serving_tables),
        shared_snapshot_key=_required_shared_snapshot_key(serving_tables),
        schema_name=PTG2_SCHEMA,
    )
    if set(price_ids_by_key) != price_keys:
        raise PTG2ManifestArtifactError(
            "PTG2 v3 forward page references an unknown price set"
        )
    return provider_ids_by_key, price_ids_by_key


def _version_three_provider_code_entries(
    provider_pages_by_key: Mapping[int, PTG2V3ProviderPage],
    code_key: int,
) -> tuple[PTG2V3PageRecord, ...] | None:
    """Return complete projected rows for a code, or None at a truncated boundary."""

    selected_entries: list[PTG2V3PageRecord] = []
    for provider_set_key in sorted(provider_pages_by_key):
        provider_page = provider_pages_by_key[provider_set_key]
        page_entries = provider_page.entries
        if not page_entries:
            raise PTG2ManifestArtifactError(
                "PTG2 v3 provider-page projection has no rows"
            )
        selected_entries.extend(
            page_entry
            for page_entry in page_entries
            if page_entry.code_key == code_key
        )
        if (
            provider_page.total_row_count > len(page_entries)
            and page_entries[-1].code_key <= code_key
        ):
            return None
    selected_entries.sort(
        key=lambda page_entry: (
            page_entry.price_key,
            page_entry.provider_set_key,
            page_entry.source_key,
            page_entry.provider_count,
        )
    )
    return tuple(selected_entries)


async def _version_three_provider_filtered_page_rows(
    session,
    serving_tables: PTG2ServingTables,
    *,
    code_metadata: Mapping[str, Any],
    provider_pages_by_key: Mapping[int, PTG2V3ProviderPage],
    network_names: list[str],
    limit: int | None,
    offset: int,
    descending: bool,
) -> list[dict[str, Any]] | None:
    """Materialize an exact provider-filtered code window from provider pages."""

    if descending or code_metadata.get("code_key") is None:
        return None
    page_entries = _version_three_provider_code_entries(
        provider_pages_by_key,
        int(code_metadata["code_key"]),
    )
    if page_entries is None:
        return None
    start = max(int(offset), 0)
    selected_entries = (
        page_entries[start:]
        if limit is None
        else page_entries[start : start + max(int(limit), 0)]
    )
    if not selected_entries:
        return []
    provider_ids_by_key, price_ids_by_key = await _version_three_forward_page_ids(
        session,
        serving_tables,
        selected_entries,
    )
    return _version_three_forward_page_payloads(
        selected_entries,
        code_metadata,
        provider_ids_by_key,
        price_ids_by_key,
        network_names,
        len(selected_entries),
        0,
    )


async def _version_three_forward_page_rows(
    session,
    serving_tables: PTG2ServingTables,
    *,
    code_metadata: Mapping[str, Any],
    source_trace_set_hash: str | None,
    network_names: list[str],
    limit: int,
    offset: int,
    descending: bool = False,
) -> list[dict[str, Any]] | None:
    """Materialize one bounded code page without scanning the full forward block."""

    _require_strict_shared_v3(serving_tables)
    if descending:
        return None
    page_end = max(int(offset), 0) + max(int(limit), 0)
    if page_end > PTG2_SERVING_BINARY_V3_PAGE_ROWS:
        return None
    code_key = code_metadata.get("code_key")
    if code_key is None:
        return None
    page_entries = await lookup_shared_code_page_from_db(
        session,
        _required_shared_snapshot_key(serving_tables),
        int(code_key),
        source_count=_required_source_count(serving_tables),
        schema_name=PTG2_SCHEMA,
    )
    if page_entries is None:
        return None
    total_rows = int(code_metadata.get("rate_count") or 0)
    if total_rows <= 0 or len(page_entries) != min(total_rows, PTG2_SERVING_BINARY_V3_PAGE_ROWS):
        raise PTG2ManifestArtifactError("PTG2 v3 forward page has an invalid row count")
    start = max(int(offset), 0)
    selected_entries = page_entries[start : start + max(int(limit), 0)]
    provider_ids_by_key, price_ids_by_key = await _version_three_forward_page_ids(
        session,
        serving_tables,
        selected_entries,
    )
    return _version_three_forward_page_payloads(
        selected_entries,
        code_metadata,
        provider_ids_by_key,
        price_ids_by_key,
        network_names,
        len(selected_entries),
        0,
    )


async def _shared_rows_for_code(
    session,
    serving_tables: PTG2ServingTables,
    *,
    code_data: Mapping[str, Any],
    provider_set_keys: Iterable[int] | None,
    provider_pages_by_key: Mapping[int, PTG2V3ProviderPage] | None = None,
    source_trace_set_hash: str | None,
    network_names: list[str],
    limit: int | None = None,
    offset: int = 0,
    descending: bool = False,
) -> list[dict[str, Any]] | None:
    """Read strict shared code rows and materialize only one page."""
    code_key = code_data.get("code_key")
    if code_key is None:
        return None
    _require_strict_shared_v3(serving_tables)
    if provider_set_keys is None and limit is not None:
        page_rows = await _version_three_forward_page_rows(
            session,
            serving_tables,
            code_metadata=code_data,
            source_trace_set_hash=source_trace_set_hash,
            network_names=network_names,
            limit=limit,
            offset=offset,
            descending=descending,
        )
        if page_rows is not None:
            return page_rows
        prefix_end = max(int(offset), 0) + max(int(limit), 0)
        if prefix_end > 0:
            prefix_rows = await lookup_serving_binary_by_code_prefix_from_db(
                session,
                int(code_key),
                limit=prefix_end,
                descending=descending,
                shared_snapshot_key=_required_shared_snapshot_key(serving_tables),
                source_count=_required_source_count(serving_tables),
                **_version_three_page_price_lookup_hints(serving_tables),
                schema_name=PTG2_SCHEMA,
            )
            if not prefix_rows:
                await _raise_missing_v3_block(session, serving_tables, int(code_key))
                return []
            selected_rows = prefix_rows[max(int(offset), 0) : prefix_end]
            provider_set_ids_by_key = await _provider_set_ids_for_keys(
                session,
                serving_tables,
                [
                    selected_row.provider_set_key
                    for selected_row in selected_rows
                ],
            )
            if set(provider_set_ids_by_key) != {
                selected_row.provider_set_key for selected_row in selected_rows
            }:
                raise PTG2ManifestArtifactError(
                    "PTG2 v3 provider-set dictionary is missing a prefix-referenced key"
                )
            return [
                _shared_forward_response_row(
                    selected_row,
                    provider_set_ids_by_key[selected_row.provider_set_key],
                    code_data,
                    source_trace_set_hash,
                    network_names,
                )
                for selected_row in selected_rows
            ]
    return await _full_shared_code_rows(
        session,
        serving_tables,
        code_data=code_data,
        provider_set_keys=provider_set_keys,
        provider_pages_by_key=provider_pages_by_key,
        source_trace_set_hash=source_trace_set_hash,
        network_names=network_names,
        limit=limit,
        offset=offset,
        descending=descending,
    )


def _manifest_response_row_order(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        int(row.get("price_key") if row.get("price_key") is not None else 2**32),
        int(
            row.get("_ptg_provider_set_key")
            if row.get("_ptg_provider_set_key") is not None
            else 2**31
        ),
        int(row.get("source_key") or 0),
        int(row.get("provider_count") or 0),
        int(row.get("code_key") or 0),
        str(row.get("reported_code") or ""),
        str(row.get("serving_content_hash_128") or ""),
        str(row.get("plan_id") or ""),
        str(row.get("plan_market_type") or ""),
    )


def _manifest_response_row_order_for_direction(
    row: Mapping[str, Any],
    *,
    descending: bool,
) -> tuple[Any, ...]:
    order = _manifest_response_row_order(row)
    if not descending:
        return order
    return (-int(order[0]), *order[1:])


@dataclass(frozen=True)
class _ManifestCodeReadScope:
    provider_set_keys: tuple[int, ...] | None
    provider_pages_by_key: Mapping[int, PTG2V3ProviderPage] | None
    source_trace_set_hash: str | None
    network_names: list[str]
    descending: bool


async def _version_three_provider_filter_scope(
    session,
    serving_tables: PTG2ServingTables,
    provider_set_keys: Iterable[int] | None,
    source_trace_set_hash: str | None,
    network_names: list[str],
    *,
    descending: bool,
) -> _ManifestCodeReadScope:
    """Normalize one provider filter and read its bounded page projection."""

    normalized_keys = (
        tuple(sorted({int(provider_set_key) for provider_set_key in provider_set_keys}))
        if provider_set_keys is not None
        else None
    )
    provider_pages_by_key = (
        await _version_three_provider_pages_for_keys(
            session,
            serving_tables,
            normalized_keys,
        )
        if normalized_keys
        and len(normalized_keys) <= _PTG2_VERSION_THREE_PAGE_PROVIDER_SET_LIMIT
        and not descending
        else None
    )
    return _ManifestCodeReadScope(
        provider_set_keys=normalized_keys,
        provider_pages_by_key=provider_pages_by_key,
        source_trace_set_hash=source_trace_set_hash,
        network_names=network_names,
        descending=descending,
    )


async def _shared_rows_for_scope(
    session,
    serving_tables: PTG2ServingTables,
    code_data: Mapping[str, Any],
    read_scope: _ManifestCodeReadScope,
    *,
    limit: int | None,
    offset: int,
) -> list[dict[str, Any]] | None:
    """Read one code with a provider projection shared across variants."""

    return await _shared_rows_for_code(
        session, serving_tables, code_data=code_data,
        provider_set_keys=read_scope.provider_set_keys,
        provider_pages_by_key=read_scope.provider_pages_by_key,
        source_trace_set_hash=read_scope.source_trace_set_hash,
        network_names=read_scope.network_names, limit=limit, offset=offset,
        descending=read_scope.descending,
    )


async def _merge_manifest_code_variant_rows(
    session,
    serving_tables: PTG2ServingTables,
    *,
    code_rows: list[Mapping[str, Any]],
    provider_set_keys: Iterable[int] | None,
    source_trace_set_hash: str | None,
    network_names: list[str],
    limit: int | None,
    offset: int,
    descending: bool = False,
) -> list[dict[str, Any]] | None:
    """Merge one ordered serving window across compatible persisted code forms."""

    read_scope = await _version_three_provider_filter_scope(
        session, serving_tables, provider_set_keys,
        source_trace_set_hash, network_names, descending=descending,
    )
    if len(code_rows) == 1:
        return await _shared_rows_for_scope(
            session, serving_tables, code_rows[0], read_scope,
            limit=limit, offset=offset,
        )
    start = max(int(offset), 0)
    per_code_limit = None if limit is None else start + max(int(limit), 0)
    logical_rows_by_code_key: dict[int, list[Mapping[str, Any]]] = {}
    for code_row in code_rows:
        if code_row.get("code_key") is None:
            continue
        logical_rows_by_code_key.setdefault(int(code_row["code_key"]), []).append(code_row)
    combined_rows: list[dict[str, Any]] = []
    for logical_code_rows in logical_rows_by_code_key.values():
        physical_code_row = logical_code_rows[0]
        variant_rows = await _shared_rows_for_scope(
            session, serving_tables, physical_code_row, read_scope,
            limit=per_code_limit, offset=0,
        )
        if variant_rows is None:
            return None
        for logical_code_row in logical_code_rows:
            combined_rows.extend(
                {
                    **variant_row,
                    "plan_id": logical_code_row.get("plan_id"),
                    "plan_market_type": logical_code_row.get("plan_market_type"),
                }
                for variant_row in variant_rows
            )
    combined_rows.sort(
        key=lambda row: _manifest_response_row_order_for_direction(
            row,
            descending=descending,
        )
    )
    if limit is None:
        return combined_rows[start:]
    return combined_rows[start : start + max(int(limit), 0)]


async def _version_three_projected_code_rows(
    session,
    serving_tables: PTG2ServingTables,
    code_data: Mapping[str, Any],
    provider_pages_by_key: Mapping[int, PTG2V3ProviderPage] | None,
    network_names: list[str],
    limit: int | None,
    offset: int,
    descending: bool,
) -> tuple[list[dict[str, Any]] | None, Mapping[int, int] | None]:
    """Use complete provider pages or return counts for the full-block fallback."""

    if provider_pages_by_key is None:
        return None, None
    projected_rows = await _version_three_provider_filtered_page_rows(
        session,
        serving_tables,
        code_metadata=code_data,
        provider_pages_by_key=provider_pages_by_key,
        network_names=network_names,
        limit=limit,
        offset=offset,
        descending=descending,
    )
    if projected_rows is not None:
        return projected_rows, None
    return None, {
        provider_set_key: provider_page.provider_count
        for provider_set_key, provider_page in provider_pages_by_key.items()
    }


async def _full_shared_code_rows(
    session,
    serving_tables: PTG2ServingTables,
    *,
    code_data: Mapping[str, Any],
    provider_set_keys: Iterable[int] | None,
    provider_pages_by_key: Mapping[int, PTG2V3ProviderPage] | None,
    source_trace_set_hash: str | None,
    network_names: list[str],
    limit: int | None,
    offset: int,
    descending: bool,
) -> list[dict[str, Any]]:
    """Read and materialize an authoritative complete by-code block."""

    code_key = int(code_data["code_key"])
    projected_rows, provider_counts_by_key = await _version_three_projected_code_rows(
        session, serving_tables, code_data, provider_pages_by_key,
        network_names, limit, offset, descending,
    )
    if projected_rows is not None:
        return projected_rows
    forward_rows = await _lookup_shared_forward_rows(
        session,
        serving_tables,
        code_key,
        provider_set_keys=provider_set_keys,
        provider_counts_by_key=provider_counts_by_key,
    )
    if not forward_rows:
        await _raise_missing_v3_block(session, serving_tables, code_key)
        return []
    provider_set_ids_by_key = await _provider_set_ids_for_keys(
        session,
        serving_tables,
        [forward_entry.provider_set_key for forward_entry in forward_rows],
    )
    if (
        set(provider_set_ids_by_key)
        != {forward_entry.provider_set_key for forward_entry in forward_rows}
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 v3 provider-set dictionary is missing a referenced key"
        )
    return _materialize_full_shared_rows(
        forward_rows, provider_set_ids_by_key, code_data,
        source_trace_set_hash, network_names, limit, offset, descending,
    )


def _materialize_full_shared_rows(
    forward_rows: Iterable[Any],
    provider_set_ids_by_key: Mapping[int, str],
    code_data: Mapping[str, Any],
    source_trace_set_hash: str | None,
    network_names: list[str],
    limit: int | None,
    offset: int,
    descending: bool,
) -> list[dict[str, Any]]:
    """Order complete forward rows and shape their response payloads."""

    ordered_rows = _shared_forward_row_window(
        forward_rows,
        provider_set_ids_by_key,
        limit=limit,
        offset=offset,
        descending=descending,
    )
    response_rows: list[dict[str, Any]] = []
    for forward_row in ordered_rows:
        provider_set_id = provider_set_ids_by_key.get(forward_row.provider_set_key)
        if not provider_set_id:
            continue
        response_rows.append(
            _shared_forward_response_row(
                forward_row,
                provider_set_id,
                code_data,
                source_trace_set_hash,
                network_names,
            )
        )
    return response_rows


def _shared_forward_response_row(
    forward_row: Any,
    provider_set_id: str,
    code_data: Mapping[str, Any],
    source_trace_set_hash: str | None,
    network_names: list[str],
) -> dict[str, Any]:
    """Shape one decoded shared forward row for the serving response."""

    return {
        "serving_content_hash_128": _ptg2_manifest_serving_content_hash(
            forward_row.code_key,
            forward_row.provider_set_key,
            forward_row.price_set_global_id_128,
        ),
        "code_key": int(forward_row.code_key),
        "plan_id": code_data.get("plan_id"),
        "plan_market_type": code_data.get("plan_market_type"),
        "reported_code_system": code_data.get("reported_code_system"),
        "reported_code": code_data.get("reported_code"),
        "negotiation_arrangement": code_data.get("negotiation_arrangement"),
        "billing_code_type_version": code_data.get("billing_code_type_version"),
        "source_procedure_name": code_data.get("source_name"),
        "source_procedure_description": code_data.get("source_description"),
        "procedure_global_id_128": None,
        "provider_set_global_id_128": provider_set_id,
        "_ptg_provider_set_key": int(forward_row.provider_set_key),
        "provider_count": forward_row.provider_count,
        "price_set_global_id_128": forward_row.price_set_global_id_128,
        "price_key": getattr(forward_row, "price_key", None),
        "source_key": int(forward_row.source_key),
        "network_names": network_names,
    }


async def _raise_missing_v3_block(
    session: Any,
    serving_tables: PTG2ServingTables,
    code_key: int,
) -> None:
    """Raise when a v3 code reference has no persisted forward artifact block."""

    _require_strict_shared_v3(serving_tables)
    if not await serving_binary_code_block_exists(
        session,
        code_key,
        shared_snapshot_key=_required_shared_snapshot_key(serving_tables),
        schema_name=PTG2_SCHEMA,
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 v3 forward artifact is missing a referenced code block"
        )


_PTG2_PROVIDER_REVERSE_TEXT_FILTER_SQL = """
            (
                LOWER(COALESCE(reported_code, '')) LIKE :q_like
             OR LOWER(COALESCE(reported_code_system, '')) LIKE :q_like
            )
            """


def _append_provider_reverse_text_filter(
    filters: list[str],
    params: dict[str, Any],
    q_text: str,
) -> None:
    if not q_text:
        return
    filters.append(_PTG2_PROVIDER_REVERSE_TEXT_FILTER_SQL)
    params["q_like"] = f"%{q_text}%"


@dataclass(frozen=True)
class _VersionThreeReverseQuery:
    provider_set_ids: Iterable[str]
    requested_plan: str
    code_value: str
    code_system: Any
    q_text: str
    code_context: dict[str, Any] | None
    source_trace_set_hash: str | None
    network_names: list[str]
    limit: int | None
    offset: int
    apply_window: bool
    plan_market_type: str = ""


async def _manifest_reverse_code_rows(
    session,
    serving_tables: PTG2ServingTables,
    *,
    requested_plan: str,
    code_value: str,
    code_system: Any,
    q_text: str,
    code_context: dict[str, Any] | None,
    code_keys: Iterable[int] | None = None,
    limit_rows: int | None = None,
    offset_rows: int = 0,
    plan_market_type: str = "",
) -> list[dict[str, Any]] | None:
    """Query strict V3 code metadata for a provider-reverse selection window."""
    _require_strict_shared_v3(serving_tables)
    scope_join_sql, filters, params, plan_order = _shared_v3_code_scope_sql(
        serving_tables,
        requested_plan=requested_plan,
        plan_market_type=plan_market_type,
    )
    filters.append("code_metadata.snapshot_key = :shared_snapshot_key")
    params["shared_snapshot_key"] = _required_shared_snapshot_key(serving_tables)
    normalized_code_keys = sorted(
        {int(code_key_value) for code_key_value in code_keys or () if code_key_value is not None}
    )
    if normalized_code_keys:
        filters.append("code_metadata.code_key = ANY(CAST(:code_keys AS integer[]))")
        params["code_keys"] = normalized_code_keys
    _append_manifest_reported_code_filter(
        filters,
        params,
        code=code_value,
        code_system=code_system,
        code_context=code_context,
    )
    _append_provider_reverse_text_filter(filters, params, q_text)
    where_sql = "WHERE " + " AND ".join(filters) if filters else ""
    window_sql = ""
    if limit_rows is not None:
        window_sql = "LIMIT :code_row_limit OFFSET :code_row_offset"
        params["code_row_limit"] = max(int(limit_rows), 0)
        params["code_row_offset"] = max(int(offset_rows or 0), 0)
    code_row_result = await session.execute(
        text(
            f"""
            SELECT code_metadata.code_key,
                   logical_scope.plan_id,
                   logical_scope.plan_market_type,
                   code_metadata.reported_code_system,
                   code_metadata.reported_code,
                   code_metadata.negotiation_arrangement,
                   code_metadata.billing_code_type_version,
                   code_metadata.source_name,
                   code_metadata.source_description,
                   code_metadata.rate_count
            FROM {_shared_v3_code_table()} code_metadata
            {scope_join_sql}
            {where_sql}
            ORDER BY {plan_order},
                     code_metadata.reported_code_system,
                     code_metadata.reported_code,
                     code_metadata.negotiation_arrangement,
                     code_metadata.code_key
            {window_sql}
            """
        ),
        params,
    )
    return [_canonical_code_metadata_row(code_row) for code_row in code_row_result]


def _version_three_candidate_rows(
    code_metadata_rows: Iterable[Mapping[str, Any]],
    forward_entries_by_code: Mapping[int, Iterable[Any]],
    provider_set_id_by_key: Mapping[int, str],
    source_trace_set_hash: str | None,
    network_names: list[str],
) -> list[list[dict[str, Any]]]:
    """Build exactly ordered reverse candidates without collapsing duplicates."""

    candidate_row_groups: list[list[dict[str, Any]]] = []
    for code_metadata in code_metadata_rows:
        if code_metadata.get("code_key") is None:
            continue
        code_key = int(code_metadata["code_key"])
        code_candidates = [
            {
                "serving_content_hash_128": _ptg2_manifest_serving_content_hash(
                    code_key,
                    forward_entry.provider_set_key,
                    forward_entry.price_set_global_id_128,
                ),
                "plan_id": code_metadata.get("plan_id"),
                "plan_market_type": code_metadata.get("plan_market_type"),
                "reported_code_system": code_metadata.get("reported_code_system"),
                "reported_code": code_metadata.get("reported_code"),
                "negotiation_arrangement": code_metadata.get(
                    "negotiation_arrangement"
                ),
                "billing_code_type_version": code_metadata.get("billing_code_type_version"),
                "source_procedure_name": code_metadata.get("source_name"),
                "source_procedure_description": code_metadata.get("source_description"),
                "procedure_global_id_128": None,
                "provider_set_global_id_128": provider_set_id_by_key.get(forward_entry.provider_set_key),
                "provider_count": forward_entry.provider_count,
                "price_set_global_id_128": forward_entry.price_set_global_id_128,
                "price_key": forward_entry.price_key,
                "source_key": int(forward_entry.source_key),
                "network_names": network_names,
            }
            for forward_entry in forward_entries_by_code.get(code_key, ())
            if provider_set_id_by_key.get(forward_entry.provider_set_key)
        ]
        code_candidates.sort(
            key=lambda candidate: (
                -(int(candidate.get("provider_count") or 0)),
                _ptg2_manifest_id(candidate.get("provider_set_global_id_128")),
                _ptg2_manifest_id(candidate.get("price_set_global_id_128")),
                int(candidate.get("source_key") or 0),
            )
        )
        candidate_row_groups.append(code_candidates)
    return candidate_row_groups


_PTG2_VERSION_THREE_REVERSE_CODE_BATCH_SIZE = 128
_PTG2_VERSION_THREE_REVERSE_INITIAL_BATCH_SIZE = 1
_PTG2_VERSION_THREE_PAGE_PROVIDER_SET_LIMIT = 64


@dataclass(frozen=True)
class _VersionThreeReverseScope:
    provider_set_id_by_key: Mapping[int, str]
    candidate_code_keys: tuple[int, ...]
    exact_code_metadata_rows: tuple[Mapping[str, Any], ...] | None = None


async def _version_three_exact_code_metadata(
    session,
    serving_tables: PTG2ServingTables,
    reverse_query: _VersionThreeReverseQuery,
) -> tuple[Mapping[str, Any], ...]:
    """Resolve exact code metadata without expanding provider reverse sets."""

    code_metadata_rows = await _manifest_reverse_code_rows(
        session,
        serving_tables,
        requested_plan=reverse_query.requested_plan,
        code_value=reverse_query.code_value,
        code_system=reverse_query.code_system,
        q_text=reverse_query.q_text,
        code_context=reverse_query.code_context,
        plan_market_type=reverse_query.plan_market_type,
    )
    if code_metadata_rows is None:
        raise PTG2ManifestArtifactError("PTG2 v3 code dictionary is unavailable")
    return tuple(code_metadata_rows)


@dataclass
class _VersionThreeRowWindow:
    limit: int | None
    remaining_offset: int
    candidates: list[dict[str, Any]] = field(default_factory=list)
    rows_seen: int = 0

    @property
    def is_full(self) -> bool:
        """Return whether the requested candidate count has been collected."""

        return self.limit is not None and len(self.candidates) >= self.limit

    def add_code_candidates(self, code_candidates: list[dict[str, Any]]) -> None:
        """Consume one ordered code group while preserving duplicate rows."""

        if self.is_full:
            return
        self.rows_seen += len(code_candidates)
        if self.remaining_offset >= len(code_candidates):
            self.remaining_offset -= len(code_candidates)
            return
        local_start = self.remaining_offset
        self.remaining_offset = 0
        if self.limit is None:
            self.candidates.extend(code_candidates[local_start:])
            return
        remaining_capacity = self.limit - len(self.candidates)
        self.candidates.extend(code_candidates[local_start : local_start + remaining_capacity])


def _version_three_row_window(reverse_query: _VersionThreeReverseQuery) -> _VersionThreeRowWindow:
    row_limit = None if reverse_query.limit is None else max(int(reverse_query.limit), 0)
    row_offset = max(int(reverse_query.offset or 0), 0) if reverse_query.apply_window else 0
    return _VersionThreeRowWindow(limit=row_limit, remaining_offset=row_offset)


@dataclass(frozen=True)
class _VersionThreeReverseSelection:
    """An exact ordered reverse prefix with honest cardinality metadata."""

    rows: tuple[dict[str, Any], ...]
    exhausted: bool
    total_row_count: int | None = None

    @property
    def total_is_exact(self) -> bool:
        """Return whether the reverse selection has an exact total row count."""
        return self.total_row_count is not None


async def _version_three_scope_code_keys(
    session,
    serving_tables: PTG2ServingTables,
    reverse_query: _VersionThreeReverseQuery,
    provider_set_id_by_key: Mapping[int, str],
) -> tuple[tuple[int, ...], tuple[Mapping[str, Any], ...] | None]:
    """Resolve candidate codes through the cheapest correct direction."""

    if str(reverse_query.code_value or "").strip():
        exact_code_metadata_rows = await _version_three_exact_code_metadata(
            session,
            serving_tables,
            reverse_query,
        )
        return (
            tuple(
                dict.fromkeys(
                    int(code_metadata["code_key"])
                    for code_metadata in exact_code_metadata_rows
                    if code_metadata.get("code_key") is not None
                )
            ),
            exact_code_metadata_rows,
        )
    provider_set_code_keys = await lookup_shared_provider_code_keys_from_db(
        session,
        _required_shared_snapshot_key(serving_tables),
        provider_set_id_by_key,
        schema_name=PTG2_SCHEMA,
    )
    missing_provider_code_keys = set(provider_set_id_by_key).difference(provider_set_code_keys)
    empty_provider_code_keys = {
        provider_set_key
        for provider_set_key, code_keys in provider_set_code_keys.items()
        if not code_keys
    }
    if missing_provider_code_keys or empty_provider_code_keys:
        raise PTG2ManifestArtifactError(
            "PTG2 v3 provider-code artifact is missing a referenced provider set"
        )
    return (
        tuple(
            sorted(
                {
                    code_key
                    for code_keys in provider_set_code_keys.values()
                    for code_key in code_keys
                }
            )
        ),
        None,
    )


async def _version_three_reverse_scope(
    session,
    serving_tables: PTG2ServingTables,
    reverse_query: _VersionThreeReverseQuery,
) -> _VersionThreeReverseScope | None:
    """Resolve provider keys and their candidate codes without forward reads."""

    _require_strict_shared_v3(serving_tables)
    provider_set_key_by_id = await _provider_set_keys_for_ids(
        session,
        serving_tables,
        reverse_query.provider_set_ids,
    )
    if not provider_set_key_by_id:
        return None
    missing_provider_set_ids = set(_ptg2_manifest_ids(tuple(reverse_query.provider_set_ids))).difference(
        provider_set_key_by_id
    )
    if missing_provider_set_ids:
        raise PTG2ManifestArtifactError(
            "PTG2 v3 provider-set dictionary is missing a referenced provider set"
        )
    provider_set_id_by_key = {
        provider_set_key: provider_set_id
        for provider_set_id, provider_set_key in provider_set_key_by_id.items()
    }
    candidate_code_keys, exact_code_metadata_rows = await _version_three_scope_code_keys(
        session,
        serving_tables,
        reverse_query,
        provider_set_id_by_key,
    )
    if not candidate_code_keys:
        return None
    return _VersionThreeReverseScope(
        provider_set_id_by_key=provider_set_id_by_key,
        candidate_code_keys=candidate_code_keys,
        exact_code_metadata_rows=exact_code_metadata_rows,
    )


def _version_three_code_batch_size(reverse_query: _VersionThreeReverseQuery) -> int | None:
    """Start broad scans narrowly while keeping exact-code queries unbounded."""

    if str(reverse_query.code_value or "").strip():
        return None
    return _PTG2_VERSION_THREE_REVERSE_INITIAL_BATCH_SIZE


def _next_version_three_code_batch_size(metadata_batch_size: int | None) -> int | None:
    if metadata_batch_size is None:
        return None
    return min(metadata_batch_size * 2, _PTG2_VERSION_THREE_REVERSE_CODE_BATCH_SIZE)


async def _version_three_forward_entries_for_batch(
    session,
    serving_tables: PTG2ServingTables,
    code_metadata_by_key: Mapping[int, Mapping[str, Any]],
    provider_set_id_by_key: Mapping[int, str],
) -> dict[int, tuple[PTG2ServingBinaryRow, ...]]:
    """Read one sparse v3 forward batch using provider-page count projections."""

    provider_counts_by_key = await _version_three_provider_counts_for_keys(
        session,
        serving_tables,
        provider_set_id_by_key,
    )
    return await lookup_binary_code_batch_from_db(
        session,
        code_metadata_by_key,
        provider_set_keys=provider_set_id_by_key,
        provider_counts_by_key=provider_counts_by_key,
        **_version_three_page_price_lookup_hints(serving_tables),
        shared_snapshot_key=_required_shared_snapshot_key(serving_tables),
        source_count=_required_source_count(serving_tables),
        schema_name=PTG2_SCHEMA,
    )


async def _version_three_candidate_batch(
    session,
    serving_tables: PTG2ServingTables,
    reverse_query: _VersionThreeReverseQuery,
    reverse_scope: _VersionThreeReverseScope,
    metadata_offset: int,
    metadata_batch_size: int | None,
) -> tuple[list[list[dict[str, Any]]], int] | None:
    """Read and materialize one ordered metadata/forward block batch."""

    if reverse_scope.exact_code_metadata_rows is not None:
        code_metadata_rows = (
            list(reverse_scope.exact_code_metadata_rows) if metadata_offset == 0 else []
        )
    else:
        code_metadata_rows = await _manifest_reverse_code_rows(
            session,
            serving_tables,
            requested_plan=reverse_query.requested_plan,
            code_value=reverse_query.code_value,
            code_system=reverse_query.code_system,
            q_text=reverse_query.q_text,
            code_context=reverse_query.code_context,
            code_keys=reverse_scope.candidate_code_keys,
            limit_rows=metadata_batch_size,
            offset_rows=metadata_offset,
            plan_market_type=reverse_query.plan_market_type,
        )
    if code_metadata_rows is None:
        return None
    if not code_metadata_rows:
        return [], 0
    code_metadata_by_key = {
        int(code_metadata["code_key"]): code_metadata
        for code_metadata in code_metadata_rows
        if code_metadata.get("code_key") is not None
    }
    forward_entries_by_code = await _version_three_forward_entries_for_batch(
        session,
        serving_tables,
        code_metadata_by_key,
        reverse_scope.provider_set_id_by_key,
    )
    candidate_row_groups = _version_three_candidate_rows(
        code_metadata_rows,
        forward_entries_by_code,
        reverse_scope.provider_set_id_by_key,
        reverse_query.source_trace_set_hash,
        reverse_query.network_names,
    )
    return candidate_row_groups, len(code_metadata_rows)


def _is_version_three_reverse_page_eligible(reverse_query: _VersionThreeReverseQuery) -> bool:
    requested_end = max(int(reverse_query.offset or 0), 0) + max(int(reverse_query.limit or 0), 0)
    return bool(
        reverse_query.apply_window
        and reverse_query.limit is not None
        and requested_end <= PTG2_SERVING_BINARY_V3_PAGE_ROWS
        and not str(reverse_query.code_value or "").strip()
        and not reverse_query.code_system
        and not reverse_query.q_text
        and not reverse_query.code_context
    )


async def _has_single_plan_page_order(
    session,
    serving_tables: PTG2ServingTables,
    requested_plan: str,
    plan_market_type: str = "",
) -> bool:
    """Allow projected pages only for one logical mapping and one physical scope."""

    _require_strict_shared_v3(serving_tables)
    normalized_plan = str(requested_plan or "").strip()
    normalized_market_type = _normalized_plan_market_type(plan_market_type)
    if normalized_plan and normalized_plan != str(serving_tables.plan_id or "").strip():
        return False
    if normalized_market_type and normalized_market_type != _normalized_plan_market_type(
        serving_tables.plan_market_type or ""
    ):
        return False
    return bool(serving_tables.coverage_scope_id)


@dataclass(frozen=True)
class _VersionThreePageProjectionScope:
    provider_set_id_by_key: Mapping[int, str]
    provider_pages_by_key: Mapping[int, PTG2V3ProviderPage]
    page_entries: tuple[PTG2V3PageRecord, ...]


async def _load_version_three_page_projection(
    session,
    serving_tables: PTG2ServingTables,
    reverse_query: _VersionThreeReverseQuery,
) -> _VersionThreePageProjectionScope | None:
    """Load and validate provider-keyed page blocks for one reverse query."""

    provider_set_key_by_id = await _provider_set_keys_for_ids(
        session,
        serving_tables,
        reverse_query.provider_set_ids,
    )
    normalized_provider_ids = set(_ptg2_manifest_ids(tuple(reverse_query.provider_set_ids)))
    if set(provider_set_key_by_id) != normalized_provider_ids:
        raise PTG2ManifestArtifactError(
            "PTG2 v3 provider-set dictionary is missing a referenced provider set"
        )
    provider_set_id_by_key = {
        provider_set_key: provider_set_id
        for provider_set_id, provider_set_key in provider_set_key_by_id.items()
    }
    provider_pages_by_key = await lookup_shared_provider_pages_from_db(
        session,
        _required_shared_snapshot_key(serving_tables),
        provider_set_id_by_key,
        source_count=_required_source_count(serving_tables),
        schema_name=PTG2_SCHEMA,
    )
    if provider_pages_by_key is None:
        return None
    if set(provider_pages_by_key) != set(provider_set_id_by_key):
        raise PTG2ManifestArtifactError("PTG2 v3 reverse page is missing a referenced provider set")
    page_entries = tuple(
        page_entry
        for provider_set_key in provider_set_id_by_key
        for page_entry in provider_pages_by_key[provider_set_key].entries
    )
    return _VersionThreePageProjectionScope(
        provider_set_id_by_key=provider_set_id_by_key,
        provider_pages_by_key=provider_pages_by_key,
        page_entries=page_entries,
    )


async def _version_three_page_projection_scope(
    session,
    serving_tables: PTG2ServingTables,
    reverse_query: _VersionThreeReverseQuery,
) -> _VersionThreePageProjectionScope | None:
    """Validate page eligibility and load the requested provider projections."""

    if not _is_version_three_reverse_page_eligible(reverse_query):
        return None
    _require_strict_shared_v3(serving_tables)
    if not await has_shared_provider_pages_in_db(
        session,
        _required_shared_snapshot_key(serving_tables),
        schema_name=PTG2_SCHEMA,
    ):
        return None
    try:
        provider_set_count = len(reverse_query.provider_set_ids)
    except TypeError:
        return None
    if provider_set_count > _PTG2_VERSION_THREE_PAGE_PROVIDER_SET_LIMIT:
        return None
    if not await _has_single_plan_page_order(
        session,
        serving_tables,
        reverse_query.requested_plan,
        reverse_query.plan_market_type,
    ):
        return None
    return await _load_version_three_page_projection(
        session,
        serving_tables,
        reverse_query,
    )


def _version_three_ordered_page_entries(
    code_metadata_rows: Iterable[Mapping[str, Any]],
    page_entries: Iterable[PTG2V3PageRecord],
) -> tuple[PTG2V3PageRecord, ...]:
    """Order dense page entries exactly like the authoritative reverse path."""

    entries_by_code: dict[int, list[PTG2V3PageRecord]] = {}
    for page_entry in page_entries:
        entries_by_code.setdefault(page_entry.code_key, []).append(page_entry)
    ordered_entries = []
    for code_metadata in code_metadata_rows:
        code_key = int(code_metadata["code_key"])
        code_entries = entries_by_code.get(code_key, [])
        code_entries.sort(
            key=lambda page_entry: (
                -page_entry.provider_count,
                page_entry.provider_set_key,
                page_entry.price_key,
                page_entry.source_key,
            )
        )
        ordered_entries.extend(code_entries)
    return tuple(ordered_entries)


def _version_three_reverse_page_candidates(
    code_metadata_by_key: Mapping[int, Mapping[str, Any]],
    page_entries: Iterable[PTG2V3PageRecord],
    provider_set_id_by_key: Mapping[int, str],
    price_ids_by_key: Mapping[int, str],
    reverse_query: _VersionThreeReverseQuery,
) -> list[dict[str, Any]]:
    return [
        {
            "serving_content_hash_128": _ptg2_manifest_serving_content_hash(
                page_entry.code_key,
                page_entry.provider_set_key,
                price_ids_by_key[page_entry.price_key],
            ),
            "plan_id": code_metadata_by_key[page_entry.code_key].get("plan_id"),
            "plan_market_type": code_metadata_by_key[page_entry.code_key].get(
                "plan_market_type"
            ),
            "reported_code_system": code_metadata_by_key[page_entry.code_key].get(
                "reported_code_system"
            ),
            "reported_code": code_metadata_by_key[page_entry.code_key].get("reported_code"),
            "negotiation_arrangement": code_metadata_by_key[page_entry.code_key].get(
                "negotiation_arrangement"
            ),
            "billing_code_type_version": code_metadata_by_key[page_entry.code_key].get(
                "billing_code_type_version"
            ),
            "source_procedure_name": code_metadata_by_key[page_entry.code_key].get("source_name"),
            "source_procedure_description": code_metadata_by_key[page_entry.code_key].get(
                "source_description"
            ),
            "procedure_global_id_128": None,
            "provider_set_global_id_128": provider_set_id_by_key[page_entry.provider_set_key],
            "provider_count": page_entry.provider_count,
            "price_set_global_id_128": price_ids_by_key[page_entry.price_key],
            "price_key": page_entry.price_key,
            "source_key": page_entry.source_key,
            "network_names": reverse_query.network_names,
        }
        for page_entry in page_entries
    ]


async def _version_three_page_window(
    session,
    serving_tables: PTG2ServingTables,
    reverse_query: _VersionThreeReverseQuery,
    page_scope: _VersionThreePageProjectionScope,
) -> tuple[dict[int, Mapping[str, Any]], tuple[PTG2V3PageRecord, ...]] | None:
    """Validate projected code metadata and select the requested dense window."""

    code_metadata_rows = await _manifest_reverse_code_rows(
        session,
        serving_tables,
        requested_plan=reverse_query.requested_plan,
        code_value="",
        code_system=None,
        q_text="",
        code_context=None,
        code_keys=(page_entry.code_key for page_entry in page_scope.page_entries),
        plan_market_type=reverse_query.plan_market_type,
    )
    if code_metadata_rows is None:
        return None
    code_metadata_by_key = {
        int(code_metadata["code_key"]): code_metadata
        for code_metadata in code_metadata_rows
        if code_metadata.get("code_key") is not None
    }
    projected_code_keys = {page_entry.code_key for page_entry in page_scope.page_entries}
    if set(code_metadata_by_key) != projected_code_keys:
        raise PTG2ManifestArtifactError("PTG2 v3 reverse page references an unknown code")
    ordered_entries = _version_three_ordered_page_entries(
        code_metadata_rows,
        page_scope.page_entries,
    )
    requested_end = max(int(reverse_query.offset or 0), 0) + max(int(reverse_query.limit or 0), 0)
    has_truncated_page = any(
        provider_page.total_row_count > len(provider_page.entries)
        for provider_page in page_scope.provider_pages_by_key.values()
    )
    if len(ordered_entries) < requested_end and has_truncated_page:
        return None
    start = max(int(reverse_query.offset or 0), 0)
    return code_metadata_by_key, ordered_entries[start:requested_end]


async def _version_three_reverse_page_selection(
    session,
    serving_tables: PTG2ServingTables,
    reverse_query: _VersionThreeReverseQuery,
) -> _VersionThreeReverseSelection | None:
    """Serve a shallow provider page with an exact projected row count."""

    page_scope = await _version_three_page_projection_scope(
        session,
        serving_tables,
        reverse_query,
    )
    if page_scope is None:
        return None
    page_window = await _version_three_page_window(
        session,
        serving_tables,
        reverse_query,
        page_scope,
    )
    if page_window is None:
        return None
    code_metadata_by_key, selected_entries = page_window
    price_ids_by_key = await lookup_price_ids_from_db(
        session,
        (page_entry.price_key for page_entry in selected_entries),
        **_version_three_page_price_lookup_hints(serving_tables),
        shared_snapshot_key=_required_shared_snapshot_key(serving_tables),
        schema_name=PTG2_SCHEMA,
    )
    if set(price_ids_by_key) != {page_entry.price_key for page_entry in selected_entries}:
        raise PTG2ManifestArtifactError("PTG2 v3 reverse page references an unknown price set")
    candidate_rows = tuple(
        _version_three_reverse_page_candidates(
            code_metadata_by_key,
            selected_entries,
            page_scope.provider_set_id_by_key,
            price_ids_by_key,
            reverse_query,
        )
    )
    total_row_count = sum(
        provider_page.total_row_count
        for provider_page in page_scope.provider_pages_by_key.values()
    )
    return _VersionThreeReverseSelection(
        rows=candidate_rows,
        exhausted=(
            max(int(reverse_query.offset or 0), 0) + len(candidate_rows)
            >= total_row_count
        ),
        total_row_count=total_row_count,
    )


async def _version_three_reverse_page_rows(
    session,
    serving_tables: PTG2ServingTables,
    reverse_query: _VersionThreeReverseQuery,
) -> list[dict[str, Any]] | None:
    """Compatibility helper returning only projected reverse rows."""

    selection = await _version_three_reverse_page_selection(
        session,
        serving_tables,
        reverse_query,
    )
    return None if selection is None else list(selection.rows)


async def _version_three_reverse_selection(
    session,
    serving_tables: PTG2ServingTables,
    reverse_query: _VersionThreeReverseQuery,
) -> _VersionThreeReverseSelection:
    """Serve an ordered reverse window without overstating its total count."""

    row_window = _version_three_row_window(reverse_query)
    if row_window.is_full:
        return _VersionThreeReverseSelection(rows=(), exhausted=False)
    page_selection = await _version_three_reverse_page_selection(
        session,
        serving_tables,
        reverse_query,
    )
    if page_selection is not None:
        return page_selection
    reverse_scope = await _version_three_reverse_scope(session, serving_tables, reverse_query)
    if reverse_scope is None:
        return _VersionThreeReverseSelection(rows=(), exhausted=True, total_row_count=0)
    metadata_batch_size = _version_three_code_batch_size(reverse_query)
    metadata_offset = 0
    while not row_window.is_full:
        candidate_batch = await _version_three_candidate_batch(
            session,
            serving_tables,
            reverse_query,
            reverse_scope,
            metadata_offset,
            metadata_batch_size,
        )
        if candidate_batch is None:
            raise PTG2ManifestArtifactError("PTG2 v3 reverse metadata is unavailable")
        candidate_row_groups, code_metadata_count = candidate_batch
        if code_metadata_count == 0:
            return _VersionThreeReverseSelection(
                rows=tuple(row_window.candidates),
                exhausted=True,
                total_row_count=row_window.rows_seen,
            )
        for code_candidates in candidate_row_groups:
            row_window.add_code_candidates(code_candidates)
            if row_window.is_full:
                return _VersionThreeReverseSelection(
                    rows=tuple(row_window.candidates),
                    exhausted=False,
                )
        if metadata_batch_size is None or code_metadata_count < metadata_batch_size:
            return _VersionThreeReverseSelection(
                rows=tuple(row_window.candidates),
                exhausted=True,
                total_row_count=row_window.rows_seen,
            )
        metadata_offset += code_metadata_count
        metadata_batch_size = _next_version_three_code_batch_size(metadata_batch_size)
    return _VersionThreeReverseSelection(rows=tuple(row_window.candidates), exhausted=False)


async def _version_three_reverse_rows(
    session,
    serving_tables: PTG2ServingTables,
    reverse_query: _VersionThreeReverseQuery,
) -> list[dict[str, Any]]:
    """Compatibility helper returning only reverse rows."""

    selection = await _version_three_reverse_selection(
        session,
        serving_tables,
        reverse_query,
    )
    return list(selection.rows)


@dataclass(frozen=True)
class _VersionThreeFilteredReverseSelection:
    """A filtered reverse prefix with prices retained for response shaping."""

    rows: tuple[dict[str, Any], ...]
    prices_by_price_set: Mapping[str, list[dict[str, Any]]]
    exhausted: bool
    matched_rows_seen: int
    total_row_count: int | None = None


async def _version_three_filtered_reverse_selection(
    session,
    serving_tables: PTG2ServingTables,
    reverse_query: _VersionThreeReverseQuery,
    args: Mapping[str, Any],
    *,
    offset: int,
    limit: int,
) -> _VersionThreeFilteredReverseSelection:
    """Read ordered batches until a filtered page sentinel or exhaustion."""

    reverse_scope = await _version_three_reverse_scope(session, serving_tables, reverse_query)
    if reverse_scope is None:
        return _VersionThreeFilteredReverseSelection(
            rows=(),
            prices_by_price_set={},
            exhausted=True,
            matched_rows_seen=0,
            total_row_count=0,
        )
    requested_offset = max(int(offset), 0)
    requested_limit = max(int(limit), 0)
    filter_args_by_name = dict(args)
    selected_rows: list[dict[str, Any]] = []
    selected_prices_by_set: dict[str, list[dict[str, Any]]] = {}
    matched_rows_seen = 0
    metadata_offset = 0
    metadata_batch_size = (
        None
        if reverse_scope.exact_code_metadata_rows is not None
        else _PTG2_VERSION_THREE_REVERSE_CODE_BATCH_SIZE
    )
    while True:
        candidate_batch = await _version_three_candidate_batch(
            session,
            serving_tables,
            reverse_query,
            reverse_scope,
            metadata_offset,
            metadata_batch_size,
        )
        if candidate_batch is None:
            raise PTG2ManifestArtifactError("PTG2 v3 reverse metadata is unavailable")
        candidate_row_groups, code_metadata_count = candidate_batch
        if code_metadata_count == 0:
            return _VersionThreeFilteredReverseSelection(
                rows=tuple(selected_rows),
                prices_by_price_set=selected_prices_by_set,
                exhausted=True,
                matched_rows_seen=matched_rows_seen,
                total_row_count=matched_rows_seen,
            )
        candidate_rows = [
            candidate_row
            for row_group in candidate_row_groups
            for candidate_row in row_group
        ]
        price_key_by_set_id = {
            _ptg2_manifest_id(
                candidate_row.get("price_set_global_id_128")
            ): int(candidate_row["price_key"])
            for candidate_row in candidate_rows
            if candidate_row.get("price_key") is not None
            and _ptg2_manifest_id(
                candidate_row.get("price_set_global_id_128")
            )
        }
        prices_by_price_set = await _prices_for_price_sets(
            session,
            serving_tables,
            tuple(price_key_by_set_id),
            price_key_by_set_id=price_key_by_set_id,
        )
        for candidate_row in candidate_rows:
            price_set_id = _ptg2_manifest_id(
                candidate_row.get("price_set_global_id_128")
            )
            prices = _ptg2_manifest_filter_prices(
                prices_by_price_set.get(price_set_id, []),
                filter_args_by_name,
            )
            if not prices:
                continue
            matched_rows_seen += 1
            if matched_rows_seen <= requested_offset:
                continue
            if len(selected_rows) < requested_limit:
                selected_rows.append(candidate_row)
                selected_prices_by_set[price_set_id] = prices
            if len(selected_rows) >= requested_limit:
                return _VersionThreeFilteredReverseSelection(
                    rows=tuple(selected_rows),
                    prices_by_price_set=selected_prices_by_set,
                    exhausted=False,
                    matched_rows_seen=matched_rows_seen,
                )
        if metadata_batch_size is None or code_metadata_count < metadata_batch_size:
            return _VersionThreeFilteredReverseSelection(
                rows=tuple(selected_rows),
                prices_by_price_set=selected_prices_by_set,
                exhausted=True,
                matched_rows_seen=matched_rows_seen,
                total_row_count=matched_rows_seen,
            )
        metadata_offset += code_metadata_count


async def _shared_graph_members_by_id(
    session,
    serving_tables: PTG2ServingTables,
    name: str,
    owner_ids: list[str] | tuple[str, ...],
    *,
    max_members: int | None = None,
) -> dict[str, tuple[str, ...]]:
    """Resolve dense shared-graph members for normalized 128-bit owner IDs."""

    _require_strict_shared_v3(serving_tables)
    owner_id_list = list(_ptg2_manifest_ids(tuple(owner_ids)))
    if not owner_id_list:
        return {}
    return await _shared_graph_members_many(
        session,
        serving_tables,
        name,
        owner_id_list,
        max_members=max_members,
    )

async def _shared_provider_group_ids_for_keys(
    session,
    serving_tables: PTG2ServingTables,
    provider_group_keys: Iterable[int],
) -> dict[int, str]:
    keys = tuple(sorted({int(provider_group_key) for provider_group_key in provider_group_keys}))
    if not keys:
        return {}
    result = await session.execute(
        text(
            f"""
            SELECT provider_group_key, provider_group_global_id_128
              FROM {_shared_v3_provider_group_table()}
             WHERE snapshot_key = :shared_snapshot_key
               AND provider_group_key = ANY(CAST(:provider_group_keys AS integer[]))
            """
        ),
        {
            "shared_snapshot_key": _required_shared_snapshot_key(serving_tables),
            "provider_group_keys": keys,
        },
    )
    return {
        int(row.get("provider_group_key")): _ptg2_manifest_id(row.get("provider_group_global_id_128"))
        for row in (_row_mapping(raw_row) for raw_row in result)
        if row.get("provider_group_key") is not None
        and _ptg2_manifest_id(row.get("provider_group_global_id_128"))
    }


async def _shared_provider_group_keys_for_ids(
    session,
    serving_tables: PTG2ServingTables,
    provider_group_ids: Iterable[str],
) -> dict[str, int]:
    ids = tuple(_ptg2_manifest_ids(tuple(provider_group_ids)))
    if not ids:
        return {}
    result = await session.execute(
        text(
            f"""
            SELECT provider_group_key, provider_group_global_id_128
              FROM {_shared_v3_provider_group_table()}
             WHERE snapshot_key = :shared_snapshot_key
               AND provider_group_global_id_128 = ANY(CAST(:provider_group_ids AS bytea[]))
            """
        ),
        {
            "shared_snapshot_key": _required_shared_snapshot_key(serving_tables),
            "provider_group_ids": [bytes.fromhex(provider_group_id) for provider_group_id in ids],
        },
    )
    return {
        _ptg2_manifest_id(row.get("provider_group_global_id_128")): int(row.get("provider_group_key"))
        for row in (_row_mapping(raw_row) for raw_row in result)
        if row.get("provider_group_key") is not None
        and _ptg2_manifest_id(row.get("provider_group_global_id_128"))
    }


async def _shared_graph_members_many(
    session,
    serving_tables: PTG2ServingTables,
    name: str,
    owner_ids: list[str],
    *,
    max_members: int | None,
) -> dict[str, tuple[str, ...]]:
    """Resolve shared-graph member IDs for each requested owner ID."""
    direction_by_name = {
        "provider_npi_group": PTG2_V3_GRAPH_NPI_TO_GROUP,
        "provider_group_npi": PTG2_V3_GRAPH_GROUP_TO_NPI,
        "provider_inverted": PTG2_V3_GRAPH_GROUP_TO_PROVIDER_SET,
        "provider_forward": PTG2_V3_GRAPH_PROVIDER_SET_TO_GROUP,
    }
    direction = direction_by_name.get(name)
    if direction is None:
        raise PTG2ManifestArtifactError(f"unsupported PTG2 shared graph artifact: {name}")
    if direction == PTG2_V3_GRAPH_NPI_TO_GROUP:
        owner_key_by_id = {
            owner_id: npi
            for owner_id in owner_ids
            if (npi := _ptg2_npi_from_member_id(owner_id)) is not None
        }
        if len(owner_key_by_id) != len(owner_ids):
            raise PTG2ManifestArtifactError("PTG2 shared NPI graph owner is malformed")
    elif direction == PTG2_V3_GRAPH_PROVIDER_SET_TO_GROUP:
        owner_key_by_id = await _provider_set_keys_for_ids(
            session,
            serving_tables,
            owner_ids,
        )
    else:
        owner_key_by_id = await _shared_provider_group_keys_for_ids(
            session,
            serving_tables,
            owner_ids,
        )
    members_by_owner_key = await lookup_shared_graph_members_from_db(
        session,
        _required_shared_snapshot_key(serving_tables),
        direction,
        owner_key_by_id.values(),
        schema_name=PTG2_SCHEMA,
        max_members=max_members,
    )
    member_keys = {
        member_key
        for members in members_by_owner_key.values()
        for member_key in members
    }
    if direction in {PTG2_V3_GRAPH_NPI_TO_GROUP, PTG2_V3_GRAPH_PROVIDER_SET_TO_GROUP}:
        member_id_by_key = await _shared_provider_group_ids_for_keys(
            session,
            serving_tables,
            member_keys,
        )
    elif direction == PTG2_V3_GRAPH_GROUP_TO_PROVIDER_SET:
        member_id_by_key = await _provider_set_ids_for_keys(
            session,
            serving_tables,
            member_keys,
        )
    else:
        member_id_by_key = {
            npi: _ptg2_npi_member_id(npi)
            for npi in member_keys
        }
    if set(member_id_by_key) != member_keys:
        raise PTG2ManifestArtifactError("PTG2 shared graph references a missing support dictionary key")
    return {
        owner_id: tuple(
            member_id_by_key[member_key]
            for member_key in members_by_owner_key.get(owner_key_by_id.get(owner_id, -1), ())
        )
        for owner_id in owner_ids
    }


async def _shared_graph_members_for_id(
    session,
    serving_tables: PTG2ServingTables,
    name: str,
    owner_id: str,
    *,
    max_members: int | None = None,
) -> tuple[str, ...]:
    owner_id = _ptg2_manifest_id(owner_id)
    if not owner_id:
        return ()
    members_by_owner = await _shared_graph_members_by_id(
        session,
        serving_tables,
        name,
        (owner_id,),
        max_members=max_members,
    )
    return members_by_owner.get(owner_id, ())


async def _manifest_sets_by_group(
    session,
    serving_tables: PTG2ServingTables,
    group_ids: list[str] | tuple[str, ...],
) -> dict[str, tuple[str, ...]]:
    normalized_group_ids = _ptg2_manifest_ids(tuple(group_ids))
    if not normalized_group_ids:
        return {}
    return await _shared_graph_members_by_id(
        session,
        serving_tables,
        "provider_inverted",
        normalized_group_ids,
    )

def _ptg2_build_rate_scope(group_ids: tuple[str, ...]) -> _ManifestRateScope:
    normalized_group_ids = tuple(sorted(set(_ptg2_manifest_ids(group_ids))))
    sql_group_ids = normalized_group_ids if len(normalized_group_ids) <= _ptg2_sql_scope_limit() else ()
    binary_group_ids = [_ptg2_manifest_id_bytes(group_id) for group_id in normalized_group_ids]
    if any(not group_id for group_id in binary_group_ids):
        raise PTG2ManifestArtifactError("PTG2 shared graph returned a malformed provider-group id")
    return _ManifestRateScope(
        group_ids=sql_group_ids,
        group_id_bytes=frozenset(binary_group_ids),
        id_count=len(normalized_group_ids),
    )


async def _shared_rate_scope(
    session,
    serving_tables: PTG2ServingTables,
    *,
    plan_id: str,
    plan_market_type: str = "",
    reported_code: str,
    code_system: str | None,
    provider_set_keys: Iterable[int] | None = None,
) -> _ManifestRateScope:
    group_ids = await _shared_rate_provider_groups(
        session,
        serving_tables,
        plan_id=plan_id,
        plan_market_type=plan_market_type,
        reported_code=reported_code,
        code_system=code_system,
        provider_set_keys=provider_set_keys,
    )
    return _ptg2_build_rate_scope(group_ids)

async def _shared_forward_entries_for_code_rows(
    session,
    serving_tables: PTG2ServingTables,
    code_rows: Iterable[Mapping[str, Any]],
    *,
    provider_set_keys: Iterable[int] | None = None,
) -> list[Any]:
    """Load complete forward entries for each compatible code dictionary row."""

    normalized_provider_set_keys = (
        tuple(sorted({int(provider_set_key) for provider_set_key in provider_set_keys}))
        if provider_set_keys is not None
        else None
    )
    provider_counts_by_key = await _version_three_provider_counts_for_keys(
        session,
        serving_tables,
        normalized_provider_set_keys,
    )
    forward_entries: list[Any] = []
    for code_row in code_rows:
        code_key = code_row.get("code_key")
        if code_key is None:
            continue
        forward_entries.extend(
            await _lookup_shared_forward_rows(
                session,
                serving_tables,
                int(code_key),
                provider_set_keys=normalized_provider_set_keys,
                provider_counts_by_key=provider_counts_by_key,
            )
        )
    return forward_entries


async def _shared_group_ids_for_set_keys(
    session,
    serving_tables: PTG2ServingTables,
    provider_set_keys: Iterable[int],
) -> tuple[str, ...]:
    """Resolve dense provider-set keys to stable provider-group IDs."""

    normalized_provider_set_keys = tuple(
        sorted({int(provider_set_key) for provider_set_key in provider_set_keys})
    )
    if not normalized_provider_set_keys:
        return ()
    groups_by_provider_set = await lookup_shared_graph_members_from_db(
        session,
        _required_shared_snapshot_key(serving_tables),
        PTG2_V3_GRAPH_PROVIDER_SET_TO_GROUP,
        normalized_provider_set_keys,
        schema_name=PTG2_SCHEMA,
    )
    group_keys = tuple(
        sorted(
            {
                int(group_key)
                for provider_set_group_keys in groups_by_provider_set.values()
                for group_key in provider_set_group_keys
            }
        )
    )
    group_id_by_key = await _shared_provider_group_ids_for_keys(
        session,
        serving_tables,
        group_keys,
    )
    if set(group_id_by_key) != set(group_keys):
        raise PTG2ManifestArtifactError(
            "PTG2 shared graph references a missing provider-group dictionary key"
        )
    return tuple(group_id_by_key[group_key] for group_key in group_keys)


async def _shared_rate_provider_groups(
    session,
    serving_tables: PTG2ServingTables,
    *,
    plan_id: str,
    plan_market_type: str = "",
    reported_code: str,
    code_system: str | None,
    provider_set_keys: Iterable[int] | None = None,
) -> tuple[str, ...]:
    """Resolve the shared-graph groups that carry one plan/code rate."""

    _require_strict_shared_v3(serving_tables)
    if not plan_id or not reported_code:
        return ()
    scope_join_sql, filters, params, plan_order = _shared_v3_code_scope_sql(
        serving_tables,
        requested_plan=plan_id,
        plan_market_type=plan_market_type,
    )
    filters.append("code_metadata.snapshot_key = :shared_snapshot_key")
    params["shared_snapshot_key"] = _required_shared_snapshot_key(serving_tables)
    _append_reported_code_value_filter(
        filters,
        params,
        column="code_metadata.reported_code",
        param_name="reported_code",
        values=_ptg2_reported_code_lookup_values(code_system, reported_code),
    )
    _append_reported_code_system_filter(
        filters,
        params,
        column="code_metadata.reported_code_system",
        code_system=code_system,
    )
    code_query_result = await session.execute(
        text(
            f"""
            SELECT code_metadata.code_key,
                   logical_scope.plan_id,
                   logical_scope.plan_market_type,
                   code_metadata.reported_code_system,
                   code_metadata.reported_code
              FROM {_shared_v3_code_table()} code_metadata
              {scope_join_sql}
             WHERE {" AND ".join(filters)}
             ORDER BY {plan_order}, code_metadata.reported_code, code_metadata.code_key
            """
        ),
        params,
    )
    code_rows = [
        _canonical_code_metadata_row(code_record)
        for code_record in code_query_result
    ]
    if not code_rows:
        return ()
    forward_rows = await _shared_forward_entries_for_code_rows(
        session,
        serving_tables,
        code_rows,
        provider_set_keys=provider_set_keys,
    )
    return await _shared_group_ids_for_set_keys(
        session,
        serving_tables,
        [forward_entry.provider_set_key for forward_entry in forward_rows],
    )


def _ptg2_npi_from_member_id(member_id: str) -> int | None:
    try:
        raw = bytes.fromhex(member_id)
    except ValueError:
        return None
    if len(raw) != 16:
        return None
    npi = int.from_bytes(raw[8:16], "big", signed=False)
    return npi if npi > 0 else None


def _ptg2_npi_member_id(npi: int) -> str:
    return (b"\x00" * 8 + int(npi).to_bytes(8, "big", signed=False)).hex()


def _cached_provider_npi_prefixes(
    serving_tables: PTG2ServingTables,
    provider_set_ids: tuple[str, ...],
    requested_limit: int,
) -> tuple[dict[str, tuple[int, ...]], tuple[str, ...]]:
    """Return reusable prefixes and provider sets that still need loading."""

    shared_snapshot_key = _required_shared_snapshot_key(serving_tables)
    npis_by_set: dict[str, tuple[int, ...]] = {}
    missing_provider_set_ids: list[str] = []
    for provider_set_id in provider_set_ids:
        cache_key = (shared_snapshot_key, provider_set_id)
        cache_entry = _PTG2_PROVIDER_NPI_PREFIX_CACHE.get(cache_key)
        if cache_entry is None:
            missing_provider_set_ids.append(provider_set_id)
            continue
        cached_limit, cached_npis, is_complete = cache_entry
        if not is_complete and cached_limit < requested_limit:
            missing_provider_set_ids.append(provider_set_id)
            continue
        _PTG2_PROVIDER_NPI_PREFIX_CACHE.move_to_end(cache_key)
        npis_by_set[provider_set_id] = cached_npis[:requested_limit]
    return npis_by_set, tuple(missing_provider_set_ids)


def _cache_provider_npi_prefix(
    serving_tables: PTG2ServingTables,
    provider_set_id: str,
    requested_limit: int,
    npis: tuple[int, ...],
    *,
    is_complete: bool,
) -> None:
    """Store one bounded provider-set prefix under its sealed snapshot key."""

    cache_key = (
        _required_shared_snapshot_key(serving_tables),
        provider_set_id,
    )
    _PTG2_PROVIDER_NPI_PREFIX_CACHE[cache_key] = (
        requested_limit,
        npis,
        is_complete,
    )
    _PTG2_PROVIDER_NPI_PREFIX_CACHE.move_to_end(cache_key)
    while (
        len(_PTG2_PROVIDER_NPI_PREFIX_CACHE)
        > _PTG2_PROVIDER_NPI_PREFIX_CACHE_MAX_ENTRIES
    ):
        _PTG2_PROVIDER_NPI_PREFIX_CACHE.popitem(last=False)


async def _provider_npis_for_sets(
    session,
    serving_tables: PTG2ServingTables,
    provider_set_global_ids: list[str] | tuple[str, ...],
    *,
    limit_per_set: int | None = None,
) -> dict[str, tuple[int, ...]]:
    """Load bounded NPI prefixes for sealed provider sets."""

    provider_set_ids = _ptg2_manifest_ids(tuple(provider_set_global_ids))
    if limit_per_set is None:
        uncached_provider_set_ids = provider_set_ids
        requested_limit = None
        npis_by_set: dict[str, tuple[int, ...]] = {}
    else:
        requested_limit = max(int(limit_per_set), 1)
        npis_by_set, uncached_provider_set_ids = _cached_provider_npi_prefixes(
            serving_tables,
            provider_set_ids,
            requested_limit,
        )
        if not uncached_provider_set_ids:
            return {
                provider_set_id: npis_by_set.get(provider_set_id, ())
                for provider_set_id in provider_set_ids
            }
    member_ids_by_set = await _provider_npi_member_ids_by_set(
        session,
        serving_tables,
        uncached_provider_set_ids,
        limit_per_set=requested_limit,
    )
    for provider_set_id in uncached_provider_set_ids:
        npis = tuple(
            dict.fromkeys(
                npi
                for member_id in member_ids_by_set.get(provider_set_id, ())
                if (npi := _ptg2_npi_from_member_id(member_id)) is not None
            )
        )
        npis_by_set[provider_set_id] = (
            npis[:requested_limit] if requested_limit is not None else npis
        )
        if requested_limit is not None:
            _cache_provider_npi_prefix(
                serving_tables,
                provider_set_id,
                requested_limit,
                npis_by_set[provider_set_id],
                is_complete=(
                    len(member_ids_by_set.get(provider_set_id, ()))
                    < requested_limit
                ),
            )
    return {
        provider_set_id: npis_by_set.get(provider_set_id, ())
        for provider_set_id in provider_set_ids
    }


async def _provider_npi_member_ids_by_set(
    session,
    serving_tables: PTG2ServingTables,
    provider_set_ids: tuple[str, ...],
    *,
    limit_per_set: int | None,
) -> dict[str, tuple[str, ...]]:
    """Resolve provider-set NPI membership through the dense shared graph."""

    _require_strict_shared_v3(serving_tables)
    groups_by_set = await _shared_graph_members_by_id(
        session,
        serving_tables,
        "provider_forward",
        provider_set_ids,
    )
    if limit_per_set is not None:
        return await _limited_graph_member_ids_by_set(
            session,
            serving_tables,
            groups_by_set,
            max(int(limit_per_set), 1),
        )
    group_ids = tuple(
        dict.fromkeys(
            group_id
            for provider_set_id in provider_set_ids
            for group_id in groups_by_set.get(provider_set_id, ())
        )
    )
    member_ids_by_group = await _shared_graph_members_by_id(
        session,
        serving_tables,
        "provider_group_npi",
        group_ids,
    )
    return {
        provider_set_id: tuple(
            dict.fromkeys(
                member_id
                for group_id in groups_by_set.get(provider_set_id, ())
                for member_id in member_ids_by_group.get(group_id, ())
            )
        )
        for provider_set_id in provider_set_ids
    }


async def _limited_graph_member_ids_by_set(
    session,
    serving_tables: PTG2ServingTables,
    groups_by_set: dict[str, tuple[str, ...]],
    limit_per_set: int,
) -> dict[str, tuple[str, ...]]:
    """Read graph groups in bounded batches until every set has enough NPIs."""
    member_ids_by_set: dict[str, list[str]] = {provider_set_id: [] for provider_set_id in groups_by_set}
    seen_ids_by_set: dict[str, set[str]] = {provider_set_id: set() for provider_set_id in groups_by_set}
    provider_sets_by_group: dict[str, list[str]] = defaultdict(list)
    ordered_group_ids: list[str] = []
    for provider_set_id, group_ids in groups_by_set.items():
        for group_id in group_ids:
            if group_id not in provider_sets_by_group:
                ordered_group_ids.append(group_id)
            provider_sets_by_group[group_id].append(provider_set_id)
    for batch_start in range(0, len(ordered_group_ids), 256):
        group_batch_ids = tuple(ordered_group_ids[batch_start : batch_start + 256])
        member_ids_by_group = await _shared_graph_members_by_id(
            session,
            serving_tables,
            "provider_group_npi",
            group_batch_ids,
            max_members=limit_per_set,
        )
        _collect_limited_graph_batch(
            group_batch_ids,
            member_ids_by_group,
            provider_sets_by_group,
            member_ids_by_set,
            seen_ids_by_set,
            limit_per_set,
        )
        if all(len(member_ids) >= limit_per_set for member_ids in member_ids_by_set.values()):
            break
    return {provider_set_id: tuple(member_ids) for provider_set_id, member_ids in member_ids_by_set.items()}


def _collect_limited_graph_batch(
    group_batch_ids: tuple[str, ...],
    member_ids_by_group: dict[str, tuple[str, ...]],
    provider_sets_by_group: dict[str, list[str]],
    member_ids_by_set: dict[str, list[str]],
    seen_ids_by_set: dict[str, set[str]],
    limit_per_set: int,
) -> None:
    """Accumulate one graph-owner batch without exceeding per-set limits."""
    for group_id in group_batch_ids:
        for provider_set_id in provider_sets_by_group[group_id]:
            if len(member_ids_by_set[provider_set_id]) >= limit_per_set:
                continue
            for member_id in member_ids_by_group.get(group_id, ()):
                if member_id in seen_ids_by_set[provider_set_id]:
                    continue
                seen_ids_by_set[provider_set_id].add(member_id)
                member_ids_by_set[provider_set_id].append(member_id)
                if len(member_ids_by_set[provider_set_id]) >= limit_per_set:
                    break


def _is_price_filter_match(price: dict[str, Any], args: dict[str, Any]) -> bool:
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
    return [price for price in prices if _is_price_filter_match(price, args)]


def _ptg2_price_atom_attr_specs() -> tuple[tuple[str, str, str, str], ...]:
    """Describe lean price-atom dictionary attributes in response order."""
    return (
        ("negotiated_type", "negotiated_type_key", "text", "negotiated_type.text_value AS negotiated_type"),
        ("expiration_date", "expiration_date_key", "text", "expiration_date.text_value AS expiration_date"),
        ("service_code", "service_code_key", "array", "COALESCE(service_code.text_array, ARRAY[]::text[]) AS service_code"),
        ("billing_class", "billing_class_key", "text", "billing_class.text_value AS billing_class"),
        ("setting", "setting_key", "text", "setting.text_value AS setting"),
        (
            "billing_code_modifier",
            "billing_code_modifier_key",
            "array",
            "COALESCE(billing_code_modifier.text_array, ARRAY[]::text[]) AS billing_code_modifier",
        ),
        (
            "additional_information",
            "additional_information_key",
            "text",
            "additional_information.text_value AS additional_information",
        ),
    )


def _version_three_atom_key_bits(serving_tables: PTG2ServingTables) -> int:
    """Return the manifest-declared dense atom width for a v3 snapshot."""

    try:
        atom_key_bits = int(serving_tables.atom_key_bits)
    except (TypeError, ValueError) as exc:
        raise PTG2ManifestArtifactError("PTG2 postgres_binary_v3 snapshot is missing atom_key_bits") from exc
    if atom_key_bits not in {24, 32}:
        raise PTG2ManifestArtifactError("PTG2 postgres_binary_v3 atom_key_bits must be 24 or 32")
    return atom_key_bits


def _version_three_dictionary_entry(
    dictionary_record: Any,
) -> tuple[tuple[str, int], Any] | None:
    dictionary_entry = _row_mapping(dictionary_record)
    attr_kind = dictionary_entry.get("attribute_kind")
    attr_key = dictionary_entry.get("attribute_key")
    if attr_kind is None or attr_key is None:
        return None
    dictionary_value = dictionary_entry.get("value")
    if str(attr_kind) in {"service_code", "billing_code_modifier"}:
        try:
            dictionary_value = json.loads(str(dictionary_value or "[]"))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise PTG2ManifestArtifactError(
                "PTG2 v3 price atom array dictionary value is malformed"
            ) from exc
        if not isinstance(dictionary_value, list):
            raise PTG2ManifestArtifactError(
                "PTG2 v3 price atom array dictionary value is malformed"
            )
    return (str(attr_kind), int(attr_key)), dictionary_value


async def _version_three_dictionary_values(
    session,
    serving_tables: PTG2ServingTables,
    price_atoms_by_key: Mapping[int, Any],
) -> dict[tuple[str, int], Any]:
    """Read only dictionary values referenced by the requested dense atoms."""

    attribute_specs = _ptg2_price_atom_attr_specs()
    constant_values = (
        serving_tables.price_atom_constant_values
        if isinstance(serving_tables.price_atom_constant_values, dict)
        else {}
    )
    required_keys: set[tuple[str, int]] = set()
    for price_atom in price_atoms_by_key.values():
        if len(price_atom.attribute_keys) != len(attribute_specs):
            raise PTG2ManifestArtifactError("PTG2 v3 price atom has an invalid attribute-key count")
        for (attr_kind, _key_column, _value_kind, _select_sql), attr_key in zip(
            attribute_specs,
            price_atom.attribute_keys,
        ):
            if attr_kind not in constant_values and attr_key is not None:
                required_keys.add((attr_kind, int(attr_key)))
    if not required_keys:
        return {}
    _require_strict_shared_v3(serving_tables)
    dictionary_table = _shared_v3_price_attr_table()
    dictionary_result = await session.execute(
        text(
            f"""
            SELECT attribute_kind, attribute_key, value
            FROM {dictionary_table}
            WHERE snapshot_key = :shared_snapshot_key
              AND attribute_kind = ANY(CAST(:attr_kinds AS varchar[]))
              AND attribute_key = ANY(CAST(:attr_keys AS integer[]))
            """
        ),
        {
            "shared_snapshot_key": _required_shared_snapshot_key(serving_tables),
            "attr_kinds": sorted({attr_kind for attr_kind, _attr_key in required_keys}),
            "attr_keys": sorted({attr_key for _attr_kind, attr_key in required_keys}),
        },
    )
    values_by_key: dict[tuple[str, int], Any] = {}
    for dictionary_record in dictionary_result:
        dictionary_entry = _version_three_dictionary_entry(dictionary_record)
        if dictionary_entry is not None:
            dictionary_key, dictionary_value = dictionary_entry
            values_by_key[dictionary_key] = dictionary_value
    missing_keys = required_keys.difference(values_by_key)
    if missing_keys:
        raise PTG2ManifestArtifactError("PTG2 v3 price atom dictionary key is missing")
    return values_by_key


def _version_three_price_payload(
    price_atom: Any,
    dictionary_values: Mapping[tuple[str, int], Any],
    constant_values: Mapping[str, Any],
) -> dict[str, Any]:
    attribute_specs = _ptg2_price_atom_attr_specs()
    if len(price_atom.attribute_keys) != len(attribute_specs):
        raise PTG2ManifestArtifactError("PTG2 v3 price atom has an invalid attribute-key count")
    payload: dict[str, Any] = {"negotiated_rate": price_atom.negotiated_rate}
    for (attr_kind, _key_column, value_kind, _select_sql), attr_key in zip(
        attribute_specs,
        price_atom.attribute_keys,
    ):
        if attr_kind in constant_values:
            value = constant_values[attr_kind]
        elif attr_key is None:
            value = [] if value_kind == "array" else None
        else:
            value = dictionary_values[(attr_kind, int(attr_key))]
        payload[attr_kind] = value or [] if value_kind == "array" else value
    return payload


async def _version_three_prices_by_key(
    session,
    serving_tables: PTG2ServingTables,
    price_keys: Iterable[int],
) -> dict[int, list[dict[str, Any]]]:
    """Hydrate v3 price keys from compact memberships and dense atoms only."""

    _require_strict_shared_v3(serving_tables)
    normalized_price_keys = tuple(sorted({int(price_key) for price_key in price_keys}))
    if not normalized_price_keys:
        return {}
    atom_key_bits = _version_three_atom_key_bits(serving_tables)
    atom_keys_by_price_key = await lookup_shared_price_atom_memberships_from_db(
        session,
        _required_shared_snapshot_key(serving_tables),
        normalized_price_keys,
        atom_key_bits=atom_key_bits,
        block_span=serving_tables.price_key_block_span,
        schema_name=PTG2_SCHEMA,
    )
    _validate_version_three_price_memberships(normalized_price_keys, atom_keys_by_price_key)
    requested_atom_keys = tuple(
        dict.fromkeys(
            atom_key
            for price_key in normalized_price_keys
            for atom_key in atom_keys_by_price_key.get(price_key, ())
        )
    )
    price_atoms_by_key = await lookup_shared_price_atoms_from_db(
        session,
        _required_shared_snapshot_key(serving_tables),
        requested_atom_keys,
        atom_key_bits=atom_key_bits,
        block_span=serving_tables.atom_key_block_span,
        schema_name=PTG2_SCHEMA,
    )
    missing_atom_keys = set(requested_atom_keys).difference(price_atoms_by_key)
    if missing_atom_keys:
        raise PTG2ManifestArtifactError(
            "PTG2 v3 price-atom artifact is missing a referenced atom key"
        )
    dictionary_values = await _version_three_dictionary_values(
        session,
        serving_tables,
        price_atoms_by_key,
    )
    constant_values = (
        serving_tables.price_atom_constant_values
        if isinstance(serving_tables.price_atom_constant_values, dict)
        else {}
    )
    return _version_three_price_rows(
        normalized_price_keys,
        atom_keys_by_price_key,
        price_atoms_by_key,
        dictionary_values,
        constant_values,
    )


def _validate_version_three_price_memberships(
    price_keys: tuple[int, ...],
    atom_keys_by_price_key: Mapping[int, tuple[int, ...]],
) -> None:
    """Reject missing or empty v3 price memberships."""

    missing_price_keys = set(price_keys).difference(atom_keys_by_price_key)
    empty_price_keys = {
        price_key for price_key, atom_keys in atom_keys_by_price_key.items() if not atom_keys
    }
    if missing_price_keys or empty_price_keys:
        raise PTG2ManifestArtifactError(
            "PTG2 v3 price-membership artifact is missing a referenced price key"
        )


def _version_three_price_rows(
    price_keys: tuple[int, ...],
    atom_keys_by_price_key: Mapping[int, tuple[int, ...]],
    price_atoms_by_key: Mapping[int, Any],
    dictionary_values: Mapping[tuple[str, int], str],
    constant_values: Mapping[str, Any],
) -> dict[int, list[dict[str, Any]]]:
    """Build response price payloads in requested dense-key order."""

    return {
        price_key: [
            _version_three_price_payload(price_atoms_by_key[atom_key], dictionary_values, constant_values)
            for atom_key in atom_keys_by_price_key.get(price_key, ())
            if atom_key in price_atoms_by_key
        ]
        for price_key in price_keys
    }


async def _prices_for_price_sets(
    session,
    serving_tables: PTG2ServingTables,
    price_set_global_ids: list[str] | tuple[str, ...],
    *,
    price_key_by_set_id: Mapping[str, int] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Return hydrated prices for each requested price set, preserving atom order."""

    price_set_ids = _ptg2_manifest_ids(tuple(price_set_global_ids))
    if not price_set_ids:
        return {}
    _require_strict_shared_v3(serving_tables)
    price_key_by_set_id = price_key_by_set_id or {}
    missing_price_key_ids = set(price_set_ids).difference(price_key_by_set_id)
    if missing_price_key_ids:
        raise PTG2ManifestArtifactError(
            "PTG2 v3 forward row is missing a referenced price key"
        )
    prices_by_price_key = await _version_three_prices_by_key(
        session,
        serving_tables,
        [price_key_by_set_id[price_set_id] for price_set_id in price_set_ids],
    )
    return {
        price_set_id: prices_by_price_key.get(price_key_by_set_id[price_set_id], [])
        for price_set_id in price_set_ids
    }


async def _taxonomy_rows_for_npis(
    session,
    npis: list[int] | tuple[int, ...],
) -> dict[int, dict[str, Any]]:
    npis = tuple(sorted({int(npi) for npi in npis if int(npi) > 0}))
    if not npis:
        return {}
    taxonomy_table = f"{PTG2_SCHEMA}.npi_taxonomy"
    vocabulary_table = f"{PTG2_SCHEMA}.nucc_taxonomy"
    if not await _is_relation_available(
        session,
        taxonomy_table,
    ) or not await _is_relation_available(session, vocabulary_table):
        return {}
    taxonomy_query = await session.execute(
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
    for taxonomy_record in taxonomy_query:
        taxonomy_by_field = _row_mapping(taxonomy_record)
        npi = taxonomy_by_field.get("npi")
        if npi is None:
            continue
        taxonomy_by_npi[int(npi)] = {
            "taxonomy_codes": taxonomy_by_field.get("taxonomy_codes") or [],
            "specialties": taxonomy_by_field.get("specialties") or [],
            "classifications": taxonomy_by_field.get("classifications") or [],
            "specializations": taxonomy_by_field.get("specializations") or [],
            "primary_specialty": taxonomy_by_field.get("primary_specialty"),
            "primary_specialization": taxonomy_by_field.get(
                "primary_specialization"
            ),
        }
    return taxonomy_by_npi


async def _enriched_provider_rows_for_npis(
    session,
    *,
    npis: list[int] | tuple[int, ...],
    limit: int,
    plan_id: str | None = None,
    snapshot_id: str | None = None,
    source_key: str | None = None,
) -> list[dict[str, Any]] | None:
    """Enrich NPIs with provider, taxonomy, and address serving data."""

    npis = tuple(sorted({int(npi) for npi in npis if int(npi) > 0}))[:limit]
    if not npis:
        return []
    npi_data_table = f"{PTG2_SCHEMA}.npi"
    npi_address_table = await _ptg2_address_serving_table(
        session,
        _PTG2_LEGACY_ADDRESS_COLUMNS,
        require_legacy_available=True,
    )
    if not await _is_relation_available(session, npi_data_table) or not npi_address_table:
        taxonomy_by_npi = await _taxonomy_rows_for_npis(session, npis)
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
    is_using_unified_enrichment = _is_unified_address_table(npi_address_table)
    # Same street-fill as the location search: when the unified row has no
    # first_line (or no row at all), fall back to the NPPES npi_address row.
    if is_using_unified_enrichment:
        fallback_columns = set(
            await _ptg2_table_columns(session, f"{PTG2_SCHEMA}.npi_address")
        )

        def _fallback_column(column: str, sql_type: str = "varchar") -> str:
            return f"na.{column}" if column in fallback_columns else f"NULL::{sql_type} AS {column}"

        enrich_address_fallback_cte = f"""
            , fallback_addresses AS MATERIALIZED (
                SELECT DISTINCT ON (na.npi)
                       na.npi, na.first_line, na.second_line, na.city_name, na.state_name,
                       na.postal_code, na.country_code,
                       {_fallback_column('telephone_number')},
                       {_fallback_column('fax_number')},
                       {_fallback_column('phone_number')},
                       {_fallback_column('phone_extension')},
                       {_fallback_column('fax_number_digits')},
                       {_fallback_column('fax_extension')},
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
                jsonb_build_object(
                    'npi', source_npis.npi,
                    'type', addr.type,
                    'checksum', addr.checksum,
                    'first_line', {_eff_enrich('first_line')},
                    'second_line', {_eff_enrich('second_line')},
                    'city_name', {_eff_enrich('city_name')},
                    'state_name', {_eff_enrich('state_name')},
                    'city', {_eff_enrich('city_name')},
                    'state', {_eff_enrich('state_name')},
                    'postal_code', {_eff_enrich('postal_code')},
                    'country_code', {_eff_enrich('country_code')},
                    'telephone_number', {_eff_enrich('telephone_number')},
                    'fax_number', {_eff_enrich('fax_number')},
                    'phone_number', {_eff_enrich('phone_number')},
                    'phone_extension', {_eff_enrich('phone_extension')},
                    'fax_number_digits', {_eff_enrich('fax_number_digits')},
                    'fax_extension', {_eff_enrich('fax_extension')},
                    'address_key', addr.address_key::text,
                    'address_site_key', addr.premise_key::text,
                    'lat', {_eff_enrich('lat')},
                    'long', {_eff_enrich('long')}
                )::text AS address_payload,
                {_eff_enrich('telephone_number')} AS telephone_number,
                {_eff_enrich('fax_number')} AS fax_number,
                {_eff_enrich('phone_number')} AS phone_number,
                {_eff_enrich('phone_extension')} AS phone_extension,
                {_eff_enrich('fax_number_digits')} AS fax_number_digits,
                {_eff_enrich('fax_extension')} AS fax_extension,
                COALESCE(tax.taxonomy_codes, ARRAY[]::varchar[]) AS taxonomy_codes,
                COALESCE(tax.specialties, ARRAY[]::varchar[]) AS specialties,
                COALESCE(tax.classifications, ARRAY[]::varchar[]) AS classifications,
                COALESCE(tax.specializations, ARRAY[]::varchar[]) AS specializations,
                tax.primary_specialty,
                tax.primary_specialization,
                n.provider_sex_code,
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
    enrichment_query = await session.execute(enrich_stmt, {"npis": npis})
    enriched_provider_rows = [
        _row_mapping(provider_record)
        for provider_record in enrichment_query
    ]
    return await _overlay_provider_directory_corroboration(
        session,
        enriched_provider_rows,
        plan_id=plan_id,
        snapshot_id=snapshot_id,
        source_key=source_key,
    )


def _is_ptg2_provider_filter_requested(args: dict[str, Any]) -> bool:
    if args.get("provider_sex_code") not in (None, "", "null"):
        return True
    if resolve_provider_specialty_filter(args).active:
        return True
    return _inferred_provider_taxonomy_rule(args) is not None


_PTG2_COST_ORDER_FIELDS = frozenset(
    {
        "total_allowed_amount",
        "total_drug_cost",
        "cost",
        "price",
        "rate",
        "negotiated_rate",
        "amount",
    }
)


def _is_cost_order_descending(args: Mapping[str, Any]) -> bool:
    order_by = str(args.get("order_by") or "total_allowed_amount").strip().lower()
    return order_by in _PTG2_COST_ORDER_FIELDS and str(
        args.get("order") or "asc"
    ).strip().lower() == "desc"


def _ptg2_manifest_rate_candidate_limit(
    args: dict[str, Any],
    pagination,
    *,
    expand_providers: bool,
    location_filter_requested: bool,
) -> int:
    requested_limit = max(int(pagination.limit), 1)
    requested_offset = max(int(getattr(pagination, "offset", 0) or 0), 0)
    if location_filter_requested and not expand_providers:
        return requested_offset + requested_limit + 1
    if expand_providers and location_filter_requested:
        # Bound the nearby-candidate pool the location expansion materializes.
        # The downstream provider_group_member fan-out + per-row enrichment cost
        # scales with this pool. Keep the default close to the requested page,
        # and make the overfetch/floor tunable for dense metros without a code
        # deploy.
        candidate_multiplier = _ptg2_manifest_location_candidate_multiplier()
        candidate_window = requested_limit * candidate_multiplier
        if os.getenv("HLTHPRT_PTG2_MANIFEST_LOCATION_CANDIDATE_MULTIPLIER") is None:
            candidate_window = requested_limit + min(
                max(candidate_window - requested_limit, 0),
                _location_candidate_overfetch_cap(),
            )
        candidate_floor = _ptg2_manifest_location_candidate_floor()
        return max(
            candidate_window,
            requested_offset + requested_limit + 1,
            candidate_floor,
        )
    if expand_providers and not location_filter_requested and _is_ptg2_provider_filter_requested(args):
        return min(
            _PTG2_MANIFEST_TAXONOMY_RATE_CANDIDATE_LIMIT,
            max(requested_limit, requested_offset + requested_limit, requested_limit * 5, 5),
        )
    if expand_providers:
        return requested_offset + requested_limit
    return requested_limit


def _ptg2_manifest_serving_row_limit(
    args: Mapping[str, Any],
    rate_candidate_limit: int,
    *,
    expand_providers: bool,
) -> int | None:
    """Return the safe pre-merge serving-row limit for a provider response."""

    if expand_providers:
        return None
    return int(rate_candidate_limit)


def _ptg2_decimal_rate_sort_value(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        decimal_rate = value
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            decimal_rate = Decimal(text)
        except (InvalidOperation, ValueError):
            return None
    return decimal_rate if decimal_rate.is_finite() else None


def _ptg2_provider_price_sort_value(item: dict[str, Any]) -> Decimal:
    rates: list[Decimal] = []
    for price in _coerce_json_payload(item.get("prices"), []):
        if not isinstance(price, dict):
            continue
        rate = _ptg2_decimal_rate_sort_value(price.get("negotiated_rate"))
        if rate is not None:
            rates.append(rate)
    for summary in _coerce_json_payload(item.get("price_summary"), []):
        if not isinstance(summary, dict):
            continue
        rate = _ptg2_decimal_rate_sort_value(summary.get("rate"))
        if rate is not None:
            rates.append(rate)
    return min(rates) if rates else Decimal("Infinity")


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
    provider_items: list[dict[str, Any]],
    args: dict[str, Any],
    *,
    location_filter_requested: bool,
) -> list[dict[str, Any]]:
    """Sort provider items by rank or the query's deterministic order."""
    if provider_items and all(
        provider_item.get("_ptg_provider_rank") is not None
        for provider_item in provider_items
    ):
        return sorted(
            provider_items,
            key=lambda provider_item: int(provider_item["_ptg_provider_rank"]),
        )
    requested_order = str(args.get("order_by") or "").strip().lower()
    order_by = requested_order or ("distance" if location_filter_requested else "")
    order = str(args.get("order") or "").strip().lower()
    is_descending = order == "desc"
    distance_order_fields = {"distance", "distance_miles"}
    if order_by in _PTG2_COST_ORDER_FIELDS:
        return sorted(
            provider_items,
            key=lambda provider_item: (
                (
                    -_ptg2_provider_price_sort_value(provider_item)
                    if is_descending
                    and _ptg2_provider_price_sort_value(
                        provider_item
                    ).is_finite()
                    else _ptg2_provider_price_sort_value(provider_item)
                ),
                int(
                    provider_item["_ptg_price_key"]
                    if provider_item.get("_ptg_price_key") is not None
                    else 2**32
                ),
                _ptg2_provider_distance_sort_value(provider_item),
                str(provider_item.get("provider_name") or ""),
                int(provider_item.get("npi") or 2**63 - 1),
            ),
        )
    if order_by in distance_order_fields:
        return sorted(
            provider_items,
            key=lambda provider_item: (
                (
                    -_ptg2_provider_distance_sort_value(provider_item)
                    if is_descending
                    and math.isfinite(
                        _ptg2_provider_distance_sort_value(provider_item)
                    )
                    else _ptg2_provider_distance_sort_value(provider_item)
                ),
                int(provider_item.get("npi") or 2**63 - 1),
                _ptg2_provider_price_sort_value(provider_item),
                str(provider_item.get("provider_name") or ""),
                int(
                    provider_item["_ptg_price_key"]
                    if provider_item.get("_ptg_price_key") is not None
                    else 2**32
                ),
            ),
        )
    return sorted(
        provider_items,
        key=lambda provider_item: (
            _ptg2_address_verification_sort_value(provider_item),
            _ptg2_provider_price_sort_value(provider_item),
            _ptg2_provider_distance_sort_value(provider_item),
            str(provider_item.get("provider_name") or ""),
            str(provider_item.get("npi") or ""),
        ),
    )


def _ptg2_manifest_provider_procedure_item(
    *,
    npi: int,
    serving_data: dict[str, Any],
    prices: list[dict[str, Any]],
    procedure_detail: dict[str, Any],
    provider_context: dict[str, Any] | None,
    args: dict[str, Any],
) -> dict[str, Any]:
    """Shape one provider and negotiated-price match into an API result item."""
    reported_code = serving_data.get("reported_code")
    reported_system = serving_data.get("reported_code_system")
    provider_set_hash = _ptg2_manifest_id(
        serving_data.get("provider_set_global_id_128")
    )
    price_set_hash = _ptg2_manifest_id(
        serving_data.get("price_set_global_id_128")
    )
    rate_pack_hash = _ptg2_manifest_id(
        serving_data.get("serving_content_hash_128")
    )
    source_artifact_key = serving_data.get("source_artifact_key")
    if source_artifact_key is None:
        source_artifact_key = serving_data.get("source_key")
    is_exact_source_mode = (
        normalize_ptg2_mode(args.get("mode")) == PTG2_MODE_EXACT_SOURCE
    )
    source_procedure_name = serving_data.get("source_procedure_name")
    source_procedure_description = serving_data.get(
        "source_procedure_description"
    )
    provider_item_by_field = dict(provider_context or {})
    provider_item_by_field.update(
        {
            "npi": npi,
            "plan_id": serving_data.get("plan_id"),
            "plan_market_type": serving_data.get("plan_market_type"),
            "provider_set_hash": provider_set_hash,
            "provider_count": serving_data.get("provider_count") or 0,
            "provider_set_count": 1 if provider_set_hash else 0,
            "network_names": _coerce_str_list_payload(
                serving_data.get("network_names")
            ),
            "procedure_code": reported_code,
            "billing_code_type_version": serving_data.get(
                "billing_code_type_version"
            ),
            "procedure_name": (
                source_procedure_name
                if is_exact_source_mode
                else source_procedure_name or procedure_detail.get("procedure_name")
            ),
            "procedure_description": (
                source_procedure_description
                if is_exact_source_mode
                else source_procedure_description
                or procedure_detail.get("procedure_description")
            ),
            "source_procedure_name": source_procedure_name,
            "source_procedure_description": source_procedure_description,
            "catalog_procedure_name": procedure_detail.get("procedure_name"),
            "catalog_procedure_description": procedure_detail.get("procedure_description"),
            "reported_code": reported_code,
            "reported_code_system": reported_system,
            "negotiation_arrangement": serving_data.get(
                "negotiation_arrangement"
            ),
            "billing_code": reported_code,
            "billing_code_type": reported_system,
            "prices": prices,
            "price_set_hash": price_set_hash,
            "rate_pack_hash": rate_pack_hash,
            "source_key": serving_data.get("logical_source_key")
            or args.get("source_key"),
            "source_artifact_key": source_artifact_key,
            "source_type": serving_data.get("source_type"),
            "identity_kind": serving_data.get("identity_kind"),
            "identity_sha256": serving_data.get("identity_sha256"),
            "raw_container_sha256": serving_data.get("raw_container_sha256"),
            "logical_json_sha256": serving_data.get("logical_json_sha256"),
            "logical_hash_deferred": serving_data.get("logical_hash_deferred"),
            "source_trace_set_hash": serving_data.get("source_trace_set_hash"),
            "source_trace": serving_data.get("source_trace"),
        }
    )
    return _compact_item_from_row(provider_item_by_field, args)


def _source_code_variant_group_key(
    provider_rate: Mapping[str, Any],
) -> tuple[Any, ...]:
    return (
        (
            provider_rate.get("billing_code_type_version") is not None,
            str(provider_rate.get("billing_code_type_version") or ""),
        ),
        (
            provider_rate.get("source_procedure_name") is not None,
            str(provider_rate.get("source_procedure_name") or ""),
        ),
        (
            provider_rate.get("source_procedure_description") is not None,
            str(provider_rate.get("source_procedure_description") or ""),
        ),
        tuple(
            sorted(_coerce_str_list_payload(provider_rate.get("network_names")))
        ),
    )


def _ptg2_provider_rate_group_key(
    provider_rate: dict[str, Any],
) -> tuple[Any, ...] | None:
    """Return the identity used to merge equivalent provider rate rows."""

    npi = provider_rate.get("npi")
    if npi in (None, ""):
        return None
    address_by_field = _coerce_json_payload(provider_rate.get("address"), {})
    if not isinstance(address_by_field, dict):
        address_by_field = {}
    location_key = (
        provider_rate.get("location_hash")
        or address_by_field.get("address_key")
        or address_by_field.get("address_site_key")
        or address_by_field.get("premise_key")
    )
    if not location_key:
        location_key = "|".join(
            str(
                address_by_field.get(key)
                or provider_rate.get(key)
                or ""
            ).strip().upper()
            for key in ("first_line", "second_line", "city", "state", "zip5")
        )
    reported_system = (
        provider_rate.get("reported_code_system")
        or provider_rate.get("service_code_system")
        or provider_rate.get("billing_code_type")
        or ""
    )
    reported_code = (
        provider_rate.get("reported_code")
        or provider_rate.get("service_code")
        or provider_rate.get("billing_code")
        or ""
    )
    negotiation_arrangement = provider_rate.get("negotiation_arrangement") or ""
    return (
        str(npi),
        str(location_key),
        str(reported_system),
        str(reported_code),
        str(negotiation_arrangement),
        *_source_code_variant_group_key(provider_rate),
        str(
            provider_rate.get("source_artifact_key")
            if provider_rate.get("source_artifact_key") is not None
            else ""
        ),
    )


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
    seen_payload_keys = {
        _price_row_key(item) if isinstance(item, dict) else str(item)
        for item in target_values
    }
    for item in payload:
        if item in (None, ""):
            continue
        key = _price_row_key(item) if isinstance(item, dict) else str(item)
        if key in seen_payload_keys:
            continue
        seen_payload_keys.add(key)
        target_values.append(item)


def _ensure_provider_rate_price_fields(item: dict[str, Any]) -> None:
    prices = item.get("prices")
    if isinstance(prices, list) and "price_summary" in item:
        item["tic_prices"] = list(item.get("tic_prices") or prices)
        return
    item.update(_price_response_fields(prices))


def _merge_ptg2_provider_rate_items(
    provider_rate_items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Collapse duplicate provider/location rows while preserving every rate option."""
    merged_provider_rates: list[dict[str, Any]] = []
    provider_rate_by_group_key: dict[tuple[Any, ...], dict[str, Any]] = {}
    price_dirty_group_keys: set[tuple[Any, ...]] = set()
    for provider_rate in provider_rate_items:
        group_key = _ptg2_provider_rate_group_key(provider_rate)
        if group_key is None:
            merged_provider_rates.append(provider_rate)
            continue
        merged_provider_rate_by_field = provider_rate_by_group_key.get(group_key)
        if merged_provider_rate_by_field is None:
            merged_provider_rate_by_field = dict(provider_rate)
            _ensure_provider_rate_price_fields(merged_provider_rate_by_field)
            owned_prices = list(merged_provider_rate_by_field.get("prices") or [])
            merged_provider_rate_by_field["prices"] = owned_prices
            merged_provider_rate_by_field["tic_prices"] = owned_prices
            for list_field in (
                "price_set_hashes",
                "rate_pack_hashes",
                "provider_set_hashes",
                "source_trace",
            ):
                merged_provider_rate_by_field[list_field] = list(
                    merged_provider_rate_by_field.get(list_field) or []
                )
            _append_unique_value(
                merged_provider_rate_by_field["price_set_hashes"],
                provider_rate.get("price_set_hash"),
            )
            _append_unique_value(
                merged_provider_rate_by_field["rate_pack_hashes"],
                provider_rate.get("rate_pack_hash"),
            )
            _append_unique_value(
                merged_provider_rate_by_field["provider_set_hashes"],
                provider_rate.get("provider_set_hash"),
            )
            _merge_unique_payload_list(
                merged_provider_rate_by_field,
                "price_set_hashes",
                provider_rate.get("price_set_hashes"),
            )
            _merge_unique_payload_list(
                merged_provider_rate_by_field,
                "rate_pack_hashes",
                provider_rate.get("rate_pack_hashes"),
            )
            _merge_unique_payload_list(
                merged_provider_rate_by_field,
                "provider_set_hashes",
                provider_rate.get("provider_set_hashes"),
            )
            provider_rate_by_group_key[group_key] = merged_provider_rate_by_field
            merged_provider_rates.append(merged_provider_rate_by_field)
            continue

        incoming_prices = provider_rate.get("prices")
        normalized_incoming_prices = (
            list(incoming_prices)
            if isinstance(incoming_prices, list) and "price_summary" in provider_rate
            else _normalize_price_payload(incoming_prices)
        )
        combined_prices = merged_provider_rate_by_field["prices"]
        combined_prices.extend(normalized_incoming_prices)
        merged_provider_rate_by_field["tic_prices"] = combined_prices
        price_dirty_group_keys.add(group_key)
        existing_price_key = merged_provider_rate_by_field.get("_ptg_price_key")
        item_price_key = provider_rate.get("_ptg_price_key")
        if item_price_key is not None and (
            existing_price_key is None or int(item_price_key) < int(existing_price_key)
        ):
            merged_provider_rate_by_field["_ptg_price_key"] = int(item_price_key)
        for hash_field in (
            "price_set_hash",
            "rate_pack_hash",
            "provider_set_hash",
        ):
            _append_unique_value(
                merged_provider_rate_by_field.setdefault(f"{hash_field}es", []),
                provider_rate.get(hash_field),
            )
        for hash_list_field in (
            "price_set_hashes",
            "rate_pack_hashes",
            "provider_set_hashes",
            "source_trace",
        ):
            _merge_unique_payload_list(
                merged_provider_rate_by_field,
                hash_list_field,
                provider_rate.get(hash_list_field),
            )
        merged_provider_rate_by_field["price_set_count"] = len(
            merged_provider_rate_by_field.get("price_set_hashes") or []
        )
        merged_provider_rate_by_field["rate_pack_count"] = len(
            merged_provider_rate_by_field.get("rate_pack_hashes") or []
        )
    for group_key in price_dirty_group_keys:
        grouped_provider_rate = provider_rate_by_group_key[group_key]
        grouped_provider_rate.update(
            _price_response_fields(grouped_provider_rate.get("prices"))
        )
    return merged_provider_rates


def _manifest_base_provider_predicates(
    args: Mapping[str, Any],
    query_parameters_by_name: dict[str, Any],
) -> list[str]:
    """Return strict-V3 non-taxonomy provider predicates."""

    provider_sex_predicate = provider_sex_exists_sql(
        "source_npis.npi",
        query_parameters_by_name,
        "manifest_provider_sex",
        args.get("provider_sex_code"),
        schema=PTG2_SCHEMA,
    )
    return [provider_sex_predicate] if provider_sex_predicate else []


async def _filter_npis_by_taxonomy(
    session,
    args: dict[str, Any],
    npis: list[int] | tuple[int, ...],
    *,
    limit: int,
) -> tuple[int, ...]:
    """Filter candidate NPIs by canonical demographic and taxonomy predicates."""
    candidate_npis = tuple(sorted({int(npi) for npi in npis if int(npi) > 0}))
    if not candidate_npis:
        return ()
    specialty_filter = resolve_provider_specialty_filter(args)
    query_parameters_by_name: dict[str, Any] = {
        "npis": list(candidate_npis), "limit": max(int(limit), 1)
    }
    predicates = _manifest_base_provider_predicates(args, query_parameters_by_name)
    if specialty_filter.active:
        predicates.append(
            provider_specialty_taxonomy_exists_sql(
                "source_npis.npi",
                query_parameters_by_name,
                "manifest_provider_specialty",
                specialty_filter,
                schema=PTG2_SCHEMA,
            )
        )
    inferred_sql = _inferred_provider_taxonomy_code_sql(
        args,
        nt_alias="nt",
        schema=PTG2_SCHEMA,
        params=query_parameters_by_name,
        param_prefix="manifest_provider_inferred_taxonomy",
    )
    if inferred_sql:
        predicates.append(
            f"EXISTS (SELECT 1 FROM {PTG2_SCHEMA}.npi_taxonomy nt WHERE nt.npi = source_npis.npi AND {inferred_sql})"
        )
        predicates.append(_ptg2_individual_npi_exists_sql("source_npis.npi"))
    if not predicates:
        return candidate_npis[: max(int(limit), 1)]
    filtered_npi_query = await session.execute(
        text(
            f"""
            SELECT source_npis.npi
            FROM (SELECT UNNEST(CAST(:npis AS bigint[])) AS npi) source_npis
            WHERE {" AND ".join(predicates)}
            ORDER BY source_npis.npi
            LIMIT :limit
            """
        ),
        query_parameters_by_name,
    )
    return tuple(
        int(npi_by_field["npi"])
        for npi_by_field in (
            _row_mapping(npi_record)
            for npi_record in filtered_npi_query
        )
        if npi_by_field.get("npi") is not None
    )


def _ptg2_manifest_location_match_limit() -> int:
    raw_value = os.getenv("HLTHPRT_PTG2_MANIFEST_LOCATION_MATCH_LIMIT", "5000")
    try:
        return max(int(raw_value), 1)
    except ValueError:
        return 5000


def _ptg2_manifest_location_candidate_multiplier() -> int:
    raw_value = os.getenv("HLTHPRT_PTG2_MANIFEST_LOCATION_CANDIDATE_MULTIPLIER", "2")
    try:
        return max(int(raw_value), 1)
    except ValueError:
        return 2


def _location_candidate_overfetch_cap() -> int:
    raw_value = os.getenv("HLTHPRT_PTG2_MANIFEST_LOCATION_CANDIDATE_OVERFETCH_CAP", "100")
    try:
        return max(int(raw_value), 0)
    except ValueError:
        return 100


def _ptg2_manifest_location_candidate_floor() -> int:
    raw_value = os.getenv("HLTHPRT_PTG2_MANIFEST_LOCATION_CANDIDATE_FLOOR", "100")
    try:
        return max(int(raw_value), 1)
    except ValueError:
        return 100


def _ptg2_sql_scope_limit() -> int:
    raw_value = os.getenv("HLTHPRT_PTG2_MANIFEST_SQL_RATE_SCOPE_MAX_IDS", "10000")
    try:
        return max(int(raw_value), 0)
    except ValueError:
        return 10000


def _has_rate_scope_group(rate_scope: _ManifestRateScope, value: Any) -> bool:
    group_id = _ptg2_manifest_id(value)
    if not group_id:
        return False
    binary_group_id = _ptg2_manifest_id_bytes(group_id)
    return bool(binary_group_id and binary_group_id in rate_scope.group_id_bytes)


@dataclass(frozen=True)
class _MembershipLocationQuery:
    address_table: str
    npi_scope_table: str
    filter_sql: str
    parameter_map: dict[str, Any]
    distance_sql: str
    knn_order_sql: str | None


_MEMBERSHIP_LOCATION_SQL = """
WITH matched AS MATERIALIZED (
    SELECT
        addr.npi,
        {location_hash_sql} AS location_hash,
        addr.state_name AS state,
        addr.city_name AS city,
        LEFT(COALESCE(addr.postal_code, ''), 5) AS zip5,
        {distance_sql} AS distance_miles,
        addr.type,
        addr.checksum,
        addr.telephone_number,
        addr.fax_number,
        addr.phone_number,
        addr.phone_extension,
        addr.fax_number_digits,
        addr.fax_extension,
        jsonb_build_object(
            'first_line', addr.first_line,
            'second_line', addr.second_line,
            'city', addr.city_name,
            'state', addr.state_name,
            'postal_code', addr.postal_code,
            'country_code', addr.country_code,
            'telephone_number', addr.telephone_number,
            'fax_number', addr.fax_number,
            'phone_number', addr.phone_number,
            'phone_extension', addr.phone_extension,
            'fax_number_digits', addr.fax_number_digits,
            'fax_extension', addr.fax_extension,
            'address_key', addr.address_key::text,
            'lat', addr.lat,
            'long', addr.long
        )::text AS address_payload,
        ROW_NUMBER() OVER (
            PARTITION BY addr.npi
            ORDER BY {distance_sql} ASC NULLS LAST,
                     CASE addr.type WHEN 'practice' THEN 0 WHEN 'primary' THEN 1 ELSE 2 END,
                     addr.checksum
        ) AS address_rank
    FROM {address_table} addr
    JOIN {npi_scope_table} npi_scope ON npi_scope.npi = addr.npi
    WHERE {filter_sql}
)
SELECT *
FROM matched
WHERE address_rank = 1
ORDER BY distance_miles ASC NULLS LAST, npi
LIMIT :limit OFFSET :offset
"""


_MEMBERSHIP_LOCATION_KNN_SQL = """
WITH nearest_addresses AS MATERIALIZED (
    SELECT
        addr.npi,
        addr.state_name,
        addr.city_name,
        addr.postal_code,
        addr.lat,
        addr.long,
        addr.type,
        addr.checksum,
        addr.telephone_number,
        addr.fax_number,
        addr.phone_number,
        addr.phone_extension,
        addr.fax_number_digits,
        addr.fax_extension,
        addr.first_line,
        addr.second_line,
        addr.country_code,
        addr.address_key,
        {distance_sql} AS candidate_distance_miles
    FROM {address_table} addr
    JOIN {npi_scope_table} npi_scope ON npi_scope.npi = addr.npi
    WHERE {filter_sql}
    ORDER BY {knn_order_sql},
             addr.npi,
             CASE addr.type WHEN 'practice' THEN 0 WHEN 'primary' THEN 1 ELSE 2 END,
             addr.checksum
    LIMIT :raw_probe_limit
), probe_stats AS MATERIALIZED (
    SELECT COUNT(*)::bigint AS raw_probe_count
    FROM nearest_addresses
), matched AS MATERIALIZED (
    SELECT
        addr.npi,
        {location_hash_sql} AS location_hash,
        addr.state_name AS state,
        addr.city_name AS city,
        LEFT(COALESCE(addr.postal_code, ''), 5) AS zip5,
        addr.candidate_distance_miles AS distance_miles,
        addr.type,
        addr.checksum,
        addr.telephone_number,
        addr.fax_number,
        addr.phone_number,
        addr.phone_extension,
        addr.fax_number_digits,
        addr.fax_extension,
        jsonb_build_object(
            'first_line', addr.first_line,
            'second_line', addr.second_line,
            'city', addr.city_name,
            'state', addr.state_name,
            'postal_code', addr.postal_code,
            'country_code', addr.country_code,
            'telephone_number', addr.telephone_number,
            'fax_number', addr.fax_number,
            'phone_number', addr.phone_number,
            'phone_extension', addr.phone_extension,
            'fax_number_digits', addr.fax_number_digits,
            'fax_extension', addr.fax_extension,
            'address_key', addr.address_key::text,
            'lat', addr.lat,
            'long', addr.long
        )::text AS address_payload,
        ROW_NUMBER() OVER (
            PARTITION BY addr.npi
            ORDER BY addr.candidate_distance_miles ASC NULLS LAST,
                     CASE addr.type WHEN 'practice' THEN 0 WHEN 'primary' THEN 1 ELSE 2 END,
                     addr.checksum
        ) AS address_rank
    FROM nearest_addresses addr
)
SELECT matched.*,
       (probe_stats.raw_probe_count < :raw_probe_limit) AS _ptg_source_exhausted
FROM matched
CROSS JOIN probe_stats
WHERE address_rank = 1
ORDER BY distance_miles ASC NULLS LAST, npi
LIMIT :limit
"""


def _membership_geo_sql(
    args: dict[str, Any],
    *,
    uses_unified_addresses: bool,
    parameter_map: dict[str, Any],
) -> tuple[str, list[str]] | None:
    """Build distance projection and indexable geo predicates."""
    has_geo_filter = args.get("lat") not in (None, "", "null") or args.get("long") not in (None, "", "null")
    if not has_geo_filter:
        if args.get("radius_miles") not in (None, "", "null"):
            return None
        return "NULL::double precision", []
    try:
        geo_lat = float(args.get("lat"))
        geo_long = float(args.get("long"))
        geo_radius = max(float(args.get("radius_miles") or 25.0), 0.0)
    except (TypeError, ValueError):
        return None
    parameter_map.update(
        geo_lat=geo_lat,
        geo_long=geo_long,
        geo_radius_miles=geo_radius,
        geo_min_lat=geo_lat - geo_radius / 69.0,
        geo_max_lat=geo_lat + geo_radius / 69.0,
        geo_min_long=geo_long - geo_radius / 69.0,
        geo_max_long=geo_long + geo_radius / 69.0,
    )
    distance_sql = _ptg2_geo_distance_miles_sql("addr.lat::float8", "addr.long::float8")
    if uses_unified_addresses:
        return distance_sql, [
            "addr.lat IS NOT NULL",
            "addr.long IS NOT NULL",
            "COALESCE(addr.address_precision, '') <> 'city_zip'",
            _ptg2_geo_dwithin_sql("addr.lat", "addr.long"),
        ]
    return distance_sql, [
        "addr.lat::float8 BETWEEN :geo_min_lat AND :geo_max_lat",
        "addr.long::float8 BETWEEN :geo_min_long AND :geo_max_long",
        f"{distance_sql} <= CAST(:geo_radius_miles AS double precision)",
    ]


def _membership_taxonomy_filters(
    args: dict[str, Any],
    parameter_map: dict[str, Any],
) -> list[str]:
    """Build provider predicates that must run before location limits."""
    filter_clauses: list[str] = []
    provider_sex_predicate = provider_sex_exists_sql(
        "addr.npi",
        parameter_map,
        "membership_provider_sex",
        args.get("provider_sex_code"),
        schema=PTG2_SCHEMA,
    )
    if provider_sex_predicate:
        filter_clauses.append(provider_sex_predicate)
    specialty_filter = resolve_provider_specialty_filter(args)
    if specialty_filter.active:
        filter_clauses.append(
            "addr.npi IN ("
            + provider_specialty_taxonomy_semijoin_sql(
                parameter_map,
                "membership_location_specialty",
                specialty_filter,
                schema=PTG2_SCHEMA,
            )
            + ")"
        )
    inferred_taxonomy_sql = _inferred_provider_taxonomy_code_sql(
        args,
        nt_alias="membership_location_nt",
        schema=PTG2_SCHEMA,
        params=parameter_map,
        param_prefix="membership_location_inferred_taxonomy",
    )
    if inferred_taxonomy_sql:
        filter_clauses.append(
            f"addr.npi IN (SELECT membership_location_nt.npi FROM {PTG2_SCHEMA}.npi_taxonomy "
            f"membership_location_nt WHERE {inferred_taxonomy_sql})"
        )
        filter_clauses.append(_ptg2_individual_npi_exists_sql("addr.npi"))
    return filter_clauses


def _membership_filter_sql(
    args: dict[str, Any],
    *,
    candidate_npis: tuple[int, ...] | None,
    uses_unified_addresses: bool,
    address_zip5_sql: str,
    parameter_map: dict[str, Any],
    literal_service_address_types: bool = False,
    include_taxonomy_filters: bool = True,
) -> tuple[str, str] | None:
    """Build address filters while preserving ZIP-or-radius semantics."""
    address_type_filter = (
        "addr.type IN ('primary', 'secondary', 'practice', 'site')"
        if literal_service_address_types
        else "addr.type = ANY(CAST(:address_types AS varchar[]))"
    )
    filter_clauses = ["addr.npi IS NOT NULL", address_type_filter]
    if candidate_npis is not None:
        if not candidate_npis:
            return None
        filter_clauses.append("addr.npi = ANY(CAST(:candidate_npis AS bigint[]))")
        parameter_map["candidate_npis"] = list(candidate_npis)
    if include_taxonomy_filters:
        filter_clauses.extend(_membership_taxonomy_filters(args, parameter_map))
    state_code = str(args.get("state") or "").strip().upper()
    city_name = str(args.get("city") or "").strip().upper()
    zip5 = _normalize_zip5(args.get("zip5") or args.get("zip"))
    if state_code:
        filter_clauses.append("UPPER(COALESCE(addr.state_name, '')) = :state_value")
        parameter_map["state_value"] = state_code
    if city_name:
        filter_clauses.append("UPPER(COALESCE(addr.city_name, '')) = :city_value")
        parameter_map["city_value"] = city_name
    geo_sql_parts = _membership_geo_sql(
        args,
        uses_unified_addresses=uses_unified_addresses,
        parameter_map=parameter_map,
    )
    if geo_sql_parts is None:
        return None
    distance_sql, geo_clauses = geo_sql_parts
    if zip5:
        parameter_map["zip5"] = zip5
        zip_clause = f"{address_zip5_sql} = :zip5"
        filter_clauses.append(f"({zip_clause} OR ({' AND '.join(geo_clauses)}))" if geo_clauses else zip_clause)
    elif geo_clauses:
        filter_clauses.extend(geo_clauses)
    if args.get("npi") not in (None, "", "null"):
        try:
            parameter_map["provider_npi"] = int(args["npi"])
        except (TypeError, ValueError):
            return None
        filter_clauses.append("addr.npi = :provider_npi")
    return " AND ".join(filter_clauses), distance_sql


def _membership_knn_order_sql(
    args: dict[str, Any],
    *,
    candidate_npis: tuple[int, ...] | None,
    uses_unified_addresses: bool,
    offset: int,
) -> str | None:
    """Select indexed coordinate ordering for an unscoped first-page probe."""
    if not uses_unified_addresses or candidate_npis is not None or offset != 0:
        return None
    has_coordinate_pair = all(
        args.get(field) not in (None, "", "null")
        for field in ("lat", "long")
    )
    has_non_coordinate_locator = any(
        args.get(field) not in (None, "", "null")
        for field in ("state", "city", "zip5", "zip", "npi")
    )
    if not has_coordinate_pair or has_non_coordinate_locator:
        return None
    return _ptg2_geo_knn_meters_sql("addr.lat", "addr.long")


async def _membership_location_query(
    session,
    serving_tables: PTG2ServingTables,
    args: dict[str, Any],
    *,
    candidate_npis: tuple[int, ...] | None,
    limit: int,
    offset: int = 0,
) -> _MembershipLocationQuery | None:
    """Build one bounded address lookup against immutable snapshot membership."""
    _require_strict_shared_v3(serving_tables)
    provider_npi_scope_table = f"{PTG2_SCHEMA}.ptg2_v3_npi_scope"
    has_geo_filter = args.get("lat") not in (None, "", "null") or args.get("long") not in (None, "", "null")
    address_table = await _ptg2_address_serving_table(
        session,
        _PTG2_UNIFIED_ADDRESS_COLUMNS if has_geo_filter else _PTG2_LEGACY_ADDRESS_COLUMNS,
        require_legacy_available=True,
    )
    if not address_table:
        return None
    uses_unified_addresses = _is_unified_address_table(address_table)
    parameter_map: dict[str, Any] = {
        "limit": max(int(limit), 1),
        "offset": max(int(offset), 0),
        "address_types": ["practice", "primary", "secondary", "site"]
        if uses_unified_addresses
        else ["primary", "secondary"],
    }
    knn_order_sql = _membership_knn_order_sql(
        args,
        candidate_npis=candidate_npis,
        uses_unified_addresses=uses_unified_addresses,
        offset=offset,
    )
    filter_sql_parts = _membership_filter_sql(
        args,
        candidate_npis=candidate_npis,
        uses_unified_addresses=uses_unified_addresses,
        address_zip5_sql=_ptg2_address_zip5_sql("addr", unified=uses_unified_addresses),
        parameter_map=parameter_map,
        literal_service_address_types=knn_order_sql is not None,
        include_taxonomy_filters=knn_order_sql is None,
    )
    if filter_sql_parts is None:
        return None
    filter_sql, distance_sql = filter_sql_parts
    parameter_map["shared_snapshot_key"] = _required_shared_snapshot_key(serving_tables)
    filter_sql = f"npi_scope.snapshot_key = :shared_snapshot_key AND ({filter_sql})"
    return _MembershipLocationQuery(
        address_table=address_table,
        npi_scope_table=provider_npi_scope_table,
        filter_sql=filter_sql,
        parameter_map=parameter_map,
        distance_sql=distance_sql,
        knn_order_sql=knn_order_sql,
    )


async def _enable_serial_knn_planning(session) -> tuple[str, str]:
    """Apply request-local KNN planner settings and return their prior values."""
    settings_result = await session.execute(
        text(
            """
            WITH previous_settings AS MATERIALIZED (
                SELECT current_setting('plan_cache_mode') AS plan_cache_mode,
                       current_setting('max_parallel_workers_per_gather') AS parallel_workers
            )
            SELECT previous_settings.plan_cache_mode,
                   previous_settings.parallel_workers,
                   set_config('plan_cache_mode', 'force_custom_plan', true),
                   set_config('max_parallel_workers_per_gather', '0', true)
              FROM previous_settings
            """
        )
    )
    settings_row = _row_mapping(settings_result.first())
    return str(settings_row["plan_cache_mode"]), str(settings_row["parallel_workers"])


async def _restore_knn_planning(session, prior_settings: tuple[str, str]) -> None:
    """Restore planner settings after the bounded KNN statement finishes."""
    await session.execute(
        text(
            """
            SELECT set_config('plan_cache_mode', :plan_cache_mode, true),
                   set_config('max_parallel_workers_per_gather', :parallel_workers, true)
            """
        ),
        {
            "plan_cache_mode": prior_settings[0],
            "parallel_workers": prior_settings[1],
        },
    )


async def _membership_location_rows(
    session,
    serving_tables: PTG2ServingTables,
    args: dict[str, Any],
    *,
    candidate_npis: tuple[int, ...] | None,
    limit: int,
    offset: int = 0,
) -> list[dict[str, Any]] | None:
    """Read address candidates scoped to NPIs represented by the snapshot."""
    if candidate_npis == ():
        return []
    query_context = await _membership_location_query(
        session,
        serving_tables,
        args,
        candidate_npis=candidate_npis,
        limit=limit,
        offset=offset,
    )
    if query_context is None:
        return None
    location_hash_sql = _ptg2_address_location_hash_sql("addr", query_context.address_table)
    if query_context.knn_order_sql is not None and offset == 0:
        requested_limit = max(int(limit), 1)
        probe_limit = requested_limit + max(requested_limit // 2, 64)
        query_context.parameter_map["raw_probe_limit"] = probe_limit + 1
        location_sql = _MEMBERSHIP_LOCATION_KNN_SQL.format(
            location_hash_sql=location_hash_sql,
            distance_sql=query_context.distance_sql,
            knn_order_sql=query_context.knn_order_sql,
            address_table=query_context.address_table,
            npi_scope_table=query_context.npi_scope_table,
            filter_sql=query_context.filter_sql,
        )
    else:
        location_sql = _MEMBERSHIP_LOCATION_SQL.format(
            location_hash_sql=location_hash_sql,
            distance_sql=query_context.distance_sql,
            address_table=query_context.address_table,
            npi_scope_table=query_context.npi_scope_table,
            filter_sql=query_context.filter_sql,
        )
    location_statement = text(location_sql)
    prior_planner_settings = None
    if query_context.knn_order_sql is not None and offset == 0:
        prior_planner_settings = await _enable_serial_knn_planning(session)
    try:
        query_result = await session.execute(location_statement, query_context.parameter_map)
    except Exception:
        # PostgreSQL errors abort the transaction; its rollback also restores
        # transaction-local planner settings. A restore query would only mask
        # the original failure while the transaction is aborted.
        raise
    try:
        return [_row_mapping(query_row) for query_row in query_result]
    finally:
        if prior_planner_settings is not None:
            await _restore_knn_planning(session, prior_planner_settings)


@dataclass
class _GraphLocationCandidates:
    location_rows: list[dict[str, Any]]
    group_ids_by_npi: dict[int, set[str]]
    taxonomy_filtered: bool = False


async def _append_rate_matched_locations(
    session,
    serving_tables: PTG2ServingTables,
    rate_scope: _ManifestRateScope,
    candidate_location_rows: list[dict[str, Any]],
    matched_location_rows: list[dict[str, Any]],
    group_ids_by_npi: dict[int, set[str]],
    seen_candidate_npis: set[int],
) -> int:
    """Append newly encountered locations whose provider groups carry the rate."""
    new_location_rows = [
        location
        for location in candidate_location_rows
        if int(location["npi"]) not in seen_candidate_npis
    ]
    if not new_location_rows:
        return 0
    prior_match_count = len(matched_location_rows)
    seen_candidate_npis.update(int(location["npi"]) for location in new_location_rows)
    owner_ids = tuple(_ptg2_npi_member_id(int(location["npi"])) for location in new_location_rows)
    group_ids_by_owner = await _shared_graph_members_by_id(
        session,
        serving_tables,
        "provider_npi_group",
        owner_ids,
    )
    for location_data, owner_id in zip(new_location_rows, owner_ids):
        npi = int(location_data["npi"])
        matching_group_ids = {
            group_id
            for group_id in group_ids_by_owner.get(owner_id, ())
            if _has_rate_scope_group(rate_scope, group_id)
        }
        if matching_group_ids:
            group_ids_by_npi[npi].update(matching_group_ids)
            matched_location_rows.append(location_data)
    return len(matched_location_rows) - prior_match_count


async def _direct_group_ids_by_npi(
    session,
    serving_tables: PTG2ServingTables,
    rate_scope: _ManifestRateScope,
) -> dict[int, set[str]] | None:
    """Expand a bounded rate group scope directly into its member NPIs."""
    if not rate_scope.group_ids or len(rate_scope.group_ids) > 1024:
        return None
    member_ids_by_group = await _shared_graph_members_by_id(
        session,
        serving_tables,
        "provider_group_npi",
        rate_scope.group_ids,
    )
    group_ids_by_npi: dict[int, set[str]] = defaultdict(set)
    for group_id, member_ids in member_ids_by_group.items():
        for member_id in member_ids:
            npi = _ptg2_npi_from_member_id(member_id)
            if npi is not None:
                group_ids_by_npi[npi].add(group_id)
    return group_ids_by_npi if len(group_ids_by_npi) <= 200_000 else None


@dataclass
class _GraphLocationProbeState:
    matched_location_rows: list[dict[str, Any]] = field(default_factory=list)
    group_ids_by_npi: dict[int, set[str]] = field(default_factory=lambda: defaultdict(set))
    seen_candidate_npis: set[int] = field(default_factory=set)
    filtered_candidates: _GraphLocationCandidates | None = None

    async def has_enough_after_append(
        self,
        session,
        serving_tables: PTG2ServingTables,
        args: dict[str, Any],
        rate_scope: _ManifestRateScope,
        candidate_location_rows: list[dict[str, Any]],
        *,
        taxonomy_filter_requested: bool,
        candidate_limit: int,
    ) -> bool:
        """Add one ordered address prefix and report when enough matches exist."""
        appended_count = await _append_rate_matched_locations(
            session,
            serving_tables,
            rate_scope,
            candidate_location_rows,
            self.matched_location_rows,
            self.group_ids_by_npi,
            self.seen_candidate_npis,
        )
        if not appended_count:
            return False
        current_candidates = _GraphLocationCandidates(
            self.matched_location_rows,
            dict(self.group_ids_by_npi),
        )
        if not taxonomy_filter_requested:
            return len(self.matched_location_rows) >= candidate_limit
        self.filtered_candidates = await _taxonomy_filtered_candidates(
            session,
            args,
            current_candidates,
            candidate_limit,
        )
        return len(self.filtered_candidates.location_rows) >= candidate_limit

    def observed_match_count(self, *, taxonomy_filter_requested: bool) -> int:
        """Return matches relevant to the next density projection."""
        if taxonomy_filter_requested and self.filtered_candidates is not None:
            return len(self.filtered_candidates.location_rows)
        return len(self.matched_location_rows)

    def result(self, *, taxonomy_filter_requested: bool) -> _GraphLocationCandidates:
        """Build the final candidate view after the bounded probe loop."""
        if taxonomy_filter_requested:
            return self.filtered_candidates or _GraphLocationCandidates([], {}, taxonomy_filtered=True)
        return _GraphLocationCandidates(self.matched_location_rows, dict(self.group_ids_by_npi))


def _graph_location_probe_batch_size(
    candidate_limit: int,
    *,
    taxonomy_filter_requested: bool,
) -> int:
    """Choose a bounded first probe that accounts for sparse taxonomies."""
    batch_size = min(max((candidate_limit * 3 + 1) // 2, 64), 1000)
    if taxonomy_filter_requested:
        return min(max(batch_size, candidate_limit * 16), 2000)
    return batch_size


def _next_graph_location_probe_limit(
    current_limit: int,
    *,
    batch_size: int,
    max_candidates: int,
    observed_matches: int,
    required_matches: int,
) -> int:
    """Grow a prefix probe from observed match density without changing its result set."""
    current_limit = max(int(current_limit), 1)
    batch_size = max(int(batch_size), 1)
    max_candidates = max(int(max_candidates), current_limit)
    minimum_growth = max(current_limit * 2, current_limit + batch_size)
    if observed_matches <= 0:
        proposed_limit = current_limit * 4
    else:
        required_matches = max(int(required_matches), 1)
        projected_limit = (
            current_limit * required_matches + int(observed_matches) - 1
        ) // int(observed_matches)
        proposed_limit = projected_limit + max(projected_limit // 4, batch_size)
    return min(max(minimum_growth, proposed_limit), current_limit * 8, max_candidates)


def _is_graph_location_source_exhausted(
    candidate_location_rows: Sequence[dict[str, Any]],
    probe_limit: int,
) -> bool:
    """Return whether a location probe consumed the full available source."""
    if "_ptg_source_exhausted" in candidate_location_rows[0]:
        return bool(candidate_location_rows[0].get("_ptg_source_exhausted"))
    return len(candidate_location_rows) < probe_limit


async def _paged_graph_candidates(
    session,
    serving_tables: PTG2ServingTables,
    args: dict[str, Any],
    rate_scope: _ManifestRateScope,
    candidate_limit: int,
) -> _GraphLocationCandidates | None:
    """Scan indexed addresses in bounded pages and reverse-check graph membership."""
    is_provider_filter_requested = _is_ptg2_provider_filter_requested(args)
    batch_size = _graph_location_probe_batch_size(
        candidate_limit, taxonomy_filter_requested=is_provider_filter_requested
    )
    max_candidates = max(_ptg2_manifest_location_match_limit() * 20, batch_size)
    probe_limit = batch_size
    probe_state = _GraphLocationProbeState()
    while probe_limit <= max_candidates:
        candidate_location_rows = await _membership_location_rows(
            session,
            serving_tables,
            args,
            candidate_npis=None,
            limit=probe_limit,
            offset=0,
        )
        if candidate_location_rows is None:
            return None
        if not candidate_location_rows:
            break
        has_enough_matches = await probe_state.has_enough_after_append(
            session,
            serving_tables,
            args,
            rate_scope,
            candidate_location_rows,
            taxonomy_filter_requested=is_provider_filter_requested,
            candidate_limit=candidate_limit,
        )
        if has_enough_matches:
            return probe_state.result(taxonomy_filter_requested=is_provider_filter_requested)
        is_source_exhausted = _is_graph_location_source_exhausted(
            candidate_location_rows,
            probe_limit,
        )
        if is_source_exhausted:
            break
        if probe_limit >= max_candidates:
            raise PTG2ManifestArtifactError(
                "PTG2 location traversal reached its configured exactness bound"
            )
        probe_limit = _next_graph_location_probe_limit(
            probe_limit,
            batch_size=batch_size,
            max_candidates=max_candidates,
            observed_matches=probe_state.observed_match_count(
                taxonomy_filter_requested=is_provider_filter_requested
            ),
            required_matches=candidate_limit,
        )
    return probe_state.result(taxonomy_filter_requested=is_provider_filter_requested)


async def _graph_location_candidates(
    session,
    serving_tables: PTG2ServingTables,
    args: dict[str, Any],
    rate_scope: _ManifestRateScope,
    candidate_limit: int,
) -> _GraphLocationCandidates | None:
    """Choose direct or address-first traversal based on rate-scope cardinality."""
    direct_groups_by_npi = await _direct_group_ids_by_npi(session, serving_tables, rate_scope)
    if direct_groups_by_npi is None:
        return await _paged_graph_candidates(session, serving_tables, args, rate_scope, candidate_limit)
    location_rows = await _membership_location_rows(
        session,
        serving_tables,
        args,
        candidate_npis=tuple(direct_groups_by_npi),
        limit=max(int(candidate_limit), 1),
    )
    if location_rows is None:
        return None
    return _GraphLocationCandidates(
        location_rows,
        direct_groups_by_npi,
        taxonomy_filtered=_is_ptg2_provider_filter_requested(args),
    )


async def _taxonomy_filtered_candidates(
    session,
    args: dict[str, Any],
    candidates: _GraphLocationCandidates,
    candidate_limit: int,
) -> _GraphLocationCandidates:
    """Apply provider taxonomy filters after graph membership resolution."""
    if candidates.taxonomy_filtered:
        return candidates
    if not candidates.location_rows:
        return _GraphLocationCandidates([], {}, taxonomy_filtered=True)
    matching_npis = set(
        await _filter_npis_by_taxonomy(
            session,
            args,
            tuple(int(location["npi"]) for location in candidates.location_rows),
            limit=len(candidates.location_rows),
        )
    )
    filtered_location_rows = [
        location
        for location in candidates.location_rows
        if int(location["npi"]) in matching_npis
    ][:candidate_limit]
    filtered_groups_by_npi = {
        int(location["npi"]): candidates.group_ids_by_npi.get(int(location["npi"]), set())
        for location in filtered_location_rows
    }
    return _GraphLocationCandidates(
        filtered_location_rows,
        filtered_groups_by_npi,
        taxonomy_filtered=True,
    )


def _graph_provider_data(
    location_data: dict[str, Any],
    enriched_data: dict[str, Any] | None,
    location_source: str,
) -> dict[str, Any]:
    """Overlay the matched location without erasing richer contact values."""
    npi = int(location_data["npi"])
    provider_data_map = dict(enriched_data or {"npi": npi, "provider_name": "TiC provider"})
    location_fields = ("distance_miles", "location_hash")
    if _has_street_address_payload(location_data.get("address_payload")) or not _has_street_address_payload(
        provider_data_map.get("address_payload")
    ):
        location_fields += ("state", "city", "zip5", "address_payload")
    provider_data_map.update({field: location_data.get(field) for field in location_fields})
    contact_fields = (
        "telephone_number",
        "fax_number",
        "phone_number",
        "phone_extension",
        "fax_number_digits",
        "fax_extension",
    )
    provider_data_map.update(
        {field: location_data.get(field) for field in contact_fields if location_data.get(field) is not None}
    )
    provider_data_map["location_source"] = location_source
    provider_data_map["location_confidence_code"] = location_source
    return provider_data_map


def _has_street_address_payload(address_payload: Any) -> bool:
    if not address_payload:
        return False
    try:
        address_data = json.loads(address_payload) if isinstance(address_payload, str) else address_payload
    except (TypeError, json.JSONDecodeError):
        return False
    if not isinstance(address_data, dict):
        return False
    return bool(str(address_data.get("first_line") or "").strip())


def _graph_providers_by_set(
    candidates: _GraphLocationCandidates,
    provider_data_by_npi: dict[int, dict[str, Any]],
    provider_sets_by_group: dict[str, tuple[str, ...]],
    location_source: str,
) -> tuple[set[str], dict[str, list[dict[str, Any]]]]:
    """Project matched graph groups back onto priced provider sets."""
    provider_set_ids: set[str] = set()
    providers_by_set: dict[str, list[dict[str, Any]]] = defaultdict(list)
    seen_npis_by_set: dict[str, set[int]] = defaultdict(set)
    for location_data in candidates.location_rows:
        npi = int(location_data["npi"])
        provider_data = _graph_provider_data(location_data, provider_data_by_npi.get(npi), location_source)
        for group_id in candidates.group_ids_by_npi.get(npi, ()):
            for provider_set_id in provider_sets_by_group.get(group_id, ()):
                if npi in seen_npis_by_set[provider_set_id]:
                    continue
                seen_npis_by_set[provider_set_id].add(npi)
                provider_set_ids.add(provider_set_id)
                providers_by_set[provider_set_id].append(provider_data)
    return provider_set_ids, dict(providers_by_set)


async def _project_graph_candidates(
    session,
    serving_tables: PTG2ServingTables,
    candidates: _GraphLocationCandidates,
    *,
    plan_id: str,
    snapshot_id: str | None,
    source_key: str | None,
) -> tuple[set[str], dict[str, list[dict[str, Any]]]] | None:
    """Enrich candidate NPIs and map their groups to provider sets."""
    group_ids = tuple(
        dict.fromkeys(
            group_id
            for matching_group_ids in candidates.group_ids_by_npi.values()
            for group_id in matching_group_ids
        )
    )
    provider_sets_by_group = await _manifest_sets_by_group(session, serving_tables, group_ids)
    if provider_sets_by_group is None:
        return None
    enriched_provider_rows = await _enriched_provider_rows_for_npis(
        session,
        npis=tuple(int(location["npi"]) for location in candidates.location_rows),
        limit=len(candidates.location_rows),
        plan_id=plan_id,
        snapshot_id=snapshot_id,
        source_key=source_key or serving_tables.source_key,
    )
    provider_data_by_npi = {
        int(provider_data["npi"]): provider_data
        for provider_data in enriched_provider_rows or []
    }
    address_table = await _ptg2_address_serving_table(
        session,
        _PTG2_LEGACY_ADDRESS_COLUMNS,
        require_legacy_available=True,
    )
    location_source = _ptg2_address_location_source(address_table or f"{PTG2_SCHEMA}.npi_address")
    return _graph_providers_by_set(candidates, provider_data_by_npi, provider_sets_by_group, location_source)


@dataclass(frozen=True)
class _ExplicitNpiGraphScope:
    npi: int
    group_ids: tuple[str, ...]
    provider_set_keys: tuple[int, ...]


async def _version_three_explicit_npi_graph_scope(
    session,
    serving_tables: PTG2ServingTables,
    args: Mapping[str, Any],
) -> _ExplicitNpiGraphScope | None:
    """Resolve one NPI to dense provider-set keys before reading a code block."""

    _require_strict_shared_v3(serving_tables)
    requested_npi = _normalize_npi(args.get("npi"))
    if requested_npi is None:
        return None
    shared_snapshot_key = _required_shared_snapshot_key(serving_tables)
    group_keys_by_npi = await lookup_shared_graph_members_from_db(
        session,
        shared_snapshot_key,
        PTG2_V3_GRAPH_NPI_TO_GROUP,
        (requested_npi,),
        schema_name=PTG2_SCHEMA,
    )
    group_keys = tuple(sorted(group_keys_by_npi.get(requested_npi, ())))
    if not group_keys:
        return _ExplicitNpiGraphScope(requested_npi, (), ())
    group_id_by_key = await _shared_provider_group_ids_for_keys(
        session,
        serving_tables,
        group_keys,
    )
    if set(group_id_by_key) != set(group_keys):
        raise PTG2ManifestArtifactError(
            "PTG2 v3 provider-group dictionary is missing an NPI-referenced group"
        )
    provider_set_keys_by_group = await lookup_shared_graph_members_from_db(
        session,
        shared_snapshot_key,
        PTG2_V3_GRAPH_GROUP_TO_PROVIDER_SET,
        group_keys,
        schema_name=PTG2_SCHEMA,
    )
    provider_set_keys = tuple(
        sorted(
            {
                int(provider_set_key)
                for group_provider_set_keys in provider_set_keys_by_group.values()
                for provider_set_key in group_provider_set_keys
            }
        )
    )
    return _ExplicitNpiGraphScope(
        requested_npi,
        tuple(group_id_by_key[group_key] for group_key in group_keys),
        provider_set_keys,
    )


async def _graph_candidates_for_rate_scope(
    session,
    serving_tables: PTG2ServingTables,
    args: dict[str, Any],
    rate_scope: _ManifestRateScope,
    candidate_limit: int,
    explicit_npi_scope: _ExplicitNpiGraphScope | None,
) -> _GraphLocationCandidates | None:
    """Use exact-NPI membership when available, otherwise retain broad traversal."""

    if explicit_npi_scope is None:
        return await _graph_location_candidates(
            session,
            serving_tables,
            args,
            rate_scope,
            candidate_limit,
        )
    matching_group_ids = {
        group_id
        for group_id in explicit_npi_scope.group_ids
        if _has_rate_scope_group(rate_scope, group_id)
    }
    if not matching_group_ids:
        return _GraphLocationCandidates([], {})
    location_rows = await _membership_location_rows(
        session,
        serving_tables,
        args,
        candidate_npis=(explicit_npi_scope.npi,),
        limit=1,
    )
    if location_rows is None:
        return None
    return _GraphLocationCandidates(
        location_rows,
        {explicit_npi_scope.npi: matching_group_ids},
    )


async def _graph_candidates_for_request(
    session,
    serving_tables: PTG2ServingTables,
    args: dict[str, Any],
    *,
    requested_code: str,
    requested_system: str | None,
    plan_id: str,
    candidate_limit: int,
    provider_set_keys: Iterable[int] | None = None,
    explicit_npi_scope: _ExplicitNpiGraphScope | None = None,
) -> _GraphLocationCandidates | None:
    """Resolve a code scope, preferring exact-NPI traversal for v3 snapshots."""

    if explicit_npi_scope is None:
        explicit_npi_scope = await _version_three_explicit_npi_graph_scope(
            session,
            serving_tables,
            args,
        )
    if explicit_npi_scope is not None and not explicit_npi_scope.provider_set_keys:
        return _GraphLocationCandidates([], {})
    scoped_provider_set_keys = (
        set(int(provider_set_key) for provider_set_key in provider_set_keys)
        if provider_set_keys is not None
        else None
    )
    if explicit_npi_scope is not None:
        explicit_provider_set_keys = set(explicit_npi_scope.provider_set_keys)
        scoped_provider_set_keys = (
            explicit_provider_set_keys
            if scoped_provider_set_keys is None
            else scoped_provider_set_keys.intersection(explicit_provider_set_keys)
        )
    if scoped_provider_set_keys is not None and not scoped_provider_set_keys:
        return _GraphLocationCandidates([], {})
    rate_scope = await _shared_rate_scope(
        session,
        serving_tables,
        plan_id=plan_id,
        plan_market_type=args.get("plan_market_type") or args.get("market_type") or "",
        reported_code=requested_code,
        code_system=requested_system,
        provider_set_keys=(
            tuple(sorted(scoped_provider_set_keys))
            if scoped_provider_set_keys is not None
            else None
        ),
    )
    if rate_scope.id_count == 0:
        return _GraphLocationCandidates([], {})
    return await _graph_candidates_for_rate_scope(
        session,
        serving_tables,
        args,
        rate_scope,
        candidate_limit,
        explicit_npi_scope,
    )


async def _graph_location_matches(
    session,
    serving_tables: PTG2ServingTables,
    args: dict[str, Any],
    *,
    candidate_limit: int,
    plan_id: str,
    snapshot_id: str | None = None,
    source_key: str | None = None,
    provider_set_keys: Iterable[int] | None = None,
    explicit_npi_scope: _ExplicitNpiGraphScope | None = None,
) -> tuple[set[str], dict[str, list[dict[str, Any]]]] | None:
    """Resolve geo-filtered provider sets through normalized membership artifacts."""
    requested_system = _normalize_code_system(args.get("code_system") or args.get("reported_code_system"))
    requested_code = (
        canonical_catalog_code(requested_system, args.get("code") or args.get("reported_code"))
        if requested_system
        else str(args.get("code") or args.get("reported_code") or "").strip()
    )
    if not plan_id or not requested_code:
        return None
    candidates = await _graph_candidates_for_request(
        session,
        serving_tables,
        args,
        requested_code=requested_code,
        requested_system=requested_system,
        plan_id=plan_id,
        candidate_limit=candidate_limit,
        provider_set_keys=provider_set_keys,
        explicit_npi_scope=explicit_npi_scope,
    )
    if candidates is None:
        return None
    filtered_candidates = await _taxonomy_filtered_candidates(session, args, candidates, candidate_limit)
    if not filtered_candidates.location_rows:
        return set(), {}
    return await _project_graph_candidates(
        session,
        serving_tables,
        filtered_candidates,
        plan_id=plan_id,
        snapshot_id=snapshot_id,
        source_key=source_key,
    )


async def _ptg2_manifest_location_provider_matches(
    session,
    serving_tables: PTG2ServingTables,
    args: dict[str, Any],
    *,
    candidate_limit: int | None = None,
    plan_id: str | None = None,
    snapshot_id: str | None = None,
    source_key: str | None = None,
    provider_set_keys: Iterable[int] | None = None,
    explicit_npi_scope: _ExplicitNpiGraphScope | None = None,
    require_exhaustive: bool = False,
) -> tuple[set[str], dict[str, list[dict[str, Any]]]] | None:
    """Resolve location-filtered provider sets through the strict shared graph."""

    _require_strict_shared_v3(serving_tables)
    configured_match_limit = _ptg2_manifest_location_match_limit()
    graph_candidate_limit = configured_match_limit
    if require_exhaustive:
        graph_candidate_limit = configured_match_limit + 1
    elif candidate_limit is not None:
        requested_candidate_limit = max(int(candidate_limit), 1)
        if requested_candidate_limit > configured_match_limit:
            raise PTG2ManifestArtifactError(
                "PTG2 location pagination exceeds its configured exactness bound"
            )
        graph_candidate_limit = requested_candidate_limit
    matches = await _graph_location_matches(
        session,
        serving_tables,
        args,
        candidate_limit=graph_candidate_limit,
        plan_id=str(plan_id or args.get("plan_id") or args.get("plan_external_id") or "").strip(),
        snapshot_id=snapshot_id,
        source_key=source_key,
        provider_set_keys=provider_set_keys,
        explicit_npi_scope=explicit_npi_scope,
    )
    if matches is None or not require_exhaustive:
        return matches
    provider_set_ids, providers_by_set = matches
    matched_npis = {
        int(provider["npi"])
        for providers in providers_by_set.values()
        for provider in providers
        if provider.get("npi") not in (None, "")
    }
    if len(matched_npis) > configured_match_limit:
        raise PTG2ManifestArtifactError(
            "PTG2 location traversal reached its configured exactness bound"
        )
    return provider_set_ids, providers_by_set

async def _provider_rows_for_sets(
    session,
    serving_tables: PTG2ServingTables,
    provider_set_global_ids: list[str] | tuple[str, ...],
    *,
    limit_per_set: int | None,
    args: dict[str, Any] | None = None,
) -> dict[str, list[dict[str, Any]]] | None:
    """Resolve enriched provider rows for each requested provider set."""

    provider_set_ids = _ptg2_manifest_ids(tuple(provider_set_global_ids))
    if not provider_set_ids:
        return {}
    args = args or {}
    is_provider_filter_requested = _is_ptg2_provider_filter_requested(args)
    candidate_limit_per_set = (
        max(int(limit_per_set), 1) if limit_per_set is not None else None
    )
    if is_provider_filter_requested:
        candidate_limit_per_set = (
            max(candidate_limit_per_set * 200, 1000)
            if candidate_limit_per_set is not None
            else None
        )

    npis_by_set = await _provider_npis_for_sets(
        session,
        serving_tables,
        provider_set_ids,
        limit_per_set=candidate_limit_per_set,
    )
    if is_provider_filter_requested:
        filtered_npis_by_set: dict[str, tuple[int, ...]] = {}
        for provider_set_id in provider_set_ids:
            provider_npis = npis_by_set.get(provider_set_id, ())
            filtered_npis_by_set[provider_set_id] = await _filter_npis_by_taxonomy(
                session,
                args,
                provider_npis,
                limit=(
                    max(int(limit_per_set), 1)
                    if limit_per_set is not None
                    else max(len(provider_npis), 1)
                ),
            )
        npis_by_set = filtered_npis_by_set

    def selected_npis(provider_set_id: str) -> tuple[int, ...]:
        """Return provider-set NPIs capped by the optional per-set limit."""
        provider_npis = npis_by_set.get(provider_set_id, ())
        return (
            provider_npis[: max(int(limit_per_set), 1)]
            if limit_per_set is not None
            else provider_npis
        )

    all_npis = tuple(
        dict.fromkeys(
            npi
            for provider_set_id in provider_set_ids
            for npi in selected_npis(provider_set_id)
        )
    )
    provider_rows = await _enriched_provider_rows_for_npis(
        session,
        npis=all_npis,
        limit=max(len(all_npis), 1),
        plan_id=str(args.get("plan_id") or args.get("plan_external_id") or "").strip() or None,
        snapshot_id=str(args.get("snapshot_id") or "").strip() or None,
        source_key=serving_tables.source_key or str(args.get("source_key") or "").strip() or None,
    )
    if provider_rows is None:
        return None
    providers_by_npi = {
        int(provider_row.get("npi")): provider_row
        for provider_row in provider_rows
        if provider_row.get("npi") is not None
    }
    return {
        provider_set_id: [
            providers_by_npi.get(npi) or {"npi": npi, "provider_name": "TiC provider"}
            for npi in selected_npis(provider_set_id)
        ]
        for provider_set_id in provider_set_ids
    }


async def _provider_sets_for_npi(
    session,
    serving_tables: PTG2ServingTables,
    npi: int,
) -> tuple[str, ...]:
    return await _provider_sets_from_membership_graph(session, serving_tables, npi)

async def _provider_sets_from_membership_graph(
    session,
    serving_tables: PTG2ServingTables,
    npi: int,
) -> tuple[str, ...]:
    """Resolve reverse NPI membership from the strict shared graph."""

    _require_strict_shared_v3(serving_tables)
    group_ids = await _shared_graph_members_for_id(
        session,
        serving_tables,
        "provider_npi_group",
        _ptg2_npi_member_id(npi),
    )
    if not group_ids:
        return ()
    provider_sets_by_group = await _manifest_sets_by_group(session, serving_tables, group_ids)
    return tuple(
        sorted(
            {
                provider_set_id
                for provider_set_ids in provider_sets_by_group.values()
                for provider_set_id in provider_set_ids
            }
        )
    )


_ProviderExpansionKey = tuple[str, str, str, str, str, str]


@dataclass(frozen=True)
class _ProviderExpansionSelection:
    row_data: list[dict[str, Any]]
    providers_by_set: dict[str, list[dict[str, Any]]]
    rank_by_key: dict[_ProviderExpansionKey, int]
    exhausted: bool

    @property
    def total_lower_bound(self) -> int:
        """Return the number of distinct ranked provider expansion keys."""
        return len(self.rank_by_key)


_PTG2_PROVIDER_EXPANSION_SELECTION_CACHE_MAX_ENTRIES = 1024
_PTG2_PROVIDER_EXPANSION_SELECTION_CACHE: OrderedDict[
    tuple[int, str, int, bool, str],
    _ProviderExpansionSelection,
] = OrderedDict()


def _provider_expansion_selection_cache_key(
    serving_tables: PTG2ServingTables,
    *,
    code_rows: list[Mapping[str, Any]],
    args: Mapping[str, Any],
    snapshot_id: str,
    source_trace_set_hash: str | None,
    network_names: list[str],
    target_count: int,
    descending: bool,
) -> tuple[int, str, int, bool, str] | None:
    """Build an immutable sealed-snapshot key for exact provider expansion."""

    shared_snapshot_key = getattr(serving_tables, "shared_snapshot_key", None)
    if shared_snapshot_key is None:
        return None
    selection_signature = json.dumps(
        {
            "args": dict(args),
            "code_rows": code_rows,
            "network_names": network_names,
            "source_key": getattr(serving_tables, "source_key", None),
            "source_trace_set_hash": source_trace_set_hash,
        },
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    )
    return (
        int(shared_snapshot_key),
        snapshot_id,
        max(int(target_count), 1),
        bool(descending),
        selection_signature,
    )


def _provider_expansion_key(
    serving_row: Mapping[str, Any],
    *,
    npi: int | None,
) -> _ProviderExpansionKey:
    source_key = serving_row.get("source_artifact_key")
    if source_key is None:
        source_key = serving_row.get("source_key")
    reported_system = str(
        serving_row.get("reported_code_system")
        or serving_row.get("service_code_system")
        or serving_row.get("billing_code_type")
        or ""
    )
    reported_code = str(
        serving_row.get("reported_code")
        or serving_row.get("service_code")
        or serving_row.get("billing_code")
        or ""
    )
    arrangement = str(serving_row.get("negotiation_arrangement") or "")
    if npi is not None:
        return (
            "npi",
            str(int(npi)),
            reported_system,
            reported_code,
            arrangement,
            str(source_key if source_key is not None else ""),
        )
    occurrence_id = _ptg2_manifest_id(
        serving_row.get("serving_content_hash_128")
        or serving_row.get("rate_pack_hash")
    )
    if not occurrence_id:
        raise PTG2ManifestArtifactError(
            "PTG2 strict V3 NPI-free rate is missing its occurrence identity"
        )
    return (
        "rate",
        occurrence_id,
        reported_system,
        reported_code,
        arrangement,
        str(source_key if source_key is not None else ""),
    )


def _rank_provider_expansion_prefix(
    row_data: list[dict[str, Any]],
    npis_by_set: Mapping[str, tuple[int, ...]],
    *,
    target_count: int,
) -> tuple[
    dict[_ProviderExpansionKey, int],
    tuple[int, ...],
    tuple[str, ...],
]:
    rank_by_key: dict[_ProviderExpansionKey, int] = {}
    selected_npi_order_by_value: dict[int, None] = {}
    selected_provider_set_order_by_id: dict[str, None] = {}
    for serving_row in row_data:
        provider_set_id = _ptg2_manifest_id(
            serving_row.get("provider_set_global_id_128")
        )
        if not provider_set_id:
            raise PTG2ManifestArtifactError(
                "PTG2 strict V3 rate is missing its provider-set identity"
            )
        provider_npis = npis_by_set.get(provider_set_id, ())
        candidates: tuple[int | None, ...] = provider_npis or (None,)
        for npi in candidates:
            key = _provider_expansion_key(serving_row, npi=npi)
            if key in rank_by_key:
                continue
            rank_by_key[key] = len(rank_by_key)
            selected_provider_set_order_by_id[provider_set_id] = None
            if npi is not None:
                selected_npi_order_by_value[int(npi)] = None
            if len(rank_by_key) >= target_count:
                return (
                    rank_by_key,
                    tuple(selected_npi_order_by_value),
                    tuple(selected_provider_set_order_by_id),
                )
    return (
        rank_by_key,
        tuple(selected_npi_order_by_value),
        tuple(selected_provider_set_order_by_id),
    )


def _filtered_provider_prefix_cache_key(
    serving_tables: PTG2ServingTables,
    provider_set_id: str,
    args: Mapping[str, Any],
    target_count: int,
) -> tuple[int, str, int, str] | None:
    shared_snapshot_key = getattr(serving_tables, "shared_snapshot_key", None)
    if shared_snapshot_key is None:
        return None
    filter_signature = json.dumps(
        dict(args),
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    )
    return (
        int(shared_snapshot_key),
        provider_set_id,
        max(int(target_count), 1),
        filter_signature,
    )


async def _filtered_provider_npis_for_expansion_set(
    session,
    serving_tables: PTG2ServingTables,
    provider_set_id: str,
    args: Mapping[str, Any],
    *,
    target_count: int,
) -> tuple[int, ...]:
    """Read enough ordered set members to prove a filtered provider prefix."""

    cache_key = _filtered_provider_prefix_cache_key(
        serving_tables,
        provider_set_id,
        args,
        target_count,
    )
    if cache_key is not None:
        cached_npis = _PTG2_FILTERED_PROVIDER_PREFIX_CACHE.get(cache_key)
        if cached_npis is not None:
            _PTG2_FILTERED_PROVIDER_PREFIX_CACHE.move_to_end(cache_key)
            return cached_npis
    raw_limit = max(max(int(target_count), 1) * 4, 32)
    while True:
        npis_by_set = await _provider_npis_for_sets(
            session,
            serving_tables,
            [provider_set_id],
            limit_per_set=raw_limit,
        )
        provider_npis = npis_by_set.get(provider_set_id, ())
        filtered_npis = await _filter_npis_by_taxonomy(
            session,
            dict(args),
            provider_npis,
            limit=max(int(target_count), 1),
        )
        if len(filtered_npis) >= target_count or len(provider_npis) < raw_limit:
            if cache_key is not None:
                _PTG2_FILTERED_PROVIDER_PREFIX_CACHE[cache_key] = filtered_npis
                _PTG2_FILTERED_PROVIDER_PREFIX_CACHE.move_to_end(cache_key)
                while (
                    len(_PTG2_FILTERED_PROVIDER_PREFIX_CACHE)
                    > _PTG2_PROVIDER_NPI_PREFIX_CACHE_MAX_ENTRIES
                ):
                    _PTG2_FILTERED_PROVIDER_PREFIX_CACHE.popitem(last=False)
            return filtered_npis
        raw_limit *= 2


async def _rank_filtered_provider_expansion_prefix(
    session,
    serving_tables: PTG2ServingTables,
    row_data: list[dict[str, Any]],
    args: Mapping[str, Any],
    *,
    target_count: int,
    npis_by_set: dict[str, tuple[int, ...]],
) -> tuple[
    dict[_ProviderExpansionKey, int],
    tuple[int, ...],
    tuple[str, ...],
]:
    """Rank a provider-filtered prefix without expanding every rate member."""

    rank_by_key: dict[_ProviderExpansionKey, int] = {}
    selected_npi_order_by_value: dict[int, None] = {}
    selected_provider_set_order_by_id: dict[str, None] = {}
    for serving_row in row_data:
        provider_set_id = _ptg2_manifest_id(
            serving_row.get("provider_set_global_id_128")
        )
        if not provider_set_id:
            raise PTG2ManifestArtifactError(
                "PTG2 strict V3 rate is missing its provider-set identity"
            )
        if provider_set_id not in npis_by_set:
            npis_by_set[provider_set_id] = (
                await _filtered_provider_npis_for_expansion_set(
                    session,
                    serving_tables,
                    provider_set_id,
                    args,
                    target_count=target_count,
                )
            )
        for npi in npis_by_set[provider_set_id]:
            key = _provider_expansion_key(serving_row, npi=npi)
            if key in rank_by_key:
                continue
            rank_by_key[key] = len(rank_by_key)
            selected_provider_set_order_by_id[provider_set_id] = None
            selected_npi_order_by_value[int(npi)] = None
            if len(rank_by_key) >= target_count:
                return (
                    rank_by_key,
                    tuple(selected_npi_order_by_value),
                    tuple(selected_provider_set_order_by_id),
                )
    return (
        rank_by_key,
        tuple(selected_npi_order_by_value),
        tuple(selected_provider_set_order_by_id),
    )


def _cached_provider_set_ids_for_npis(
    shared_snapshot_key: int,
    npis: tuple[int, ...],
) -> tuple[dict[int, tuple[str, ...]], tuple[int, ...]]:
    """Return cached reverse memberships and NPIs that still need loading."""

    provider_set_ids_by_npi: dict[int, tuple[str, ...]] = {}
    uncached_npis: list[int] = []
    for npi in npis:
        cache_key = (shared_snapshot_key, npi)
        cached_provider_set_ids = _PTG2_PROVIDER_SET_IDS_BY_NPI_CACHE.get(
            cache_key
        )
        if cached_provider_set_ids is None:
            uncached_npis.append(npi)
            continue
        _PTG2_PROVIDER_SET_IDS_BY_NPI_CACHE.move_to_end(cache_key)
        provider_set_ids_by_npi[npi] = cached_provider_set_ids
    return provider_set_ids_by_npi, tuple(uncached_npis)


def _cache_provider_set_ids_for_npis(
    shared_snapshot_key: int,
    provider_set_ids_by_npi: Mapping[int, tuple[str, ...]],
) -> None:
    """Cache reverse provider-set memberships for one sealed snapshot."""

    for npi, provider_set_ids in provider_set_ids_by_npi.items():
        cache_key = (shared_snapshot_key, npi)
        _PTG2_PROVIDER_SET_IDS_BY_NPI_CACHE[cache_key] = provider_set_ids
        _PTG2_PROVIDER_SET_IDS_BY_NPI_CACHE.move_to_end(cache_key)
        while (
            len(_PTG2_PROVIDER_SET_IDS_BY_NPI_CACHE)
            > _PTG2_PROVIDER_NPI_PREFIX_CACHE_MAX_ENTRIES
        ):
            _PTG2_PROVIDER_SET_IDS_BY_NPI_CACHE.popitem(last=False)


async def _provider_set_ids_for_selected_npis(
    session,
    serving_tables: PTG2ServingTables,
    npis: tuple[int, ...],
) -> dict[int, tuple[str, ...]]:
    """Resolve provider-set memberships for selected NPIs with sealed caching."""

    if not npis:
        return {}
    shared_snapshot_key = _required_shared_snapshot_key(serving_tables)
    provider_set_ids_by_npi, uncached_npis = (
        _cached_provider_set_ids_for_npis(
            shared_snapshot_key,
            npis,
        )
    )
    if not uncached_npis:
        return provider_set_ids_by_npi
    member_id_by_npi = {
        npi: _ptg2_npi_member_id(npi)
        for npi in uncached_npis
    }
    groups_by_member = await _shared_graph_members_by_id(
        session,
        serving_tables,
        "provider_npi_group",
        tuple(member_id_by_npi.values()),
    )
    group_ids = tuple(
        dict.fromkeys(
            group_id
            for member_id in member_id_by_npi.values()
            for group_id in groups_by_member.get(member_id, ())
        )
    )
    sets_by_group = await _manifest_sets_by_group(
        session,
        serving_tables,
        group_ids,
    )
    resolved_provider_set_ids_by_npi = {
        npi: tuple(
            dict.fromkeys(
                provider_set_id
                for group_id in groups_by_member.get(member_id, ())
                for provider_set_id in sets_by_group.get(group_id, ())
            )
        )
        for npi, member_id in member_id_by_npi.items()
    }
    provider_set_ids_by_npi.update(resolved_provider_set_ids_by_npi)
    _cache_provider_set_ids_for_npis(
        shared_snapshot_key,
        resolved_provider_set_ids_by_npi,
    )
    return provider_set_ids_by_npi


async def _selected_provider_rows_by_set(
    session,
    serving_tables: PTG2ServingTables,
    *,
    npis: tuple[int, ...],
    provider_set_ids_by_npi: Mapping[int, tuple[str, ...]],
    args: Mapping[str, Any],
    snapshot_id: str,
) -> dict[str, list[dict[str, Any]]] | None:
    provider_rows = await _enriched_provider_rows_for_npis(
        session,
        npis=npis,
        limit=max(len(npis), 1),
        plan_id=str(args.get("plan_id") or args.get("plan_external_id") or "").strip()
        or None,
        snapshot_id=snapshot_id,
        source_key=serving_tables.source_key
        or str(args.get("source_key") or "").strip()
        or None,
    )
    if provider_rows is None:
        return None
    provider_by_npi = {
        int(provider_row["npi"]): provider_row
        for provider_row in provider_rows
        if provider_row.get("npi") is not None
    }
    provider_set_ids = tuple(
        dict.fromkeys(
            provider_set_id
            for npi in npis
            for provider_set_id in provider_set_ids_by_npi.get(npi, ())
        )
    )
    return {
        provider_set_id: [
            provider_by_npi.get(npi)
            or {"npi": npi, "provider_name": "TiC provider"}
            for npi in npis
            if provider_set_id in provider_set_ids_by_npi.get(npi, ())
        ]
        for provider_set_id in provider_set_ids
    }


def _candidate_audit_provider_rows_by_set(
    *,
    candidate_npi: int,
    serving_rows: Iterable[Mapping[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Resolve only the audited NPI, without requiring address enrichment."""

    candidate_provider_set_ids = tuple(
        dict.fromkeys(
            provider_set_id
            for serving_row in serving_rows
            if (
                provider_set_id := _ptg2_manifest_id(
                    serving_row.get("provider_set_global_id_128")
                )
            )
        )
    )
    return {
        provider_set_id: [
            {
                "npi": candidate_npi,
                "provider_name": "TiC provider",
            }
        ]
        for provider_set_id in candidate_provider_set_ids
    }


async def _exact_npi_provider_rows_by_set(
    session,
    serving_tables: PTG2ServingTables,
    *,
    npi: int,
    serving_rows: Iterable[Mapping[str, Any]],
    args: Mapping[str, Any],
    snapshot_id: str,
) -> dict[str, list[dict[str, Any]]] | None:
    """Enrich only the requested NPI for its already-proven provider sets."""

    provider_set_ids = tuple(
        dict.fromkeys(
            provider_set_id
            for serving_row in serving_rows
            if (
                provider_set_id := _ptg2_manifest_id(
                    serving_row.get("provider_set_global_id_128")
                )
            )
        )
    )
    return await _selected_provider_rows_by_set(
        session,
        serving_tables,
        npis=(npi,),
        provider_set_ids_by_npi={npi: provider_set_ids},
        args=args,
        snapshot_id=snapshot_id,
    )


def _next_provider_expansion_rate_window(
    current_window: int,
    *,
    target_count: int,
    distinct_count: int,
    declared_rate_count: int,
) -> int:
    projected = (
        current_window * target_count + max(distinct_count, 1) - 1
    ) // max(distinct_count, 1)
    return min(
        declared_rate_count,
        max(current_window + PTG2_SERVING_BINARY_V3_PAGE_ROWS, current_window * 2, projected),
    )


async def _strict_cost_provider_expansion_selection(
    session,
    serving_tables: PTG2ServingTables,
    *,
    code_rows: list[Mapping[str, Any]],
    args: Mapping[str, Any],
    snapshot_id: str,
    source_trace_set_hash: str | None,
    network_names: list[str],
    target_count: int,
    descending: bool,
) -> _ProviderExpansionSelection | None:
    """Expand cost-ordered rates until the requested provider prefix is complete."""
    cache_key = _provider_expansion_selection_cache_key(
        serving_tables,
        code_rows=code_rows,
        args=args,
        snapshot_id=snapshot_id,
        source_trace_set_hash=source_trace_set_hash,
        network_names=network_names,
        target_count=target_count,
        descending=descending,
    )
    if cache_key is not None:
        cached_selection = _PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.get(
            cache_key
        )
        if cached_selection is not None:
            _PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.move_to_end(cache_key)
            return deepcopy(cached_selection)
    declared_rate_count = sum(
        max(int(code_row.get("rate_count") or 0), 0)
        for code_row in code_rows
    )
    if declared_rate_count <= 0:
        return _ProviderExpansionSelection([], {}, {}, True)
    rate_window = min(
        declared_rate_count,
        max(PTG2_SERVING_BINARY_V3_PAGE_ROWS, max(int(target_count), 1)),
    )
    rank_by_key: dict[_ProviderExpansionKey, int] = {}
    selected_npis: tuple[int, ...] = ()
    selected_provider_set_ids: tuple[str, ...] = ()
    is_exhausted = False
    serving_rows: list[dict[str, Any]] = []
    filtered_npis_by_set: dict[str, tuple[int, ...]] = {}
    is_provider_filter_requested = _is_ptg2_provider_filter_requested(dict(args))
    while True:
        serving_rows = await _merge_manifest_code_variant_rows(
            session,
            serving_tables,
            code_rows=code_rows,
            provider_set_keys=None,
            source_trace_set_hash=source_trace_set_hash,
            network_names=network_names,
            limit=rate_window,
            offset=0,
            descending=descending,
        )
        if serving_rows is None:
            return None
        provider_set_ids = tuple(
            dict.fromkeys(
                provider_set_id
                for serving_row in serving_rows
                if (
                    provider_set_id := _ptg2_manifest_id(
                        serving_row.get("provider_set_global_id_128")
                    )
                )
            )
        )
        if is_provider_filter_requested:
            rank_by_key, selected_npis, selected_provider_set_ids = (
                await _rank_filtered_provider_expansion_prefix(
                    session,
                    serving_tables,
                    serving_rows,
                    args,
                    target_count=max(int(target_count), 1),
                    npis_by_set=filtered_npis_by_set,
                )
            )
        else:
            npis_by_set = await _provider_npis_for_sets(
                session,
                serving_tables,
                provider_set_ids,
                limit_per_set=max(int(target_count), 1),
            )
            rank_by_key, selected_npis, selected_provider_set_ids = (
                _rank_provider_expansion_prefix(
                    serving_rows,
                    npis_by_set,
                    target_count=max(int(target_count), 1),
                )
            )
        is_exhausted = (
            rate_window >= declared_rate_count
            or len(serving_rows) < rate_window
        )
        if len(rank_by_key) >= target_count or is_exhausted:
            break
        next_window = _next_provider_expansion_rate_window(
            rate_window,
            target_count=target_count,
            distinct_count=len(rank_by_key),
            declared_rate_count=declared_rate_count,
        )
        if next_window <= rate_window:
            raise PTG2ManifestArtifactError(
                "PTG2 strict V3 provider expansion did not make progress"
            )
        rate_window = next_window

    provider_set_ids_by_npi = await _provider_set_ids_for_selected_npis(
        session,
        serving_tables,
        selected_npis,
    )
    completion_provider_set_ids = tuple(
        dict.fromkeys(
            (
                provider_set_id
                for npi in selected_npis
                for provider_set_id in provider_set_ids_by_npi.get(npi, ())
            )
        )
    )
    completion_provider_set_ids = tuple(
        dict.fromkeys((*completion_provider_set_ids, *selected_provider_set_ids))
    )
    provider_set_key_by_id = await _provider_set_keys_for_ids(
        session,
        serving_tables,
        completion_provider_set_ids,
    )
    if set(provider_set_key_by_id) != set(completion_provider_set_ids):
        raise PTG2ManifestArtifactError(
            "PTG2 strict V3 provider expansion references an unknown provider set"
        )
    completion_rows = await _merge_manifest_code_variant_rows(
        session,
        serving_tables,
        code_rows=code_rows,
        provider_set_keys=provider_set_key_by_id.values(),
        source_trace_set_hash=source_trace_set_hash,
        network_names=network_names,
        limit=None,
        offset=0,
        descending=descending,
    )
    if completion_rows is None:
        return None
    providers_by_set = await _selected_provider_rows_by_set(
        session,
        serving_tables,
        npis=selected_npis,
        provider_set_ids_by_npi=provider_set_ids_by_npi,
        args=args,
        snapshot_id=snapshot_id,
    )
    if providers_by_set is None:
        return None
    for provider_set_id in selected_provider_set_ids:
        providers_by_set.setdefault(provider_set_id, [])
    selection = _ProviderExpansionSelection(
        row_data=completion_rows,
        providers_by_set=providers_by_set,
        rank_by_key=rank_by_key,
        exhausted=is_exhausted and len(rank_by_key) < target_count,
    )
    if cache_key is not None:
        _PTG2_PROVIDER_EXPANSION_SELECTION_CACHE[cache_key] = deepcopy(
            selection
        )
        _PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.move_to_end(cache_key)
        while (
            len(_PTG2_PROVIDER_EXPANSION_SELECTION_CACHE)
            > _PTG2_PROVIDER_EXPANSION_SELECTION_CACHE_MAX_ENTRIES
        ):
            _PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.popitem(last=False)
    return selection


async def _procedure_details_for_rows(
    session,
    serving_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> dict[tuple[str, str], dict[str, Any]]:
    lookup_keys = sorted(
        key
        for key in {
            _catalog_key(
                serving_row.get("reported_code_system"),
                serving_row.get("reported_code"),
            )
            for serving_row in serving_rows
            if serving_row.get("reported_code_system")
            and serving_row.get("reported_code")
        }
        if key is not None
    )
    if not lookup_keys:
        return {}
    clauses: list[str] = []
    query_parameters_by_name: dict[str, Any] = {}
    for idx, (code_system, code) in enumerate(lookup_keys):
        clauses.append(f"(code_system = :code_system_{idx} AND code = :code_{idx})")
        query_parameters_by_name[f"code_system_{idx}"] = code_system
        query_parameters_by_name[f"code_{idx}"] = code
    try:
        procedure_catalog_query = await session.execute(
            text(
                f"""
                SELECT code_system, code, display_name, short_description
                FROM {PTG2_SCHEMA}.code_catalog
                WHERE {" OR ".join(clauses)}
                """
            ),
            query_parameters_by_name,
        )
    except Exception:
        await _rollback_optional_ptg2_query(session)
        return {}
    return {
        (
            str(procedure_metadata.get("code_system") or ""),
            str(procedure_metadata.get("code") or ""),
        ): {
            "procedure_name": procedure_metadata.get("display_name"),
            "procedure_description": procedure_metadata.get(
                "short_description"
            )
            or procedure_metadata.get("display_name"),
        }
        for procedure_metadata in (
            _row_mapping(procedure_record)
            for procedure_record in procedure_catalog_query
        )
    }


async def _search_manifest_serving_table(
    session,
    snapshot_id: str,
    args: dict[str, Any],
    pagination,
    serving_tables: PTG2ServingTables,
    mode_value: str,
) -> dict[str, Any] | None:
    """Serve one strict shared V3 snapshot through sparse PostgreSQL reads."""

    _require_strict_shared_v3(serving_tables)
    requested_plan = str(args.get("plan_id") or args.get("plan_external_id") or "").strip()
    requested_system = _normalize_code_system(
        args.get("code_system") or args.get("reported_code_system")
    )
    requested_code = (
        canonical_catalog_code(
            requested_system,
            args.get("code") or args.get("reported_code"),
        )
        if requested_system
        else str(args.get("code") or args.get("reported_code") or "").strip()
    )
    explicit_source_scope = bool(
        str(args.get("source_key") or "").strip()
        or str(args.get("snapshot_id") or "").strip()
    )
    if args.get("q") or not requested_code or (not requested_plan and not explicit_source_scope):
        return None

    expand_providers = (
        _request_bool(args.get("include_providers"))
        or _is_ptg2_provider_filter_requested(args)
    )
    candidate_audit_npi = (
        _normalize_npi(args.get("npi"))
        if candidate_audit_access_from_args(args) is not None
        else None
    )
    requested_npi = _normalize_npi(args.get("npi"))
    geographic_filter_requested = _has_location_filter(
        args,
        include_npi=False,
    )
    direct_npi_filter_requested = bool(
        requested_npi is not None
        and not geographic_filter_requested
        and not _is_ptg2_provider_filter_requested(args)
    )
    location_filter_requested = bool(
        geographic_filter_requested
        or (requested_npi is not None and not direct_npi_filter_requested)
    )
    price_filter_requested = any(
        args.get(field)
        for field in (
            "pos",
            "place_of_service",
            "service_code",
            "modifier",
            "modifiers",
            "billing_code_modifier",
            "rate",
            "negotiated_rate",
        )
    )
    requested_order = str(args.get("order_by") or "").strip().lower()
    requested_direction = str(args.get("order") or "asc").strip().lower()
    distance_order_fields = {"", "distance", "distance_miles"}
    location_requires_exhaustive = bool(
        location_filter_requested
        and (
            not expand_providers
            or requested_order not in distance_order_fields
            or requested_direction == "desc"
        )
    )
    deferred_location_selection = bool(
        location_filter_requested and price_filter_requested
    )
    if direct_npi_filter_requested:
        rate_candidate_limit = (
            max(int(getattr(pagination, "offset", 0) or 0), 0)
            + max(int(getattr(pagination, "limit", 25) or 25), 1)
            + 1
        )
    else:
        rate_candidate_limit = _ptg2_manifest_rate_candidate_limit(
            args,
            pagination,
            expand_providers=expand_providers,
            location_filter_requested=location_filter_requested,
        )
    # One NPI can participate in more serving rows than the provider-page size.
    # All matching rows must reach the merge so no price occurrence becomes
    # unreachable behind a pre-merge candidate cap.
    serving_row_limit = _ptg2_manifest_serving_row_limit(
        args,
        rate_candidate_limit,
        expand_providers=expand_providers and not direct_npi_filter_requested,
    )

    def no_match_response() -> dict[str, Any]:
        """Build the exact empty response for the current serving query."""
        return _shape_ptg2_manifest_response(
            {
                "items": [],
                "pagination": {
                    "total": 0,
                    "limit": pagination.limit,
                    "offset": pagination.offset,
                    "page": (pagination.offset // pagination.limit) + 1
                    if pagination.limit
                    else 1,
                    "has_more": False,
                    "total_is_exact": True,
                    "total_lower_bound": 0,
                },
                "query": {
                    "plan_id": args.get("plan_id"),
                    "plan_external_id": args.get("plan_external_id"),
                    "plan_market_type": args.get("plan_market_type")
                    or args.get("market_type")
                    or None,
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
                    "provider_sex_code": args.get("provider_sex_code") or None,
                    "source": "ptg2_db",
                    "serving_table": None,
                    "include_providers": expand_providers,
                    "procedure_consolidation": "REPORTED_CODE",
                    "status": "no_match",
                },
            },
            args,
            database_evidence=serving_tables.database_evidence,
        )

    location_providers_by_set: dict[str, list[dict[str, Any]]] = {}
    is_location_selection_exhausted = False
    location_candidate_count = 0
    provider_set_keys: list[int] | None = None
    explicit_npi_scope = await _version_three_explicit_npi_graph_scope(
        session,
        serving_tables,
        args,
    )
    if explicit_npi_scope is not None:
        if not explicit_npi_scope.provider_set_keys:
            return no_match_response()
        provider_set_keys = list(explicit_npi_scope.provider_set_keys)
    if location_filter_requested and not deferred_location_selection:
        location_matches = await _ptg2_manifest_location_provider_matches(
            session,
            serving_tables,
            args,
            candidate_limit=rate_candidate_limit,
            plan_id=requested_plan,
            snapshot_id=snapshot_id,
            source_key=serving_tables.source_key or args.get("source_key"),
            provider_set_keys=provider_set_keys,
            explicit_npi_scope=explicit_npi_scope,
            require_exhaustive=location_requires_exhaustive,
        )
        if location_matches is None:
            return None
        provider_set_ids, location_providers_by_set = location_matches
        location_candidate_count = len(
            {
                int(provider["npi"])
                for providers in location_providers_by_set.values()
                for provider in providers
                if provider.get("npi") not in (None, "")
            }
        )
        is_location_selection_exhausted = (
            location_requires_exhaustive
            or location_candidate_count < rate_candidate_limit
        )
        if not provider_set_ids:
            return no_match_response()
        provider_set_key_by_id = await _provider_set_keys_for_ids(
            session,
            serving_tables,
            sorted(provider_set_ids),
        )
        if set(provider_set_key_by_id) != set(provider_set_ids):
            raise PTG2ManifestArtifactError(
                "PTG2 shared graph references an unknown provider set"
            )
        provider_set_keys = list(provider_set_key_by_id.values())
        if not provider_set_keys:
            return no_match_response()

    requested_code_values = _ptg2_reported_code_lookup_values(
        requested_system,
        requested_code,
    )
    scope_join_sql, code_filters, code_params, code_plan_order = _shared_v3_code_scope_sql(
        serving_tables,
        requested_plan=requested_plan,
        plan_market_type=args.get("plan_market_type") or args.get("market_type") or "",
    )
    code_filters.append("code_metadata.snapshot_key = :shared_snapshot_key")
    code_params.update(
        {
            "shared_snapshot_key": _required_shared_snapshot_key(serving_tables),
        }
    )
    _append_reported_code_system_filter(
        code_filters,
        code_params,
        column="code_metadata.reported_code_system",
        code_system=requested_system,
    )
    _append_reported_code_value_filter(
        code_filters,
        code_params,
        column="code_metadata.reported_code",
        param_name="reported_code",
        values=requested_code_values,
    )
    code_result = await session.execute(
        text(
            f"""
            SELECT code_metadata.code_key,
                   logical_scope.plan_id,
                   logical_scope.plan_market_type,
                   code_metadata.reported_code_system,
                   code_metadata.reported_code,
                   code_metadata.negotiation_arrangement,
                   code_metadata.billing_code_type_version,
                   code_metadata.source_name,
                   code_metadata.source_description,
                   code_metadata.rate_count
              FROM {_shared_v3_code_table()} code_metadata
              {scope_join_sql}
             WHERE {" AND ".join(code_filters)}
             ORDER BY {code_plan_order},
                      CASE WHEN code_metadata.reported_code = :reported_code THEN 0 ELSE 1 END,
                      code_metadata.code_key
            """
        ),
        code_params,
    )
    code_rows = [
        _canonical_code_metadata_row(code_record)
        for code_record in code_result
    ]
    if not code_rows:
        return None
    if not all(code_row.get("code_key") is not None for code_row in code_rows):
        raise PTG2ManifestArtifactError("PTG2 shared code dictionary contains an invalid key")

    total: int | None = None
    if not location_filter_requested and not direct_npi_filter_requested:
        total = sum(int(code_row.get("rate_count") or 0) for code_row in code_rows)
        if total <= 0:
            return None

    network_names = serving_tables.network_names or []
    exact_provider_selection: _ProviderExpansionSelection | None = None
    strict_cost_provider_expansion = (
        expand_providers
        and not location_filter_requested
        and not direct_npi_filter_requested
        and not price_filter_requested
        and str(args.get("order_by") or "total_allowed_amount").strip().lower()
        in _PTG2_COST_ORDER_FIELDS
    )
    if strict_cost_provider_expansion:
        exact_provider_selection = await _strict_cost_provider_expansion_selection(
            session,
            serving_tables,
            code_rows=code_rows,
            args=args,
            snapshot_id=snapshot_id,
            source_trace_set_hash=None,
            network_names=network_names,
            target_count=max(
                int(pagination.offset) + int(pagination.limit) + 1,
                1,
            ),
            descending=_is_cost_order_descending(args),
        )
        if exact_provider_selection is None:
            return None
        serving_rows = exact_provider_selection.row_data
    else:
        serving_rows = await _merge_manifest_code_variant_rows(
            session,
            serving_tables,
            code_rows=code_rows,
            provider_set_keys=provider_set_keys,
            source_trace_set_hash=None,
            network_names=network_names,
            limit=None if price_filter_requested else serving_row_limit,
            offset=(
                0
                if (
                    expand_providers
                    or price_filter_requested
                    or location_filter_requested
                    or direct_npi_filter_requested
                )
                else int(pagination.offset)
            ),
            descending=_is_cost_order_descending(args),
        )
    if serving_rows is None:
        return None
    is_serving_row_selection_exhausted = (
        serving_row_limit is None or len(serving_rows) < int(serving_row_limit)
    )

    response_items: list[dict[str, Any]] = []
    if not serving_rows:
        return None
    await _hydrate_provider_set_network_names(
        session,
        serving_tables,
        serving_rows,
    )
    source_provenance_by_key = (
        await _ptg2_source_provenance_for_rows(
            session,
            serving_tables,
            serving_rows,
        )
        if _include_ptg2_sources(args)
        else {}
    )
    price_key_by_set_id = {
        _ptg2_manifest_id(serving_row.get("price_set_global_id_128")): int(
            serving_row.get("price_key")
        )
        for serving_row in serving_rows
        if serving_row.get("price_key") is not None
        and _ptg2_manifest_id(serving_row.get("price_set_global_id_128"))
    }
    prices_by_price_set = await _prices_for_price_sets(
        session,
        serving_tables,
        [
            _ptg2_manifest_id(serving_row.get("price_set_global_id_128"))
            for serving_row in serving_rows
        ],
        price_key_by_set_id=price_key_by_set_id,
    )
    if price_filter_requested:
        prices_by_price_set = {
            price_set_id: _ptg2_manifest_filter_prices(prices, args)
            for price_set_id, prices in prices_by_price_set.items()
        }
        matching_price_set_ids = {
            price_set_id
            for price_set_id, prices in prices_by_price_set.items()
            if prices
        }
        serving_rows = [
            serving_row
            for serving_row in serving_rows
            if _ptg2_manifest_id(serving_row.get("price_set_global_id_128"))
            in matching_price_set_ids
        ]
        if not serving_rows:
            return no_match_response()
    if deferred_location_selection:
        filtered_provider_set_keys = {
            int(serving_row["_ptg_provider_set_key"])
            for serving_row in serving_rows
            if serving_row.get("_ptg_provider_set_key") is not None
        }
        if not filtered_provider_set_keys:
            raise PTG2ManifestArtifactError(
                "PTG2 v3 price-filtered location rows are missing provider-set keys"
            )
        location_matches = await _ptg2_manifest_location_provider_matches(
            session,
            serving_tables,
            args,
            candidate_limit=rate_candidate_limit,
            plan_id=requested_plan,
            snapshot_id=snapshot_id,
            source_key=serving_tables.source_key or args.get("source_key"),
            provider_set_keys=filtered_provider_set_keys,
            explicit_npi_scope=explicit_npi_scope,
            require_exhaustive=location_requires_exhaustive,
        )
        if location_matches is None:
            return None
        provider_set_ids, location_providers_by_set = location_matches
        if not provider_set_ids:
            return no_match_response()
        location_candidate_count = len(
            {
                int(provider["npi"])
                for providers in location_providers_by_set.values()
                for provider in providers
                if provider.get("npi") not in (None, "")
            }
        )
        is_location_selection_exhausted = (
            location_requires_exhaustive
            or location_candidate_count < rate_candidate_limit
        )
        serving_rows = [
            serving_row
            for serving_row in serving_rows
            if _ptg2_manifest_id(serving_row.get("provider_set_global_id_128"))
            in provider_set_ids
        ]
        if not serving_rows:
            raise PTG2ManifestArtifactError(
                "PTG2 v3 location projection did not retain a matching serving row"
            )
    retained_price_set_ids = {
        _ptg2_manifest_id(serving_row.get("price_set_global_id_128"))
        for serving_row in serving_rows
    }
    price_fields_by_price_set = {
        price_set_id: _price_response_fields(prices_by_price_set.get(price_set_id, []))
        for price_set_id in retained_price_set_ids
    }
    providers_by_set: dict[str, list[dict[str, Any]]] = {}
    if expand_providers:
        if exact_provider_selection is not None:
            providers_by_set = exact_provider_selection.providers_by_set
        elif location_filter_requested:
            providers_by_set = location_providers_by_set
        elif candidate_audit_npi is not None:
            providers_by_set = _candidate_audit_provider_rows_by_set(
                candidate_npi=candidate_audit_npi,
                serving_rows=serving_rows,
            )
        elif direct_npi_filter_requested and explicit_npi_scope is not None:
            exact_npi_provider_rows = await _exact_npi_provider_rows_by_set(
                session,
                serving_tables,
                npi=explicit_npi_scope.npi,
                serving_rows=serving_rows,
                args=args,
                snapshot_id=snapshot_id,
            )
            if exact_npi_provider_rows is None:
                return None
            providers_by_set = exact_npi_provider_rows
        else:
            provider_set_ids = [
                _ptg2_manifest_id(
                    serving_row.get("provider_set_global_id_128")
                )
                for serving_row in serving_rows
            ]
            provider_rows_by_set = await _provider_rows_for_sets(
                session,
                serving_tables,
                provider_set_ids,
                limit_per_set=None,
                args={**args, "snapshot_id": snapshot_id, "source_key": serving_tables.source_key or args.get("source_key")},
            )
            if provider_rows_by_set is None:
                return None
            providers_by_set = provider_rows_by_set
    procedure_details = await _procedure_details_for_rows(
        session,
        serving_rows,
    )
    for serving_row in serving_rows:
        if (
            not expand_providers
            and not price_filter_requested
            and not location_filter_requested
            and not direct_npi_filter_requested
            and len(response_items) >= int(pagination.limit)
        ):
            break
        reported_code = serving_row.get("reported_code")
        reported_system = serving_row.get("reported_code_system")
        provider_set_hash = _ptg2_manifest_id(
            serving_row.get("provider_set_global_id_128")
        )
        price_set_hash = _ptg2_manifest_id(
            serving_row.get("price_set_global_id_128")
        )
        rate_pack_hash = _ptg2_manifest_id(
            serving_row.get("serving_content_hash_128")
        )
        price_response_by_field = price_fields_by_price_set.get(
            price_set_hash,
            {"prices": [], "tic_prices": [], "price_summary": []},
        )
        procedure_detail = procedure_details.get(_catalog_key(reported_system, reported_code) or ("", ""), {})
        source_procedure_name = serving_row.get("source_procedure_name")
        source_procedure_description = serving_row.get(
            "source_procedure_description"
        )
        is_exact_source_mode = mode_value == PTG2_MODE_EXACT_SOURCE
        base_response_by_field = {
            "plan_id": serving_row.get("plan_id"),
            "plan_market_type": serving_row.get("plan_market_type"),
            "provider_ordinal": provider_set_hash,
            "provider_set_hash": provider_set_hash,
            "provider_set_hashes": [provider_set_hash] if provider_set_hash else [],
            "provider_name": "TiC provider set",
            "source_key": _logical_source_key(serving_tables, args),
            "source_artifact_key": int(serving_row["source_key"]),
            "snapshot_id": snapshot_id,
            "network_names": _coerce_str_list_payload(
                serving_row.get("network_names")
            ),
            "provider_count": serving_row.get("provider_count") or 0,
            "provider_set_count": 1 if provider_set_hash else 0,
            "procedure_code": reported_code,
            "hp_procedure_code": reported_code,
            "billing_code_type_version": serving_row.get(
                "billing_code_type_version"
            ),
            "procedure_name": (
                source_procedure_name
                if is_exact_source_mode
                else source_procedure_name or procedure_detail.get("procedure_name")
            ),
            "procedure_description": (
                source_procedure_description
                if is_exact_source_mode
                else source_procedure_description
                or procedure_detail.get("procedure_description")
            ),
            "source_procedure_name": source_procedure_name,
            "source_procedure_description": source_procedure_description,
            "catalog_procedure_name": procedure_detail.get("procedure_name"),
            "catalog_procedure_description": procedure_detail.get("procedure_description"),
            "service_code": reported_code,
            "service_code_system": reported_system or requested_system or "CPT",
            "reported_code": reported_code,
            "reported_code_system": reported_system,
            "negotiation_arrangement": serving_row.get(
                "negotiation_arrangement"
            ),
            "billing_code": reported_code,
            "billing_code_type": reported_system,
            **price_response_by_field,
            "price_set_hash": price_set_hash,
            "rate_pack_hash": rate_pack_hash,
            "_ptg_price_key": (
                int(serving_row["price_key"])
                if serving_row.get("price_key") is not None
                else None
            ),
            "source_trace": [],
            "confidence": {"network": "tic_rate_npi_tin", "location": "nppes_practice_location"},
        }
        source_provenance = source_provenance_by_key.get(
            int(serving_row["source_key"])
        )
        if source_provenance is not None:
            base_response_by_field.update(
                _item_source_provenance(source_provenance)
            )
        base_response_by_field["address_verification"] = (
            _address_verification_payload(base_response_by_field, {}, {})
        )
        _apply_address_display_policy(base_response_by_field, args)
        if not expand_providers:
            response_items.append(base_response_by_field)
            continue
        provider_rows = providers_by_set.get(
            _ptg2_manifest_id(
                serving_row.get("provider_set_global_id_128")
            ),
            [],
        )
        if (
            not provider_rows
            and not location_filter_requested
            and not _is_ptg2_provider_filter_requested(args)
        ):
            response_item_by_field = dict(base_response_by_field)
            response_item_by_field["npi"] = None
            response_item_by_field["provider_expansion_status"] = (
                "no_npi_members"
            )
            response_items.append(response_item_by_field)
            continue
        for provider in provider_rows:
            response_item_by_field = dict(base_response_by_field)
            address_payload = _coerce_json_payload(provider.get("address_payload"), {})
            response_item_by_field.update(
                {
                    "provider_ordinal": provider.get("npi") or provider_set_hash,
                    "npi": provider.get("npi"),
                    "provider_name": provider.get("provider_name")
                    or base_response_by_field["provider_name"],
                    "provider_sex_code": provider.get("provider_sex_code"),
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
            _add_location_phone_fields(
                response_item_by_field,
                provider,
                address_payload,
            )
            _promote_address_provenance_fields(
                response_item_by_field,
                address_payload,
            )
            response_item_by_field["address_verification"] = (
                _address_verification_payload(
                    response_item_by_field,
                    provider,
                    address_payload,
                )
            )
            _apply_address_display_policy(response_item_by_field, args)
            response_items.append(response_item_by_field)
    if not response_items:
        return None
    if expand_providers:
        response_items = _merge_ptg2_provider_rate_items(response_items)
    if exact_provider_selection is not None:
        selected_items: list[dict[str, Any]] = []
        materialized_keys: set[_ProviderExpansionKey] = set()
        for response_item_by_field in response_items:
            item_key = _provider_expansion_key(
                response_item_by_field,
                npi=(
                    int(response_item_by_field["npi"])
                    if response_item_by_field.get("npi") not in (None, "")
                    else None
                ),
            )
            rank = exact_provider_selection.rank_by_key.get(item_key)
            if rank is None:
                continue
            response_item_by_field["_ptg_provider_rank"] = rank
            selected_items.append(response_item_by_field)
            materialized_keys.add(item_key)
        if materialized_keys != set(exact_provider_selection.rank_by_key):
            raise PTG2ManifestArtifactError(
                "PTG2 strict V3 provider expansion failed to materialize its selected page"
            )
        response_items = selected_items
    response_items = _sort_ptg2_manifest_provider_items(
        response_items,
        args,
        location_filter_requested=location_filter_requested and expand_providers,
    )
    total_items = len(response_items)
    requested_page_end = max(int(pagination.offset), 0) + max(
        int(pagination.limit),
        0,
    )
    membership_filter_requested = bool(
        location_filter_requested or direct_npi_filter_requested
    )
    membership_selection_exhausted = bool(
        direct_npi_filter_requested or is_location_selection_exhausted
    )
    if (
        membership_filter_requested
        and not (
            membership_selection_exhausted
            and is_serving_row_selection_exhausted
        )
        and total_items <= requested_page_end
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 provider traversal could not prove the requested page boundary"
        )
    has_more_page_rows = False
    if expand_providers or price_filter_requested or membership_filter_requested:
        start = max(int(pagination.offset), 0)
        end = start + max(int(pagination.limit), 0)
        response_items = response_items[start:end]
        has_more_page_rows = end < total_items
    elif total is not None:
        has_more_page_rows = (
            int(pagination.offset) + len(response_items)
        ) < int(total)
    _hide_source_artifact_key_unless_requested(response_items, args)
    for response_item_by_field in response_items:
        response_item_by_field.pop("_ptg_price_key", None)
        response_item_by_field.pop("_ptg_provider_rank", None)
    return _shape_ptg2_manifest_response(
        {
            "items": response_items,
            "pagination": {
                "total": (
                    total_items
                    if expand_providers
                    or price_filter_requested
                    or membership_filter_requested
                    else total
                    if total is not None
                    else int(pagination.offset) + len(response_items)
                ),
                **(
                    {
                        "total_is_exact": exact_provider_selection.exhausted,
                        "total_lower_bound": exact_provider_selection.total_lower_bound,
                    }
                    if exact_provider_selection is not None
                    else (
                        {
                            "total_is_exact": (
                                membership_selection_exhausted
                                and is_serving_row_selection_exhausted
                            ),
                            "total_lower_bound": total_items,
                        }
                        if membership_filter_requested
                        else (
                            {
                                "total_is_exact": True,
                                "total_lower_bound": total_items,
                            }
                            if expand_providers or price_filter_requested
                            else {}
                        )
                    )
                ),
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": (pagination.offset // pagination.limit) + 1 if pagination.limit else 1,
                "has_more": has_more_page_rows,
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
                "provider_sex_code": args.get("provider_sex_code") or None,
                "source": "ptg2_db",
                "serving_table": None,
                "include_providers": expand_providers,
                "procedure_consolidation": "REPORTED_CODE",
            },
        },
        args,
        database_evidence=serving_tables.database_evidence,
    )


async def _ptg2_source_provenance_for_rows(
    session: Any,
    serving_tables: PTG2ServingTables,
    serving_rows: Iterable[Mapping[str, Any]],
) -> dict[int, dict[str, Any]]:
    source_keys: set[int] = set()
    for serving_row in serving_rows:
        source_key = serving_row.get("source_artifact_key")
        if source_key is None:
            source_key = serving_row.get("source_key")
        if isinstance(source_key, bool) or source_key is None:
            raise PTG2ManifestArtifactError(
                "PTG2 v3 serving row is missing exact source provenance"
            )
        try:
            source_keys.add(int(source_key))
        except (TypeError, ValueError) as exc:
            raise PTG2ManifestArtifactError(
                "PTG2 v3 serving row has an invalid source key"
            ) from exc
    try:
        return await fetch_snapshot_source_provenance(
            session,
            schema_name=PTG2_SCHEMA,
            logical_snapshot_id=_required_logical_snapshot_id(serving_tables),
            source_keys=source_keys,
            expected_source_count=_required_source_count(serving_tables),
        )
    except PTG2SharedBlockError as exc:
        raise PTG2ManifestArtifactError(str(exc)) from exc


def _logical_source_key(
    serving_tables: PTG2ServingTables,
    args: Mapping[str, Any],
) -> str | None:
    source_key = serving_tables.source_key or args.get("source_key")
    normalized = str(source_key or "").strip()
    return normalized or None


def _item_source_provenance(
    provenance: Mapping[str, Any],
) -> dict[str, Any]:
    source_key = provenance.get("source_key")
    if isinstance(source_key, bool) or source_key is None:
        raise PTG2ManifestArtifactError(
            "PTG2 v3 source provenance is missing its dense artifact key"
        )
    return {
        **{
            key: value
            for key, value in provenance.items()
            if key != "source_key"
        },
        "source_artifact_key": int(source_key),
    }


def _hide_source_artifact_key_unless_requested(
    items: Iterable[dict[str, Any]],
    args: Mapping[str, Any],
) -> None:
    if _include_ptg2_sources(dict(args)):
        return
    for item in items:
        item.pop("source_artifact_key", None)


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


def _compact_item_from_row(
    serving_row_by_field: dict[str, Any],
    args: dict[str, Any],
) -> dict[str, Any]:
    """Shape one compact database row into the public provider payload."""

    prices = _normalize_price_payload(serving_row_by_field.get("prices") or [])
    provider_set_hashes = _coerce_json_payload(
        serving_row_by_field.get("provider_set_hashes"),
        [],
    )
    provider_set_hash = serving_row_by_field.get("provider_set_hash") or (
        provider_set_hashes[0] if provider_set_hashes else None
    )
    specialties = _coerce_json_payload(
        serving_row_by_field.get("specialties"),
        [],
    )
    specializations = _coerce_json_payload(
        serving_row_by_field.get("specializations"),
        [],
    )
    classifications = _coerce_json_payload(
        serving_row_by_field.get("classifications"),
        [],
    )
    primary_specialty = serving_row_by_field.get("primary_specialty") or (
        specialties[0] if specialties else None
    )
    primary_specialization = serving_row_by_field.get(
        "primary_specialization"
    ) or (specializations[0] if specializations else None)
    address_payload = _coerce_json_payload(
        serving_row_by_field.get("address_payload"),
        {},
    )
    provider_item_by_field = {
        "npi": serving_row_by_field.get("npi") or args.get("npi"),
        "provider_ordinal": serving_row_by_field.get("provider_ordinal")
        or serving_row_by_field.get("npi")
        or provider_set_hash,
        "provider_name": serving_row_by_field.get("provider_name"),
        "plan_id": serving_row_by_field.get("plan_id"),
        "plan_market_type": serving_row_by_field.get("plan_market_type"),
        "state": serving_row_by_field.get("state"),
        "city": serving_row_by_field.get("city"),
        "zip5": serving_row_by_field.get("zip5"),
        "location_hash": serving_row_by_field.get("location_hash"),
        "location_source": serving_row_by_field.get("location_source"),
        "location_confidence_code": serving_row_by_field.get(
            "location_confidence_code"
        ),
        "address": address_payload,
        "taxonomy_codes": _coerce_json_payload(
            serving_row_by_field.get("taxonomy_codes"),
            [],
        ),
        "specialties": specialties,
        "primary_specialty": primary_specialty,
        "classification": classifications[0] if classifications else None,
        "classifications": classifications,
        "specialization": primary_specialization,
        "primary_specialization": primary_specialization,
        "specializations": specializations,
        "procedure_code": serving_row_by_field.get("procedure_code"),
        "hp_procedure_code": serving_row_by_field.get("procedure_code"),
        "procedure_name": (
            serving_row_by_field.get("procedure_name")
            if "procedure_name" in serving_row_by_field
            else serving_row_by_field.get("procedure_display_name")
        ),
        "procedure_description": serving_row_by_field.get(
            "procedure_description"
        ),
        "billing_code_type_version": serving_row_by_field.get(
            "billing_code_type_version"
        ),
        "source_procedure_name": serving_row_by_field.get(
            "source_procedure_name"
        ),
        "source_procedure_description": serving_row_by_field.get(
            "source_procedure_description"
        ),
        "catalog_procedure_name": serving_row_by_field.get(
            "catalog_procedure_name"
        ),
        "catalog_procedure_description": serving_row_by_field.get(
            "catalog_procedure_description"
        ),
        "service_code": serving_row_by_field.get("billing_code")
        or serving_row_by_field.get("reported_code"),
        "service_code_system": serving_row_by_field.get("billing_code_type")
        or serving_row_by_field.get("reported_code_system"),
        "reported_code": serving_row_by_field.get("reported_code"),
        "reported_code_system": serving_row_by_field.get(
            "reported_code_system"
        ),
        "negotiation_arrangement": serving_row_by_field.get(
            "negotiation_arrangement"
        ),
        "billing_code": serving_row_by_field.get("billing_code")
        or serving_row_by_field.get("reported_code"),
        "billing_code_type": serving_row_by_field.get("billing_code_type")
        or serving_row_by_field.get("reported_code_system"),
        "provider_set_hash": provider_set_hash,
        "provider_set_hashes": provider_set_hashes
        or ([provider_set_hash] if provider_set_hash else []),
        "provider_count": serving_row_by_field.get("provider_count"),
        "provider_set_count": serving_row_by_field.get("provider_set_count"),
        "price_set_hash": serving_row_by_field.get("price_set_hash"),
        "rate_pack_hash": serving_row_by_field.get("rate_pack_hash")
        or serving_row_by_field.get("serving_rate_id"),
        "source_key": serving_row_by_field.get("source_key")
        or args.get("source_key"),
        "source_artifact_key": serving_row_by_field.get("source_artifact_key"),
        "source_type": serving_row_by_field.get("source_type"),
        "identity_kind": serving_row_by_field.get("identity_kind"),
        "identity_sha256": serving_row_by_field.get("identity_sha256"),
        "raw_container_sha256": serving_row_by_field.get(
            "raw_container_sha256"
        ),
        "logical_json_sha256": serving_row_by_field.get(
            "logical_json_sha256"
        ),
        "logical_hash_deferred": serving_row_by_field.get(
            "logical_hash_deferred"
        ),
        "source_trace_set_hash": serving_row_by_field.get(
            "source_trace_set_hash"
        ),
        "snapshot_id": serving_row_by_field.get("snapshot_id")
        or args.get("snapshot_id"),
        "network_names": _coerce_str_list_payload(
            serving_row_by_field.get("network_names")
        ),
        **_price_response_fields(prices),
        "source_trace": _coerce_json_payload(
            _first_payload_value(
                serving_row_by_field.get("hydrated_source_trace"),
                serving_row_by_field.get("source_trace"),
            ),
            [],
        ),
        "confidence": serving_row_by_field.get("confidence")
        or {"network": "tic_rate_npi_tin"},
    }
    if serving_row_by_field.get("distance_miles") is not None:
        provider_item_by_field["distance_miles"] = serving_row_by_field.get(
            "distance_miles"
        )
    if serving_row_by_field.get("zip_match_type") is not None:
        provider_item_by_field["zip_match_type"] = serving_row_by_field.get(
            "zip_match_type"
        )
    if serving_row_by_field.get("anchor_zip5") is not None:
        provider_item_by_field["anchor_zip5"] = serving_row_by_field.get(
            "anchor_zip5"
        )
    if serving_row_by_field.get("zip_radius_miles") is not None:
        provider_item_by_field["zip_radius_miles"] = serving_row_by_field.get(
            "zip_radius_miles"
        )
    _add_location_phone_fields(
        provider_item_by_field,
        serving_row_by_field,
        address_payload,
    )
    _promote_address_provenance_fields(provider_item_by_field, address_payload)
    provider_item_by_field["address_verification"] = (
        _address_verification_payload(
            provider_item_by_field,
            serving_row_by_field,
            address_payload,
        )
    )
    _apply_address_display_policy(provider_item_by_field, args)
    compact_item_by_field = {
        field_name: field_value
        for field_name, field_value in provider_item_by_field.items()
        if field_value is not None
    }
    if normalize_ptg2_mode(args.get("mode")) == "exact_source":
        for field_name in (
            "billing_code_type_version",
            "procedure_name",
            "procedure_description",
        ):
            compact_item_by_field[field_name] = provider_item_by_field[field_name]
    return compact_item_by_field


async def search_ptg2_serving_table(
    session,
    snapshot_id: str,
    args: dict[str, Any],
    pagination,
    *,
    serving_tables: PTG2ServingTables | None = None,
) -> dict[str, Any] | None:
    """Serve a published snapshot through the strict shared V3 architecture."""

    mode_value = normalize_ptg2_mode(args.get("mode"))
    tables = serving_tables or await snapshot_serving_tables(
        session,
        snapshot_id,
        candidate_audit_access=candidate_audit_access_from_args(args),
    )
    _require_strict_shared_v3(tables)
    resolved_args_by_name = dict(args)
    if tables.source_key and not resolved_args_by_name.get("source_key"):
        resolved_args_by_name["source_key"] = tables.source_key
    return await _search_manifest_serving_table(
        session,
        snapshot_id,
        resolved_args_by_name,
        pagination,
        tables,
        mode_value,
    )

async def _search_ptg2_manifest_provider_procedures(
    session,
    npi: int,
    args: dict[str, Any],
    pagination,
    *,
    snapshot_id: str,
    serving_tables: PTG2ServingTables,
) -> dict[str, Any] | None:
    """Search strict shared V3 procedures and prices for one provider NPI."""

    _require_strict_shared_v3(serving_tables)
    provider_set_ids = await _provider_sets_for_npi(
        session,
        serving_tables,
        npi,
    )
    if not provider_set_ids:
        return _shape_ptg2_response(
            {
                "items": [],
                "pagination": {
                    "total": 0,
                    "limit": pagination.limit,
                    "offset": pagination.offset,
                    "page": (pagination.offset // pagination.limit) + 1
                    if pagination.limit
                    else 1,
                    "has_more": False,
                    "total_is_exact": True,
                    "total_lower_bound": 0,
                },
                "query": {
                    "npi": npi,
                    "plan_id": args.get("plan_id") or None,
                    "plan_external_id": args.get("plan_external_id") or None,
                    "plan_market_type": str(args.get("plan_market_type") or "").strip().lower()
                    or None,
                    "source_key": args.get("source_key") or None,
                    "snapshot_id": snapshot_id,
                    "mode": normalize_ptg2_mode(args.get("mode")),
                    "code": args.get("code") or args.get("reported_code") or None,
                    "code_system": args.get("code_system") or None,
                    "q": args.get("q") or args.get("service_name") or None,
                    "source": "ptg2_db",
                    "serving_table": None,
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
    code_context = await _resolve_ptg2_code_search_context(
        session,
        code=code_value,
        code_system=args.get("code_system"),
    )
    price_filter_params_by_name: dict[str, Any] = {}
    _, price_filter_query = _price_filter_clauses(
        args,
        price_filter_params_by_name,
    )
    has_price_filter = bool(price_filter_query)
    requested_limit = max(int(getattr(pagination, "limit", 25) or 25), 1)
    requested_offset = max(int(getattr(pagination, "offset", 0) or 0), 0)
    sentinel_limit = requested_limit + 1
    reverse_query = _VersionThreeReverseQuery(
        provider_set_ids=provider_set_ids,
        requested_plan=requested_plan,
        code_value=code_value,
        code_system=args.get("code_system"),
        q_text=q_text,
        code_context=code_context,
        source_trace_set_hash=None,
        network_names=serving_tables.network_names or [],
        limit=None if has_price_filter else sentinel_limit,
        offset=0 if has_price_filter else requested_offset,
        apply_window=not has_price_filter,
        plan_market_type=market_type,
    )
    if has_price_filter:
        filtered_selection = await _version_three_filtered_reverse_selection(
            session,
            serving_tables,
            reverse_query,
            args,
            offset=requested_offset,
            limit=sentinel_limit,
        )
        serving_rows = list(filtered_selection.rows)
        prices_by_price_set = dict(filtered_selection.prices_by_price_set)
        exact_total = filtered_selection.total_row_count
        observed_total_lower_bound = filtered_selection.matched_rows_seen
    else:
        reverse_selection = await _version_three_reverse_selection(
            session,
            serving_tables,
            reverse_query,
        )
        serving_rows = list(reverse_selection.rows)
        exact_total = reverse_selection.total_row_count
        observed_total_lower_bound = requested_offset + len(serving_rows)
        price_key_by_set_id = {
            _ptg2_manifest_id(
                serving_row.get("price_set_global_id_128")
            ): int(serving_row.get("price_key"))
            for serving_row in serving_rows
            if serving_row.get("price_key") is not None
            and _ptg2_manifest_id(
                serving_row.get("price_set_global_id_128")
            )
        }
        prices_by_price_set = await _prices_for_price_sets(
            session,
            serving_tables,
            [
                _ptg2_manifest_id(
                    serving_row.get("price_set_global_id_128")
                )
                for serving_row in serving_rows
            ],
            price_key_by_set_id=price_key_by_set_id,
        )
    await _hydrate_provider_set_network_names(
        session,
        serving_tables,
        serving_rows,
    )

    source_provenance_by_key = (
        await _ptg2_source_provenance_for_rows(
            session,
            serving_tables,
            serving_rows,
        )
        if _include_ptg2_sources(args)
        else {}
    )
    procedure_details = await _procedure_details_for_rows(
        session,
        serving_rows,
    )
    provider_context_rows = await _enriched_provider_rows_for_npis(
        session,
        npis=[npi],
        limit=1,
        plan_id=requested_plan or None,
        snapshot_id=snapshot_id,
        source_key=args.get("source_key") or None,
    )
    provider_context = provider_context_rows[0] if provider_context_rows else {"npi": npi}
    item_args_by_name = {
        **args,
        "snapshot_id": snapshot_id,
        "source_key": _logical_source_key(serving_tables, args),
    }

    response_items: list[dict[str, Any]] = []
    for serving_row in serving_rows:
        prices = prices_by_price_set.get(
            _ptg2_manifest_id(
                serving_row.get("price_set_global_id_128")
            ),
            [],
        )
        reported_code = serving_row.get("reported_code")
        reported_system = serving_row.get("reported_code_system")
        procedure_detail = procedure_details.get(_catalog_key(reported_system, reported_code) or ("", ""), {})
        source_provenance = source_provenance_by_key.get(
            int(serving_row["source_key"])
        )
        serving_row_with_trace_by_field = {
            **serving_row,
            **(
                _item_source_provenance(source_provenance)
                if source_provenance is not None
                else {
                    "source_artifact_key": int(serving_row["source_key"])
                }
            ),
        }
        response_items.append(
            _ptg2_manifest_provider_procedure_item(
                npi=npi,
                serving_data=serving_row_with_trace_by_field,
                prices=prices,
                procedure_detail=procedure_detail,
                provider_context=provider_context,
                args=item_args_by_name,
            )
        )
    has_more = len(response_items) > requested_limit
    response_items = response_items[:requested_limit]
    is_total_exact = exact_total is not None
    total_lower_bound = (
        int(exact_total)
        if is_total_exact
        else max(
            observed_total_lower_bound,
            requested_offset + len(response_items),
        )
    )
    total = int(exact_total) if is_total_exact else total_lower_bound

    _hide_source_artifact_key_unless_requested(response_items, args)

    return _shape_ptg2_response(
        {
            "items": response_items,
            "pagination": {
                "total": total,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": (pagination.offset // pagination.limit) + 1 if pagination.limit else 1,
                "has_more": has_more,
                "total_is_exact": is_total_exact,
                "total_lower_bound": total_lower_bound,
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
                "serving_table": None,
                "provider_reverse_index": True,
                "status": None if total else "no_match",
                **_ptg2_code_query_fields(code_context, args),
            },
        },
        args,
    )


async def _search_ptg2_provider_procedures_snapshot(
    session,
    npi: int,
    args: dict[str, Any],
    pagination,
    *,
    snapshot_id: str,
) -> dict[str, Any] | None:
    """Search one explicitly selected snapshot for a provider's procedures."""
    serving_tables = await snapshot_serving_tables(
        session,
        snapshot_id,
        candidate_audit_access=candidate_audit_access_from_args(args),
    )
    _require_strict_shared_v3(serving_tables)
    resolved_args_by_name = dict(args)
    if serving_tables.source_key and not resolved_args_by_name.get("source_key"):
        resolved_args_by_name["source_key"] = serving_tables.source_key
    return await _search_ptg2_manifest_provider_procedures(
        session,
        npi,
        resolved_args_by_name,
        pagination,
        snapshot_id=snapshot_id,
        serving_tables=serving_tables,
    )


def _provider_procedure_sort_key(item: Mapping[str, Any]) -> tuple[Any, ...]:
    """Order a merged reverse lookup consistently with single-snapshot readers."""
    return (
        str(item.get("reported_code_system") or item.get("billing_code_type") or ""),
        str(item.get("reported_code") or item.get("billing_code") or ""),
        -int(item.get("provider_count") or 0),
        str(item.get("provider_set_hash") or ""),
        str(item.get("price_set_hash") or ""),
        str(item.get("network") or ""),
    )


def _ptg2_multi_network_concurrency() -> int:
    try:
        configured = int(
            os.getenv(
                _PTG2_MULTI_NETWORK_CONCURRENCY_ENV,
                _PTG2_MULTI_NETWORK_CONCURRENCY_DEFAULT,
            )
        )
    except (TypeError, ValueError):
        configured = _PTG2_MULTI_NETWORK_CONCURRENCY_DEFAULT
    return max(1, min(configured, 32))


async def _gather_ptg2_network_reads(
    network_snapshots: list[tuple[str, str]],
    network_reader: Callable[[str, str], Awaitable[Any]],
) -> list[Any]:
    """Run immutable network reads concurrently without sharing a DB session."""
    semaphore = asyncio.Semaphore(_ptg2_multi_network_concurrency())

    async def run_bounded(source_key: str, snapshot_id: str):
        """Run one reader after obtaining a bounded concurrency slot."""
        async with semaphore:
            return await network_reader(source_key, snapshot_id)

    return list(
        await asyncio.gather(
            *(
                run_bounded(source_key, snapshot_id)
                for source_key, snapshot_id in network_snapshots
            )
        )
    )


async def _search_provider_procedures_network(
    source_key: str,
    snapshot_id: str,
    npi: int,
    args: dict[str, Any],
    pagination,
) -> tuple[str, str, dict[str, Any] | None]:
    async with sa_db.session() as network_session:
        response = await _search_ptg2_provider_procedures_snapshot(
            network_session,
            npi,
            args,
            pagination,
            snapshot_id=snapshot_id,
        )
    return source_key, snapshot_id, response


def _multi_provider_procedure_page(
    combined_items: list[dict[str, Any]],
    *,
    total_lower_bound: int,
    is_total_exact: bool,
    pagination: Any,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    combined_items.sort(key=_provider_procedure_sort_key)
    start = max(int(pagination.offset), 0)
    end = start + max(int(pagination.limit), 0)
    page_items = combined_items[start:end]
    has_more = len(combined_items) > end
    total_lower_bound = max(
        total_lower_bound,
        start + len(page_items) + int(has_more),
    )
    return page_items, {
        "total": total_lower_bound,
        "limit": pagination.limit,
        "offset": pagination.offset,
        "page": (pagination.offset // pagination.limit) + 1
        if pagination.limit
        else 1,
        "has_more": has_more,
        "total_is_exact": is_total_exact,
        "total_lower_bound": total_lower_bound,
    }


def _shape_multi_provider_procedure_response(
    network_responses: list[tuple[str, str, dict[str, Any] | None]],
    network_snapshots: list[tuple[str, str]],
    args: dict[str, Any],
    pagination,
) -> dict[str, Any] | None:
    """Merge per-network procedure responses into one paginated API response."""
    combined_items: list[dict[str, Any]] = []
    matched_networks: list[dict[str, str]] = []
    total_lower_bound = 0
    is_combined_total_exact = True
    base_query_by_field: dict[str, Any] | None = None
    for source_key, snapshot_id, network_response_by_field in network_responses:
        if not network_response_by_field:
            continue
        if base_query_by_field is None:
            base_query_by_field = dict(network_response_by_field.get("query") or {})
        network_pagination = network_response_by_field.get("pagination") or {}
        total_lower_bound += int(
            network_pagination.get("total_lower_bound")
            if network_pagination.get("total_lower_bound") is not None
            else network_pagination.get("total") or 0
        )
        is_combined_total_exact = (
            is_combined_total_exact
            and network_pagination.get("total_is_exact") is True
        )
        network_items = network_response_by_field.get("items") or []
        if network_items:
            matched_networks.append({"source_key": source_key, "snapshot_id": snapshot_id})
        for network_procedure in network_items:
            tagged_procedure_by_field = dict(network_procedure)
            tagged_procedure_by_field.setdefault("network", source_key)
            combined_items.append(tagged_procedure_by_field)
    if base_query_by_field is None:
        return None
    page_items, pagination_by_field = _multi_provider_procedure_page(
        combined_items,
        total_lower_bound=total_lower_bound,
        is_total_exact=is_combined_total_exact,
        pagination=pagination,
    )
    base_query_by_field.update(
        source_key=None,
        snapshot_id=None,
        snapshots=[snapshot_id for _, snapshot_id in network_snapshots],
        networks=matched_networks,
        combined=True,
    )
    return _shape_ptg2_response(
        {
            "items": page_items,
            "pagination": pagination_by_field,
            "query": base_query_by_field,
        },
        args,
    )


async def _search_multi_ptg2_provider_procedures(
    session,
    npi: int,
    network_snapshots: list[tuple[str, str]],
    args: dict[str, Any],
    pagination,
) -> dict[str, Any] | None:
    """Combine one provider's priced procedures across every plan network."""
    fetch_count = max(1, int(pagination.offset) + int(pagination.limit) + 1)
    sub_pagination = PaginationParams(page=1, limit=fetch_count, offset=0, source="page")

    async def read_network(source_key: str, snapshot_id: str):
        """Read one network through an independent database session."""
        return await _search_provider_procedures_network(
            source_key,
            snapshot_id,
            npi,
            args,
            sub_pagination,
        )

    network_responses = await _gather_ptg2_network_reads(
        network_snapshots,
        read_network,
    )
    return _shape_multi_provider_procedure_response(
        network_responses,
        network_snapshots,
        args,
        pagination,
    )


async def search_ptg2_provider_procedures(
    session,
    npi: int,
    args: dict[str, Any],
    pagination,
) -> dict[str, Any] | None:
    """Search one or every plan-network snapshot for a provider's procedures."""
    explicit_snapshot = str(args.get("snapshot_id") or "").strip()
    explicit_source = str(args.get("source_key") or "").strip()
    plan_scoped = bool(str(args.get("plan_id") or args.get("plan_external_id") or "").strip())
    if plan_scoped and not explicit_snapshot and not explicit_source:
        network_snapshots = await current_source_snapshot_ids_for_plan(session, args)
        if len(network_snapshots) > 1:
            return await _search_multi_ptg2_provider_procedures(
                session,
                npi,
                network_snapshots,
                args,
                pagination,
            )
        if len(network_snapshots) == 1:
            snapshot_id = network_snapshots[0][1]
        else:
            return None
    else:
        snapshot_id = await resolve_current_ptg2_snapshot_id(session, args)
        if not snapshot_id:
            return None
    return await _search_ptg2_provider_procedures_snapshot(
        session,
        npi,
        args,
        pagination,
        snapshot_id=snapshot_id,
    )


def _has_location_filter(
    args: dict[str, Any],
    *,
    include_npi: bool = True,
) -> bool:
    return bool(
        args.get("state")
        or args.get("city")
        or args.get("zip5")
        or args.get("zip")
        or (include_npi and args.get("npi"))
        or args.get("lat") is not None
        or args.get("long") is not None
        or args.get("radius_miles") is not None
    )


def _ptg2_manifest_plan_code_values(args: dict[str, Any]) -> tuple[str, str | None, str] | None:
    requested_plan = str(args.get("plan_id") or args.get("plan_external_id") or "").strip()
    requested_system = _normalize_code_system(args.get("code_system") or args.get("reported_code_system"))
    requested_code = (
        canonical_catalog_code(requested_system, args.get("code") or args.get("reported_code"))
        if requested_system
        else str(args.get("code") or args.get("reported_code") or "").strip()
    )
    if not requested_plan or not requested_code:
        return None
    return requested_plan, requested_system or None, requested_code


async def _has_ptg2_table_plan_code(
    session,
    serving_tables: PTG2ServingTables,
    *,
    requested_plan: str,
    requested_system: str | None,
    requested_code: str,
    plan_market_type: str = "",
) -> bool:
    _require_strict_shared_v3(serving_tables)
    scope_join_sql, code_filters, query_params_by_name, _ = _shared_v3_code_scope_sql(
        serving_tables,
        requested_plan=requested_plan,
        plan_market_type=plan_market_type,
    )
    code_filters.append("code_metadata.snapshot_key = :shared_snapshot_key")
    query_params_by_name["shared_snapshot_key"] = _required_shared_snapshot_key(serving_tables)
    _append_reported_code_value_filter(
        code_filters,
        query_params_by_name,
        column="code_metadata.reported_code",
        param_name="reported_code",
        values=_ptg2_reported_code_lookup_values(requested_system, requested_code),
    )
    _append_reported_code_system_filter(
        code_filters,
        query_params_by_name,
        column="code_metadata.reported_code_system",
        code_system=requested_system,
    )
    exists_result = await session.execute(
        text(
            f"""
            SELECT EXISTS (
                SELECT 1
                FROM {_shared_v3_code_table()} code_metadata
                {scope_join_sql}
                WHERE {" AND ".join(code_filters)}
                LIMIT 1
            )
            """
        ),
        query_params_by_name,
    )
    return bool(exists_result.scalar())


async def _has_snapshot_plan_code(
    session,
    snapshot_id: str,
    args: dict[str, Any],
    *,
    serving_tables: PTG2ServingTables | None = None,
) -> bool:
    """Fail closed when a bound shared snapshot cannot prove a requested route."""

    requested = _ptg2_manifest_plan_code_values(args)
    if requested is None:
        return True
    requested_plan, requested_system, requested_code = requested
    tables = serving_tables or await snapshot_serving_tables(
        session,
        snapshot_id,
        candidate_audit_access=candidate_audit_access_from_args(args),
    )
    return await _has_ptg2_table_plan_code(
        session,
        tables,
        requested_plan=requested_plan,
        requested_system=requested_system,
        requested_code=requested_code,
        plan_market_type=args.get("plan_market_type") or args.get("market_type") or "",
    )


async def _search_one_ptg2_snapshot(
    session,
    snapshot_id: str,
    args: dict[str, Any],
    pagination,
    *,
    serving_tables: PTG2ServingTables | None = None,
) -> dict[str, Any] | None:
    serving_tables = serving_tables or await snapshot_serving_tables(
        session,
        snapshot_id,
        candidate_audit_access=candidate_audit_access_from_args(args),
    )
    db_payload = await search_ptg2_serving_table(
        session,
        snapshot_id,
        args,
        pagination,
        serving_tables=serving_tables,
    )
    if db_payload is not None:
        db_payload = await _enrich_ptg2_code_details(session, db_payload, args)
        return _shape_ptg2_response(db_payload, args)
    return None


def _cache_network_serving_tables(serving_tables: PTG2ServingTables) -> None:
    """Remember immutable sealed metadata after its strict database validation."""

    snapshot_id = str(serving_tables.snapshot_id)
    _PTG2_NETWORK_SERVING_TABLES_CACHE[snapshot_id] = serving_tables
    _PTG2_NETWORK_SERVING_TABLES_CACHE.move_to_end(snapshot_id)
    while (
        len(_PTG2_NETWORK_SERVING_TABLES_CACHE)
        > _PTG2_NETWORK_SERVING_TABLES_CACHE_MAX_ENTRIES
    ):
        _PTG2_NETWORK_SERVING_TABLES_CACHE.popitem(last=False)


def _is_network_serving_tables_current(
    serving_tables: PTG2ServingTables,
    row_fields: Mapping[str, Any],
) -> bool:
    """Match a cached descriptor to its current published/sealed database chain."""

    try:
        snapshot_key = int(row_fields.get("snapshot_key"))
        layout_snapshot_key = int(row_fields.get("layout_snapshot_key"))
        layout_code_count = int(row_fields.get("layout_code_count"))
        layout_source_count = int(row_fields.get("layout_source_count"))
    except (TypeError, ValueError):
        return False
    audit_sample = serving_tables.audit_sample or {}
    source_set = serving_tables.source_set or {}
    return (
        snapshot_key == serving_tables.shared_snapshot_key
        and layout_snapshot_key == serving_tables.shared_snapshot_key
        and layout_code_count == serving_tables.code_count
        and layout_source_count == serving_tables.source_count
        and str(row_fields.get("snapshot_coverage_scope_id") or "")
        == serving_tables.coverage_scope_id
        and str(row_fields.get("layout_coverage_scope_id") or "")
        == serving_tables.coverage_scope_id
        and str(row_fields.get("attested_coverage_scope_id") or "")
        == serving_tables.coverage_scope_id
        and str(row_fields.get("snapshot_plan_id") or "").strip()
        == serving_tables.plan_id
        and str(row_fields.get("snapshot_plan_market_type") or "").strip()
        == serving_tables.plan_market_type
        and str(row_fields.get("attested_source_key") or "").strip()
        == serving_tables.source_key
        and str(row_fields.get("attested_audit_sample_digest") or "")
        == str(audit_sample.get("sample_digest") or "")
        and str(row_fields.get("attested_source_set_digest") or "")
        == str(source_set.get("raw_container_sha256_digest") or "")
    )


async def _is_cached_network_serving_tables_current(
    session,
    serving_tables_by_snapshot_id: Mapping[str, PTG2ServingTables],
) -> bool:
    """Revalidate cached immutable metadata in one live database round trip."""

    snapshot_ids = tuple(serving_tables_by_snapshot_id)
    if not snapshot_ids:
        return True
    validation_result = await session.execute(
        text(_PTG2_NETWORK_SERVING_TABLES_REVALIDATION_SQL),
        {
            "snapshot_ids": list(snapshot_ids),
            "storage_generation": PTG2_V3_SHARED_GENERATION,
            "attestation_contract": PTG2_CANDIDATE_ATTESTATION_CONTRACT,
        },
    )
    validated_snapshot_ids: set[str] = set()
    for validation_row in validation_result:
        row_fields = _row_mapping(validation_row)
        snapshot_id = str(row_fields.get("snapshot_id") or "")
        serving_tables = serving_tables_by_snapshot_id.get(snapshot_id)
        if serving_tables is None or not _is_network_serving_tables_current(
            serving_tables,
            row_fields,
        ):
            return False
        validated_snapshot_ids.add(snapshot_id)
    return validated_snapshot_ids == set(snapshot_ids)


async def _network_tables_by_snapshot_id(
    session,
    network_snapshots: Sequence[tuple[str, str]],
) -> dict[str, PTG2ServingTables]:
    """Load or cheaply revalidate every published network snapshot descriptor."""

    snapshot_ids = tuple(
        dict.fromkeys(str(snapshot_id) for _, snapshot_id in network_snapshots)
    )
    cached_tables_by_snapshot_id = {
        snapshot_id: _PTG2_NETWORK_SERVING_TABLES_CACHE[snapshot_id]
        for snapshot_id in snapshot_ids
        if snapshot_id in _PTG2_NETWORK_SERVING_TABLES_CACHE
    }
    cache_is_current = await _is_cached_network_serving_tables_current(
        session,
        cached_tables_by_snapshot_id,
    )
    if not cache_is_current:
        for snapshot_id in cached_tables_by_snapshot_id:
            _PTG2_NETWORK_SERVING_TABLES_CACHE.pop(snapshot_id, None)
        cached_tables_by_snapshot_id.clear()
    for snapshot_id in snapshot_ids:
        if snapshot_id in cached_tables_by_snapshot_id:
            _PTG2_NETWORK_SERVING_TABLES_CACHE.move_to_end(snapshot_id)
            continue
        serving_tables = await snapshot_serving_tables(session, snapshot_id)
        cached_tables_by_snapshot_id[snapshot_id] = serving_tables
        _cache_network_serving_tables(serving_tables)
    return cached_tables_by_snapshot_id


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

    combined_provider_items: list[dict[str, Any]] = []
    total = 0
    base_query_by_field: dict[str, Any] | None = None
    matched_networks: list[dict[str, str]] = []
    serving_tables_by_snapshot_id = (
        await _network_tables_by_snapshot_id(
        session,
        network_snapshots,
        )
    )

    network_responses = []
    for source_key, snapshot_id in network_snapshots:
        network_response = await _search_one_ptg2_snapshot(
            session,
            snapshot_id,
            args,
            sub_pagination,
            serving_tables=serving_tables_by_snapshot_id[snapshot_id],
        )
        network_responses.append((source_key, snapshot_id, network_response))
    for source_key, snapshot_id, network_response in network_responses:
        if not network_response:
            continue
        if base_query_by_field is None:
            base_query_by_field = dict(network_response.get("query") or {})
        page_info = network_response.get("pagination") or {}
        try:
            page_total = int(page_info.get("total") or 0)
        except (TypeError, ValueError):
            page_total = 0
        total += page_total
        network_items = network_response.get("items") or []
        if network_items:
            matched_networks.append({"source_key": source_key, "snapshot_id": snapshot_id})
        for network_item in network_items:
            tagged_provider_item_by_field = dict(network_item)
            if source_key:
                tagged_provider_item_by_field.setdefault("network", source_key)
            combined_provider_items.append(tagged_provider_item_by_field)

    if base_query_by_field is None:
        # No network produced a payload, so behave like
        # the single-snapshot path returning no match.
        return None

    combined_provider_items = _sort_ptg2_manifest_provider_items(
        combined_provider_items,
        args,
        location_filter_requested=_has_location_filter(args),
    )
    start = max(int(pagination.offset), 0)
    end = start + max(int(pagination.limit), 0)
    page_items = combined_provider_items[start:end]

    query_by_field = dict(base_query_by_field)
    query_by_field["source_key"] = None
    query_by_field["snapshot_id"] = None
    query_by_field["snapshots"] = [
        snapshot_id for _, snapshot_id in network_snapshots
    ]
    query_by_field["networks"] = matched_networks
    query_by_field["combined"] = True

    return {
        "items": page_items,
        "pagination": {
            "total": total,
            "limit": pagination.limit,
            "offset": pagination.offset,
            "page": (pagination.offset // pagination.limit) + 1 if pagination.limit else 1,
            "has_more": (int(pagination.offset) + len(page_items)) < total,
        },
        "query": query_by_field,
    }


async def search_current_ptg2_index(session, args: dict[str, Any], pagination) -> dict[str, Any] | None:
    """Resolve current plan snapshots and execute a single or multi-network query."""

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
        # A plan without a published plan/source pointer has no safe snapshot.
        # Falling through to the global pointer can return another plan's rates.
        return None
    snapshot_id = await resolve_current_ptg2_snapshot_id(session, args)
    if not snapshot_id:
        return None
    return await _search_one_ptg2_snapshot(session, snapshot_id, args, pagination)
