# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
# pylint: disable=too-many-lines

from __future__ import annotations

import math
import os
import time
from pathlib import Path
from typing import Any

from sqlalchemy import bindparam, text

from api.ptg2_code_filters import (
    EXTERNAL_PROCEDURE_CODE_SYSTEMS,
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
    _serving_table_available,
    _serving_table_candidates,
    _serving_table_name,
    snapshot_serving_table,
    snapshot_serving_tables,
)
from api.ptg2_types import PTG2ServingIndex, PTG2ServingTables
from api.ptg2_v3_artifacts import search_ptg2_v3_serving_snapshot
from process.ptg_parts.ptg2_v3_artifacts import lookup_global_sidecar_members, lookup_global_sidecar_members_many
from api.ptg2_serving_utils import (
    _normalize_zip5,
    _price_filter_clauses,
    _provider_payload,
    _row_mapping,
    _uuid_to_hex,
)

PTG2_MODE_EXACT_SOURCE = "exact_source"
PTG2_MODE_PRODUCT_SEARCH = "product_search"
PTG2_SCHEMA = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
PTG2_WARM_P95_MAX_MS = max(float(os.getenv("HLTHPRT_PTG2_WARM_P95_MAX_MS", "50")), 1.0)
PTG2_JSON_FALLBACK_ENV = "HLTHPRT_PTG2_ENABLE_JSON_FALLBACK"
PTG2_SERVING_TABLE_ENV = "HLTHPRT_PTG2_SERVING_TABLE"
PTG2_FAST_COMPACT_COUNTS_ENV = "HLTHPRT_PTG2_FAST_COMPACT_COUNTS"
_PTG2_V3_SIDECAR_CACHE: dict[tuple[str, str, str], tuple[str, ...]] = {}


def normalize_ptg2_mode(value: str | None) -> str:
    mode = str(value or PTG2_MODE_PRODUCT_SEARCH).strip().lower()
    if mode not in {PTG2_MODE_EXACT_SOURCE, PTG2_MODE_PRODUCT_SEARCH}:
        raise ValueError("mode must be exact_source or product_search")
    return mode


def _inferred_provider_taxonomy_rule(args: dict[str, Any]) -> InferredProviderTaxonomyRule | None:
    requested_system = _normalize_code_system(args.get("code_system"))
    requested_code = _normalize_code(args.get("code"))
    if requested_system not in EXTERNAL_PROCEDURE_CODE_SYSTEMS or not requested_code or not requested_code.isdigit():
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


def _ptg2_v3_storage_enabled(serving_tables: PTG2ServingTables) -> bool:
    return (serving_tables.storage or "").strip().lower() == "v3_manifest_snapshot"


def _ptg2_v3_id_array_cast(serving_tables: PTG2ServingTables) -> str:
    return "uuid[]" if serving_tables.uses_uuid_ids else "char(32)[]"


def _ptg2_v3_id(value: Any) -> str:
    return _uuid_to_hex(value)


def _ptg2_v3_ids(values: list[Any] | tuple[Any, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(hex_value for value in values if (hex_value := _ptg2_v3_id(value))))


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


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
    q_text = str(args.get("q") or "").strip().lower()
    zip_text = _normalize_zip5(args.get("zip5"))
    state_text = str(args.get("state") or "").strip().upper()
    city_text = str(args.get("city") or "").strip().lower()
    geo_lat = _optional_float(args.get("lat"))
    geo_long = _optional_float(args.get("long"))
    geo_radius_miles = _optional_float(args.get("radius_miles"))
    coordinate_filter_requested = geo_lat is not None and geo_long is not None and geo_radius_miles is not None
    specialty_text = str(args.get("specialty") or "").strip().lower()
    taxonomy_code = _normalize_taxonomy_code(args.get("taxonomy_code"))
    taxonomy_classification = str(args.get("taxonomy_classification") or "").strip()
    taxonomy_specialization = str(args.get("taxonomy_specialization") or "").strip()
    taxonomy_section = str(args.get("taxonomy_section") or "").strip()
    provider_npi = _normalize_npi(args.get("npi"))
    if provider_npi is not None:
        params["provider_npi"] = provider_npi
    expand_providers = _request_bool(args.get("include_providers"))
    geo_filters: list[str] = []
    if zip_text:
        geo_filters.append("pl.zip5 = :zip5")
        params["zip5"] = zip_text
    if state_text:
        geo_filters.append("UPPER(COALESCE(pl.state, '')) = :state")
        params["state"] = state_text
    if city_text:
        geo_filters.append("pl.city_norm LIKE :city_like")
        params["city_like"] = f"%{city_text}%"
        params["city_exact"] = city_text.upper()
    if coordinate_filter_requested:
        radius_miles = max(float(geo_radius_miles or 0.0), 0.0)
        cos_lat = abs(math.cos(math.radians(float(geo_lat)))) or 1e-6
        params.update(
            {
                "geo_lat": float(geo_lat),
                "geo_long": float(geo_long),
                "geo_radius_miles": radius_miles,
                "geo_min_lat": float(geo_lat) - radius_miles / 69.0,
                "geo_max_lat": float(geo_lat) + radius_miles / 69.0,
                "geo_min_long": float(geo_long) - radius_miles / (69.0 * cos_lat),
                "geo_max_long": float(geo_long) + radius_miles / (69.0 * cos_lat),
            }
        )
    taxonomy_filters: list[str] = []
    if taxonomy_code:
        taxonomy_filters.append("nt.healthcare_provider_taxonomy_code = :taxonomy_code")
        params["taxonomy_code"] = taxonomy_code
    if taxonomy_classification:
        taxonomy_filters.append("nucc.classification = :taxonomy_classification")
        params["taxonomy_classification"] = taxonomy_classification
    if taxonomy_specialization:
        taxonomy_filters.append("nucc.specialization = :taxonomy_specialization")
        params["taxonomy_specialization"] = taxonomy_specialization
    if taxonomy_section:
        taxonomy_filters.append("nucc.section = :taxonomy_section")
        params["taxonomy_section"] = taxonomy_section
    if specialty_text:
        taxonomy_filters.append(
            """
            (
                LOWER(COALESCE(nucc.display_name, '')) LIKE :specialty_like
             OR LOWER(COALESCE(nucc.classification, '')) LIKE :specialty_like
             OR LOWER(COALESCE(nucc.specialization, '')) LIKE :specialty_like
             OR LOWER(COALESCE(nucc.section, '')) LIKE :specialty_like
            )
            """
        )
        params["specialty_like"] = f"%{specialty_text}%"
    schema = PTG2_SCHEMA
    inferred_taxonomy_sql = ""
    inferred_provider_taxonomy_sql = ""
    if not taxonomy_filters and provider_npi is None:
        inferred_taxonomy_sql = _inferred_provider_taxonomy_code_sql(
            args,
            nt_alias="nt",
            schema=schema,
            params=params,
            param_prefix="inferred_taxonomy",
        )
        inferred_provider_taxonomy_sql = _inferred_provider_taxonomy_code_sql(
            args,
            nt_alias="nt_filter",
            schema=schema,
            params=params,
            param_prefix="inferred_provider_taxonomy",
        )
        if inferred_taxonomy_sql:
            taxonomy_filters.append(inferred_taxonomy_sql)
    inferred_taxonomy_filter_only = bool(inferred_taxonomy_sql) and taxonomy_filters == [inferred_taxonomy_sql]
    q_filter = ""
    if q_text:
        q_filter = """
          AND (
                LOWER(COALESCE(proc.name, '')) LIKE :q_like
             OR LOWER(COALESCE(proc.description, '')) LIKE :q_like
             OR LOWER(COALESCE(proc.billing_code, '')) LIKE :q_like
             OR LOWER(COALESCE(r.reported_code, '')) LIKE :q_like
          )
        """
        params["q_like"] = f"%{q_text}%"
    where_sql = " AND ".join(_qualify_compact_filters(filters))
    procedure_table = serving_tables.procedure_table or f"{schema}.ptg2_procedure"
    use_normalized_price_tables = bool(
        serving_tables.price_atom_table
        and serving_tables.price_set_entry_table
        and serving_tables.price_code_set_table
    )
    provider_set_component_table = serving_tables.provider_set_component_table
    provider_set_entry_table = serving_tables.provider_set_entry_table
    provider_entry_component_table = serving_tables.provider_entry_component_table
    provider_group_member_table = serving_tables.provider_group_member_table or f"{schema}.ptg2_provider_group_member"
    provider_group_location_table = serving_tables.provider_group_location_table
    use_direct_provider_tables = bool(provider_set_component_table and serving_tables.provider_group_member_table)
    use_provider_entry_tables = bool(
        provider_set_entry_table
        and provider_entry_component_table
        and serving_tables.provider_group_member_table
    )
    if not use_normalized_price_tables or not (use_direct_provider_tables or use_provider_entry_tables):
        return None
    procedure_join_sql = (
        f"""
        JOIN LATERAL (
            SELECT proc.*
              FROM {procedure_table} proc
             WHERE proc.procedure_hash = r.procedure_hash
             LIMIT 1
        ) proc ON TRUE
        """
        if serving_tables.procedure_table
        else f"JOIN {procedure_table} proc ON proc.procedure_hash = r.procedure_hash"
    )
    price_join_sql = f"""
        {_normalized_price_join_sql(serving_tables)}
    """
    if use_direct_provider_tables:
        provider_join_sql = f"""
            JOIN {provider_set_component_table} psc ON psc.provider_set_hash = r.provider_set_hash
            JOIN {provider_group_member_table} pgm ON pgm.provider_group_hash = psc.provider_group_hash
        """
    else:
        provider_join_sql = f"""
            JOIN {provider_set_entry_table} pse ON pse.provider_set_hash = r.provider_set_hash
            JOIN {provider_entry_component_table} pec ON pec.provider_entry_hash = pse.provider_entry_hash
            JOIN {provider_group_member_table} pgm ON pgm.provider_group_hash = pec.provider_group_hash
        """
    coordinate_sql = ""
    if coordinate_filter_requested:
        coordinate_sql = """
                addr_alias.lat IS NOT NULL
            AND addr_alias.long IS NOT NULL
            AND addr_alias.lat::float8 BETWEEN :geo_min_lat AND :geo_max_lat
            AND addr_alias.long::float8 BETWEEN :geo_min_long AND :geo_max_long
            AND (
                69.0 * sqrt(
                    power(addr_alias.lat::float8 - :geo_lat, 2)
                  + power(
                        (addr_alias.long::float8 - :geo_long)
                        * cos(radians((addr_alias.lat::float8 + :geo_lat) / 2.0)),
                        2
                    )
                )
            ) <= :geo_radius_miles
        """
    coordinate_geography_sql = ""
    if coordinate_filter_requested:
        coordinate_geography_sql = """
            ST_DWithin(
                Geography(ST_MakePoint(addr_alias.long::float8, addr_alias.lat::float8)),
                Geography(ST_MakePoint(:geo_long, :geo_lat)),
                :geo_radius_miles * 1609.34
            )
        """
    provider_filter_requested = bool(provider_npi or geo_filters or coordinate_filter_requested or taxonomy_filters)
    provider_filter_sql = ""
    if provider_filter_requested:
        provider_geo_sql = " AND ".join(
            filter_sql
            for filter_sql in (
                "LEFT(COALESCE(addr_filter.postal_code, ''), 5) = :zip5" if zip_text else "",
                "addr_filter.state_name = :state" if state_text else "",
                "addr_filter.city_name = :city_exact" if city_text else "",
                coordinate_sql.replace("addr_alias", "addr_filter") if coordinate_filter_requested else "",
            )
            if filter_sql
        )
        provider_taxonomy_sql = " AND ".join(
            filter_sql
            for filter_sql in (
                "nt_filter.healthcare_provider_taxonomy_code = :taxonomy_code" if taxonomy_code else "",
                "nucc_filter.classification = :taxonomy_classification" if taxonomy_classification else "",
                "nucc_filter.specialization = :taxonomy_specialization" if taxonomy_specialization else "",
                "nucc_filter.section = :taxonomy_section" if taxonomy_section else "",
                """
                (
                    LOWER(COALESCE(nucc_filter.display_name, '')) LIKE :specialty_like
                 OR LOWER(COALESCE(nucc_filter.classification, '')) LIKE :specialty_like
                 OR LOWER(COALESCE(nucc_filter.specialization, '')) LIKE :specialty_like
                 OR LOWER(COALESCE(nucc_filter.section, '')) LIKE :specialty_like
                )
                """ if specialty_text else "",
                inferred_provider_taxonomy_sql,
            )
            if filter_sql
        )
        provider_npi_where_sql = "AND pgm_filter.npi = :provider_npi" if provider_npi else ""
        if use_direct_provider_tables:
            provider_npi_join_sql = f"""
                FROM LATERAL (
                    SELECT pgm_filter.npi
                      FROM {provider_set_component_table} psc_filter
                      JOIN {provider_group_member_table} pgm_filter
                        ON pgm_filter.provider_group_hash = psc_filter.provider_group_hash
                     WHERE psc_filter.provider_set_hash = r.provider_set_hash
                       {provider_npi_where_sql}
                     OFFSET 0
                ) provider_filter_npi
            """
        else:
            provider_npi_join_sql = f"""
                FROM LATERAL (
                    SELECT pgm_filter.npi
                      FROM {provider_set_entry_table} pse_filter
                      JOIN {provider_entry_component_table} pec_filter
                        ON pec_filter.provider_entry_hash = pse_filter.provider_entry_hash
                      JOIN {provider_group_member_table} pgm_filter
                        ON pgm_filter.provider_group_hash = pec_filter.provider_group_hash
                     WHERE pse_filter.provider_set_hash = r.provider_set_hash
                       {provider_npi_where_sql}
                     OFFSET 0
                ) provider_filter_npi
            """
        provider_geo_match_sql = (
            f"""
                JOIN LATERAL (
                    SELECT 1
                      FROM {schema}.npi_address addr_filter
                     WHERE addr_filter.npi = provider_filter_npi.npi
                       AND addr_filter.type IN ('primary', 'secondary')
                       AND {provider_geo_sql}
                     LIMIT 1
                ) addr_match ON TRUE
            """
            if geo_filters or coordinate_filter_requested
            else ""
        )
        provider_taxonomy_match_sql = (
            f"""
                JOIN LATERAL (
                    SELECT 1
                      FROM {schema}.npi_taxonomy nt_filter
                      {"JOIN " + schema + ".nucc_taxonomy nucc_filter ON nucc_filter.code = nt_filter.healthcare_provider_taxonomy_code" if not inferred_taxonomy_filter_only else ""}
                     WHERE nt_filter.npi = provider_filter_npi.npi
                       AND {provider_taxonomy_sql}
                     LIMIT 1
                ) taxonomy_match ON TRUE
            """
            if taxonomy_filters
            else ""
        )
        provider_filter_sql = f"""
          AND EXISTS (
                SELECT 1
                  {provider_npi_join_sql}
                  {provider_geo_match_sql}
                  {provider_taxonomy_match_sql}
                 LIMIT 1
          )
        """
    compact_price_jsonb = _normalized_price_json_sql(json_type="jsonb")
    compact_price_json = _normalized_price_json_sql(json_type="json")
    has_provider_filters = expand_providers
    if has_provider_filters:
        params["candidate_rate_limit"] = max(int(getattr(pagination, "limit", 25) or 25) * 8, 64)
        expansion_geo_sql = " AND ".join(
            filter_sql
            for filter_sql in (
                "LEFT(COALESCE(addr.postal_code, ''), 5) = :zip5" if zip_text else "",
                "addr.state_name = :state" if state_text else "",
                "addr.city_name = :city_exact" if city_text else "",
                coordinate_sql.replace("addr_alias", "addr") if coordinate_filter_requested else "",
            )
            if filter_sql
        )
        taxonomy_sql = " AND ".join(taxonomy_filters)
        location_join_sql = (
            f"""
            JOIN {schema}.npi_address addr
              ON addr.npi = pgm.npi
             AND addr.type IN ('primary', 'secondary')
            """
            if geo_filters or coordinate_filter_requested
            else f"""
            LEFT JOIN {schema}.npi_address addr
              ON addr.npi = pgm.npi
             AND addr.type IN ('primary', 'secondary')
            """
        )
        taxonomy_select_sql = """
                    ARRAY[]::varchar[] AS taxonomy_codes,
                    ARRAY[]::varchar[] AS specialties,
        """
        if taxonomy_filters:
            taxonomy_join_sql = f"""
                JOIN {schema}.npi_taxonomy nt ON nt.npi = pgm.npi
                JOIN {schema}.nucc_taxonomy nucc ON nucc.code = nt.healthcare_provider_taxonomy_code
            """
            taxonomy_select_sql = """
                    array_agg(DISTINCT nt.healthcare_provider_taxonomy_code ORDER BY nt.healthcare_provider_taxonomy_code) FILTER (WHERE nt.healthcare_provider_taxonomy_code IS NOT NULL) AS taxonomy_codes,
                    array_agg(DISTINCT nucc.display_name ORDER BY nucc.display_name) FILTER (WHERE nucc.display_name IS NOT NULL) AS specialties,
            """
        else:
            taxonomy_join_sql = ""
        if use_direct_provider_tables:
            candidate_set_groups_sql = f"""
                        SELECT DISTINCT
                            psc.provider_set_hash,
                            psc.provider_group_hash
                          FROM rate_candidates r
                          JOIN {provider_set_component_table} psc
                            ON psc.provider_set_hash = r.provider_set_hash
            """
        else:
            candidate_set_groups_sql = f"""
                        SELECT DISTINCT
                            pse.provider_set_hash,
                            pec.provider_group_hash
                          FROM rate_candidates r
                          JOIN {provider_set_entry_table} pse
                            ON pse.provider_set_hash = r.provider_set_hash
                          JOIN {provider_entry_component_table} pec
                            ON pec.provider_entry_hash = pse.provider_entry_hash
            """
        if use_direct_provider_tables and not (geo_filters or coordinate_filter_requested or taxonomy_filters or q_text or provider_npi):
            row_result = await session.execute(
                text(
                    f"""
                    WITH selected_rate AS MATERIALIZED (
                        SELECT r.*
                        FROM {table_name} r
                        WHERE {where_sql}
                        ORDER BY r.provider_count DESC NULLS LAST, r.serving_rate_id
                        LIMIT 1
                    ),
                    selected_providers AS MATERIALIZED (
                        SELECT DISTINCT pgm.npi
                        FROM selected_rate r
                        JOIN {provider_set_component_table} psc
                          ON psc.provider_set_hash = r.provider_set_hash
                        JOIN {provider_group_member_table} pgm
                          ON pgm.provider_group_hash = psc.provider_group_hash
                        ORDER BY pgm.npi
                        LIMIT :limit OFFSET :offset
                    )
                    SELECT
                        sp.npi,
                        CONCAT('npi_address:', addr.npi, ':', addr.type, ':', addr.checksum) AS location_hash,
                        addr.state_name AS state,
                        addr.city_name AS city,
                        LEFT(COALESCE(addr.postal_code, ''), 5) AS zip5,
                        'npi_address' AS location_source,
                        'npi_address' AS location_confidence_code,
                        json_build_object(
                            'first_line', addr.first_line,
                            'second_line', addr.second_line,
                            'city', addr.city_name,
                            'state', addr.state_name,
                            'postal_code', addr.postal_code,
                            'country_code', addr.country_code,
                            'lat', addr.lat,
                            'long', addr.long
                        )::text AS address_payload,
                        ARRAY[]::varchar[] AS taxonomy_codes,
                        ARRAY[]::varchar[] AS specialties,
                        COALESCE(
                            NULLIF(BTRIM(n.provider_organization_name), ''),
                            NULLIF(BTRIM(CONCAT_WS(' ', n.provider_first_name, n.provider_middle_name, n.provider_last_name)), ''),
                            'TiC provider'
                        ) AS provider_name,
                        r.procedure_code,
                        r.reported_code_system,
                        r.reported_code,
                        proc.billing_code AS billing_code,
                        proc.billing_code_type AS billing_code_type,
                        COALESCE(proc.name, proc.description, proc.billing_code) AS procedure_display_name,
                        proc.name AS procedure_name,
                        proc.description AS procedure_description,
                        ARRAY[r.provider_set_hash] AS provider_set_hashes,
                        1::int AS rate_count,
                        COALESCE(price_payload.prices, '[]'::jsonb) AS prices,
                        CAST('[]' AS jsonb) AS source_trace
                    FROM selected_rate r
                    JOIN selected_providers sp ON TRUE
                    {procedure_join_sql}
                    LEFT JOIN LATERAL (
                        SELECT addr.*
                        FROM {schema}.npi_address addr
                        WHERE addr.npi = sp.npi
                          AND addr.type IN ('primary', 'secondary')
                        ORDER BY CASE WHEN addr.type = 'primary' THEN 0 ELSE 1 END, addr.checksum
                        LIMIT 1
                    ) addr ON TRUE
                    LEFT JOIN {schema}.npi n ON n.npi = sp.npi
                    {price_join_sql}
                    """
                ),
                params,
            )
            items = []
            for row in row_result:
                data = _row_mapping(row)
                items.append(
                    {
                        "npi": data.get("npi"),
                        "provider_name": data.get("provider_name"),
                        "location_hash": data.get("location_hash"),
                        "state": data.get("state"),
                        "city": data.get("city"),
                        "zip5": data.get("zip5"),
                        "location_source": data.get("location_source"),
                        "address": _coerce_json_payload(data.get("address_payload"), {}),
                        "taxonomy_codes": data.get("taxonomy_codes") or [],
                        "specialties": data.get("specialties") or [],
                        "provider_set_hashes": data.get("provider_set_hashes") or [],
                        "provider_count": 1,
                        "rate_count": data.get("rate_count") or 0,
                        "procedure_code": data.get("procedure_code") if data.get("procedure_code") is not None else data.get("reported_code"),
                        "hp_procedure_code": data.get("procedure_code"),
                        "procedure_name": data.get("procedure_display_name") or data.get("procedure_name"),
                        "procedure_description": data.get("procedure_description"),
                        "service_code": data.get("billing_code"),
                        "service_code_system": data.get("billing_code_type") or data.get("reported_code_system") or "CPT",
                        "reported_code": data.get("reported_code") or data.get("billing_code"),
                        "reported_code_system": data.get("reported_code_system") or data.get("billing_code_type"),
                        "billing_code": data.get("billing_code"),
                        "billing_code_type": data.get("billing_code_type"),
                        **_price_response_fields(data.get("prices")),
                        "source_trace": _coerce_json_payload(data.get("source_trace"), []),
                        "confidence": {"network": "tic_rate_npi_tin", "location": "nppes_practice_location"},
                    }
                )
            if not items:
                return None
            total = int(pagination.offset) + len(items)
            return {
                "items": items,
                "pagination": {
                    "total": total,
                    "limit": pagination.limit,
                    "offset": pagination.offset,
                    "page": (pagination.offset // pagination.limit) + 1 if pagination.limit else 1,
                },
                "query": {
                    "plan_id": args.get("plan_id"),
                    "plan_external_id": args.get("plan_external_id"),
                    "plan_market_type": args.get("plan_market_type") or None,
                    "source_key": args.get("source_key") or None,
                    "snapshot_id": snapshot_id,
                    "mode": mode_value,
                    "code": args.get("code") or None,
                    "code_system": args.get("code_system") or None,
                    "q": args.get("q") or None,
                    "state": state_text or None,
                    "city": city_text or None,
                    "zip5": zip_text,
                    "lat": None,
                    "long": None,
                    "radius_miles": None,
                    "specialty": specialty_text or None,
                    "taxonomy_code": taxonomy_code,
                    "taxonomy_classification": taxonomy_classification or None,
                    "taxonomy_specialization": taxonomy_specialization or None,
                    "taxonomy_section": taxonomy_section or None,
                    "npi": provider_npi,
                    "include_providers": expand_providers,
                    "source": "ptg2_db_compact",
                    "serving_table": table_name,
                    "result_granularity": "provider",
                    "procedure_consolidation": "HP_PROCEDURE_CODE",
                },
            }
        if use_direct_provider_tables and provider_group_location_table and (geo_filters or coordinate_filter_requested) and not q_text:
            params["location_rate_candidate_limit"] = max(
                int(getattr(pagination, "limit", 25) or 25) * 320,
                2560,
            )
            params["provider_match_limit"] = max(
                int(getattr(pagination, "offset", 0) or 0) + int(getattr(pagination, "limit", 25) or 25) * 10,
                64,
            )
            location_filter_sql = " AND ".join(
                filter_sql
                for filter_sql in (
                    "loc.zip5 = :zip5" if zip_text else "",
                    "loc.state_name = :state" if state_text else "",
                    "loc.city_name = :city_exact" if city_text else "",
                    coordinate_geography_sql.replace("addr_alias", "loc") if coordinate_filter_requested else "",
                )
                if filter_sql
            ) or "TRUE"
            location_taxonomy_where_sql = ""
            location_taxonomy_select_sql = """
                ARRAY[]::varchar[] AS taxonomy_codes,
                ARRAY[]::varchar[] AS specialties,
            """
            if taxonomy_filters:
                if taxonomy_code and not (
                    taxonomy_classification
                    or taxonomy_specialization
                    or taxonomy_section
                    or specialty_text
                    or inferred_taxonomy_filter_only
                ):
                    location_taxonomy_where_sql = f"""
                        AND CASE
                            WHEN loc.taxonomy_array IS NOT NULL THEN
                                loc.taxonomy_array && ARRAY[
                                    (SELECT int_code FROM {schema}.nucc_taxonomy WHERE code = :taxonomy_code)
                                ]::integer[]
                            ELSE EXISTS (
                                SELECT 1
                                  FROM {schema}.npi_taxonomy nt
                                 WHERE nt.npi = loc.npi
                                   AND nt.healthcare_provider_taxonomy_code = :taxonomy_code
                                 LIMIT 1
                            )
                        END
                    """
                elif inferred_taxonomy_filter_only:
                    location_taxonomy_where_sql = f"""
                        AND EXISTS (
                            SELECT 1
                              FROM {schema}.npi_taxonomy nt
                             WHERE nt.npi = loc.npi
                               AND {taxonomy_sql}
                             LIMIT 1
                        )
                    """
                else:
                    location_taxonomy_where_sql = f"""
                        AND EXISTS (
                            SELECT 1
                              FROM {schema}.npi_taxonomy nt
                              JOIN {schema}.nucc_taxonomy nucc
                                ON nucc.code = nt.healthcare_provider_taxonomy_code
                             WHERE nt.npi = loc.npi
                               AND {taxonomy_sql}
                             LIMIT 1
                        )
                    """
                location_taxonomy_select_sql = """
                    array_agg(DISTINCT nt.healthcare_provider_taxonomy_code ORDER BY nt.healthcare_provider_taxonomy_code)
                        FILTER (WHERE nt.healthcare_provider_taxonomy_code IS NOT NULL) AS taxonomy_codes,
                    array_agg(DISTINCT nucc.display_name ORDER BY nucc.display_name)
                        FILTER (WHERE nucc.display_name IS NOT NULL) AS specialties,
                """
            filtered_location_cte_sql = f"""
                    filtered_locations AS MATERIALIZED (
                        SELECT
                            loc.provider_group_hash,
                            loc.npi,
                            loc.zip5,
                            loc.state_name,
                            loc.city_name,
                            loc.lat,
                            loc.long,
                            loc.address_type,
                            loc.address_checksum,
                            loc.first_line,
                            loc.second_line,
                            loc.postal_code,
                            loc.country_code
                          FROM {provider_group_location_table} loc
                         WHERE {location_filter_sql}
                           AND loc.npi IS NOT NULL
                           {location_taxonomy_where_sql}
                    ),
            """
            location_row_result = await session.execute(
                text(
                    f"""
                    WITH rate_candidates AS MATERIALIZED (
                        SELECT r.*
                          FROM {table_name} r
                         WHERE {where_sql}
                         ORDER BY r.provider_count DESC NULLS LAST, r.serving_rate_id
                         LIMIT :location_rate_candidate_limit
                    ),
                    {filtered_location_cte_sql}
                    candidate_matches AS MATERIALIZED (
                        SELECT
                            provider_match.npi,
                            provider_match.provider_group_hash,
                            provider_match.zip5,
                            provider_match.state_name,
                            provider_match.city_name,
                            provider_match.lat,
                            provider_match.long,
                            provider_match.address_type,
                            provider_match.address_checksum,
                            provider_match.first_line,
                            provider_match.second_line,
                            provider_match.postal_code,
                            provider_match.country_code,
                            r.serving_rate_id,
                            r.provider_set_hash,
                            r.provider_count,
                            r.procedure_hash,
                            r.procedure_code,
                            r.reported_code_system,
                            r.reported_code,
                            r.price_set_hash,
                            r.source_trace_set_hash
                          FROM rate_candidates r
                          JOIN LATERAL (
                              SELECT DISTINCT
                                  loc.npi,
                                  loc.provider_group_hash,
                                  loc.zip5,
                                  loc.state_name,
                                  loc.city_name,
                                  loc.lat,
                                  loc.long,
                                  CASE WHEN loc.address_type = 'primary' THEN 0 ELSE 1 END AS address_rank,
                                  loc.address_type,
                                  loc.address_checksum,
                                  loc.first_line,
                                  loc.second_line,
                                  loc.postal_code,
                                  loc.country_code
                                FROM (
                                    SELECT psc.provider_group_hash
                                      FROM {provider_set_component_table} psc
                                     WHERE psc.provider_set_hash = r.provider_set_hash
                                     OFFSET 0
                                ) psc
                                JOIN filtered_locations loc
                                  ON loc.provider_group_hash = psc.provider_group_hash
                                 {"AND loc.npi = :provider_npi" if provider_npi is not None else ""}
                               ORDER BY loc.npi, address_rank, loc.address_checksum
                               LIMIT :provider_match_limit
                          ) provider_match ON TRUE
                         ORDER BY r.provider_count DESC NULLS LAST, r.serving_rate_id, provider_match.npi
                         LIMIT :provider_match_limit
                    ),
                    matched_rates AS MATERIALIZED (
                        SELECT DISTINCT ON (cm.npi)
                            cm.*
                          FROM candidate_matches cm
                         ORDER BY cm.npi, cm.provider_count DESC NULLS LAST, cm.serving_rate_id
                    )
                    SELECT
                        r.npi,
                        CONCAT('npi_address:', r.npi, ':', r.address_type, ':', r.address_checksum) AS location_hash,
                        r.state_name AS state,
                        r.city_name AS city,
                        r.zip5,
                        'npi_address' AS location_source,
                        'npi_address' AS location_confidence_code,
                        json_build_object(
                            'first_line', r.first_line,
                            'second_line', r.second_line,
                            'city', r.city_name,
                            'state', r.state_name,
                            'postal_code', r.postal_code,
                            'country_code', r.country_code,
                            'lat', r.lat,
                            'long', r.long
                        )::text AS address_payload,
                        {location_taxonomy_select_sql}
                        COALESCE(
                            NULLIF(BTRIM(n.provider_organization_name), ''),
                            NULLIF(BTRIM(CONCAT_WS(' ', n.provider_first_name, n.provider_middle_name, n.provider_last_name)), ''),
                            'TiC provider'
                        ) AS provider_name,
                        r.procedure_code,
                        r.reported_code_system,
                        r.reported_code,
                        proc.billing_code AS billing_code,
                        proc.billing_code_type AS billing_code_type,
                        COALESCE(proc.name, proc.description, proc.billing_code) AS procedure_display_name,
                        proc.name AS procedure_name,
                        proc.description AS procedure_description,
                        ARRAY[r.provider_set_hash] AS provider_set_hashes,
                        1::int AS rate_count,
                        COALESCE(price_payload.prices, '[]'::jsonb) AS prices,
                        CAST('[]' AS jsonb) AS source_trace
                    FROM matched_rates r
                    {procedure_join_sql}
                    LEFT JOIN {schema}.npi n ON n.npi = r.npi
                    LEFT JOIN {schema}.npi_taxonomy nt ON nt.npi = r.npi
                    LEFT JOIN {schema}.nucc_taxonomy nucc
                      ON nucc.code = nt.healthcare_provider_taxonomy_code
                    {price_join_sql}
                    GROUP BY
                        r.npi,
                        r.provider_group_hash,
                        r.zip5,
                        r.state_name,
                        r.city_name,
                        r.lat,
                        r.long,
                        r.address_type,
                        r.address_checksum,
                        r.first_line,
                        r.second_line,
                        r.postal_code,
                        r.country_code,
                        r.provider_set_hash,
                        r.provider_count,
                        r.procedure_code,
                        r.reported_code_system,
                        r.reported_code,
                        proc.billing_code,
                        proc.billing_code_type,
                        proc.name,
                        proc.description,
                        price_payload.prices,
                        provider_name
                    ORDER BY provider_name, r.npi
                    LIMIT :limit OFFSET :offset
                    """
                ),
                params,
            )
            items = []
            for row in location_row_result:
                data = _row_mapping(row)
                confidence = {
                    "network": "tic_rate_npi_tin",
                    "location": data.get("location_confidence_code") or "ptg2_provider_group_location",
                }
                items.append(
                    {
                        "npi": data.get("npi"),
                        "provider_name": data.get("provider_name"),
                        "location_hash": data.get("location_hash"),
                        "state": data.get("state"),
                        "city": data.get("city"),
                        "zip5": data.get("zip5"),
                        "location_source": data.get("location_source"),
                        "address": _coerce_json_payload(data.get("address_payload"), {}),
                        "taxonomy_codes": data.get("taxonomy_codes") or [],
                        "specialties": data.get("specialties") or [],
                        "provider_set_hashes": data.get("provider_set_hashes") or [],
                        "provider_count": 1,
                        "rate_count": data.get("rate_count") or 0,
                        "procedure_code": data.get("procedure_code") if data.get("procedure_code") is not None else data.get("reported_code"),
                        "hp_procedure_code": data.get("procedure_code"),
                        "procedure_name": data.get("procedure_display_name") or data.get("procedure_name"),
                        "procedure_description": data.get("procedure_description"),
                        "service_code": data.get("billing_code"),
                        "service_code_system": data.get("billing_code_type") or data.get("reported_code_system") or "CPT",
                        "reported_code": data.get("reported_code") or data.get("billing_code"),
                        "reported_code_system": data.get("reported_code_system") or data.get("billing_code_type"),
                        "billing_code": data.get("billing_code"),
                        "billing_code_type": data.get("billing_code_type"),
                        **_price_response_fields(data.get("prices")),
                        "source_trace": _coerce_json_payload(data.get("source_trace"), []),
                        "confidence": confidence,
                    }
                )
            if not items:
                return None
            total = int(pagination.offset) + len(items)
            return {
                "items": items,
                "pagination": {
                    "total": total,
                    "limit": pagination.limit,
                    "offset": pagination.offset,
                    "page": (pagination.offset // pagination.limit) + 1 if pagination.limit else 1,
                },
                "query": {
                    "plan_id": args.get("plan_id"),
                    "plan_external_id": args.get("plan_external_id"),
                    "plan_market_type": args.get("plan_market_type") or None,
                    "source_key": args.get("source_key") or None,
                    "snapshot_id": snapshot_id,
                    "mode": mode_value,
                    "code": args.get("code") or None,
                    "code_system": args.get("code_system") or None,
                    "q": args.get("q") or None,
                    "state": state_text or None,
                    "city": city_text or None,
                    "zip5": zip_text,
                    "lat": geo_lat,
                    "long": geo_long,
                    "radius_miles": geo_radius_miles,
                    "specialty": specialty_text or None,
                    "taxonomy_code": taxonomy_code,
                    "taxonomy_classification": taxonomy_classification or None,
                    "taxonomy_specialization": taxonomy_specialization or None,
                    "taxonomy_section": taxonomy_section or None,
                    "npi": provider_npi,
                    "include_providers": expand_providers,
                    "source": "ptg2_db_compact",
                    "serving_table": table_name,
                    "result_granularity": "provider",
                    "procedure_consolidation": "HP_PROCEDURE_CODE",
                },
            }
        if coordinate_filter_requested and not q_text:
            geo_member_filters = [
                "addr.type IN ('primary', 'secondary')",
                coordinate_geography_sql.replace("addr_alias", "addr"),
            ]
            if zip_text:
                geo_member_filters.append("LEFT(COALESCE(addr.postal_code, ''), 5) = :zip5")
            if state_text:
                geo_member_filters.append("addr.state_name = :state")
            if city_text:
                geo_member_filters.append("addr.city_name = :city_exact")
            geo_member_taxonomy_join_sql = ""
            if taxonomy_code:
                geo_member_filters.append(
                    f"""
                    addr.taxonomy_array && ARRAY[
                        (SELECT int_code FROM {schema}.nucc_taxonomy WHERE code = :taxonomy_code)
                    ]::integer[]
                    """
                )
            if taxonomy_classification or taxonomy_specialization or taxonomy_section or specialty_text:
                geo_member_taxonomy_join_sql = f"""
                    JOIN {schema}.npi_taxonomy nt_geo ON nt_geo.npi = addr.npi
                    JOIN {schema}.nucc_taxonomy nucc_geo
                      ON nucc_geo.code = nt_geo.healthcare_provider_taxonomy_code
                """
                if taxonomy_classification:
                    geo_member_filters.append("nucc_geo.classification = :taxonomy_classification")
                if taxonomy_specialization:
                    geo_member_filters.append("nucc_geo.specialization = :taxonomy_specialization")
                if taxonomy_section:
                    geo_member_filters.append("nucc_geo.section = :taxonomy_section")
                if specialty_text:
                    geo_member_filters.append(
                        """
                        (
                            LOWER(COALESCE(nucc_geo.display_name, '')) LIKE :specialty_like
                         OR LOWER(COALESCE(nucc_geo.classification, '')) LIKE :specialty_like
                         OR LOWER(COALESCE(nucc_geo.specialization, '')) LIKE :specialty_like
                         OR LOWER(COALESCE(nucc_geo.section, '')) LIKE :specialty_like
                        )
                        """
                    )
            if provider_npi is not None:
                geo_member_filters.append("addr.npi = :provider_npi")
            geo_member_where_sql = "\n              AND ".join(geo_member_filters)
            coordinate_row_result = await session.execute(
                text(
                    f"""
                    WITH geo_members AS MATERIALIZED (
                        SELECT DISTINCT
                            addr.npi,
                            addr.type,
                            addr.checksum,
                            addr.first_line,
                            addr.second_line,
                            addr.city_name,
                            addr.state_name,
                            addr.postal_code,
                            addr.country_code,
                            addr.lat,
                            addr.long,
                            LEFT(COALESCE(addr.postal_code, ''), 5) AS zip5,
                            pgm_geo.provider_group_hash
                          FROM {schema}.npi_address addr
                          {geo_member_taxonomy_join_sql}
                          JOIN {provider_group_member_table} pgm_geo
                            ON pgm_geo.npi = addr.npi
                         WHERE {geo_member_where_sql}
                    ),
                    rate_candidates AS MATERIALIZED (
                        SELECT
                            r.serving_rate_id,
                            r.provider_set_hash,
                            r.procedure_hash,
                            r.procedure_code,
                            r.reported_code_system,
                            r.reported_code,
                            r.price_set_hash,
                            r.source_trace_set_hash
                          FROM {table_name} r
                         WHERE {where_sql}
                    ),
                    candidate_set_groups AS MATERIALIZED (
{candidate_set_groups_sql}
                    ),
                    top_providers AS MATERIALIZED (
                        SELECT
                            gm.npi,
                            gm.type,
                            gm.checksum,
                            gm.first_line,
                            gm.second_line,
                            gm.city_name,
                            gm.state_name,
                            gm.postal_code,
                            gm.country_code,
                            gm.lat,
                            gm.long,
                            gm.zip5,
                            COUNT(DISTINCT r.serving_rate_id)::int AS rate_count
                        FROM geo_members gm
                        JOIN candidate_set_groups csg
                          ON csg.provider_group_hash = gm.provider_group_hash
                        JOIN rate_candidates r
                          ON r.provider_set_hash = csg.provider_set_hash
                        GROUP BY
                            gm.npi,
                            gm.type,
                            gm.checksum,
                            gm.first_line,
                            gm.second_line,
                            gm.city_name,
                            gm.state_name,
                            gm.postal_code,
                            gm.country_code,
                            gm.lat,
                            gm.long,
                            gm.zip5
                        ORDER BY rate_count DESC, gm.npi
                        LIMIT :limit OFFSET :offset
                    )
                    SELECT
                        tp.npi,
                        CONCAT('npi_address:', tp.npi, ':', tp.type, ':', tp.checksum) AS location_hash,
                        tp.state_name AS state,
                        tp.city_name AS city,
                        tp.zip5,
                        'npi_address' AS location_source,
                        'npi_address' AS location_confidence_code,
                        json_build_object(
                            'first_line', tp.first_line,
                            'second_line', tp.second_line,
                            'city', tp.city_name,
                            'state', tp.state_name,
                            'postal_code', tp.postal_code,
                            'country_code', tp.country_code,
                            'lat', tp.lat,
                            'long', tp.long
                        )::text AS address_payload,
                        array_agg(DISTINCT nt.healthcare_provider_taxonomy_code ORDER BY nt.healthcare_provider_taxonomy_code)
                            FILTER (WHERE nt.healthcare_provider_taxonomy_code IS NOT NULL) AS taxonomy_codes,
                        array_agg(DISTINCT nucc.display_name ORDER BY nucc.display_name)
                            FILTER (WHERE nucc.display_name IS NOT NULL) AS specialties,
                        COALESCE(
                            NULLIF(BTRIM(n.provider_organization_name), ''),
                            NULLIF(BTRIM(CONCAT_WS(' ', n.provider_first_name, n.provider_middle_name, n.provider_last_name)), ''),
                            'TiC provider'
                        ) AS provider_name,
                        r.procedure_code,
                        r.reported_code_system,
                        r.reported_code,
                        proc.billing_code AS billing_code,
                        proc.billing_code_type AS billing_code_type,
                        COALESCE(proc.name, proc.description, proc.billing_code) AS procedure_display_name,
                        proc.name AS procedure_name,
                        proc.description AS procedure_description,
                        array_agg(DISTINCT r.provider_set_hash ORDER BY r.provider_set_hash) AS provider_set_hashes,
                        tp.rate_count,
                        COALESCE(jsonb_agg(DISTINCT price_item.price_item) FILTER (WHERE price_item.price_item IS NOT NULL), '[]'::jsonb) AS prices,
                        COALESCE(jsonb_agg(DISTINCT trace_item.trace_item) FILTER (WHERE trace_item.trace_item IS NOT NULL), '[]'::jsonb) AS source_trace
                    FROM top_providers tp
                    JOIN geo_members gm
                      ON gm.npi = tp.npi
                     AND gm.checksum = tp.checksum
                    JOIN candidate_set_groups csg
                      ON csg.provider_group_hash = gm.provider_group_hash
                    JOIN rate_candidates r
                      ON r.provider_set_hash = csg.provider_set_hash
                    {procedure_join_sql}
                    LEFT JOIN {schema}.npi n ON n.npi = tp.npi
                    LEFT JOIN {schema}.npi_taxonomy nt ON nt.npi = tp.npi
                    LEFT JOIN {schema}.nucc_taxonomy nucc
                      ON nucc.code = nt.healthcare_provider_taxonomy_code
                    {price_join_sql}
                    LEFT JOIN LATERAL jsonb_array_elements({compact_price_jsonb}) AS price_item(price_item) ON TRUE
                    LEFT JOIN {schema}.ptg2_source_trace_set sts ON sts.source_trace_set_hash = r.source_trace_set_hash
                    LEFT JOIN LATERAL unnest(COALESCE(sts.source_trace_hashes, ARRAY[]::varchar[])) AS sth(source_trace_hash) ON TRUE
                    LEFT JOIN {schema}.ptg2_source_trace st ON st.source_trace_hash = sth.source_trace_hash
                    LEFT JOIN LATERAL (
                        SELECT CASE WHEN st.source_trace_hash IS NULL THEN NULL ELSE jsonb_build_object(
                            'url', st.original_url,
                            'canonical_url', st.canonical_url,
                            'statement', 'Published negotiated rate from Transparency in Coverage source file.'
                        ) END AS trace_item
                    ) trace_item ON TRUE
                    GROUP BY
                        tp.npi,
                        tp.type,
                        tp.checksum,
                        tp.first_line,
                        tp.second_line,
                        tp.city_name,
                        tp.state_name,
                        tp.postal_code,
                        tp.country_code,
                        tp.lat,
                        tp.long,
                        tp.zip5,
                        tp.rate_count,
                        provider_name,
                        r.procedure_code,
                        r.reported_code_system,
                        r.reported_code,
                        proc.billing_code,
                        proc.billing_code_type,
                        proc.name,
                        proc.description
                    ORDER BY tp.rate_count DESC, provider_name, tp.npi
                    """
                ),
                params,
            )
            items = []
            for row in coordinate_row_result:
                data = _row_mapping(row)
                confidence = {
                    "network": "tic_rate_npi_tin",
                    "location": data.get("location_confidence_code") or "nppes_practice_location",
                }
                items.append(
                    {
                        "npi": data.get("npi"),
                        "provider_name": data.get("provider_name"),
                        "location_hash": data.get("location_hash"),
                        "state": data.get("state"),
                        "city": data.get("city"),
                        "zip5": data.get("zip5"),
                        "location_source": data.get("location_source"),
                        "address": _coerce_json_payload(data.get("address_payload"), {}),
                        "taxonomy_codes": data.get("taxonomy_codes") or [],
                        "specialties": data.get("specialties") or [],
                        "provider_set_hashes": data.get("provider_set_hashes") or [],
                        "provider_count": 1,
                        "rate_count": data.get("rate_count") or 0,
                        "procedure_code": data.get("procedure_code") if data.get("procedure_code") is not None else data.get("reported_code"),
                        "hp_procedure_code": data.get("procedure_code"),
                        "procedure_name": data.get("procedure_display_name") or data.get("procedure_name"),
                        "procedure_description": data.get("procedure_description"),
                        "service_code": data.get("billing_code"),
                        "service_code_system": data.get("billing_code_type") or data.get("reported_code_system") or "CPT",
                        "reported_code": data.get("reported_code") or data.get("billing_code"),
                        "reported_code_system": data.get("reported_code_system") or data.get("billing_code_type"),
                        "billing_code": data.get("billing_code"),
                        "billing_code_type": data.get("billing_code_type"),
                        **_price_response_fields(data.get("prices")),
                        "source_trace": _coerce_json_payload(data.get("source_trace"), []),
                        "confidence": confidence,
                    }
                )
            if not items:
                return None
            total = int(pagination.offset) + len(items)
            return {
                "items": items,
                "pagination": {
                    "total": total,
                    "limit": pagination.limit,
                    "offset": pagination.offset,
                    "page": (pagination.offset // pagination.limit) + 1 if pagination.limit else 1,
                },
                "query": {
                    "plan_id": args.get("plan_id"),
                    "plan_external_id": args.get("plan_external_id"),
                    "plan_market_type": args.get("plan_market_type") or None,
                    "source_key": args.get("source_key") or None,
                    "snapshot_id": snapshot_id,
                    "mode": mode_value,
                    "code": args.get("code") or None,
                    "code_system": args.get("code_system") or None,
                    "q": args.get("q") or None,
                    "state": state_text or None,
                    "city": city_text or None,
                    "zip5": zip_text,
                    "lat": geo_lat,
                    "long": geo_long,
                    "radius_miles": geo_radius_miles,
                    "specialty": specialty_text or None,
                    "taxonomy_code": taxonomy_code,
                    "taxonomy_classification": taxonomy_classification or None,
                    "taxonomy_specialization": taxonomy_specialization or None,
                    "taxonomy_section": taxonomy_section or None,
                    "npi": provider_npi,
                    "include_providers": expand_providers,
                    "source": "ptg2_db_compact",
                    "serving_table": table_name,
                    "result_granularity": "provider",
                    "procedure_consolidation": "HP_PROCEDURE_CODE",
                },
            }
        total = None
        if not _env_bool(PTG2_FAST_COMPACT_COUNTS_ENV, True):
            count_result = await session.execute(
                text(
                    f"""
                    SELECT COUNT(*)
                    FROM (
                        SELECT DISTINCT pgm.npi, addr.type, addr.checksum
                        FROM {table_name} r
                        {procedure_join_sql}
                        {provider_join_sql}
                        {location_join_sql}
                        {taxonomy_join_sql}
                        WHERE {where_sql}
                          {"AND " + expansion_geo_sql if expansion_geo_sql else ""}
                          {"AND " + taxonomy_sql if taxonomy_filters else ""}
                          {q_filter}
                    ) matched
                    """
                ),
                params,
            )
            total = int(count_result.scalar() or 0)
            if total <= 0:
                return None
        row_result = await session.execute(
            text(
                f"""
                WITH rate_candidates AS MATERIALIZED (
                    SELECT r.*
                    FROM {table_name} r
                    {procedure_join_sql}
                    WHERE {where_sql}
                    {q_filter}
                    {provider_filter_sql}
                    ORDER BY r.provider_count DESC NULLS LAST, r.serving_rate_id
                    LIMIT :candidate_rate_limit
                )
                    SELECT
                        pgm.npi,
                    CONCAT('npi_address:', addr.npi, ':', addr.type, ':', addr.checksum) AS location_hash,
                    addr.state_name AS state,
                    addr.city_name AS city,
                    LEFT(COALESCE(addr.postal_code, ''), 5) AS zip5,
                    'npi_address' AS location_source,
                    'npi_address' AS location_confidence_code,
                    MAX(json_build_object(
                        'first_line', addr.first_line,
                        'second_line', addr.second_line,
                        'city', addr.city_name,
                        'state', addr.state_name,
                        'postal_code', addr.postal_code,
                        'country_code', addr.country_code,
                        'lat', addr.lat,
                        'long', addr.long
                    )::text) AS address_payload,
                    {taxonomy_select_sql}
                    COALESCE(
                        NULLIF(BTRIM(n.provider_organization_name), ''),
                        NULLIF(BTRIM(CONCAT_WS(' ', n.provider_first_name, n.provider_middle_name, n.provider_last_name)), ''),
                        'TiC provider'
                    ) AS provider_name,
                    r.procedure_code,
                    r.reported_code_system,
                    r.reported_code,
                    proc.billing_code AS billing_code,
                    proc.billing_code_type AS billing_code_type,
                    COALESCE(proc.name, proc.description, proc.billing_code) AS procedure_display_name,
                    proc.name AS procedure_name,
                    proc.description AS procedure_description,
                    array_agg(DISTINCT r.provider_set_hash ORDER BY r.provider_set_hash) AS provider_set_hashes,
                    COUNT(DISTINCT r.serving_rate_id)::int AS rate_count,
                    COALESCE(jsonb_agg(DISTINCT price_item.price_item) FILTER (WHERE price_item.price_item IS NOT NULL), '[]'::jsonb) AS prices,
                    COALESCE(jsonb_agg(DISTINCT trace_item.trace_item) FILTER (WHERE trace_item.trace_item IS NOT NULL), '[]'::jsonb) AS source_trace
                FROM rate_candidates r
                {procedure_join_sql}
                {provider_join_sql}
                {location_join_sql}
                {taxonomy_join_sql}
                LEFT JOIN {schema}.npi n ON n.npi = pgm.npi
                {price_join_sql}
                LEFT JOIN LATERAL jsonb_array_elements({compact_price_jsonb}) AS price_item(price_item) ON TRUE
                LEFT JOIN {schema}.ptg2_source_trace_set sts ON sts.source_trace_set_hash = r.source_trace_set_hash
                LEFT JOIN LATERAL unnest(COALESCE(sts.source_trace_hashes, ARRAY[]::varchar[])) AS sth(source_trace_hash) ON TRUE
                LEFT JOIN {schema}.ptg2_source_trace st ON st.source_trace_hash = sth.source_trace_hash
                LEFT JOIN LATERAL (
                    SELECT CASE WHEN st.source_trace_hash IS NULL THEN NULL ELSE jsonb_build_object(
                        'url', st.original_url,
                        'canonical_url', st.canonical_url,
                        'statement', 'Published negotiated rate from Transparency in Coverage source file.'
                    ) END AS trace_item
                ) trace_item ON TRUE
                WHERE {"TRUE" if not expansion_geo_sql else expansion_geo_sql}
                  {"AND " + taxonomy_sql if taxonomy_filters else ""}
                GROUP BY
                    pgm.npi,
                    addr.npi,
                    addr.type,
                    addr.checksum,
                    addr.state_name,
                    addr.city_name,
                    LEFT(COALESCE(addr.postal_code, ''), 5),
                    provider_name,
                    r.procedure_code,
                    r.reported_code_system,
                    r.reported_code,
                    proc.billing_code,
                    proc.billing_code_type,
                    proc.name,
                    proc.description
                ORDER BY rate_count DESC, provider_name, pgm.npi
                LIMIT :limit OFFSET :offset
                """
            ),
            params,
        )
        items = []
        for row in row_result:
            data = _row_mapping(row)
            confidence = {
                "network": "tic_rate_npi_tin",
                "location": data.get("location_confidence_code") or "nppes_practice_location",
            }
            items.append(
                {
                    "npi": data.get("npi"),
                    "provider_name": data.get("provider_name"),
                    "location_hash": data.get("location_hash"),
                    "state": data.get("state"),
                    "city": data.get("city"),
                    "zip5": data.get("zip5"),
                    "location_source": data.get("location_source"),
                    "address": _coerce_json_payload(data.get("address_payload"), {}),
                    "taxonomy_codes": data.get("taxonomy_codes") or [],
                    "specialties": data.get("specialties") or [],
                    "provider_set_hashes": data.get("provider_set_hashes") or [],
                    "provider_count": 1,
                    "rate_count": data.get("rate_count") or 0,
                    "procedure_code": data.get("procedure_code") if data.get("procedure_code") is not None else data.get("reported_code"),
                    "hp_procedure_code": data.get("procedure_code"),
                    "procedure_name": data.get("procedure_display_name") or data.get("procedure_name"),
                    "procedure_description": data.get("procedure_description"),
                    "service_code": data.get("billing_code"),
                    "service_code_system": data.get("billing_code_type") or data.get("reported_code_system") or "CPT",
                    "reported_code": data.get("reported_code") or data.get("billing_code"),
                    "reported_code_system": data.get("reported_code_system") or data.get("billing_code_type"),
                    "billing_code": data.get("billing_code"),
                    "billing_code_type": data.get("billing_code_type"),
                    **_price_response_fields(data.get("prices")),
                    "source_trace": _coerce_json_payload(data.get("source_trace"), []),
                    "confidence": confidence,
                }
            )
        if total is None:
            total = int(pagination.offset) + len(items)
    else:
        total = None
        if not _env_bool(PTG2_FAST_COMPACT_COUNTS_ENV, True):
            count_result = await session.execute(
                text(
                    f"""
                    SELECT COUNT(*)
                    FROM {table_name} r
                    {procedure_join_sql}
                    WHERE {where_sql}
                    {q_filter}
                    {provider_filter_sql}
                    """
                ),
                params,
            )
            total = int(count_result.scalar() or 0)
            if total <= 0:
                return None
        direct_provider_filter_cte_sql = ""
        direct_provider_filter_join_sql = ""
        direct_provider_filter_where_sql = provider_filter_sql
        direct_provider_filter_from_sql = f"{table_name} r"
        if provider_filter_requested and use_direct_provider_tables:
            params["candidate_rate_limit"] = max(int(getattr(pagination, "limit", 25) or 25) * 8, 8)
            direct_provider_filter_where_sql = ""
            direct_provider_filter_npi_sql = "pgm_filter.npi = :provider_npi" if provider_npi else ""
            direct_provider_filter_geo_join_sql = ""
            direct_provider_filter_taxonomy_join_sql = ""
            direct_provider_filter_conditions: list[str] = [direct_provider_filter_npi_sql]
            direct_provider_filter_from_sql = f"""
                          FROM {provider_set_component_table} psc_filter
                          JOIN {provider_group_member_table} pgm_filter
                            ON pgm_filter.provider_group_hash = psc_filter.provider_group_hash
            """
            use_location_dictionary = bool(
                provider_group_location_table
                and (geo_filters or coordinate_filter_requested)
                and not taxonomy_filters
                and not provider_npi
            )
            if geo_filters or coordinate_filter_requested:
                if use_location_dictionary:
                    direct_provider_filter_from_sql = f"""
                          FROM {provider_set_component_table} psc_filter
                          JOIN {provider_group_location_table} loc_filter
                            ON loc_filter.provider_group_hash = psc_filter.provider_group_hash
                    """
                    location_geo_sql = " AND ".join(
                        filter_sql
                        for filter_sql in (
                            "loc_filter.zip5 = :zip5" if zip_text else "",
                            "loc_filter.state_name = :state" if state_text else "",
                            "loc_filter.city_name = :city_exact" if city_text else "",
                            coordinate_sql.replace("addr_alias", "loc_filter") if coordinate_filter_requested else "",
                        )
                        if filter_sql
                    )
                    if location_geo_sql:
                        direct_provider_filter_conditions.append(location_geo_sql)
                else:
                    direct_provider_filter_geo_join_sql = f"""
                    JOIN {schema}.npi_address addr_filter
                      ON addr_filter.npi = pgm_filter.npi
                     AND addr_filter.type IN ('primary', 'secondary')
                    """
                    if provider_geo_sql:
                        direct_provider_filter_conditions.append(provider_geo_sql)
            if taxonomy_filters:
                direct_provider_filter_taxonomy_join_sql = f"""
                    JOIN {schema}.npi_taxonomy nt_filter
                      ON nt_filter.npi = pgm_filter.npi
                    JOIN {schema}.nucc_taxonomy nucc_filter
                      ON nucc_filter.code = nt_filter.healthcare_provider_taxonomy_code
                """
                direct_provider_filter_conditions.append(provider_taxonomy_sql)
            direct_provider_filter_condition_sql = "\n                     AND ".join(
                condition for condition in direct_provider_filter_conditions if condition.strip()
            ) or "TRUE"
            direct_provider_filter_cte_sql = f"""
                WITH rate_candidates AS MATERIALIZED (
                    SELECT r.*
                      FROM {table_name} r
                      {procedure_join_sql if q_filter else ""}
                     WHERE {where_sql}
                     {q_filter}
                     ORDER BY r.provider_count DESC NULLS LAST, r.serving_rate_id
                     LIMIT :candidate_rate_limit
                ),
                provider_filtered_rates AS MATERIALIZED (
                    SELECT r.*
                     FROM rate_candidates r
                     WHERE EXISTS (
                        SELECT 1
                          {direct_provider_filter_from_sql}
                          {direct_provider_filter_geo_join_sql}
                          {direct_provider_filter_taxonomy_join_sql}
                         WHERE psc_filter.provider_set_hash = r.provider_set_hash
                           AND {direct_provider_filter_condition_sql}
                         LIMIT 1
                     )
                     ORDER BY r.provider_count DESC NULLS LAST, r.serving_rate_id
                     LIMIT :limit OFFSET :offset
                )
            """
            direct_provider_filter_from_sql = "provider_filtered_rates r"
        row_result = await session.execute(
            text(
                f"""
                {direct_provider_filter_cte_sql}
                SELECT
                    r.serving_rate_id,
                    r.provider_set_hash,
                    r.provider_count,
                    r.procedure_code,
                    r.reported_code_system,
                    r.reported_code,
                    proc.billing_code AS billing_code,
                    proc.billing_code_type AS billing_code_type,
                    r.price_set_hash,
                    NULL::varchar AS rate_pack_hash,
                    COALESCE(proc.name, proc.description, proc.billing_code) AS procedure_display_name,
                    proc.name AS procedure_name,
                    proc.description AS procedure_description,
                    {compact_price_json} AS prices,
                    COALESCE(trace_payload.source_trace, CAST('[]' AS json)) AS source_trace
                FROM {direct_provider_filter_from_sql}
                {direct_provider_filter_join_sql}
                {procedure_join_sql}
                {price_join_sql}
                LEFT JOIN {schema}.ptg2_source_trace_set sts ON sts.source_trace_set_hash = r.source_trace_set_hash
                LEFT JOIN LATERAL (
                    SELECT json_agg(
                        json_build_object(
                            'url', st.original_url,
                            'canonical_url', st.canonical_url,
                            'statement', 'Published negotiated rate from Transparency in Coverage source file.'
                        )
                        ORDER BY st.source_trace_hash
                    ) AS source_trace
                    FROM {schema}.ptg2_source_trace st
                    WHERE sts.source_trace_hashes IS NOT NULL
                      AND st.source_trace_hash = ANY(sts.source_trace_hashes)
                ) trace_payload ON TRUE
                WHERE {"TRUE" if direct_provider_filter_cte_sql else where_sql}
                {"" if direct_provider_filter_cte_sql else q_filter}
                {direct_provider_filter_where_sql}
                ORDER BY r.provider_count DESC NULLS LAST, r.serving_rate_id
                LIMIT :limit OFFSET :offset
                """
            ),
            params,
        )
        items = []
        for row in row_result:
            data = _row_mapping(row)
            prices = _normalize_price_payload(data.get("prices"))
            items.append(
                {
                    "provider_ordinal": data.get("provider_set_hash"),
                    "provider_set_hash": data.get("provider_set_hash"),
                    "provider_set_hashes": [data.get("provider_set_hash")] if data.get("provider_set_hash") else [],
                    "provider_name": "TiC provider set",
                    "provider_count": data.get("provider_count") or 0,
                    "procedure_code": data.get("procedure_code") if data.get("procedure_code") is not None else data.get("reported_code"),
                    "hp_procedure_code": data.get("procedure_code"),
                    "procedure_name": data.get("procedure_display_name") or data.get("procedure_name"),
                    "procedure_description": data.get("procedure_description"),
                    "service_code": data.get("billing_code"),
                    "service_code_system": data.get("billing_code_type") or data.get("reported_code_system") or "CPT",
                    "reported_code": data.get("reported_code") or data.get("billing_code"),
                    "reported_code_system": data.get("reported_code_system") or data.get("billing_code_type"),
                    "billing_code": data.get("billing_code"),
                    "billing_code_type": data.get("billing_code_type"),
                    **_price_response_fields(prices),
                    "price_set_hash": data.get("price_set_hash"),
                    "rate_pack_hash": data.get("rate_pack_hash"),
                    "source_trace": _coerce_json_payload(data.get("source_trace"), []),
                    "confidence": {"network": "tic_rate_npi_tin", "location": "nppes_practice_location"},
                }
            )
        if total is None:
            total = int(pagination.offset) + len(items)
    if not items:
        return None
    return {
        "items": items,
        "pagination": {
            "total": total,
            "limit": pagination.limit,
            "offset": pagination.offset,
            "page": (pagination.offset // pagination.limit) + 1 if pagination.limit else 1,
        },
        "query": {
            "plan_id": args.get("plan_id"),
            "plan_external_id": args.get("plan_external_id"),
            "plan_market_type": args.get("plan_market_type") or None,
            "source_key": args.get("source_key") or None,
            "snapshot_id": snapshot_id,
            "mode": mode_value,
            "code": args.get("code") or None,
            "code_system": args.get("code_system") or None,
            "q": args.get("q") or None,
            "state": state_text or None,
            "city": city_text or None,
            "zip5": zip_text,
            "lat": geo_lat if coordinate_filter_requested else None,
            "long": geo_long if coordinate_filter_requested else None,
            "radius_miles": geo_radius_miles if coordinate_filter_requested else None,
            "specialty": specialty_text or None,
            "taxonomy_code": taxonomy_code,
            "taxonomy_classification": taxonomy_classification or None,
            "taxonomy_specialization": taxonomy_specialization or None,
            "taxonomy_section": taxonomy_section or None,
            "npi": provider_npi,
            "include_providers": expand_providers,
            "source": "ptg2_db_compact",
            "serving_table": table_name,
            "result_granularity": "provider" if has_provider_filters else "provider_set",
            "procedure_consolidation": "HP_PROCEDURE_CODE",
        },
    }


def _ptg2_v3_artifact_entries(serving_tables: PTG2ServingTables, name: str) -> list[dict[str, Any]]:
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


def _ptg2_v3_artifact_entry(serving_tables: PTG2ServingTables, name: str) -> dict[str, Any] | None:
    entries = _ptg2_v3_artifact_entries(serving_tables, name)
    return entries[0] if entries else None


def _ptg2_v3_sidecar_members(serving_tables: PTG2ServingTables, name: str, owner_id: str) -> tuple[str, ...]:
    owner_id = _ptg2_v3_id(owner_id)
    if not owner_id:
        return ()
    members: set[str] = set()
    for entry in _ptg2_v3_artifact_entries(serving_tables, name):
        path = str(entry.get("path") or "").strip()
        if not path:
            continue
        cache_key = (path, str(entry.get("sha256") or ""), owner_id)
        cached = _PTG2_V3_SIDECAR_CACHE.get(cache_key)
        if cached is None:
            cached = tuple(member.hex() for member in lookup_global_sidecar_members(Path(path), owner_id, metadata=entry))
            _PTG2_V3_SIDECAR_CACHE[cache_key] = cached
        members.update(cached)
    return tuple(sorted(members))


def _ptg2_v3_sidecar_members_many(
    serving_tables: PTG2ServingTables,
    name: str,
    owner_ids: list[str] | tuple[str, ...],
) -> dict[str, tuple[str, ...]]:
    owner_id_list = list(_ptg2_v3_ids(tuple(owner_ids)))
    result_sets: dict[str, set[str]] = {owner_id: set() for owner_id in owner_id_list}
    for entry in _ptg2_v3_artifact_entries(serving_tables, name):
        path = str(entry.get("path") or "").strip()
        if not path:
            continue
        sha = str(entry.get("sha256") or "")
        missing: list[str] = []
        for owner_id in owner_id_list:
            cache_key = (path, sha, owner_id)
            cached = _PTG2_V3_SIDECAR_CACHE.get(cache_key)
            if cached is None:
                missing.append(owner_id)
            else:
                result_sets[owner_id].update(cached)
        if missing:
            members_by_owner = lookup_global_sidecar_members_many(Path(path), missing, metadata=entry)
            for owner_id in missing:
                try:
                    owner_bytes = bytes.fromhex(owner_id)
                except ValueError:
                    owner_bytes = b""
                members = tuple(member.hex() for member in members_by_owner.get(owner_bytes, ()))
                _PTG2_V3_SIDECAR_CACHE[(path, sha, owner_id)] = members
                result_sets[owner_id].update(members)
    return {owner_id: tuple(sorted(members)) for owner_id, members in result_sets.items()}


def _ptg2_v3_provider_npis_for_provider_set(
    serving_tables: PTG2ServingTables,
    provider_set_global_id: str,
) -> tuple[int, ...]:
    members = _ptg2_v3_sidecar_members(serving_tables, "provider_npi", provider_set_global_id)
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


def _ptg2_v3_provider_npis_for_provider_sets(
    serving_tables: PTG2ServingTables,
    provider_set_global_ids: list[str] | tuple[str, ...],
) -> dict[str, tuple[int, ...]]:
    provider_set_ids = _ptg2_v3_ids(tuple(provider_set_global_ids))
    members_by_set = _ptg2_v3_sidecar_members_many(serving_tables, "provider_npi", provider_set_ids)
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
        result[provider_set_id] = tuple(sorted(set(npis)))
    return result


def _ptg2_v3_price_matches_filter(price: dict[str, Any], args: dict[str, Any]) -> bool:
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


def _ptg2_v3_filter_prices(prices: list[dict[str, Any]], args: dict[str, Any]) -> list[dict[str, Any]]:
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
    return [price for price in prices if _ptg2_v3_price_matches_filter(price, args)]


async def _ptg2_v3_prices_for_price_set(
    session,
    serving_tables: PTG2ServingTables,
    price_set_global_id: str,
) -> list[dict[str, Any]]:
    price_atom_table = _safe_table_name(serving_tables.price_atom_table)
    price_set_global_id = _ptg2_v3_id(price_set_global_id)
    if not price_atom_table or not price_set_global_id:
        return []
    price_members = _ptg2_v3_sidecar_members(serving_tables, "price_forward", price_set_global_id)
    if not price_members:
        return []
    atom_ids = list(price_members)
    atom_array_cast = _ptg2_v3_id_array_cast(serving_tables)
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
    rows_by_id = {_ptg2_v3_id(_row_mapping(row).get("price_atom_global_id_128")): _row_mapping(row) for row in result}
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


async def _ptg2_v3_prices_for_price_sets(
    session,
    serving_tables: PTG2ServingTables,
    price_set_global_ids: list[str] | tuple[str, ...],
) -> dict[str, list[dict[str, Any]]]:
    price_atom_table = _safe_table_name(serving_tables.price_atom_table)
    price_set_ids = _ptg2_v3_ids(tuple(price_set_global_ids))
    if not price_atom_table or not price_set_ids:
        return {price_set_id: [] for price_set_id in price_set_ids}
    members_by_price_set = _ptg2_v3_sidecar_members_many(serving_tables, "price_forward", price_set_ids)
    atom_ids = tuple(dict.fromkeys(atom_id for atoms in members_by_price_set.values() for atom_id in atoms))
    if not atom_ids:
        return {price_set_id: [] for price_set_id in price_set_ids}
    atom_array_cast = _ptg2_v3_id_array_cast(serving_tables)
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
    rows_by_id = {_ptg2_v3_id(_row_mapping(row).get("price_atom_global_id_128")): _row_mapping(row) for row in result}
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


async def _ptg2_v3_enriched_provider_rows_for_npis(
    session,
    *,
    npis: list[int] | tuple[int, ...],
    limit: int,
) -> list[dict[str, Any]] | None:
    npis = tuple(sorted({int(npi) for npi in npis if int(npi) > 0}))[:limit]
    if not npis:
        return []
    npi_data_table = f"{PTG2_SCHEMA}.npi_data"
    npi_address_table = f"{PTG2_SCHEMA}.npi_address"
    if not await _serving_table_available(session, npi_data_table) or not await _serving_table_available(
        session,
        npi_address_table,
    ):
        return [{"npi": npi, "provider_name": "TiC provider"} for npi in npis]
    enrich_stmt = (
        text(
            f"""
            SELECT
                source_npis.npi,
                CONCAT('npi_address:', addr.npi, ':', addr.type, ':', addr.checksum) AS location_hash,
                addr.state_name AS state,
                addr.city_name AS city,
                LEFT(COALESCE(addr.postal_code, ''), 5) AS zip5,
                'npi_address' AS location_source,
                'npi_address' AS location_confidence_code,
                json_build_object(
                    'first_line', addr.first_line,
                    'second_line', addr.second_line,
                    'city', addr.city_name,
                    'state', addr.state_name,
                    'postal_code', addr.postal_code,
                    'country_code', addr.country_code,
                    'lat', addr.lat,
                    'long', addr.long
                )::text AS address_payload,
                ARRAY[]::varchar[] AS taxonomy_codes,
                ARRAY[]::varchar[] AS specialties,
                COALESCE(
                    NULLIF(BTRIM(n.provider_organization_name), ''),
                    NULLIF(BTRIM(CONCAT_WS(' ', n.provider_first_name, n.provider_middle_name, n.provider_last_name)), ''),
                    'TiC provider'
                ) AS provider_name
            FROM (SELECT UNNEST(CAST(:npis AS bigint[])) AS npi) source_npis
            LEFT JOIN {npi_data_table} n ON n.npi = source_npis.npi
            LEFT JOIN LATERAL (
                SELECT addr.*
                  FROM {npi_address_table} addr
                 WHERE addr.npi = source_npis.npi
                 ORDER BY (addr.type = 'primary') DESC, addr.type, addr.checksum
                 LIMIT 1
            ) addr ON TRUE
            ORDER BY provider_name, source_npis.npi
            """
        )
    )
    result = await session.execute(enrich_stmt, {"npis": npis})
    return [_row_mapping(row) for row in result]


def _ptg2_v3_location_match_limit() -> int:
    raw_value = os.getenv("HLTHPRT_PTG2_V3_LOCATION_MATCH_LIMIT", "5000")
    try:
        return max(int(raw_value), 1)
    except ValueError:
        return 5000


async def _ptg2_v3_location_provider_matches(
    session,
    serving_tables: PTG2ServingTables,
    args: dict[str, Any],
    *,
    candidate_limit: int | None = None,
) -> tuple[set[str], dict[str, list[dict[str, Any]]]] | None:
    provider_group_member_table = _safe_table_name(serving_tables.provider_group_member_table)
    if not provider_group_member_table or not _ptg2_v3_artifact_entry(serving_tables, "provider_inverted"):
        return None

    state_value = str(args.get("state") or "").strip().upper()
    city_value = str(args.get("city") or "").strip().lower()
    zip_value = str(args.get("zip5") or args.get("zip") or "").strip()[:5]
    provider_npi = args.get("npi")
    npi_address_table = f"{PTG2_SCHEMA}.npi_address"
    npi_data_table = f"{PTG2_SCHEMA}.npi_data"
    if not await _serving_table_available(session, npi_address_table):
        return None

    filters = ["addr.npi IS NOT NULL"]
    configured_limit = _ptg2_v3_location_match_limit()
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
        filters.append("left(coalesce(addr.postal_code, ''), 5) = :zip5")
        params["zip5"] = zip_value
    if provider_npi is not None:
        try:
            params["provider_npi"] = int(provider_npi)
            filters.append("addr.npi = :provider_npi")
        except (TypeError, ValueError):
            return None
    if len(filters) == 1:
        return None

    has_npi_data = await _serving_table_available(session, npi_data_table)
    provider_join = f"LEFT JOIN {npi_data_table} n ON n.npi = addr.npi" if has_npi_data else ""
    provider_name_sql = (
        """
        COALESCE(
            NULLIF(BTRIM(n.provider_organization_name), ''),
            NULLIF(BTRIM(CONCAT_WS(' ', n.provider_first_name, n.provider_middle_name, n.provider_last_name)), ''),
            'TiC provider'
        )
        """
        if has_npi_data
        else "'TiC provider'"
    )

    async def _query_location_provider_rows(address_types: tuple[str, ...]) -> list[dict[str, Any]]:
        address_filters = [*filters]
        address_params = {**params, "address_types": list(address_types)}
        result = await session.execute(
            text(
                f"""
            WITH location_npis AS MATERIALIZED (
                SELECT DISTINCT ON (addr.npi)
                    addr.npi,
                    addr.type,
                    addr.checksum,
                    addr.state_name,
                    addr.city_name,
                    addr.postal_code,
                    addr.country_code,
                    addr.lat,
                    addr.long,
                    addr.first_line,
                    addr.second_line
                FROM {npi_address_table} addr
                WHERE {" AND ".join(address_filters)}
                  AND addr.type = ANY(CAST(:address_types AS varchar[]))
                ORDER BY addr.npi, (addr.type = 'primary') DESC, addr.type, addr.checksum
                LIMIT :limit
            )
            SELECT DISTINCT ON (pgm.provider_group_global_id_128, addr.npi)
                pgm.provider_group_global_id_128,
                addr.npi,
                CONCAT('npi_address:', addr.npi, ':', addr.type, ':', addr.checksum) AS location_hash,
                addr.state_name AS state,
                addr.city_name AS city,
                LEFT(COALESCE(addr.postal_code, ''), 5) AS zip5,
                'npi_address' AS location_source,
                'npi_address' AS location_confidence_code,
                json_build_object(
                    'first_line', addr.first_line,
                    'second_line', addr.second_line,
                    'city', addr.city_name,
                    'state', addr.state_name,
                    'postal_code', addr.postal_code,
                    'country_code', addr.country_code,
                    'lat', addr.lat,
                    'long', addr.long
                )::text AS address_payload,
                ARRAY[]::varchar[] AS taxonomy_codes,
                ARRAY[]::varchar[] AS specialties,
                {provider_name_sql} AS provider_name
            FROM location_npis addr
            JOIN {provider_group_member_table} pgm ON pgm.npi = addr.npi
            {provider_join}
            ORDER BY pgm.provider_group_global_id_128, addr.npi, (addr.type = 'primary') DESC, addr.type, addr.checksum
            LIMIT :limit
            """
            ),
            address_params,
        )
        return [_row_mapping(row) for row in result]

    # Primary addresses map to the existing (type, state, city, npi) and
    # (type, zip5) indexes. The broad primary+secondary form forces a large
    # sort in dense cities, so use primary practice locations for the hot path
    # and fall back to secondary only when primary has no candidates.
    rows = await _query_location_provider_rows(("primary",) if provider_npi is None else ("primary", "secondary"))
    if not rows and provider_npi is None:
        rows = await _query_location_provider_rows(("secondary",))
    if not rows:
        return set(), {}
    group_ids = tuple(
        sorted({_ptg2_v3_id(row.get("provider_group_global_id_128")) for row in rows if row.get("provider_group_global_id_128")})
    )
    if not group_ids:
        return set(), {}
    sets_by_group = _ptg2_v3_sidecar_members_many(serving_tables, "provider_inverted", group_ids)
    provider_set_ids: set[str] = set()
    providers_by_set: dict[str, list[dict[str, Any]]] = {}
    seen_provider_rows: dict[str, set[int]] = {}
    for row in rows:
        group_id = _ptg2_v3_id(row.get("provider_group_global_id_128"))
        npi = row.get("npi")
        if not group_id or npi is None:
            continue
        provider_row = {
            "npi": int(npi),
            "provider_name": row.get("provider_name") or "TiC provider",
            "state": row.get("state"),
            "city": row.get("city"),
            "zip5": row.get("zip5"),
            "location_hash": row.get("location_hash"),
            "location_source": row.get("location_source"),
            "location_confidence_code": row.get("location_confidence_code"),
            "address_payload": row.get("address_payload"),
            "taxonomy_codes": row.get("taxonomy_codes") or [],
            "specialties": row.get("specialties") or [],
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


async def _ptg2_v3_provider_rows_for_provider_set(
    session,
    serving_tables: PTG2ServingTables,
    provider_set_global_id: str,
    *,
    limit: int,
) -> list[dict[str, Any]] | None:
    provider_set_global_id = _ptg2_v3_id(provider_set_global_id)
    if not provider_set_global_id:
        return None
    provider_npis = _ptg2_v3_provider_npis_for_provider_set(serving_tables, provider_set_global_id)
    if provider_npis:
        return await _ptg2_v3_enriched_provider_rows_for_npis(session, npis=provider_npis, limit=limit)

    provider_group_member_table = _safe_table_name(serving_tables.provider_group_member_table)
    if not provider_group_member_table:
        return None
    provider_groups = _ptg2_v3_sidecar_members(serving_tables, "provider_forward", provider_set_global_id)
    if not provider_groups:
        return None
    group_ids = list(provider_groups)
    group_array_cast = _ptg2_v3_id_array_cast(serving_tables)
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
    return await _ptg2_v3_enriched_provider_rows_for_npis(session, npis=npis, limit=limit)


async def _ptg2_v3_provider_rows_for_provider_sets(
    session,
    serving_tables: PTG2ServingTables,
    provider_set_global_ids: list[str] | tuple[str, ...],
    *,
    limit_per_set: int,
) -> dict[str, list[dict[str, Any]]] | None:
    provider_set_ids = _ptg2_v3_ids(tuple(provider_set_global_ids))
    if not provider_set_ids:
        return {}

    npis_by_set = _ptg2_v3_provider_npis_for_provider_sets(serving_tables, provider_set_ids)
    missing_provider_set_ids = [provider_set_id for provider_set_id in provider_set_ids if not npis_by_set.get(provider_set_id)]
    if missing_provider_set_ids:
        provider_group_member_table = _safe_table_name(serving_tables.provider_group_member_table)
        if not provider_group_member_table:
            return None
        groups_by_set = _ptg2_v3_sidecar_members_many(serving_tables, "provider_forward", missing_provider_set_ids)
        group_ids = tuple(dict.fromkeys(group_id for group_ids in groups_by_set.values() for group_id in group_ids))
        if not group_ids:
            return None
        group_array_cast = _ptg2_v3_id_array_cast(serving_tables)
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
            group_id = _ptg2_v3_id(data.get("provider_group_global_id_128"))
            npi = data.get("npi")
            if group_id and npi is not None:
                npis_by_group.setdefault(group_id, set()).add(int(npi))
        for provider_set_id in missing_provider_set_ids:
            npis: set[int] = set()
            for group_id in groups_by_set.get(provider_set_id, ()):
                npis.update(npis_by_group.get(group_id, set()))
            npis_by_set[provider_set_id] = tuple(sorted(npis))

    all_npis = tuple(
        dict.fromkeys(
            npi
            for provider_set_id in provider_set_ids
            for npi in npis_by_set.get(provider_set_id, ())[: max(int(limit_per_set), 1)]
        )
    )
    provider_rows = await _ptg2_v3_enriched_provider_rows_for_npis(
        session,
        npis=all_npis,
        limit=max(len(all_npis), 1),
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


async def _ptg2_v3_provider_sets_for_npi(
    session,
    serving_tables: PTG2ServingTables,
    npi: int,
) -> tuple[str, ...] | None:
    provider_group_member_table = _safe_table_name(serving_tables.provider_group_member_table)
    if not provider_group_member_table or not _ptg2_v3_artifact_entry(serving_tables, "provider_inverted"):
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
        _ptg2_v3_id(_row_mapping(row).get("provider_group_global_id_128"))
        for row in group_result
        if _row_mapping(row).get("provider_group_global_id_128")
    ]
    if not group_ids:
        return ()
    sets_by_group = _ptg2_v3_sidecar_members_many(serving_tables, "provider_inverted", tuple(group_ids))
    provider_set_ids = tuple(
        sorted({provider_set_id for provider_set_ids in sets_by_group.values() for provider_set_id in provider_set_ids})
    )
    return provider_set_ids


async def _ptg2_v3_procedure_details_for_rows(
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
    """Serve the additive v3 skinny table for exact plan/code lookups.

    Provider, geography, and text expansion still require v3 sidecar readers; keep
    this path intentionally bounded until those readers are wired into the API.
    """
    table_name = _safe_table_name(serving_tables.serving_table)
    if not table_name or not await _serving_table_available(session, table_name):
        return None
    requested_plan = str(args.get("plan_id") or args.get("plan_external_id") or "").strip()
    requested_code = str(args.get("code") or args.get("reported_code") or "").strip()
    requested_system = str(args.get("code_system") or args.get("reported_code_system") or "").strip().upper()
    expand_providers = _request_bool(args.get("include_providers"))
    location_filter_requested = bool(
        args.get("state")
        or args.get("city")
        or args.get("zip5")
        or args.get("zip")
        or args.get("npi")
    )
    unsupported_filters = (
        args.get("q")
        or args.get("lat")
        or args.get("long")
        or args.get("radius_miles")
    )
    if unsupported_filters or not requested_plan or not requested_code:
        return None
    has_provider_npi_sidecar = bool(_ptg2_v3_artifact_entry(serving_tables, "provider_npi"))
    if expand_providers and not serving_tables.provider_group_member_table and not has_provider_npi_sidecar:
        return None

    filters = ["plan_id = :plan_id", "reported_code = :reported_code"]
    params: dict[str, Any] = {
        "plan_id": requested_plan,
        "reported_code": requested_code,
        "limit": int(pagination.limit),
        "offset": int(pagination.offset),
    }
    if requested_system:
        filters.append("reported_code_system = :reported_code_system")
        params["reported_code_system"] = requested_system
    location_providers_by_set: dict[str, list[dict[str, Any]]] = {}
    if location_filter_requested:
        location_matches = await _ptg2_v3_location_provider_matches(
            session,
            serving_tables,
            args,
            candidate_limit=max(int(pagination.limit) * 4, 200),
        )
        if location_matches is None:
            return None
        provider_set_ids, location_providers_by_set = location_matches
        if not provider_set_ids:
            return {
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
                    "plan_market_type": args.get("plan_market_type") or None,
                    "source_key": args.get("source_key") or None,
                    "snapshot_id": snapshot_id,
                    "mode": mode_value,
                    "code": args.get("code") or None,
                    "code_system": args.get("code_system") or None,
                    "state": args.get("state") or None,
                    "city": args.get("city") or None,
                    "zip5": args.get("zip5") or None,
                    "npi": args.get("npi") or None,
                    "source": "ptg2_db",
                    "serving_table": table_name,
                    "include_providers": expand_providers,
                    "result_granularity": "provider" if expand_providers else "provider_set",
                    "procedure_consolidation": "REPORTED_CODE",
                    "status": "no_match",
                },
            }
        filters.append(f"provider_set_global_id_128 = ANY(CAST(:provider_set_ids AS {_ptg2_v3_id_array_cast(serving_tables)}))")
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
                source_trace_set_hash
            FROM {table_name}
            WHERE {where_sql}
            ORDER BY provider_count DESC NULLS LAST, serving_content_hash_128
            LIMIT :limit OFFSET :offset
            """
        ),
        params,
    )
    items: list[dict[str, Any]] = []
    row_data = [_row_mapping(row) for row in row_result]
    if not row_data:
        return None
    prices_by_price_set = await _ptg2_v3_prices_for_price_sets(
        session,
        serving_tables,
        [_ptg2_v3_id(data.get("price_set_global_id_128")) for data in row_data],
    )
    providers_by_set: dict[str, list[dict[str, Any]]] = {}
    if expand_providers:
        if location_filter_requested:
            providers_by_set = location_providers_by_set
        else:
            provider_set_ids = [_ptg2_v3_id(data.get("provider_set_global_id_128")) for data in row_data]
            provider_rows_by_set = await _ptg2_v3_provider_rows_for_provider_sets(
                session,
                serving_tables,
                provider_set_ids,
                limit_per_set=max(int(pagination.limit), 1),
            )
            if provider_rows_by_set is None:
                return None
            providers_by_set = provider_rows_by_set
    procedure_details = await _ptg2_v3_procedure_details_for_rows(session, row_data)
    for data in row_data:
        if len(items) >= int(pagination.limit):
            break
        reported_code = data.get("reported_code")
        reported_system = data.get("reported_code_system")
        provider_set_hash = _ptg2_v3_id(data.get("provider_set_global_id_128"))
        price_set_hash = _ptg2_v3_id(data.get("price_set_global_id_128"))
        rate_pack_hash = _ptg2_v3_id(data.get("serving_content_hash_128"))
        prices = prices_by_price_set.get(price_set_hash, [])
        procedure_detail = procedure_details.get(_catalog_key(reported_system, reported_code) or ("", ""), {})
        base_item = {
                "provider_ordinal": provider_set_hash,
                "provider_set_hash": provider_set_hash,
                "provider_set_hashes": [provider_set_hash] if provider_set_hash else [],
                "provider_name": "TiC provider set",
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
                "source_trace": [],
                "confidence": {"network": "tic_rate_npi_tin", "location": "nppes_practice_location"},
            }
        if not expand_providers:
            items.append(base_item)
            continue
        remaining = max(int(pagination.limit) - len(items), 0)
        if remaining <= 0:
            break
        for provider in providers_by_set.get(_ptg2_v3_id(data.get("provider_set_global_id_128")), [])[:remaining]:
            item = dict(base_item)
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
                    "address": _coerce_json_payload(provider.get("address_payload"), {}),
                    "taxonomy_codes": _coerce_json_payload(provider.get("taxonomy_codes"), []),
                    "specialties": _coerce_json_payload(provider.get("specialties"), []),
                }
            )
            items.append(item)
    if not items:
        return None
    return {
        "items": items,
        "pagination": {
            "total": total if total is not None else int(pagination.offset) + len(items),
            "limit": pagination.limit,
            "offset": pagination.offset,
            "page": (pagination.offset // pagination.limit) + 1 if pagination.limit else 1,
        },
            "query": {
                "plan_id": args.get("plan_id"),
                "plan_external_id": args.get("plan_external_id"),
                "plan_market_type": args.get("plan_market_type") or None,
                "source_key": args.get("source_key") or None,
                "snapshot_id": snapshot_id,
                "mode": mode_value,
                "code": args.get("code") or None,
                "code_system": args.get("code_system") or None,
                "state": args.get("state") or None,
                "city": args.get("city") or None,
                "zip5": args.get("zip5") or None,
                "npi": args.get("npi") or None,
                "source": "ptg2_db",
                "serving_table": table_name,
                "include_providers": expand_providers,
            "result_granularity": "provider" if expand_providers else "provider_set",
            "procedure_consolidation": "REPORTED_CODE",
        },
    }


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
    if _ptg2_v3_storage_enabled(serving_tables):
        if serving_tables.serving_table:
            return await _search_ptg2_manifest_db_serving_table(
                session,
                snapshot_id,
                args,
                pagination,
                serving_tables,
                mode_value,
            )
        if serving_tables.is_manifest_backed_snapshot:
            return await search_ptg2_v3_serving_snapshot(
                snapshot_id,
                args,
                pagination,
                serving_tables=serving_tables,
                mode_value=mode_value,
            )
        return None
    requested_plan = str(args.get("plan_id") or args.get("plan_external_id") or "").strip()
    q_text = str(args.get("q") or "").strip().lower()
    zip_text = _normalize_zip5(args.get("zip5"))
    filters = ["snapshot_id = :snapshot_id"]
    params: dict[str, Any] = {
        "snapshot_id": snapshot_id,
        "limit": int(pagination.limit),
        "offset": int(pagination.offset),
    }
    if requested_plan:
        filters.append("plan_id = :plan_id")
        params["plan_id"] = requested_plan
    code_context = await _resolve_ptg2_code_search_context(
        session,
        code=args.get("code"),
        code_system=args.get("code_system"),
    )
    _append_resolved_code_filter(
        filters,
        params,
        code=args.get("code"),
        code_system=args.get("code_system"),
        code_context=code_context,
    )
    if q_text:
        params["q_like"] = f"%{q_text}%"

    preferred_table = serving_tables.serving_table
    price_set_table = f"{PTG2_SCHEMA}.ptg2_price_set"
    source_trace_set_table = f"{PTG2_SCHEMA}.ptg2_source_trace_set"
    source_trace_table = f"{PTG2_SCHEMA}.ptg2_source_trace"
    total = 0
    row_result = None
    table_name = None
    for candidate in _ordered_serving_table_candidates(preferred_table):
        if not await _serving_table_available(session, candidate):
            continue
        if _is_compact_serving_table(candidate):
            compact_payload = await _search_compact_serving_table(
                session,
                candidate,
                serving_tables,
                snapshot_id,
                args,
                pagination,
                filters,
                dict(params),
                mode_value,
            )
            if compact_payload is not None:
                compact_payload["query"] = {
                    **dict(compact_payload.get("query") or {}),
                    **_ptg2_code_query_fields(code_context, args),
                }
                return compact_payload
            continue
        noncompact_filters = list(filters)
        if q_text:
            noncompact_filters.append(
                """
                (
                    LOWER(COALESCE(procedure_display_name, '')) LIKE :q_like
                 OR LOWER(COALESCE(procedure_name, '')) LIKE :q_like
                 OR LOWER(COALESCE(procedure_description, '')) LIKE :q_like
                 OR LOWER(COALESCE(billing_code, '')) LIKE :q_like
                 OR LOWER(COALESCE(reported_code, '')) LIKE :q_like
                )
                """
            )
            params["q_like"] = f"%{q_text}%"
        noncompact_where_sql = " AND ".join(noncompact_filters)
        count_result = await session.execute(text(f"SELECT COUNT(*) FROM {candidate} WHERE {noncompact_where_sql}"), params)
        candidate_total = int(count_result.scalar() or 0)
        if candidate_total <= 0:
            continue
        total = candidate_total
        table_name = candidate
        if candidate.endswith(".ptg2_serving_rate_stage"):
            price_expr = "COALESCE(r.prices, CAST('[]' AS json)) AS prices"
            source_trace_expr = "COALESCE(r.source_trace, CAST('[]' AS json)) AS source_trace"
            join_sql = ""
        else:
            price_expr = f"COALESCE(r.prices, {_typed_price_json_sql('ps', json_type='json')}) AS prices"
            source_trace_expr = "COALESCE(r.source_trace, trace_payload.source_trace, CAST('[]' AS json)) AS source_trace"
            join_sql = f"""
                LEFT JOIN {price_set_table} ps
                  ON ps.price_set_hash = r.price_set_hash
                LEFT JOIN {source_trace_set_table} sts
                  ON sts.source_trace_set_hash = r.source_trace_set_hash
                LEFT JOIN LATERAL (
                    SELECT json_agg(
                        json_build_object(
                            'url', st.original_url,
                            'canonical_url', st.canonical_url,
                            'statement', 'Published negotiated rate from Transparency in Coverage source file.'
                        )
                        ORDER BY st.source_trace_hash
                    ) AS source_trace
                    FROM {source_trace_table} st
                    WHERE sts.source_trace_hashes IS NOT NULL
                      AND st.source_trace_hash = ANY(sts.source_trace_hashes)
                ) trace_payload ON TRUE
                """
        row_result = await session.execute(
            text(
                f"""
                SELECT
                    r.serving_rate_id,
                    r.snapshot_id,
                    r.plan_id,
                    r.plan_name,
                    r.plan_id_type,
                    r.plan_market_type,
                    r.issuer_name,
                    r.plan_sponsor_name,
                    r.procedure_code,
                    r.reported_code_system,
                    r.reported_code,
                    r.billing_code,
                    r.billing_code_type,
                    r.procedure_name,
                    r.procedure_description,
                    r.procedure_display_name,
                    r.rate_pack_hash,
                    r.provider_set_hash,
                    r.provider_set_hashes,
                    r.provider_count,
                    r.provider_set_count,
                    r.price_set_hash,
                    {price_expr},
                    {source_trace_expr},
                    r.confidence
                FROM {candidate} r
                {join_sql}
                WHERE {noncompact_where_sql}
                ORDER BY r.provider_count DESC NULLS LAST, r.provider_set_count DESC NULLS LAST, r.serving_rate_id
                LIMIT :limit OFFSET :offset
                """
            ),
            params,
        )
        break
    if row_result is None or table_name is None:
        return None
    items: list[dict[str, Any]] = []
    for row in row_result:
        data = _row_mapping(row)
        prices = _normalize_price_payload(data.get("prices"))
        source_trace = _coerce_json_payload(data.get("source_trace"), [])
        confidence = _coerce_json_payload(data.get("confidence"), {}) or {
            "network": "tic_rate_npi_tin",
            "location": "nppes_practice_location",
        }
        hp_code = data.get("procedure_code")
        reported_code = data.get("reported_code") or data.get("billing_code")
        reported_system = data.get("reported_code_system") or data.get("billing_code_type")
        item = {
            "provider_ordinal": data.get("provider_set_hash"),
            "provider_set_hash": data.get("provider_set_hash"),
            "provider_set_hashes": data.get("provider_set_hashes") or [],
            "provider_name": "TiC provider set",
            "provider_count": data.get("provider_count") or 0,
            "provider_set_count": data.get("provider_set_count") or 0,
            "procedure_code": hp_code if hp_code is not None else reported_code,
            "hp_procedure_code": hp_code,
            "procedure_name": data.get("procedure_display_name") or data.get("procedure_name"),
            "procedure_description": data.get("procedure_description"),
            "service_code": data.get("billing_code"),
            "service_code_system": data.get("billing_code_type") or reported_system or "CPT",
            "reported_code": reported_code,
            "reported_code_system": reported_system,
            "billing_code": data.get("billing_code"),
            "billing_code_type": data.get("billing_code_type"),
            **_price_response_fields(prices),
            "price_set_hash": data.get("price_set_hash"),
            "rate_pack_hash": data.get("rate_pack_hash"),
            "source_trace": source_trace,
            "confidence": confidence,
        }
        items.append(item)

    return {
        "items": items,
        "pagination": {
            "total": total,
            "limit": pagination.limit,
            "offset": pagination.offset,
            "page": (pagination.offset // pagination.limit) + 1 if pagination.limit else 1,
        },
        "query": {
            "plan_id": args.get("plan_id"),
            "plan_external_id": args.get("plan_external_id"),
            "snapshot_id": snapshot_id,
            "mode": mode_value,
            "code": args.get("code") or None,
            "code_system": args.get("code_system") or None,
            "q": args.get("q") or None,
            "state": args.get("state") or None,
            "city": args.get("city") or None,
            "zip5": zip_text,
            "source": "ptg2_db_stage" if table_name.endswith(".ptg2_serving_rate_stage") else "ptg2_db",
            "serving_table": table_name,
            "procedure_consolidation": "HP_PROCEDURE_CODE",
            **_ptg2_code_query_fields(code_context, args),
        },
    }


def _append_v3_reported_code_filter(
    filters: list[str],
    params: dict[str, Any],
    *,
    code: Any,
    code_system: Any,
    code_context: dict[str, Any] | None,
) -> None:
    requested_code = _normalize_code(code)
    if not requested_code:
        return
    requested_system = _normalize_code_system(code_system)
    if not code_context:
        params["reported_code"] = requested_code
        filters.append("reported_code = :reported_code")
        if requested_system and requested_system != INTERNAL_PROCEDURE_CODE_SYSTEM:
            params["reported_code_system"] = requested_system
            filters.append("reported_code_system = :reported_code_system")
        elif requested_system == INTERNAL_PROCEDURE_CODE_SYSTEM:
            filters.append("FALSE")
        return

    clauses: list[str] = []
    for idx, resolved_code in enumerate(code_context.get("resolved_codes") or []):
        system = str(resolved_code.get("code_system") or "").strip().upper()
        value = str(resolved_code.get("code") or "").strip().upper()
        if not system or not value or system == INTERNAL_PROCEDURE_CODE_SYSTEM:
            continue
        params[f"v3_reported_code_{idx}"] = value
        params[f"v3_reported_code_system_{idx}"] = system
        clauses.append(
            f"""
            (
                reported_code = :v3_reported_code_{idx}
            AND reported_code_system = :v3_reported_code_system_{idx}
            )
            """
        )
    if clauses:
        filters.append("(" + " OR ".join(clauses) + ")")
    elif requested_system == INTERNAL_PROCEDURE_CODE_SYSTEM:
        filters.append("FALSE")


async def _search_ptg2_v3_provider_procedures(
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
    provider_set_ids = await _ptg2_v3_provider_sets_for_npi(session, serving_tables, npi)
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
        "provider_set_ids": list(_ptg2_v3_ids(tuple(provider_set_ids))),
        "limit": int(pagination.limit),
        "offset": int(pagination.offset),
    }
    code_context = await _resolve_ptg2_code_search_context(
        session,
        code=code_value,
        code_system=args.get("code_system"),
    )
    filters = [f"provider_set_global_id_128 = ANY(CAST(:provider_set_ids AS {_ptg2_v3_id_array_cast(serving_tables)}))"]
    if requested_plan:
        filters.append("plan_id = :plan_id")
        params["plan_id"] = requested_plan
    _append_v3_reported_code_filter(
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
                source_trace_set_hash
            FROM {table_name}
            WHERE {where_sql}
            ORDER BY reported_code_system, reported_code, provider_count DESC NULLS LAST, serving_content_hash_128
            {limit_sql} {offset_sql}
            """
    )
    row_result = await session.execute(row_stmt, params)
    row_data = [_row_mapping(row) for row in row_result]
    prices_by_price_set = await _ptg2_v3_prices_for_price_sets(
        session,
        serving_tables,
        [_ptg2_v3_id(data.get("price_set_global_id_128")) for data in row_data],
    )
    procedure_details = await _ptg2_v3_procedure_details_for_rows(session, row_data)

    items: list[dict[str, Any]] = []
    skipped_for_offset = 0
    for data in row_data:
        prices = _ptg2_v3_filter_prices(
            prices_by_price_set.get(_ptg2_v3_id(data.get("price_set_global_id_128")), []),
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
        provider_set_hash = _ptg2_v3_id(data.get("provider_set_global_id_128"))
        price_set_hash = _ptg2_v3_id(data.get("price_set_global_id_128"))
        rate_pack_hash = _ptg2_v3_id(data.get("serving_content_hash_128"))
        items.append(
            {
                "npi": npi,
                "provider_set_hash": provider_set_hash,
                "provider_count": data.get("provider_count") or 0,
                "provider_set_count": 1 if provider_set_hash else 0,
                "procedure_code": reported_code,
                "hp_procedure_code": reported_code,
                "procedure_name": procedure_detail.get("procedure_name"),
                "procedure_description": procedure_detail.get("procedure_description"),
                "service_code": reported_code,
                "service_code_system": reported_system or args.get("code_system") or "CPT",
                "reported_code": reported_code,
                "reported_code_system": reported_system,
                "billing_code": reported_code,
                "billing_code_type": reported_system,
                **_price_response_fields(prices),
                "price_set_hash": price_set_hash,
                "rate_pack_hash": rate_pack_hash,
                "confidence": {"network": "tic_rate_npi_tin", "location": "nppes_practice_location"},
            }
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


async def search_ptg2_provider_procedures(session, npi: int, args: dict[str, Any], pagination) -> dict[str, Any] | None:
    snapshot_id = await resolve_current_ptg2_snapshot_id(session, args)
    if not snapshot_id:
        return None
    serving_tables = await snapshot_serving_tables(session, snapshot_id)
    if _ptg2_v3_storage_enabled(serving_tables):
        return await _search_ptg2_v3_provider_procedures(
            session,
            npi,
            args,
            pagination,
            snapshot_id=snapshot_id,
            serving_tables=serving_tables,
        )
    table_name = serving_tables.serving_table
    if not table_name or not _is_compact_serving_table(table_name):
        return None
    if not await _serving_table_available(session, table_name):
        return None

    schema = PTG2_SCHEMA
    provider_set_component_table = serving_tables.provider_set_component_table
    provider_set_entry_table = serving_tables.provider_set_entry_table
    provider_entry_component_table = serving_tables.provider_entry_component_table
    provider_group_member_table = serving_tables.provider_group_member_table or f"{schema}.ptg2_provider_group_member"
    procedure_table = serving_tables.procedure_table or f"{schema}.ptg2_procedure"
    use_normalized_price_tables = bool(
        serving_tables.price_atom_table
        and serving_tables.price_set_entry_table
        and serving_tables.price_code_set_table
    )
    use_direct_provider_tables = bool(provider_set_component_table and serving_tables.provider_group_member_table)
    use_provider_entry_tables = bool(
        provider_set_entry_table
        and provider_entry_component_table
        and serving_tables.provider_group_member_table
    )
    if not use_normalized_price_tables or not (use_direct_provider_tables or use_provider_entry_tables):
        return None
    has_reverse_provider_index = True

    params: dict[str, Any] = {
        "snapshot_id": snapshot_id,
        "provider_npi": npi,
        "limit": pagination.limit,
        "offset": pagination.offset,
    }
    requested_plan = str(args.get("plan_id") or args.get("plan_external_id") or "").strip()
    code_value = str(args.get("code") or args.get("reported_code") or "").strip()
    q_text = str(args.get("q") or args.get("service_name") or "").strip().lower()
    market_type = str(args.get("plan_market_type") or "").strip().lower()
    if not has_reverse_provider_index:
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
                    "plan_market_type": market_type or None,
                    "source_key": args.get("source_key") or None,
                    "snapshot_id": snapshot_id,
                    "mode": normalize_ptg2_mode(args.get("mode")),
                    "code": code_value or None,
                    "code_system": args.get("code_system") or None,
                    "q": q_text or None,
                    "source": "ptg2_db",
                    "serving_table": table_name,
                    "provider_reverse_index": False,
                    "status": "provider_reverse_index_missing",
                },
            },
            args,
        )
    filters = ["r.snapshot_id = :snapshot_id"]
    if requested_plan:
        filters.append("r.plan_id = :plan_id")
        params["plan_id"] = requested_plan

    code_context = await _resolve_ptg2_code_search_context(
        session,
        code=code_value,
        code_system=args.get("code_system"),
    )
    _append_resolved_code_filter(
        filters,
        params,
        code=code_value,
        code_system=args.get("code_system"),
        code_context=code_context,
    )
    q_filter = ""
    if q_text:
        q_filter = """
          AND (
                LOWER(COALESCE(proc.name, '')) LIKE :q_like
             OR LOWER(COALESCE(proc.description, '')) LIKE :q_like
             OR LOWER(COALESCE(proc.billing_code, '')) LIKE :q_like
             OR LOWER(COALESCE(r.reported_code, '')) LIKE :q_like
          )
        """
        params["q_like"] = f"%{q_text}%"
    price_filter_clauses, price_filter_query = _price_filter_clauses(args, params)
    price_filter_join_sql = (
        "\n             AND " + "\n             AND ".join(price_filter_clauses) if price_filter_clauses else ""
    )
    where_sql = " AND ".join(_qualify_compact_filters(filters))
    compact_price_json = _normalized_price_json_sql(json_type="json")
    normalized_price_join_sql = _normalized_price_join_sql(serving_tables, price_filter_sql=price_filter_join_sql)
    if use_direct_provider_tables:
        provider_sets_sql = f"""
            provider_sets AS MATERIALIZED (
                SELECT DISTINCT psc.provider_set_hash
                  FROM provider_groups pg
                  JOIN {provider_set_component_table} psc
                    ON psc.provider_group_hash = pg.provider_group_hash
            )
        """
    else:
        provider_sets_sql = f"""
            provider_sets AS MATERIALIZED (
                SELECT DISTINCT pse.provider_set_hash
                  FROM provider_groups pg
                  JOIN {provider_entry_component_table} pec
                    ON pec.provider_group_hash = pg.provider_group_hash
                  JOIN {provider_set_entry_table} pse
                    ON pse.provider_entry_hash = pec.provider_entry_hash
            )
        """

    total: int | None = None
    if not price_filter_clauses and not _env_bool(PTG2_FAST_COMPACT_COUNTS_ENV, True):
        count_result = await session.execute(
            text(
                f"""
                WITH provider_groups AS MATERIALIZED (
                    SELECT provider_group_hash
                      FROM {provider_group_member_table}
                     WHERE npi = :provider_npi
                ),
                {provider_sets_sql}
                SELECT COUNT(*)
                  FROM provider_sets provider_set_match
                  JOIN {table_name} r
                    ON r.provider_set_hash = provider_set_match.provider_set_hash
                  JOIN LATERAL (
                        SELECT proc.*
                          FROM {procedure_table} proc
                         WHERE proc.procedure_hash = r.procedure_hash
                         LIMIT 1
                  ) proc ON TRUE
                 WHERE {where_sql}
                 {q_filter}
                """
            ),
            params,
        )
        total = int(count_result.scalar() or 0)
        if total <= 0:
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
                        "provider_reverse_index": has_reverse_provider_index,
                        "status": "no_match",
                        **_ptg2_code_query_fields(code_context, args),
                    },
                },
                args,
            )

    matched_rate_procedure_join = ""
    matched_rate_q_filter = ""
    if q_filter:
        matched_rate_procedure_join = f"""
                  JOIN LATERAL (
                        SELECT proc.*
                          FROM {procedure_table} proc
                         WHERE proc.procedure_hash = r.procedure_hash
                         LIMIT 1
                  ) proc ON TRUE
        """
        matched_rate_q_filter = q_filter
    if price_filter_clauses:
        params["price_candidate_rate_limit"] = max(int(getattr(pagination, "limit", 25) or 25) * 200, 500)
        matched_rates_limit_sql = "LIMIT :price_candidate_rate_limit"
        matched_rates_offset_sql = ""
        filtered_price_where_sql = "WHERE price_payload.prices IS NOT NULL"
        final_price_page_sql = "LIMIT :limit OFFSET :offset"
    else:
        matched_rates_limit_sql = "LIMIT :limit"
        matched_rates_offset_sql = "OFFSET :offset"
        filtered_price_where_sql = ""
        final_price_page_sql = ""

    row_result = await session.execute(
        text(
            f"""
            WITH provider_groups AS MATERIALIZED (
                SELECT provider_group_hash
                  FROM {provider_group_member_table}
                 WHERE npi = :provider_npi
            ),
            {provider_sets_sql},
            matched_rates AS MATERIALIZED (
                SELECT
                    r.serving_rate_id,
                    r.snapshot_id,
                    r.plan_id,
                    r.procedure_code,
                    r.reported_code_system,
                    r.reported_code,
                    r.procedure_hash,
                    r.provider_set_hash,
                    r.provider_count,
                    r.price_set_hash
                  FROM provider_sets provider_set_match
                  JOIN {table_name} r
                    ON r.provider_set_hash = provider_set_match.provider_set_hash
                  {matched_rate_procedure_join}
                 WHERE {where_sql}
                 {matched_rate_q_filter}
                 ORDER BY r.reported_code_system, r.reported_code, r.provider_count DESC NULLS LAST, r.serving_rate_id
                 {matched_rates_limit_sql} {matched_rates_offset_sql}
            )
            SELECT
                r.serving_rate_id,
                r.snapshot_id,
                r.plan_id,
                NULL::varchar AS plan_name,
                NULL::varchar AS plan_id_type,
                NULL::varchar AS plan_market_type,
                NULL::varchar AS issuer_name,
                NULL::varchar AS plan_sponsor_name,
                r.procedure_code,
                r.reported_code_system,
                r.reported_code,
                proc.billing_code AS billing_code,
                proc.billing_code_type AS billing_code_type,
                proc.name AS procedure_name,
                proc.description AS procedure_description,
                r.provider_set_hash,
                r.provider_count,
                NULL::integer AS provider_set_count,
                r.price_set_hash,
                COALESCE({compact_price_json}, CAST('[]' AS json)) AS prices
              FROM matched_rates r
              JOIN LATERAL (
                    SELECT proc.*
                      FROM {procedure_table} proc
                     WHERE proc.procedure_hash = r.procedure_hash
                     LIMIT 1
              ) proc ON TRUE
              {normalized_price_join_sql}
             {filtered_price_where_sql}
             ORDER BY r.reported_code_system, r.reported_code, r.provider_count DESC NULLS LAST, r.serving_rate_id
             {final_price_page_sql}
            """
        ),
        params,
    )

    items: list[dict[str, Any]] = []
    for row in row_result:
        data = _row_mapping(row)
        prices = _normalize_price_payload(data.get("prices"))
        reported_code = data.get("reported_code") or data.get("billing_code")
        reported_system = data.get("reported_code_system") or data.get("billing_code_type")
        items.append(
            {
                "npi": npi,
                "provider_set_hash": data.get("provider_set_hash"),
                "provider_count": data.get("provider_count") or 0,
                "provider_set_count": data.get("provider_set_count") or 0,
                "procedure_code": data.get("procedure_code") if data.get("procedure_code") is not None else reported_code,
                "hp_procedure_code": data.get("procedure_code"),
                "procedure_name": data.get("procedure_name"),
                "procedure_description": data.get("procedure_description"),
                "service_code": data.get("billing_code"),
                "service_code_system": data.get("billing_code_type") or reported_system or "CPT",
                "reported_code": reported_code,
                "reported_code_system": reported_system,
                "billing_code": data.get("billing_code"),
                "billing_code_type": data.get("billing_code_type"),
                **_price_response_fields(prices),
                "price_set_hash": data.get("price_set_hash"),
            }
        )
    if total is None:
        total = int(pagination.offset) + len(items)
    if not items:
        return _shape_ptg2_response(
            {
                "items": [],
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
                    "provider_reverse_index": has_reverse_provider_index,
                    "status": "no_match",
                    **_ptg2_code_query_fields(code_context, args),
                },
            },
            args,
        )

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
                "provider_reverse_index": has_reverse_provider_index,
                **_ptg2_code_query_fields(code_context, args),
            },
        },
        args,
    )


def search_ptg2_index(
    index: PTG2ServingIndex,
    *,
    plan_id: str | None = None,
    plan_external_id: str | None = None,
    code: str | None = None,
    q: str | None = None,
    state: str | None = None,
    city: str | None = None,
    zip5: str | None = None,
    limit: int = 25,
    offset: int = 0,
    mode: str | None = None,
) -> dict[str, Any]:
    mode_value = normalize_ptg2_mode(mode)
    requested_plan = str(plan_id or plan_external_id or "").strip()
    requested_code = _normalize_code(code)
    q_text = str(q or "").strip().lower()
    state_text = str(state or "").strip().upper()
    city_text = str(city or "").strip().lower()
    zip_text = _normalize_zip5(zip5)

    plan_rates = index.rates.get(requested_plan, {}) if requested_plan else {}
    candidate_codes = [requested_code] if requested_code else []
    if not candidate_codes and q_text:
        for procedure_code, procedure in index.procedures.items():
            searchable = " ".join(
                str(procedure.get(key) or "")
                for key in ("code", "name", "description", "billing_code", "billing_code_type")
            ).lower()
            if q_text in searchable:
                candidate_codes.append(_normalize_code(procedure_code))

    raw_items: list[dict[str, Any]] = []
    for procedure_code in candidate_codes:
        for rate in plan_rates.get(procedure_code, []):
            provider = _provider_payload(index, rate.get("provider_ordinal") or rate.get("npi"))
            item = {**provider, **dict(rate)}
            item["procedure_code"] = procedure_code
            item["service_code"] = procedure_code
            item["service_code_system"] = item.get("billing_code_type") or "CPT"
            item["tic_prices"] = item.get("prices") or []
            item["source_trace"] = item.get("source_trace") or []
            item["confidence"] = item.get("confidence") or {
                "network": "tic_rate_npi_tin",
                "location": "nppes_practice_location",
            }
            raw_items.append(item)

    def _matches_geo(item: dict[str, Any]) -> bool:
        if state_text and str(item.get("state") or "").strip().upper() != state_text:
            return False
        if city_text and city_text not in str(item.get("city") or "").strip().lower():
            return False
        if zip_text and _normalize_zip5(item.get("zip5")) != zip_text:
            return False
        return True

    filtered = [item for item in raw_items if _matches_geo(item)]
    page_items = filtered[offset: offset + limit]
    return {
        "items": page_items,
        "pagination": {
            "total": len(filtered),
            "limit": limit,
            "offset": offset,
            "page": (offset // limit) + 1 if limit else 1,
        },
        "query": {
            "plan_id": plan_id,
            "plan_external_id": plan_external_id,
            "snapshot_id": index.snapshot_id,
            "mode": mode_value,
            "code": code or None,
            "q": q or None,
            "state": state or None,
            "city": city or None,
            "zip5": zip_text,
            "source": "ptg2",
        },
    }


async def search_current_ptg2_index(session, args: dict[str, Any], pagination) -> dict[str, Any] | None:
    snapshot_id = await resolve_current_ptg2_snapshot_id(session, args)
    if not snapshot_id:
        return None
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


def warm_cache_benchmark(index: PTG2ServingIndex, request_count: int = 200) -> dict[str, Any]:
    durations_ms: list[float] = []
    plan_ids = list(index.rates.keys())
    procedure_codes = list(index.procedures.keys())
    if not plan_ids or not procedure_codes:
        return {"request_count": 0, "p95_ms": 0.0, "passed": True}
    for idx in range(max(request_count, 1)):
        start = time.perf_counter()
        search_ptg2_index(
            index,
            plan_id=plan_ids[idx % len(plan_ids)],
            code=procedure_codes[idx % len(procedure_codes)],
            limit=25,
            offset=0,
        )
        durations_ms.append((time.perf_counter() - start) * 1000.0)
    durations_ms.sort()
    p95_index = min(max(int(len(durations_ms) * 0.95) - 1, 0), len(durations_ms) - 1)
    p95_ms = durations_ms[p95_index]
    return {
        "request_count": len(durations_ms),
        "p50_ms": durations_ms[len(durations_ms) // 2],
        "p95_ms": p95_ms,
        "p99_ms": durations_ms[min(max(int(len(durations_ms) * 0.99) - 1, 0), len(durations_ms) - 1)],
        "passed": p95_ms <= PTG2_WARM_P95_MAX_MS,
        "threshold_ms": PTG2_WARM_P95_MAX_MS,
    }
