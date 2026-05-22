# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
# pylint: disable=too-many-lines

from __future__ import annotations

import math
import os
import time
from decimal import Decimal
from typing import Any

from sqlalchemy import text

from api.ptg2_code_filters import (
    EXTERNAL_PROCEDURE_CODE_SYSTEMS,
    INFERRED_PROVIDER_TAXONOMY_RULES,
    INTERNAL_PROCEDURE_CODE_SYSTEM,
    PROCEDURE_CODE_SYSTEMS,
    PTG2_CODE_EXPANSION_HOPS,
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
    _ptg2_code_context,
    _ptg2_code_query_fields,
    _ptg2_equivalent_external_pairs,
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
from api.ptg2_response import (
    CODE_SYSTEM_ALIASES,
    PTG2_ITEM_DIAGNOSTIC_FIELDS,
    PTG2_ITEM_SOURCE_FIELDS,
    PTG2_QUERY_DIAGNOSTIC_FIELDS,
    PTG2_QUERY_SOURCE_FIELDS,
    _canonical_catalog_code,
    _canonical_price_row,
    _catalog_detail,
    _catalog_key,
    _coerce_json_payload,
    _coerce_numeric_rate,
    _include_ptg2_details,
    _include_ptg2_sources,
    _missing_modifier_detail,
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

PTG2_MODE_EXACT_SOURCE = "exact_source"
PTG2_MODE_PRODUCT_SEARCH = "product_search"
PTG2_SCHEMA = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
PTG2_WARM_P95_MAX_MS = max(float(os.getenv("HLTHPRT_PTG2_WARM_P95_MAX_MS", "50")), 1.0)
PTG2_JSON_FALLBACK_ENV = "HLTHPRT_PTG2_ENABLE_JSON_FALLBACK"
PTG2_SERVING_TABLE_ENV = "HLTHPRT_PTG2_SERVING_TABLE"
PTG2_FAST_COMPACT_COUNTS_ENV = "HLTHPRT_PTG2_FAST_COMPACT_COUNTS"


def normalize_ptg2_mode(value: str | None) -> str:
    mode = str(value or PTG2_MODE_PRODUCT_SEARCH).strip().lower()
    if mode not in {PTG2_MODE_EXACT_SOURCE, PTG2_MODE_PRODUCT_SEARCH}:
        raise ValueError("mode must be exact_source or product_search")
    return mode


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


async def _enrich_ptg2_code_details(session, payload: dict[str, Any], args: dict[str, Any]) -> dict[str, Any]:
    if not _request_bool(args.get("include_code_details")):
        return payload

    lookup_keys: set[tuple[str, str]] = set()
    items = [dict(item) for item in payload.get("items", [])]
    for item in items:
        billing_key = _catalog_key(
            item.get("reported_code_system") or item.get("billing_code_type") or item.get("service_code_system"),
            item.get("reported_code") or item.get("billing_code") or item.get("service_code"),
        )
        if billing_key:
            lookup_keys.add(billing_key)
        for price in item.get("prices") or []:
            for service_code in price.get("service_code") or []:
                service_key = _catalog_key("POS", service_code)
                if service_key:
                    lookup_keys.add(service_key)
            for modifier_code in price.get("billing_code_modifier") or []:
                modifier_key = _catalog_key("MODIFIER", modifier_code)
                if modifier_key:
                    lookup_keys.add(modifier_key)

    if not lookup_keys:
        return payload

    clauses: list[str] = []
    params: dict[str, Any] = {}
    for idx, (code_system, code) in enumerate(sorted(lookup_keys)):
        clauses.append(f"(code_system = :code_system_{idx} AND code = :code_{idx})")
        params[f"code_system_{idx}"] = code_system
        params[f"code_{idx}"] = code
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
    detail_map = {
        (str(row_data.get("code_system") or ""), str(row_data.get("code") or "")): _catalog_detail(row_data)
        for row_data in (_row_mapping(row) for row in result)
    }

    for item in items:
        billing_key = _catalog_key(
            item.get("reported_code_system") or item.get("billing_code_type") or item.get("service_code_system"),
            item.get("reported_code") or item.get("billing_code") or item.get("service_code"),
        )
        if billing_key and billing_key in detail_map:
            item["billing_code_detail"] = detail_map[billing_key]
        enriched_prices = []
        for price in item.get("prices") or []:
            price_payload = dict(price)
            service_details = []
            for service_code in price_payload.get("service_code") or []:
                detail = detail_map.get(_catalog_key("POS", service_code))
                if detail:
                    service_details.append(detail)
            if service_details:
                price_payload["service_code_details"] = service_details
            modifier_details = []
            for modifier_code in price_payload.get("billing_code_modifier") or []:
                detail = detail_map.get(_catalog_key("MODIFIER", modifier_code))
                if not detail:
                    detail = _missing_modifier_detail(modifier_code)
                if detail:
                    modifier_details.append(detail)
            if modifier_details:
                price_payload["billing_code_modifier_details"] = modifier_details
            enriched_prices.append(price_payload)
        item["prices"] = enriched_prices
        item["tic_prices"] = enriched_prices
        item["price_summary"] = _summarize_price_payload(enriched_prices)

    enriched = dict(payload)
    enriched["items"] = items
    return enriched


async def current_snapshot_id(session, requested_snapshot_id: str | None = None) -> str | None:
    if requested_snapshot_id:
        return str(requested_snapshot_id)
    result = await session.execute(
        text(f"SELECT snapshot_id FROM {PTG2_SCHEMA}.ptg2_current_snapshot WHERE slot = 'current'")
    )
    value = result.scalar()
    return str(value) if value else None


async def current_source_snapshot_id_for_plan(session, args: dict[str, Any]) -> str | None:
    requested_plan = str(args.get("plan_id") or args.get("plan_external_id") or "").strip()
    if not requested_plan:
        return None
    market_type = str(args.get("plan_market_type") or "").strip().lower()
    source_key = str(args.get("source_key") or "").strip().lower()
    params: dict[str, Any] = {"plan_id": requested_plan}
    market_sql = ""
    if market_type:
        params["plan_market_type"] = market_type
        market_sql = "AND cps.plan_market_type = :plan_market_type"
    source_sql = ""
    if source_key:
        params["source_key"] = source_key
        source_sql = "AND cps.source_key = :source_key"
    try:
        result = await session.execute(
            text(
                f"""
                SELECT cps.snapshot_id
                  FROM {PTG2_SCHEMA}.ptg2_current_plan_source cps
                  JOIN {PTG2_SCHEMA}.ptg2_snapshot s ON s.snapshot_id = cps.snapshot_id
                 WHERE cps.plan_id = :plan_id
                   {market_sql}
                   {source_sql}
                   AND s.status = 'published'
                   AND s.manifest->'serving_index'->>'table' IS NOT NULL
                 ORDER BY cps.import_month DESC NULLS LAST, cps.updated_at DESC NULLS LAST
                 LIMIT 1
                """
            ),
            params,
        )
    except Exception:
        rollback = getattr(session, "rollback", None)
        if callable(rollback):
            try:
                await rollback()
            except Exception:
                pass
        return None
    value = result.scalar()
    return str(value) if value else None


async def resolve_current_ptg2_snapshot_id(session, args: dict[str, Any]) -> str | None:
    if args.get("snapshot_id"):
        return str(args["snapshot_id"])
    source_snapshot_id = await current_source_snapshot_id_for_plan(session, args)
    if source_snapshot_id:
        return source_snapshot_id
    return await current_snapshot_id(session)


async def snapshot_artifact_uri(session, snapshot_id: str) -> str | None:
    result = await session.execute(
        text(
            f"""
            SELECT storage_uri
              FROM {PTG2_SCHEMA}.ptg2_artifact_manifest
             WHERE snapshot_id = :snapshot_id
               AND artifact_kind = :artifact_kind
             ORDER BY created_at DESC NULLS LAST
             LIMIT 1
            """
        ),
        {"snapshot_id": snapshot_id, "artifact_kind": PTG2_ARTIFACT_KIND_SNAPSHOT_INDEX},
    )
    value = result.scalar()
    return str(value) if value else None


async def load_current_ptg2_index(session, requested_snapshot_id: str | None = None) -> PTG2ServingIndex | None:
    snapshot_id = await current_snapshot_id(session, requested_snapshot_id=requested_snapshot_id)
    if not snapshot_id:
        return None
    cached = _PTG2_INDEX_CACHE.get(snapshot_id)
    if cached is not None:
        cached_at, cached_index = cached
        if PTG2_INDEX_CACHE_TTL_SECONDS == 0 or (time.monotonic() - cached_at) <= PTG2_INDEX_CACHE_TTL_SECONDS:
            return cached_index
        _PTG2_INDEX_CACHE.pop(snapshot_id, None)

    storage_uri = await snapshot_artifact_uri(session, snapshot_id)
    candidate_paths = []
    if storage_uri:
        candidate_paths.append(_path_from_uri(storage_uri))
    candidate_paths.append(_artifact_root() / PTG2_ARTIFACT_KIND_SNAPSHOT_INDEX / f"{snapshot_id}.json")

    for path in candidate_paths:
        if path.exists():
            index = load_ptg2_index_from_path(path)
            _PTG2_INDEX_CACHE[snapshot_id] = (time.monotonic(), index)
            return index
    return None


def _normalize_zip5(value: Any) -> str | None:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits[:5] if len(digits) >= 5 else None


def _provider_payload(index: PTG2ServingIndex, ordinal: Any) -> dict[str, Any]:
    provider = dict(index.providers.get(str(ordinal)) or {})
    if "provider_ordinal" not in provider:
        provider["provider_ordinal"] = ordinal
    return provider


def _row_mapping(row: Any) -> dict[str, Any]:
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return dict(mapping)
    if isinstance(row, dict):
        return dict(row)
    return dict(row)


def _price_filter_clauses(
    args: dict[str, Any],
    params: dict[str, Any],
    *,
    atom_alias: str = "pa",
    service_alias: str = "service_set",
    modifier_alias: str = "modifier_set",
) -> tuple[list[str], dict[str, Any]]:
    query_payload: dict[str, Any] = {}
    clauses: list[str] = []

    service_codes = _normalize_filter_string_list(
        args.get("pos") or args.get("place_of_service") or args.get("service_code"),
        code_system="POS",
    )
    if service_codes:
        params["price_service_codes"] = service_codes
        query_payload["service_code"] = service_codes
        query_payload["pos"] = service_codes[0] if len(service_codes) == 1 else service_codes
        clauses.append(
            f"COALESCE({service_alias}.codes, ARRAY[]::varchar[]) && CAST(:price_service_codes AS varchar[])"
        )

    modifier_codes = _normalize_filter_string_list(
        args.get("modifier") or args.get("modifiers") or args.get("billing_code_modifier"),
        upper=True,
    )
    if modifier_codes:
        params["price_modifier_codes"] = modifier_codes
        query_payload["billing_code_modifier"] = modifier_codes
        clauses.append(
            f"""
            COALESCE({modifier_alias}.codes, ARRAY[]::varchar[]) @> CAST(:price_modifier_codes AS varchar[])
            AND CAST(:price_modifier_codes AS varchar[]) @> COALESCE({modifier_alias}.codes, ARRAY[]::varchar[])
            """
        )

    requested_rate = _optional_decimal(args.get("rate") or args.get("negotiated_rate"))
    if requested_rate is not None:
        tolerance = _optional_decimal(args.get("rate_tolerance") or args.get("negotiated_rate_tolerance"))
        if tolerance is None:
            tolerance = Decimal("0.01")
        params["price_negotiated_rate"] = requested_rate
        params["price_rate_tolerance"] = tolerance
        query_payload["negotiated_rate"] = _coerce_numeric_rate(requested_rate)
        query_payload["rate_tolerance"] = _coerce_numeric_rate(tolerance)
        clauses.append(
            f"""
            {atom_alias}.negotiated_rate ~ '^-?[0-9]+(\\.[0-9]+)?$'
            AND ABS({atom_alias}.negotiated_rate::numeric - :price_negotiated_rate) <= :price_rate_tolerance
            """
        )

    return clauses, query_payload


async def _query_ptg2_code_crosswalk_edges(session, pairs: set[tuple[str, str]]) -> list[dict[str, Any]]:
    if not pairs:
        return []
    clauses = []
    params: dict[str, Any] = {}
    for idx, (system, code) in enumerate(sorted(pairs)):
        params[f"system_{idx}"] = system
        params[f"code_{idx}"] = code
        clauses.append(
            f"""
            (
                UPPER(from_system) = :system_{idx}
            AND UPPER(from_code) = :code_{idx}
            )
            """
        )
        clauses.append(
            f"""
            (
                UPPER(to_system) = :system_{idx}
            AND UPPER(to_code) = :code_{idx}
            )
            """
        )
    try:
        result = await session.execute(
            text(
                f"""
                SELECT from_system, from_code, to_system, to_code, match_type, confidence, source
                  FROM {PTG2_SCHEMA}.code_crosswalk
                 WHERE {" OR ".join(clauses)}
                """
            ),
            params,
        )
    except Exception:
        return []
    return [_row_mapping(row) for row in result]


async def _resolve_ptg2_code_search_context(
    session,
    *,
    code: Any,
    code_system: Any,
) -> dict[str, Any] | None:
    requested_code = _normalize_code(code)
    if not requested_code:
        return None
    requested_system = _normalize_code_system(code_system)
    if requested_system not in PROCEDURE_CODE_SYSTEMS:
        return None
    if requested_system == INTERNAL_PROCEDURE_CODE_SYSTEM and not _is_signed_int_text(requested_code):
        return _ptg2_code_context(
            input_system=requested_system,
            input_code=requested_code,
            resolved_pairs={(requested_system, requested_code)},
            internal_codes=set(),
        )

    resolved_pairs: set[tuple[str, str]] = {(requested_system, requested_code)}
    internal_codes: set[int] = set()
    matched_via: list[dict[str, Any]] = []
    seen_edges: set[tuple[str, str, str, str]] = set()
    if requested_system == INTERNAL_PROCEDURE_CODE_SYSTEM:
        internal_codes.add(int(requested_code))
    else:
        resolved_pairs.update(_ptg2_equivalent_external_pairs(requested_system, requested_code))

    frontier = set(resolved_pairs)
    for _ in range(PTG2_CODE_EXPANSION_HOPS):
        edges = await _query_ptg2_code_crosswalk_edges(session, frontier)
        next_frontier: set[tuple[str, str]] = set()
        for edge in edges:
            from_pair = (
                _normalize_code_system(edge.get("from_system")) or "",
                _normalize_code(edge.get("from_code")),
            )
            to_pair = (
                _normalize_code_system(edge.get("to_system")) or "",
                _normalize_code(edge.get("to_code")),
            )
            if from_pair[0] not in PROCEDURE_CODE_SYSTEMS or to_pair[0] not in PROCEDURE_CODE_SYSTEMS:
                continue
            edge_key = (*from_pair, *to_pair)
            if edge_key not in seen_edges:
                seen_edges.add(edge_key)
                matched_via.append(
                    {
                        "from_system": from_pair[0],
                        "from_code": from_pair[1],
                        "to_system": to_pair[0],
                        "to_code": to_pair[1],
                        "match_type": edge.get("match_type"),
                        "confidence": edge.get("confidence"),
                        "source": edge.get("source"),
                    }
                )
            for pair in (from_pair, to_pair):
                candidate_pairs = {pair}
                candidate_pairs.update(_ptg2_equivalent_external_pairs(pair[0], pair[1]))
                for candidate_pair in candidate_pairs:
                    if candidate_pair[0] == INTERNAL_PROCEDURE_CODE_SYSTEM and _is_signed_int_text(candidate_pair[1]):
                        internal_codes.add(int(candidate_pair[1]))
                    if candidate_pair not in resolved_pairs:
                        resolved_pairs.add(candidate_pair)
                        next_frontier.add(candidate_pair)
        if not next_frontier:
            break
        frontier = next_frontier

    return _ptg2_code_context(
        input_system=requested_system,
        input_code=requested_code,
        resolved_pairs=resolved_pairs,
        internal_codes=internal_codes,
        matched_via=matched_via,
    )


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
    inferred_taxonomy_sql = ""
    inferred_provider_taxonomy_sql = ""
    if not taxonomy_filters and provider_npi is None:
        inferred_taxonomy_sql = _inferred_provider_taxonomy_sql(args, nt_alias="nt", nucc_alias="nucc")
        inferred_provider_taxonomy_sql = _inferred_provider_taxonomy_sql(
            args,
            nt_alias="nt_filter",
            nucc_alias="nucc_filter",
        )
        if inferred_taxonomy_sql:
            taxonomy_filters.append(inferred_taxonomy_sql)
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
    schema = PTG2_SCHEMA
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
                      JOIN {schema}.nucc_taxonomy nucc_filter
                        ON nucc_filter.code = nt_filter.healthcare_provider_taxonomy_code
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
                int(getattr(pagination, "limit", 25) or 25) * 512,
                4096,
            )
            params["provider_match_limit"] = max(
                int(getattr(pagination, "offset", 0) or 0) + int(getattr(pagination, "limit", 25) or 25) * 16,
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
                                JOIN {provider_group_location_table} loc
                                  ON loc.provider_group_hash = psc.provider_group_hash
                               WHERE {location_filter_sql}
                                 AND loc.npi IS NOT NULL
                                 {"AND loc.npi = :provider_npi" if provider_npi is not None else ""}
                                 {location_taxonomy_where_sql}
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


async def search_ptg2_serving_table(
    session,
    snapshot_id: str,
    args: dict[str, Any],
    pagination,
    *,
    serving_tables: PTG2ServingTables | None = None,
) -> dict[str, Any] | None:
    mode_value = normalize_ptg2_mode(args.get("mode"))
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

    serving_tables = serving_tables or PTG2ServingTables()
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


async def search_ptg2_provider_procedures(session, npi: int, args: dict[str, Any], pagination) -> dict[str, Any] | None:
    snapshot_id = await resolve_current_ptg2_snapshot_id(session, args)
    if not snapshot_id:
        return None
    serving_tables = await snapshot_serving_tables(session, snapshot_id)
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
    if not _env_bool(PTG2_JSON_FALLBACK_ENV, False):
        return None

    index = await load_current_ptg2_index(session, requested_snapshot_id=snapshot_id)
    if index is None:
        return None
    json_payload = search_ptg2_index(
        index,
        plan_id=args.get("plan_id"),
        plan_external_id=args.get("plan_external_id"),
        code=args.get("code"),
        q=args.get("q"),
        state=args.get("state"),
        city=args.get("city"),
        zip5=args.get("zip5"),
        limit=pagination.limit,
        offset=pagination.offset,
        mode=args.get("mode"),
    )
    json_payload = await _enrich_ptg2_code_details(session, json_payload, args)
    return _ptg2_response_cache_set(cache_key, _shape_ptg2_response(json_payload, args))


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
