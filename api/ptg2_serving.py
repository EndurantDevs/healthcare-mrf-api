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
    _safe_table_name,
    _serving_table_available,
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

PTG2_MODE_EXACT_SOURCE = "exact_source"
PTG2_MODE_PRODUCT_SEARCH = "product_search"
PTG2_SCHEMA = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
PTG2_WARM_P95_MAX_MS = max(float(os.getenv("HLTHPRT_PTG2_WARM_P95_MAX_MS", "50")), 1.0)
PTG2_JSON_FALLBACK_ENV = "HLTHPRT_PTG2_ENABLE_JSON_FALLBACK"
PTG2_SERVING_TABLE_ENV = "HLTHPRT_PTG2_SERVING_TABLE"
PTG2_FAST_COMPACT_COUNTS_ENV = "HLTHPRT_PTG2_FAST_COMPACT_COUNTS"
_PTG2_MANIFEST_SIDECAR_CACHE: dict[tuple[str, str, str], tuple[str, ...]] = {}


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


def _ptg2_manifest_storage_enabled(serving_tables: PTG2ServingTables) -> bool:
    return (serving_tables.storage or "").strip().lower() == "manifest_snapshot"


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
    requested_code = _normalize_code(code)
    if not requested_code:
        return
    requested_system = _normalize_code_system(code_system)
    external_pairs: set[tuple[str, str]] = set()
    if code_context:
        for resolved_code in code_context.get("resolved_codes") or []:
            system = _normalize_code_system(resolved_code.get("code_system"))
            value = _normalize_code(resolved_code.get("code"))
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


def _ptg2_manifest_sidecar_members(serving_tables: PTG2ServingTables, name: str, owner_id: str) -> tuple[str, ...]:
    owner_id = _ptg2_manifest_id(owner_id)
    if not owner_id:
        return ()
    members: set[str] = set()
    for entry in _ptg2_manifest_artifact_entries(serving_tables, name):
        path = str(entry.get("path") or "").strip()
        if not path:
            continue
        cache_key = (path, str(entry.get("sha256") or ""), owner_id)
        cached = _PTG2_MANIFEST_SIDECAR_CACHE.get(cache_key)
        if cached is None:
            cached = tuple(member.hex() for member in lookup_global_sidecar_members(Path(path), owner_id, metadata=entry))
            _PTG2_MANIFEST_SIDECAR_CACHE[cache_key] = cached
        members.update(cached)
    return tuple(sorted(members))


def _ptg2_manifest_sidecar_members_many(
    serving_tables: PTG2ServingTables,
    name: str,
    owner_ids: list[str] | tuple[str, ...],
) -> dict[str, tuple[str, ...]]:
    owner_id_list = list(_ptg2_manifest_ids(tuple(owner_ids)))
    result_sets: dict[str, set[str]] = {owner_id: set() for owner_id in owner_id_list}
    for entry in _ptg2_manifest_artifact_entries(serving_tables, name):
        path = str(entry.get("path") or "").strip()
        if not path:
            continue
        sha = str(entry.get("sha256") or "")
        missing: list[str] = []
        for owner_id in owner_id_list:
            cache_key = (path, sha, owner_id)
            cached = _PTG2_MANIFEST_SIDECAR_CACHE.get(cache_key)
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
                _PTG2_MANIFEST_SIDECAR_CACHE[(path, sha, owner_id)] = members
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
) -> dict[str, tuple[int, ...]]:
    provider_set_ids = _ptg2_manifest_ids(tuple(provider_set_global_ids))
    members_by_set = _ptg2_manifest_sidecar_members_many(serving_tables, "provider_npi", provider_set_ids)
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


async def _ptg2_manifest_enriched_provider_rows_for_npis(
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
) -> tuple[set[str], dict[str, list[dict[str, Any]]]] | None:
    provider_group_member_table = _safe_table_name(serving_tables.provider_group_member_table)
    if not provider_group_member_table or not _ptg2_manifest_artifact_entry(serving_tables, "provider_inverted"):
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
) -> dict[str, list[dict[str, Any]]] | None:
    provider_set_ids = _ptg2_manifest_ids(tuple(provider_set_global_ids))
    if not provider_set_ids:
        return {}

    npis_by_set = _ptg2_manifest_provider_npis_for_provider_sets(serving_tables, provider_set_ids)
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
    has_provider_npi_sidecar = bool(_ptg2_manifest_artifact_entry(serving_tables, "provider_npi"))
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
        location_matches = await _ptg2_manifest_location_provider_matches(
            session,
            serving_tables,
            args,
            candidate_limit=max(int(pagination.limit) * 4, 200),
        )
        if location_matches is None:
            return None
        provider_set_ids, location_providers_by_set = location_matches
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
            )
            if provider_rows_by_set is None:
                return None
            providers_by_set = provider_rows_by_set
    procedure_details = await _ptg2_manifest_procedure_details_for_rows(session, row_data)
    for data in row_data:
        if len(items) >= int(pagination.limit):
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
        for provider in providers_by_set.get(_ptg2_manifest_id(data.get("provider_set_global_id_128")), [])[:remaining]:
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
    return _shape_ptg2_response(
        {
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
    if not _ptg2_manifest_storage_enabled(serving_tables):
        return None
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
        payload = await search_ptg2_manifest_serving_snapshot(
            snapshot_id,
            args,
            pagination,
            serving_tables=serving_tables,
            mode_value=mode_value,
        )
        return _shape_ptg2_response(payload, args) if payload is not None else None
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
    prices_by_price_set = await _ptg2_manifest_prices_for_price_sets(
        session,
        serving_tables,
        [_ptg2_manifest_id(data.get("price_set_global_id_128")) for data in row_data],
    )
    procedure_details = await _ptg2_manifest_procedure_details_for_rows(session, row_data)

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
        provider_set_hash = _ptg2_manifest_id(data.get("provider_set_global_id_128"))
        price_set_hash = _ptg2_manifest_id(data.get("price_set_global_id_128"))
        rate_pack_hash = _ptg2_manifest_id(data.get("serving_content_hash_128"))
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
