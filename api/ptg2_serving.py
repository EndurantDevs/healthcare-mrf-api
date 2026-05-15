# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json
import math
import os
import re
import tempfile
import time
from decimal import Decimal, InvalidOperation
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

from sqlalchemy import text


PTG2_ARTIFACT_KIND_SNAPSHOT_INDEX = "snapshot_index"
PTG2_MODE_EXACT_SOURCE = "exact_source"
PTG2_MODE_PRODUCT_SEARCH = "product_search"
PTG2_SCHEMA = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
PTG2_INDEX_CACHE_TTL_SECONDS = max(float(os.getenv("HLTHPRT_PTG2_INDEX_CACHE_TTL_SECONDS", "300")), 0.0)
PTG2_WARM_P95_MAX_MS = max(float(os.getenv("HLTHPRT_PTG2_WARM_P95_MAX_MS", "50")), 1.0)
PTG2_JSON_FALLBACK_ENV = "HLTHPRT_PTG2_ENABLE_JSON_FALLBACK"
PTG2_SERVING_TABLE_ENV = "HLTHPRT_PTG2_SERVING_TABLE"
PTG2_FAST_COMPACT_COUNTS_ENV = "HLTHPRT_PTG2_FAST_COMPACT_COUNTS"
INTERNAL_PROCEDURE_CODE_SYSTEM = "HP_PROCEDURE_CODE"
NUMERIC_PATTERN = re.compile(r"^-?\d+(\.\d+)?$")

_PTG2_INDEX_CACHE: dict[str, tuple[float, "PTG2ServingIndex"]] = {}
PTG2_ITEM_SOURCE_FIELDS = {"source_trace"}
PTG2_ITEM_DIAGNOSTIC_FIELDS = {
    "confidence",
    "location_source",
    "price_set_hash",
    "provider_ordinal",
    "provider_set_hash",
    "provider_set_hashes",
    "rate_pack_hash",
}
PTG2_QUERY_SOURCE_FIELDS = {"source", "serving_table"}
PTG2_QUERY_DIAGNOSTIC_FIELDS = {"procedure_consolidation", "result_granularity"}
CODE_SYSTEM_ALIASES = {
    "CLM_REV_CNTR_CD": "RC",
    "PLACE_OF_SERVICE": "POS",
    "REVENUE_CENTER": "RC",
    "REVENUE_CODE": "RC",
    "REV_CNTR": "RC",
    "SERVICE_CODE": "POS",
    "BILLING_CODE_MODIFIER": "MODIFIER",
    "CPT_MODIFIER": "MODIFIER",
    "HCPCS_MODIFIER": "MODIFIER",
    "MOD": "MODIFIER",
}


@dataclass(frozen=True)
class PTG2ServingIndex:
    snapshot_id: str
    version: int
    plans: dict[str, Any]
    procedures: dict[str, Any]
    providers: dict[str, Any]
    rates: dict[str, Any]
    source_uri: str | None = None

    @classmethod
    def from_payload(cls, payload: dict[str, Any], source_uri: str | None = None) -> "PTG2ServingIndex":
        return cls(
            snapshot_id=str(payload.get("snapshot_id") or ""),
            version=int(payload.get("version") or 1),
            plans=dict(payload.get("plans") or {}),
            procedures=dict(payload.get("procedures") or {}),
            providers={str(k): v for k, v in dict(payload.get("providers") or {}).items()},
            rates=dict(payload.get("rates") or {}),
            source_uri=source_uri,
        )


@dataclass(frozen=True)
class PTG2ServingTables:
    serving_table: str | None = None
    price_table: str | None = None
    procedure_table: str | None = None
    provider_set_table: str | None = None
    provider_group_member_table: str | None = None


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


def _request_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (list, tuple)):
        value = value[-1] if value else None
        if value is None:
            return default
    text_value = str(value).strip().lower()
    if text_value in {"1", "true", "yes", "on"}:
        return True
    if text_value in {"0", "false", "no", "off"}:
        return False
    return default


def _optional_float(value: Any) -> float | None:
    if value in (None, "", "null"):
        return None
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _include_ptg2_details(args: dict[str, Any]) -> bool:
    return _request_bool(args.get("include_details")) or _request_bool(args.get("include_debug"))


def _include_ptg2_sources(args: dict[str, Any]) -> bool:
    return _include_ptg2_details(args) or _request_bool(args.get("include_sources"))


def _shape_ptg2_response(payload: dict[str, Any], args: dict[str, Any]) -> dict[str, Any]:
    """Keep app-facing PTG responses lean unless callers opt into provenance/debug fields."""
    if _include_ptg2_details(args):
        return payload

    include_sources = _include_ptg2_sources(args)
    hidden_item_fields = set(PTG2_ITEM_DIAGNOSTIC_FIELDS)
    if not include_sources:
        hidden_item_fields.update(PTG2_ITEM_SOURCE_FIELDS)
    hidden_query_fields = set(PTG2_QUERY_DIAGNOSTIC_FIELDS)
    if not include_sources:
        hidden_query_fields.update(PTG2_QUERY_SOURCE_FIELDS)

    shaped = dict(payload)
    shaped["items"] = [
        {key: value for key, value in dict(item).items() if key not in hidden_item_fields}
        for item in payload.get("items", [])
    ]
    shaped["query"] = {
        key: value for key, value in dict(payload.get("query") or {}).items() if key not in hidden_query_fields
    }
    return shaped


def _normalize_catalog_code_system(raw_system: Any) -> str:
    system = str(raw_system or "").strip().upper()
    return CODE_SYSTEM_ALIASES.get(system, system)


def _canonical_catalog_code(code_system: str, raw_code: Any) -> str:
    code = str(raw_code or "").strip().upper()
    digits = "".join(ch for ch in code if ch.isdigit())
    if code_system == "RC" and digits:
        return digits.zfill(4)
    if code_system == "POS" and digits:
        return digits.zfill(2)
    return code


def _catalog_key(code_system: Any, code: Any) -> tuple[str, str] | None:
    normalized_system = _normalize_catalog_code_system(code_system)
    normalized_code = _canonical_catalog_code(normalized_system, code)
    if not normalized_system or not normalized_code:
        return None
    return normalized_system, normalized_code


def _catalog_detail(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "code_system": row.get("code_system"),
        "code": row.get("code"),
        "display_name": row.get("display_name"),
        "short_description": row.get("short_description"),
    }


def _missing_modifier_detail(modifier_code: Any) -> dict[str, Any] | None:
    modifier_key = _catalog_key("MODIFIER", modifier_code)
    if not modifier_key:
        return None
    _, normalized_code = modifier_key
    return {
        "code_system": "MODIFIER",
        "code": normalized_code,
        "display_name": f"Modifier {normalized_code}",
        "short_description": None,
        "catalog_status": "missing",
    }


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


def clear_ptg2_index_cache() -> None:
    _PTG2_INDEX_CACHE.clear()


def _typed_price_json_sql(alias: str = "ps", *, json_type: str = "json") -> str:
    is_jsonb = json_type == "jsonb"
    build_array = "jsonb_build_array" if is_jsonb else "json_build_array"
    build_object = "jsonb_build_object" if is_jsonb else "json_build_object"
    empty_array = "'[]'::jsonb" if is_jsonb else "CAST('[]' AS json)"
    canonical_payload = f"{alias}.canonical_payload::jsonb" if is_jsonb else f"{alias}.canonical_payload"
    to_json_func = "to_jsonb" if is_jsonb else "to_json"
    return f"""
        COALESCE(
            {canonical_payload},
            CASE
                WHEN {alias}.negotiated_rate IS NULL THEN {empty_array}
                ELSE {build_array}(
                    {build_object}(
                        'negotiated_type', {alias}.negotiated_type,
                        'negotiated_rate', {alias}.negotiated_rate,
                        'expiration_date', {alias}.expiration_date,
                        'service_code', COALESCE({to_json_func}({alias}.service_code), {empty_array}),
                        'billing_class', {alias}.billing_class,
                        'setting', {alias}.setting,
                        'billing_code_modifier', COALESCE({to_json_func}({alias}.billing_code_modifier), {empty_array}),
                        'additional_information', {alias}.additional_information
                    )
                )
            END
        )
    """


def _artifact_root() -> Path:
    configured = os.getenv("HLTHPRT_PTG2_ARTIFACT_DIR")
    return Path(configured) if configured else Path(tempfile.gettempdir()) / "healthporta-ptg2-artifacts"


def _path_from_uri(uri: str) -> Path:
    if uri.startswith("file://"):
        return Path(unquote(urlsplit(uri).path))
    return Path(uri)


def load_ptg2_index_from_path(path: str | Path) -> PTG2ServingIndex:
    artifact_path = Path(path)
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    return PTG2ServingIndex.from_payload(payload, source_uri=artifact_path.resolve().as_uri())


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


def _normalize_code(value: Any) -> str:
    return str(value or "").strip().upper()


def _normalize_code_system(value: Any) -> str | None:
    text = str(value or "").strip().upper()
    return text or None


def _is_signed_int_text(value: str) -> bool:
    return value.lstrip("-").isdigit() and value not in {"", "-"}


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


def _coerce_json_payload(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
    return value


def _coerce_numeric_rate(value: Any) -> Any:
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, str):
        text_value = value.strip()
        if not text_value or not NUMERIC_PATTERN.fullmatch(text_value):
            return value
        try:
            decimal_value = Decimal(text_value)
        except InvalidOperation:
            return value
        return int(decimal_value) if decimal_value == decimal_value.to_integral_value() else float(decimal_value)
    return value


def _canonical_price_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized_row = dict(row)
    normalized_row["negotiated_rate"] = _coerce_numeric_rate(normalized_row.get("negotiated_rate"))
    if "service_code" in normalized_row:
        normalized_row["service_code"] = sorted(
            {_canonical_catalog_code("POS", code) for code in _normalize_string_list(normalized_row.get("service_code"))}
        )
    if "billing_code_modifier" in normalized_row:
        normalized_row["billing_code_modifier"] = sorted(
            {modifier.upper() for modifier in _normalize_string_list(normalized_row.get("billing_code_modifier"))}
        )
    return normalized_row


def _price_row_key(row: dict[str, Any]) -> str:
    return json.dumps(row, sort_keys=True, separators=(",", ":"), default=str)


def _normalize_price_payload(prices: Any) -> list[dict[str, Any]]:
    payload = _coerce_json_payload(prices, [])
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    if not isinstance(payload, list):
        return normalized
    for row in payload:
        if not isinstance(row, dict):
            continue
        normalized_row = _canonical_price_row(row)
        key = _price_row_key(normalized_row)
        if key in seen:
            continue
        seen.add(key)
        normalized.append(normalized_row)
    return normalized


def _normalize_string_list(value: Any) -> list[str]:
    if value in (None, "", "null"):
        return []
    if isinstance(value, str):
        payload = _coerce_json_payload(value, value)
        if isinstance(payload, list):
            value = payload
        else:
            return [value]
    if not isinstance(value, (list, tuple, set)):
        return [str(value)]
    return [str(item) for item in value if item not in (None, "", "null")]


def _price_component(modifiers: list[str]) -> str:
    normalized = {modifier.upper() for modifier in modifiers}
    if not normalized:
        return "global"
    if normalized == {"26"}:
        return "professional"
    if normalized == {"TC"}:
        return "technical"
    return "modifier"


def _summarize_price_payload(prices: Any) -> list[dict[str, Any]]:
    normalized_prices = _normalize_price_payload(prices)
    grouped: dict[tuple[Any, ...], dict[str, Any]] = {}
    for price in normalized_prices:
        modifiers = sorted({modifier.upper() for modifier in _normalize_string_list(price.get("billing_code_modifier"))})
        service_codes = sorted({_canonical_catalog_code("POS", code) for code in _normalize_string_list(price.get("service_code"))})
        rate = _coerce_numeric_rate(price.get("negotiated_rate"))
        key = (
            _price_component(modifiers),
            tuple(modifiers),
            rate,
            price.get("negotiated_type"),
            price.get("billing_class"),
            price.get("setting"),
        )
        summary = grouped.setdefault(
            key,
            {
                "component": key[0],
                "modifier": list(modifiers),
                "rate": rate,
                "negotiated_type": price.get("negotiated_type"),
                "billing_class": price.get("billing_class"),
                "setting": price.get("setting"),
                "service_code": [],
                "raw_price_count": 0,
            },
        )
        summary["raw_price_count"] += 1
        summary["service_code"] = sorted(set(summary["service_code"]) | set(service_codes))

    component_order = {"global": 0, "professional": 1, "technical": 2, "modifier": 3}
    return sorted(
        grouped.values(),
        key=lambda item: (
            component_order.get(str(item.get("component")), 99),
            str(item.get("billing_class") or ""),
            str(item.get("setting") or ""),
            str(item.get("modifier") or ""),
            float(item.get("rate")) if isinstance(item.get("rate"), (int, float)) else 0.0,
        ),
    )


def _price_response_fields(prices: Any) -> dict[str, list[dict[str, Any]]]:
    normalized = _normalize_price_payload(prices)
    return {
        "prices": normalized,
        "tic_prices": normalized,
        "price_summary": _summarize_price_payload(normalized),
    }


async def _serving_table_available(session, table_name: str) -> bool:
    try:
        result = await session.execute(
            text("SELECT to_regclass(:table_name)"),
            {"table_name": table_name},
        )
        return bool(result.scalar())
    except Exception:
        return False


async def _index_available(session, index_name: str) -> bool:
    try:
        result = await session.execute(
            text("SELECT to_regclass(:index_name)"),
            {"index_name": index_name},
        )
        return bool(result.scalar())
    except Exception:
        return False


def _serving_table_name() -> str:
    configured = str(os.getenv(PTG2_SERVING_TABLE_ENV) or "ptg2_serving_rate").strip()
    if "." in configured:
        return configured
    return f"{PTG2_SCHEMA}.{configured}"


def _safe_table_name(value: Any, *, default_schema: str = PTG2_SCHEMA) -> str | None:
    if not value:
        return None
    text_value = str(value).strip()
    if not text_value:
        return None
    parts = text_value.split(".", 1)
    if len(parts) == 1:
        schema_name = default_schema
        table_name = parts[0]
    else:
        schema_name, table_name = parts
    ident_re = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
    if not ident_re.fullmatch(schema_name) or not ident_re.fullmatch(table_name):
        return None
    return f"{schema_name}.{table_name}"


def _serving_table_candidates() -> list[str]:
    primary = _safe_table_name(_serving_table_name()) or f"{PTG2_SCHEMA}.ptg2_serving_rate"
    stage = f"{PTG2_SCHEMA}.ptg2_serving_rate_stage"
    candidates = [primary]
    if stage not in candidates:
        candidates.append(stage)
    return candidates


async def snapshot_serving_table(session, snapshot_id: str) -> str | None:
    tables = await snapshot_serving_tables(session, snapshot_id)
    return tables.serving_table


async def snapshot_serving_tables(session, snapshot_id: str) -> PTG2ServingTables:
    result = await session.execute(
        text(
            f"""
            SELECT manifest->'serving_index'
              FROM {PTG2_SCHEMA}.ptg2_snapshot
             WHERE snapshot_id = :snapshot_id
             LIMIT 1
            """
        ),
        {"snapshot_id": snapshot_id},
    )
    value = result.scalar()
    if not value:
        return PTG2ServingTables()
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return PTG2ServingTables()
    if not isinstance(value, dict):
        return PTG2ServingTables()
    return PTG2ServingTables(
        serving_table=_safe_table_name(value.get("table")),
        price_table=_safe_table_name(value.get("price_table")) or f"{PTG2_SCHEMA}.ptg2_price_set",
        procedure_table=_safe_table_name(value.get("procedure_table")) or f"{PTG2_SCHEMA}.ptg2_procedure",
        provider_set_table=_safe_table_name(value.get("provider_set_table")),
        provider_group_member_table=_safe_table_name(value.get("provider_group_member_table")),
    )


def _ordered_serving_table_candidates(preferred_table: str | None = None) -> list[str]:
    candidates: list[str] = []
    if preferred_table:
        safe_preferred = _safe_table_name(preferred_table)
        if safe_preferred:
            candidates.append(safe_preferred)
    for candidate in _serving_table_candidates():
        if candidate not in candidates:
            candidates.append(candidate)
    return candidates


def _is_compact_serving_table(table_name: str) -> bool:
    return table_name.rsplit(".", 1)[-1].startswith("ptg2_serving_rate_compact")


def _append_code_filter(filters: list[str], params: dict[str, Any], *, code: Any, code_system: Any) -> None:
    requested_code = _normalize_code(code)
    if not requested_code:
        return
    requested_system = _normalize_code_system(code_system)
    params["reported_code"] = requested_code
    if requested_system == INTERNAL_PROCEDURE_CODE_SYSTEM:
        if _is_signed_int_text(requested_code):
            filters.append("procedure_code = :procedure_code")
            params["procedure_code"] = int(requested_code)
        else:
            filters.append("FALSE")
        return
    if requested_system:
        filters.append(
            """
            (
                reported_code_system = :reported_code_system
            AND reported_code = :reported_code
            )
            """
        )
        params["reported_code_system"] = requested_system
        return

    code_clauses = [
        "reported_code = :reported_code",
        "billing_code = :reported_code",
    ]
    if _is_signed_int_text(requested_code) and (requested_code.startswith("-") or len(requested_code) > 5):
        code_clauses.append("procedure_code = :procedure_code")
        params["procedure_code"] = int(requested_code)
    filters.append("(" + " OR ".join(code_clauses) + ")")


def _qualify_compact_filters(filters: list[str]) -> list[str]:
    qualified = []
    replacements = {
        "reported_code_system": "r.reported_code_system",
        "procedure_code": "r.procedure_code",
        "reported_code": "r.reported_code",
        "billing_code": "r.billing_code",
        "snapshot_id": "r.snapshot_id",
        "plan_id": "r.plan_id",
    }
    for value in filters:
        text_value = value
        for source, target in replacements.items():
            text_value = re.sub(rf"(?<![:.])\b{re.escape(source)}\b", target, text_value)
        qualified.append(text_value)
    return qualified


def _normalize_taxonomy_code(value: Any) -> str | None:
    text_value = str(value or "").strip().upper()
    if not text_value:
        return None
    if not re.fullmatch(r"[A-Z0-9X]{10}", text_value):
        return None
    return text_value


def _normalize_npi(value: Any) -> int | None:
    text_value = str(value or "").strip()
    if not text_value or not text_value.isdigit():
        return None
    parsed = int(text_value)
    return parsed if parsed > 0 else None


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
    q_filter = ""
    if q_text:
        q_filter = """
          AND (
                LOWER(COALESCE(proc.name, '')) LIKE :q_like
             OR LOWER(COALESCE(proc.description, '')) LIKE :q_like
             OR LOWER(COALESCE(r.billing_code, '')) LIKE :q_like
             OR LOWER(COALESCE(r.reported_code, '')) LIKE :q_like
          )
        """
        params["q_like"] = f"%{q_text}%"
    where_sql = " AND ".join(_qualify_compact_filters(filters))
    schema = PTG2_SCHEMA
    procedure_table = serving_tables.procedure_table or f"{schema}.ptg2_procedure"
    price_set_table = serving_tables.price_table or f"{schema}.ptg2_price_set"
    provider_set_table = serving_tables.provider_set_table or f"{schema}.ptg2_provider_set"
    provider_group_member_table = serving_tables.provider_group_member_table or f"{schema}.ptg2_provider_group_member"
    use_snapshot_provider_tables = bool(serving_tables.provider_set_table and serving_tables.provider_group_member_table)
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
        LEFT JOIN LATERAL (
            SELECT ps.*
              FROM {price_set_table} ps
             WHERE ps.price_set_hash = r.price_set_hash
             LIMIT 1
        ) ps ON TRUE
    """
    if use_snapshot_provider_tables:
        provider_join_sql = f"""
            JOIN LATERAL (
                SELECT provider_set.*
                  FROM {provider_set_table} provider_set
                 WHERE provider_set.provider_set_hash = r.provider_set_hash
                 LIMIT 1
            ) provider_set ON TRUE
            JOIN LATERAL jsonb_array_elements_text(
                COALESCE(
                    to_jsonb(provider_set.provider_group_hashes),
                    provider_set.canonical_payload::jsonb->'provider_group_hashes',
                    '[]'::jsonb
                )
            ) AS provider_group_ref(provider_group_hash) ON TRUE
            JOIN {provider_group_member_table} pgm
              ON pgm.provider_group_hash = provider_group_ref.provider_group_hash::bigint
        """
    else:
        provider_join_sql = f"""
            JOIN {schema}.ptg2_provider_set_component psc ON psc.provider_set_hash = r.provider_set_hash
            JOIN {schema}.ptg2_provider_group_member pgm ON pgm.provider_group_hash = psc.provider_group_hash
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
            )
            if filter_sql
        )
        if use_snapshot_provider_tables:
            provider_npi_where_sql = "WHERE pgm_filter.npi = :provider_npi" if provider_npi else ""
            provider_npi_join_sql = f"""
                JOIN LATERAL (
                    SELECT pgm_filter.npi
                      FROM jsonb_array_elements_text(
                            COALESCE(
                                to_jsonb(provider_filter_set.provider_group_hashes),
                                provider_filter_set.canonical_payload::jsonb->'provider_group_hashes',
                                '[]'::jsonb
                            )
                      ) AS provider_group_filter_ref(provider_group_hash)
                      JOIN {provider_group_member_table} pgm_filter
                        ON pgm_filter.provider_group_hash = provider_group_filter_ref.provider_group_hash::bigint
                     {provider_npi_where_sql}
                     OFFSET 0
                ) provider_filter_npi ON TRUE
            """
        else:
            provider_npi_where_sql = "AND pgm_filter.npi = :provider_npi" if provider_npi else ""
            provider_npi_join_sql = f"""
                JOIN LATERAL (
                    SELECT pgm_filter.npi
                      FROM {schema}.ptg2_provider_set_component psc_filter
                      JOIN {schema}.ptg2_provider_group_member pgm_filter
                        ON pgm_filter.provider_group_hash = psc_filter.provider_group_hash
                     WHERE psc_filter.provider_set_hash = provider_filter_set.provider_set_hash
                       {provider_npi_where_sql}
                     OFFSET 0
                ) provider_filter_npi ON TRUE
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
                  FROM {provider_set_table} provider_filter_set
                  {provider_npi_join_sql}
                  {provider_geo_match_sql}
                  {provider_taxonomy_match_sql}
                 WHERE provider_filter_set.provider_set_hash = r.provider_set_hash
                 LIMIT 1
          )
        """
    compact_price_jsonb = _typed_price_json_sql("ps", json_type="jsonb")
    compact_price_json = _typed_price_json_sql("ps", json_type="json")
    has_provider_filters = expand_providers
    if has_provider_filters:
        geo_sql = " AND ".join(geo_filters)
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
        if coordinate_filter_requested and use_snapshot_provider_tables and not q_text:
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
                            r.billing_code,
                            r.billing_code_type,
                            r.price_set_hash,
                            r.rate_pack_hash,
                            r.source_trace_set_hash
                          FROM {table_name} r
                         WHERE {where_sql}
                    ),
                    candidate_set_groups AS MATERIALIZED (
                        SELECT DISTINCT
                            ps.provider_set_hash,
                            group_ref.provider_group_hash::bigint AS provider_group_hash
                          FROM rate_candidates r
                          JOIN {provider_set_table} ps
                            ON ps.provider_set_hash = r.provider_set_hash
                          JOIN LATERAL unnest(
                                COALESCE(ps.provider_group_hashes, ARRAY[]::bigint[])
                          ) AS group_ref(provider_group_hash) ON TRUE
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
                        r.billing_code,
                        r.billing_code_type,
                        COALESCE(proc.name, proc.description, r.billing_code) AS procedure_display_name,
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
                        r.billing_code,
                        r.billing_code_type,
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
                    r.billing_code,
                    r.billing_code_type,
                    COALESCE(proc.name, proc.description, r.billing_code) AS procedure_display_name,
                    proc.name AS procedure_name,
                    proc.description AS procedure_description,
                    array_agg(DISTINCT r.provider_set_hash ORDER BY r.provider_set_hash) AS provider_set_hashes,
                    COUNT(DISTINCT r.serving_rate_id)::int AS rate_count,
                    COALESCE(jsonb_agg(DISTINCT price_item.price_item) FILTER (WHERE price_item.price_item IS NOT NULL), '[]'::jsonb) AS prices,
                    COALESCE(jsonb_agg(DISTINCT trace_item.trace_item) FILTER (WHERE trace_item.trace_item IS NOT NULL), '[]'::jsonb) AS source_trace
                FROM {table_name} r
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
                WHERE {where_sql}
                  {"AND " + expansion_geo_sql if expansion_geo_sql else ""}
                  {"AND " + taxonomy_sql if taxonomy_filters else ""}
                  {q_filter}
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
                    r.billing_code,
                    r.billing_code_type,
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
        row_result = await session.execute(
            text(
                f"""
                SELECT
                    r.serving_rate_id,
                    r.provider_set_hash,
                    r.provider_count,
                    r.procedure_code,
                    r.reported_code_system,
                    r.reported_code,
                    r.billing_code,
                    r.billing_code_type,
                    r.price_set_hash,
                    r.rate_pack_hash,
                    COALESCE(proc.name, proc.description, r.billing_code) AS procedure_display_name,
                    proc.name AS procedure_name,
                    proc.description AS procedure_description,
                    {compact_price_json} AS prices,
                    COALESCE(trace_payload.source_trace, CAST('[]' AS json)) AS source_trace
                FROM {table_name} r
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
                WHERE {where_sql}
                {q_filter}
                {provider_filter_sql}
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
    _append_code_filter(filters, params, code=args.get("code"), code_system=args.get("code_system"))
    if q_text:
        params["q_like"] = f"%{q_text}%"

    where_sql = " AND ".join(filters)
    serving_tables = serving_tables or PTG2ServingTables()
    preferred_table = serving_tables.serving_table
    price_set_table = serving_tables.price_table or f"{PTG2_SCHEMA}.ptg2_price_set"
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
    provider_set_table = serving_tables.provider_set_table or f"{schema}.ptg2_provider_set"
    provider_group_member_table = serving_tables.provider_group_member_table or f"{schema}.ptg2_provider_group_member"
    procedure_table = serving_tables.procedure_table or f"{schema}.ptg2_procedure"
    price_set_table = serving_tables.price_table or f"{schema}.ptg2_price_set"
    provider_group_hashes_index = f"{provider_set_table}_group_hashes_gin_idx"
    has_reverse_provider_index = await _index_available(session, provider_group_hashes_index)

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
    if market_type:
        filters.append("r.plan_market_type = :plan_market_type")
        params["plan_market_type"] = market_type

    _append_code_filter(filters, params, code=code_value, code_system=args.get("code_system"))
    q_filter = ""
    if q_text:
        q_filter = """
          AND (
                LOWER(COALESCE(proc.name, '')) LIKE :q_like
             OR LOWER(COALESCE(proc.description, '')) LIKE :q_like
             OR LOWER(COALESCE(r.billing_code, '')) LIKE :q_like
             OR LOWER(COALESCE(r.reported_code, '')) LIKE :q_like
          )
        """
        params["q_like"] = f"%{q_text}%"
    where_sql = " AND ".join(_qualify_compact_filters(filters))
    compact_price_json = _typed_price_json_sql("ps", json_type="json")

    count_result = await session.execute(
        text(
            f"""
            WITH provider_groups AS MATERIALIZED (
                SELECT provider_group_hash
                  FROM {provider_group_member_table}
                 WHERE npi = :provider_npi
            ),
            provider_sets AS MATERIALIZED (
                SELECT DISTINCT ps.provider_set_hash
                  FROM provider_groups pg
                  JOIN {provider_set_table} ps
                    ON ps.provider_group_hashes @> ARRAY[pg.provider_group_hash]::bigint[]
            )
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
        return None

    row_result = await session.execute(
        text(
            f"""
            WITH provider_groups AS MATERIALIZED (
                SELECT provider_group_hash
                  FROM {provider_group_member_table}
                 WHERE npi = :provider_npi
            ),
            provider_sets AS MATERIALIZED (
                SELECT DISTINCT ps.provider_set_hash
                  FROM provider_groups pg
                  JOIN {provider_set_table} ps
                    ON ps.provider_group_hashes @> ARRAY[pg.provider_group_hash]::bigint[]
            )
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
                proc.name AS procedure_name,
                proc.description AS procedure_description,
                r.provider_set_hash,
                r.provider_count,
                r.provider_set_count,
                r.price_set_hash,
                COALESCE({compact_price_json}, CAST('[]' AS json)) AS prices
              FROM provider_sets provider_set_match
              JOIN {table_name} r
                ON r.provider_set_hash = provider_set_match.provider_set_hash
              JOIN LATERAL (
                    SELECT proc.*
                      FROM {procedure_table} proc
                     WHERE proc.procedure_hash = r.procedure_hash
                     LIMIT 1
              ) proc ON TRUE
              LEFT JOIN LATERAL (
                    SELECT ps.*
                      FROM {price_set_table} ps
                     WHERE ps.price_set_hash = r.price_set_hash
                     LIMIT 1
              ) ps ON TRUE
             WHERE {where_sql}
             {q_filter}
             ORDER BY r.reported_code_system, r.reported_code, r.provider_count DESC NULLS LAST, r.serving_rate_id
             LIMIT :limit OFFSET :offset
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
                "source": "ptg2_db",
                "serving_table": table_name,
                "provider_reverse_index": has_reverse_provider_index,
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
        return _shape_ptg2_response(db_payload, args)
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
    return _shape_ptg2_response(json_payload, args)


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
