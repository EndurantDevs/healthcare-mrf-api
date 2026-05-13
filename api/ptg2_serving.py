# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json
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


def _normalize_price_payload(prices: Any) -> list[dict[str, Any]]:
    payload = _coerce_json_payload(prices, [])
    normalized: list[dict[str, Any]] = []
    if not isinstance(payload, list):
        return normalized
    for row in payload:
        if not isinstance(row, dict):
            continue
        normalized_row = dict(row)
        normalized_row["negotiated_rate"] = _coerce_numeric_rate(normalized_row.get("negotiated_rate"))
        normalized.append(normalized_row)
    return normalized


async def _serving_table_available(session, table_name: str) -> bool:
    try:
        result = await session.execute(
            text("SELECT to_regclass(:table_name)"),
            {"table_name": table_name},
        )
        return bool(result.scalar())
    except Exception:
        return False


def _serving_table_name() -> str:
    configured = str(os.getenv(PTG2_SERVING_TABLE_ENV) or "ptg2_serving_rate").strip()
    if "." in configured:
        return configured
    return f"{PTG2_SCHEMA}.{configured}"


def _serving_table_candidates() -> list[str]:
    primary = _serving_table_name()
    stage = f"{PTG2_SCHEMA}.ptg2_serving_rate_stage"
    candidates = [primary]
    if stage not in candidates:
        candidates.append(stage)
    return candidates


async def snapshot_serving_table(session, snapshot_id: str) -> str | None:
    result = await session.execute(
        text(
            f"""
            SELECT manifest->'serving_index'->>'table'
              FROM {PTG2_SCHEMA}.ptg2_snapshot
             WHERE snapshot_id = :snapshot_id
             LIMIT 1
            """
        ),
        {"snapshot_id": snapshot_id},
    )
    value = result.scalar()
    return str(value).strip() if value else None


def _ordered_serving_table_candidates(preferred_table: str | None = None) -> list[str]:
    candidates: list[str] = []
    if preferred_table:
        candidates.append(preferred_table if "." in preferred_table else f"{PTG2_SCHEMA}.{preferred_table}")
    for candidate in _serving_table_candidates():
        if candidate not in candidates:
            candidates.append(candidate)
    return candidates


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


async def _search_compact_serving_table(
    session,
    table_name: str,
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
    compact_price_jsonb = _typed_price_json_sql("ps", json_type="jsonb")
    compact_price_json = _typed_price_json_sql("ps", json_type="json")
    has_geo = bool(geo_filters)
    if has_geo:
        geo_sql = " AND ".join(geo_filters)
        total = None
        if not _env_bool(PTG2_FAST_COMPACT_COUNTS_ENV, True):
            count_result = await session.execute(
                text(
                    f"""
                    SELECT COUNT(*)
                    FROM (
                        SELECT DISTINCT pgm.npi, pl.location_hash
                        FROM {table_name} r
                        JOIN {schema}.ptg2_procedure proc ON proc.procedure_hash = r.procedure_hash
                        JOIN {schema}.ptg2_provider_set_component psc ON psc.provider_set_hash = r.provider_set_hash
                        JOIN {schema}.ptg2_provider_group_member pgm ON pgm.provider_group_hash = psc.provider_group_hash
                        JOIN {schema}.ptg2_provider_location pl ON pl.npi = pgm.npi
                        WHERE {where_sql}
                          AND {geo_sql}
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
                    pl.location_hash,
                    pl.state,
                    pl.city,
                    pl.zip5,
                    pl.location_source,
                    pl.confidence_code AS location_confidence_code,
                    MAX(pl.address_payload::text) AS address_payload,
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
                JOIN {schema}.ptg2_procedure proc ON proc.procedure_hash = r.procedure_hash
                JOIN {schema}.ptg2_provider_set_component psc ON psc.provider_set_hash = r.provider_set_hash
                JOIN {schema}.ptg2_provider_group_member pgm ON pgm.provider_group_hash = psc.provider_group_hash
                JOIN {schema}.ptg2_provider_location pl ON pl.npi = pgm.npi
                LEFT JOIN {schema}.npi n ON n.npi = pgm.npi
                LEFT JOIN {schema}.ptg2_price_set ps ON ps.price_set_hash = r.price_set_hash
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
                  AND {geo_sql}
                  {q_filter}
                GROUP BY
                    pgm.npi,
                    pl.location_hash,
                    pl.state,
                    pl.city,
                    pl.zip5,
                    pl.location_source,
                    pl.confidence_code,
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
                    "prices": _normalize_price_payload(data.get("prices")),
                    "tic_prices": _normalize_price_payload(data.get("prices")),
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
                    JOIN {schema}.ptg2_procedure proc ON proc.procedure_hash = r.procedure_hash
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
                JOIN {schema}.ptg2_procedure proc ON proc.procedure_hash = r.procedure_hash
                LEFT JOIN {schema}.ptg2_price_set ps ON ps.price_set_hash = r.price_set_hash
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
                    "prices": prices,
                    "tic_prices": prices,
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
            "snapshot_id": snapshot_id,
            "mode": mode_value,
            "code": args.get("code") or None,
            "code_system": args.get("code_system") or None,
            "q": args.get("q") or None,
            "state": state_text or None,
            "city": city_text or None,
            "zip5": zip_text,
            "source": "ptg2_db_compact",
            "serving_table": table_name,
            "result_granularity": "provider" if has_geo else "provider_set",
            "procedure_consolidation": "HP_PROCEDURE_CODE",
        },
    }


async def search_ptg2_serving_table(
    session,
    snapshot_id: str,
    args: dict[str, Any],
    pagination,
    *,
    preferred_table: str | None = None,
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
    price_set_table = f"{PTG2_SCHEMA}.ptg2_price_set"
    source_trace_set_table = f"{PTG2_SCHEMA}.ptg2_source_trace_set"
    source_trace_table = f"{PTG2_SCHEMA}.ptg2_source_trace"
    total = 0
    row_result = None
    table_name = None
    for candidate in _ordered_serving_table_candidates(preferred_table):
        if not await _serving_table_available(session, candidate):
            continue
        if candidate.endswith(".ptg2_serving_rate_compact"):
            compact_payload = await _search_compact_serving_table(
                session,
                candidate,
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
            "prices": prices,
            "tic_prices": prices,
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
    snapshot_id = await current_snapshot_id(session, requested_snapshot_id=args.get("snapshot_id"))
    if not snapshot_id:
        return None
    preferred_table = await snapshot_serving_table(session, snapshot_id)
    db_payload = await search_ptg2_serving_table(
        session,
        snapshot_id,
        args,
        pagination,
        preferred_table=preferred_table,
    )
    if db_payload is not None:
        return db_payload
    if not _env_bool(PTG2_JSON_FALLBACK_ENV, False):
        return None

    index = await load_current_ptg2_index(session, requested_snapshot_id=snapshot_id)
    if index is None:
        return None
    return search_ptg2_index(
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
