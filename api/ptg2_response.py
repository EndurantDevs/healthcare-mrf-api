# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PTG2 response, catalog-code, and price-payload helpers."""

from __future__ import annotations

import json
import re
from decimal import Decimal, InvalidOperation
from typing import Any

import orjson

from api.code_systems import canonical_catalog_code, normalize_code_system

NUMERIC_PATTERN = re.compile(r"^-?\d+(\.\d+)?$")
ORJSON_INTEGER_MIN = -(1 << 63)
ORJSON_INTEGER_MAX = (1 << 64) - 1


class _ExactNumericText(str):
    """Canonical numeric text awaiting final JSON response shaping."""


PTG2_ITEM_SOURCE_FIELDS = {
    "identity_kind",
    "identity_sha256",
    "logical_hash_deferred",
    "logical_json_sha256",
    "snapshot_id",
    "source_artifact_key",
    "source_key",
    "source_trace_set_hash",
    "source_type",
    "source_trace",
    "raw_container_sha256",
    "network_names",
    "billing_code_type_version",
    "source_procedure_name",
    "source_procedure_description",
}
PTG2_ITEM_DIAGNOSTIC_FIELDS = {
    "billing_code",
    "billing_code_type",
    "catalog_procedure_name",
    "catalog_procedure_description",
    "confidence",
    "hp_procedure_code",
    "location_confidence_code",
    "location_hash",
    "price_set_hash",
    "price_set_hashes",
    "provider_ordinal",
    "provider_set_hash",
    "provider_set_hashes",
    "rate_pack_hash",
    "rate_pack_hashes",
}
PTG2_QUERY_SOURCE_FIELDS = {
    "source",
    "source_key",
    "serving_table",
}
PTG2_QUERY_DIAGNOSTIC_FIELDS = {
    "mode",
    "price_filter",
    "procedure_consolidation",
    "provider_reverse_index",
    "route_item_table",
    "snapshot_id",
}
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


def _optional_decimal(value: Any) -> Decimal | None:
    if value in (None, "", "null"):
        return None
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError):
        return None


def _include_ptg2_details(args: dict[str, Any]) -> bool:
    return _request_bool(args.get("include_details")) or _request_bool(args.get("include_debug"))


def _include_ptg2_sources(args: dict[str, Any]) -> bool:
    return _include_ptg2_details(args) or _request_bool(args.get("include_sources"))


def _shape_ptg2_response(payload: dict[str, Any], args: dict[str, Any]) -> dict[str, Any]:
    """Keep app-facing PTG responses lean unless callers opt into provenance/debug fields."""
    if _include_ptg2_details(args):
        return _fragment_exact_numbers(payload)

    include_sources = _include_ptg2_sources(args)
    hidden_item_fields = set(PTG2_ITEM_DIAGNOSTIC_FIELDS)
    if not include_sources:
        hidden_item_fields.update(PTG2_ITEM_SOURCE_FIELDS)
    hidden_query_fields = set(PTG2_QUERY_DIAGNOSTIC_FIELDS)
    if not include_sources:
        hidden_query_fields.update(PTG2_QUERY_SOURCE_FIELDS)

    shaped_response_by_field = dict(payload)
    shaped_response_by_field["items"] = [
        {key: value for key, value in dict(item).items() if key not in hidden_item_fields}
        for item in payload.get("items", [])
    ]
    shaped_response_by_field["query"] = {
        key: value for key, value in dict(payload.get("query") or {}).items() if key not in hidden_query_fields
    }
    return _fragment_exact_numbers(shaped_response_by_field)


def _fragment_exact_numbers(value: Any) -> Any:
    if isinstance(value, _ExactNumericText):
        return orjson.Fragment(value.encode("ascii"))
    if isinstance(value, dict):
        return {key: _fragment_exact_numbers(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_fragment_exact_numbers(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_fragment_exact_numbers(item) for item in value)
    return value


def _response_wire_value(value: Any) -> Any:
    """Round-trip one response value through its exact JSON wire form."""

    return json.loads(
        orjson.dumps(_fragment_exact_numbers(value)),
        parse_float=Decimal,
        parse_int=int,
    )


def _normalize_catalog_code_system(raw_system: Any) -> str:
    return normalize_code_system(raw_system)


def _canonical_catalog_code(code_system: str, raw_code: Any) -> str:
    return canonical_catalog_code(code_system, raw_code)


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


def _coerce_json_payload(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
    return value


def _coerce_numeric_rate(rate_value: Any) -> Any:
    if rate_value is None or isinstance(rate_value, bool):
        return rate_value
    if isinstance(rate_value, float):
        return rate_value
    if isinstance(rate_value, Decimal):
        decimal_value = rate_value
    elif isinstance(rate_value, int):
        decimal_value = Decimal(rate_value)
    elif isinstance(rate_value, str):
        text_value = rate_value.strip()
        if not text_value or not NUMERIC_PATTERN.fullmatch(text_value):
            return rate_value
        try:
            decimal_value = Decimal(text_value)
        except InvalidOperation:
            return rate_value
    else:
        return rate_value

    if not decimal_value.is_finite():
        return rate_value
    expanded = format(decimal_value, "f")
    if decimal_value.is_zero():
        expanded = "0"
    elif "." in expanded:
        expanded = expanded.rstrip("0").rstrip(".")

    # Retain ordinary native numbers when orjson emits the exact same token.
    # Precision-sensitive decimals and integers outside orjson's range remain
    # validated text until final response shaping turns them into fragments.
    if "." not in expanded:
        if ORJSON_INTEGER_MIN <= decimal_value <= ORJSON_INTEGER_MAX:
            return int(decimal_value)
    else:
        float_value = float(expanded)
        if orjson.dumps(float_value).decode("ascii") == expanded:
            return float_value

    return _ExactNumericText(expanded)


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


def _canonical_price_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized_price_by_field = dict(row)
    normalized_price_by_field["negotiated_rate"] = _coerce_numeric_rate(normalized_price_by_field.get("negotiated_rate"))
    if "service_code" in normalized_price_by_field:
        normalized_price_by_field["service_code"] = sorted(
            {_canonical_catalog_code("POS", code) for code in _normalize_string_list(normalized_price_by_field.get("service_code"))}
        )
    if "billing_code_modifier" in normalized_price_by_field:
        normalized_price_by_field["billing_code_modifier"] = sorted(
            {modifier.upper() for modifier in _normalize_string_list(normalized_price_by_field.get("billing_code_modifier"))}
        )
    return normalized_price_by_field


def _price_row_key(row: dict[str, Any]) -> str:
    return json.dumps(row, sort_keys=True, separators=(",", ":"), default=str)


def _normalize_price_payload(prices: Any) -> list[dict[str, Any]]:
    payload = _coerce_json_payload(prices, [])
    normalized_prices: list[dict[str, Any]] = []
    if not isinstance(payload, list):
        return normalized_prices
    for row in payload:
        if not isinstance(row, dict):
            continue
        normalized_price_by_field = _canonical_price_row(row)
        normalized_prices.append(normalized_price_by_field)
    return normalized_prices


def _normalize_filter_string_list(value: Any, *, upper: bool = True, code_system: str | None = None) -> list[str]:
    values: list[str] = []
    for item in _normalize_string_list(value):
        for part in str(item).split(","):
            text_value = part.strip()
            if not text_value:
                continue
            if code_system:
                text_value = _canonical_catalog_code(code_system, text_value)
            elif upper:
                text_value = text_value.upper()
            values.append(text_value)
    return sorted(set(values))


def _price_component(modifiers: list[str]) -> str:
    normalized_modifiers = {modifier.upper() for modifier in modifiers}
    if not normalized_modifiers:
        return "global"
    if normalized_modifiers == {"26"}:
        return "professional"
    if normalized_modifiers == {"TC"}:
        return "technical"
    return "modifier"


def _summarize_normalized_price_payload(normalized_prices: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summaries_by_group: dict[tuple[Any, ...], dict[str, Any]] = {}
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
        summary = summaries_by_group.setdefault(
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

    component_rank_by_name = {"global": 0, "professional": 1, "technical": 2, "modifier": 3}
    return sorted(
        summaries_by_group.values(),
        key=lambda item: (
            component_rank_by_name.get(str(item.get("component")), 99),
            str(item.get("billing_class") or ""),
            str(item.get("setting") or ""),
            str(item.get("modifier") or ""),
            float(item.get("rate")) if isinstance(item.get("rate"), (int, float)) else 0.0,
        ),
    )


def _summarize_price_payload(prices: Any) -> list[dict[str, Any]]:
    return _summarize_normalized_price_payload(_normalize_price_payload(prices))


def _price_response_fields(prices: Any) -> dict[str, list[dict[str, Any]]]:
    normalized = _normalize_price_payload(prices)
    return {
        "prices": normalized,
        "tic_prices": normalized,
        "price_summary": _summarize_normalized_price_payload(normalized),
    }
