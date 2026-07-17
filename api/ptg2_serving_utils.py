# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Small PTG2 serving helpers shared by DB and fixture-backed paths."""

from __future__ import annotations

from decimal import Decimal
import re
from typing import Any
from uuid import UUID

from api.ptg2_response import (
    _coerce_numeric_rate,
    _normalize_filter_string_list,
    _optional_decimal,
)
from api.ptg2_types import PTG2ServingIndex


def _normalize_zip5(value: Any) -> str | None:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits[:5] if len(digits) >= 5 else None


def ein_plan_id_variants(value: Any) -> list[str]:
    """Return raw, digit-only, and hyphenated EIN plan-id lookup variants."""
    raw = str(value or "").strip()
    if not raw:
        return []
    variants = [raw]
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 9:
        for candidate in (digits, f"{digits[:2]}-{digits[2:]}"):
            if candidate not in variants:
                variants.append(candidate)
    return variants


def _provider_payload(index: PTG2ServingIndex, ordinal: Any) -> dict[str, Any]:
    provider_fields_by_name = dict(index.providers.get(str(ordinal)) or {})
    if "provider_ordinal" not in provider_fields_by_name:
        provider_fields_by_name["provider_ordinal"] = ordinal
    return provider_fields_by_name


def _row_mapping(row: Any) -> dict[str, Any]:
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return dict(mapping)
    if isinstance(row, dict):
        return dict(row)
    return dict(row)


def _uuid_to_hex(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, UUID):
        return value.hex
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        return raw.hex() if len(raw) == 16 else raw.hex()
    text_value = str(value).strip().lower()
    if not text_value:
        return ""
    if "-" in text_value:
        try:
            return UUID(text_value).hex
        except ValueError:
            return text_value.replace("-", "")
    return text_value


def _price_filter_clauses(
    args: dict[str, Any],
    params: dict[str, Any],
    *,
    atom_alias: str = "pa",
    service_alias: str = "service_set",
    modifier_alias: str = "modifier_set",
) -> tuple[list[str], dict[str, Any]]:
    query_values_by_field: dict[str, Any] = {}
    clauses: list[str] = []

    service_codes = _normalize_filter_string_list(
        args.get("pos") or args.get("place_of_service") or args.get("service_code"),
        code_system="POS",
    )
    if service_codes:
        params["price_service_codes"] = service_codes
        query_values_by_field["service_code"] = service_codes
        query_values_by_field["pos"] = service_codes[0] if len(service_codes) == 1 else service_codes
        clauses.append(
            f"COALESCE({service_alias}.codes, ARRAY[]::varchar[]) && CAST(:price_service_codes AS varchar[])"
        )

    modifier_codes = _normalize_filter_string_list(
        args.get("modifier") or args.get("modifiers") or args.get("billing_code_modifier"),
        upper=True,
    )
    if modifier_codes:
        params["price_modifier_codes"] = modifier_codes
        query_values_by_field["billing_code_modifier"] = modifier_codes
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
        query_values_by_field["negotiated_rate"] = _coerce_numeric_rate(requested_rate)
        query_values_by_field["rate_tolerance"] = _coerce_numeric_rate(tolerance)
        clauses.append(
            f"""
            {atom_alias}.negotiated_rate ~ '^-?[0-9]+(\\.[0-9]+)?$'
            AND ABS({atom_alias}.negotiated_rate::numeric - :price_negotiated_rate) <= :price_rate_tolerance
            """
        )

    return clauses, query_values_by_field
