# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Optional code-catalog enrichment for PTG2 serving responses."""

from __future__ import annotations

import os
from typing import Any

from sqlalchemy import text

from api.ptg2_response import (
    _catalog_detail,
    _catalog_key,
    _missing_modifier_detail,
    _request_bool,
    _summarize_price_payload,
)
from api.ptg2_serving_utils import _row_mapping

PTG2_SCHEMA = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")


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
