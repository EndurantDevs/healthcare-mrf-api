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


async def _enrich_ptg2_code_details(
    session,
    response_payload: dict[str, Any],
    args: dict[str, Any],
) -> dict[str, Any]:
    """Attach requested code-catalog details to a PTG pricing response."""

    if not _request_bool(args.get("include_code_details")):
        return response_payload

    lookup_keys: set[tuple[str, str]] = set()
    response_items = [
        dict(response_item) for response_item in response_payload.get("items", [])
    ]
    for response_item in response_items:
        billing_key = _catalog_key(
            response_item.get("reported_code_system")
            or response_item.get("billing_code_type")
            or response_item.get("service_code_system"),
            response_item.get("reported_code")
            or response_item.get("billing_code")
            or response_item.get("service_code"),
        )
        if billing_key:
            lookup_keys.add(billing_key)
        for price in response_item.get("prices") or []:
            for service_code in price.get("service_code") or []:
                service_key = _catalog_key("POS", service_code)
                if service_key:
                    lookup_keys.add(service_key)
            for modifier_code in price.get("billing_code_modifier") or []:
                modifier_key = _catalog_key("MODIFIER", modifier_code)
                if modifier_key:
                    lookup_keys.add(modifier_key)

    if not lookup_keys:
        return response_payload

    clauses: list[str] = []
    query_params_by_name: dict[str, Any] = {}
    for idx, (code_system, code) in enumerate(sorted(lookup_keys)):
        clauses.append(f"(code_system = :code_system_{idx} AND code = :code_{idx})")
        query_params_by_name[f"code_system_{idx}"] = code_system
        query_params_by_name[f"code_{idx}"] = code
    query_result = await session.execute(
        text(
            f"""
            SELECT code_system, code, display_name, short_description
            FROM {PTG2_SCHEMA}.code_catalog
            WHERE {" OR ".join(clauses)}
            """
        ),
        query_params_by_name,
    )
    detail_map = {
        (
            str(catalog_row.get("code_system") or ""),
            str(catalog_row.get("code") or ""),
        ): _catalog_detail(catalog_row)
        for catalog_row in (
            _row_mapping(query_row) for query_row in query_result
        )
    }

    for response_item in response_items:
        billing_key = _catalog_key(
            response_item.get("reported_code_system")
            or response_item.get("billing_code_type")
            or response_item.get("service_code_system"),
            response_item.get("reported_code")
            or response_item.get("billing_code")
            or response_item.get("service_code"),
        )
        if billing_key and billing_key in detail_map:
            response_item["billing_code_detail"] = detail_map[billing_key]
        enriched_prices = []
        for price in response_item.get("prices") or []:
            price_fields_by_name = dict(price)
            service_details = []
            for service_code in price_fields_by_name.get("service_code") or []:
                detail = detail_map.get(_catalog_key("POS", service_code))
                if detail:
                    service_details.append(detail)
            if service_details:
                price_fields_by_name["service_code_details"] = service_details
            modifier_details = []
            for modifier_code in price_fields_by_name.get("billing_code_modifier") or []:
                detail = detail_map.get(_catalog_key("MODIFIER", modifier_code))
                if not detail:
                    detail = _missing_modifier_detail(modifier_code)
                if detail:
                    modifier_details.append(detail)
            if modifier_details:
                price_fields_by_name["billing_code_modifier_details"] = modifier_details
            enriched_prices.append(price_fields_by_name)
        response_item["prices"] = enriched_prices
        response_item["tic_prices"] = enriched_prices
        response_item["price_summary"] = _summarize_price_payload(enriched_prices)

    enriched_response_by_field = dict(response_payload)
    enriched_response_by_field["items"] = response_items
    return enriched_response_by_field
