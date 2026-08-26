# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read immutable plan-pricing card and aggregate projections."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import orjson
from sqlalchemy import text

from api.plan_pricing_projection_contract import (
    COST_ORDER_FIELDS,
    MAX_GEO_CELLS,
    PROJECTION_CONTRACT,
    ZIP5,
    PlanPricingProjectionUnavailable,
    PlanPricingProjectionUnsupported,
    projection_code_identity,
    table,
)
from api.plan_release_serving import (
    PlanReleaseServingSelection,
    annotate_plan_release_response,
)
from api.ptg2_response import _is_request_flag_enabled


@dataclass(frozen=True)
class _ProjectionRequest:
    result_type: str
    projection_id: str
    code_system: str
    code: str


def projection_result_type(args: Mapping[str, Any]) -> str | None:
    """Select the additive card or omitted-view aggregate response."""

    raw_view = args.get("view")
    view = str(raw_view or "full").strip().lower()
    include_providers = _is_request_flag_enabled(
        args.get("include_providers"),
        default=True,
    )
    if view == "card":
        return "provider_cards" if include_providers else "rate_aggregates"
    if raw_view is None and not include_providers:
        return "rate_aggregates"
    return None


def unsupported_projection_fields(
    args: Mapping[str, Any],
) -> tuple[str, ...]:
    """List filters whose current semantics are not preprojected."""

    unsupported_fields = [
        field_name
        for field_name in (
            "q", "npi", "specialty", "provider_type", "classification",
            "taxonomy_codes", "taxonomy_code", "taxonomy_classification",
            "taxonomy_specialization", "taxonomy_section", "provider_sex_code",
            "pos", "place_of_service", "service_code", "modifier", "modifiers",
            "billing_code_modifier", "rate", "negotiated_rate", "rate_tolerance",
            "negotiated_rate_tolerance",
        )
        if args.get(field_name) not in (None, "", "null", False)
    ]
    unsupported_fields.extend(
        field_name
        for field_name in (
            "include_code_details", "include_sources", "include_evidence",
            "include_unverified_addresses", "include_details", "include_debug",
        )
        if _is_request_flag_enabled(args.get(field_name), default=False)
    )
    order_by = str(args.get("order_by") or "total_allowed_amount").strip().lower()
    if order_by not in COST_ORDER_FIELDS:
        unsupported_fields.append("order_by")
    order = str(args.get("order") or "asc").strip().lower()
    if order not in {"asc", "desc"}:
        unsupported_fields.append("order")
    return tuple(unsupported_fields)


def _projection_radius(args: Mapping[str, Any], zip5: str) -> float:
    raw_radius = (
        args.get("zip_radius_miles") if zip5 else args.get("radius_miles")
    )
    try:
        return max(float(raw_radius or 0), 0.0)
    except (TypeError, ValueError) as exc:
        raise PlanPricingProjectionUnsupported(
            "card projection radius is invalid"
        ) from exc


def _center_query(
    args: Mapping[str, Any],
    zip5: str,
) -> tuple[str, dict[str, Any]]:
    if zip5:
        return (
            f"""
            SELECT latitude, longitude
              FROM {table('geo_zip_lookup')}
             WHERE zip_code = :zip5
            """,
            {"zip5": zip5},
        )
    try:
        requested_latitude = float(args.get("lat"))
        requested_longitude = float(args.get("long"))
    except (TypeError, ValueError) as exc:
        raise PlanPricingProjectionUnsupported(
            "card projection requires ZIP5 or coordinates"
        ) from exc
    return (
        """
        SELECT CAST(:latitude AS double precision) AS latitude,
               CAST(:longitude AS double precision) AS longitude
        """,
        {
            "latitude": requested_latitude,
            "longitude": requested_longitude,
        },
    )


def _radius_cells_sql(center_sql: str) -> str:
    return f"""
        WITH center AS MATERIALIZED ({center_sql})
        SELECT cells.zip_code
          FROM center
          CROSS JOIN LATERAL (
              SELECT zip_code
                FROM {table('geo_zip_lookup')} candidate
               WHERE candidate.latitude BETWEEN center.latitude - :radius / 69.0
                         AND center.latitude + :radius / 69.0
                 AND candidate.longitude BETWEEN center.longitude - :radius / (
                         69.0 * greatest(abs(cos(radians(center.latitude))), 0.1)
                     ) AND center.longitude + :radius / (
                         69.0 * greatest(abs(cos(radians(center.latitude))), 0.1)
                     )
                 AND 69.0 * sqrt(
                     power(candidate.latitude - center.latitude, 2)
                     + power(
                         (candidate.longitude - center.longitude)
                         * cos(radians(
                             (candidate.latitude + center.latitude) / 2.0
                         )),
                         2
                     )
                 ) <= :radius
               ORDER BY 69.0 * sqrt(
                     power(candidate.latitude - center.latitude, 2)
                     + power(
                         (candidate.longitude - center.longitude)
                         * cos(radians(
                             (candidate.latitude + center.latitude) / 2.0
                         )),
                         2
                     )
               ), candidate.zip_code
               LIMIT :limit
          ) cells
    """


async def geo_cells(
    session: Any,
    args: Mapping[str, Any],
    *,
    result_type: str,
) -> list[str]:
    """Resolve exact or radius-bounded ZIP cells without inventing matches."""

    zip5 = str(args.get("zip5") or args.get("zip") or "").strip()
    city = str(args.get("city") or "").strip().lower()
    state = str(args.get("state") or "").strip().upper()
    radius = _projection_radius(args, zip5)
    if city or state:
        raise PlanPricingProjectionUnsupported(
            "card projection supports ZIP5 or coordinates, not city/state"
        )
    if zip5 and not ZIP5.fullmatch(zip5):
        raise PlanPricingProjectionUnsupported(
            "card projection requires a valid ZIP5"
        )
    if zip5 and radius <= 0:
        return [zip5]
    center_sql, center_parameters_by_name = _center_query(args, zip5)
    cell_result = await session.execute(
        text(_radius_cells_sql(center_sql)),
        {
            **center_parameters_by_name,
            "radius": radius,
            "limit": MAX_GEO_CELLS + 1,
        },
    )
    cells = [str(cell) for cell in cell_result.scalars().all()]
    if len(cells) > MAX_GEO_CELLS:
        raise PlanPricingProjectionUnsupported(
            f"card projection radius exceeds {MAX_GEO_CELLS} ZIP cells"
        )
    if not cells and result_type == "provider_cards":
        return []
    return cells


def _empty_pagination(pagination: Any) -> dict[str, Any]:
    return {
        "total": 0,
        "total_is_exact": True,
        "total_lower_bound": 0,
        "limit": int(pagination.limit),
        "offset": int(pagination.offset),
        "page": int(pagination.page),
        "has_more": False,
    }


def _projection_query(
    args: Mapping[str, Any],
    *,
    result_type: str,
) -> dict[str, Any]:
    return {
        "code": args.get("code") or None,
        "code_system": args.get("code_system") or None,
        "zip5": args.get("zip5") or None,
        "zip_radius_miles": args.get("zip_radius_miles"),
        "lat": args.get("lat"),
        "long": args.get("long"),
        "radius_miles": args.get("radius_miles"),
        "state": args.get("state") or None,
        "city": args.get("city") or None,
        "view": str(args.get("view") or "full").strip().lower(),
        "include_providers": result_type == "provider_cards",
        "projection_contract": PROJECTION_CONTRACT,
        "source": "plan_pricing_projection",
    }


def _validated_projection_request(
    selection: PlanReleaseServingSelection,
    args: Mapping[str, Any],
) -> _ProjectionRequest | None:
    result_type = projection_result_type(args)
    if result_type is None:
        return None
    unsupported_fields = unsupported_projection_fields(args)
    if unsupported_fields:
        if result_type == "rate_aggregates":
            return None
        raise PlanPricingProjectionUnsupported(
            "view=card does not support filters: " + ", ".join(unsupported_fields)
        )
    code_identity = projection_code_identity(
        args.get("code_system"), args.get("code")
    )
    if code_identity is None:
        if result_type == "rate_aggregates":
            return None
        raise PlanPricingProjectionUnsupported(
            "view=card requires code_system and code"
        )
    if not selection.pricing_projection_id:
        if args.get("view") is None and result_type == "rate_aggregates":
            return None
        raise PlanPricingProjectionUnavailable(
            "the selected release has no ready card projection"
        )
    return _ProjectionRequest(
        result_type,
        selection.pricing_projection_id,
        *code_identity,
    )


def _page_sql(result_type: str, order_direction: str) -> str:
    table_name = (
        "plan_pricing_card"
        if result_type == "provider_cards"
        else "plan_pricing_cell_aggregate"
    )
    order_sql = (
        f"projected.minimum_negotiated_rate {order_direction}, projected.npi"
        if result_type == "provider_cards"
        else f"projected.minimum_negotiated_rate {order_direction}, "
        "projected.geo_cell"
    )
    matched_rank_sql = (
        ", ROW_NUMBER() OVER (PARTITION BY item.npi "
        "ORDER BY cells.ordinal, item.geo_cell) AS address_rank"
        if result_type == "provider_cards"
        else ""
    )
    projected_source_sql = (
        "(SELECT * FROM matched WHERE address_rank = 1) matched"
        if result_type == "provider_cards"
        else "matched"
    )
    return f"""
        WITH cells AS MATERIALIZED (
            SELECT geo_cell, ordinal
              FROM unnest(CAST(:geo_cells AS varchar[]))
                   WITH ORDINALITY AS selected(geo_cell, ordinal)
        ), matched AS MATERIALIZED (
            SELECT item.*, cells.ordinal{matched_rank_sql}
              FROM cells
              JOIN {table(table_name)} item ON item.geo_cell = cells.geo_cell
             WHERE item.projection_id = :projection_id
               AND item.code_system = :code_system AND item.code = :code
        ), projected AS MATERIALIZED (
            SELECT matched.* FROM {projected_source_sql}
        ), page AS MATERIALIZED (
            SELECT projected.fragment,
                   ROW_NUMBER() OVER (ORDER BY {order_sql}) AS page_rank
              FROM projected ORDER BY {order_sql}
             LIMIT :limit OFFSET :offset
        )
        SELECT page.fragment, totals.total
          FROM (SELECT count(*) AS total FROM projected) totals
          LEFT JOIN page ON TRUE
         ORDER BY page.page_rank NULLS LAST
    """


async def _read_page(
    session: Any,
    request: _ProjectionRequest,
    geo_cells: list[str],
    args: Mapping[str, Any],
    pagination: Any,
) -> tuple[list[orjson.Fragment], int]:
    order_direction = (
        "DESC"
        if str(args.get("order") or "asc").strip().lower() == "desc"
        else "ASC"
    )
    projected_result = await session.execute(
        text(_page_sql(request.result_type, order_direction)),
        {
            "geo_cells": geo_cells,
            "projection_id": request.projection_id,
            "code_system": request.code_system,
            "code": request.code,
            "limit": int(pagination.limit),
            "offset": int(pagination.offset),
        },
    )
    projected_rows = projected_result.all()
    total = int(projected_rows[0][1]) if projected_rows else 0
    projected_items = [
        orjson.Fragment(bytes(projected_row[0]))
        for projected_row in projected_rows
        if projected_row[0] is not None
    ]
    return projected_items, total


def _page_response(
    request: _ProjectionRequest,
    args: Mapping[str, Any],
    pagination: Any,
    projected_items: list[orjson.Fragment],
    total: int,
) -> dict[str, Any]:
    return {
        "result_type": request.result_type,
        "result_state": "matched" if total else "no_matching_rates",
        "pricing_scope": "plan_scoped_ptg",
        "resolved": True,
        "items": projected_items,
        "pagination": {
            "total": total,
            "total_is_exact": True,
            "total_lower_bound": total,
            "limit": int(pagination.limit),
            "offset": int(pagination.offset),
            "page": int(pagination.page),
            "has_more": int(pagination.offset) + len(projected_items) < total,
        },
        "query": _projection_query(args, result_type=request.result_type),
    }


async def search_plan_pricing_projection(
    session: Any,
    selection: PlanReleaseServingSelection,
    args: Mapping[str, Any],
    pagination: Any,
) -> dict[str, Any] | None:
    """Read one card/aggregate page from its immutable ZIP-cell projection."""

    request = _validated_projection_request(selection, args)
    if request is None:
        return None
    try:
        selected_geo_cells = await geo_cells(
            session, args, result_type=request.result_type
        )
    except PlanPricingProjectionUnsupported:
        if request.result_type == "rate_aggregates":
            return None
        raise
    if not selected_geo_cells:
        response_by_field = {
            "result_type": request.result_type,
            "result_state": "no_match_in_radius",
            "pricing_scope": "plan_scoped_ptg",
            "resolved": True,
            "items": [],
            "pagination": _empty_pagination(pagination),
            "query": _projection_query(args, result_type=request.result_type),
        }
    else:
        projected_items, total = await _read_page(
            session, request, selected_geo_cells, args, pagination
        )
        response_by_field = _page_response(
            request, args, pagination, projected_items, total
        )
    return (
        annotate_plan_release_response(response_by_field, selection)
        or response_by_field
    )
