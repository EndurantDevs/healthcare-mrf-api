# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read immutable plan-pricing card and aggregate projections."""

from __future__ import annotations

from dataclasses import dataclass
from contextlib import nullcontext
from typing import Any, Mapping

import orjson
from sqlalchemy import Integer, bindparam, text
from sqlalchemy.dialects.postgresql import ARRAY

from api.network_address_scope import current_network_address_scope
from api.plan_pricing_projection_aggregate_read import read_aggregate_pack_page
from api.plan_pricing_projection_contract import (
    COST_ORDER_FIELDS,
    FACTORIZED_PROJECTION_CONTRACTS,
    LEGACY_PROJECTION_CONTRACT,
    MAX_GEO_CELLS,
    PROJECTION_CONTRACT,
    ZIP5,
    PlanPricingProjectionUnavailable,
    PlanPricingProjectionUnsupported,
    projection_code_identity,
    table,
    current_code_projection,
    code_projection_scope,
)
from api.plan_release_serving import (
    PlanReleaseServingSelection,
    annotate_plan_release_response,
)
from api.provider_service_source_policy import admitted_plan_release_bindings
from api.ptg2_response import _is_request_flag_enabled


@dataclass(frozen=True)
class _ProjectionRequest:
    result_type: str
    projection_id: str
    contract: str
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
            "q",
            "npi",
            "specialty",
            "provider_type",
            "classification",
            "taxonomy_codes",
            "taxonomy_code",
            "taxonomy_classification",
            "taxonomy_specialization",
            "taxonomy_section",
            "provider_sex_code",
            "pos",
            "place_of_service",
            "service_code",
            "modifier",
            "modifiers",
            "billing_code_modifier",
            "rate",
            "negotiated_rate",
            "rate_tolerance",
            "negotiated_rate_tolerance",
        )
        if args.get(field_name) not in (None, "", "null", False)
    ]
    unsupported_fields.extend(
        field_name
        for field_name in (
            "include_code_details",
            "include_sources",
            "include_evidence",
            "include_unverified_addresses",
            "include_details",
            "include_debug",
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
    raw_radius = args.get("zip_radius_miles") if zip5 else args.get("radius_miles")
    try:
        return max(float(raw_radius or 0), 0.0)
    except (TypeError, ValueError) as exc:
        raise PlanPricingProjectionUnsupported("card projection radius is invalid") from exc


def _center_query(
    args: Mapping[str, Any],
    zip5: str,
) -> tuple[str, dict[str, Any]]:
    if zip5:
        return (
            f"""
            SELECT latitude, longitude
              FROM {table("geo_zip_lookup")}
             WHERE zip_code = :zip5
            """,
            {"zip5": zip5},
        )
    try:
        requested_latitude = float(args.get("lat"))
        requested_longitude = float(args.get("long"))
    except (TypeError, ValueError) as exc:
        raise PlanPricingProjectionUnsupported("card projection requires ZIP5 or coordinates") from exc
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
                FROM {table("geo_zip_lookup")} candidate
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
        raise PlanPricingProjectionUnsupported("card projection supports ZIP5 or coordinates, not city/state")
    if zip5 and not ZIP5.fullmatch(zip5):
        raise PlanPricingProjectionUnsupported("card projection requires a valid ZIP5")
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
        raise PlanPricingProjectionUnsupported(f"card projection radius exceeds {MAX_GEO_CELLS} ZIP cells")
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
    contract: str = PROJECTION_CONTRACT,
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
        "projection_contract": contract,
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
        raise PlanPricingProjectionUnsupported("view=card does not support filters: " + ", ".join(unsupported_fields))
    code_identity = projection_code_identity(args.get("code_system"), args.get("code"))
    if code_identity is None:
        if result_type == "rate_aggregates":
            return None
        raise PlanPricingProjectionUnsupported("view=card requires code_system and code")
    if not selection.pricing_projection_id:
        if args.get("view") is None and result_type == "rate_aggregates":
            return None
        raise PlanPricingProjectionUnavailable("the selected release has no ready card projection")
    contract = selection.pricing_projection_contract or LEGACY_PROJECTION_CONTRACT
    if contract not in {
        LEGACY_PROJECTION_CONTRACT,
        *FACTORIZED_PROJECTION_CONTRACTS,
    } and not (
        current_code_projection() is not None
        and contract == current_code_projection().identity["contract"]
        and selection.pricing_projection_id == current_code_projection().projection_id
    ):
        raise PlanPricingProjectionUnavailable("the selected pricing projection contract is unsupported")
    return _ProjectionRequest(
        result_type,
        selection.pricing_projection_id,
        contract,
        *code_identity,
    )


def _page_sql(result_type: str, order_direction: str) -> str:
    table_name = "plan_pricing_card" if result_type == "provider_cards" else "plan_pricing_cell_aggregate"
    order_sql = (
        f"projected.minimum_negotiated_rate {order_direction}, projected.npi"
        if result_type == "provider_cards"
        else f"projected.minimum_negotiated_rate {order_direction}, projected.geo_cell"
    )
    matched_rank_sql = (
        ", ROW_NUMBER() OVER (PARTITION BY item.npi ORDER BY cells.ordinal, item.geo_cell) AS address_rank"
        if result_type == "provider_cards"
        else ""
    )
    projected_source_sql = (
        "(SELECT * FROM matched WHERE address_rank = 1) matched" if result_type == "provider_cards" else "matched"
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
    *,
    binding_ordinals: tuple[int, ...] | None = None,
) -> tuple[list[orjson.Fragment], int]:
    if request.result_type == "rate_aggregates" and request.contract != LEGACY_PROJECTION_CONTRACT:
        return await read_aggregate_pack_page(
            session,
            request,
            geo_cells,
            args,
            pagination,
            **({"binding_ordinals": binding_ordinals} if binding_ordinals is not None else {}),
        )
    order_direction = "DESC" if str(args.get("order") or "asc").strip().lower() == "desc" else "ASC"
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
        orjson.Fragment(bytes(projected_row[0])) for projected_row in projected_rows if projected_row[0] is not None
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
        "query": _projection_query(
            args,
            result_type=request.result_type,
            contract=request.contract,
        ),
    }


async def _projection_page_response(session, request, args, pagination, contributing_bindings, selection):
    """Keep attributed reads and full-pack fallback within the same policy decision."""
    try:
        selected_geo_cells = await geo_cells(session, args, result_type=request.result_type)
    except PlanPricingProjectionUnsupported:
        if contributing_bindings is None and request.result_type == "rate_aggregates":
            return None
        raise
    if not selected_geo_cells:
        response = _page_response(request, args, pagination, [], 0)
        response["result_state"] = "no_match_in_radius"
        return response
    if contributing_bindings == ():
        return _page_response(request, args, pagination, [], 0)
    binding_ordinals = (
        None
        if contributing_bindings is None or contributing_bindings == selection.in_network_bindings
        else tuple(binding.binding_ordinal for binding in contributing_bindings)
    )
    if binding_ordinals is not None and request.contract == LEGACY_PROJECTION_CONTRACT:
        return None
    try:
        projected_items, total = await _read_page(
            session,
            request,
            selected_geo_cells,
            args,
            pagination,
            **({"binding_ordinals": binding_ordinals} if binding_ordinals is not None else {}),
        )
    except PlanPricingProjectionUnsupported:
        if contributing_bindings is None and request.result_type == "rate_aggregates" and args.get("view") is None:
            return None
        raise
    return _page_response(request, args, pagination, projected_items, total)


async def search_plan_pricing_projection(
    session: Any,
    selection: PlanReleaseServingSelection,
    args: Mapping[str, Any],
    pagination: Any,
) -> dict[str, Any] | None:
    """Admit source contributors before reading an immutable card or aggregate page."""
    if (
        selection is not None
        and not selection.pricing_projection_id
        and projection_result_type(args) == "rate_aggregates"
    ):
        from api.plan_pricing_code_read import resolve_code_pricing

        capability = await resolve_code_pricing(session, selection, args)
        if capability is not None:
            with code_projection_scope(capability):
                return await search_plan_pricing_projection(session, capability.selection(selection), args, pagination)
    request = _validated_projection_request(selection, args)
    if request is None:
        return None
    if request.result_type == "provider_cards" and request.contract != LEGACY_PROJECTION_CONTRACT:
        return None
    contributing_bindings = (
        await admitted_plan_release_bindings(session, selection)
        if current_network_address_scope() is not None
        else None
    )
    scope = current_code_projection()
    with code_projection_scope(scope.contributors(contributing_bindings)) if scope is not None else nullcontext():
        response = await _projection_page_response(session, request, args, pagination, contributing_bindings, selection)
    if response is None:
        return None
    return annotate_plan_release_response(response, selection, contributing_bindings=contributing_bindings) or response


_FACTORIZED_CARD_CANDIDATE_SQL = """
    WITH profile_window AS MATERIALIZED (
        SELECT binding_ordinal, provider_set_key, membership_count,
               minimum_negotiated_rate
          FROM {profile_table}
         WHERE projection_id = :projection_id
           AND code_system = :code_system
           AND code = :code{binding_sql}
         ORDER BY minimum_negotiated_rate, binding_ordinal,
                  provider_set_key
         LIMIT :provider_set_probe_limit
    ), ranked_profiles AS MATERIALIZED (
        SELECT profile_window.*,
               ROW_NUMBER() OVER (
                   ORDER BY minimum_negotiated_rate, binding_ordinal,
                            provider_set_key
               ) AS profile_rank,
               SUM(membership_count) OVER (
                   ORDER BY minimum_negotiated_rate, binding_ordinal,
                            provider_set_key
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
               ) AS cumulative_memberships
          FROM profile_window
    ), admitted_profiles AS MATERIALIZED (
        SELECT * FROM ranked_profiles
         WHERE profile_rank <= :provider_set_limit
           AND cumulative_memberships <= :membership_limit
    ), unread_profile AS MATERIALIZED (
        SELECT minimum_negotiated_rate
          FROM ranked_profiles
         WHERE profile_rank > :provider_set_limit
            OR cumulative_memberships > :membership_limit
         ORDER BY profile_rank
         LIMIT 1
    ), admission AS MATERIALIZED (
        SELECT COUNT(*)::bigint AS provider_set_count,
               COALESCE(MAX(cumulative_memberships), 0)::bigint
                   AS membership_count,
               COALESCE(BOOL_AND(membership_count > 0), TRUE)
                   AS profiles_valid
          FROM admitted_profiles
    ), cells AS MATERIALIZED (
        SELECT geo_cell FROM unnest(CAST(:geo_cells AS varchar[]))
             AS requested(geo_cell)
    ), eligible AS MATERIALIZED (
        SELECT member.npi,
               MIN(profile.minimum_negotiated_rate) AS minimum_rate
          FROM admitted_profiles profile
          JOIN {membership_table} member
            ON member.projection_id = :projection_id
           AND member.binding_ordinal = profile.binding_ordinal
           AND member.provider_set_key = profile.provider_set_key
          JOIN {provider_table} provider
            ON provider.projection_id = member.projection_id
           AND provider.npi = member.npi
          JOIN cells ON cells.geo_cell = provider.geo_cell
         WHERE TRUE {taxonomy_sql}
         GROUP BY member.npi
    ), boundary AS MATERIALIZED (
        SELECT minimum_rate FROM eligible
         ORDER BY minimum_rate, npi
        OFFSET :boundary_offset LIMIT 1
    ), metadata AS MATERIALIZED (
        SELECT (SELECT COUNT(*) FROM eligible)::bigint
                   AS total_lower_bound,
               NOT EXISTS (SELECT 1 FROM unread_profile)
                   AS profile_exhausted,
               (SELECT minimum_negotiated_rate FROM unread_profile)
                   AS unread_minimum_rate,
               (SELECT minimum_rate FROM boundary) AS boundary_rate,
               admission.provider_set_count,
               admission.membership_count,
               admission.profiles_valid
          FROM admission
    ), selected AS MATERIALIZED (
        SELECT eligible.npi, eligible.minimum_rate
          FROM eligible
         WHERE NOT EXISTS (SELECT 1 FROM boundary)
            OR eligible.minimum_rate <= (
                SELECT minimum_rate FROM boundary
            )
         ORDER BY eligible.minimum_rate, eligible.npi
         LIMIT :candidate_probe_limit
    )
    SELECT selected.npi, selected.minimum_rate,
           metadata.total_lower_bound, metadata.profile_exhausted,
           metadata.unread_minimum_rate, metadata.boundary_rate,
           metadata.provider_set_count, metadata.membership_count,
           metadata.profiles_valid
      FROM metadata
      LEFT JOIN selected ON TRUE
     ORDER BY selected.minimum_rate NULLS LAST,
              selected.npi NULLS LAST
"""

_FACTORIZED_CARD_COMPLETION_SQL = """
    WITH selected AS MATERIALIZED (
        SELECT npi FROM unnest(CAST(:selected_npis AS bigint[]))
             AS requested(npi)
    ), cells AS MATERIALIZED (
        SELECT geo_cell, cell_rank
          FROM unnest(CAST(:geo_cells AS varchar[])) WITH ORDINALITY
               AS requested(geo_cell, cell_rank)
    ), selected_memberships AS MATERIALIZED (
        SELECT member.npi, member.binding_ordinal,
               member.provider_set_key
          FROM selected
          JOIN {membership_table} member USING (npi)
          JOIN {profile_table} profile
            ON profile.projection_id = member.projection_id
           AND profile.code_system = :code_system
           AND profile.code = :code
           AND profile.binding_ordinal = member.binding_ordinal
           AND profile.provider_set_key = member.provider_set_key
         WHERE member.projection_id = :projection_id{binding_sql}
         ORDER BY member.npi, member.binding_ordinal,
                  member.provider_set_key
         LIMIT :membership_probe_limit
    ), profile_keys AS MATERIALIZED (
        SELECT DISTINCT binding_ordinal, provider_set_key
          FROM selected_memberships
         ORDER BY binding_ordinal, provider_set_key
         LIMIT :provider_set_probe_limit
    ), scope_admission AS MATERIALIZED (
        SELECT (SELECT COUNT(*) FROM selected_memberships)::bigint
                   AS membership_count,
               (SELECT COUNT(*) FROM profile_keys)::bigint
                   AS provider_set_count
    ), admitted_profile_keys AS MATERIALIZED (
        SELECT profile_keys.*
          FROM profile_keys
         CROSS JOIN scope_admission
         WHERE scope_admission.membership_count <= :membership_limit
           AND scope_admission.provider_set_count <= :provider_set_limit
    ), profile_scope AS MATERIALIZED (
        SELECT profile.binding_ordinal, profile.provider_set_key,
               cardinality(profile.negotiated_rates)::bigint
                   AS rate_value_count
          FROM admitted_profile_keys keys
          JOIN {profile_table} profile
            ON profile.projection_id = :projection_id
           AND profile.code_system = :code_system
           AND profile.code = :code
           AND profile.binding_ordinal = keys.binding_ordinal
           AND profile.provider_set_key = keys.provider_set_key
    ), rate_admission AS MATERIALIZED (
        SELECT COALESCE(SUM(rate_value_count), 0)::bigint
                   AS rate_value_count
          FROM profile_scope
    ), admitted_profiles AS MATERIALIZED (
        SELECT profile.binding_ordinal, profile.provider_set_key,
               profile.minimum_negotiated_rate,
               profile.maximum_negotiated_rate, profile.rate_count,
               profile.negotiated_rates, profile.rate_multiplicities
          FROM profile_scope scope
          JOIN {profile_table} profile
            ON profile.projection_id = :projection_id
           AND profile.code_system = :code_system
           AND profile.code = :code
           AND profile.binding_ordinal = scope.binding_ordinal
           AND profile.provider_set_key = scope.provider_set_key
         CROSS JOIN rate_admission
         WHERE rate_admission.rate_value_count <= :rate_value_limit
    ), profile_validation AS MATERIALIZED (
        SELECT COALESCE(BOOL_AND(
                   cardinality(negotiated_rates) > 0
                   AND cardinality(negotiated_rates)
                       = cardinality(rate_multiplicities)
                   AND minimum_negotiated_rate = negotiated_rates[1]
                   AND maximum_negotiated_rate = negotiated_rates[
                       cardinality(negotiated_rates)
                   ]
                   AND rate_count = (
                       SELECT SUM(multiplicity)::bigint
                         FROM unnest(rate_multiplicities)
                              AS listed(multiplicity)
                   )
                   AND NOT EXISTS (
                       SELECT 1
                         FROM generate_subscripts(
                             negotiated_rates, 1
                         ) AS indexes(rate_index)
                        WHERE rate_index > 1
                          AND negotiated_rates[rate_index]
                              <= negotiated_rates[rate_index - 1]
                   )
                   AND NOT EXISTS (
                       SELECT 1
                         FROM unnest(rate_multiplicities)
                              AS listed(multiplicity)
                        WHERE multiplicity <= 0
                   )
               ), TRUE) AS profiles_valid
          FROM admitted_profiles
    ), admission AS MATERIALIZED (
        SELECT scope_admission.membership_count,
               scope_admission.provider_set_count,
               rate_admission.rate_value_count,
               profile_validation.profiles_valid
          FROM scope_admission
         CROSS JOIN rate_admission
         CROSS JOIN profile_validation
    ), stats AS MATERIALIZED (
        SELECT member.npi,
               MIN(profile.minimum_negotiated_rate) AS minimum_rate,
               MAX(profile.maximum_negotiated_rate) AS maximum_rate,
               SUM(profile.rate_count)::bigint AS rate_count
          FROM selected_memberships member
          JOIN admitted_profiles profile USING (
               binding_ordinal, provider_set_key
          )
         CROSS JOIN profile_validation
         WHERE profile_validation.profiles_valid
         GROUP BY member.npi
    ), ranked_provider AS MATERIALIZED (
        SELECT provider.npi, provider.geo_cell,
               provider.entity_type_code, provider.taxonomy_codes,
               provider.fragment,
               ROW_NUMBER() OVER (
                   PARTITION BY provider.npi
                   ORDER BY cells.cell_rank, provider.geo_cell
               ) AS address_rank
          FROM selected
          JOIN {provider_table} provider USING (npi)
          JOIN cells ON cells.geo_cell = provider.geo_cell
         CROSS JOIN admission
         WHERE provider.projection_id = :projection_id
           AND admission.membership_count <= :membership_limit
           AND admission.provider_set_count <= :provider_set_limit
           AND admission.rate_value_count <= :rate_value_limit
           {taxonomy_sql}
    )
    SELECT selected.npi, stats.minimum_rate, stats.maximum_rate,
           stats.rate_count, ranked_provider.geo_cell,
           ranked_provider.entity_type_code,
           ranked_provider.taxonomy_codes, ranked_provider.fragment,
           admission.membership_count, admission.provider_set_count,
           admission.rate_value_count, admission.profiles_valid
      FROM selected
     CROSS JOIN admission
      LEFT JOIN stats USING (npi)
      LEFT JOIN ranked_provider
        ON ranked_provider.npi = selected.npi
       AND ranked_provider.address_rank = 1
     ORDER BY selected.npi
"""


def _factorized_card_binding_parameter(binding_ordinals: tuple[int, ...]) -> Any:
    """Bind a bounded set of original release ordinals without SQL interpolation."""

    if (
        type(binding_ordinals) is not tuple
        or len(binding_ordinals) > 5000
        or any(type(value) is not int or not 0 <= value <= 2_147_483_647 for value in binding_ordinals)
        or len(set(binding_ordinals)) != len(binding_ordinals)
    ):
        raise PlanPricingProjectionUnavailable("plan-pricing binding ordinals are invalid")
    return bindparam("allowed_binding_ordinals", list(binding_ordinals), type_=ARRAY(Integer))
