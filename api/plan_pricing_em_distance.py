# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Release-bound distance cards for broad E&M office visits."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping

from sqlalchemy import text

from api.plan_pricing_projection_contract import SCHEMA
from api.plan_release_serving import (
    PlanReleaseServingSelection,
    annotate_plan_release_response,
)
from api.ptg2_response import _coerce_numeric_rate, _is_request_flag_enabled


PROJECTION_CONTRACT = "plan_pricing_em_distance_v1"
EM_CODES = ("99203", "99204", "99205", "99213", "99214", "99215")
_INITIAL_LOCATION_WINDOW = 512
_MAX_LOCATION_WINDOW = 8192
_CODE_INDEX = {code: index for index, code in enumerate(EM_CODES)}
_UNSUPPORTED_FIELDS = (
    "q",
    "npi",
    "mode",
    "state",
    "city",
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


def _table(name: str) -> str:
    return f'"{SCHEMA}"."{name}"'


def _radius(args: Mapping[str, Any]) -> float | None:
    raw_radius = args.get("zip_radius_miles")
    if raw_radius in (None, "", "null"):
        raw_radius = args.get("radius_miles")
    try:
        radius = float(25 if raw_radius in (None, "", "null") else raw_radius)
    except (TypeError, ValueError):
        return None
    return radius if 0 <= radius <= 25 else None


def _is_bounded_page(args: Mapping[str, Any]) -> bool:
    try:
        limit = int(args.get("limit") or 25)
        offset = int(args.get("offset") or 0)
    except (TypeError, ValueError):
        return False
    return 1 <= limit <= 100 and 0 <= offset and offset + limit <= 200


def _is_bounded_pagination(pagination: Any) -> bool:
    limit = int(pagination.limit)
    offset = int(pagination.offset)
    return 1 <= limit <= 100 and 0 <= offset and offset + limit <= 200


def _request_code_index(args: Mapping[str, Any]) -> int | None:
    """Return the fixed code slot only for the closed projected shape."""

    code = str(args.get("code") or "").strip()
    code_system = str(args.get("code_system") or "CPT").strip().upper()
    has_location = bool(
        str(args.get("zip5") or args.get("zip") or "").strip()
        or (
            args.get("lat") not in (None, "", "null")
            and args.get("long") not in (None, "", "null")
        )
    )
    if (
        code_system not in {"CPT", "HCPCS"}
        or code not in _CODE_INDEX
        or str(args.get("view") or "").strip().lower() != "card"
        or not _is_request_flag_enabled(args.get("include_providers"), default=True)
        or str(args.get("order_by") or "").strip().lower()
        not in {"distance", "distance_miles"}
        or (
            args.get("order") not in (None, "", "null")
            and str(args.get("order")).strip().lower() != "asc"
        )
        or not has_location
        or _radius(args) is None
        or not _is_bounded_page(args)
        or any(args.get(field) not in (None, "", "null", False) for field in _UNSUPPORTED_FIELDS)
        or any(
            _is_request_flag_enabled(args.get(field), default=False)
            for field in (
                "include_code_details",
                "include_allowed_amounts",
                "include_sources",
                "include_evidence",
                "include_unverified_addresses",
                "include_details",
                "include_debug",
            )
        )
    ):
        return None
    return _CODE_INDEX[code]


def em_distance_retry_option(
    args: Mapping[str, Any],
    pagination: Any | None = None,
) -> dict[str, Any] | None:
    """Return the self-contained retry only when the merged shape is projected."""

    retry_by_field = {
        "order_by": "distance",
        "order": "asc",
        "include_providers": True,
        "view": "card",
    }
    if pagination is not None and not _is_bounded_pagination(pagination):
        return None
    return (
        retry_by_field
        if _request_code_index({**args, **retry_by_field}) is not None
        else None
    )


_PROJECTION_READY_SQL = f"""
SELECT EXISTS (
    SELECT 1
      FROM {_table('plan_pricing_em_distance_attachment')} attachment
      JOIN {_table('plan_pricing_em_distance_candidate')} candidate
        ON candidate.projection_id = attachment.projection_id
     WHERE attachment.serving_revision_id = :serving_revision_id
       AND candidate.serving_revision_id = :serving_revision_id
       AND candidate.binding_set_digest = :binding_set_digest
       AND candidate.contract_version = :contract
       AND candidate.state = 'ready'
) AS projection_ready
"""


async def is_em_distance_projection_ready(
    session: Any,
    selection: PlanReleaseServingSelection,
) -> bool:
    """Return whether this exact serving revision has the sealed projection."""

    ready_result = await session.execute(
        text(_PROJECTION_READY_SQL),
        {
            "serving_revision_id": selection.serving_revision_id,
            "binding_set_digest": selection.binding_set_digest,
            "contract": PROJECTION_CONTRACT,
        },
    )
    ready_row_by_field = ready_result.mappings().first()
    return bool(ready_row_by_field and ready_row_by_field.get("projection_ready"))


_PAGE_SQL = f"""
WITH center AS MATERIALIZED (
    SELECT COALESCE(CAST(:latitude AS double precision), zip.latitude) AS latitude,
           COALESCE(CAST(:longitude AS double precision), zip.longitude) AS longitude,
           ST_SetSRID(
               ST_MakePoint(
                   COALESCE(CAST(:longitude AS double precision), zip.longitude),
                   COALESCE(CAST(:latitude AS double precision), zip.latitude)
               ),
               4326
           )::geography AS point
      FROM (SELECT 1) singleton
      LEFT JOIN {_table('geo_zip_lookup')} zip ON zip.zip_code = :zip5
     WHERE (CAST(:latitude AS double precision) IS NOT NULL
            AND CAST(:longitude AS double precision) IS NOT NULL)
        OR zip.zip_code IS NOT NULL
), selected_projection AS MATERIALIZED (
    SELECT candidate.projection_id
      FROM {_table('plan_pricing_em_distance_attachment')} attachment
      JOIN {_table('plan_pricing_em_distance_candidate')} candidate
        ON candidate.projection_id = attachment.projection_id
     WHERE attachment.serving_revision_id = :serving_revision_id
       AND candidate.serving_revision_id = :serving_revision_id
       AND candidate.binding_set_digest = :binding_set_digest
       AND candidate.contract_version = :contract
       AND candidate.state = 'ready'
), nearest AS MATERIALIZED (
    SELECT candidate.*
      FROM center
      CROSS JOIN selected_projection projection
      CROSS JOIN LATERAL (
        SELECT location.npi,
               location.provider_name,
               location.entity_type_code,
               location.credential,
               location.taxonomy_code,
               location.primary_specialty,
               location.classification,
               location.city,
               location.state,
               location.zip5,
               location.address_type_rank,
               location.address_checksum,
               location.location_key,
               rate.minimum_rates,
               rate.maximum_rates,
               rate.rate_counts,
               (location.point <-> center.point) / 1609.344 AS distance_miles
          FROM {_table('plan_pricing_em_distance_location')} location
          JOIN {_table('plan_pricing_em_distance_rate')} rate
            ON rate.projection_id = location.projection_id
           AND rate.npi = location.npi
         WHERE location.projection_id = projection.projection_id
           AND (rate.code_mask & CAST(:code_bit AS smallint)) <> 0
           AND ST_DWithin(location.point, center.point, :radius_meters)
         ORDER BY location.point <-> center.point,
                  location.npi,
                  location.address_type_rank,
                  location.address_checksum,
                  location.location_key
         LIMIT :candidate_limit
      ) candidate
), ranked AS MATERIALIZED (
    SELECT nearest.*,
           ROW_NUMBER() OVER (
               PARTITION BY nearest.npi
               ORDER BY nearest.distance_miles,
                        nearest.address_type_rank,
                        nearest.address_checksum,
                        nearest.location_key
           ) AS address_rank
      FROM nearest
), projected AS MATERIALIZED (
    SELECT * FROM ranked WHERE address_rank = 1
), page AS MATERIALIZED (
    SELECT projected.*
      FROM projected
     ORDER BY distance_miles, npi
     LIMIT :page_limit OFFSET :offset
), counts AS MATERIALIZED (
    SELECT (SELECT COUNT(*) FROM nearest)::bigint AS candidate_count,
           (SELECT COUNT(*) FROM projected)::bigint AS unique_count
)
SELECT page.*, counts.candidate_count, counts.unique_count,
       EXISTS (SELECT 1 FROM selected_projection) AS projection_ready
  FROM counts
  LEFT JOIN page ON TRUE
 ORDER BY page.distance_miles NULLS LAST, page.npi NULLS LAST
"""


def _card_item(
    card_row_by_field: Mapping[str, Any], code_index: int
) -> dict[str, Any]:
    minimum_rate_list = list(card_row_by_field.get("minimum_rates") or ())
    maximum_rate_list = list(card_row_by_field.get("maximum_rates") or ())
    rate_count_list = list(card_row_by_field.get("rate_counts") or ())
    rate_arrays = (minimum_rate_list, maximum_rate_list, rate_count_list)
    if not all(len(rate_array) == len(EM_CODES) for rate_array in rate_arrays):
        raise ValueError("E&M distance projection rate arrays are invalid")
    minimum = minimum_rate_list[code_index]
    maximum = maximum_rate_list[code_index]
    count = rate_count_list[code_index]
    if minimum is None or maximum is None or count is None:
        raise ValueError("E&M distance projection lost the requested code")
    return {
        "npi": int(card_row_by_field["npi"]),
        "provider_name": card_row_by_field.get("provider_name") or "TiC provider",
        "entity_type_code": card_row_by_field.get("entity_type_code"),
        "credential": card_row_by_field.get("credential"),
        "taxonomy_code": card_row_by_field.get("taxonomy_code"),
        "primary_specialty": card_row_by_field.get("primary_specialty"),
        "classification": card_row_by_field.get("classification"),
        "city": card_row_by_field.get("city"),
        "state": card_row_by_field.get("state"),
        "zip5": card_row_by_field.get("zip5"),
        "distance_miles": float(card_row_by_field["distance_miles"]),
        "minimum_negotiated_rate": _coerce_numeric_rate(Decimal(minimum)),
        "maximum_negotiated_rate": _coerce_numeric_rate(Decimal(maximum)),
        "rate_count": int(count),
    }


def _projection_response(
    args: Mapping[str, Any],
    pagination: Any,
    selection: PlanReleaseServingSelection,
    zip5: str | None,
    total: int,
    card_list: list[dict[str, Any]],
    *,
    is_total_exact: bool,
    has_more: bool,
) -> dict[str, Any]:
    """Shape the compact response without changing release coordinates."""

    query_by_field = {
        "code": args.get("code"),
        "code_system": args.get("code_system") or "CPT",
        "zip5": zip5,
        "zip_radius_miles": args.get("zip_radius_miles"),
        "lat": args.get("lat"),
        "long": args.get("long"),
        "radius_miles": args.get("radius_miles"),
        "view": "card",
        "include_providers": True,
        "order_by": "distance",
        "order": "asc",
        "projection_contract": PROJECTION_CONTRACT,
        "source": "plan_pricing_em_distance_projection",
        "status": "matched" if total else "no_match",
        "snapshots": [
            {
                "source_key": binding.source_key,
                "snapshot_id": binding.snapshot_id,
                "plan_id": binding.plan_id,
                "plan_market_type": binding.plan_market_type,
            }
            for binding in selection.in_network_bindings
        ],
    }
    response_by_field = {
        "result_type": "provider_cards",
        "result_state": "matched" if total else "no_match_in_radius",
        "pricing_scope": "plan_scoped_ptg",
        "resolved": True,
        "items": card_list,
        "pagination": {
            "total": total,
            "total_is_exact": is_total_exact,
            "total_lower_bound": total,
            "limit": int(pagination.limit),
            "offset": int(pagination.offset),
            "page": int(pagination.page),
            "has_more": has_more,
        },
        "query": query_by_field,
    }
    return annotate_plan_release_response(response_by_field, selection) or response_by_field


async def _progressive_page_rows(
    session: Any,
    selection: PlanReleaseServingSelection,
    args: Mapping[str, Any],
    pagination: Any,
    zip5: str | None,
    latitude: float | None,
    longitude: float | None,
    code_index: int,
) -> tuple[list[Mapping[str, Any]], bool, bool, int] | None:
    """Read only enough nearest locations to prove the requested page."""

    candidate_limit = max(
        _INITIAL_LOCATION_WINDOW,
        (int(pagination.offset) + int(pagination.limit) + 1) * 4,
    )
    while True:
        page_result = await session.execute(
            text(_PAGE_SQL),
            {
                "serving_revision_id": selection.serving_revision_id,
                "binding_set_digest": selection.binding_set_digest,
                "contract": PROJECTION_CONTRACT,
                "code_bit": 1 << code_index,
                "zip5": zip5,
                "latitude": latitude,
                "longitude": longitude,
                "radius_meters": float(_radius(args) or 0) * 1609.344,
                "candidate_limit": candidate_limit,
                "page_limit": int(pagination.limit) + 1,
                "offset": int(pagination.offset),
            },
        )
        page_rows_by_field = page_result.mappings().all()
        if not page_rows_by_field or not page_rows_by_field[0].get("projection_ready"):
            return None
        unique_count = int(page_rows_by_field[0].get("unique_count") or 0)
        candidate_count = int(page_rows_by_field[0].get("candidate_count") or 0)
        is_window_exhausted = candidate_count < candidate_limit
        has_page_boundary = unique_count > (
            int(pagination.offset) + int(pagination.limit)
        )
        if is_window_exhausted or has_page_boundary or candidate_limit >= _MAX_LOCATION_WINDOW:
            return (
                page_rows_by_field,
                is_window_exhausted,
                has_page_boundary,
                unique_count,
            )
        candidate_limit = min(candidate_limit * 2, _MAX_LOCATION_WINDOW)


async def search_plan_pricing_em_distance(
    session: Any,
    selection: PlanReleaseServingSelection,
    args: Mapping[str, Any],
    pagination: Any,
) -> dict[str, Any] | None:
    """Serve one exact bounded E&M distance-card page when attached."""

    code_index = _request_code_index(args)
    if code_index is None:
        return None
    if not _is_bounded_pagination(pagination):
        return None
    zip5 = str(args.get("zip5") or args.get("zip") or "").strip() or None
    try:
        latitude = None if zip5 else float(args.get("lat"))
        longitude = None if zip5 else float(args.get("long"))
    except (TypeError, ValueError):
        return None
    page_window = await _progressive_page_rows(
        session,
        selection,
        args,
        pagination,
        zip5,
        latitude,
        longitude,
        code_index,
    )
    if page_window is None:
        return None
    page_rows_by_field, is_window_exhausted, has_page_boundary, unique_count = (
        page_window
    )
    card_list = [
        _card_item(card_row_by_field, code_index)
        for card_row_by_field in page_rows_by_field
        if card_row_by_field.get("npi") is not None
    ]
    has_more = len(card_list) > int(pagination.limit) or (
        not is_window_exhausted and not has_page_boundary
    )
    card_list = card_list[: int(pagination.limit)]
    total = unique_count if is_window_exhausted else max(
        unique_count,
        int(pagination.offset) + len(card_list) + int(has_more),
    )
    return _projection_response(
        args,
        pagination,
        selection,
        zip5,
        total,
        card_list,
        is_total_exact=is_window_exhausted,
        has_more=has_more,
    )
