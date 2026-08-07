# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Build the non-sensitive exact-reader query from one authorized POST."""

from __future__ import annotations

from collections.abc import Mapping
import math

from api.billing_search_post_request_projection import (
    BillingSearchPostServiceQuery,
)
from api.ptg2_billing_search_contract import (
    BillingSearchResolvedQuery,
    serving_unavailable,
)


def _coordinate(
    value: object,
    *,
    minimum: float,
    maximum: float,
) -> float:
    if type(value) not in {float, int}:
        raise serving_unavailable()
    coordinate = float(value)
    if not math.isfinite(coordinate) or not minimum <= coordinate <= maximum:
        raise serving_unavailable()
    return 0.0 if coordinate == 0.0 else coordinate


def build_billing_search_resolved_query(
    service_query: BillingSearchPostServiceQuery,
    *,
    plan_release_id: str,
    radius_zip_context: Mapping[str, object] | None,
    after_sort_key: tuple[int | float | str, ...] | None,
) -> BillingSearchResolvedQuery:
    """Convert exact ZIP or server-resolved radius fields for the reader."""

    if type(service_query) is not BillingSearchPostServiceQuery:
        raise serving_unavailable()
    radius_miles = service_query.radius_miles
    if radius_miles == 0.0:
        if radius_zip_context is not None:
            raise serving_unavailable()
        geo_fields_by_name = {
            "zip5": service_query.zip5,
            "latitude": None,
            "longitude": None,
            "radius_miles": None,
        }
    else:
        if not isinstance(radius_zip_context, Mapping):
            raise serving_unavailable()
        context_zip5 = radius_zip_context.get("zip5")
        if type(context_zip5) is not str or context_zip5 != service_query.zip5:
            raise serving_unavailable()
        geo_fields_by_name = {
            "zip5": None,
            "latitude": _coordinate(
                radius_zip_context.get("latitude"),
                minimum=-90.0,
                maximum=90.0,
            ),
            "longitude": _coordinate(
                radius_zip_context.get("longitude"),
                minimum=-180.0,
                maximum=180.0,
            ),
            "radius_miles": radius_miles,
        }
    return BillingSearchResolvedQuery(
        plan_release_id=plan_release_id,
        selector_kind=service_query.selector_kind,
        tax_identity_type=service_query.tax_identity_type,
        code_system=service_query.code_system,
        code=service_query.code,
        provider_npi=service_query.provider_npi,
        modifiers=service_query.modifiers,
        place_of_service=service_query.place_of_service,
        include_evidence=service_query.include_evidence,
        limit=service_query.limit,
        after_sort_key=after_sort_key,
        **geo_fields_by_name,
    )


__all__ = ["build_billing_search_resolved_query"]
