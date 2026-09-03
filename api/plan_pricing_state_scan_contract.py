# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Public request and fixed-work limits for statewide pricing traversal."""

from __future__ import annotations

import re
from typing import Any, Mapping

import orjson

from api.plan_pricing_projection_contract import (
    PROJECTION_CONTRACT,
    PlanPricingProjectionUnsupported,
    projection_code_identity,
)
from api.plan_release_serving import annotate_plan_release_response
from api.ptg2_response import _is_request_flag_enabled, _shape_ptg2_response


STATE_SCAN_MAX_LIMIT = 200
STATE_SCAN_RATE_OCCURRENCE_LIMIT = 256
STATE_SCAN_PRICE_ATOM_LIMIT = 256
STATE_SCAN_RESPONSE_BYTE_LIMIT = 256 * 1024
STATE_SCAN_PROVIDER_MEMBERSHIP_LIMIT = 2_048
_STATE = re.compile(r"[A-Z]{2}", flags=re.ASCII)
_UNSUPPORTED_FILTERS = (
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
    "include_subspecialties",
    "primary_only",
    "city",
    "zip5",
    "zip",
    "lat",
    "long",
    "radius",
    "radius_miles",
    "zip_radius_miles",
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
    "plan_id",
    "plan_external_id",
    "plan_id_type",
    "plan_market_type",
    "market_type",
    "snapshot_id",
    "source_key",
    "year",
    "min_claims",
    "min_total_cost",
)


class PlanPricingStateScanBudgetExceeded(RuntimeError):
    """The requested fixed NPI page has too many complete rate groups."""


def is_plan_pricing_state_scan(args: Mapping[str, Any]) -> bool:
    """Select only the explicit additive NPI-ordered state scan lane."""

    return bool(
        str(args.get("plan_release_id") or "").strip()
        and str(args.get("order_by") or "").strip().lower() == "npi"
        and str(args.get("state") or "").strip()
    )


def _has_argument(args: Mapping[str, Any], field: str) -> bool:
    return args.get(field) not in (None, "", "null", False)


def _is_explicit_false(value: Any) -> bool:
    if isinstance(value, (list, tuple)):
        value = value[-1] if value else None
    return value is False or str(value or "").strip().lower() in {
        "0",
        "false",
        "no",
        "off",
    }


def _validate_scan_options(args: Mapping[str, Any], state: str) -> None:
    view = str(args.get("view") or "full").strip().lower()
    order = str(args.get("order") or "asc").strip().lower()
    if _STATE.fullmatch(state) is None:
        raise PlanPricingProjectionUnsupported("state scan requires a two-letter state")
    if view != "full" or not _is_request_flag_enabled(args.get("include_providers"), default=True):
        raise PlanPricingProjectionUnsupported("state scan requires view=full and include_providers=true")
    if not _is_explicit_false(args.get("include_allowed_amounts")):
        raise PlanPricingProjectionUnsupported(
            "state scan requires include_allowed_amounts=false"
        )
    if order != "asc":
        raise PlanPricingProjectionUnsupported("state scan requires order_by=npi and order=asc")


def validate_plan_pricing_state_scan(
    args: Mapping[str, Any],
) -> tuple[str, str, str]:
    """Return canonical code/state or reject semantics not represented by v4."""

    code_identity = projection_code_identity(args.get("code_system"), args.get("code"))
    if code_identity is None:
        raise PlanPricingProjectionUnsupported("state scan requires code_system and code")
    state = str(args.get("state") or "").strip().upper()
    _validate_scan_options(args, state)
    unsupported_filters = tuple(field for field in _UNSUPPORTED_FILTERS if _has_argument(args, field))
    if unsupported_filters:
        raise PlanPricingProjectionUnsupported("state scan does not support filters: " + ", ".join(unsupported_filters))
    return code_identity[0], code_identity[1], state


def pagination_metadata(
    limit: int,
    emitted_before: int,
    scanned_after: int,
    emitted_after: int,
    page_number: int,
    has_more: bool,
    next_cursor: str | None,
) -> dict[str, Any]:
    """Describe one exact-progress cursor page."""

    return {
        "total": emitted_after,
        "total_is_exact": not has_more,
        "total_lower_bound": emitted_after,
        "limit": limit,
        "offset": emitted_before,
        "page": page_number,
        "has_more": has_more,
        "next_cursor": next_cursor,
        "scanned_npi_count": scanned_after,
    }


def query_metadata(code_system: str, code: str, state: str) -> dict[str, Any]:
    """Describe the closed state-scan query shape."""

    return {
        "code": code,
        "code_system": code_system,
        "state": state,
        "order_by": "npi",
        "order": "asc",
        "view": "full",
        "include_providers": True,
        "projection_contract": PROJECTION_CONTRACT,
        "source": "plan_pricing_projection",
    }


def response_document(
    selection: Any,
    response_items: list[dict[str, Any]],
    pagination_by_field: dict[str, Any],
    query_by_field: dict[str, Any],
    args: Mapping[str, Any],
    *,
    byte_limit: int,
) -> dict[str, Any]:
    """Shape one public response before enforcing its byte budget."""

    response_by_field = {
        "items": response_items,
        "pagination": pagination_by_field,
        "query": query_by_field,
    }
    annotated = annotate_plan_release_response(response_by_field, selection)
    public_response = _shape_ptg2_response(annotated or response_by_field, dict(args))
    if len(orjson.dumps(public_response, default=str)) > byte_limit:
        raise PlanPricingStateScanBudgetExceeded(
            "state scan page exceeds its serialized response budget"
        )
    return public_response


__all__ = [
    "PlanPricingStateScanBudgetExceeded",
    "STATE_SCAN_MAX_LIMIT",
    "STATE_SCAN_PRICE_ATOM_LIMIT",
    "STATE_SCAN_PROVIDER_MEMBERSHIP_LIMIT",
    "STATE_SCAN_RATE_OCCURRENCE_LIMIT",
    "STATE_SCAN_RESPONSE_BYTE_LIMIT",
    "is_plan_pricing_state_scan",
    "pagination_metadata",
    "query_metadata",
    "response_document",
    "validate_plan_pricing_state_scan",
]
