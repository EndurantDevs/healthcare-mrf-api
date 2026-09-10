# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Public boundary for source-hidden packed hospital prices."""

from __future__ import annotations

import logging
import re
from typing import Any

import orjson
from sanic import Blueprint, response

from api.hospital_price_serving import HOSPITAL_PRICE_CACHE_CONTROL
from api.hospital_price_serving import MAX_HOSPITAL_PRICE_PUBLIC_BYTES
from api.hospital_price_serving import HospitalPriceCursorStaleError
from api.hospital_price_serving import HospitalPriceInvalidRequestError
from api.hospital_price_serving import HospitalPriceNotFoundError
from api.hospital_price_serving import HospitalPriceServingUnavailableError
from api.hospital_price_serving import read_hospital_price_page
from api.hospital_price_serving import validate_hospital_price_query
from api.hospital_price_request import validate_hospital_price_plan
from api.hospital_price_status import list_hospital_price_status_page


blueprint = Blueprint(
    "hospital_prices",
    url_prefix="/hospital-prices",
    version=1,
)
logger = logging.getLogger(__name__)
_QUERY_FIELDS = frozenset(
    {"code_type", "code", "payer_name", "plan_name", "plan_missing", "version_id", "cursor", "limit"}
)
_FACILITY_QUERY_FIELDS = frozenset({"q", "published", "cursor", "limit"})
_FACILITY_CURSOR_PATTERN = re.compile(r"hospital-[0-9]{6}\Z")
_FACILITY_LIMIT_PATTERN = re.compile(r"[1-9][0-9]{0,2}\Z")
_MAX_FACILITY_LIMIT = 200
_DEFAULT_FACILITY_LIMIT = 50
_ERROR_BY_STATUS = {
    400: ("hospital_price_invalid_request", "Hospital price request is invalid."),
    404: ("hospital_price_not_found", "Hospital price resource not found."),
    409: ("hospital_price_cursor_stale", "Hospital price pagination must restart."),
    503: (
        "hospital_price_serving_unavailable",
        "Hospital price serving is temporarily unavailable.",
    ),
}


def _get_session(request: Any) -> Any:
    session = getattr(getattr(request, "ctx", None), "sa_session", None)
    if session is None:
        raise HospitalPriceServingUnavailableError(
            "SQLAlchemy session is unavailable"
        )
    return session


def _query_values(
    request: Any,
    allowed_fields: frozenset[str] = _QUERY_FIELDS,
) -> dict[str, str]:
    args = getattr(request, "args", None)
    if args is None:
        return {}
    try:
        supplied_fields = set(args.keys())
    except (AttributeError, TypeError):
        raise HospitalPriceInvalidRequestError(
            "hospital price query is invalid"
        ) from None
    if not supplied_fields.issubset(allowed_fields):
        raise HospitalPriceInvalidRequestError("hospital price query is invalid")
    values_by_field: dict[str, str] = {}
    for field_name in sorted(supplied_fields):
        raw_values = (
            list(args.getlist(field_name))
            if hasattr(args, "getlist") else [args.get(field_name)]
        )
        if len(raw_values) != 1 or type(raw_values[0]) is not str:
            raise HospitalPriceInvalidRequestError(
                "hospital price query is invalid"
            )
        values_by_field[field_name] = raw_values[0]
    return values_by_field


def _facility_search_query(values_by_field: dict[str, str]) -> dict[str, object]:
    query = values_by_field.get("q")
    if query is not None and (
        not query or query != query.strip() or len(query) > 256
    ):
        raise HospitalPriceInvalidRequestError("hospital facility query is invalid")
    published_text = values_by_field.get("published")
    if published_text not in {None, "true", "false"}:
        raise HospitalPriceInvalidRequestError("hospital facility publication filter is invalid")
    cursor = values_by_field.get("cursor")
    if cursor is not None and _FACILITY_CURSOR_PATTERN.fullmatch(cursor) is None:
        raise HospitalPriceInvalidRequestError("hospital facility cursor is invalid")
    limit_text = values_by_field.get("limit")
    if limit_text is None:
        limit = _DEFAULT_FACILITY_LIMIT
    elif _FACILITY_LIMIT_PATTERN.fullmatch(limit_text):
        limit = int(limit_text)
    else:
        raise HospitalPriceInvalidRequestError("hospital facility limit is invalid")
    if limit > _MAX_FACILITY_LIMIT:
        raise HospitalPriceInvalidRequestError("hospital facility limit is invalid")
    return {
        "query": query,
        "status": (
            "succeeded"
            if published_text == "true"
            else "unpublished"
            if published_text == "false"
            else None
        ),
        "cursor": cursor,
        "limit": limit,
    }


def _public_facility_item(status_item: dict[str, Any]) -> dict[str, object]:
    publication = status_item.get("publication")
    publication_by_field = None
    if isinstance(publication, dict):
        publication_by_field = {
            "version_id": publication.get("version_id"),
            "source_format": publication.get("source_format"),
            "schema_version": publication.get("template_version"),
            "detected_schema_profile": publication.get("detected_schema_profile"),
            "last_updated_on": publication.get("last_updated_on"),
            "last_success_at": publication.get("last_success_at"),
            "service_count": publication.get("service_count"),
            "charge_count": publication.get("charge_count"),
            "payer_charge_count": publication.get("payer_charge_count"),
        }
    return {
        "hospital_id": status_item["hospital_id"],
        "alias_hospital_ids": status_item["alias_hospital_ids"],
        "name": status_item["name"],
        "publication": publication_by_field,
    }


def _json_response(payload: dict[str, object], *, status: int):
    encoded = orjson.dumps(payload)
    if status < 400 and len(encoded) > MAX_HOSPITAL_PRICE_PUBLIC_BYTES:
        raise HospitalPriceServingUnavailableError(
            "hospital price response exceeds its bound"
        )
    return response.raw(
        encoded,
        status=status,
        headers={"Cache-Control": HOSPITAL_PRICE_CACHE_CONTROL},
        content_type="application/json",
    )


def _error_response(status: int):
    code, message = _ERROR_BY_STATUS[status]
    return _json_response(
        {"error": {"code": code, "message": message}},
        status=status,
    )


def _failure_response(failure: Exception):
    if isinstance(failure, HospitalPriceInvalidRequestError):
        return _error_response(400)
    if isinstance(failure, HospitalPriceNotFoundError):
        return _error_response(404)
    if isinstance(failure, HospitalPriceCursorStaleError):
        return _error_response(409)
    logger.warning(
        "hospital price request failed",
        extra={"hospital_price_failure_class": type(failure).__name__},
    )
    return _error_response(503)


@blueprint.get("/facilities", name="hospital_prices.facilities")
async def search_hospital_price_facilities(request: Any):
    """Return source-hidden canonical facility identities for price discovery."""

    try:
        search_by_field = _facility_search_query(
            _query_values(request, _FACILITY_QUERY_FIELDS)
        )
        search_by_field["identity_query_only"] = True
        status_page = await list_hospital_price_status_page(**search_by_field)
        return _json_response(
            {
                "items": [
                    _public_facility_item(item) for item in status_page["items"]
                ],
                "next_cursor": status_page["next_cursor"],
            },
            status=200,
        )
    except Exception as failure:
        return _failure_response(failure)


@blueprint.get(
    "/facilities/<hospital_id>/prices",
    name="hospital_prices.facility_prices",
)
async def get_hospital_prices(request: Any, hospital_id: str):
    """Return one exact code's charge page and optional matching payer facts."""

    try:
        values = _query_values(request)
        query = validate_hospital_price_query(
            hospital_id,
            code_type=values.get("code_type"),
            code=values.get("code"),
            payer_name=values.get("payer_name"),
            plan_name=validate_hospital_price_plan(values.get("plan_name"), values.get("plan_missing")),
            version_id=values.get("version_id"),
            cursor=values.get("cursor"),
            limit=values.get("limit"),
        )
        return _json_response(
            await read_hospital_price_page(_get_session(request), query),
            status=200,
        )
    except Exception as failure:
        return _failure_response(failure)
