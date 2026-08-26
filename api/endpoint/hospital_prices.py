# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Public boundary for source-hidden packed hospital prices."""

from __future__ import annotations

import logging
from typing import Any

import orjson
from sanic import Blueprint, response

from api.hospital_price_serving import HOSPITAL_PRICE_CACHE_CONTROL
from api.hospital_price_serving import HospitalPriceCursorStaleError
from api.hospital_price_serving import HospitalPriceInvalidRequestError
from api.hospital_price_serving import HospitalPriceNotFoundError
from api.hospital_price_serving import HospitalPriceServingUnavailableError
from api.hospital_price_serving import read_hospital_price_page
from api.hospital_price_serving import validate_hospital_price_query


blueprint = Blueprint(
    "hospital_prices",
    url_prefix="/hospital-prices",
    version=1,
)
logger = logging.getLogger(__name__)
_QUERY_FIELDS = frozenset(
    {"code_type", "code", "payer_name", "plan_name", "version_id", "cursor", "limit"}
)
_ERROR_BY_STATUS = {
    400: ("hospital_price_invalid_request", "Hospital price request is invalid."),
    404: ("hospital_price_not_found", "Hospital price resource not found."),
    409: ("hospital_price_cursor_stale", "Hospital price pagination must restart."),
    503: (
        "hospital_price_serving_unavailable",
        "Hospital price serving is temporarily unavailable.",
    ),
}
_MAX_SUCCESS_BODY_BYTES = 2 << 20


def _get_session(request: Any) -> Any:
    session = getattr(getattr(request, "ctx", None), "sa_session", None)
    if session is None:
        raise HospitalPriceServingUnavailableError(
            "SQLAlchemy session is unavailable"
        )
    return session


def _query_values(request: Any) -> dict[str, str]:
    args = getattr(request, "args", None)
    if args is None:
        return {}
    try:
        supplied_fields = set(args.keys())
    except (AttributeError, TypeError):
        raise HospitalPriceInvalidRequestError(
            "hospital price query is invalid"
        ) from None
    if not supplied_fields.issubset(_QUERY_FIELDS):
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


def _json_response(payload: dict[str, object], *, status: int):
    encoded = orjson.dumps(payload)
    if status < 400 and len(encoded) > _MAX_SUCCESS_BODY_BYTES:
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
            plan_name=values.get("plan_name"),
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
