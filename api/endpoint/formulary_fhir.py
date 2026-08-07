# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Public boundary for one current FHIR formulary detail."""

from __future__ import annotations

import logging
from typing import Any

import orjson
from sanic import Blueprint, response

from api.formulary_fhir_serving import FHIR_FORMULARY_CACHE_CONTROL
from api.formulary_fhir_serving import FHIRFormularyNotFoundError
from api.formulary_fhir_serving import FHIRFormularyServingUnavailableError
from api.formulary_fhir_serving import public_fhir_formulary_payload
from api.formulary_fhir_serving import read_current_fhir_formulary


blueprint = Blueprint(
    "formulary_fhir",
    url_prefix="/formulary/fhir",
    version=1,
)
logger = logging.getLogger(__name__)
_ERROR_BY_STATUS = {
    404: (
        "formulary_fhir_not_found",
        "FHIR formulary not found.",
    ),
    503: (
        "formulary_fhir_serving_unavailable",
        "FHIR formulary serving is temporarily unavailable.",
    ),
}


def _get_session(request: Any) -> Any:
    session = getattr(request.ctx, "sa_session", None)
    if session is None:
        raise FHIRFormularyServingUnavailableError(
            "SQLAlchemy session is unavailable"
        )
    return session


def _json_response(payload: dict[str, object], *, status: int):
    return response.raw(
        orjson.dumps(payload),
        status=status,
        headers={"Cache-Control": FHIR_FORMULARY_CACHE_CONTROL},
        content_type="application/json",
    )


def _error_response(status: int):
    code, message = _ERROR_BY_STATUS[status]
    return _json_response(
        {"error": {"code": code, "message": message}},
        status=status,
    )


def _failure_response(failure: Exception):
    if isinstance(failure, FHIRFormularyNotFoundError):
        return _error_response(404)
    if not isinstance(failure, FHIRFormularyServingUnavailableError):
        logger.warning(
            "FHIR formulary detail request failed",
            extra={"formulary_fhir_failure_class": type(failure).__name__},
        )
    return _error_response(503)


@blueprint.get("/<formulary_id>", name="formulary_fhir.detail")
async def get_current_formulary_detail(request: Any, formulary_id: str):
    """Return one current published FHIR plan without source identity."""

    try:
        detail = await read_current_fhir_formulary(
            _get_session(request),
            formulary_id,
        )
        return _json_response(public_fhir_formulary_payload(detail), status=200)
    except Exception as failure:
        return _failure_response(failure)


__all__ = ("blueprint", "get_current_formulary_detail")
