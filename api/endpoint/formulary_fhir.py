# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Public boundary for current source-hidden FHIR formulary data."""

from __future__ import annotations

import logging
import re
from typing import Any

import orjson
from sanic import Blueprint, response

from api.formulary_fhir_catalog import public_fhir_formulary_alias_page_payload
from api.formulary_fhir_catalog import public_fhir_formulary_page_payload
from api.formulary_fhir_catalog import read_current_fhir_formularies
from api.formulary_fhir_catalog import read_current_fhir_formulary_aliases
from api.formulary_fhir_drug_values import FHIRFormularyDrugFilters
from api.formulary_fhir_drug_values import public_fhir_formulary_drug_page_payload
from api.formulary_fhir_drug_values import public_fhir_formulary_drug_payload
from api.formulary_fhir_drugs import read_current_fhir_formulary_drug
from api.formulary_fhir_drugs import read_current_fhir_formulary_drug_page
from api.formulary_fhir_serving import FHIR_FORMULARY_CACHE_CONTROL
from api.formulary_fhir_serving import FHIRFormularyCursorConflictError
from api.formulary_fhir_serving import FHIRFormularyInvalidRequestError
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
    400: (
        "formulary_fhir_invalid_request",
        "FHIR formulary request is invalid.",
    ),
    404: (
        "formulary_fhir_not_found",
        "FHIR formulary not found.",
    ),
    409: (
        "formulary_fhir_cursor_stale",
        "FHIR formulary pagination must restart.",
    ),
    503: (
        "formulary_fhir_serving_unavailable",
        "FHIR formulary serving is temporarily unavailable.",
    ),
}
_DEFAULT_LIMIT = 25
_LIMIT_PATTERN = re.compile(r"[1-9][0-9]{0,2}\Z")
_COLLECTION_QUERY_FIELDS = frozenset({"cursor", "limit"})
_DRUG_QUERY_FIELDS = frozenset(
    {
        "cursor",
        "limit",
        "ndc11",
        "prior_authorization",
        "quantity_limit",
        "rxnorm_id",
        "step_therapy",
        "tier",
    }
)


def _get_session(request: Any) -> Any:
    session = getattr(request.ctx, "sa_session", None)
    if session is None:
        raise FHIRFormularyServingUnavailableError(
            "SQLAlchemy session is unavailable"
        )
    return session


def _query_values(
    request: Any,
    allowed_fields: frozenset[str],
) -> dict[str, str]:
    args = getattr(request, "args", None)
    if args is None:
        return {}
    try:
        supplied_fields = set(args.keys())
    except (AttributeError, TypeError):
        raise FHIRFormularyInvalidRequestError(
            "FHIR formulary query is invalid"
        ) from None
    if not supplied_fields.issubset(allowed_fields):
        raise FHIRFormularyInvalidRequestError(
            "FHIR formulary query is invalid"
        )
    values_by_field: dict[str, str] = {}
    for field_name in sorted(supplied_fields):
        if hasattr(args, "getlist"):
            raw_values = list(args.getlist(field_name))
        else:
            raw_values = [args.get(field_name)]
        if len(raw_values) != 1 or type(raw_values[0]) is not str:
            raise FHIRFormularyInvalidRequestError(
                "FHIR formulary query is invalid"
            )
        values_by_field[field_name] = raw_values[0]
    return values_by_field


def _limit(values_by_field: dict[str, str]) -> int:
    raw_limit = values_by_field.get("limit")
    if raw_limit is None:
        return _DEFAULT_LIMIT
    if _LIMIT_PATTERN.fullmatch(raw_limit) is None:
        raise FHIRFormularyInvalidRequestError(
            "FHIR formulary limit is invalid"
        )
    limit = int(raw_limit)
    if limit > 100:
        raise FHIRFormularyInvalidRequestError(
            "FHIR formulary limit is invalid"
        )
    return limit


def _optional_boolean(values_by_field: dict[str, str], field_name: str):
    raw_value = values_by_field.get(field_name)
    if raw_value is None:
        return None
    if raw_value == "true":
        return True
    if raw_value == "false":
        return False
    raise FHIRFormularyInvalidRequestError(
        "FHIR formulary policy filter is invalid"
    )


def _drug_filters(values_by_field: dict[str, str]) -> FHIRFormularyDrugFilters:
    return FHIRFormularyDrugFilters(
        rxnorm_id=values_by_field.get("rxnorm_id"),
        ndc11=values_by_field.get("ndc11"),
        tier=values_by_field.get("tier"),
        prior_authorization=_optional_boolean(
            values_by_field,
            "prior_authorization",
        ),
        step_therapy=_optional_boolean(values_by_field, "step_therapy"),
        quantity_limit=_optional_boolean(values_by_field, "quantity_limit"),
    )


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
    if isinstance(failure, FHIRFormularyInvalidRequestError):
        return _error_response(400)
    if isinstance(failure, FHIRFormularyNotFoundError):
        return _error_response(404)
    if isinstance(failure, FHIRFormularyCursorConflictError):
        return _error_response(409)
    if not isinstance(failure, FHIRFormularyServingUnavailableError):
        logger.warning(
            "FHIR formulary request failed",
            extra={"formulary_fhir_failure_class": type(failure).__name__},
        )
    return _error_response(503)


@blueprint.get("", strict_slashes=False, name="formulary_fhir.list")
async def get_current_formularies(request: Any):
    """Return one page of current published FHIR formularies."""

    try:
        query_by_field = _query_values(request, _COLLECTION_QUERY_FIELDS)
        page = await read_current_fhir_formularies(
            _get_session(request),
            limit=_limit(query_by_field),
            cursor=query_by_field.get("cursor"),
        )
        return _json_response(public_fhir_formulary_page_payload(page), status=200)
    except Exception as failure:
        return _failure_response(failure)


@blueprint.get("/<formulary_id>", name="formulary_fhir.detail")
async def get_current_formulary_detail(request: Any, formulary_id: str):
    """Return one current published FHIR plan without source identity."""

    try:
        _query_values(request, frozenset())
        detail = await read_current_fhir_formulary(
            _get_session(request),
            formulary_id,
        )
        return _json_response(public_fhir_formulary_payload(detail), status=200)
    except Exception as failure:
        return _failure_response(failure)


@blueprint.get(
    "/<formulary_id>/aliases",
    name="formulary_fhir.aliases",
)
async def get_current_formulary_aliases(request: Any, formulary_id: str):
    """Return opaque aliases for one current published formulary."""

    try:
        query_by_field = _query_values(request, _COLLECTION_QUERY_FIELDS)
        page = await read_current_fhir_formulary_aliases(
            _get_session(request),
            formulary_id,
            limit=_limit(query_by_field),
            cursor=query_by_field.get("cursor"),
        )
        payload = public_fhir_formulary_alias_page_payload(page)
        return _json_response(payload, status=200)
    except Exception as failure:
        return _failure_response(failure)


@blueprint.get(
    "/<formulary_id>/aliases/<alias_id>/drugs",
    name="formulary_fhir.drugs",
)
async def get_current_formulary_drugs(
    request: Any,
    formulary_id: str,
    alias_id: str,
):
    """Return one filtered page from one exact current DrugPlan alias."""

    try:
        query_by_field = _query_values(request, _DRUG_QUERY_FIELDS)
        page = await read_current_fhir_formulary_drug_page(
            _get_session(request),
            formulary_id,
            alias_id,
            filters=_drug_filters(query_by_field),
            limit=_limit(query_by_field),
            cursor=query_by_field.get("cursor"),
        )
        payload = public_fhir_formulary_drug_page_payload(page)
        return _json_response(payload, status=200)
    except Exception as failure:
        return _failure_response(failure)


@blueprint.get(
    "/<formulary_id>/aliases/<alias_id>/drugs/<drug_id>",
    name="formulary_fhir.drug_detail",
)
async def get_current_formulary_drug_detail(
    request: Any,
    formulary_id: str,
    alias_id: str,
    drug_id: str,
):
    """Return one version-scoped medication from one current alias."""

    try:
        _query_values(request, frozenset())
        drug = await read_current_fhir_formulary_drug(
            _get_session(request),
            formulary_id,
            alias_id,
            drug_id,
        )
        return _json_response(public_fhir_formulary_drug_payload(drug), status=200)
    except Exception as failure:
        return _failure_response(failure)


__all__ = (
    "blueprint",
    "get_current_formularies",
    "get_current_formulary_aliases",
    "get_current_formulary_detail",
    "get_current_formulary_drug_detail",
    "get_current_formulary_drugs",
)
