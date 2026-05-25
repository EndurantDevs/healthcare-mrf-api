# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
from decimal import Decimal
from typing import Any
from urllib.parse import unquote

import sanic.exceptions
from sanic import Blueprint, response
from sqlalchemy import and_, func, or_, select

from api.endpoint.pagination import parse_pagination
from db.models import (
    ClinicalArea,
    ClinicalAreaCondition,
    ClinicalAreaTreatment,
    ClinicalCodeCatalog,
    ClinicalCodeCrosswalk,
    ClinicalCodeRelationship,
    ClinicalCodeSynonym,
)

blueprint = Blueprint("clinical", url_prefix="/clinical", version=1)

code_table = ClinicalCodeCatalog.__table__
crosswalk_table = ClinicalCodeCrosswalk.__table__
synonym_table = ClinicalCodeSynonym.__table__
relationship_table = ClinicalCodeRelationship.__table__
area_table = ClinicalArea.__table__
area_condition_table = ClinicalAreaCondition.__table__
area_treatment_table = ClinicalAreaTreatment.__table__

MAX_LIMIT = 200
SYSTEM_ALIASES = {
    "ICD-10-CM": "ICD10CM",
    "ICD10": "ICD10CM",
    "ICD10CM_COMPACT": "ICD10CM_COMPACT",
    "RXCUI": "RXNORM",
    "SNOMED": "SNOMEDCT_US",
    "SNOMEDCT": "SNOMEDCT_US",
}


def _session(request):
    session = getattr(request.ctx, "sa_session", None)
    if session is None:
        raise RuntimeError("SQLAlchemy session not available on request context")
    return session


def _normalize_system(raw: Any) -> str:
    system = str(raw or "").strip().upper()
    return SYSTEM_ALIASES.get(system, system)


def _normalize_code(raw: Any) -> str:
    return str(raw or "").strip().upper()


def _decode_path_value(raw: Any) -> str:
    return unquote(str(raw or "").strip())


def _row_to_dict(row) -> dict[str, Any]:
    mapping = getattr(row, "_mapping", None)
    return dict(mapping) if mapping is not None else dict(row)


def _json_safe(value: Any) -> Any:
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def _json_safe_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: _json_safe(value) for key, value in row.items()}


async def _resolve_code(session, system: str, code: str, code_type: str | None = None) -> tuple[str, str]:
    direct_filters = [
        func.upper(code_table.c.code_system) == system,
        func.upper(code_table.c.code) == code,
    ]
    if code_type:
        direct_filters.append(code_table.c.code_type == code_type)
    direct = await session.execute(select(code_table.c.code_system, code_table.c.code).where(and_(*direct_filters)).limit(1))
    found = direct.first()
    if found:
        mapped = _row_to_dict(found)
        return mapped["code_system"], mapped["code"]

    mapped = await session.execute(
        select(crosswalk_table.c.to_system, crosswalk_table.c.to_code)
        .where(
            and_(
                func.upper(crosswalk_table.c.from_system) == system,
                func.upper(crosswalk_table.c.from_code) == code,
            )
        )
        .limit(1)
    )
    found = mapped.first()
    if found:
        row = _row_to_dict(found)
        return row["to_system"], row["to_code"]
    return system, code


async def _list_codes(request, code_type: str):
    session = _session(request)
    args = request.args
    pagination = parse_pagination(args, default_limit=25, max_limit=MAX_LIMIT)
    system = _normalize_system(args.get("system") or args.get("code_system"))
    q = str(args.get("q") or "").strip().lower()
    clinical_area_id = str(args.get("clinical_area_id") or "").strip()

    if clinical_area_id and code_type in {"condition", "treatment"}:
        return await _list_area_concepts_response(
            request,
            clinical_area_id=clinical_area_id,
            mapping_kind=code_type,
            system=system,
            q=q,
            require_area=False,
        )

    filters = [code_table.c.code_type == code_type]
    if system:
        filters.append(func.upper(code_table.c.code_system) == system)
    if q:
        q_like = f"%{q}%"
        filters.append(
            or_(
                func.lower(code_table.c.code).like(q_like),
                func.lower(code_table.c.display_name).like(q_like),
                func.lower(code_table.c.short_description).like(q_like),
            )
        )

    where_clause = and_(*filters)
    count_result = await session.execute(select(func.count()).select_from(code_table).where(where_clause))
    total = int(count_result.scalar() or 0)
    rows = await session.execute(
        select(code_table)
        .where(where_clause)
        .order_by(code_table.c.code_system, code_table.c.code)
        .limit(pagination.limit)
        .offset(pagination.offset)
    )
    return response.json(
        {
            "items": [_json_safe_row(_row_to_dict(row)) for row in rows],
            "pagination": {
                "total": total,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": pagination.page,
            },
            "query": {
                "code_type": code_type,
                "system": system or None,
                "q": q or None,
                "clinical_area_id": clinical_area_id or None,
            },
        }
    )


async def _area_counts(session, clinical_area_id: str) -> tuple[int, int]:
    condition_count_result = await session.execute(
        select(func.count())
        .select_from(area_condition_table)
        .where(area_condition_table.c.clinical_area_id == clinical_area_id)
    )
    treatment_count_result = await session.execute(
        select(func.count())
        .select_from(area_treatment_table)
        .where(area_treatment_table.c.clinical_area_id == clinical_area_id)
    )
    return int(condition_count_result.scalar() or 0), int(treatment_count_result.scalar() or 0)


async def _area_payload(session, area_row) -> dict[str, Any]:
    payload = _json_safe_row(_row_to_dict(area_row))
    condition_count, treatment_count = await _area_counts(session, payload["clinical_area_id"])
    payload["condition_count"] = condition_count
    payload["treatment_count"] = treatment_count
    return payload


def _area_mapping_columns(mapping_kind: str):
    if mapping_kind == "condition":
        return area_condition_table, area_condition_table.c.condition_system, area_condition_table.c.condition_code
    if mapping_kind == "treatment":
        return area_treatment_table, area_treatment_table.c.treatment_system, area_treatment_table.c.treatment_code
    raise ValueError(f"Unsupported clinical area mapping kind: {mapping_kind}")


async def _list_area_concepts_response(
    request,
    *,
    clinical_area_id: str,
    mapping_kind: str,
    system: str,
    q: str,
    require_area: bool,
):
    session = _session(request)
    pagination = parse_pagination(request.args, default_limit=25, max_limit=MAX_LIMIT)
    mapping_table, system_col, code_col = _area_mapping_columns(mapping_kind)

    if require_area:
        area_exists = await session.execute(
            select(area_table.c.clinical_area_id).where(area_table.c.clinical_area_id == clinical_area_id).limit(1)
        )
        if not area_exists.first():
            raise sanic.exceptions.NotFound

    filters = [mapping_table.c.clinical_area_id == clinical_area_id]
    if system:
        filters.append(func.upper(system_col) == system)
    if q:
        q_like = f"%{q}%"
        filters.append(
            or_(
                func.lower(code_table.c.code).like(q_like),
                func.lower(code_table.c.display_name).like(q_like),
                func.lower(code_table.c.short_description).like(q_like),
            )
        )

    where_clause = and_(*filters)
    joined = mapping_table.join(
        code_table,
        and_(
            code_table.c.code_system == system_col,
            code_table.c.code == code_col,
        ),
    )
    count_result = await session.execute(select(func.count()).select_from(joined).where(where_clause))
    rows = await session.execute(
        select(code_table, mapping_table.c.source.label("area_mapping_source"))
        .select_from(joined)
        .where(where_clause)
        .order_by(code_table.c.code_system, code_table.c.code)
        .limit(pagination.limit)
        .offset(pagination.offset)
    )
    return response.json(
        {
            "items": [_json_safe_row(_row_to_dict(row)) for row in rows],
            "pagination": {
                "total": int(count_result.scalar() or 0),
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": pagination.page,
            },
            "query": {
                "clinical_area_id": clinical_area_id,
                "mapping_kind": mapping_kind,
                "system": system or None,
                "q": q or None,
            },
        }
    )


@blueprint.get("/concepts")
async def list_concepts(request):
    session = _session(request)
    args = request.args
    pagination = parse_pagination(args, default_limit=25, max_limit=MAX_LIMIT)
    system = _normalize_system(args.get("system") or args.get("code_system"))
    code_type = str(args.get("code_type") or "").strip().lower()
    q = str(args.get("q") or "").strip().lower()
    filters = []
    if system:
        filters.append(func.upper(code_table.c.code_system) == system)
    if code_type:
        filters.append(func.lower(code_table.c.code_type) == code_type)
    if q:
        q_like = f"%{q}%"
        filters.append(
            or_(
                func.lower(code_table.c.code).like(q_like),
                func.lower(code_table.c.display_name).like(q_like),
                func.lower(code_table.c.short_description).like(q_like),
            )
        )
    where_clause = and_(*filters) if filters else None
    count_query = select(func.count()).select_from(code_table)
    query = select(code_table).order_by(code_table.c.code_system, code_table.c.code).limit(pagination.limit).offset(pagination.offset)
    if where_clause is not None:
        count_query = count_query.where(where_clause)
        query = query.where(where_clause)
    count_result = await session.execute(count_query)
    rows = await session.execute(query)
    return response.json(
        {
            "items": [_json_safe_row(_row_to_dict(row)) for row in rows],
            "pagination": {
                "total": int(count_result.scalar() or 0),
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": pagination.page,
            },
            "query": {"system": system or None, "code_type": code_type or None, "q": q or None},
        }
    )


@blueprint.get("/concepts/<system>/<code>")
async def get_concept(request, system: str, code: str):
    session = _session(request)
    resolved_system, resolved_code = await _resolve_code(session, _normalize_system(system), _normalize_code(code))
    result = await session.execute(
        select(code_table).where(and_(code_table.c.code_system == resolved_system, code_table.c.code == resolved_code)).limit(1)
    )
    row = result.first()
    if not row:
        raise sanic.exceptions.NotFound
    payload = _json_safe_row(_row_to_dict(row))
    synonym_rows = await session.execute(
        select(synonym_table)
        .where(and_(synonym_table.c.code_system == resolved_system, synonym_table.c.code == resolved_code))
        .limit(100)
    )
    rel_rows = await session.execute(
        select(relationship_table)
        .where(and_(relationship_table.c.from_system == resolved_system, relationship_table.c.from_code == resolved_code))
        .limit(100)
    )
    payload["synonyms"] = [_json_safe_row(_row_to_dict(item)) for item in synonym_rows]
    payload["relationships"] = [_json_safe_row(_row_to_dict(item)) for item in rel_rows]
    return response.json(payload)


async def _get_code(request, code_type: str, system: str, code: str):
    session = _session(request)
    resolved_system, resolved_code = await _resolve_code(
        session,
        _normalize_system(system),
        _normalize_code(code),
        code_type=code_type,
    )
    result = await session.execute(
        select(code_table).where(
            and_(
                code_table.c.code_type == code_type,
                code_table.c.code_system == resolved_system,
                code_table.c.code == resolved_code,
            )
        )
    )
    row = result.first()
    if not row:
        raise sanic.exceptions.NotFound
    return response.json(_json_safe_row(_row_to_dict(row)))


@blueprint.get("/relationships")
async def list_relationships(request):
    session = _session(request)
    args = request.args
    pagination = parse_pagination(args, default_limit=25, max_limit=MAX_LIMIT)
    from_system = _normalize_system(args.get("from_system"))
    to_system = _normalize_system(args.get("to_system"))
    from_code = _normalize_code(args.get("from_code"))
    to_code = _normalize_code(args.get("to_code"))
    relationship = str(args.get("relationship") or "").strip()
    filters = []
    if from_system:
        filters.append(func.upper(relationship_table.c.from_system) == from_system)
    if from_code:
        filters.append(func.upper(relationship_table.c.from_code) == from_code)
    if to_system:
        filters.append(func.upper(relationship_table.c.to_system) == to_system)
    if to_code:
        filters.append(func.upper(relationship_table.c.to_code) == to_code)
    if relationship:
        filters.append(relationship_table.c.relationship == relationship)
    where_clause = and_(*filters) if filters else None
    count_query = select(func.count()).select_from(relationship_table)
    query = select(relationship_table).limit(pagination.limit).offset(pagination.offset)
    if where_clause is not None:
        count_query = count_query.where(where_clause)
        query = query.where(where_clause)
    count_result = await session.execute(count_query)
    rows = await session.execute(query)
    return response.json(
        {
            "items": [_json_safe_row(_row_to_dict(row)) for row in rows],
            "pagination": {
                "total": int(count_result.scalar() or 0),
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": pagination.page,
            },
            "query": {
                "from_system": from_system or None,
                "from_code": from_code or None,
                "to_system": to_system or None,
                "to_code": to_code or None,
                "relationship": relationship or None,
            },
        }
    )


@blueprint.get("/conditions")
async def list_conditions(request):
    return await _list_codes(request, "condition")


@blueprint.get("/conditions/<system>/<code>")
async def get_condition(request, system: str, code: str):
    return await _get_code(request, "condition", system, code)


@blueprint.get("/treatments")
async def list_treatments(request):
    return await _list_codes(request, "treatment")


@blueprint.get("/treatments/<system>/<code>")
async def get_treatment(request, system: str, code: str):
    return await _get_code(request, "treatment", system, code)


@blueprint.get("/clinical-areas")
async def list_clinical_areas(request):
    session = _session(request)
    args = request.args
    pagination = parse_pagination(args, default_limit=25, max_limit=MAX_LIMIT)
    q = str(args.get("q") or "").strip().lower()
    filters = []
    if q:
        q_like = f"%{q}%"
        filters.append(
            or_(
                func.lower(area_table.c.clinical_area_id).like(q_like),
                func.lower(area_table.c.display_name).like(q_like),
                func.lower(area_table.c.description).like(q_like),
            )
        )
    where_clause = and_(*filters) if filters else None
    count_query = select(func.count()).select_from(area_table)
    query = select(area_table).order_by(area_table.c.display_name).limit(pagination.limit).offset(pagination.offset)
    if where_clause is not None:
        count_query = count_query.where(where_clause)
        query = query.where(where_clause)
    count_result = await session.execute(count_query)
    rows = await session.execute(query)
    items = [await _area_payload(session, row) for row in rows]
    return response.json(
        {
            "items": items,
            "pagination": {
                "total": int(count_result.scalar() or 0),
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": pagination.page,
            },
            "query": {"q": q or None},
        }
    )


@blueprint.get("/clinical-areas/<clinical_area_id>")
async def get_clinical_area(request, clinical_area_id: str):
    session = _session(request)
    clinical_area_id = _decode_path_value(clinical_area_id)
    area_result = await session.execute(
        select(area_table).where(area_table.c.clinical_area_id == clinical_area_id).limit(1)
    )
    area = area_result.first()
    if not area:
        raise sanic.exceptions.NotFound
    payload = await _area_payload(session, area)
    return response.json(payload)


@blueprint.get("/clinical-areas/<clinical_area_id>/conditions")
async def list_clinical_area_conditions(request, clinical_area_id: str):
    args = request.args
    return await _list_area_concepts_response(
        request,
        clinical_area_id=_decode_path_value(clinical_area_id),
        mapping_kind="condition",
        system=_normalize_system(args.get("system") or args.get("code_system")),
        q=str(args.get("q") or "").strip().lower(),
        require_area=True,
    )


@blueprint.get("/clinical-areas/<clinical_area_id>/treatments")
async def list_clinical_area_treatments(request, clinical_area_id: str):
    args = request.args
    return await _list_area_concepts_response(
        request,
        clinical_area_id=_decode_path_value(clinical_area_id),
        mapping_kind="treatment",
        system=_normalize_system(args.get("system") or args.get("code_system")),
        q=str(args.get("q") or "").strip().lower(),
        require_area=True,
    )


@blueprint.get("/crosswalk")
async def list_crosswalk(request):
    session = _session(request)
    args = request.args
    pagination = parse_pagination(args, default_limit=25, max_limit=MAX_LIMIT)
    from_system = _normalize_system(args.get("from_system"))
    to_system = _normalize_system(args.get("to_system"))
    code = _normalize_code(args.get("code"))
    filters = []
    if from_system:
        filters.append(func.upper(crosswalk_table.c.from_system) == from_system)
    if to_system:
        filters.append(func.upper(crosswalk_table.c.to_system) == to_system)
    if code:
        filters.append(or_(func.upper(crosswalk_table.c.from_code) == code, func.upper(crosswalk_table.c.to_code) == code))
    where_clause = and_(*filters) if filters else None
    count_query = select(func.count()).select_from(crosswalk_table)
    query = select(crosswalk_table).limit(pagination.limit).offset(pagination.offset)
    if where_clause is not None:
        count_query = count_query.where(where_clause)
        query = query.where(where_clause)
    count_result = await session.execute(count_query)
    rows = await session.execute(query)
    return response.json(
        {
            "items": [_json_safe_row(_row_to_dict(row)) for row in rows],
            "pagination": {
                "total": int(count_result.scalar() or 0),
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": pagination.page,
            },
            "query": {"from_system": from_system or None, "to_system": to_system or None, "code": code or None},
        }
    )
